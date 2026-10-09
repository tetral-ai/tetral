package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type handoffRuntimeChild struct {
	directory string
	port      int
	httpURL   string
	command   *exec.Cmd
	done      chan struct{}
	err       error
}

type handoffProviderEntry struct {
	SessionID       string `json:"sessionId"`
	SessionThreadID string `json:"sessionThreadId"`
	RequestID       string `json:"requestId"`
	ModelRequestID  string `json:"modelRequestId"`
	Ordinal         int    `json:"ordinal"`
	MessagesJSON    string `json:"messagesJson"`
}

func (p *handoffRuntimeChild) providerEntries(t *testing.T, session string) []handoffProviderEntry {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(p.directory, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ledger []handoffProviderEntry
	if err := json.Unmarshal(raw, &ledger); err != nil {
		t.Fatal(err)
	}
	var entries []handoffProviderEntry
	for _, entry := range ledger {
		if entry.SessionID == session {
			entries = append(entries, entry)
		}
	}
	return entries
}

type handoffObservedBridgeStore struct {
	bridge.BridgeAPIStore
	activeAwait, joinedAwait, releaseDuringAwait atomic.Int32
	cancelledAwait                               atomic.Int32
	lastAwaitDuration                            atomic.Int64
}

type handoffFaultBridgeStore struct {
	bridge.BridgeAPIStore
	beforeRelease func(*bridgev1.ReleaseRuntimeBindingRequest) error
	afterWrite    func(*bridgev1.WriteEventRequest, *bridgev1.WriteEventResponse) error
	afterAccept   func(*bridgev1.AcceptSandboxExecutionRequest, *bridgev1.AcceptSandboxExecutionResponse) error
}

func (s *handoffFaultBridgeStore) ReleaseRuntimeBinding(ctx context.Context, r *bridgev1.ReleaseRuntimeBindingRequest) (*bridgev1.ReleaseRuntimeBindingResponse, error) {
	if s.beforeRelease != nil {
		if err := s.beforeRelease(r); err != nil {
			return nil, err
		}
	}
	return s.BridgeAPIStore.ReleaseRuntimeBinding(ctx, r)
}
func (s *handoffFaultBridgeStore) WriteEvent(ctx context.Context, r *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	response, err := s.BridgeAPIStore.WriteEvent(ctx, r)
	if err == nil && s.afterWrite != nil {
		err = s.afterWrite(r, response)
	}
	return response, err
}
func (s *handoffFaultBridgeStore) AcceptSandboxExecution(ctx context.Context, r *bridgev1.AcceptSandboxExecutionRequest) (*bridgev1.AcceptSandboxExecutionResponse, error) {
	response, err := s.BridgeAPIStore.AcceptSandboxExecution(ctx, r)
	if err == nil && s.afterAccept != nil {
		err = s.afterAccept(r, response)
	}
	return response, err
}

// handoffOrderedBridgeStore establishes one selected legal order at the actual
// ACK boundary. Waiting happens before the production store opens a transaction;
// independent SQL confirms the original request/tool before releasing it.
type handoffOrderedBridgeStore struct {
	bridge.BridgeAPIStore
	admin                         *sql.DB
	session                       string
	endFirst                      bool
	modelRequest                  atomic.Value
	endCommitted, resultCommitted chan struct{}
	endOnce, resultOnce           sync.Once
	endEntered, resultEntered     atomic.Bool
	ctx                           context.Context
	cancel                        context.CancelFunc
	mu                            sync.Mutex
	closed                        bool
	calls                         sync.WaitGroup
}

func newHandoffOrderedBridgeStore(store bridge.BridgeAPIStore, admin *sql.DB, session string, endFirst bool) *handoffOrderedBridgeStore {
	//nolint:gosec // The returned fixture's close method owns cancellation and bounded call join.
	ctx, cancel := context.WithCancel(context.Background())
	s := &handoffOrderedBridgeStore{BridgeAPIStore: store, admin: admin, session: session, endFirst: endFirst, endCommitted: make(chan struct{}), resultCommitted: make(chan struct{}), ctx: ctx, cancel: cancel}
	s.modelRequest.Store("")
	return s
}

func (s *handoffOrderedBridgeStore) enter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.calls.Add(1)
	return true
}

func (s *handoffOrderedBridgeStore) close(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	joined := make(chan struct{})
	go func() { s.calls.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Error("ordering Bridge calls did not join within cleanup bound")
	}
}

func (s *handoffOrderedBridgeStore) wait(ctx context.Context, committed <-chan struct{}) error {
	select {
	case <-committed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *handoffOrderedBridgeStore) WriteRequestEnd(ctx context.Context, r *bridgev1.WriteRequestEndRequest) (*bridgev1.WriteRequestEndResponse, error) {
	if r.GetScope().GetSessionId() != s.session || r.GetModelRequestId() != s.modelRequest.Load().(string) {
		return s.BridgeAPIStore.WriteRequestEnd(ctx, r)
	}
	if !s.enter() {
		return nil, context.Canceled
	}
	defer s.calls.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	s.endEntered.Store(true)
	if !s.endFirst {
		if err := s.wait(ctx, s.resultCommitted); err != nil {
			return nil, err
		}
	}
	response, err := s.BridgeAPIStore.WriteRequestEnd(ctx, r)
	if err != nil {
		return response, err
	}
	if response.GetCommitted() == nil && response.GetDuplicate() == nil {
		return nil, status.Error(codes.Internal, "original ordering End was not committed")
	}
	var count int
	if err := s.admin.QueryRowContext(ctx, `SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'`, s.session, r.ModelRequestId).Scan(&count); err != nil || count != 1 {
		return nil, status.Errorf(codes.Internal, "original ordering End oracle count=%d err=%v", count, err)
	}
	s.endOnce.Do(func() { close(s.endCommitted) })
	return response, nil
}

func (s *handoffOrderedBridgeStore) SettleToolResult(ctx context.Context, r *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	if r.GetScope().GetSessionId() != s.session {
		return s.BridgeAPIStore.SettleToolResult(ctx, r)
	}
	if !s.enter() {
		return nil, context.Canceled
	}
	defer s.calls.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	var modelRequest string
	if err := s.admin.QueryRowContext(ctx, `SELECT model_request_id FROM session_events WHERE session_id=$1 AND event_id=$2 AND type='agent.tool_use'`, s.session, r.GetSettlement().GetToolUseEventId()).Scan(&modelRequest); err != nil {
		return nil, err
	}
	if modelRequest != s.modelRequest.Load().(string) {
		return s.BridgeAPIStore.SettleToolResult(ctx, r)
	}
	s.resultEntered.Store(true)
	if s.endFirst {
		if err := s.wait(ctx, s.endCommitted); err != nil {
			return nil, err
		}
	}
	response, err := s.BridgeAPIStore.SettleToolResult(ctx, r)
	if err != nil {
		return response, err
	}
	if response.GetCommitted() == nil && response.GetDuplicate() == nil {
		return nil, status.Error(codes.Internal, "original ordering result was not committed")
	}
	var count int
	if err := s.admin.QueryRowContext(ctx, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$2`, s.session, r.Settlement.ToolUseEventId).Scan(&count); err != nil || count != 1 {
		return nil, status.Errorf(codes.Internal, "original ordering result oracle count=%d err=%v", count, err)
	}
	s.resultOnce.Do(func() { close(s.resultCommitted) })
	return response, nil
}

type handoffObservedDeliverer struct {
	jobrunner.RuntimePodDirectDeliverer
	result                          jobrunner.RuntimeDeliveryResult
	jobID, sessionID, kind, inputID string
}

func (d *handoffObservedDeliverer) DeliverRuntimeJob(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, error) {
	d.jobID, d.sessionID, d.kind, d.inputID = job.JobID, job.SessionID, job.Kind, job.RuntimeInputID
	result, err := d.RuntimePodDirectDeliverer.DeliverRuntimeJob(ctx, job)
	d.result = result
	return result, err
}

func (s *handoffObservedBridgeStore) AwaitSandboxExecution(ctx context.Context, request *bridgev1.AwaitSandboxExecutionRequest) (*bridgev1.AwaitSandboxExecutionResponse, error) {
	s.activeAwait.Add(1)
	started := time.Now()
	defer func() {
		s.lastAwaitDuration.Store(time.Since(started).Nanoseconds())
		if ctx.Err() != nil {
			s.cancelledAwait.Add(1)
		}
		s.activeAwait.Add(-1)
		s.joinedAwait.Add(1)
	}()
	return s.BridgeAPIStore.AwaitSandboxExecution(ctx, request)
}
func (s *handoffObservedBridgeStore) ReleaseRuntimeBinding(ctx context.Context, request *bridgev1.ReleaseRuntimeBindingRequest) (*bridgev1.ReleaseRuntimeBindingResponse, error) {
	if s.activeAwait.Load() > 0 {
		s.releaseDuringAwait.Add(1)
	}
	return s.BridgeAPIStore.ReleaseRuntimeBinding(ctx, request)
}

func startHandoffRuntimeChild(t *testing.T, address, podUID, processID, token string, replacement bool, idle []map[string]any, review ...string) *handoffRuntimeChild {
	t.Helper()
	child := &handoffRuntimeChild{directory: t.TempDir(), done: make(chan struct{})}
	scenario := ""
	if len(review) > 0 {
		scenario = review[0]
	}
	params := map[string]any{"bridgeAddress": address, "podUID": podUID, "processID": processID, "token": token, "directory": child.directory, "replacement": replacement, "idle": idle}
	if scenario != "" && !replacement && len(idle) > 0 {
		params["unrelatedReviewer"] = map[string]any{"sessionId": idle[0]["sessionId"], "sessionThreadId": idle[0]["sessionThreadId"], "parentThreadId": idle[0]["parentThreadId"], "bindingId": idle[0]["bindingId"], "bindingGeneration": idle[0]["bindingGeneration"]}
	}
	if len(review) > 2 {
		params["loggerMode"] = review[2]
	}
	if len(review) > 1 && review[1] != "" {
		params["mcpAddress"] = review[1]
	}
	if scenario != "" {
		params["reviewerScenario"] = scenario
	}
	input, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(child.directory, "input.json")
	if err = os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(filepath.Join(child.directory, "output.log"))
	if err != nil {
		t.Fatal(err)
	}
	child.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/replica-runtime-handoff.ts", path) //nolint:gosec // Fixed repository child and test-owned input.
	child.command.Dir = "../services/agent-runtime"
	child.command.Stdout = output
	child.command.Stderr = output
	if err = child.command.Start(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	go func() { child.err = child.command.Wait(); _ = output.Close(); close(child.done) }()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = child.command.Process.Kill()
			select {
			case <-child.done:
			case <-time.After(5 * time.Second):
				t.Error("Runtime child cleanup did not join")
			}
		}
	})
	waitHandoffCondition(t, "actual Runtime registration and readiness", func() bool {
		raw, err := os.ReadFile(filepath.Join(child.directory, "ready.json"))
		var ready struct {
			Port    int    `json:"port"`
			HTTPURL string `json:"httpUrl"`
		}
		if err == nil && json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
			child.port = ready.Port
			child.httpURL = ready.HTTPURL
			return true
		}
		select {
		case <-child.done:
			out, _ := os.ReadFile(filepath.Join(child.directory, "output.log"))
			t.Fatalf("Runtime startup exited: %v\n%s", child.err, out)
		default:
		}
		return false
	})
	return child
}
func waitHandoffCondition(t *testing.T, name string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("required barrier %s not reached", name)
}
func waitHandoffAdmissionClosed(t *testing.T, child *handoffRuntimeChild) {
	t.Helper()
	waitHandoffCondition(t, "actual admission closure", func() bool {
		raw, err := os.ReadFile(filepath.Join(child.directory, "quiescing.json"))
		return err == nil && strings.Contains(string(raw), `"accepting":false`)
	})
}
func assertHandoffUnrelatedInputRejected(t *testing.T, child *handoffRuntimeChild, session, thread, binding string) {
	t.Helper()
	sender := jobrunner.NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})
	defer func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := sender.AcceptInput(ctx, jobrunner.RuntimePodTarget{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_old", RuntimeProcessID: "process_pod_old", PodIP: "127.0.0.1", Port: child.port}, &agentruntimev1.AcceptInputRequest{WorkspaceId: "default", SessionId: session, SessionThreadId: thread, BindingId: binding, BindingGeneration: 1, TargetPodUid: "pod_old", RuntimeProcessId: "process_pod_old", RuntimeInputId: "rin_unrelated_after_fence", InputOrder: 999, Content: &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: `{"messages":[]}`}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unrelated input after admission closure=%v", err)
	}
}
func (p *handoffRuntimeChild) signal(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(p.directory, name), []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
}
func (p *handoffRuntimeChild) calls(session string) int {
	raw, err := os.ReadFile(filepath.Join(p.directory, "ledger.json"))
	if err != nil {
		return 0
	}
	var entries []struct {
		SessionID string `json:"sessionId"`
	}
	if json.Unmarshal(raw, &entries) != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.SessionID == session {
			count++
		}
	}
	return count
}
func (p *handoffRuntimeChild) join(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		if p.err != nil {
			out, _ := os.ReadFile(filepath.Join(p.directory, "output.log"))
			t.Fatalf("Runtime exit %v\n%s", p.err, out)
		}
	case <-time.After(6 * time.Second):
		diagnostics, _ := os.ReadFile(filepath.Join(p.directory, "diagnostics.jsonl"))
		t.Fatalf("Runtime exceeded configured phase deadline: %s", diagnostics)
	}
}
func (p *handoffRuntimeChild) kill(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		if p.err == nil {
			t.Fatal("selected killed child falsely reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selected killed child did not join")
	}
	if _, err := os.Stat(filepath.Join(p.directory, "closed.json")); err == nil {
		t.Fatal("selected death fabricated cooperative closeout")
	}
}
func appendHandoffMessage(t *testing.T, client *dbconnect.Client, sessionID, key string) {
	t.Helper()
	service := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "continue " + key}}}}})
	if err != nil || len(result.Data) != 1 {
		t.Fatalf("durable producer message %v/%v", result, err)
	}
}
func seedHandoffRuntimeBinding(t *testing.T, admin *sql.DB, session, binding string, generation int64, pod string) {
	t.Helper()
	seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, generation, pod)
	// Fixture INSERTs must obey the same monotonic generation prerequisite as
	// the production binding creator's nextval allocation.
	if _, err := admin.Exec(`SELECT setval('session_runtime_binding_generation_seq',GREATEST((SELECT last_value FROM session_runtime_binding_generation_seq),$1),true)`, generation); err != nil {
		t.Fatal(err)
	}
}
func handoffBindingCount(t *testing.T, db *sql.DB, session string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func deliverHandoffRecovery(t *testing.T, runtimeDB *sql.DB, child *handoffRuntimeChild, podUID string, batch ...int) {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	maxJobs := 1
	if len(batch) > 0 {
		maxJobs = batch[0]
	}
	leases, err := queueStore.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "handoff-runner", MaxJobs: maxJobs, LeaseDuration: time.Minute})
	if err != nil || len(leases) < 1 {
		t.Fatalf("handoff recovery lease %v/%v", leases, err)
	}
	targetURL, err := url.Parse(child.httpURL)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, targetURL.Host)
	}}
	defer transport.CloseIdleConnections()
	delivery := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, child.port, jobrunner.KubernetesRuntimeTargetResolver{GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
		uid, ip, ready := podUID, "127.0.0.3", true
		if name == "runtime-pod-0" {
			uid, ip, ready = "pod_old", "127.0.0.1", false
		}
		return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: uid, IP: ip, Running: true, Ready: ready}, nil
	}, LoadClient: &http.Client{Transport: transport}, Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-new", PodUID: podUID, PodIP: "127.0.0.3"}})
	}})
	// Discovered IPs stay identical to the placement fixture's stored binding;
	// only the socket maps to the selected actual child's ephemeral listener.
	sender := jobrunner.NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(child.port)))
	}))
	defer func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	}()
	observed := &handoffObservedDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: sender}}
	runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: observed}
	for _, lease := range leases {
		if err := runIssuedLeaseThroughRunner(context.Background(), runner, queueJobProto(lease), jobrunner.JobRunnerConfig{LeaseOwner: "handoff-runner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}); err != nil {
			t.Fatal(err)
		}
		t.Logf("actual handoff wake delivery job=%s status=%s retryable=%t error_kind=%s", observed.jobID, observed.result.Status, observed.result.Retryable, observed.result.ErrorKind)
	}
}

// Both live Pods are sampled through their real HTTP listeners. The test maps
// their distinct discovered loopback IPs to ephemeral child listeners only at
// socket dial; actual placement, binding, Queue custody and RPC admission remain
// production owners.
func deliverHandoffInputWithPlacement(t *testing.T, runtimeDB *sql.DB, old, next *handoffRuntimeChild, sessionID string) {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtimeDB)
	oldURL, err := url.Parse(old.httpURL)
	if err != nil {
		t.Fatal(err)
	}
	nextURL, err := url.Parse(next.httpURL)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		target := nextURL.Host
		if host == "127.0.0.2" {
			target = oldURL.Host
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}}
	defer transport.CloseIdleConnections()
	candidates := []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_old", PodIP: "127.0.0.2"}, {Namespace: "tetral-agent-runtime", PodName: "runtime-pod-new", PodUID: "pod_new", PodIP: "127.0.0.3"}}
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, 19090, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: &http.Client{Transport: transport}, Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, candidates)
	}, GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
		candidate := candidates[1]
		if name == "runtime-pod-0" {
			candidate = candidates[0]
		}
		return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: candidate.PodUID, IP: candidate.PodIP, Running: true, Ready: true}, nil
	}})
	sender := jobrunner.NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		port := next.port
		if host == "127.0.0.2" {
			port = old.port
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
	}))
	defer func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	}()
	deliverer := &handoffObservedDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}}
	queueStore := queue.NewPostgreSQLStore(client)
	runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}
	// Quiesce also queues B's recovery. Select the actual input owner rather
	// than mistaking activity on that earlier recovery for fresh D admission.
	leases, err := queueStore.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "handoff-placement", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(leases) != 1 {
		t.Fatalf("fresh input lease=%+v err=%v", leases, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leases[0]))
	if err != nil || job.Kind != queue.KindRuntimeInput || job.SessionID != sessionID || job.RuntimeInputID == "" {
		t.Fatalf("fresh D actual lease=%+v err=%v", job, err)
	}
	if err := runIssuedLeaseThroughRunner(context.Background(), runner, queueJobProto(leases[0]), jobrunner.JobRunnerConfig{LeaseOwner: "handoff-placement", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}
	t.Logf("post-fence input Queue lease job=%s kind=%s session=%s input=%s delivery=%s", deliverer.jobID, deliverer.kind, deliverer.sessionID, deliverer.inputID, deliverer.result.Status)
	if deliverer.jobID != job.JobID || deliverer.sessionID != sessionID || deliverer.kind != queue.KindRuntimeInput || deliverer.inputID != job.RuntimeInputID {
		t.Fatalf("fresh D delivery did not consume its actual Queue lease: %+v", deliverer)
	}
	if deliverer.result.Status != jobrunner.RuntimeDeliveryAccepted && deliverer.result.Status != jobrunner.RuntimeDeliveryDuplicate {
		t.Fatalf("post-closure real delivery job=%s session=%s outcome=%+v", deliverer.jobID, deliverer.sessionID, deliverer.result)
	}
}

// Input preparation creates residency; recovery creates it in activation.
// Both real owners wait before those calls while the session has no binding.
type handoffCompetingStore struct {
	*jobrunner.PostgreSQLRuntimeDeliveryStore
	entered  chan<- jobrunner.RuntimeJob
	gate     <-chan struct{}
	recovery bool
}

func (s *handoffCompetingStore) wait(ctx context.Context, job jobrunner.RuntimeJob) error {
	select {
	case s.entered <- job:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *handoffCompetingStore) PrepareRuntimeCommand(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeCommandPlan, error) {
	if !s.recovery {
		if err := s.wait(ctx, job); err != nil {
			return jobrunner.RuntimeCommandPlan{}, err
		}
	}
	return s.PostgreSQLRuntimeDeliveryStore.PrepareRuntimeCommand(ctx, job)
}
func (s *handoffCompetingStore) ActivateRuntimeRecovery(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeCommandPlan, error) {
	if err := s.wait(ctx, job); err != nil {
		return jobrunner.RuntimeCommandPlan{}, err
	}
	return s.PostgreSQLRuntimeDeliveryStore.ActivateRuntimeRecovery(ctx, job)
}
func raceHandoffBinders(t *testing.T, runtimeDB, admin *sql.DB, child *handoffRuntimeChild, session, mainThread, childThread string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	entered, gate := make(chan jobrunner.RuntimeJob, 2), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	targetURL, err := url.Parse(child.httpURL)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, targetURL.Host)
	}}
	defer transport.CloseIdleConnections()
	sender := jobrunner.NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})
	defer func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	}()
	results := make(chan error, 2)
	var owners sync.WaitGroup
	var jobs [2]jobrunner.RuntimeJob
	defer func() { cancel(); release(); owners.Wait() }()
	for index, kind := range []string{queue.KindRuntimeInput, queue.KindRuntimeRecovery} {
		leases, err := queueStore.Lease(ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{kind}, LeaseOwner: fmt.Sprintf("competing-%d", index), MaxJobs: 1, LeaseDuration: time.Minute})
		if err != nil || len(leases) != 1 {
			t.Fatalf("competing actual lease kind=%s leases=%+v err=%v", kind, leases, err)
		}
		job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leases[0]))
		if err != nil {
			t.Fatal(err)
		}
		expectedThread := mainThread
		if index == 1 {
			expectedThread = childThread
		}
		if job.SessionID != session || job.SessionThreadID != expectedThread || job.Kind != kind {
			t.Fatalf("competing custody=%+v expected thread=%s", job, expectedThread)
		}
		jobs[index] = job
		store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, child.port, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: &http.Client{Transport: transport}, Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-new", PodUID: "pod_new", PodIP: "127.0.0.1"}})
		}, GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
			return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: "pod_new", IP: "127.0.0.1", Running: true, Ready: true}, nil
		}})
		competing := &handoffCompetingStore{PostgreSQLRuntimeDeliveryStore: store, entered: entered, gate: gate, recovery: index == 1}
		deliverer := &handoffObservedDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: competing, Sender: sender}}
		runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}
		lease := queueJobProto(leases[0])
		owner := fmt.Sprintf("competing-%d", index)
		owners.Add(1)
		go func() {
			defer owners.Done()
			err := runIssuedLeaseThroughRunner(ctx, runner, lease, jobrunner.JobRunnerConfig{LeaseOwner: owner, MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour})
			if err == nil && deliverer.result.Status != jobrunner.RuntimeDeliveryAccepted && deliverer.result.Status != jobrunner.RuntimeDeliveryDuplicate {
				err = fmt.Errorf("competing %s delivery outcome=%+v", deliverer.kind, deliverer.result)
			}
			results <- err
		}()
	}
	for range 2 {
		select {
		case job := <-entered:
			t.Logf("actual no-binding competitor job=%s kind=%s thread=%s", job.JobID, job.Kind, job.SessionThreadID)
		case <-ctx.Done():
			t.Fatal("competing preparation barrier: ", ctx.Err())
		}
	}
	if count := handoffBindingCount(t, admin, session); count != 0 {
		t.Fatalf("binding created before competing owners released: %d", count)
	}
	release()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	owners.Wait()
	var bindings int
	var pod, process string
	var generation int64
	if err := admin.QueryRow(`SELECT count(*),min(agent_runtime_pod_uid),min(runtime_process_id),min(binding_generation) FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&bindings, &pod, &process, &generation); err != nil || bindings != 1 || pod != "pod_new" || process != "process_pod_new" || generation <= 1 {
		t.Fatalf("competing binding count=%d pod=%s process=%s generation=%d err=%v", bindings, pod, process, generation, err)
	}
	for _, job := range jobs {
		var state string
		if err := admin.QueryRow(`SELECT status FROM queue_jobs WHERE id=$1`, job.JobID).Scan(&state); err != nil || state != "acknowledged" {
			t.Fatalf("competing original job=%s kind=%s state=%s err=%v", job.JobID, job.Kind, state, err)
		}
	}
	var inputState string
	if err := admin.QueryRow(`SELECT status FROM session_runtime_inbox WHERE session_id=$1 AND session_thread_id=$2 AND runtime_input_id=$3`, session, mainThread, jobs[0].RuntimeInputID).Scan(&inputState); err != nil || inputState != "committed" {
		t.Fatalf("competing original main input=%s state=%s err=%v", jobs[0].RuntimeInputID, inputState, err)
	}

}

type handoffInputCustody struct{ InputID, JobID, EventsJSON, InboxStatus, QueueStatus string }

func readHandoffPendingInput(t *testing.T, admin *sql.DB, session string) handoffInputCustody {
	t.Helper()
	var input handoffInputCustody
	if err := admin.QueryRow(`SELECT i.runtime_input_id,j.id,i.event_ids_json,i.status,j.status FROM session_runtime_inbox i JOIN queue_jobs j ON j.workspace_id=i.workspace_id AND j.dedupe_key='runtime_input:'||i.workspace_id||':'||i.session_id||':'||i.runtime_input_id WHERE i.session_id=$1 ORDER BY i.created_at DESC,i.runtime_input_id DESC LIMIT 1`, session).Scan(&input.InputID, &input.JobID, &input.EventsJSON, &input.InboxStatus, &input.QueueStatus); err != nil {
		t.Fatal(err)
	}
	return input
}

func assertHandoffExactToolContext(t *testing.T, admin *sql.DB, session, modelRequestID, contextJSON string, names ...string) {
	t.Helper()
	expectedName := "list_agents"
	if len(names) > 0 {
		expectedName = names[0]
	}
	var durableOutput, durableInput string
	if err := admin.QueryRow(`SELECT (SELECT part->'result'->'output' FROM jsonb_array_elements(data_json::jsonb->'parts') part WHERE part->>'type'='tool_result' AND part->>'modelToolCallId'='tool-current')::text,(SELECT part->'canonicalInput' FROM jsonb_array_elements(data_json::jsonb->'parts') part WHERE part->>'type'='tool_call' AND part->>'modelToolCallId'='tool-current')::text FROM session_messages WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'`, session, modelRequestID).Scan(&durableOutput, &durableInput); err != nil {
		t.Fatal(err)
	}
	var contextEntries []struct {
		Content []struct {
			ToolCall *struct {
				ModelToolCallID string `json:"modelToolCallId"`
				Name            string `json:"name"`
				InputJSON       string `json:"inputJson"`
			} `json:"toolCall"`
			ToolResult *struct {
				ModelToolCallID string `json:"modelToolCallId"`
				Completed       *struct {
					OutputJSON string `json:"outputJson"`
				} `json:"completed"`
			} `json:"toolResult"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(contextJSON), &contextEntries); err != nil {
		t.Fatal(err)
	}
	calls, results := 0, 0
	for _, entry := range contextEntries {
		for _, item := range entry.Content {
			if item.ToolCall != nil && item.ToolCall.ModelToolCallID == "tool-current" {
				calls++
				if item.ToolCall.Name != expectedName || !handoffEqualJSON(item.ToolCall.InputJSON, durableInput) {
					t.Fatalf("original Tool Use context=%+v durable input=%s", item.ToolCall, durableInput)
				}
			}
			if item.ToolResult != nil && item.ToolResult.ModelToolCallID == "tool-current" {
				results++
				if item.ToolResult.Completed == nil || !handoffEqualJSON(item.ToolResult.Completed.OutputJSON, durableOutput) {
					t.Fatalf("original Tool Result context=%+v durable output=%s", item.ToolResult, durableOutput)
				}
			}
		}
	}
	if calls != 1 || results != 1 {
		t.Fatalf("original paired Tool context calls=%d results=%d", calls, results)
	}
}
func handoffEqualJSON(left, right string) bool {
	var a, b any
	if json.Unmarshal([]byte(left), &a) != nil || json.Unmarshal([]byte(right), &b) != nil {
		return false
	}
	av, _ := json.Marshal(a)
	bv, _ := json.Marshal(b)
	return string(av) == string(bv)
}

func assertHandoffOldOwnerFence(t *testing.T, admin *sql.DB, endpoint replicaBridge, session, thread, binding string, generation int64) {
	t.Helper()
	var nextBinding, nextProcess string
	var nextGeneration int64
	if err := admin.QueryRow(`SELECT binding_id,binding_generation,runtime_process_id FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&nextBinding, &nextGeneration, &nextProcess); err != nil {
		t.Fatal(err)
	}
	if nextBinding == binding || nextGeneration <= generation || nextProcess != "process_pod_new" {
		t.Fatalf("replacement binding %s/%d/%s", nextBinding, nextGeneration, nextProcess)
	}
	var operationID, handoffID, queueID string
	if err := admin.QueryRow(`SELECT h.operation_id,h.handoff_id,t.queue_job_id FROM session_runtime_handoffs h JOIN session_runtime_handoff_threads t USING(workspace_id,session_id,handoff_id) WHERE h.session_id=$1 AND h.binding_id=$2 AND h.binding_generation=$3 AND t.session_thread_id=$4`, session, binding, generation, thread).Scan(&operationID, &handoffID, &queueID); err != nil {
		t.Fatal(err)
	}
	request := &bridgev1.ReleaseRuntimeBindingRequest{WorkspaceId: "default", SessionId: session, BindingId: binding, BindingGeneration: generation, RuntimeProcessId: "process_pod_old", OperationId: operationID}
	replayed, err := endpoint.Client.ReleaseRuntimeBinding(replicaRuntimeContext(context.Background(), "old"), request)
	if err != nil || replayed.HandoffId != handoffID || len(replayed.Threads) != 1 || replayed.Threads[0].QueueJobId != queueID {
		t.Fatalf("retired immutable receipt replay=%v err=%v", replayed, err)
	}
	second, err := endpoint.Client.ReleaseRuntimeBinding(replicaRuntimeContext(context.Background(), "old"), request)
	if err != nil || !proto.Equal(replayed, second) {
		t.Fatalf("repeat retired receipt changed=%v err=%v", second, err)
	}
	assertHandoffLateWriteFence(t, admin, endpoint, session, thread, binding, generation)
}

func assertHandoffLateWriteFence(t *testing.T, admin *sql.DB, endpoint replicaBridge, session, thread, binding string, generation int64) {
	t.Helper()
	var nextBinding, nextProcess string
	var nextGeneration int64
	if err := admin.QueryRow(`SELECT binding_id,binding_generation,runtime_process_id FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&nextBinding, &nextGeneration, &nextProcess); err != nil {
		t.Fatal(err)
	}
	if nextBinding == binding || nextGeneration <= generation || nextProcess != "process_pod_new" {
		t.Fatalf("replacement before old write binding=%s/%d/%s", nextBinding, nextGeneration, nextProcess)
	}
	var beforeEvents, beforeWakes int
	if err := admin.QueryRow(`SELECT count(*),(SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND disposition='recover') FROM session_events WHERE session_id=$1`, session).Scan(&beforeEvents, &beforeWakes); err != nil {
		t.Fatal(err)
	}
	response, err := endpoint.Client.WriteEvent(replicaRuntimeContext(context.Background(), "old"), &bridgev1.WriteEventRequest{Scope: sessionfixture.BridgeAPIScope(session, thread, binding, generation, "pod_old"), RuntimeWriteId: "rwrite_late_old_generation", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`})
	if err != nil || response.GetStale() == nil {
		t.Fatalf("late old write=%v err=%v", response, err)
	}
	var afterEvents, wakes int
	var bindingAfter, processAfter string
	var generationAfter int64
	if err := admin.QueryRow(`SELECT count(*),(SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND disposition='recover') FROM session_events WHERE session_id=$1`, session).Scan(&afterEvents, &wakes); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT binding_id,binding_generation,runtime_process_id FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&bindingAfter, &generationAfter, &processAfter); err != nil {
		t.Fatal(err)
	}
	if afterEvents != beforeEvents || wakes != beforeWakes || bindingAfter != nextBinding || generationAfter != nextGeneration || processAfter != nextProcess {
		t.Fatalf("old owner crossed replacement fence events=%d/%d wakes=%d binding=%s/%d/%s", beforeEvents, afterEvents, wakes, bindingAfter, generationAfter, processAfter)
	}
}

func TestPostgreSQLReplicaRuntimeHandoff(t *testing.T) {
	runPrimary := func(t *testing.T, endFirst bool) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		client := dbconnect.NewClientForTesting(runtimeDB)
		store := bridge.NewPostgreSQLBridgeAPIStore(client)
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		startHandoffResultListener(t, store)
		sessions := []string{"sesn_replica_a1", "sesn_replica_a2", "sesn_replica_c", "sesn_replica_b"}
		for _, session := range sessions {
			sessionfixture.SeedBridgeAPISession(t, admin, "default", session, "thr_"+session)
			seedHandoffRuntimeBinding(t, admin, session, "bind_"+session, 1, "pod_old")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, "bind_"+session, 1)
		}
		if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'`); err != nil {
			t.Fatal(err)
		}
		var lost, preCommitLost, declarationLost, acceptanceLost atomic.Bool
		var faultMu sync.Mutex
		var releaseOperations, acceptTools []string
		orderedStore := newHandoffOrderedBridgeStore(store, admin, sessions[0], endFirst)
		defer orderedStore.close(t)
		faultStore := &handoffFaultBridgeStore{BridgeAPIStore: orderedStore}
		faultStore.beforeRelease = func(r *bridgev1.ReleaseRuntimeBindingRequest) error {
			if r.SessionId != sessions[2] {
				return nil
			}
			faultMu.Lock()
			releaseOperations = append(releaseOperations, r.OperationId)
			faultMu.Unlock()
			if preCommitLost.CompareAndSwap(false, true) {
				var receipts, bindings int
				if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_handoffs WHERE session_id=$1),(SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1)`, r.SessionId).Scan(&receipts, &bindings); err != nil || receipts != 0 || bindings != 1 {
					return status.Errorf(codes.Internal, "before-commit oracle receipts=%d bindings=%d err=%v", receipts, bindings, err)
				}
				return status.Error(codes.Unavailable, "release response unavailable before commit")
			}
			return nil
		}
		faultStore.afterWrite = func(r *bridgev1.WriteEventRequest, response *bridgev1.WriteEventResponse) error {
			if r.GetScope().GetSessionId() == sessions[3] && r.ToolDeclaration != nil && response.GetCommitted() != nil && declarationLost.CompareAndSwap(false, true) {
				var events int
				if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id=$2 AND type='agent.tool_use' AND model_request_id=$3`, sessions[3], response.GetCommitted().GetEventId(), r.ModelRequestId).Scan(&events); err != nil || events != 1 {
					return status.Errorf(codes.Internal, "committed declaration oracle=%d err=%v", events, err)
				}
				return status.Error(codes.Unavailable, "committed declaration response lost")
			}
			return nil
		}
		faultStore.afterAccept = func(r *bridgev1.AcceptSandboxExecutionRequest, response *bridgev1.AcceptSandboxExecutionResponse) error {
			if r.GetScope().GetSessionId() != sessions[3] {
				return nil
			}
			faultMu.Lock()
			acceptTools = append(acceptTools, r.ToolUseEventId)
			faultMu.Unlock()
			if response.GetCommitted() != nil && acceptanceLost.CompareAndSwap(false, true) {
				var accepted int
				if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2`, sessions[3], r.ToolUseEventId).Scan(&accepted); err != nil || accepted != 1 {
					return status.Errorf(codes.Internal, "committed execution oracle=%d err=%v", accepted, err)
				}
				return status.Error(codes.Unavailable, "committed acceptance response lost")
			}
			return nil
		}
		a := serveReplicaBridge(t, faultStore, map[string]string{"old": "pod_old", "new": "pod_new", "third": "pod_third"}, func(_ context.Context, method string, value any) error {
			if method == bridgev1.AgentRuntimeBridgeService_ReleaseRuntimeBinding_FullMethodName && value.(*bridgev1.ReleaseRuntimeBindingResponse).ReleasedBinding.BindingId == "bind_"+sessions[2] && lost.CompareAndSwap(false, true) {
				response := value.(*bridgev1.ReleaseRuntimeBindingResponse)
				var receipts, bindings int
				if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_handoffs WHERE handoff_id=$1 AND operation_id=$2),(SELECT count(*) FROM session_runtime_bindings WHERE session_id=$3)`, response.HandoffId, response.OperationId, sessions[2]).Scan(&receipts, &bindings); err != nil || receipts != 1 || bindings != 0 {
					return status.Errorf(codes.Internal, "after-commit oracle receipt=%d bindings=%d err=%v", receipts, bindings, err)
				}
				return status.Error(codes.Unavailable, "committed release response lost")
			}
			return nil
		})
		old := startHandoffRuntimeChild(t, a.Address, "pod_old", "process_pod_old", "old", false, []map[string]any{{"sessionId": sessions[2], "sessionThreadId": "thr_" + sessions[2], "bindingId": "bind_" + sessions[2], "bindingGeneration": 1}})
		replacement := startHandoffRuntimeChild(t, a.Address, "pod_new", "process_pod_new", "new", true, nil)
		for _, session := range sessions[:2] {
			appendHandoffMessage(t, client, session, "initial_"+session)
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
			waitHandoffCondition(t, "held current frame "+session, func() bool {
				_, err := os.Stat(filepath.Join(old.directory, session+"-1.frame"))
				return old.calls(session) == 1 && err == nil
			})
		}
		orderedStore.modelRequest.Store(old.providerEntries(t, sessions[0])[0].ModelRequestID)
		var originalTurnID string
		if err := admin.QueryRow(`SELECT event_id FROM session_events WHERE session_id=$1 AND type='session.status_running' ORDER BY sequence DESC LIMIT 1`, sessions[0]).Scan(&originalTurnID); err != nil {
			t.Fatal(err)
		}
		sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", sessions[3])
		appendHandoffMessage(t, client, sessions[3], "initial_b")
		deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, sessions[3], "runtime-pod-0", "pod_old")
		waitHandoffCondition(t, "B accepted Sandbox execution", func() bool {
			var n int
			_ = admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1`, sessions[3]).Scan(&n)
			return n == 1
		})
		external := startHandoffSandboxOwner(t, runtimeDB, sessions[3])
		waitHandoffCondition(t, "external command reference persisted", func() bool {
			select {
			case err := <-external.done:
				t.Fatalf("external runner ended before observation: %v, calls=%d", err, external.calls.Load())
			default:
			}
			return external.observations.Load() > 0
		})
		old.signal(t, "quiesce")
		waitHandoffCondition(t, "old admission closed with both provider barriers held", func() bool {
			raw, err := os.ReadFile(filepath.Join(old.directory, "quiescing.json"))
			return err == nil && strings.Contains(string(raw), `"accepting":false`)
		})
		const fresh = "sesn_replica_d"
		sessionfixture.SeedBridgeAPISession(t, admin, "default", fresh, "thr_"+fresh)
		if _, err := admin.Exec(`INSERT INTO session_runtime_status(workspace_id,session_id,status,created_at,updated_at) VALUES('default',$1,'idle',clock_timestamp(),clock_timestamp())`, fresh); err != nil {
			t.Fatal(err)
		}
		appendHandoffMessage(t, client, fresh, "fresh_after_admission_closed")
		appendHandoffMessage(t, client, sessions[0], "queued_during_quiesce")
		newA1Input := readHandoffPendingInput(t, admin, sessions[0])
		if newA1Input.InboxStatus != "queued" || newA1Input.QueueStatus != queue.StatusPending {
			t.Fatalf("post-fence original A1 custody=%+v", newA1Input)
		}
		deliverHandoffInputWithPlacement(t, runtimeDB, old, replacement, fresh)
		waitHandoffCondition(t, "fresh D uses accepting replacement while both old providers held", func() bool { return replacement.calls(fresh) == 1 })
		var freshProcess, freshPod string
		var pendingA1 int
		if err := admin.QueryRow(`SELECT runtime_process_id,agent_runtime_pod_uid FROM session_runtime_bindings WHERE session_id=$1`, fresh).Scan(&freshProcess, &freshPod); err != nil || freshProcess != "process_pod_new" || freshPod != "pod_new" {
			t.Fatalf("fresh Session placement process=%s Pod=%s err=%v", freshProcess, freshPod, err)
		}
		if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND status='queued'`, sessions[0]).Scan(&pendingA1); err != nil || pendingA1 != 1 || old.calls(sessions[0]) != 1 || old.calls(sessions[1]) != 1 {
			t.Fatalf("post-closure A1 custody=%d old requests=%d/%d err=%v", pendingA1, old.calls(sessions[0]), old.calls(sessions[1]), err)
		}
		replacement.signal(t, fresh+"-1.release")
		waitHandoffCondition(t, "idle Session independent release", func() bool { return handoffBindingCount(t, admin, sessions[2]) == 0 })
		if old.calls(sessions[0]) != 1 || old.calls(sessions[1]) != 1 || replacement.calls(sessions[2]) != 0 {
			t.Fatal("idle release crossed an admitted provider step")
		}
		if handoffBindingCount(t, admin, sessions[2]) != 0 {
			t.Fatal("idle C eagerly rebound")
		}
		waitHandoffCondition(t, "B releases while A1 and A2 held", func() bool { return handoffBindingCount(t, admin, sessions[3]) == 0 })
		deliverHandoffRecovery(t, runtimeDB, replacement, "pod_new")
		if replacement.calls(sessions[3]) != 0 || external.calls.Load() != 1 {
			t.Fatal("B resubmitted execution or dispatched a model before result")
		}
		old.signal(t, sessions[0]+"-1.release")
		waitHandoffCondition(t, "A1 current step release", func() bool { return handoffBindingCount(t, admin, sessions[0]) == 0 })
		// The earlier business input is the next legitimate Queue lease. Its
		// admission cold-restores the original open turn; no new user retry is
		// submitted to make that continuation possible.
		deliverHandoffInputWithPlacement(t, runtimeDB, old, replacement, sessions[0])
		waitHandoffCondition(t, "replacement successor provider request", func() bool { return replacement.calls(sessions[0]) == 1 })
		afterA1Input := readHandoffPendingInput(t, admin, sessions[0])
		t.Logf("A1 original post-fence input custody before=%+v after=%+v", newA1Input, afterA1Input)
		if afterA1Input.InputID != newA1Input.InputID || afterA1Input.JobID != newA1Input.JobID || afterA1Input.EventsJSON != newA1Input.EventsJSON || afterA1Input.InboxStatus != "committed" || afterA1Input.QueueStatus != queue.StatusAcknowledged {
			t.Fatalf("A1 original custody changed before=%+v after=%+v", newA1Input, afterA1Input)
		}
		originalA1 := old.providerEntries(t, sessions[0])[0]
		nextA1 := replacement.providerEntries(t, sessions[0])[0]
		var originalTool, resultPayload, turnID string
		var originalEnd, resultSequence int64
		if err := admin.QueryRow(`SELECT tool.event_id,ended.sequence,result.sequence,result.payload_json,(SELECT event_id FROM session_events WHERE session_id=$1 AND type='session.status_running' ORDER BY sequence DESC LIMIT 1) FROM session_events tool JOIN session_events ended ON ended.session_id=tool.session_id AND ended.model_request_id=tool.model_request_id AND ended.type='span.model_request_end' JOIN session_events result ON result.session_id=tool.session_id AND result.type='agent.tool_result' AND result.payload_json::jsonb->>'tool_use_id'=tool.event_id WHERE tool.session_id=$1 AND tool.model_request_id=$2 AND tool.type='agent.tool_use'`, sessions[0], originalA1.ModelRequestID).Scan(&originalTool, &originalEnd, &resultSequence, &resultPayload, &turnID); err != nil {
			t.Fatal(err)
		}
		if (endFirst && originalEnd >= resultSequence) || (!endFirst && resultSequence >= originalEnd) || turnID != originalTurnID || originalA1.RequestID == nextA1.RequestID || originalA1.ModelRequestID == nextA1.ModelRequestID || !strings.Contains(nextA1.MessagesJSON, "tool-current") || !strings.Contains(nextA1.MessagesJSON, "list_agents") || !strings.Contains(nextA1.MessagesJSON, "initial_"+sessions[0]) || !strings.Contains(nextA1.MessagesJSON, "queued_during_quiesce") {
			t.Fatalf("original continuation identity/order/context original=%+v next=%+v tool=%s End=%d result=%d turn=%s/%s resultPayload=%s", originalA1, nextA1, originalTool, originalEnd, resultSequence, turnID, originalTurnID, resultPayload)
		}
		if !orderedStore.endEntered.Load() || !orderedStore.resultEntered.Load() {
			t.Fatalf("original Bridge ordering boundaries not both entered End=%t result=%t", orderedStore.endEntered.Load(), orderedStore.resultEntered.Load())
		}
		t.Logf("original Bridge ACK ordering end_first=%t request=%s tool=%s End=%d result=%d", endFirst, originalA1.ModelRequestID, originalTool, originalEnd, resultSequence)
		assertHandoffExactToolContext(t, admin, sessions[0], originalA1.ModelRequestID, nextA1.MessagesJSON)
		assertHandoffOldOwnerFence(t, admin, a, sessions[0], "thr_"+sessions[0], "bind_"+sessions[0], 1)
		assertHandoffUnrelatedInputRejected(t, old, sessions[0], "thr_"+sessions[0], "bind_"+sessions[0])
		if old.calls(sessions[0]) != 1 || old.calls(sessions[1]) != 1 {
			t.Fatal("successor dispatched on retiring Runtime or A2 was interrupted")
		}
		old.signal(t, sessions[1]+"-1.release")
		old.join(t)
		if replacement.calls(sessions[0]) != 1 {
			t.Fatal("old process exit failed while replacement work remained held")
		}
		select {
		case <-external.done:
			t.Fatal("old exit waited for or completed independent B command")
		default:
		}
		var errors, ends, results, inbox int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id IN ('sesn_replica_a1','sesn_replica_a2') AND type IN ('session.error','session.runtime_pod_lost')),(SELECT count(*) FROM session_events WHERE session_id IN ('sesn_replica_a1','sesn_replica_a2') AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id='sesn_replica_a1' AND type='agent.tool_result'),(SELECT count(*) FROM session_runtime_inbox WHERE session_id='sesn_replica_a1' AND status='queued')`).Scan(&errors, &ends, &results, &inbox); err != nil {
			t.Fatal(err)
		}
		if errors != 0 || ends != 2 || results != 1 {
			t.Fatalf("checkpoint facts errors=%d ends=%d toolresults=%d queued=%d", errors, ends, results, inbox)
		}
		if !lost.Load() || !preCommitLost.Load() || !declarationLost.Load() || !acceptanceLost.Load() {
			t.Fatal("release response-loss barrier was not exercised")
		}
		faultMu.Lock()
		releaseOpsCopy := append([]string(nil), releaseOperations...)
		acceptToolsCopy := append([]string(nil), acceptTools...)
		faultMu.Unlock()
		if len(releaseOpsCopy) < 3 || len(acceptToolsCopy) < 2 {
			t.Fatalf("fault retry identities release=%v acceptance=%v", releaseOpsCopy, acceptToolsCopy)
		}
		for _, op := range releaseOpsCopy {
			if op != releaseOpsCopy[0] {
				t.Fatalf("release retry changed operation %v", releaseOpsCopy)
			}
		}
		for _, tool := range acceptToolsCopy {
			if tool != acceptToolsCopy[0] {
				t.Fatalf("execution retry changed tool %v", acceptToolsCopy)
			}
		}
		replacement.signal(t, "quiesce")
		waitHandoffCondition(t, "replacement quiesce before response", func() bool {
			_, err := os.Stat(filepath.Join(replacement.directory, "quiescing.json"))
			return err == nil
		})
		replacement.signal(t, sessions[0]+"-1.release")
		replacement.signal(t, fresh+"-1.release")
		replacement.join(t)
		if external.calls.Load() != 1 {
			t.Fatal("Sandbox execution duplicated across two handoff receipts")
		}
		third := startHandoffRuntimeChild(t, a.Address, "pod_third", "process_pod_third", "third", true, nil)
		deliverHandoffRecovery(t, runtimeDB, third, "pod_third", 10)
		if third.calls(sessions[3]) != 0 {
			t.Fatal("third process resubmitted model before original Sandbox result")
		}
		var handoffs int
		if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_handoffs WHERE session_id=$1`, sessions[3]).Scan(&handoffs); err != nil || handoffs != 2 {
			t.Fatalf("B successive handoff receipts=%d/%v", handoffs, err)
		}
		var wakes int
		if err := admin.QueryRow(`SELECT count(DISTINCT queue_job_id) FROM session_runtime_handoff_threads WHERE session_id=$1 AND disposition='recover'`, sessions[3]).Scan(&wakes); err != nil || wakes != 2 {
			t.Fatalf("two upgrades original execution distinct wakes=%d err=%v", wakes, err)
		}
		external.finish()
		select {
		case err := <-external.done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Sandbox command owner did not join")
		}
		waitHandoffCondition(t, "original Sandbox result resumes third process", func() bool { return third.calls(sessions[3]) == 1 })
		if external.calls.Load() != 1 {
			t.Fatal("two upgrades replayed original external command")
		}
		third.signal(t, "quiesce")
		waitHandoffCondition(t, "third process quiesce fence", func() bool { _, err := os.Stat(filepath.Join(third.directory, "quiescing.json")); return err == nil })
		for _, session := range sessions {
			if third.calls(session) > 0 {
				third.signal(t, session+"-1.release")
			}
		}
		third.join(t)
		oldRecords := readHandoffDiagnostics(t, old)
		recovered := readHandoffDiagnostics(t, replacement)
		seenIdle, seenRecover, seenNewProcess, seenRecovery := false, false, false, false
		for _, record := range oldRecords {
			if record["event"] == "runtime_binding_handoff_committed" {
				if record["runtime.process.id"] != "process_pod_old" || record["binding.id"] == "" || record["handoff.id"] == "" || record["operation.id"] == "" {
					t.Fatalf("incomplete handoff diagnostic %+v", record)
				}
				seenIdle = seenIdle || record["handoff.disposition"] == "idle"
				seenRecover = seenRecover || record["handoff.disposition"] == "recover"
			}
		}
		for _, record := range recovered {
			seenNewProcess = seenNewProcess || (record["event"] == "runtime_process_accepting" && record["runtime.process.id"] == "process_pod_new")
			seenRecovery = seenRecovery || (record["event"] == "runtime_command_accepted" && record["operation"] == "RecoverThread" && record["runtime.process.id"] == "process_pod_new" && record["binding.id"] != "")
		}
		if !seenIdle || !seenRecover || !seenNewProcess || !seenRecovery {
			t.Fatalf("actual diagnostic correlation idle=%t recover=%t new=%t recovery=%t", seenIdle, seenRecover, seenNewProcess, seenRecovery)
		}
	}
	t.Run("independent current step and successor process", func(t *testing.T) { runPrimary(t, true) })
	t.Run("independent current step and successor process result before End", func(t *testing.T) { runPrimary(t, false) })

	t.Run("queued business input before checkpoint retains original continuation", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		client := dbconnect.NewClientForTesting(runtimeDB)
		const session = "sesn_queued_a1"
		const thread = "thr_queued_a1"
		sessionfixture.SeedBridgeAPISession(t, admin, "default", session, thread)
		seedHandoffRuntimeBinding(t, admin, session, "bind_queued", 1, "pod_old")
		sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, "bind_queued", 1)
		if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'`); err != nil {
			t.Fatal(err)
		}
		store := bridge.NewPostgreSQLBridgeAPIStore(client)
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		startHandoffResultListener(t, store)
		a := serveReplicaBridge(t, store, map[string]string{"old": "pod_old", "new": "pod_new"}, nil)
		old := startHandoffRuntimeChild(t, a.Address, "pod_old", "process_pod_old", "old", false, nil)
		next := startHandoffRuntimeChild(t, a.Address, "pod_new", "process_pod_new", "new", true, nil)
		appendHandoffMessage(t, client, session, "initial_before_checkpoint")
		deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
		waitHandoffCondition(t, "admitted current provider before new input", func() bool { return old.calls(session) == 1 })
		appendHandoffMessage(t, client, session, "queued_before_checkpoint")
		old.signal(t, "quiesce")
		old.signal(t, session+"-1.release")
		old.join(t)
		deliverAttachmentRuntimeInput(t, runtimeDB, admin, next.port, session, "runtime-pod-new", "pod_new")
		waitHandoffCondition(t, "queued input continues original turn", func() bool { return next.calls(session) == 1 })
		raw, err := os.ReadFile(filepath.Join(next.directory, "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		var ledger []struct {
			MessagesJSON string `json:"messagesJson"`
		}
		if err := json.Unmarshal(raw, &ledger); err != nil || len(ledger) != 1 {
			t.Fatalf("replacement provider ledger=%s/%v", raw, err)
		}
		if !strings.Contains(ledger[0].MessagesJSON, "initial_before_checkpoint") || !strings.Contains(ledger[0].MessagesJSON, "queued_before_checkpoint") {
			t.Fatalf("queued context lost original input: %s", ledger[0].MessagesJSON)
		}
		var errors, ends, results, inputs int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type IN('session.error','session.runtime_pod_lost')),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'),(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1)`, session).Scan(&errors, &ends, &results, &inputs); err != nil {
			t.Fatal(err)
		}
		if errors != 0 || ends != 1 || results != 1 || inputs != 2 {
			t.Fatalf("queued checkpoint facts errors=%d ends=%d results=%d inputs=%d", errors, ends, results, inputs)
		}
		next.signal(t, "quiesce")
		waitHandoffCondition(t, "queued replacement quiesce fence", func() bool { _, err := os.Stat(filepath.Join(next.directory, "quiescing.json")); return err == nil })
		next.signal(t, session+"-1.release")
		next.join(t)
	})
	t.Run("idle main waits for active child checkpoint", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session, main, binding = "sesn_child_idle_a1", "thr_idle_main", "bind_child_idle"
		sessionfixture.SeedBridgeAPISession(t, admin, "default", session, main)
		seedHandoffRuntimeBinding(t, admin, session, binding, 1, "pod_old")
		sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, binding, 1)
		if _, err := admin.Exec(`UPDATE session_runtime_status SET status='idle' WHERE session_id=$1`, session); err != nil {
			t.Fatal(err)
		}
		store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", session)
		startHandoffOutputCaptures(t, runtimeDB)
		startHandoffResultListener(t, store)
		endpoint := serveReplicaBridge(t, store, map[string]string{"old": "pod_old", "new": "pod_new"}, nil)
		old := startHandoffRuntimeChild(t, endpoint.Address, "pod_old", "process_pod_old", "old", false, []map[string]any{{"sessionId": session, "sessionThreadId": main, "bindingId": binding, "bindingGeneration": 1}})
		next := startHandoffRuntimeChild(t, endpoint.Address, "pod_new", "process_pod_new", "new", true, nil)
		parentScope := sessionfixture.BridgeAPIScope(session, main, binding, 1, "pod_old")
		const senderModel = "mreq_idle_main_send"
		source := writeDurableOrdinaryToolUseForTest(t, store, parentScope, senderModel, "call_spawn_child", "spawn_agent", `{"task_name":"active-child","agent_type":"worker","prompt":"held original child input"}`)
		var senderMessageSequence int64
		if err := admin.QueryRow(`SELECT sequence FROM session_messages WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'`, session, senderModel).Scan(&senderMessageSequence); err != nil {
			t.Fatal(err)
		}
		created, err := endpoint.Client.CreateSubagentThread(replicaRuntimeContext(context.Background(), "old"), &bridgev1.CreateSubagentThreadRequest{Scope: parentScope, SourceToolUseEventId: source, TaskName: "active-child", AgentType: "worker", InitialPrompt: "held original child input"})
		if err != nil || created.GetCommitted().GetChildThreadId() == "" {
			t.Fatalf("actual child birth=%v err=%v", created, err)
		}
		child := created.GetCommitted().GetChildThreadId()
		deliveryID := runtimecontrol.AgentMailDeliveryID(source, child)
		ended, err := endpoint.Client.WriteRequestEnd(replicaRuntimeContext(context.Background(), "old"), &bridgev1.WriteRequestEndRequest{Scope: parentScope, RuntimeWriteId: "rwrite_idle_main_send_end", ModelRequestId: senderModel, FinishReason: "tool-calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &senderMessageSequence, ToolUseEventIds: []string{source}}})
		if err != nil || (ended.GetCommitted() == nil && ended.GetDuplicate() == nil) {
			t.Fatalf("main original sender End=%v err=%v", ended, err)
		}
		settled, err := endpoint.Client.SettleToolResult(replicaRuntimeContext(context.Background(), "old"), sessionfixture.BridgeToolSettlementRequestForTest(parentScope, sessionfixture.BridgeCompletedToolSettlementForTest(source, "original child mail delivered")))
		if err != nil || settled.GetCommitted() == nil {
			t.Fatalf("main mail source settlement=%v err=%v", settled, err)
		}
		deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
		waitHandoffCondition(t, "child current provider held", func() bool { return old.calls(session) == 1 })
		entries := old.providerEntries(t, session)
		if entries[0].SessionThreadID != child {
			t.Fatalf("main dispatched instead of child: %+v", entries)
		}

		old.signal(t, "quiesce")
		waitHandoffAdmissionClosed(t, old)
		if handoffBindingCount(t, admin, session) != 1 {
			t.Fatal("idle main released while child current step held")
		}
		old.signal(t, session+"-1.release")
		waitHandoffCondition(t, "all-thread child checkpoint release", func() bool { return handoffBindingCount(t, admin, session) == 0 })
		// Handback re-enqueues the same accepted mail ahead of its handoff wake.
		// Drive that existing input through the real Runner before the wake;
		// Queue's same-thread ordering must not be bypassed by the fixture.
		var originalInput, originalEvents, beforeStatus, receivedID, wakeID string
		if err := admin.QueryRow(`SELECT i.runtime_input_id,i.event_ids_json,i.status,e.event_id,h.queue_job_id FROM session_runtime_inbox i JOIN session_events e ON e.session_id=i.session_id AND e.session_thread_id=i.session_thread_id AND e.type='agent.thread_message_received' AND e.payload_json::jsonb->>'delivery_id'=$2 JOIN session_runtime_handoff_threads h ON h.session_id=i.session_id AND h.session_thread_id=i.session_thread_id WHERE i.session_id=$1 AND i.runtime_input_id=$3`, session, deliveryID, "agent_mail:"+deliveryID).Scan(&originalInput, &originalEvents, &beforeStatus, &receivedID, &wakeID); err != nil {
			t.Fatal(err)
		}
		expectedBefore := "queued"

		if beforeStatus != expectedBefore || originalInput != "agent_mail:"+deliveryID || wakeID == "" {
			t.Fatalf("handed-back original mail input=%s status=%s events=%s wake=%s", originalInput, beforeStatus, originalEvents, wakeID)
		}

		deliverHandoffInputWithPlacement(t, runtimeDB, old, next, session)

		expectedCalls := 1

		waitHandoffCondition(t, "original child continuation on replacement", func() bool { return next.calls(session) == expectedCalls })
		var afterEvents, afterStatus, wakeStatus string
		var receivedCount, sourceCount int
		if err := admin.QueryRow(`SELECT i.event_ids_json,i.status,j.status,(SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='agent.thread_message_received' AND payload_json::jsonb->>'delivery_id'=$3),(SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id=$4 AND payload_json::jsonb->>'delivery_id'=$3) FROM session_runtime_inbox i JOIN queue_jobs j ON j.id=$5 WHERE i.session_id=$1 AND i.runtime_input_id=$6`, session, child, deliveryID, receivedID, wakeID, originalInput).Scan(&afterEvents, &afterStatus, &wakeStatus, &receivedCount, &sourceCount); err != nil {
			t.Fatal(err)
		}
		t.Logf("original child mail input=%s source=%s events=%s inbox=%s->%s handoffWake=%s/%s receivedEvents=%d", originalInput, receivedID, originalEvents, beforeStatus, afterStatus, wakeID, wakeStatus, receivedCount)
		expectedAfter, expectedWake := "accepted", "pending"

		if afterEvents != originalEvents || afterStatus != expectedAfter || wakeStatus != expectedWake || receivedCount != 1 || sourceCount != 1 {
			t.Fatalf("original mail custody changed events=%s status=%s wake=%s received=%d source=%d", afterEvents, afterStatus, wakeStatus, receivedCount, sourceCount)
		}
		old.join(t)
		nextEntries := next.providerEntries(t, session)
		var nextChild handoffProviderEntry
		for _, entry := range nextEntries {
			if entry.SessionThreadID == child {
				nextChild = entry
			}
		}
		if nextChild.SessionThreadID != child || entries[0].ModelRequestID == nextChild.ModelRequestID {
			t.Fatalf("child continuation identities old=%+v next=%+v", entries, nextEntries)
		}
		assertHandoffExactToolContext(t, admin, session, entries[0].ModelRequestID, nextChild.MessagesJSON)
		mailStatus := "accepted"

		var idleMain, recoverChild, loss, mail int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND session_thread_id=$2 AND disposition='idle'),(SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND session_thread_id=$3 AND disposition='recover' AND queue_job_id IS NOT NULL),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type IN('session.error','session.runtime_pod_lost')),(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND session_thread_id=$3 AND runtime_input_id=$4 AND status=$5)`, session, main, child, "agent_mail:"+deliveryID, mailStatus).Scan(&idleMain, &recoverChild, &loss, &mail); err != nil || idleMain != 1 || recoverChild != 1 || loss != 0 || mail != 1 {
			t.Fatalf("all-thread checkpoint mainIdle=%d childRecover=%d loss=%d originalMail=%d err=%v", idleMain, recoverChild, loss, mail, err)
		}
		for ordinal := 1; ordinal <= expectedCalls; ordinal++ {
			next.signal(t, fmt.Sprintf("%s-%d.release", session, ordinal))
		}
		waitHandoffCondition(t, "original child successor finishes", func() bool {
			var idle int
			return admin.QueryRow(`SELECT count(*) FROM session_threads WHERE session_id=$1 AND id=$2 AND status='idle'`, session, child).Scan(&idle) == nil && idle == 1
		})
		waitHandoffCondition(t, "completed original child wake acknowledged", func() bool {
			var ready bool
			if err := admin.QueryRow(`SELECT status,available_at<=clock_timestamp() FROM queue_jobs WHERE id=$1`, wakeID).Scan(&wakeStatus, &ready); err != nil {
				t.Fatal(err)
			}
			if wakeStatus == "acknowledged" {
				return true
			}
			if wakeStatus != "pending" {
				t.Fatalf("original child wake changed custody: %s", wakeStatus)
			}
			if ready {
				deliverHandoffRecovery(t, runtimeDB, next, "pod_new")
			}
			return false
		})
		if err := admin.QueryRow(`SELECT status FROM queue_jobs WHERE id=$1`, wakeID).Scan(&wakeStatus); err != nil || wakeStatus != "acknowledged" || next.calls(session) != expectedCalls {
			t.Fatalf("completed child wake custody=%s successor calls=%d err=%v", wakeStatus, next.calls(session), err)
		}
		next.signal(t, "quiesce")
		waitHandoffAdmissionClosed(t, next)
		next.join(t)
	})
	t.Run("concurrent main input and child recovery create one binding", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session, main, child, binding = "sesn_competing_child_b", "thr_competing_main", "thr_competing_child", "bind_competing"
		sessionfixture.SeedBridgeAPISession(t, admin, "default", session, main)
		sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", session, main, child)
		seedHandoffRuntimeBinding(t, admin, session, binding, 1, "pod_old")
		sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, binding, 1)
		sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", session)
		startHandoffOutputCaptures(t, runtimeDB)
		store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		startHandoffResultListener(t, store)
		endpoint := serveReplicaBridge(t, store, map[string]string{"old": "pod_old", "new": "pod_new"}, nil)
		scope := sessionfixture.BridgeAPIScope(session, child, binding, 1, "pod_old")
		const model = "mreq_competing_child_read"
		tool := writeDurableOrdinaryToolUseForTest(t, store, scope, model, "call_competing_read", "Read", `{"file_path":"/workspace/input.txt"}`)
		var sequence int64
		if err := admin.QueryRow(`SELECT sequence FROM session_messages WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'`, session, model).Scan(&sequence); err != nil {
			t.Fatal(err)
		}
		ended, err := endpoint.Client.WriteRequestEnd(replicaRuntimeContext(context.Background(), "old"), &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "rwrite_competing_child_end", ModelRequestId: model, FinishReason: "tool-calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{tool}}})
		if err != nil || (ended.GetCommitted() == nil && ended.GetDuplicate() == nil) {
			t.Fatalf("competing original child End=%v err=%v", ended, err)
		}
		accepted, err := endpoint.Client.AcceptSandboxExecution(replicaRuntimeContext(context.Background(), "old"), &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: tool})
		if err != nil || accepted.GetCommitted() == nil {
			t.Fatalf("competing child execution receipt=%v err=%v", accepted, err)
		}
		external := startHandoffSandboxOwner(t, runtimeDB, session)
		waitHandoffCondition(t, "competing original external command owned", func() bool { return external.observations.Load() > 0 })
		old := startHandoffRuntimeChild(t, endpoint.Address, "pod_old", "process_pod_old", "old", false, []map[string]any{{"sessionId": session, "sessionThreadId": main, "bindingId": binding, "bindingGeneration": 1}, {"sessionId": session, "sessionThreadId": child, "bindingId": binding, "bindingGeneration": 1}})
		next := startHandoffRuntimeChild(t, endpoint.Address, "pod_new", "process_pod_new", "new", true, nil)
		old.signal(t, "quiesce")
		waitHandoffAdmissionClosed(t, old)
		waitHandoffCondition(t, "accepted child execution handoff", func() bool { return handoffBindingCount(t, admin, session) == 0 })
		var mail, wakes int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND input_kind='agent_mail'),(SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND session_thread_id=$2 AND disposition='recover' AND queue_job_id IS NOT NULL)`, session, child).Scan(&mail, &wakes); err != nil || mail != 0 || wakes != 1 {
			t.Fatalf("competing legal custody mail=%d childWake=%d err=%v", mail, wakes, err)
		}
		appendHandoffMessage(t, dbconnect.NewClientForTesting(runtimeDB), session, "main-input-competes-with-child-recovery")
		raceHandoffBinders(t, runtimeDB, admin, next, session, main, child)
		waitHandoffCondition(t, "competing main input uses replacement", func() bool { return next.calls(session) == 1 })
		if entries := next.providerEntries(t, session); len(entries) != 1 || entries[0].SessionThreadID != main {
			t.Fatalf("competing main provider=%+v", entries)
		}
		if external.calls.Load() != 1 {
			t.Fatalf("competing recovery duplicated external invocation: %d", external.calls.Load())
		}
		old.join(t)
		external.finish()
		select {
		case err := <-external.done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("competing original external owner did not join")
		}
		waitHandoffCondition(t, "competing original child tool continues", func() bool { return next.calls(session) == 2 })
		var resultCount, loss int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result' AND tool_use_event_id=$3),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error')`, session, child, tool).Scan(&resultCount, &loss); err != nil || resultCount != 1 || loss != 0 || external.calls.Load() != 1 {
			t.Fatalf("competing original result=%d loss=%d external=%d err=%v", resultCount, loss, external.calls.Load(), err)
		}
		next.signal(t, "quiesce")
		waitHandoffAdmissionClosed(t, next)
		for ordinal := 1; ordinal <= 2; ordinal++ {
			next.signal(t, fmt.Sprintf("%s-%d.release", session, ordinal))
		}
		next.join(t)
	})
	for _, commitBoundary := range []string{"before commit", "after committed receipt"} {
		t.Run("selected process death "+commitBoundary, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			const session, thread, binding = "sesn_release_death_a1", "thr_release_death", "bind_release_death"
			sessionfixture.SeedBridgeAPISession(t, admin, "default", session, thread)
			seedHandoffRuntimeBinding(t, admin, session, binding, 1, "pod_old")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, binding, 1)
			store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
			startHandoffResultListener(t, store)
			entered := make(chan *bridgev1.ReleaseRuntimeBindingRequest, 1)
			resume, joined := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			var held atomic.Bool
			var releaseRequest *bridgev1.ReleaseRuntimeBindingRequest
			faultStore := &handoffFaultBridgeStore{BridgeAPIStore: store, beforeRelease: func(r *bridgev1.ReleaseRuntimeBindingRequest) error {
				if commitBoundary == "before commit" && held.CompareAndSwap(false, true) {
					defer close(joined)
					entered <- proto.Clone(r).(*bridgev1.ReleaseRuntimeBindingRequest)
					<-resume
					return status.Error(codes.Unavailable, "selected process died before release commit")
				}
				releaseRequest = proto.Clone(r).(*bridgev1.ReleaseRuntimeBindingRequest)
				return nil
			}}
			endpoint := serveReplicaBridge(t, faultStore, map[string]string{"old": "pod_old", "new": "pod_new"}, func(_ context.Context, method string, _ any) error {
				if commitBoundary == "after committed receipt" && method == bridgev1.AgentRuntimeBridgeService_ReleaseRuntimeBinding_FullMethodName && held.CompareAndSwap(false, true) {
					defer close(joined)
					entered <- releaseRequest
					<-resume
					return status.Error(codes.Unavailable, "selected process died before committed release acknowledgement")
				}
				return nil
			})
			t.Cleanup(func() {
				release()
				if held.Load() {
					select {
					case <-joined:
					case <-time.After(time.Second):
						t.Error("held release handler did not join")
					}
				}
			})
			old := startHandoffRuntimeChild(t, endpoint.Address, "pod_old", "process_pod_old", "old", false, nil)
			next := startHandoffRuntimeChild(t, endpoint.Address, "pod_new", "process_pod_new", "new", true, nil)
			appendHandoffMessage(t, dbconnect.NewClientForTesting(runtimeDB), session, "original_before_selected_death")
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
			waitHandoffCondition(t, "death variant original current frame", func() bool { return old.calls(session) == 1 })
			original := old.providerEntries(t, session)[0]
			old.signal(t, "quiesce")
			waitHandoffAdmissionClosed(t, old)
			old.signal(t, session+"-1.release")
			var request *bridgev1.ReleaseRuntimeBindingRequest
			select {
			case request = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("selected release commit boundary not reached")
			}
			var receipts, wakes, bindings, successfulEnds, failedEnds, results int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_handoffs WHERE session_id=$1 AND operation_id=$2),(SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND queue_job_id IS NOT NULL),(SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1),(SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$3 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result')`, session, request.OperationId, original.ModelRequestID).Scan(&receipts, &wakes, &bindings, &successfulEnds, &failedEnds, &results); err != nil {
				t.Fatal(err)
			}
			if successfulEnds != 1 || failedEnds != 0 || results != 1 {
				t.Fatalf("selected release boundary changed current successful settlement End=%d failed=%d result=%d", successfulEnds, failedEnds, results)
			}
			if commitBoundary == "before commit" {
				if receipts != 0 || wakes != 0 || bindings != 1 {
					t.Fatalf("precommit receipt=%d wake=%d binding=%d", receipts, wakes, bindings)
				}
			} else if receipts != 1 || wakes != 1 || bindings != 0 {
				t.Fatalf("committed receipt=%d wake=%d binding=%d", receipts, wakes, bindings)
			}
			old.kill(t)
			release()
			select {
			case <-joined:
			case <-time.After(time.Second):
				t.Fatal("selected release raw handler did not join")
			}
			if commitBoundary == "before commit" {
				repair := runtimePodLossSweepStore(t, runtimeDB, nil, func() enginekubernetes.BindingVisibilitySnapshot {
					return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-new", PodUID: "pod_new", PodIP: "127.0.0.1"}})
				})
				if repaired, err := repair.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 1 {
					t.Fatalf("actual confirmed-death repair=%d err=%v", repaired, err)
				}
				var idle, errors, originalEnd, noReceipt, rewrittenEnds int
				if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle' AND payload_json::jsonb->'stop_reason'->>'type'='retries_exhausted'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error' AND payload_json::jsonb->'error'->>'type'='unknown_error' AND payload_json::jsonb->'error'->'retry_status'->>'type'='exhausted'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'),(SELECT count(*) FROM session_runtime_handoffs WHERE session_id=$1),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true')`, session, original.ModelRequestID).Scan(&idle, &errors, &originalEnd, &noReceipt, &rewrittenEnds); err != nil || idle != 1 || errors != 1 || originalEnd != 1 || noReceipt != 0 || rewrittenEnds != 0 || handoffBindingCount(t, admin, session) != 0 {
					t.Fatalf("precommit confirmed-loss settlement idle=%d error=%d originalEnd=%d receipt=%d failedEnds=%d err=%v", idle, errors, originalEnd, noReceipt, rewrittenEnds, err)
				}
				// Loss settled the closed request's unfinished turn; this is new
				// business work, not a claim of cooperative continuation success.
				appendHandoffMessage(t, dbconnect.NewClientForTesting(runtimeDB), session, "new_business_after_loss_settlement")
				deliverHandoffInputWithPlacement(t, runtimeDB, old, next, session)
			} else {
				deliverHandoffRecovery(t, runtimeDB, next, "pod_new")
			}
			waitHandoffCondition(t, "replacement after selected death", func() bool { return next.calls(session) == 1 })
			nextEntry := next.providerEntries(t, session)[0]
			assertHandoffExactToolContext(t, admin, session, original.ModelRequestID, nextEntry.MessagesJSON)
			assertHandoffLateWriteFence(t, admin, endpoint, session, thread, binding, 1)
			if commitBoundary == "after committed receipt" {
				assertHandoffOldOwnerFence(t, admin, endpoint, session, thread, binding, 1)
				var loss int
				if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type IN('session.error','session.runtime_pod_lost')`, session).Scan(&loss); err != nil || loss != 0 {
					t.Fatalf("committed death fabricated loss=%d err=%v", loss, err)
				}
			}
			next.signal(t, "quiesce")
			waitHandoffAdmissionClosed(t, next)
			next.signal(t, session+"-1.release")
			next.join(t)
		})
	}
	for _, reviewerCase := range []struct {
		scenario, stage string
		holdRead        bool
	}{{"hold", "starts-after-quiesce", false}, {"allow", "starts-after-quiesce", false}, {"deny", "starts-after-quiesce", false}, {"allow", "continues-after-read", false}, {"deny", "continues-after-read", false}, {"allow", "read-expiry", true}} {
		scenario, stage := reviewerCase.scenario, reviewerCase.stage
		expired := scenario == "hold" || reviewerCase.holdRead
		t.Run("reviewer "+stage+" "+scenario, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			client := dbconnect.NewClientForTesting(runtimeDB)
			session, thread := "sesn_review_"+scenario+"_"+strings.ReplaceAll(stage, "-", "_"), "thr_review_"+scenario+"_"+strings.ReplaceAll(stage, "-", "_")
			sessionfixture.SeedBridgeAPISession(t, admin, "default", session, thread)
			seedHandoffRuntimeBinding(t, admin, session, "bind_review", 1, "pod_old")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, "bind_review", 1)
			unrelatedReviewerID := "thr_unrelated_" + strings.ReplaceAll(stage, "-", "_") + "_" + scenario
			sessionfixture.SeedBridgeAPIInternalReviewerThread(t, admin, "default", session, thread, unrelatedReviewerID)
			if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'`); err != nil {
				t.Fatal(err)
			}
			sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", session)
			startHandoffOutputCaptures(t, runtimeDB)
			store := bridge.NewPostgreSQLBridgeAPIStore(client)
			store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
			startHandoffResultListener(t, store)
			observed := &handoffObservedBridgeStore{BridgeAPIStore: store}
			a := serveReplicaBridge(t, observed, map[string]string{"old": "pod_old", "new": "pod_new"}, nil)
			old := startHandoffRuntimeChild(t, a.Address, "pod_old", "process_pod_old", "old", false, []map[string]any{{"sessionId": session, "sessionThreadId": unrelatedReviewerID, "parentThreadId": thread, "bindingId": "bind_review", "bindingGeneration": 1}}, scenario)
			next := startHandoffRuntimeChild(t, a.Address, "pod_new", "process_pod_new", "new", true, nil, scenario)
			appendHandoffMessage(t, client, session, "review_admitted_step")
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
			waitHandoffCondition(t, "parent admitted provider", func() bool { return old.calls(session) == 1 })
			var quiesceStarted time.Time
			if stage == "starts-after-quiesce" {
				quiesceStarted = time.Now()
				old.signal(t, "quiesce")
				waitHandoffAdmissionClosed(t, old)
				if scenario == "hold" {
					assertHandoffUnrelatedInputRejected(t, old, session, thread, "bind_review")
				}
			}
			old.signal(t, session+"-1.release")
			waitHandoffCondition(t, "admitted reviewer provider", func() bool {
				select {
				case <-old.done:
					out, _ := os.ReadFile(filepath.Join(old.directory, "output.log"))
					diag, _ := os.ReadFile(filepath.Join(old.directory, "diagnostics.jsonl"))
					t.Fatalf("reviewer child exited calls=%d error=%v output=%s diagnostics=%s", old.calls(session), old.err, out, diag)
				default:
				}
				return old.calls(session) == 2
			})
			assertUnrelatedReviewRejected := func() {
				old.signal(t, "reject-unrelated-review")
				waitHandoffCondition(t, "actual unrelated reviewer owner rejection", func() bool {
					raw, err := os.ReadFile(filepath.Join(old.directory, "unrelated-review.json"))
					if err != nil {
						return false
					}
					var result struct {
						OK     bool   `json:"ok"`
						Reason string `json:"reason"`
					}
					if err := json.Unmarshal(raw, &result); err != nil {
						t.Fatal(err)
					}
					if result.OK || result.Reason != "thread_busy" {
						t.Fatalf("unrelated reviewer did not hit typed drain fence: %s", raw)
					}
					return true
				})
				if old.calls(session) != 2 {
					t.Fatal("unrelated reviewer started another provider while Read held")
				}
			}
			if stage == "starts-after-quiesce" && scenario == "hold" {
				assertUnrelatedReviewRejected()
			}
			if scenario != "hold" {
				waitHandoffCondition(t, "reviewer Read accepted", func() bool {
					var n int
					_ = admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_name='Read'`, session).Scan(&n)
					return n == 1
				})
				external := startHandoffSandboxOwner(t, runtimeDB, session)
				waitHandoffCondition(t, "reviewer Read command owned", func() bool { return external.observations.Load() > 0 })
				if stage == "starts-after-quiesce" {
					assertHandoffUnrelatedInputRejected(t, old, session, thread, "bind_review")
					assertUnrelatedReviewRejected()
				}
				if stage == "continues-after-read" || reviewerCase.holdRead {
					if reviewerCase.holdRead {
						waitHandoffCondition(t, "actual captured reviewer Await active before drain", func() bool { return observed.activeAwait.Load() == 1 })
					}
					quiesceStarted = time.Now()
					old.signal(t, "quiesce")
					waitHandoffAdmissionClosed(t, old)
					assertHandoffUnrelatedInputRejected(t, old, session, thread, "bind_review")
					assertUnrelatedReviewRejected()
					if old.calls(session) != 2 {
						t.Fatal("reviewer second provider started before held Read release")
					}
				}
				if !reviewerCase.holdRead {
					external.finish()
					select {
					case err := <-external.done:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("reviewer Read owner did not join")
					}
					waitHandoffCondition(t, "reviewer decision continuation during same deadline", func() bool { return old.calls(session) == 3 })
				}
			}
			old.join(t)
			oldExitDuration := time.Since(quiesceStarted)
			var effects int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2)+(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND session_thread_id=$2)`, session, unrelatedReviewerID).Scan(&effects); err != nil || effects != 0 {
				t.Fatalf("unrelated review effects=%d providerCalls=%d err=%v", effects, old.calls(session), err)
			}
			expectedProviderCalls := 3
			if expired {
				expectedProviderCalls = 2
			}
			if old.calls(session) != expectedProviderCalls {
				t.Fatalf("reviewer provider census=%d expected=%d", old.calls(session), expectedProviderCalls)
			}
			for _, entry := range old.providerEntries(t, session) {
				if entry.SessionThreadID == unrelatedReviewerID {
					t.Fatal("unrelated reviewer admitted provider work during drain")
				}
			}
			if expired && (quiesceStarted.IsZero() || oldExitDuration > 5*time.Second) {
				t.Fatalf("reviewer expiry exceeds unchanged 2s+2s+1s application phases: %v", oldExitDuration)
			}
			if reviewerCase.holdRead {
				t.Logf("captured Read transport returned=%d active=%d releaseOverlap=%d actualWait=%v quiesceToExit=%v", observed.joinedAwait.Load(), observed.activeAwait.Load(), observed.releaseDuringAwait.Load(), time.Duration(observed.lastAwaitDuration.Load()), oldExitDuration)
			}
			var declarations, executions, approvals, failedEnds int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='agent.tool_use'),(SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_name='Write'),(SELECT count(*) FROM session_pending_tool_uses WHERE session_id=$1 AND status='pending'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true')`, session, thread).Scan(&declarations, &executions, &approvals, &failedEnds); err != nil {
				t.Fatal(err)
			}
			if expired {
				expectedFailedEnds := 2
				if reviewerCase.holdRead {
					expectedFailedEnds = 1
				}
				if declarations != 0 || executions != 0 || approvals != 0 || failedEnds != expectedFailedEnds {
					t.Fatalf("held reviewer manufactured outcome declarations=%d executions=%d approvals=%d failedEnds=%d", declarations, executions, approvals, failedEnds)
				}
				if reviewerCase.holdRead {
					entries := old.providerEntries(t, session)
					if len(entries) != 2 {
						t.Fatalf("Read expiry provider identities=%+v", entries)
					}
					var reviewerEnds, reviewerErrors, reviewerIdle, idleThreads int
					if err := admin.QueryRow(`SELECT count(*) FILTER(WHERE e.type='span.model_request_end' AND e.model_request_id=$2 AND e.payload_json::jsonb->>'is_error'='false'),count(*) FILTER(WHERE e.type='session.error'),count(*) FILTER(WHERE e.type='session.thread_status_idle'),count(DISTINCT th.id) FILTER(WHERE th.status='idle') FROM session_events e JOIN session_threads th ON th.id=e.session_thread_id AND th.session_id=e.session_id WHERE e.session_id=$1 AND th.role='approval_reviewer' AND th.id<>$3`, session, entries[1].ModelRequestID, unrelatedReviewerID).Scan(&reviewerEnds, &reviewerErrors, &reviewerIdle, &idleThreads); err != nil || reviewerEnds != 1 || reviewerErrors != 1 || reviewerIdle != 1 || idleThreads != 1 {
						t.Fatalf("Read expiry durable reviewer closeout successful-original-End=%d error=%d thread-idle=%d idle-threads=%d err=%v", reviewerEnds, reviewerErrors, reviewerIdle, idleThreads, err)
					}
					var parentFailedEnd int
					if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end' AND model_request_id=$3 AND payload_json::jsonb->>'is_error'='true'`, session, thread, entries[0].ModelRequestID).Scan(&parentFailedEnd); err != nil || parentFailedEnd != 1 {
						t.Fatalf("original parent failed End=%d err=%v", parentFailedEnd, err)
					}
				}
			} else {
				expectedExecutions := 0
				if scenario == "allow" {
					expectedExecutions = 1
				}
				if declarations != 1 || executions != expectedExecutions || approvals != 0 || failedEnds != 0 {
					t.Fatalf("reviewer normal outcome declarations=%d executions=%d approvals=%d failedEnds=%d", declarations, executions, approvals, failedEnds)
				}
			}
			if handoffBindingCount(t, admin, session) != 0 {
				t.Fatal("completed reviewer checkpoint did not release")
			}
			if !expired {
				deliverHandoffRecovery(t, runtimeDB, next, "pod_new")
				if scenario == "allow" {
					writeOwner := startHandoffSandboxOwner(t, runtimeDB, session)
					waitHandoffCondition(t, "reviewed Write original command owned", func() bool { return writeOwner.observations.Load() > 0 })
					writeOwner.finish()
					select {
					case err := <-writeOwner.done:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("reviewed Write owner did not join")
					}
				}
				waitHandoffCondition(t, "next ordinary parent provider on replacement", func() bool { return next.calls(session) == 1 })
				if old.calls(session) != 3 {
					t.Fatal("ordinary parent successor ran on draining Runtime")
				}
			}
			next.signal(t, "quiesce")
			waitHandoffAdmissionClosed(t, next)
			if !expired {
				next.signal(t, session+"-1.release")
			}
			next.join(t)
			if expired {
				records := readHandoffDiagnostics(t, old)
				found := false
				for _, record := range records {
					if record["event"] == "runtime_checkpoint_expired" && record["parent.thread.id"] == thread && record["reviewer.thread.id"] != "" {
						found = true
					}
				}
				if !found {
					t.Fatalf("reviewer expiry lost parent correlation: %+v", records)
				}
			}
			if observed.activeAwait.Load() != 0 || observed.releaseDuringAwait.Load() != 0 || (reviewerCase.holdRead && observed.joinedAwait.Load() < 1) {
				t.Fatalf("reviewer raw Await ownership active=%d joined=%d release-while-active=%d", observed.activeAwait.Load(), observed.joinedAwait.Load(), observed.releaseDuringAwait.Load())
			}
			if reviewerCase.holdRead && (observed.cancelledAwait.Load() != 1 || time.Duration(observed.lastAwaitDuration.Load()) < 2*time.Second) {
				t.Fatalf("actual held Await expiry duration=%v cancelled=%d", time.Duration(observed.lastAwaitDuration.Load()), observed.cancelledAwait.Load())
			}
		})
	}
	for _, mode := range []string{"silent", "throw"} {
		t.Run("handoff diagnostic sink "+mode, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			session, thread := "sesn_sink_"+mode, "thr_sink_"+mode
			sessionfixture.SeedBridgeAPISession(t, admin, "default", session, thread)
			seedHandoffRuntimeBinding(t, admin, session, "bind_sink", 1, "pod_old")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, "bind_sink", 1)
			store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
			endpoint := serveReplicaBridge(t, store, map[string]string{"old": "pod_old"}, nil)
			child := startHandoffRuntimeChild(t, endpoint.Address, "pod_old", "process_pod_old", "old", false, []map[string]any{{"sessionId": session, "sessionThreadId": thread, "bindingId": "bind_sink", "bindingGeneration": 1}}, "", "", mode)
			child.signal(t, "quiesce")
			child.join(t)
			var idle, queued int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND disposition='idle'),(SELECT count(*) FROM session_runtime_handoff_threads WHERE session_id=$1 AND queue_job_id IS NOT NULL)`, session).Scan(&idle, &queued); err != nil || idle != 1 || queued != 0 || handoffBindingCount(t, admin, session) != 0 {
				t.Fatalf("sink changed durable handoff idle=%d queued=%d err=%v", idle, queued, err)
			}
		})
	}

	t.Run("same Pod new boot rejects previous promotion", func(t *testing.T) {
		runtimeDB, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
		store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		startHandoffResultListener(t, store)
		a := serveReplicaBridge(t, store, map[string]string{"one": "pod_same"}, nil)
		b := serveReplicaBridge(t, store, map[string]string{"two": "pod_same"}, nil)
		first := startHandoffRuntimeChild(t, a.Address, "pod_same", "boot_one", "one", false, nil)
		registration, err := a.Client.RegisterRuntimeProcess(replicaRuntimeContext(context.Background(), "one"), &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "boot_one"})
		if err != nil {
			t.Fatal(err)
		}
		second := startHandoffRuntimeChild(t, b.Address, "pod_same", "boot_two", "two", false, nil)
		_, err = a.Client.ReportRuntimeProcess(replicaRuntimeContext(context.Background(), "one"), &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: "boot_one", RegistrationReceipt: registration.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("old boot promotion=%v", err)
		}
		first.signal(t, "quiesce")
		first.join(t)
		second.signal(t, "quiesce")
		second.join(t)
	})
	t.Run("selected container restart retains Pod UID and fences old binding", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session, thread, binding, pod = "sesn_container_restart_b", "thr_container_restart", "bind_container_restart", "pod_same"
		sessionfixture.SeedBridgeAPISession(t, admin, "default", session, thread)
		seedHandoffRuntimeBinding(t, admin, session, binding, 1, pod)
		sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, binding, 1)
		store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		startHandoffResultListener(t, store)
		sessionfixture.SeedReadySandboxForSharedToolExecution(t, admin, "default", session)
		startHandoffOutputCaptures(t, runtimeDB)
		observed := &handoffObservedBridgeStore{BridgeAPIStore: store}
		endpoint := serveReplicaBridge(t, observed, map[string]string{"one": pod, "two": pod}, nil)
		first := startHandoffRuntimeChild(t, endpoint.Address, pod, "process_"+pod, "one", false, nil)
		registration, err := endpoint.Client.RegisterRuntimeProcess(replicaRuntimeContext(context.Background(), "one"), &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "process_" + pod})
		if err != nil {
			t.Fatal(err)
		}
		originalWrite := &bridgev1.WriteEventRequest{Scope: sessionfixture.BridgeAPIScope(session, thread, binding, 1, pod), RuntimeWriteId: "rwrite_before_container_restart", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}
		original, err := endpoint.Client.WriteEvent(replicaRuntimeContext(context.Background(), "one"), originalWrite)
		if err != nil || original.GetCommitted() == nil {
			t.Fatalf("first container write=%v err=%v", original, err)
		}
		client := dbconnect.NewClientForTesting(runtimeDB)
		appendHandoffMessage(t, client, session, "original_container_work")
		deliverAttachmentRuntimeInput(t, runtimeDB, admin, first.port, session, "runtime-pod-0", pod)
		waitHandoffCondition(t, "old container actual Read admitted", func() bool {
			var accepted int
			_ = admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1`, session).Scan(&accepted)
			return accepted == 1 && first.calls(session) == 1
		})
		originalProvider := first.providerEntries(t, session)[0]
		originalInput := readHandoffPendingInput(t, admin, session)
		var toolID, originalTurnID string
		if err := admin.QueryRow(`SELECT tool.event_id,(SELECT event_id FROM session_events WHERE session_id=$1 AND type='session.status_running' ORDER BY sequence DESC LIMIT 1) FROM session_events tool WHERE tool.session_id=$1 AND tool.model_request_id=$2 AND tool.type='agent.tool_use'`, session, originalProvider.ModelRequestID).Scan(&toolID, &originalTurnID); err != nil {
			t.Fatal(err)
		}
		external := startHandoffSandboxOwner(t, runtimeDB, session)
		waitHandoffCondition(t, "old actual external command and Bridge wait", func() bool {
			return external.calls.Load() == 1 && external.observations.Load() > 0 && observed.activeAwait.Load() == 1
		})
		// This scenario intentionally exercises End-before-result. Confirm the
		// actual successful End before death while the external result is held.
		waitHandoffCondition(t, "original container successful End before kill", func() bool {
			var count int
			_ = admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'`, session, originalProvider.ModelRequestID).Scan(&count)
			return count == 1
		})
		first.kill(t)
		waitHandoffCondition(t, "dead container raw Bridge waiter returned", func() bool { return observed.activeAwait.Load() == 0 && observed.joinedAwait.Load() > 0 })
		select {
		case <-external.done:
			t.Fatal("killing Runtime completed independent external work")
		default:
		}
		second := startHandoffRuntimeChild(t, endpoint.Address, pod, "boot_after_restart", "two", true, nil)
		_, err = endpoint.Client.ReportRuntimeProcess(replicaRuntimeContext(context.Background(), "one"), &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: "process_" + pod, RegistrationReceipt: registration.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("dead container promotion=%v", err)
		}
		replay, err := endpoint.Client.WriteEvent(replicaRuntimeContext(context.Background(), "one"), originalWrite)
		if err != nil || replay.GetDuplicate().GetEventId() != original.GetCommitted().GetEventId() {
			t.Fatalf("retired unchanged-binding named receipt=%v err=%v", replay, err)
		}
		fresh := proto.Clone(originalWrite).(*bridgev1.WriteEventRequest)
		fresh.RuntimeWriteId = "rwrite_after_container_restart_old_scope"
		late, err := endpoint.Client.WriteEvent(replicaRuntimeContext(context.Background(), "one"), fresh)
		if err != nil || late.GetStale() == nil {
			t.Fatalf("retired container fresh write=%v err=%v", late, err)
		}
		var count int
		var current string
		if err := admin.QueryRow(`SELECT count(*),MAX(runtime_process_id) FILTER(WHERE is_current) FROM runtime_processes WHERE namespace='tetral-agent-runtime' AND pod_uid=$1`, pod).Scan(&count, &current); err != nil || count != 2 || current != "boot_after_restart" {
			t.Fatalf("same Pod process rows=%d current=%s err=%v", count, current, err)
		}
		// The Pod remains the same ready object. Only the committed fresh boot
		// promotion supplies the process-loss fact; there is no absent-Pod seam.
		targetURL, err := url.Parse(second.httpURL)
		if err != nil {
			t.Fatal(err)
		}
		transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, targetURL.Host)
		}}
		defer transport.CloseIdleConnections()
		delivery := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, second.port, jobrunner.KubernetesRuntimeTargetResolver{
			GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
				return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: pod, IP: "127.0.0.1", Running: true, Ready: true}, nil
			},
			LoadClient: &http.Client{Transport: transport},
			Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: pod, PodIP: "127.0.0.1"}})
			},
		})
		if repaired, err := delivery.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 1 {
			t.Fatalf("same ready Pod superseded-process repair=%d err=%v", repaired, err)
		}
		if handoffBindingCount(t, admin, session) != 0 {
			t.Fatal("old process binding retained after real supersession repair")
		}
		queueStore := queue.NewPostgreSQLStore(client)
		leases, err := queueStore.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "container-restart-runner", MaxJobs: 1, LeaseDuration: time.Minute})
		if err != nil || len(leases) != 1 {
			t.Fatalf("original container work recovery lease=%v err=%v", leases, err)
		}
		job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leases[0]))
		if err != nil || job.SessionID != session || job.SessionThreadID != thread || job.RecoverySourceEventID != toolID || job.RecoveryHandoffID != "" {
			t.Fatalf("original recovery identity=%+v tool=%s err=%v", job, toolID, err)
		}
		sender := jobrunner.NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})
		defer func() {
			if err := sender.Close(); err != nil {
				t.Error(err)
			}
		}()
		observedDelivery := &handoffObservedDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: sender}}
		runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: observedDelivery}
		if err := runIssuedLeaseThroughRunner(context.Background(), runner, queueJobProto(leases[0]), jobrunner.JobRunnerConfig{LeaseOwner: "container-restart-runner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}); err != nil {
			t.Fatal(err)
		}
		if observedDelivery.result.Status != jobrunner.RuntimeDeliveryAccepted && observedDelivery.result.Status != jobrunner.RuntimeDeliveryDuplicate {
			t.Fatalf("same Pod original recovery admission=%+v", observedDelivery.result)
		}
		var newBinding, newPod, newProcess, newName, newIP, wakeState string
		var newGeneration int64
		if err := admin.QueryRow(`SELECT binding_id,binding_generation,agent_runtime_pod_uid,runtime_process_id,agent_runtime_pod_name,agent_runtime_pod_ip FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&newBinding, &newGeneration, &newPod, &newProcess, &newName, &newIP); err != nil || newBinding == binding || newGeneration <= 1 || newPod != pod || newProcess != "boot_after_restart" || newName != "runtime-pod-0" || newIP != "127.0.0.1" {
			t.Fatalf("same Pod fresh binding=%s/%d/%s/%s/%s/%s err=%v", newBinding, newGeneration, newPod, newProcess, newName, newIP, err)
		}
		if err := admin.QueryRow(`SELECT status FROM queue_jobs WHERE id=$1`, job.JobID).Scan(&wakeState); err != nil || wakeState != queue.StatusAcknowledged {
			t.Fatalf("original recovery Queue custody=%s status=%s err=%v", job.JobID, wakeState, err)
		}
		waitHandoffCondition(t, "same Pod new Core rejoins original external command", func() bool { return observed.activeAwait.Load() == 1 })
		if second.calls(session) != 0 || external.calls.Load() != 1 {
			t.Fatal("replacement requested model/reexecuted command before original result")
		}
		late, err = endpoint.Client.WriteEvent(replicaRuntimeContext(context.Background(), "one"), fresh)
		if err != nil || late.GetStale() == nil {
			t.Fatalf("old generation after new binding=%v err=%v", late, err)
		}
		var unchanged string
		if err := admin.QueryRow(`SELECT binding_id FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&unchanged); err != nil || unchanged != newBinding {
			t.Fatalf("old write changed new binding=%s err=%v", unchanged, err)
		}
		external.finish()
		select {
		case err := <-external.done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("original external owner did not join")
		}
		waitHandoffCondition(t, "same Pod original result starts successor provider", func() bool { return second.calls(session) == 1 })
		successor := second.providerEntries(t, session)[0]
		if successor.ModelRequestID == originalProvider.ModelRequestID || successor.RequestID == originalProvider.RequestID {
			t.Fatal("successor reused original provider request identity")
		}
		assertHandoffExactToolContext(t, admin, session, originalProvider.ModelRequestID, successor.MessagesJSON, "Read")
		if !strings.Contains(successor.MessagesJSON, "original_container_work") {
			t.Fatal("original business input absent from recovered context")
		}
		var results, errors, ends int
		var endSequence, resultSequence int64
		var recoveredTurn string
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$2),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND model_request_id=$3 AND payload_json::jsonb->>'is_error'='false'),(SELECT sequence FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND model_request_id=$3),(SELECT sequence FROM session_events WHERE session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$2),(SELECT event_id FROM session_events WHERE session_id=$1 AND type='session.status_running' ORDER BY sequence DESC LIMIT 1)`, session, toolID, originalProvider.ModelRequestID).Scan(&results, &errors, &ends, &endSequence, &resultSequence, &recoveredTurn); err != nil || results != 1 || errors != 0 || ends != 1 || endSequence >= resultSequence || recoveredTurn != originalTurnID || external.calls.Load() != 1 {
			t.Fatalf("original restart facts results=%d errors=%d End=%d order=%d/%d turn=%s/%s external=%d err=%v", results, errors, ends, endSequence, resultSequence, recoveredTurn, originalTurnID, external.calls.Load(), err)
		}
		retainedInput := readHandoffPendingInput(t, admin, session)
		if retainedInput != originalInput || retainedInput.InboxStatus != "committed" || retainedInput.QueueStatus != queue.StatusAcknowledged {
			t.Fatalf("original input custody changed before=%+v after=%+v", originalInput, retainedInput)
		}
		second.signal(t, session+"-1.release")
		waitHandoffCondition(t, "same Pod successor successful End and durable idle", func() bool {
			var idle, successful, failed int
			_ = admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true')`, session).Scan(&idle, &successful, &failed)
			return idle == 1 && successful == 2 && failed == 0
		})
		var inputs, originalInputs, userMessages, captures, originalAdoptedCaptures int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1),(SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND runtime_input_id=$2 AND status='committed'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='user.message'),(SELECT count(*) FROM sandbox_output_capture_operations WHERE session_id=$1),(SELECT count(*) FROM sandbox_output_capture_operations WHERE session_id=$1 AND finish_idle_write_id=$3 AND capture_generation=1 AND state='adopted')`, session, originalInput.InputID, originalTurnID).Scan(&inputs, &originalInputs, &userMessages, &captures, &originalAdoptedCaptures); err != nil || inputs != 1 || originalInputs != 1 || userMessages != 1 || captures != 1 || originalAdoptedCaptures != 1 {
			t.Fatalf("same Pod original-work census inputs=%d original=%d userMessages=%d captures=%d originalAdopted=%d err=%v", inputs, originalInputs, userMessages, captures, originalAdoptedCaptures, err)
		}
		second.signal(t, "quiesce")
		second.join(t)
	})
}

type handoffSandboxProvider struct {
	*bridgeMemoryProjectionProvider
	calls, observations atomic.Int32
	release             chan struct{}
	done                chan error
	once                sync.Once
}

func (p *handoffSandboxProvider) finish() { p.once.Do(func() { close(p.release) }) }
func (*handoffSandboxProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{Value: tetralsandbox.ToolPreparationResult{}}
}
func (p *handoffSandboxProvider) ExecuteTool(_ context.Context, request tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.calls.Add(1)
	target := request.Invocation.Target
	target.ProviderSandboxID = request.Handle.SandboxID
	observation := &sandboxdriver.ForegroundCommandObservation{Reference: sandboxdriver.CommandReference{Target: target, Task: sandboxdriver.BackgroundTask{TaskID: "held-handoff-command", ProviderSessionID: "provider-held", ProviderCommandID: "command-held"}, ToolUseEventID: request.Invocation.ToolUseEventID}}
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ForegroundObservation: observation}}
}
func (p *handoffSandboxProvider) ObserveTool(ctx context.Context, _ sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.observations.Add(1)
	select {
	case <-p.release:
		return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"completed","stdout":{"text":"original held execution","truncated":false},"stderr":{"text":"","truncated":false}}`}}
	case <-ctx.Done():
		return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Disposition: tetralsandbox.ProviderRetryable, ErrorKind: "fixture_cancelled", SafeMessage: "fixture stopped"}
	}
}
func startHandoffSandboxOwner(t *testing.T, db *sql.DB, sessionID string) *handoffSandboxProvider {
	t.Helper()
	client := dbconnect.NewClientForTesting(db)
	store := queue.NewPostgreSQLStore(client)
	queueServer := tetralqueue.NewServer(store, nil)
	response, err := queueServer.Lease(context.Background(), &queuev1.LeaseRequest{WorkspaceId: "default", Kinds: []string{queue.KindSandboxToolExecute}, LeaseOwner: "held-execution-owner", MaxJobs: 1, LeaseDurationMs: time.Minute.Milliseconds()})
	leases := response.GetJobs()
	if err != nil || len(leases) != 1 {
		t.Fatalf("held execution lease=%v/%v", leases, err)
	}
	provider := &handoffSandboxProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, release: make(chan struct{}), done: make(chan error, 1)}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{Queue: &issuedLeaseQueueFixture{QueueClient: queueServer, job: leases[0]}, Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute), Providers: registry, Media: backgroundNotificationMedia{}, Config: tetralsandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: "default", LeaseOwner: "held-execution-owner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PreparationTimeout: time.Second}}
	go func() { provider.done <- runner.RunOnce(ctx); close(provider.done) }()
	t.Cleanup(func() {
		provider.finish()
		cancel()
		select {
		case <-provider.done:
		case <-time.After(5 * time.Second):
			t.Error("external Sandbox fixture did not join")
		}
	})
	return provider
}

func startHandoffResultListener(t *testing.T, store *bridge.PostgreSQLBridgeAPIStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan error, 1)
	go func() { joined <- store.RunExecutionResultListener(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("handoff Bridge result listener did not join")
		}
	})
}

func readHandoffDiagnostics(t *testing.T, child *handoffRuntimeChild) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(child.directory, "diagnostics.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func startHandoffOutputCaptures(t *testing.T, runtimeDB *sql.DB) {
	startHandoffOutputCapturesWithProvider(t, runtimeDB, handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}})
}

func startHandoffOutputCapturesWithProvider(t *testing.T, runtimeDB *sql.DB, provider tetralsandbox.ProviderAdapter) {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtimeDB)
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": provider})
	if err != nil {
		t.Fatal(err)
	}
	runner := &tetralsandbox.SandboxOutputCaptureJobRunner{Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Store: tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(client), Providers: registry, BlobStore: blob.NewFakeBlobStore(), Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{WorkspaceID: "default", LeaseOwner: "reviewer-handoff-capture", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan error, 1)
	go func() {
		for {
			_, err := runner.RunOnceWithActivity(ctx)
			if err != nil || ctx.Err() != nil {
				if ctx.Err() != nil {
					err = nil
				}
				joined <- err
				return
			}
			select {
			case <-ctx.Done():
				joined <- nil
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-joined:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("reviewer capture worker did not join")
		}
	})
}

// An empty successful scan still crosses the real capture/adoption transaction.
type handoffCaptureProvider struct {
	*bridgeMemoryProjectionProvider
}

func (handoffCaptureProvider) CaptureOutputs(context.Context, sandboxdriver.OutputCaptureTarget) tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Value: sandboxdriver.OutputCaptureScan{}}
}
