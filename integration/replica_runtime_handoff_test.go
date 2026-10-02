package integration

import (
	"context"
	"database/sql"
	"encoding/json"
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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
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

func startHandoffRuntimeChild(t *testing.T, address, podUID, processID, token string, replacement bool, idle []map[string]any, review ...string) *handoffRuntimeChild {
	t.Helper()
	child := &handoffRuntimeChild{directory: t.TempDir(), done: make(chan struct{})}
	scenario := ""
	if len(review) > 0 {
		scenario = review[0]
	}
	params := map[string]any{"bridgeAddress": address, "podUID": podUID, "processID": processID, "token": token, "directory": child.directory, "replacement": replacement, "idle": idle}
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
func appendHandoffMessage(t *testing.T, client *dbconnect.Client, sessionID, key string) {
	t.Helper()
	service := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "continue " + key}}}}})
	if err != nil || len(result.Data) != 1 {
		t.Fatalf("durable producer message %v/%v", result, err)
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
	delivery := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, child.port)
	targetURL, err := url.Parse(child.httpURL)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, targetURL.Host)
	}}
	defer transport.CloseIdleConnections()
	delivery.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
		uid := podUID
		if name == "runtime-pod-0" {
			uid = "pod_old"
		}
		return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: uid, IP: "127.0.0.1", Running: true, Ready: true}, nil
	}, LoadClient: &http.Client{Transport: transport}, Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-new", PodUID: podUID, PodIP: "127.0.0.1"}, {Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_old", PodIP: "127.0.0.1"}})
	}}
	runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: jobrunner.NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}}
	for _, lease := range leases {
		if err := runIssuedLeaseThroughRunner(context.Background(), runner, queueJobProto(lease), jobrunner.JobRunnerConfig{LeaseOwner: "handoff-runner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgreSQLReplicaRuntimeHandoff(t *testing.T) {
	t.Run("independent current step and successor process", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		client := dbconnect.NewClientForTesting(runtimeDB)
		store := bridge.NewPostgreSQLBridgeAPIStore(client)
		store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
		startHandoffResultListener(t, store)
		sessions := []string{"sesn_replica_a1", "sesn_replica_a2", "sesn_replica_c", "sesn_replica_b"}
		for _, session := range sessions {
			seedBridgeAPISession(t, admin, "default", session, "thr_"+session)
			seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_"+session, 1, "pod_old")
			seedRuntimePodLostStatusFence(t, admin, session, "bind_"+session, 1)
		}
		if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'`); err != nil {
			t.Fatal(err)
		}
		var lost atomic.Bool
		a := serveReplicaBridge(t, store, map[string]string{"old": "pod_old", "new": "pod_new", "third": "pod_third"}, func(_ context.Context, method string, _ any) error {
			if method == bridgev1.AgentRuntimeBridgeService_ReleaseRuntimeBinding_FullMethodName && lost.CompareAndSwap(false, true) {
				return status.Error(codes.Unavailable, "committed release response lost")
			}
			return nil
		})
		old := startHandoffRuntimeChild(t, a.Address, "pod_old", "process_pod_old", "old", false, []map[string]any{{"sessionId": sessions[2], "sessionThreadId": "thr_" + sessions[2], "bindingId": "bind_" + sessions[2], "bindingGeneration": 1}})
		replacement := startHandoffRuntimeChild(t, a.Address, "pod_new", "process_pod_new", "new", true, nil)
		for _, session := range sessions[:2] {
			appendHandoffMessage(t, client, session, "initial_"+session)
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
			waitHandoffCondition(t, "held current frame "+session, func() bool { return old.calls(session) == 1 })
		}
		seedReadySandboxForSharedToolExecution(t, admin, "default", sessions[3])
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
		waitHandoffCondition(t, "idle Session independent release", func() bool { return handoffBindingCount(t, admin, sessions[2]) == 0 })
		if old.calls(sessions[0]) != 1 || old.calls(sessions[1]) != 1 || replacement.calls(sessions[2]) != 0 {
			t.Fatal("idle release crossed an admitted provider step")
		}
		waitHandoffCondition(t, "B releases while A1 and A2 held", func() bool { return handoffBindingCount(t, admin, sessions[3]) == 0 })
		deliverHandoffRecovery(t, runtimeDB, replacement, "pod_new")
		if replacement.calls(sessions[3]) != 0 || external.calls.Load() != 1 {
			t.Fatal("B resubmitted execution or dispatched a model before result")
		}
		old.signal(t, sessions[0]+"-1.release")
		waitHandoffCondition(t, "A1 current step release", func() bool { return handoffBindingCount(t, admin, sessions[0]) == 0 })
		deliverHandoffRecovery(t, runtimeDB, replacement, "pod_new")
		waitHandoffCondition(t, "replacement successor provider request", func() bool { return replacement.calls(sessions[0]) == 1 })
		appendHandoffMessage(t, client, sessions[0], "queued_during_quiesce")
		if old.calls(sessions[0]) != 1 || old.calls(sessions[1]) != 1 {
			t.Fatal("successor dispatched on retiring Runtime or A2 was interrupted")
		}
		old.signal(t, sessions[1]+"-1.release")
		old.join(t)
		if replacement.calls(sessions[0]) != 1 {
			t.Fatal("old process exit failed while replacement work remained held")
		}
		var errors, ends, results, inbox int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id IN ('sesn_replica_a1','sesn_replica_a2') AND type IN ('session.error','session.runtime_pod_lost')),(SELECT count(*) FROM session_events WHERE session_id IN ('sesn_replica_a1','sesn_replica_a2') AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id='sesn_replica_a1' AND type='agent.tool_result'),(SELECT count(*) FROM session_runtime_inbox WHERE session_id='sesn_replica_a1' AND status='queued')`).Scan(&errors, &ends, &results, &inbox); err != nil {
			t.Fatal(err)
		}
		if errors != 0 || ends != 2 || results != 1 || inbox != 1 {
			t.Fatalf("checkpoint facts errors=%d ends=%d toolresults=%d queued=%d", errors, ends, results, inbox)
		}
		if !lost.Load() {
			t.Fatal("release response-loss barrier was not exercised")
		}
		replacement.signal(t, "quiesce")
		waitHandoffCondition(t, "replacement quiesce before response", func() bool {
			_, err := os.Stat(filepath.Join(replacement.directory, "quiescing.json"))
			return err == nil
		})
		replacement.signal(t, sessions[0]+"-1.release")
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
	})

	t.Run("queued business input before checkpoint retains original continuation", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		client := dbconnect.NewClientForTesting(runtimeDB)
		const session = "sesn_queued_a1"
		const thread = "thr_queued_a1"
		seedBridgeAPISession(t, admin, "default", session, thread)
		seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_queued", 1, "pod_old")
		seedRuntimePodLostStatusFence(t, admin, session, "bind_queued", 1)
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
	for _, scenario := range []string{"hold", "allow", "deny"} {
		t.Run("admitted reviewer dependency "+scenario, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			client := dbconnect.NewClientForTesting(runtimeDB)
			session, thread := "sesn_review_"+scenario, "thr_review_"+scenario
			seedBridgeAPISession(t, admin, "default", session, thread)
			seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_review", 1, "pod_old")
			seedRuntimePodLostStatusFence(t, admin, session, "bind_review", 1)
			if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'`); err != nil {
				t.Fatal(err)
			}
			seedReadySandboxForSharedToolExecution(t, admin, "default", session)
			startHandoffOutputCaptures(t, runtimeDB)
			store := bridge.NewPostgreSQLBridgeAPIStore(client)
			store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
			startHandoffResultListener(t, store)
			a := serveReplicaBridge(t, store, map[string]string{"old": "pod_old"}, nil)
			old := startHandoffRuntimeChild(t, a.Address, "pod_old", "process_pod_old", "old", false, nil, scenario)
			appendHandoffMessage(t, client, session, "review_admitted_step")
			deliverAttachmentRuntimeInput(t, runtimeDB, admin, old.port, session, "runtime-pod-0", "pod_old")
			waitHandoffCondition(t, "parent admitted provider", func() bool { return old.calls(session) == 1 })
			old.signal(t, "quiesce")
			waitHandoffCondition(t, "reviewer parent quiesce fence", func() bool { _, err := os.Stat(filepath.Join(old.directory, "quiescing.json")); return err == nil })
			old.signal(t, session+"-1.release")
			waitHandoffCondition(t, "reviewer starts after quiesce", func() bool {
				select {
				case <-old.done:
					out, _ := os.ReadFile(filepath.Join(old.directory, "output.log"))
					diag, _ := os.ReadFile(filepath.Join(old.directory, "diagnostics.jsonl"))
					t.Fatalf("reviewer child exited calls=%d error=%v output=%s diagnostics=%s", old.calls(session), old.err, out, diag)
				default:
				}
				return old.calls(session) == 2
			})
			if scenario != "hold" {
				waitHandoffCondition(t, "reviewer Read accepted", func() bool {
					var n int
					_ = admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_name='Read'`, session).Scan(&n)
					return n == 1
				})
				external := startHandoffSandboxOwner(t, runtimeDB, session)
				waitHandoffCondition(t, "reviewer Read command owned", func() bool { return external.observations.Load() > 0 })
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
			old.join(t)
			var declarations, executions, approvals, failedEnds int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND session_thread_id=$2 AND type='agent.tool_use'),(SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_name='Write'),(SELECT count(*) FROM session_pending_tool_uses WHERE session_id=$1 AND status='pending'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true')`, session, thread).Scan(&declarations, &executions, &approvals, &failedEnds); err != nil {
				t.Fatal(err)
			}
			if scenario == "hold" {
				if declarations != 0 || executions != 0 || approvals != 0 || failedEnds != 2 {
					t.Fatalf("held reviewer manufactured outcome declarations=%d executions=%d approvals=%d failedEnds=%d", declarations, executions, approvals, failedEnds)
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
			if scenario == "hold" {
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
		})
	}
	for _, mode := range []string{"silent", "throw"} {
		t.Run("handoff diagnostic sink "+mode, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			session, thread := "sesn_sink_"+mode, "thr_sink_"+mode
			seedBridgeAPISession(t, admin, "default", session, thread)
			seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_sink", 1, "pod_old")
			seedRuntimePodLostStatusFence(t, admin, session, "bind_sink", 1)
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
	t.Helper()
	client := dbconnect.NewClientForTesting(runtimeDB)
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}})
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
