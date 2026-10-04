package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// Every cut is demonstrated before an explicit process kill. Provider input,
// command completion and discovery are controlled external ports; declarations,
// process registration, custody repair, cold load and settlement remain real.
func TestContentProcessRecovery(t *testing.T) {
	requireContentLifecycleDependencies(t, false)
	for _, spec := range []contentCrashCase{
		{name: "gateway-partial-output", scenario: "text-large", boundary: "none", killGateway: true},
		{name: "frame-received-before-write", scenario: "text", boundary: "frame-before-write"},
		{name: "committed-before-application-no-tool", scenario: "text", boundary: "commit-before-apply", committed: true},
		{name: "committed-before-application-with-tool", scenario: "tool-before-text", boundary: "commit-before-apply", committed: true, tool: true},
		{name: "reasoning-staged", scenario: "reasoning-only", boundary: "reasoning-staged"},
		{name: "end-with-accepted-tool", scenario: "durable-interleaved", boundary: "none", committed: true, tool: true, ended: true},
	} {
		t.Run(spec.name, func(t *testing.T) { runContentProcessRecovery(t, spec) })
	}
}

type contentCrashCase struct {
	name, scenario, boundary            string
	killGateway, committed, tool, ended bool
}

type contentCrashRuntime struct {
	*contentRuntimeChild
	httpURL string
}

func startContentCrashRuntime(t *testing.T, address, gateway, pod, process, token, boundary string) *contentCrashRuntime {
	t.Helper()
	child := &contentRuntimeChild{directory: t.TempDir(), joined: make(chan error, 1)}
	input := map[string]any{"bridgeAddress": address, "gatewayAddress": gateway, "podUID": pod, "processID": process, "token": token, "directory": child.directory, "boundary": boundary}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(child.directory, "input.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	child.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/content-process-runtime.ts", path) //nolint:gosec // Fixed repository fixture and private input.
	child.command.Dir = "../services/agent-runtime"
	child.command.Stdout, child.command.Stderr = &child.output, &child.output
	if err := child.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { child.joined <- child.command.Wait() }()
	t.Cleanup(func() { child.stop(t) })
	ready := child.marker(t, "ready")
	result := &contentCrashRuntime{contentRuntimeChild: child}
	var pid int
	if json.Unmarshal(ready["port"], &child.port) != nil || child.port <= 0 || json.Unmarshal(ready["httpUrl"], &result.httpURL) != nil || json.Unmarshal(ready["pid"], &pid) != nil || pid != child.command.Process.Pid {
		t.Fatal("Runtime ready identity/listeners differ from the actual child")
	}
	return result
}

func (c *contentRuntimeChild) killAtContentBoundary(t *testing.T) {
	t.Helper()
	if c.stopped {
		t.Fatal("crash target already stopped")
	}
	c.stopped = true
	pid := c.command.Process.Pid
	if err := c.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-c.joined:
		if err == nil {
			t.Fatal("explicit Runtime crash unexpectedly exited successfully")
		}
		t.Logf("explicit Runtime crash pid=%d joined=true", pid)
	case <-time.After(10 * time.Second):
		t.Fatal("explicit Runtime crash did not join")
	}
}

func (c *contentGatewayChild) killAtContentBoundary(t *testing.T) {
	t.Helper()
	if c.stopped {
		t.Fatal("Gateway crash target already stopped")
	}
	c.stopped = true
	defer func() { _ = c.input.Close() }()
	pid := c.command.Process.Pid
	if err := c.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-c.joined:
		if err == nil {
			t.Fatal("explicit Gateway crash unexpectedly exited successfully")
		}
		t.Logf("explicit Gateway crash pid=%d joined=true", pid)
	case <-time.After(10 * time.Second):
		t.Fatal("explicit Gateway crash did not join")
	}
}

func runContentProcessRecovery(t *testing.T, spec contentCrashCase) {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workloads := storagetest.OpenWorkloadDB(t, admin, "bridge")
	bridgeDB, queueDB := workloads.DB, workloads.OpenWorkload(t, "queue", nil)
	apiDB, sandboxDB := workloads.OpenWorkload(t, "api", nil), workloads.OpenWorkload(t, "sandbox", nil)
	runnerDB := workloads.OpenWorkload(t, "job_runner", nil)
	session, thread, binding, pod := id.New("sesn_"), id.New("thr_"), id.New("bind_"), id.New("pod_")
	seedBridgeAPISession(t, admin, "default", session, thread)
	// Consume the same generation sequence as placement so the seeded owner
	// cannot collide with the first real successor's generation.
	var originalGeneration int64
	if err := admin.QueryRow(`SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&originalGeneration); err != nil {
		t.Fatal(err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, originalGeneration, pod)
	seedRuntimePodLostStatusFence(t, admin, session, binding, originalGeneration)
	seedBridgeAPIAgentConfig(t, admin, "default", session, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}],"skills":[],"metadata":{}}`)
	seedReadySandboxForSharedToolExecution(t, admin, "default", session)
	if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1',agent_runtime_pod_name='content-runtime' WHERE session_id=$1`, session); err != nil {
		t.Fatal(err)
	}
	var objectAccess atomic.Int64
	denyObject := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		objectAccess.Add(1)
		http.Error(w, "unexpected object access", http.StatusInternalServerError)
	}))
	t.Cleanup(denyObject.Close)
	objects, err := blob.NewS3BlobStore(context.Background(), &blob.Config{Endpoint: denyObject.URL, Region: "us-east-1", Bucket: "content-crash-unused", AccessKey: "fixture-key", SecretKey: "fixture-secret", AllowInsecure: true, LocalTestMode: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	const key = "content-lifecycle-test-binding-key-32"
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(bridgeDB))
	store.RuntimeBindingTokenHMACKey, store.AttachmentBlobStore, store.FileBlobStore = []byte(key), objects, objects
	startHandoffResultListener(t, store)
	endpoint := serveContentBridge(t, store, map[string]string{"content-old": pod, "content-new": pod}, nil)
	gatewayOptions := map[string]any{"scenario": spec.scenario, "followupScenario": "done", "bindingKey": key, "runtimePodUid": pod, "recordContext": true}
	if spec.killGateway {
		gatewayOptions["measureResources"] = true
		gatewayOptions["textCodeUnits"], gatewayOptions["fragmentCodeUnits"] = 8192, 2048
		gatewayOptions["concurrentProviderBarrier"] = map[string]any{"count": 1, "retainedBytesPerRequest": 4096}
	}
	gateway := startContentGatewayChild(t, gatewayOptions)
	first := startContentCrashRuntime(t, endpoint.Address, gateway.address, pod, "process_"+pod, "content-old", spec.boundary)
	queueServer := tetralqueue.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueDB)), nil)
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}})
	if err != nil {
		t.Fatal(err)
	}
	captures := &tetralsandbox.SandboxOutputCaptureJobRunner{Queue: queueServer, Store: tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(sandboxDB)), Providers: registry, BlobStore: objects, Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-crash-capture", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
	startContentWorker(t, captures.RunOnceWithActivity, "crash-capture")
	appendContentCrashInput(t, apiDB, session, "start-content-fixture")
	deliverContentRuntimeInput(t, runnerDB, queueServer, first.port, session, pod)
	if spec.killGateway {
		waitContentGatewayPartial(t, gateway)
		assertContentCrashMessageCount(t, admin, session, 0)
		crashed := time.Now()
		gateway.killAtContentBoundary(t)
		waitContentSQLCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'`, session, 1)
		assertContentCrashMessageCount(t, admin, session, 0)
		assertContentCrashHealthy(t, first.httpURL)
		if objectAccess.Load() != 0 {
			t.Fatal("partial Gateway loss unexpectedly used object storage")
		}
		t.Logf("Gateway-loss detection and failed End elapsed=%s; uncommitted text absent", time.Since(crashed))
		first.stop(t)
		return
	}
	var boundary map[string]json.RawMessage
	if spec.boundary != "none" {
		boundary = first.marker(t, "crash-boundary")
	}
	var external *handoffSandboxProvider
	var originalToolID string
	if spec.tool {
		toolDeclaration := first.marker(t, "tool-declared")
		if json.Unmarshal(toolDeclaration["eventId"], &originalToolID) != nil || originalToolID == "" {
			t.Fatal("actual Tool declaration has no durable identity")
		}
		waitContentSQLCount(t, admin, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1`, session, 1)
		external = &handoffSandboxProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, release: make(chan struct{})}
		t.Cleanup(external.finish)
		providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": external})
		if err != nil {
			t.Fatal(err)
		}
		runner := &tetralsandbox.SandboxToolExecutionJobRunner{Queue: queueServer, Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(sandboxDB), 30*time.Minute), Providers: providers, Media: tetralsandbox.NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(sandboxDB), objects), Config: tetralsandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-crash-execution", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PreparationTimeout: time.Second}}
		startContentWorker(t, runner.RunOnceWithActivity, "crash-execution")
		waitHandoffCondition(t, "actual held command stored and observed", func() bool { return external.calls.Load() == 1 && external.observations.Load() > 0 })
	}
	if spec.ended {
		boundary = first.marker(t, "request-end-ack")
	}
	var originalRequest, committedID string
	if json.Unmarshal(boundary["modelRequestId"], &originalRequest) != nil || originalRequest == "" {
		t.Fatal("crash boundary has no original request")
	}
	if spec.committed && !spec.ended {
		if json.Unmarshal(boundary["eventId"], &committedID) != nil || committedID == "" {
			t.Fatal("committed boundary has no original event")
		}
	}
	before := readContentCrashFacts(t, admin, session, originalRequest)
	wantTexts := 0
	if spec.committed {
		wantTexts = 1
	}
	if spec.ended {
		wantTexts = 2
	}
	if len(before.Texts) != wantTexts || before.Ends != contentCrashBoolInt(spec.ended) {
		t.Fatalf("unproved crash cut: %+v", before)
	}
	if committedID != "" && (before.Texts[0].ID != committedID || before.Texts[0].Text != "alpha") {
		t.Fatal("ACK identity is not the independently read committed alpha")
	}
	if spec.boundary == "reasoning-staged" && (before.Assistant != "" || before.Thinking != 1) {
		t.Fatal("staged reasoning must have one thinking event and no durable Assistant")
	}
	crashed := time.Now()
	assertContentCrashProviderCalls(t, gateway, 1)
	first.killAtContentBoundary(t)
	second := startContentCrashRuntime(t, endpoint.Address, gateway.address, pod, "recovered_"+pod, "content-new", "none")
	delivery := contentCrashDeliveryStore(t, runnerDB, second, pod)
	observeContentCrashLoad(t, second)
	if repaired, err := delivery.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 1 {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) {
			t.Logf("repair database cause code=%s table=%s constraint=%s routine=%s usage_table_permission_denied=%t", pgError.Code, pgError.TableName, pgError.ConstraintName, pgError.Routine, pgError.Code == "42501" && pgError.Message == "permission denied for table request_usage_details")
		}
		t.Fatalf("actual superseded-process repair=%d/%v", repaired, err)
	}
	detected := time.Since(crashed)
	assertContentCrashProviderCalls(t, gateway, 1)
	if after := readContentCrashFacts(t, admin, session, originalRequest); !reflect.DeepEqual(before.Texts, after.Texts) || before.Assistant != after.Assistant {
		t.Fatalf("repair changed original committed content: before=%+v after=%+v", before, after)
	}
	var idleBeforeRecovery int
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, session).Scan(&idleBeforeRecovery); err != nil {
		t.Fatal(err)
	}
	if spec.tool {
		deliverContentCrashJob(t, queueDB, delivery, second.port, queue.KindRuntimeRecovery)
	} else {
		// The original failed request has no resumable Tool owner. A fresh user
		// input obtains new custody and demonstrates normal cold admission.
		appendContentCrashInput(t, apiDB, session, "after-crash")
		deliverContentCrashJob(t, queueDB, delivery, second.port, queue.KindRuntimeInput)
	}
	loaded := second.marker(t, "cold-context")
	var cold struct {
		Messages []struct {
			MessageSequence int64            `json:"messageSequence"`
			ContextKind     string           `json:"contextKind"`
			Parts           []map[string]any `json:"parts"`
		} `json:"messages"`
	}
	raw, _ := json.Marshal(loaded)
	if err := json.Unmarshal(raw, &cold); err != nil {
		t.Fatal(err)
	}
	var assistants []map[string]any
	for _, message := range cold.Messages {
		if message.ContextKind == "assistant" {
			assistants = append(assistants, message.Parts...)
		}
	}
	if spec.tool {
		var original struct {
			Parts []map[string]any `json:"parts"`
		}
		if json.Unmarshal([]byte(before.Assistant), &original) != nil || !reflect.DeepEqual(assistants, original.Parts) {
			t.Fatalf("actual cold load changed owning Assistant: %s", raw)
		}
		var reference struct {
			ModelRequestID           string `json:"modelRequestId"`
			AssistantMessageSequence int64  `json:"assistantMessageSequence"`
		}
		var accepted int
		if json.Unmarshal(loaded["currentRequestMessage"], &reference) != nil || reference.ModelRequestID != originalRequest || reference.AssistantMessageSequence != before.AssistantSequence || json.Unmarshal(loaded["pendingSandboxExecutionCount"], &accepted) != nil || accepted != 1 {
			t.Fatalf("cold recovery lost original accepted Tool association: %s", raw)
		}
		if external.calls.Load() != 1 {
			t.Fatal("recovery duplicated external command")
		}
		assertContentCrashProviderCalls(t, gateway, 1)
		external.finish()
		waitContentSQLCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, session, 1)
		var resultOwner string
		if err := admin.QueryRow(`SELECT payload_json::jsonb->>'tool_use_id' FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, session).Scan(&resultOwner); err != nil || resultOwner != originalToolID {
			t.Fatalf("recovered result changed original Tool identity: %s/%v", resultOwner, err)
		}
		if external.calls.Load() != 1 {
			t.Fatal("settlement duplicated external command")
		}
	} else if len(assistants) != 0 {
		t.Fatalf("failed request without retained Tool became cold provider content: %s", raw)
	}
	var newBinding, newProcess string
	var generation int64
	if err := admin.QueryRow(`SELECT binding_id,binding_generation,runtime_process_id FROM session_runtime_bindings WHERE session_id=$1`, session).Scan(&newBinding, &generation, &newProcess); err != nil || newBinding == binding || generation <= originalGeneration || newProcess != "recovered_"+pod {
		t.Fatalf("recovery lacks actual fresh custody: %s/%d/%s/%v", newBinding, generation, newProcess, err)
	}
	assertContentCrashHealthy(t, second.httpURL)
	if !spec.tool || spec.ended {
		// Repair may already have emitted idle for the failed original turn.
		// Require the new turn to finish before inspecting its provider effects.
		waitHandoffCondition(t, "new post-crash model request completed", func() bool {
			var completed int
			err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id<>$2 AND type='span.model_request_end' AND COALESCE((payload_json::jsonb->>'is_error')::boolean,false)=false`, session, originalRequest).Scan(&completed)
			if err != nil {
				t.Fatal(err)
			}
			return completed == 1
		})
		waitContentSQLCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, session, idleBeforeRecovery+1)
		assertContentCrashProviderCalls(t, gateway, 2)
	}
	if after := readContentCrashFacts(t, admin, session, originalRequest); !reflect.DeepEqual(before.Texts, after.Texts) {
		t.Fatal("post-recovery content identity changed")
	}
	if objectAccess.Load() != 0 {
		t.Fatal("content crash fixture unexpectedly used object storage")
	}
	t.Logf("crash=%s oldProcess=%s newProcess=%s oldBinding=%s newBinding=%s generation=%d detection=%s recovery_and_settlement=%s", spec.name, "process_"+pod, newProcess, binding, newBinding, generation, detected, time.Since(crashed)-detected)
	second.stop(t)
	gateway.stop(t)
}

func observeContentCrashLoad(t *testing.T, child *contentCrashRuntime) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(strings.TrimRight(child.httpURL, "/") + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Runtime metrics HTTP status=%d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 256*1024+1))
	if err != nil || len(body) > 256*1024 {
		t.Fatal("Runtime metrics response unavailable or oversized")
	}
	report, parseErr := jobrunner.ParseRuntimeLoadReport(string(body), .8)
	if parseErr != nil || report.MemoryUsage != 100 || report.MemoryLimit != 1000 || report.ActiveSessions != 0 {
		t.Fatalf("replacement Runtime must advertise actual ready capacity with fixture container observation: %+v/%v", report, parseErr)
	}
	t.Logf("new Runtime actual load report=%+v parse=%v", report, parseErr)
}

func appendContentCrashInput(t *testing.T, db *sql.DB, session, text string) {
	t.Helper()
	result, err := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))).AppendClientEvents(context.Background(), "default", session, id.New("input_"), sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: "text", Text: text}}}}})
	if err != nil || len(result.Data) != 1 {
		t.Fatalf("actual input birth %v/%v", result, err)
	}
}

func contentCrashDeliveryStore(t *testing.T, db *sql.DB, child *contentCrashRuntime, pod string) *jobrunner.PostgreSQLRuntimeDeliveryStore {
	t.Helper()
	target, err := url.Parse(child.httpURL)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, target.Host)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(db), child.port, jobrunner.KubernetesRuntimeTargetResolver{
		GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
			return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: pod, IP: "127.0.0.1", Running: true, Ready: true}, nil
		},
		LoadClient: &http.Client{Transport: transport},
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "content-runtime", PodUID: pod, PodIP: "127.0.0.1"}})
		},
	})
	return store
}

func deliverContentCrashJob(t *testing.T, db *sql.DB, delivery *jobrunner.PostgreSQLRuntimeDeliveryStore, port int, kind string) {
	t.Helper()
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	leases, err := store.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{kind}, LeaseOwner: "content-crash-recovery", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(leases) != 1 {
		t.Fatalf("actual %s queue custody %v/%v", kind, leases, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leases[0]))
	if err != nil {
		t.Fatal(err)
	}
	observed := &handoffObservedDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: fixtureRuntimeCommandClient(t, attachmentRuntimeTokenSource{})}}
	runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(store, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: observed}
	if err := runIssuedLeaseThroughRunner(context.Background(), runner, queueJobProto(leases[0]), jobrunner.JobRunnerConfig{LeaseOwner: "content-crash-recovery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if observed.result.Status != jobrunner.RuntimeDeliveryAccepted && observed.result.Status != jobrunner.RuntimeDeliveryDuplicate {
		t.Fatalf("actual %s delivery=%+v job=%s port=%d", kind, observed.result, job.JobID, port)
	}
	t.Logf("actual %s delivery=%s job=%s", kind, observed.result.Status, job.JobID)
}

type contentCrashFacts struct {
	Texts             []contentCrashText
	Assistant         string
	AssistantSequence int64
	Ends, Thinking    int
}
type contentCrashText struct{ ID, Text string }

func readContentCrashFacts(t *testing.T, db *sql.DB, session, request string) contentCrashFacts {
	t.Helper()
	result := contentCrashFacts{Texts: []contentCrashText{}}
	rows, err := db.Query(`SELECT event_id,payload_json::jsonb #>> '{content,0,text}' FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='agent.message' ORDER BY sequence`, session, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var text contentCrashText
		if err := rows.Scan(&text.ID, &text.Text); err != nil {
			t.Fatal(err)
		}
		result.Texts = append(result.Texts, text)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COALESCE((SELECT data_json::jsonb::text FROM session_messages WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'),''),COALESCE((SELECT sequence FROM session_messages WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'),0),(SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='agent.thinking')`, session, request).Scan(&result.Assistant, &result.AssistantSequence, &result.Ends, &result.Thinking); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertContentCrashMessageCount(t *testing.T, db *sql.DB, session string, want int) {
	t.Helper()
	var messages, assistants int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'),(SELECT count(*) FROM session_messages WHERE session_id=$1 AND kind='assistant')`, session).Scan(&messages, &assistants); err != nil || messages != want || assistants != want {
		t.Fatalf("durable content census messages=%d assistants=%d want=%d err=%v", messages, assistants, want, err)
	}
}

func waitContentGatewayPartial(t *testing.T, child *contentGatewayChild) {
	t.Helper()
	waitHandoffCondition(t, "real Gateway partial assembly", func() bool {
		observation := child.control(t, map[string]any{"kind": "observe"}, "observation")
		var barrier struct {
			Ready                            bool `json:"ready"`
			SourceArrivals, AssemblyArrivals int
		}
		if json.Unmarshal(observation["providerBarrier"], &barrier) != nil {
			return false
		}
		if !barrier.Ready || barrier.SourceArrivals != 1 || barrier.AssemblyArrivals != 1 {
			return false
		}
		var assembly struct{ RetainedBytes, OpenBlocks, Identities int }
		var writer struct{ WriteCalls, PendingCallbacks int }
		var providerCalls int
		if json.Unmarshal(observation["assembly"], &assembly) != nil || json.Unmarshal(observation["writer"], &writer) != nil || json.Unmarshal(observation["providerCalls"], &providerCalls) != nil || assembly.RetainedBytes != 4096 || assembly.OpenBlocks != 1 || assembly.Identities != 1 || writer.WriteCalls != 0 || writer.PendingCallbacks != 0 || providerCalls != 1 {
			t.Fatalf("Gateway partial boundary lacks exact retained block and zero complete writes: assembly=%+v writer=%+v calls=%d", assembly, writer, providerCalls)
		}
		t.Logf("Gateway partial boundary source=1 assembly=1 retained=4096 identities=1 complete_writes=0")
		return true
	})
}

func assertContentCrashProviderCalls(t *testing.T, child *contentGatewayChild, want int) {
	t.Helper()
	observation := child.control(t, map[string]any{"kind": "observe"}, "observation")
	var calls int
	if json.Unmarshal(observation["providerCalls"], &calls) != nil || calls != want {
		t.Fatalf("actual provider submissions=%d want=%d", calls, want)
	}
}

func assertContentCrashHealthy(t *testing.T, address string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(strings.TrimRight(address, "/") + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("recovered Runtime readiness=%d", response.StatusCode)
	}
}

func contentCrashBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
