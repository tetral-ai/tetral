package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// This is the actual SDK -> Gateway -> RuntimePod -> Bridge -> Queue -> Sandbox
// lifecycle. Read needs no object bytes: the production S3 route fails on access
// and must remain unused. Attachment transport has a separate MinIO composition.
type contentToolLifecycleCase struct {
	tool, scenario, callID, input, expectedOutput, expectedFile string
	captureOutput                                               bool
	queued                                                      bool
}

var contentReadCase = contentToolLifecycleCase{
	tool: "Read", scenario: "durable-interleaved", callID: "call-read-note",
	input:          `{"file_path":"/workspace/note.txt"}`,
	expectedOutput: "status: success\ncontent:\nfixture-note", expectedFile: "fixture-note",
}

func TestPostgreSQLRuntimeToolLifecycle(t *testing.T) {
	requireContentLifecycleDependencies(t, false)
	for _, delayed := range []bool{true, false} {
		name := "before-end-control"
		if delayed {
			name = "after-end-delayed-refresh"
		}
		t.Run(name, func(t *testing.T) { runContentToolLifecycle(t, delayed, contentReadCase) })
	}
}

func TestPostgreSQLRuntimeToolLifecycleWritesAndCommands(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	for _, spec := range []contentToolLifecycleCase{
		{tool: "Write", scenario: "durable-write", callID: "call-write-note", input: `{"content":"first\n","file_path":"/workspace/note.txt"}`, expectedOutput: "{\n  \"bytes_written\": 6,\n  \"created\": false\n}", expectedFile: "first\n", captureOutput: true},
		{tool: "Bash", scenario: "durable-bash", callID: "call-bash-fixture", input: `{"command":"printf fixture-note"}`, expectedOutput: "status: success\nexit_code: 0\nstdout:\nfixture-note", expectedFile: "fixture-note", captureOutput: true},
	} {
		for _, delayed := range []bool{true, false} {
			order := "before-end-control"
			if delayed {
				order = "after-end-delayed-refresh"
			}
			t.Run(spec.tool+"/"+order, func(t *testing.T) { runContentToolLifecycle(t, delayed, spec) })
		}
	}
}

func TestPostgreSQLRuntimeQueuedWriteOwnership(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	runContentToolLifecycle(t, false, contentToolLifecycleCase{
		tool: "Write", scenario: "durable-write-queued", callID: "call-write-note",
		input:          `{"content":"first\n","file_path":"/workspace/note.txt"}`,
		expectedOutput: "{\n  \"bytes_written\": 6,\n  \"created\": false\n}", expectedFile: "second\n", captureOutput: true, queued: true,
	})
}

func runContentToolLifecycle(t *testing.T, delayed bool, spec contentToolLifecycleCase) {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workloads := storagetest.OpenWorkloadDB(t, admin, "bridge")
	bridgeDB := workloads.DB
	queueDB := workloads.OpenWorkload(t, "queue", nil)
	sandboxDB := workloads.OpenWorkload(t, "sandbox", nil)
	jobRunnerDB := workloads.OpenWorkload(t, "job_runner", nil)
	apiDB := workloads.OpenWorkload(t, "api", nil)
	sessionID, threadID, bindingID, podUID := id.New("sesn_"), id.New("thr_"), id.New("bind_"), id.New("pod_")
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}],"skills":[],"metadata":{}}`)
	seedReadySandboxForSharedToolExecution(t, admin, "default", sessionID)
	if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1',agent_runtime_pod_name='content-runtime' WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatal(err)
	}
	var objectAccess atomic.Int64
	var objects *blob.S3BlobStore
	if spec.captureOutput {
		stores, _ := replicaMinIOStoresWithConfig(t)
		objects = stores()
		const probe = "content-fixture-readiness"
		if err := objects.Put(context.Background(), probe, strings.NewReader("ready"), 5); err != nil {
			t.Fatal(err)
		}
		r, err := objects.Get(context.Background(), probe)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil || string(data) != "ready" {
			t.Fatalf("MinIO independent put/read readiness: %q/%v", data, err)
		}
	} else {
		denyObject := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			objectAccess.Add(1)
			http.Error(w, "unexpected object access", http.StatusInternalServerError)
		}))
		t.Cleanup(denyObject.Close)
		var err error
		objects, err = blob.NewS3BlobStore(context.Background(), &blob.Config{Endpoint: denyObject.URL, Region: "us-east-1", Bucket: "content-unused-read", AccessKey: "content-test-key", SecretKey: "content-test-secret", AllowInsecure: true, LocalTestMode: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = objects.Close() })
	}
	const signingKey = "content-lifecycle-test-binding-key-32"
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(bridgeDB))
	store.RuntimeBindingTokenHMACKey = []byte(signingKey)
	store.AttachmentBlobStore = objects
	store.FileBlobStore = objects
	startHandoffResultListener(t, store)
	endpoint := serveContentBridge(t, store, map[string]string{"content-runtime-token": podUID}, nil)
	gateway := startContentGatewayChild(t, map[string]any{"scenario": spec.scenario, "bindingKey": signingKey, "runtimePodUid": podUID, "holdFinish": true, "followupScenario": "done", "recordContext": true})
	runtime := startContentRuntimeChildWithOptions(t, endpoint.Address, gateway.address, podUID, "process_"+podUID, "content-runtime-token", map[string]any{"holdRefreshAfterDeclaration": delayed, "observeQueuedTools": spec.queued})
	provider := &contentToolProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}, path: filepath.Join(t.TempDir(), "note.txt"), spec: spec}
	if spec.queued {
		provider.firstWritten, provider.releaseFirst = make(chan struct{}), make(chan struct{})
		// Always release the external wait before worker cleanup, including a
		// failed ownership assertion. The production cancellation path also joins it.
		t.Cleanup(func() { provider.releaseQueuedFirst() })
	}
	if err := os.WriteFile(provider.path, []byte("fixture-note"), 0600); err != nil {
		t.Fatal(err)
	}
	providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": provider})
	if err != nil {
		t.Fatal(err)
	}
	queueServer := tetralqueue.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueDB)), nil)
	captures := &tetralsandbox.SandboxOutputCaptureJobRunner{Queue: queueServer, Store: tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(sandboxDB)), Providers: providers, BlobStore: objects, Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-capture", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
	startContentWorker(t, captures.RunOnceWithActivity, "capture")
	born, err := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(apiDB))).AppendClientEvents(context.Background(), "default", sessionID, "content-input", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: "text", Text: "start-content-fixture"}}}}})
	if err != nil || len(born.Data) != 1 {
		t.Fatalf("actual user/input birth %v/%v", born, err)
	}
	deliverContentRuntimeInput(t, jobRunnerDB, queueServer, runtime.port, sessionID, podUID)
	runtime.marker(t, "tool-declared")
	toolRunner := &tetralsandbox.SandboxToolExecutionJobRunner{Queue: queueServer, Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(sandboxDB), 30*time.Minute), Providers: providers, Media: tetralsandbox.NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(sandboxDB), objects), Config: tetralsandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-read", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PreparationTimeout: time.Second}}
	if spec.queued {
		// Two actual contenders make a missing Runtime same-path permit visible:
		// the second would reach the external adapter while the first is held.
		startContentWorker(t, toolRunner.RunOnceWithActivity, "execution-one")
		secondRunner := *toolRunner
		secondRunner.Config.LeaseOwner = "content-write-second"
		startContentWorker(t, secondRunner.RunOnceWithActivity, "execution-two")
		select {
		case <-provider.firstWritten:
		case <-time.After(20 * time.Second):
			t.Fatal("first actual Write did not reach its external completion gate")
		}
		gateway.control(t, map[string]any{"kind": "release_finish"}, "released_finish")
		assertContentQueuedOwnership(t, admin, sessionID, runtime)
		if data, err := os.ReadFile(provider.path); err != nil || string(data) != "first\n" || provider.calls.Load() != 1 {
			t.Fatalf("waiting Write overtook running Write: effect %q calls %d err %v", data, provider.calls.Load(), err)
		}
		provider.releaseQueuedFirst()
	} else if delayed {
		runtime.marker(t, "refresh-held")
		gateway.control(t, map[string]any{"kind": "release_finish"}, "released_finish")
		runtime.marker(t, "request-end-ack")
		runtime.marker(t, "request-end-projected")
		if _, err := os.Stat(filepath.Join(runtime.directory, "projection-observation-failed.json")); err == nil {
			t.Fatal("real End projection observer failed")
		}
		if provider.calls.Load() != 0 || contentTraceCount(t, runtime, "sandbox-accept-attempt") != 0 {
			t.Fatal("execution began before delayed credential refresh")
		}
		assertContentPendingAfterEnd(t, admin, sessionID)
		runtime.release(t, "release-refresh")
		startContentWorker(t, toolRunner.RunOnceWithActivity, "execution")
	} else {
		startContentWorker(t, toolRunner.RunOnceWithActivity, "execution")
		waitContentSQLCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, sessionID, 1)
		var ends int
		if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'`, sessionID).Scan(&ends); err != nil || ends != 0 {
			t.Fatalf("control result must precede provider End: %d/%v", ends, err)
		}
		gateway.control(t, map[string]any{"kind": "release_finish"}, "released_finish")
		runtime.marker(t, "request-end-ack")
		runtime.marker(t, "request-end-projected")
	}
	waitContentSQLCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, sessionID, 1)
	wantCalls := int64(1)
	if spec.queued {
		wantCalls = 2
	}
	if provider.calls.Load() != wantCalls || int64(contentTraceCount(t, runtime, "sandbox-accept-attempt")) != wantCalls {
		t.Fatalf("actual execution/accept counts %d/%d", provider.calls.Load(), contentTraceCount(t, runtime, "sandbox-accept-attempt"))
	}
	assertContentLifecycleHistory(t, admin, sessionID, spec)
	observed := gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	var requests int
	_ = json.Unmarshal(observed["providerCalls"], &requests)
	if requests != 2 {
		t.Fatalf("actual SDK requests=%d want2", requests)
	}
	assertContentNativeContext(t, observed["nativeContexts"], spec)
	if objectAccess.Load() != 0 {
		t.Fatalf("Read unexpectedly used object route %d times", objectAccess.Load())
	}
	if data, err := os.ReadFile(provider.path); err != nil || string(data) != spec.expectedFile {
		t.Fatalf("independent workspace effect %q/%v want %q", data, err, spec.expectedFile)
	}
	if spec.captureOutput {
		assertContentCapturedObject(t, admin, objects, sessionID, spec.expectedFile)
	}
	runtime.stop(t)
	gateway.stop(t)
}

func deliverContentRuntimeInput(t *testing.T, db *sql.DB, q *tetralqueue.Server, port int, sessionID, podUID string) {
	t.Helper()
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(db), port, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "content-runtime", PodUID: podUID, PodIP: "127.0.0.1"}})
	}})
	runner := &jobrunner.JobRunner{Queue: q, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: fixtureRuntimeCommandClient(t, attachmentRuntimeTokenSource{})}, Config: jobrunner.JobRunnerConfig{LeaseOwner: "content-runtime-delivery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("actual Queue/Runtime delivery active=%t err=%v", active, err)
	}
}

type contentToolProvider struct {
	spec contentToolLifecycleCase
	handoffCaptureProvider
	path                       string
	calls                      atomic.Int64
	firstWritten, releaseFirst chan struct{}
	releaseOnce                sync.Once
}

func (p *contentToolProvider) ExecuteTool(ctx context.Context, r tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	ordinal := p.calls.Add(1)
	expectedInput := p.spec.input
	if p.spec.queued && ordinal == 2 {
		expectedInput = `{"content":"second\n","file_path":"/workspace/note.txt"}`
	}
	if r.Invocation.ToolName != p.spec.tool || r.Invocation.InputJSON != expectedInput || r.Invocation.ToolUseEventID == "" || ordinal > 2 || (!p.spec.queued && ordinal != 1) {
		return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Disposition: tetralsandbox.ProviderTerminal, ErrorKind: "fixture_contract_error", SafeMessage: "unexpected fixed invocation"}
	}
	var resultValue map[string]any
	helper := "read"
	switch p.spec.tool {
	case "Read":
		data, err := os.ReadFile(p.path)
		if err != nil {
			return contentExternalToolFailure()
		}
		resultValue = map[string]any{"content": string(data)}
	case "Write":
		// The external adapter accepts only the fixed plan operation. The real
		// filesystem effect is inspected independently after durable settlement.
		content := "first\n"
		if p.spec.queued && ordinal == 2 {
			content = "second\n"
		}
		if err := os.WriteFile(p.path, []byte(content), 0600); err != nil {
			return contentExternalToolFailure()
		}
		if p.spec.queued && ordinal == 1 {
			close(p.firstWritten)
			select {
			case <-p.releaseFirst:
			case <-ctx.Done():
				return contentExternalToolFailure()
			}
		}
		helper = "write"
		resultValue = map[string]any{"created": false, "bytes_written": len(content)}
	case "Bash":
		data, err := exec.Command("printf", "fixture-note").Output() //nolint:gosec // Fixed operation; no input is executed.
		if err != nil || os.WriteFile(p.path, data, 0600) != nil {
			return contentExternalToolFailure()
		}
		helper = "exec"
		resultValue = map[string]any{"exit_code": 0, "stdout": map[string]any{"text": string(data), "truncated": false}, "stderr": map[string]any{"text": "", "truncated": false}}
	default:
		return contentExternalToolFailure()
	}
	result, _ := json.Marshal(map[string]any{"schema_version": 1, "tool": helper, "status": "success", "truncated": false, "error": nil, "result": resultValue})
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: string(result)}}
}

func (p *contentToolProvider) releaseQueuedFirst() {
	p.releaseOnce.Do(func() { close(p.releaseFirst) })
}

func contentExternalToolFailure() tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Disposition: tetralsandbox.ProviderTerminal, ErrorKind: "fixture_io_error", SafeMessage: "fixed external operation failed"}
}

func (p *contentToolProvider) CaptureOutputs(context.Context, sandboxdriver.OutputCaptureTarget) tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan] {
	if !p.spec.captureOutput {
		return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Value: sandboxdriver.OutputCaptureScan{}}
	}
	data, err := os.ReadFile(p.path)
	if err != nil {
		return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Disposition: tetralsandbox.ProviderTerminal, ErrorKind: "fixture_capture_error", SafeMessage: "fixed external capture failed"}
	}
	digest := sha256.Sum256(data)
	return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Value: sandboxdriver.OutputCaptureScan{Files: []sandboxdriver.OutputCaptureFile{{
		SourcePath: "/mnt/session/outputs/note.txt", Kind: "regular", LinkCount: 1, SizeBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", digest), MIMEType: "text/plain",
		Open: func(context.Context) (io.ReadCloser, error) { return os.Open(p.path) },
	}}}}
}

func requireContentLifecycleDependencies(t *testing.T, objects bool) {
	t.Helper()
	if os.Getenv("TETRAL_TEST_DATABASE_URL") == "" {
		t.Fatal("content lifecycle requires declared PostgreSQL")
	}
	if _, err := exec.LookPath("bun"); err != nil {
		t.Fatal("content lifecycle requires declared Bun workspaces")
	}
	if objects {
		if _, err := exec.LookPath("printf"); err != nil {
			t.Fatal("fixed Bash fixture requires printf")
		}
		for _, key := range []string{"TETRAL_TEST_MINIO_ENDPOINT", "TETRAL_TEST_MINIO_REGION", "TETRAL_TEST_MINIO_ACCESS_KEY", "TETRAL_TEST_MINIO_SECRET_KEY"} {
			if os.Getenv(key) == "" {
				t.Fatalf("content object capture requires declared %s", key)
			}
		}
	}
}

func assertContentCapturedObject(t *testing.T, db *sql.DB, objects blob.BlobStore, sessionID, expected string) {
	t.Helper()
	var effects int
	if err := db.QueryRow(`SELECT count(*) FROM sandbox_output_capture_blobs
		WHERE workspace_id='default' AND session_id=$1 AND source_path='/mnt/session/outputs/note.txt' AND state='adopted'`, sessionID).Scan(&effects); err != nil || effects != 1 {
		t.Fatalf("actual capture adoption effects %d/%v want1", effects, err)
	}
	var pointer, hash, state string
	var size int64
	if err := db.QueryRow(`SELECT o.blob_key,o.sha256,o.size_bytes,b.state
		FROM sandbox_output_capture_blobs b JOIN files f ON f.file_id=b.file_id AND f.workspace_id=b.workspace_id
		JOIN file_objects o ON o.object_id=f.object_id AND o.workspace_id=f.workspace_id
		WHERE b.workspace_id='default' AND b.session_id=$1 AND b.source_path='/mnt/session/outputs/note.txt'`, sessionID).Scan(&pointer, &hash, &size, &state); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(expected))
	if state != "adopted" || size != int64(len(expected)) || hash != fmt.Sprintf("%x", digest) {
		t.Fatalf("actual capture adoption metadata %s/%d/%s", state, size, hash)
	}
	r, err := objects.Get(context.Background(), pointer)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(data) != expected {
		t.Fatalf("actual adopted MinIO bytes %q/%v want %q", data, err, expected)
	}
}

func assertContentNativeContext(t *testing.T, raw json.RawMessage, spec contentToolLifecycleCase) {
	t.Helper()
	var contexts []struct {
		RequestOrdinal int              `json:"requestOrdinal"`
		Messages       []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(raw, &contexts); err != nil || len(contexts) != 2 || contexts[0].RequestOrdinal != 1 || contexts[1].RequestOrdinal != 2 {
		t.Fatalf("actual native SDK context observations %s/%v", raw, err)
	}
	var input map[string]any
	if err := json.Unmarshal([]byte(spec.input), &input); err != nil {
		t.Fatal(err)
	}
	// These semantic native wire blocks are fixed independently of the
	// Runtime reducer and provider lowering. Cache directives are outside this
	// bounded recorder; signatures, text boundaries and call/result identity
	// are preserved. Tool output is the existing JSON object wire contract.
	resultJSON, err := json.Marshal(map[string]any{"text": spec.expectedOutput})
	if err != nil {
		t.Fatal(err)
	}
	user := map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "start-content-fixture"}}}
	assistant := map[string]any{"role": "assistant", "content": []any{
		map[string]any{"type": "thinking", "thinking": "reason-before-text", "signature": "fixture-signature-text"},
		map[string]any{"type": "text", "text": "alpha"},
		map[string]any{"type": "text", "text": "beta"},
		map[string]any{"type": "thinking", "thinking": "reason-before-tool", "signature": "fixture-signature-tool"},
		map[string]any{"type": "tool_use", "id": spec.callID, "name": spec.tool, "input": input},
	}}
	result := map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": spec.callID, "content": string(resultJSON)}}}
	if spec.queued {
		assistant["content"] = append(assistant["content"].([]any), map[string]any{"type": "tool_use", "id": "call-write-note-2", "name": "Write", "input": map[string]any{"file_path": "/workspace/note.txt", "content": "second\n"}})
		secondResult, _ := json.Marshal(map[string]any{"text": "{\n  \"bytes_written\": 7,\n  \"created\": false\n}"})
		result["content"] = append(result["content"].([]any), map[string]any{"type": "tool_result", "tool_use_id": "call-write-note-2", "content": string(secondResult)})
	}
	if !reflect.DeepEqual(contexts[0].Messages, []map[string]any{user}) || !reflect.DeepEqual(contexts[1].Messages, []map[string]any{user, assistant, result}) {
		t.Fatalf("actual SDK next-provider literal context differs: %s", raw)
	}
}

func startContentWorker(t *testing.T, run func(context.Context) (bool, error), owner string) {
	startContentWorkerContext(context.Background(), t, run, owner)
}
func startContentWorkerContext(parent context.Context, t *testing.T, run func(context.Context) (bool, error), owner string) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	joined := make(chan error, 1)
	go func() {
		for {
			if ctx.Err() != nil {
				joined <- nil
				return
			}
			_, err := run(ctx)
			if err != nil {
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
				t.Errorf("%s worker: %v", owner, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s worker failed to join", owner)
		}
	})
}
func contentTraceCount(t *testing.T, c *contentRuntimeChild, boundary string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.directory, "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var row map[string]any
		if json.Unmarshal([]byte(line), &row) == nil && row["boundary"] == boundary {
			n++
		}
	}
	return n
}
func waitContentSQLCount(t *testing.T, db *sql.DB, query, sessionID string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var got int
		if err := db.QueryRow(query, sessionID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("durable count never reached %d", want)
}
func assertContentPendingAfterEnd(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	var ends, uses, results, executions int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_use'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'),(SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_kind='sandbox_tool')`, sessionID).Scan(&ends, &uses, &results, &executions); err != nil {
		t.Fatal(err)
	}
	if ends != 1 || uses != 1 || results != 0 || executions != 0 {
		t.Fatalf("sealed pending relation %d/%d/%d/%d", ends, uses, results, executions)
	}
}

func assertContentQueuedOwnership(t *testing.T, db *sql.DB, sessionID string, runtime *contentRuntimeChild) {
	t.Helper()
	raw := runtime.marker(t, "queued-tool-ownership")
	runtime.marker(t, "request-end-projected")
	if _, err := os.Stat(filepath.Join(runtime.directory, "projection-observation-failed.json")); err == nil {
		t.Fatal("actual queued End projection observer failed")
	}
	var reference struct {
		ModelRequestID string `json:"modelRequestId"`
		Sequence       int64  `json:"assistantMessageSequence"`
	}
	var tools []struct {
		EventID     string `json:"toolUseEventId"`
		RequestID   string `json:"modelRequestId"`
		CallID      string `json:"modelToolCallId"`
		Sequence    int64  `json:"assistantMessageSequence"`
		Disposition string `json:"disposition"`
	}
	var permits struct{ Running, Waiting int }
	if json.Unmarshal(raw["currentRequestMessage"], &reference) != nil || json.Unmarshal(raw["activeToolReferences"], &tools) != nil || json.Unmarshal(raw["sessionToolPermits"], &permits) != nil || len(tools) != 2 || permits.Running != 1 || permits.Waiting != 1 {
		t.Fatalf("actual queued ownership snapshot differs: %v", raw)
	}
	var sequence int64
	var requestID, endID string
	if err := db.QueryRow(`SELECT m.sequence,m.model_request_id,e.event_id FROM session_messages m JOIN session_events e
		ON e.workspace_id=m.workspace_id AND e.session_id=m.session_id AND e.model_request_id=m.model_request_id AND e.type='span.model_request_end'
		WHERE m.workspace_id='default' AND m.session_id=$1 AND m.kind='assistant'`, sessionID).Scan(&sequence, &requestID, &endID); err != nil {
		t.Fatal(err)
	}
	var markerRequest, markerEnd string
	_ = json.Unmarshal(raw["modelRequestId"], &markerRequest)
	_ = json.Unmarshal(raw["requestEndEventId"], &markerEnd)
	if reference.ModelRequestID != requestID || reference.Sequence != sequence || markerRequest != requestID || markerEnd != endID {
		t.Fatalf("queued reference does not match original durable sealed Assistant: %v", raw)
	}
	rows, err := db.Query(`SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use' ORDER BY sequence`, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for i, callID := range []string{"call-write-note", "call-write-note-2"} {
		var eventID string
		if !rows.Next() {
			t.Fatal("missing queued durable ToolUse")
		}
		if err := rows.Scan(&eventID); err != nil {
			t.Fatal(err)
		}
		if tools[i].EventID != eventID || tools[i].CallID != callID || tools[i].RequestID != requestID || tools[i].Sequence != sequence {
			t.Fatalf("queued tool lost original request/message relation: %v", raw)
		}
		if i == 1 {
			var accepted int
			if err := db.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2`, sessionID, eventID).Scan(&accepted); err != nil || accepted != 0 || tools[i].Disposition != "hot_execution" {
				t.Fatalf("permit-waiting Tool must retain its live preaccept route: %v accepted=%d error=%v", raw, accepted, err)
			}
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatal("unexpected queued durable ToolUse rows")
	}
	var results int
	if err := db.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, sessionID).Scan(&results); err != nil || results != 0 {
		t.Fatalf("queued results committed before first external release: %d/%v", results, err)
	}
}
func assertContentLifecycleHistory(t *testing.T, db *sql.DB, sessionID string, spec contentToolLifecycleCase) {
	t.Helper()
	rows, err := db.Query(`SELECT payload_json,event_id FROM session_events WHERE session_id=$1 AND type='agent.message' ORDER BY sequence`, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var texts, ids []string
	for rows.Next() {
		var raw, id string
		if err := rows.Scan(&raw, &id); err != nil {
			t.Fatal(err)
		}
		var event struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if json.Unmarshal([]byte(raw), &event) != nil || len(event.Content) != 1 {
			t.Fatalf("unexpected text event %s", raw)
		}
		texts = append(texts, event.Content[0].Text)
		ids = append(ids, id)
	}
	if strings.Join(texts, ",") != "alpha,beta,done" || len(ids) != 3 || ids[0] == ids[1] || ids[1] == ids[2] || ids[0] == ids[2] {
		t.Fatalf("public complete parts/identity %v/%v", texts, ids)
	}
	var messages string
	if err := db.QueryRow(`SELECT jsonb_agg(data_json::jsonb ORDER BY sequence)::text FROM session_messages WHERE session_id=$1 AND kind='assistant'`, sessionID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	var asts []struct {
		Parts []map[string]any `json:"parts"`
	}
	if err := json.Unmarshal([]byte(messages), &asts); err != nil || len(asts) != 2 {
		t.Fatalf("Assistant history %s/%v", messages, err)
	}
	wantKinds := []string{"reasoning", "text", "text", "reasoning", "tool_call", "tool_result"}
	if spec.queued {
		wantKinds = []string{"reasoning", "text", "text", "reasoning", "tool_call", "tool_call", "tool_result", "tool_result"}
	}
	if len(asts[0].Parts) != len(wantKinds) {
		t.Fatalf("original Assistant members %s", messages)
	}
	for i, k := range wantKinds {
		if asts[0].Parts[i]["type"] != k {
			t.Fatalf("Assistant member order %s", messages)
		}
	}
	var input map[string]any
	if err := json.Unmarshal([]byte(spec.input), &input); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{
		{"type": "reasoning", "text": "reason-before-text", "providerMetadata": map[string]any{"anthropic": map[string]any{"signature": "fixture-signature-text"}}},
		{"type": "text", "text": "alpha"},
		{"type": "text", "text": "beta"},
		{"type": "reasoning", "text": "reason-before-tool", "providerMetadata": map[string]any{"anthropic": map[string]any{"signature": "fixture-signature-tool"}}},
		{"type": "tool_call", "modelToolCallId": spec.callID, "toolName": spec.tool, "canonicalInput": input},
		{"type": "tool_result", "modelToolCallId": spec.callID, "result": map[string]any{"type": "completed", "output": map[string]any{"text": spec.expectedOutput}}},
	}
	if spec.queued {
		want = append(want[:5],
			map[string]any{"type": "tool_call", "modelToolCallId": "call-write-note-2", "toolName": "Write", "canonicalInput": map[string]any{"file_path": "/workspace/note.txt", "content": "second\n"}},
			want[5],
			map[string]any{"type": "tool_result", "modelToolCallId": "call-write-note-2", "result": map[string]any{"type": "completed", "output": map[string]any{"text": "{\n  \"bytes_written\": 7,\n  \"created\": false\n}"}}},
		)
	}
	if !reflect.DeepEqual(asts[0].Parts, want) || !reflect.DeepEqual(asts[1].Parts, []map[string]any{{"type": "text", "text": "done"}}) {
		t.Fatalf("durable golden relation %s", messages)
	}
}
