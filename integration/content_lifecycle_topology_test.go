package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type contentE2EOptions struct {
	ExecutionWorkers      int
	Runtime, Gateway      map[string]any
	Provider              tetralsandbox.ProviderAdapter
	StopProvider          func()
	Budget                time.Duration
	ApprovalMode          string
	ACKMode, ACKEventType string
}
type contentE2E struct {
	controlMu                   sync.Mutex
	environmentID, approvalMode string
	nextSession, controlOrdinal int

	db       *sql.DB
	objects  *blob.S3BlobStore
	sdk      *contentSDKChild
	runtime  *contentRuntimeChild
	gateway  *contentGatewayChild
	provider *contentE2EProvider
	session  string
	lost     *contentLostReceiptStore
}

// Only external Pod discovery and provider/command I/O are controlled. Session
// creation, placement, admission, activation, materialization and settlement run
// through the actual owners with their installed database roles.
func startContentE2E(t *testing.T, scenario string, lost, cold bool) *contentE2E {
	return startContentE2EWithOptions(t, scenario, lost, cold, contentE2EOptions{})
}
func startContentE2EWithOptions(t *testing.T, scenario string, lost, cold bool, overrides contentE2EOptions) *contentE2E {
	t.Helper()
	budget := overrides.Budget
	if budget == 0 {
		budget = 180 * time.Second
	}
	approvalMode := overrides.ApprovalMode
	if approvalMode == "" {
		approvalMode = "full_access"
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	t.Cleanup(cancel)
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	pools := storagetest.OpenWorkloadDB(t, admin, "bridge")
	stores, _ := replicaMinIOStoresWithConfig(t)
	objects := stores()
	if err := objects.Put(ctx, "readiness", strings.NewReader("ready"), 5); err != nil {
		t.Fatal(err)
	}
	probe, err := objects.Get(ctx, "readiness")
	if err != nil {
		t.Fatal(err)
	}
	probeBytes, readErr := io.ReadAll(probe)
	closeErr := probe.Close()
	if readErr != nil || closeErr != nil || string(probeBytes) != "ready" {
		t.Fatal("MinIO readiness put/read failed")
	}
	base, key := startContentSDKPublicEdge(t, pools, objects)
	sdk := startContentSDKChildContext(ctx, t, base, key)
	env, err := environment.NewPostgreSQLEnvironmentStore(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "api", nil)), environment.WithDefaultArtifactRef("artifact_content_sdk")).Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "content-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	provision := sdk.control(t, "provision", map[string]any{"environmentId": env.ID, "agent": map[string]any{"name": "content-e2e", "model": "anthropic/claude-opus-4-8", "approval_mode": approvalMode, "tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}, "skills": []any{}, "metadata": map[string]any{}}})
	var created struct {
		Agent struct {
			ApprovalMode string `json:"approval_mode"`
			Tools        []struct {
				Family  string `json:"family"`
				Configs []struct {
					Name       string `json:"name"`
					Enabled    bool   `json:"enabled"`
					Permission struct {
						Type string `json:"type"`
					} `json:"permission_policy"`
				} `json:"configs"`
			} `json:"tools"`
		} `json:"agent"`
		Session struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	if json.Unmarshal(provision, &created) != nil || created.Session.ID == "" {
		t.Fatal("SDK provision returned no Session")
	}
	readAllowed := false
	for _, toolset := range created.Agent.Tools {
		if toolset.Family == "claude" {
			for _, config := range toolset.Configs {
				if config.Name == "read" && config.Enabled && config.Permission.Type == "always_allow" {
					readAllowed = true
				}
			}
		}
	}
	if !readAllowed || created.Agent.ApprovalMode != approvalMode {
		t.Fatal("SDK installed Agent lacks the required allow policy")
	}
	// Use the real operator key writer to exercise SQL credential selection, while
	// the provider's network port is the fixed HTTP/SSE fixture.
	keyContext, cancelKey := context.WithTimeout(ctx, 35*time.Second)
	defer cancelKey()
	command := exec.CommandContext(keyContext, "bun", "scripts/platform-key.ts", "insert", "--provider", "anthropic", "--key-id", "pfk_content_e2e", "--cache-scope", "content-e2e")
	command.Dir = "../services/gateway"
	command.Env = append(os.Environ(), "TETRAL_DATABASE_URL="+storagetest.AdminDatabaseURL(t, admin), "ENGINE_VAULT_KEY="+sdkIntegrationVaultKey)
	command.Stdin = strings.NewReader("content-fixture-provider-key")
	if err := command.Run(); err != nil {
		t.Fatal("actual platform key CLI failed")
	}
	pod := id.New("pod_")
	const bindingKey = "content-lifecycle-test-binding-key-32"
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(pools.DB))
	store.RuntimeBindingTokenHMACKey = []byte(bindingKey)
	store.AttachmentBlobStore = objects
	store.FileBlobStore = objects
	startHandoffResultListener(t, store)
	fault := &contentLostReceiptStore{BridgeAPIStore: store, enabled: lost, ackMode: overrides.ACKMode, ackEventType: overrides.ACKEventType, settlementFaults: map[string]int{}}
	endpoint := serveContentBridgeIdentities(t, fault, map[string]auth.Identity{
		"content-runtime-token": {ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: pod},
		"content-gateway-token": {ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "provider-gateway"}},
	}, nil)
	options := map[string]any{"scenario": scenario, "bindingKey": bindingKey, "runtimePodUid": pod, "recordContext": true, "databaseUrl": storagetest.RuntimeDatabaseURL(t, pools.OpenWorkload(t, "provider_gateway", nil)), "bridgeAddress": endpoint.Address, "bridgeToken": "content-gateway-token"}
	if scenario == "durable-interleaved" {
		options["followupScenario"] = "done"
		options["holdFinish"] = true
	}
	runtimeOptions := map[string]any{"observeContainerMemory": true}
	if cold {
		runtimeOptions["evictIdle"] = true
		runtimeOptions["evictIdleStopReason"] = "end_turn"
	}
	for key, value := range overrides.Gateway {
		options[key] = value
	}
	for key, value := range overrides.Runtime {
		runtimeOptions[key] = value
	}
	gateway := startContentGatewayChildContext(ctx, t, options)
	runtime := startContentRuntimeChildContext(ctx, t, endpoint.Address, gateway.address, pod, "process_"+pod, "content-runtime-token", runtimeOptions)
	ready := runtime.marker(t, "ready")
	var httpURL string
	if json.Unmarshal(ready["httpUrl"], &httpURL) != nil || httpURL == "" {
		t.Fatal("missing real Runtime load endpoint")
	}
	provider := &contentE2EProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}, path: filepath.Join(t.TempDir(), "note.txt"), release: make(chan struct{})}
	if err := os.WriteFile(provider.path, []byte("fixture-note"), 0600); err != nil {
		t.Fatal(err)
	}
	var adapter tetralsandbox.ProviderAdapter = provider
	if overrides.Provider != nil {
		adapter = overrides.Provider
	}
	providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": adapter})
	if err != nil {
		t.Fatal(err)
	}
	q := tetralqueue.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "queue", nil))), nil)
	client := dbconnect.NewClientForTesting(pools.OpenWorkload(t, "sandbox", nil))
	lifecycle := tetralsandbox.NewPostgreSQLSandboxLifecycleStore(client, sandboxmodel.NewPostgreSQLStore(client), 30*time.Minute)
	config := tetralsandbox.SandboxLifecycleRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-e2e-lifecycle", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}
	activate := &tetralsandbox.SandboxActivationJobRunner{Queue: q, Store: lifecycle, Providers: providers, Config: config}
	materialize := &tetralsandbox.SandboxMaterializationJobRunner{Queue: q, Store: lifecycle, Providers: providers, Config: config}
	capture := &tetralsandbox.SandboxOutputCaptureJobRunner{Queue: q, Store: tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(client), Providers: providers, BlobStore: objects, Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-e2e-capture", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
	execute := &tetralsandbox.SandboxToolExecutionJobRunner{Queue: q, Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute), Providers: providers, Media: tetralsandbox.NewPostgreSQLSandboxMediaMaterializer(client, objects), Config: tetralsandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: "default", LeaseOwner: "content-e2e-tool", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PreparationTimeout: time.Second}}
	delivery := contentCrashDeliveryStore(t, pools.OpenWorkload(t, "job_runner", nil), &contentCrashRuntime{contentRuntimeChild: runtime, httpURL: httpURL}, pod)
	runner := &jobrunner.JobRunner{Queue: q, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: fixtureRuntimeCommandClient(t, attachmentRuntimeTokenSource{})}, Config: jobrunner.JobRunnerConfig{LeaseOwner: "content-e2e-delivery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
	for _, worker := range []struct {
		name string
		run  func(context.Context) (bool, error)
	}{{"activation", activate.RunOnceWithActivity}, {"materialization", materialize.RunOnceWithActivity}, {"capture", capture.RunOnceWithActivity}, {"execution", execute.RunOnceWithActivity}, {"delivery", runner.RunOnceWithActivity}} {
		startContentWorkerContext(ctx, t, worker.run, worker.name)
	}
	for worker := 1; worker < overrides.ExecutionWorkers; worker++ {
		extra := *execute
		extra.Config.LeaseOwner = fmt.Sprintf("content-e2e-tool-%d", worker)
		startContentWorkerContext(ctx, t, extra.RunOnceWithActivity, extra.Config.LeaseOwner)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for name, query := range map[string]string{
			"tool_custody":      `SELECT COALESCE(jsonb_agg(jsonb_build_object('execution_state',execution_state,'tool_use_event_id',tool_use_event_id,'provider_command_reference',provider_command_reference_json,'result_json',result_json)),'[]'::jsonb)::text FROM session_runtime_tool_results WHERE session_id=$1`,
			"sandbox_lifecycle": `SELECT COALESCE(jsonb_agg(row_to_json(facts)),'[]'::jsonb)::text FROM (SELECT kind,state,error_kind,count(*) FROM sandbox_lifecycle_operations WHERE session_id=$1 GROUP BY kind,state,error_kind) facts`,
			"events":            `SELECT COALESCE(jsonb_agg(jsonb_build_object('type',type,'id',event_id,'payload',CASE WHEN type='session.error' THEN payload_json ELSE NULL END) ORDER BY sequence),'[]'::jsonb)::text FROM session_events WHERE session_id=$1`,
		} {
			var facts string
			if err := admin.QueryRow(query, created.Session.ID).Scan(&facts); err != nil {
				t.Logf("E2E %s observation error: %v", name, err)
			} else {
				t.Logf("E2E %s: %s", name, facts)
			}
		}
		if overrides.Provider == nil {
			t.Logf("E2E external command starts=%d activated=%t", provider.calls.Load(), provider.created.Load())
		}
	})
	t.Cleanup(provider.finish)
	if overrides.StopProvider != nil {
		t.Cleanup(overrides.StopProvider)
	}
	return &contentE2E{environmentID: env.ID, approvalMode: approvalMode, db: admin, objects: objects, sdk: sdk, runtime: runtime, gateway: gateway, provider: provider, session: created.Session.ID, lost: fault}
}

type contentLostReceiptStore struct {
	bridge.BridgeAPIStore
	enabled               bool
	ackMode, ackEventType string
	altered               int
	mu                    sync.Mutex
	event                 string
	attempts              int
	receipt               *bridgev1.WriteEventResponse
	original              *bridgev1.WriteEventRequest
	replayMatches         bool
	settlementFaults      map[string]int
}

func (s *contentLostReceiptStore) WriteEvent(ctx context.Context, r *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	response, err := s.BridgeAPIStore.WriteEvent(ctx, r)
	if err == nil && s.ackMode != "" && r.GetEventType() == s.ackEventType {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.event == "" {
			s.event = r.GetPreallocatedEventId()
			s.original = proto.Clone(r).(*bridgev1.WriteEventRequest)
			s.receipt = proto.Clone(response).(*bridgev1.WriteEventResponse)
		}
		if s.event != r.GetPreallocatedEventId() {
			return response, nil
		}
		s.attempts++
		if !proto.Equal(s.original, r) {
			return nil, status.Error(codes.Internal, "fixture ACK retry changed request")
		}
		if s.ackMode == "correct" {
			return response, nil
		}
		if s.ackMode == "wrong-duplicate" && s.attempts == 1 {
			return nil, status.Error(codes.Unavailable, "fixture committed response lost")
		}
		altered := proto.Clone(response).(*bridgev1.WriteEventResponse)
		const wrongID = "evt_ffffffffffffffffffffffffffffffff"
		if s.ackMode == "wrong-committed" && altered.GetCommitted() != nil {
			altered.GetCommitted().EventId = wrongID
		} else if s.ackMode == "wrong-duplicate" && altered.GetDuplicate() != nil {
			altered.GetDuplicate().EventId = wrongID
		} else {
			return nil, status.Error(codes.Internal, "fixture unexpected ACK result branch")
		}
		s.altered++
		return altered, nil
	}
	if err != nil || !s.enabled || r.GetEventType() != "agent.message" {
		return response, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.event == "" {
		s.event = r.GetPreallocatedEventId()
		s.attempts = 1
		s.receipt = response
		s.original = proto.Clone(r).(*bridgev1.WriteEventRequest)
		return nil, status.Error(codes.Unavailable, "fixture committed response lost")
	}
	if s.event == r.GetPreallocatedEventId() {
		s.attempts++
		committed, duplicate := s.receipt.GetCommitted(), response.GetDuplicate()
		s.replayMatches = proto.Equal(s.original, r) && committed != nil && duplicate != nil && committed.GetEventId() == duplicate.GetEventId() && committed.GetAssignedMessageSequence() == duplicate.GetAssignedMessageSequence()
	}
	return response, nil
}

type contentE2EProvider struct {
	handoffCaptureProvider
	path    string
	calls   atomic.Int64
	created atomic.Bool
	release chan struct{}
	once    sync.Once
}

func (p *contentE2EProvider) finish() { p.once.Do(func() { close(p.release) }) }
func (p *contentE2EProvider) InspectForExecution(_ context.Context, handle string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	value := tetralsandbox.ExecutionNeedsCreation
	if handle != "" && p.created.Load() {
		value = tetralsandbox.ExecutionReady
	}
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: value}
}
func (p *contentE2EProvider) Activate(context.Context, tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle] {
	p.created.Store(true)
	return tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle]{Value: sandboxmodel.ProviderHandle{Provider: "daytona", SandboxID: "content-e2e-external"}}
}
func (p *contentE2EProvider) MaterializeResources(_ context.Context, r tetralsandbox.MaterializationRequest) tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult] {
	// The real provider receipt includes the credential lifetime even for an
	// empty resource projection; the coordinator owns writing these facts.
	expires := time.Now().Add(time.Hour)
	r.Setup.Resources.ResourceCredExpiresAt = &expires
	r.Setup.Resources.ResourceRootsJSON = "[]"
	return tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult]{Value: tetralsandbox.MaterializationResult{Resources: r.Setup.Resources, MaterializedEnvironmentGeneration: r.TargetEnvironmentGeneration, MaterializedResourceRevision: r.TargetResourceRevision}}
}
func (p *contentE2EProvider) ExecuteTool(_ context.Context, r tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	if p.calls.Add(1) != 1 || r.Invocation.ToolName != "Read" || r.Invocation.InputJSON != `{"file_path":"/workspace/note.txt"}` || r.Invocation.ToolUseEventID == "" {
		return contentExternalToolFailure()
	}
	target := r.Invocation.Target
	target.ProviderSandboxID = r.Handle.SandboxID
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ForegroundObservation: &sandboxdriver.ForegroundCommandObservation{Reference: sandboxdriver.CommandReference{Target: target, Task: sandboxdriver.BackgroundTask{TaskID: "content-e2e-command", ProviderSessionID: "content-e2e-external", ProviderCommandID: "read-note"}, ToolUseEventID: r.Invocation.ToolUseEventID}}}}
}
func (p *contentE2EProvider) ObserveTool(ctx context.Context, _ sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	select {
	case <-p.release:
	case <-ctx.Done():
		return contentExternalToolFailure()
	}
	data, err := os.ReadFile(p.path)
	if err != nil {
		return contentExternalToolFailure()
	}
	raw, _ := json.Marshal(map[string]any{"schema_version": 1, "tool": "read", "status": "success", "truncated": false, "error": nil, "result": map[string]any{"content": string(data)}})
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: string(raw)}}
}

// The transport fault is scoped to an actual Session and occurs before the real
// settlement store is called. No business receipt or durable row is synthesized.
func (s *contentLostReceiptStore) SettleToolResult(ctx context.Context, r *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	s.mu.Lock()
	attempts, fault := s.settlementFaults[r.GetScope().GetSessionId()]
	if fault {
		s.settlementFaults[r.GetScope().GetSessionId()] = attempts + 1
	}
	s.mu.Unlock()
	if fault {
		return nil, status.Error(codes.Unavailable, "fixture settlement transport unavailable")
	}
	return s.BridgeAPIStore.SettleToolResult(ctx, r)
}
func (s *contentLostReceiptStore) faultSettlement(session string, enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if enabled {
		s.settlementFaults[session] = 0
	} else {
		delete(s.settlementFaults, session)
	}
}

func (c *contentE2E) newSession(t *testing.T) string {
	t.Helper()
	c.controlMu.Lock()
	c.nextSession++
	ordinal := c.nextSession
	c.controlMu.Unlock()
	raw := c.sdk.control(t, "provision", map[string]any{"environmentId": c.environmentID, "agent": map[string]any{"name": fmt.Sprintf("content-cycle-%d", ordinal), "model": "anthropic/claude-opus-4-8", "approval_mode": c.approvalMode, "tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}, "skills": []any{}, "metadata": map[string]any{}}})
	var value struct {
		Session struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Session.ID == "" {
		t.Fatal("resource SDK Session creation failed")
	}
	return value.Session.ID
}

func (c *contentE2E) runtimeControl(t *testing.T, operation, session string) map[string]json.RawMessage {
	t.Helper()
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.controlOrdinal++
	scope := map[string]any{"workspaceId": "default", "sessionId": session}
	var thread, binding, pod, process string
	var generation int64
	if err := c.db.QueryRow(`SELECT t.id,b.binding_id,b.binding_generation,b.agent_runtime_pod_uid,b.runtime_process_id FROM session_runtime_bindings b JOIN session_threads t ON t.workspace_id=b.workspace_id AND t.session_id=b.session_id AND t.role='main' WHERE b.session_id=$1`, session).Scan(&thread, &binding, &generation, &pod, &process); err != nil {
		t.Fatal(err)
	}
	scope["sessionThreadId"], scope["bindingId"], scope["bindingGeneration"], scope["targetPodUid"], scope["runtimeProcessId"] = thread, binding, generation, pod, process
	raw, err := json.Marshal(map[string]any{"id": c.controlOrdinal, "operation": operation, "scope": scope})
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("control-%d", c.controlOrdinal)
	temporary := filepath.Join(c.runtime.directory, name+".tmp")
	if err := os.WriteFile(temporary, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, filepath.Join(c.runtime.directory, name+".json")); err != nil {
		t.Fatal(err)
	}
	reply := c.runtime.marker(t, name+"-reply")
	var ok bool
	var id int
	if json.Unmarshal(reply["ok"], &ok) != nil || !ok || json.Unmarshal(reply["id"], &id) != nil || id != c.controlOrdinal {
		t.Fatal("Runtime fixture control rejected or mismatched")
	}
	return reply
}
