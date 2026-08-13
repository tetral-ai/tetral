package tetralsandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiclient "github.com/daytonaio/daytona/libs/api-client-go"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const daytonaDiskCapacityResponse = "Total disk limit exceeded. Maximum allowed: 30GiB."

func TestDaytonaAdapterExposesCreateCapacityAsProvedNotStarted(t *testing.T) {
	sdkClient := &scriptedDaytonaLifecycleClient{createErrors: []error{
		daytonaerrors.NewDaytonaValidationError(daytonaDiskCapacityResponse, nil),
	}}
	lifecycle := sandboxdriver.NewDaytonaLifecycleProviderForClient(sdkClient, time.Minute)
	var logs bytes.Buffer
	adapter := &DaytonaAdapter{Lifecycle: lifecycle, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	outcome := adapter.Activate(context.Background(), ActivationRequest{
		Kind: ActivationCreate,
		Setup: sandbox.SandboxSetup{
			WorkspaceID: workspace.ID("ws_capacity_boundary"), SessionID: "sesn_capacity_boundary",
			SandboxID: "sbox_capacity_boundary", LifecycleOperationID: "sop_capacity_boundary",
			EnvironmentID: "env_capacity_boundary", ProviderArtifactRef: "artifact_capacity_boundary",
			Network: sandbox.NetworkSetup{Type: "unrestricted"},
		},
	})
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderRetryable ||
		outcome.ErrorKind != "quota_exceeded" || outcome.SafeMessage != "sandbox provider capacity is unavailable" ||
		outcome.ProviderStatusCode != 400 {
		t.Fatalf("Daytona capacity outcome = %+v; want proved-not-started retryable quota", outcome)
	}
	for _, want := range []string{
		`"provider.name":"daytona"`, `"provider.status_code":400`, `"error.code":"quota_exceeded"`,
		`"error.message_safe":"sandbox provider capacity is unavailable"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("provider completion log missing %s: %s", want, logs.String())
		}
	}
	if strings.Contains(logs.String(), daytonaDiskCapacityResponse) || strings.Contains(logs.String(), "30GiB") {
		t.Fatalf("provider completion log exposed Daytona capacity response: %s", logs.String())
	}
}

func TestDaytonaCreateCapacityUsesExistingActivationCustody(t *testing.T) {
	t.Run("capacity returns before exhaustion", func(t *testing.T) {
		harness := newDaytonaActivationHarness(t, []error{
			daytonaerrors.NewDaytonaValidationError(daytonaDiskCapacityResponse, nil),
			nil,
		})
		before := harness.activationIdentity(t)
		if before.providerResourceID.Valid {
			t.Fatalf("binding provider handle exists before Create: %q", before.providerResourceID.String)
		}

		harness.runOnce(t)
		if harness.sdk.createCalls != 1 {
			t.Fatalf("Daytona Create calls = %d; want one rejected submission", harness.sdk.createCalls)
		}
		harness.assertQueue(t, queue.StatusPending, 1, "quota_exceeded")
		if got := harness.logs.String(); !strings.Contains(got, `"operation":"sandbox.provider.create"`) ||
			!strings.Contains(got, `"provider.name":"daytona"`) ||
			!strings.Contains(got, `"provider.status_code":400`) ||
			!strings.Contains(got, `"error.code":"quota_exceeded"`) ||
			!strings.Contains(got, `"error.message_safe":"sandbox provider capacity is unavailable"`) {
			t.Fatalf("quota provider completion log = %s", got)
		}
		for _, forbidden := range []string{daytonaDiskCapacityResponse, "30GiB"} {
			if strings.Contains(harness.logs.String(), forbidden) {
				t.Fatalf("provider completion log exposed %q: %s", forbidden, harness.logs.String())
			}
		}

		harness.makeQueueJobAvailable(t)
		harness.runOnce(t)
		if harness.sdk.createCalls != 2 {
			t.Fatalf("Daytona Create calls = %d; want retry followed by success", harness.sdk.createCalls)
		}
		harness.assertQueue(t, queue.StatusAcknowledged, 2, "")
		after := harness.activationIdentity(t)
		if after.operationID != before.operationID || after.queueJobID != before.queueJobID ||
			after.logicalSandboxID != before.logicalSandboxID {
			t.Fatalf("activation custody changed: before=%+v after=%+v", before, after)
		}
		if !after.providerResourceID.Valid || after.providerResourceID.String != "provider_capacity_recovered" {
			t.Fatalf("binding provider handle = %+v; want recovered Daytona handle", after.providerResourceID)
		}
		var bindingCount int
		if err := harness.admin.QueryRow(`SELECT count(*) FROM session_sandbox_bindings
			WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`).Scan(&bindingCount); err != nil {
			t.Fatalf("count logical bindings: %v", err)
		}
		if bindingCount != 1 {
			t.Fatalf("logical binding count = %d; want one stable binding", bindingCount)
		}
	})

	t.Run("unrelated validation remains terminal", func(t *testing.T) {
		harness := newDaytonaActivationHarness(t, []error{
			daytonaerrors.NewDaytonaValidationError("snapshot is invalid", nil),
		})
		harness.runOnce(t)
		if harness.sdk.createCalls != 1 {
			t.Fatalf("Daytona Create calls = %d; want one", harness.sdk.createCalls)
		}
		harness.assertQueue(t, queue.StatusDeadLettered, 1, "invalid_request")
		var state, boundary, disposition, kind string
		if err := harness.admin.QueryRow(`SELECT state, outcome_effect_boundary, outcome_disposition, error_kind
			FROM sandbox_lifecycle_operations WHERE workspace_id='ws_execution_store' AND operation_id=$1`,
			harness.operationID).Scan(&state, &boundary, &disposition, &kind); err != nil {
			t.Fatalf("read terminal validation operation: %v", err)
		}
		if state != "failed" || boundary != string(ProviderProvedNotStarted) ||
			disposition != string(ProviderTerminal) || kind != "invalid_request" {
			t.Fatalf("terminal validation operation = %q/%q/%q/%q", state, boundary, disposition, kind)
		}
	})

	t.Run("persistent capacity exhausts without a sixth Create", func(t *testing.T) {
		errorsByAttempt := make([]error, sandboxActivationSubmissionMaxAttempts)
		for index := range errorsByAttempt {
			errorsByAttempt[index] = daytonaerrors.NewDaytonaValidationError(daytonaDiskCapacityResponse, nil)
		}
		harness := newDaytonaActivationHarness(t, errorsByAttempt)
		for attempt := 1; attempt <= sandboxActivationMaxAttempts; attempt++ {
			if attempt > 1 {
				harness.makeQueueJobAvailable(t)
			}
			harness.runOnce(t)
		}
		if harness.sdk.createCalls != sandboxActivationSubmissionMaxAttempts {
			t.Fatalf("Daytona Create calls = %d; want bounded %d", harness.sdk.createCalls, sandboxActivationSubmissionMaxAttempts)
		}
		harness.assertQueue(t, queue.StatusDeadLettered, sandboxActivationMaxAttempts, "sandbox_activation_attempts_exhausted")
		var resultJSON string
		if err := harness.admin.QueryRow(`SELECT result_json FROM session_runtime_tool_results
			WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
			  AND tool_use_event_id=$1`, harness.toolUseEventID).Scan(&resultJSON); err != nil {
			t.Fatalf("read exhausted Sandbox settlement: %v", err)
		}
		const want = `{"error":{"kind":"sandbox_activation_attempts_exhausted","message":"sandbox activation could not be completed"},"status":"error"}`
		if resultJSON != want {
			t.Fatalf("exhausted Sandbox settlement = %s; want %s", resultJSON, want)
		}
		for _, forbidden := range []string{"quota_exceeded", daytonaDiskCapacityResponse, "30GiB"} {
			if strings.Contains(resultJSON, forbidden) {
				t.Fatalf("exhausted Sandbox settlement exposed %q: %s", forbidden, resultJSON)
			}
		}
		harness.assertExhaustionPublicChain(t)
	})
}

type daytonaActivationHarness struct {
	admin           *sql.DB
	queueJobID      string
	operationID     string
	runner          *SandboxActivationJobRunner
	sdk             *scriptedDaytonaLifecycleClient
	logs            *bytes.Buffer
	bridgeStore     *agentruntimebridge.PostgreSQLBridgeAPIStore
	bridgeScope     *bridgev1.RuntimeScope
	toolUseEventID  string
	modelRequestID  string
	modelToolCallID string
	inputJSON       string
	inputHash       string
}

func newDaytonaActivationHarness(t *testing.T, createErrors []error) *daytonaActivationHarness {
	t.Helper()
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	const (
		bindingID       = "bind_capacity_chain"
		podUID          = "pod_capacity_chain"
		modelRequestID  = "mreq_capacity_chain"
		modelToolCallID = "call_capacity_chain"
		inputJSON       = `{"cmd":"true"}`
	)
	if _, err := adminDB.Exec(`UPDATE session_threads SET visibility='public'
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND id='thr_execution_store'`); err != nil {
		t.Fatalf("make capacity-chain Thread public: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at
		) VALUES ('ws_execution_store', 'sesn_execution_store', $1, 1, 'tetral-agent-runtime',
			'runtime-pod-0', $2, '10.0.0.10', clock_timestamp(), clock_timestamp())`, bindingID, podUID); err != nil {
		t.Fatalf("seed Runtime binding for capacity chain: %v", err)
	}
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(client)
	bridgeScope := &bridgev1.RuntimeScope{
		RequestId: "req_capacity_chain", WorkspaceId: "ws_execution_store",
		SessionId: "sesn_execution_store", SessionThreadId: "thr_execution_store",
		Binding: &bridgev1.RuntimeBindingRef{BindingId: bindingID, BindingGeneration: 1, TargetPodUid: podUID},
	}
	zero := int64(0)
	if _, err := bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: bridgeScope, RuntimeWriteId: "rwrite_capacity_chain_start", ModelRequestId: modelRequestID,
		EventType:                     "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
		ContextThroughMessageSequence: &zero, RequestKind: "agent_provider_request",
	}); err != nil {
		t.Fatalf("commit capacity-chain Request Start: %v", err)
	}
	toolUse, err := bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: bridgeScope, RuntimeWriteId: "rwrite_capacity_chain_tool_use", ModelRequestId: modelRequestID,
		EventType:   "agent.tool_use",
		PayloadJson: `{"type":"agent.tool_use","name":"exec_command","input":{"cmd":"true"},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: &bridgev1.RuntimeAssistantPartAppend{
			Parts: []*bridgev1.RuntimePartCreate{{
				PartKind: "tool",
				PartJson: `{"type":"tool","toolCallId":"` + modelToolCallID + `","toolName":"exec_command","state":{"status":"running","input":{"value":{"cmd":"true"},"preview":"{\"cmd\":\"true\"}","truncated":false}}}`,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("commit capacity-chain Tool Use: %v", err)
	}
	inputHash := sandboxCapacitySHA256(inputJSON)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET tool_use_event_id=$1, normalized_input_hash=$2, tool_name='exec_command', input_json=$3,
			model_tool_call_id=$4
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_a'`, toolUse.GetEventId(), inputHash, inputJSON, modelToolCallID); err != nil {
		t.Fatalf("bind capacity-chain execution to durable Tool Use: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, toolUse.GetEventId())
	if err := coordinator.WaitForActivation(sandboxTestQueueContext(t, runtimeDB), work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	identity := readDaytonaActivationIdentity(t, adminDB)
	sdkClient := &scriptedDaytonaLifecycleClient{createErrors: append([]error{}, createErrors...)}
	lifecycle := sandboxdriver.NewDaytonaLifecycleProviderForClient(sdkClient, time.Minute)
	logs := &bytes.Buffer{}
	adapter := &DaytonaAdapter{
		Lifecycle: lifecycle,
		Resolver:  lifecycle,
		Tools:     &adapterToolExecutorFake{},
		Resources: &adapterResourceMaterializerFake{},
		Artifacts: &recordingArtifactBuilder{},
		BlobStore: blob.NewFakeBlobStore(),
		Logger:    slog.New(slog.NewJSONHandler(logs, nil)),
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	return &daytonaActivationHarness{
		admin: adminDB, queueJobID: identity.queueJobID, operationID: identity.operationID,
		sdk: sdkClient, logs: logs, bridgeStore: bridgeStore, bridgeScope: bridgeScope,
		toolUseEventID: toolUse.GetEventId(), modelRequestID: modelRequestID,
		modelToolCallID: modelToolCallID, inputJSON: inputJSON, inputHash: inputHash,
		runner: &SandboxActivationJobRunner{
			Queue:     sandboxProductionQueueClient(t, queue.NewPostgreSQLStore(client)),
			Store:     NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute),
			Providers: registry,
			Config: SandboxLifecycleRunnerConfig{
				WorkspaceID: "ws_execution_store", LeaseOwner: "sandbox-capacity-test", MaxJobs: 1,
				LeaseDuration: 30 * time.Second, HeartbeatInterval: 5 * time.Second,
			},
		},
	}
}

func (h *daytonaActivationHarness) runOnce(t *testing.T) {
	t.Helper()
	active, err := h.runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("activation RunOnceWithActivity = active %t, err %v", active, err)
	}
}

func (h *daytonaActivationHarness) makeQueueJobAvailable(t *testing.T) {
	t.Helper()
	if _, err := h.admin.Exec(`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='ws_execution_store' AND id=$1 AND status='pending'`, h.queueJobID); err != nil {
		t.Fatalf("make activation Queue job available: %v", err)
	}
}

func (h *daytonaActivationHarness) assertQueue(t *testing.T, wantStatus string, wantAttempts int, wantErrorKind string) {
	t.Helper()
	var status string
	var attempts int
	var errorKind sql.NullString
	if err := h.admin.QueryRow(`SELECT status, attempt_count, last_error_kind FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND id=$1`, h.queueJobID).Scan(&status, &attempts, &errorKind); err != nil {
		t.Fatalf("read activation Queue job: %v", err)
	}
	errorMatches := (!errorKind.Valid && wantErrorKind == "") || (errorKind.Valid && errorKind.String == wantErrorKind)
	if status != wantStatus || attempts != wantAttempts || !errorMatches {
		t.Fatalf("activation Queue = %q attempts=%d error=%+v; want %q/%d/%q", status, attempts, errorKind, wantStatus, wantAttempts, wantErrorKind)
	}
}

func (h *daytonaActivationHarness) activationIdentity(t *testing.T) daytonaActivationIdentity {
	t.Helper()
	return readDaytonaActivationIdentity(t, h.admin)
}

func (h *daytonaActivationHarness) assertExhaustionPublicChain(t *testing.T) {
	t.Helper()
	response, err := h.bridgeStore.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{
		Scope: h.bridgeScope, ToolUseEventId: h.toolUseEventID, ModelToolCallId: h.modelToolCallID,
		NormalizedInputHash: h.inputHash, ToolName: "exec_command", InputJson: h.inputJSON,
	})
	if err != nil {
		t.Fatalf("AwaitSandboxExecution from exhausted receipt: %v", err)
	}
	fixtureInput := map[string]any{
		"workspaceId": h.bridgeScope.GetWorkspaceId(), "sessionId": h.bridgeScope.GetSessionId(),
		"sessionThreadId": h.bridgeScope.GetSessionThreadId(), "bindingId": h.bridgeScope.GetBinding().GetBindingId(),
		"bindingGeneration": h.bridgeScope.GetBinding().GetBindingGeneration(), "targetPodUid": h.bridgeScope.GetBinding().GetTargetPodUid(),
		"modelRequestId": h.modelRequestID, "modelToolCallId": h.modelToolCallID,
		"toolUseEventId": h.toolUseEventID, "resultJson": response.GetResultJson(), "resultDigest": response.GetResultDigest(),
	}
	inputJSON, err := json.Marshal(fixtureInput)
	if err != nil {
		t.Fatalf("encode Runtime exhaustion fixture: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "sandbox-activation-exhaustion.json")
	if err := os.WriteFile(inputPath, inputJSON, 0o600); err != nil {
		t.Fatalf("write Runtime exhaustion fixture: %v", err)
	}
	command := exec.CommandContext(context.Background(), "bun", "packages/runtime-pod/test/fixtures/sandbox-activation-exhaustion.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime exhaustion fixture: %v: %s", err, output)
	}
	var runtimeResult runtimeActivationExhaustionFixtureResult
	if err := json.Unmarshal(output, &runtimeResult); err != nil {
		t.Fatalf("decode Runtime exhaustion fixture: %v: %s", err, output)
	}
	for name, result := range map[string]runtimeActivationErrorResult{
		"command": runtimeResult.CommandResult,
		"file":    runtimeResult.FileResult,
	} {
		if result.Type != "error" || result.Error.Type != "runtime" || result.Error.Code != "runtime_invalid_sequence" ||
			result.Error.Message != "sandbox activation could not be completed" || result.Error.Retryable || result.Error.Fatal ||
			result.SandboxResultDigest != response.GetResultDigest() {
			t.Fatalf("%s Runtime exhaustion result = %+v; want exact generic rejoin failure", name, result)
		}
	}
	if runtimeResult.Event.Type != "agent.tool_result" || runtimeResult.Event.ToolUseID != h.toolUseEventID ||
		!runtimeResult.Event.IsError || len(runtimeResult.Event.Content) != 1 ||
		runtimeResult.Event.Content[0].Type != "text" || runtimeResult.Event.Content[0].Text != "sandbox activation could not be completed" {
		t.Fatalf("Runtime exhaustion event = %+v; want one exact error text block", runtimeResult.Event)
	}
	payloadJSON, err := json.Marshal(runtimeResult.Event)
	if err != nil {
		t.Fatalf("encode Runtime exhaustion event: %v", err)
	}
	errorJSON := string(runtimeResult.DeclaredError)
	digest := response.GetResultDigest()
	committed, err := h.bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: h.bridgeScope, RuntimeWriteId: "rwrite_capacity_chain_result", ModelRequestId: h.modelRequestID,
		EventType: "agent.tool_result", PayloadJson: string(payloadJSON), SandboxResultDigest: &digest,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: h.toolUseEventID,
			Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: errorJSON}},
		}},
	})
	if err != nil {
		t.Fatalf("commit Runtime exhaustion Tool Result: %v", err)
	}
	var durablePayload, visibility string
	var sessionVisible bool
	if err := h.admin.QueryRow(`SELECT payload_json, visibility, session_visible FROM session_events
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND event_id=$1`,
		committed.GetEventId()).Scan(&durablePayload, &visibility, &sessionVisible); err != nil {
		t.Fatalf("read durable exhaustion Tool Result: %v", err)
	}
	if visibility != "public" || !sessionVisible {
		t.Fatalf("durable exhaustion Tool Result visibility = %q/%t; want public/session-visible", visibility, sessionVisible)
	}
	publicJSON, err := eventwire.MarshalPublicEvent(committed.GetEventId(), "agent.tool_result", json.RawMessage(durablePayload), nil)
	if err != nil {
		t.Fatalf("project public exhaustion Tool Result: %v", err)
	}
	var publicEvent runtimeActivationToolResultEvent
	if err := json.Unmarshal(publicJSON, &publicEvent); err != nil {
		t.Fatalf("decode public exhaustion Tool Result: %v", err)
	}
	if publicEvent.Type != "agent.tool_result" || publicEvent.ToolUseID != h.toolUseEventID ||
		!publicEvent.IsError || len(publicEvent.Content) != 1 ||
		publicEvent.Content[0].Type != "text" || publicEvent.Content[0].Text != "sandbox activation could not be completed" {
		t.Fatalf("public exhaustion Tool Result = %+v; want one exact error text block", publicEvent)
	}
	for _, surface := range []string{response.GetResultJson(), string(output), durablePayload, string(publicJSON)} {
		for _, forbidden := range []string{"quota_exceeded", daytonaDiskCapacityResponse, "30GiB", "Partial result"} {
			if strings.Contains(surface, forbidden) {
				t.Fatalf("exhaustion proof surface exposed %q: %s", forbidden, surface)
			}
		}
	}
}

type runtimeActivationExhaustionFixtureResult struct {
	CommandResult runtimeActivationErrorResult     `json:"commandResult"`
	FileResult    runtimeActivationErrorResult     `json:"fileResult"`
	Event         runtimeActivationToolResultEvent `json:"event"`
	DeclaredError json.RawMessage                  `json:"declaredError"`
}

type runtimeActivationErrorResult struct {
	Type  string `json:"type"`
	Error struct {
		Type      string `json:"type"`
		Code      string `json:"code"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
		Fatal     bool   `json:"fatal"`
	} `json:"error"`
	SandboxResultDigest string `json:"sandboxResultDigest"`
}

type runtimeActivationToolResultEvent struct {
	ID        string `json:"id,omitempty"`
	Type      string `json:"type"`
	ToolUseID string `json:"tool_use_id"`
	Content   []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"is_error"`
}

type daytonaActivationIdentity struct {
	logicalSandboxID   string
	operationID        string
	queueJobID         string
	providerResourceID sql.NullString
}

func readDaytonaActivationIdentity(t *testing.T, db *sql.DB) daytonaActivationIdentity {
	t.Helper()
	var identity daytonaActivationIdentity
	if err := db.QueryRow(`SELECT b.logical_sandbox_id, o.operation_id, o.queue_job_id, b.provider_resource_id
		FROM session_sandbox_bindings b
		JOIN sandbox_lifecycle_operations o ON o.workspace_id=b.workspace_id AND o.session_id=b.session_id
		WHERE b.workspace_id='ws_execution_store' AND b.session_id='sesn_execution_store' AND o.kind='create'`).Scan(
		&identity.logicalSandboxID, &identity.operationID, &identity.queueJobID, &identity.providerResourceID,
	); err != nil {
		t.Fatalf("read activation identity: %v", err)
	}
	return identity
}

func sandboxCapacitySHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

type scriptedDaytonaLifecycleClient struct {
	createErrors []error
	createCalls  int
	created      *daytona.Sandbox
}

func (c *scriptedDaytonaLifecycleClient) Create(_ context.Context, raw any, _ ...func(*options.CreateSandbox)) (*daytona.Sandbox, error) {
	c.createCalls++
	if len(c.createErrors) > 0 {
		err := c.createErrors[0]
		c.createErrors = c.createErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	params, ok := raw.(types.SnapshotParams)
	if !ok {
		return nil, errors.New("unexpected Daytona Create parameters")
	}
	c.created = &daytona.Sandbox{
		ID: "provider_capacity_recovered", Name: params.Name, Labels: params.Labels,
		State: apiclient.SANDBOXSTATE_STARTED,
	}
	return c.created, nil
}

func (c *scriptedDaytonaLifecycleClient) Get(_ context.Context, identity string) (*daytona.Sandbox, error) {
	if c.created != nil && (identity == c.created.ID || identity == c.created.Name) {
		return c.created, nil
	}
	return nil, daytonaerrors.NewDaytonaNotFoundError("sandbox not found", nil)
}
