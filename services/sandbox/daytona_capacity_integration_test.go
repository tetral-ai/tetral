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
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const daytonaDiskCapacityResponse = `Total disk limit exceeded. Maximum allowed: 30GiB.
Consider archiving your unused Sandboxes to free up available storage.
To increase concurrency limits, upgrade your organization's Tier by visiting https://app.daytona.io/dashboard/limits.`

const daytonaCPUCapacityResponse = `Total CPU limit exceeded. Maximum allowed: 10.
To increase concurrency limits, upgrade your organization's Tier by visiting https://app.daytona.io/dashboard/limits.`

const daytonaMemoryCapacityResponse = `Total memory limit exceeded. Maximum allowed: 16GiB.
To increase concurrency limits, upgrade your organization's Tier by visiting https://app.daytona.io/dashboard/limits.`

func findDaytonaCapacityLogRecord(t *testing.T, encoded []byte, key string, value any) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(encoded, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode Daytona capacity log: %v\n%s", err, line)
		}
		if record[key] == value {
			return record
		}
	}
	t.Fatalf("Daytona capacity log has no record with %s=%v: %s", key, value, encoded)
	return nil
}

func assertDaytonaCapacityLogFields(t *testing.T, record map[string]any, want map[string]any) {
	t.Helper()
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("Daytona capacity log %s=%v; want %v in %#v", key, record[key], value, record)
		}
	}
}

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
	for _, response := range []struct {
		name string
		body string
	}{
		{name: "disk", body: daytonaDiskCapacityResponse},
		{name: "CPU", body: daytonaCPUCapacityResponse},
		{name: "memory", body: daytonaMemoryCapacityResponse},
	} {
		t.Run(response.name+" capacity returns before exhaustion", func(t *testing.T) {
			harness := newDaytonaActivationHarness(t, []error{
				daytonaerrors.NewDaytonaValidationError(response.body, nil),
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
			providerRecord := findDaytonaCapacityLogRecord(t, harness.logs.Bytes(), "operation", "sandbox.provider.create")
			assertDaytonaCapacityLogFields(t, providerRecord, map[string]any{
				"provider.name": "daytona", "provider.status_code": float64(400),
				"error.code": "quota_exceeded", "error.message_safe": "sandbox provider capacity is unavailable",
			})
			attemptRecord := findDaytonaCapacityLogRecord(t, harness.logs.Bytes(), "event", "sandbox_activation_attempt_completed")
			assertDaytonaCapacityLogFields(t, attemptRecord, map[string]any{
				"provider.name": "daytona", "outcome": "retry", "error.code": "quota_exceeded",
				"error.message_safe":  "sandbox activation will be retried",
				"queue.attempt.count": float64(1), "queue.attempt.max": float64(sandboxActivationMaxAttempts),
				"session.id": "sesn_execution_store", "operation.id": harness.operationID, "job.id": harness.queueJobID,
			})
			for _, forbidden := range daytonaCapacityForbiddenDetails() {
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
	}

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
			errorsByAttempt[index] = daytonaerrors.NewDaytonaValidationError(daytonaCPUCapacityResponse, nil)
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
		for _, forbidden := range append([]string{"quota_exceeded"}, daytonaCapacityForbiddenDetails()...) {
			if strings.Contains(resultJSON, forbidden) {
				t.Fatalf("exhausted Sandbox settlement exposed %q: %s", forbidden, resultJSON)
			}
		}
		harness.assertExhaustionPublicChain(t)
	})

	t.Run("persistent capacity contains shared operation failure", func(t *testing.T) {
		errorsByAttempt := make([]error, sandboxActivationSubmissionMaxAttempts)
		for index := range errorsByAttempt {
			errorsByAttempt[index] = daytonaerrors.NewDaytonaValidationError(daytonaMemoryCapacityResponse, nil)
		}
		harness := newDaytonaActivationHarness(t, errorsByAttempt)
		sharedWaiter := harness.attachSharedActivationWaiter(t)
		otherWaiter := harness.attachOtherActivationWaiter(t)
		harness.deferOperationQueueJob(t, otherWaiter.operationID)

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
		for _, waiter := range []daytonaCapacityWaiter{harness.primaryWaiter(), sharedWaiter} {
			harness.assertExhaustedWaiter(t, waiter)
			harness.assertExhaustionPublicChainForWaiter(t, waiter)
		}
		harness.assertOtherOperationUnaffected(t, otherWaiter)
		harness.assertSessionAndThreadRemainUsable(t)
	})
}

type daytonaCapacityWaiter struct {
	sessionID       string
	threadID        string
	toolUseEventID  string
	modelToolCallID string
	operationID     string
}

type daytonaActivationHarness struct {
	runtime         *sql.DB
	admin           *sql.DB
	client          *dbconnect.Client
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
	if _, err := adminDB.Exec(`UPDATE sessions SET lifecycle_state='active', main_thread_id='thr_execution_store'
		WHERE workspace_id='ws_execution_store' AND id='sesn_execution_store'`); err != nil {
		t.Fatalf("make capacity-chain Session input-admissible: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_status (
		workspace_id, session_id, status, idle_since, created_at, updated_at
	) VALUES ('ws_execution_store', 'sesn_execution_store', 'idle', clock_timestamp(), clock_timestamp(), clock_timestamp())`); err != nil {
		t.Fatalf("seed capacity-chain Runtime status: %v", err)
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
		WorkspaceId: "ws_execution_store", SessionId: "sesn_execution_store", SessionThreadId: "thr_execution_store",
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
		EventType:                   "agent.tool_use",
		PayloadJson:                 `{"type":"agent.tool_use","name":"exec_command","input":{"cmd":"true"},"evaluated_permission":"allow"}`,
		CanonicalExecutionInputJson: inputJSON,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{
			Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{
				ToolCall: &bridgev1.RuntimeContextToolCall{
					ModelToolCallId: modelToolCallID, ToolName: "exec_command", ProviderInputJson: inputJSON,
				},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("commit capacity-chain Tool Use: %v", err)
	}
	if toolUse.GetCommitted() == nil || len(toolUse.GetCommitted().GetCreatedToolUseEventIds()) != 1 {
		t.Fatalf("capacity-chain Tool Use result = %#v; want one committed Tool identity", toolUse)
	}
	toolUseEventID := toolUse.GetCommitted().GetCreatedToolUseEventIds()[0]
	inputHash := sandboxCapacitySHA256(inputJSON)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET tool_use_event_id=$1, normalized_input_hash=$2, tool_name='exec_command', input_json=$3,
			model_tool_call_id=$4
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_a'`, toolUseEventID, inputHash, inputJSON, modelToolCallID); err != nil {
		t.Fatalf("bind capacity-chain execution to durable Tool Use: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, toolUseEventID)
	if err := coordinator.WaitForActivation(sandboxTestQueueContext(t, runtimeDB), work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	identity := readDaytonaActivationIdentity(t, adminDB)
	sdkClient := &scriptedDaytonaLifecycleClient{createErrors: append([]error{}, createErrors...)}
	lifecycle := sandboxdriver.NewDaytonaLifecycleProviderForClient(sdkClient, time.Minute)
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	adapter := &DaytonaAdapter{
		Lifecycle: lifecycle,
		Resolver:  lifecycle,
		Tools:     &adapterToolExecutorFake{},
		Resources: &adapterResourceMaterializerFake{},
		Artifacts: &recordingArtifactBuilder{},
		BlobStore: blob.NewFakeBlobStore(),
		Logger:    logger,
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	return &daytonaActivationHarness{
		runtime: runtimeDB, admin: adminDB, client: client,
		queueJobID: identity.queueJobID, operationID: identity.operationID,
		sdk: sdkClient, logs: logs, bridgeStore: bridgeStore, bridgeScope: bridgeScope,
		toolUseEventID: toolUseEventID, modelRequestID: modelRequestID,
		modelToolCallID: modelToolCallID,
		runner: &SandboxActivationJobRunner{
			Queue:     sandboxProductionQueueClient(t, queue.NewPostgreSQLStore(client)),
			Store:     NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute),
			Providers: registry,
			Logger:    logger,
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

func (h *daytonaActivationHarness) deferOperationQueueJob(t *testing.T, operationID string) {
	t.Helper()
	result, err := h.admin.Exec(`UPDATE queue_jobs SET available_at=clock_timestamp()+interval '1 hour'
		WHERE workspace_id='ws_execution_store' AND id=(
			SELECT queue_job_id FROM sandbox_lifecycle_operations
			WHERE workspace_id='ws_execution_store' AND operation_id=$1
		) AND status='pending'`, operationID)
	if err != nil {
		t.Fatalf("defer competing activation Queue job: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("defer competing activation Queue job affected=%d err=%v", affected, err)
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

func (h *daytonaActivationHarness) primaryWaiter() daytonaCapacityWaiter {
	return daytonaCapacityWaiter{
		sessionID: "sesn_execution_store", threadID: "thr_execution_store",
		toolUseEventID: h.toolUseEventID, modelToolCallID: h.modelToolCallID,
		operationID: h.operationID,
	}
}

func (h *daytonaActivationHarness) attachSharedActivationWaiter(t *testing.T) daytonaCapacityWaiter {
	t.Helper()
	const (
		modelToolCallID = "call_capacity_chain_shared"
		inputJSON       = `{"cmd":"printf shared"}`
	)
	written, err := h.bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: h.bridgeScope, RuntimeWriteId: "rwrite_capacity_chain_shared_tool_use", ModelRequestId: h.modelRequestID,
		EventType:                   "agent.tool_use",
		PayloadJson:                 `{"type":"agent.tool_use","name":"exec_command","input":{"cmd":"printf shared"},"evaluated_permission":"allow"}`,
		CanonicalExecutionInputJson: inputJSON,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{
			Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{
				ToolCall: &bridgev1.RuntimeContextToolCall{
					ModelToolCallId: modelToolCallID, ToolName: "exec_command", ProviderInputJson: inputJSON,
				},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("commit shared capacity Tool Use: %v", err)
	}
	if written.GetCommitted() == nil || len(written.GetCommitted().GetCreatedToolUseEventIds()) != 1 {
		t.Fatalf("shared capacity Tool Use result = %#v; want one committed Tool identity", written)
	}
	toolUseEventID := written.GetCommitted().GetCreatedToolUseEventIds()[0]
	inputHash := sandboxCapacitySHA256(inputJSON)
	if _, err := h.admin.Exec(`UPDATE session_runtime_tool_results
		SET tool_use_event_id=$1, normalized_input_hash=$2, tool_name='exec_command', input_json=$3,
			model_tool_call_id=$4
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_b'`, toolUseEventID, inputHash, inputJSON, modelToolCallID); err != nil {
		t.Fatalf("bind shared capacity execution to durable Tool Use: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(h.client, 30*time.Minute)
	work, current, err := coordinator.LoadExecution(context.Background(), SandboxExecutionJob{Ref: SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: toolUseEventID,
	}, AttemptGeneration: 1})
	if err != nil || !current || work.Binding == nil {
		t.Fatalf("load shared capacity execution = current %t binding %+v err %v", current, work.Binding, err)
	}
	if err := coordinator.WaitForActivation(sandboxTestQueueContext(t, h.runtime), work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("attach shared capacity waiter: %v", err)
	}
	var operationID string
	if err := h.admin.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND tool_use_event_id=$1`,
		toolUseEventID).Scan(&operationID); err != nil {
		t.Fatalf("read shared waiter operation: %v", err)
	}
	if operationID != h.operationID {
		t.Fatalf("shared waiter operation = %q; want %q", operationID, h.operationID)
	}
	return daytonaCapacityWaiter{
		sessionID: "sesn_execution_store", threadID: "thr_execution_store",
		toolUseEventID: toolUseEventID, modelToolCallID: modelToolCallID,
		operationID: operationID,
	}
}

func (h *daytonaActivationHarness) attachOtherActivationWaiter(t *testing.T) daytonaCapacityWaiter {
	t.Helper()
	const (
		sessionID      = "sesn_capacity_other"
		threadID       = "thr_capacity_other"
		toolUseEventID = "evt_capacity_other"
	)
	seedEnvironmentArtifactStoreSession(t, h.admin, "ws_execution_store", sessionID, "env_execution_store")
	if _, err := h.admin.Exec(`INSERT INTO session_threads (
		workspace_id, session_id, id, role, status, visibility, created_at, last_active_at, updated_at
	) VALUES ('ws_execution_store', $1, $2, 'main', 'idle', 'public', clock_timestamp(), clock_timestamp(), clock_timestamp())`,
		sessionID, threadID); err != nil {
		t.Fatalf("seed other capacity Thread: %v", err)
	}
	if _, err := h.admin.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, model_tool_call_id,
		execution_state, execution_attempt_generation, created_at, updated_at
	) VALUES ('ws_execution_store', $1, $2, $3, 'sandbox_tool', 'other_hash', 'exec_command',
		'{"cmd":"printf other"}', 'committed', 'call_capacity_other', 'pending', 1, clock_timestamp(), clock_timestamp())`,
		sessionID, threadID, toolUseEventID); err != nil {
		t.Fatalf("seed other capacity execution: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(h.client, 30*time.Minute)
	work, current, err := coordinator.LoadExecution(context.Background(), SandboxExecutionJob{Ref: SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: sessionID, SessionThreadID: threadID, ToolUseEventID: toolUseEventID,
	}, AttemptGeneration: 1})
	if err != nil || !current || work.Binding != nil {
		t.Fatalf("load other capacity execution = current %t binding %+v err %v", current, work.Binding, err)
	}
	if err := coordinator.WaitForActivation(sandboxTestQueueContext(t, h.runtime), work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("attach other capacity waiter: %v", err)
	}
	var operationID string
	if err := h.admin.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseEventID).Scan(&operationID); err != nil {
		t.Fatalf("read other waiter operation: %v", err)
	}
	if operationID == "" || operationID == h.operationID {
		t.Fatalf("other waiter operation = %q; want distinct from %q", operationID, h.operationID)
	}
	return daytonaCapacityWaiter{
		sessionID: sessionID, threadID: threadID, toolUseEventID: toolUseEventID,
		modelToolCallID: "call_capacity_other", operationID: operationID,
	}
}

func (h *daytonaActivationHarness) assertExhaustionPublicChain(t *testing.T) {
	t.Helper()
	h.assertExhaustionPublicChainForWaiter(t, h.primaryWaiter())
}

func (h *daytonaActivationHarness) assertExhaustionPublicChainForWaiter(t *testing.T, waiter daytonaCapacityWaiter) {
	t.Helper()
	response, err := h.bridgeStore.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{
		Scope: h.bridgeScope, ToolUseEventId: waiter.toolUseEventID,
	})
	if err != nil {
		t.Fatalf("AwaitSandboxExecution from exhausted receipt: %v", err)
	}
	if response.GetCompleted() == nil {
		t.Fatalf("AwaitSandboxExecution from exhausted receipt = %#v; want completed", response)
	}
	resultJSON := response.GetCompleted().GetResultJson()
	fixtureInput := map[string]any{
		"workspaceId": h.bridgeScope.GetWorkspaceId(), "sessionId": h.bridgeScope.GetSessionId(),
		"sessionThreadId": h.bridgeScope.GetSessionThreadId(), "bindingId": h.bridgeScope.GetBinding().GetBindingId(),
		"bindingGeneration": h.bridgeScope.GetBinding().GetBindingGeneration(), "targetPodUid": h.bridgeScope.GetBinding().GetTargetPodUid(),
		"modelRequestId": h.modelRequestID, "modelToolCallId": waiter.modelToolCallID,
		"toolUseEventId": waiter.toolUseEventID, "resultJson": resultJSON,
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
			result.Error.Message != "The requested operation could not be completed." || result.Error.Retryable || result.Error.Fatal {
			t.Fatalf("%s Runtime exhaustion result = %+v; want exact generic rejoin failure", name, result)
		}
	}
	errorJSON := string(runtimeResult.DeclaredError)
	writeRequest := &bridgev1.SettleToolResultRequest{
		Scope: h.bridgeScope,
		Settlement: &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: waiter.toolUseEventID,
			Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: errorJSON}},
		},
	}
	committed, err := h.bridgeStore.SettleToolResult(context.Background(), writeRequest)
	if err != nil {
		t.Fatalf("commit Runtime exhaustion Tool Result: %v", err)
	}
	if committed.GetCommitted() == nil {
		t.Fatalf("Runtime exhaustion Tool Result = %+v; want committed", committed)
	}
	replayed, err := h.bridgeStore.SettleToolResult(context.Background(), writeRequest)
	if err != nil || replayed.GetDuplicate() == nil {
		t.Fatalf("Runtime exhaustion Tool Result replay = %+v, %v; want duplicate", replayed, err)
	}
	var resultEventID string
	var durablePayload, visibility string
	var sessionVisible bool
	if err := h.admin.QueryRow(`SELECT event_id, payload_json, visibility, session_visible FROM session_events
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND type='agent.tool_result' AND payload_json::jsonb ->> 'tool_use_id'=$1`,
		waiter.toolUseEventID).Scan(&resultEventID, &durablePayload, &visibility, &sessionVisible); err != nil {
		t.Fatalf("read durable exhaustion Tool Result: %v", err)
	}
	if visibility != "public" || !sessionVisible {
		t.Fatalf("durable exhaustion Tool Result visibility = %q/%t; want public/session-visible", visibility, sessionVisible)
	}
	publicJSON, err := eventwire.MarshalPublicEvent(resultEventID, "agent.tool_result", json.RawMessage(durablePayload), nil)
	if err != nil {
		t.Fatalf("project public exhaustion Tool Result: %v", err)
	}
	var publicEvent runtimeActivationToolResultEvent
	if err := json.Unmarshal(publicJSON, &publicEvent); err != nil {
		t.Fatalf("decode public exhaustion Tool Result: %v", err)
	}
	if publicEvent.Type != "agent.tool_result" || publicEvent.ToolUseID != waiter.toolUseEventID ||
		!publicEvent.IsError || len(publicEvent.Content) != 1 ||
		publicEvent.Content[0].Type != "text" || publicEvent.Content[0].Text != "The requested operation could not be completed." {
		t.Fatalf("public exhaustion Tool Result = %+v; want one exact error text block", publicEvent)
	}
	for _, surface := range []string{resultJSON, string(output), durablePayload, string(publicJSON)} {
		for _, forbidden := range append([]string{"quota_exceeded"}, daytonaCapacityForbiddenDetails()...) {
			if strings.Contains(surface, forbidden) {
				t.Fatalf("exhaustion proof surface exposed %q: %s", forbidden, surface)
			}
		}
	}
	for _, surface := range []string{string(output), durablePayload, string(publicJSON)} {
		for _, forbidden := range []string{"Partial result", "sandbox activation", "daytona", "capacity", "provider.status_code", "queue.attempt"} {
			if strings.Contains(surface, forbidden) {
				t.Fatalf("public projection surface exposed %q: %s", forbidden, surface)
			}
		}
	}
	var settlementEvents int
	if err := h.admin.QueryRow(`SELECT count(*) FROM session_events
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND type='agent.tool_result' AND payload_json::jsonb ->> 'tool_use_id'=$1`, waiter.toolUseEventID).Scan(&settlementEvents); err != nil {
		t.Fatalf("count durable exhaustion Tool Results: %v", err)
	}
	if settlementEvents != 1 {
		t.Fatalf("durable exhaustion Tool Results for %s = %d; want exactly one", waiter.toolUseEventID, settlementEvents)
	}
}

func (h *daytonaActivationHarness) assertExhaustedWaiter(t *testing.T, waiter daytonaCapacityWaiter) {
	t.Helper()
	var state, resultJSON string
	var waitingOperation sql.NullString
	if err := h.admin.QueryRow(`SELECT execution_state, result_json, waiting_activation_operation_id
		FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		waiter.sessionID, waiter.threadID, waiter.toolUseEventID).Scan(&state, &resultJSON, &waitingOperation); err != nil {
		t.Fatalf("read exhausted waiter %s: %v", waiter.toolUseEventID, err)
	}
	const want = `{"error":{"kind":"sandbox_activation_attempts_exhausted","message":"sandbox activation could not be completed"},"status":"error"}`
	if state != "terminal_unconsumed" || resultJSON != want || waitingOperation.String != waiter.operationID {
		t.Fatalf("exhausted waiter %s = state %q result %s operation %+v", waiter.toolUseEventID, state, resultJSON, waitingOperation)
	}
}

func (h *daytonaActivationHarness) assertOtherOperationUnaffected(t *testing.T, waiter daytonaCapacityWaiter) {
	t.Helper()
	var state string
	var operationID string
	var resultJSON sql.NullString
	if err := h.admin.QueryRow(`SELECT execution_state, waiting_activation_operation_id, result_json
		FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		waiter.sessionID, waiter.threadID, waiter.toolUseEventID).Scan(&state, &operationID, &resultJSON); err != nil {
		t.Fatalf("read other-operation waiter: %v", err)
	}
	if state != "waiting_activation" || operationID != waiter.operationID || resultJSON.Valid {
		t.Fatalf("other-operation waiter = state %q operation %q result %+v; want unchanged waiting state", state, operationID, resultJSON)
	}
}

func (h *daytonaActivationHarness) assertSessionAndThreadRemainUsable(t *testing.T) {
	t.Helper()
	var sessionStatus, threadStatus, visibility string
	var archivedAt sql.NullTime
	if err := h.admin.QueryRow(`SELECT s.status, t.status, t.visibility, t.archived_at
		FROM sessions s JOIN session_threads t ON t.workspace_id=s.workspace_id AND t.session_id=s.id
		WHERE s.workspace_id='ws_execution_store' AND s.id='sesn_execution_store' AND t.id='thr_execution_store'`).Scan(
		&sessionStatus, &threadStatus, &visibility, &archivedAt,
	); err != nil {
		t.Fatalf("read capacity Session and Thread: %v", err)
	}
	if sessionStatus != "idle" || threadStatus != "idle" || visibility != "public" || archivedAt.Valid {
		t.Fatalf("capacity Session/Thread = %q/%q visibility=%q archived=%+v; want usable public idle lane", sessionStatus, threadStatus, visibility, archivedAt)
	}
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(h.client))
	appended, err := eventService.AppendClientEvents(
		context.Background(), workspace.ID("ws_execution_store"), "sesn_execution_store", "idem_capacity_later_input",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{
				Type: sessionevent.ContentBlockTypeText, Text: "continue after the Tool failure",
			}},
		}}},
	)
	if err != nil || len(appended.Data) != 1 {
		t.Fatalf("append later ordinary Session input = %+v, %v; want one accepted event", appended, err)
	}
	var sessionErrors int
	if err := h.admin.QueryRow(`SELECT count(*) FROM session_events
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND type='session.error'`).Scan(&sessionErrors); err != nil {
		t.Fatalf("count capacity session.error events: %v", err)
	}
	if sessionErrors != 0 {
		t.Fatalf("capacity session.error events = %d; want zero", sessionErrors)
	}
}

func daytonaCapacityForbiddenDetails() []string {
	return []string{
		daytonaDiskCapacityResponse,
		daytonaCPUCapacityResponse,
		daytonaMemoryCapacityResponse,
		"30GiB",
		"16GiB",
		"Maximum allowed",
		"organization",
		"upgrade",
		"https://app.daytona.io/dashboard/limits",
		"credential",
		"ticket",
	}
}

type runtimeActivationExhaustionFixtureResult struct {
	CommandResult runtimeActivationErrorResult `json:"commandResult"`
	FileResult    runtimeActivationErrorResult `json:"fileResult"`
	DeclaredError json.RawMessage              `json:"declaredError"`
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
