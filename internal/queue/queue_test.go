package queue

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestNewTaskNotificationRuntimeInputEnqueueRequestIsBindingNeutral(t *testing.T) {
	now := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	request, err := NewTaskNotificationRuntimeInputEnqueueRequest(
		"ws_task_notification", "sesn_task_notification", "thr_task_notification", "task_notification", now,
	)
	if err != nil {
		t.Fatalf("NewTaskNotificationRuntimeInputEnqueueRequest: %v", err)
	}
	if request.Kind != KindRuntimeInput || request.PartitionKey != "session:ws_task_notification:sesn_task_notification" ||
		request.DedupeKey != "runtime_input:ws_task_notification:sesn_task_notification:task_notification:task_notification" ||
		request.MaxAttempts != DefaultMaxAttempts || !request.Now.Equal(now) {
		t.Fatalf("request transport identity = %#v", request)
	}
	const wantPayload = `{"workspace_id":"ws_task_notification","session_id":"sesn_task_notification","session_thread_id":"thr_task_notification","runtime_input_id":"task_notification:task_notification","event_ids":[],"sequence_from":0,"sequence_to":0,"input_kind":"task_notification"}`
	if string(request.PayloadJSON) != wantPayload {
		t.Fatalf("payload = %s; want %s", request.PayloadJSON, wantPayload)
	}
	if _, err := NormalizeEnqueueRequest(request); err != nil {
		t.Fatalf("NormalizeEnqueueRequest: %v", err)
	}
}

func TestNormalizeEnqueueRequestRejectsRuntimeInputBeyondEventReferenceLimit(t *testing.T) {
	eventIDs := make([]string, MaxRuntimeInputEventRefsPerJob+1)
	for i := range eventIDs {
		eventIDs[i] = "sevt_event_reference"
	}
	payload, err := json.Marshal(map[string]any{
		"workspace_id":      "ws_event_reference_limit",
		"session_id":        "sesn_event_reference_limit",
		"session_thread_id": "thrd_event_reference_limit",
		"runtime_input_id":  "rin_event_reference_limit",
		"event_ids":         eventIDs,
		"sequence_from":     1,
		"sequence_to":       len(eventIDs),
		"input_kind":        "messages",
	})
	if err != nil {
		t.Fatalf("marshal runtime input payload: %v", err)
	}

	_, err = NormalizeEnqueueRequest(EnqueueRequest{
		WorkspaceID:  workspace.ID("ws_event_reference_limit"),
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey("ws_event_reference_limit", "sesn_event_reference_limit"),
		DedupeKey:    FormatRuntimeInputDedupeKey("ws_event_reference_limit", "sesn_event_reference_limit", "rin_event_reference_limit"),
		PayloadJSON:  payload,
	})
	if !IsValidationError(err) || !strings.Contains(err.Error(), "maximum event reference count") {
		t.Fatalf("NormalizeEnqueueRequest error = %v; want event-reference validation error", err)
	}
}

func TestNormalizeEnqueueRequestAcceptsOnlyBareAgentMailPokes(t *testing.T) {
	base := map[string]any{
		"workspace_id":      "ws_agent_mail",
		"session_id":        "sesn_agent_mail",
		"session_thread_id": "thrd_agent_mail_main",
		"runtime_input_id":  "agent_mail:delivery_agent_mail",
		"event_ids":         []string{},
		"sequence_from":     0,
		"sequence_to":       0,
		"input_kind":        "agent_mail",
	}
	normalize := func(t *testing.T, payload map[string]any) error {
		t.Helper()
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal agent-mail payload: %v", err)
		}
		_, err = NormalizeEnqueueRequest(EnqueueRequest{
			WorkspaceID:  "ws_agent_mail",
			Kind:         KindRuntimeInput,
			PartitionKey: FormatSessionPartitionKey("ws_agent_mail", "sesn_agent_mail"),
			DedupeKey:    FormatRuntimeInputDedupeKey("ws_agent_mail", "sesn_agent_mail", "agent_mail:delivery_agent_mail"),
			PayloadJSON:  payloadJSON,
		})
		return err
	}
	if err := normalize(t, base); err != nil {
		t.Fatalf("normalize bare agent-mail poke: %v", err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"event refs": func(payload map[string]any) { payload["event_ids"] = []string{"evt_not_bare"} },
		"sequence":   func(payload map[string]any) { payload["sequence_from"] = 1; payload["sequence_to"] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			payload := make(map[string]any, len(base))
			for key, value := range base {
				payload[key] = value
			}
			mutate(payload)
			if err := normalize(t, payload); !IsValidationError(err) || !strings.Contains(err.Error(), "bare runtime-input poke") {
				t.Fatalf("normalize non-bare agent-mail poke = %v; want validation error", err)
			}
		})
	}
}

func TestNormalizeEnqueueRequestAcceptsOnlyRefsOnlyRuntimeConfigPayloads(t *testing.T) {
	ws := workspace.ID("ws_config_refs")
	sessionID := "sesn_config_refs"
	requests := []EnqueueRequest{
		{
			WorkspaceID:  ws,
			Kind:         KindRuntimeConfigUpdate,
			PartitionKey: FormatSessionPartitionKey(ws, sessionID),
			DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "7"),
			PayloadJSON:  []byte(`{"workspace_id":"ws_config_refs","session_id":"sesn_config_refs","config_generation":7}`),
		},
		{
			WorkspaceID:  ws,
			Kind:         KindRuntimeConfigUpdate,
			PartitionKey: FormatSessionPartitionKey(ws, sessionID),
			DedupeKey:    FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, "github", "3"),
			PayloadJSON:  []byte(`{"workspace_id":"ws_config_refs","session_id":"sesn_config_refs","mcp_server_name":"github","manifest_generation":3}`),
		},
	}
	for _, request := range requests {
		if _, err := NormalizeEnqueueRequest(request); err != nil {
			t.Fatalf("NormalizeEnqueueRequest(%s): %v", request.DedupeKey, err)
		}
	}
	contentBearing := requests[0]
	contentBearing.PayloadJSON = []byte(`{"workspace_id":"ws_config_refs","session_id":"sesn_config_refs","config_generation":7,"approval_mode":"full_access"}`)
	if _, err := NormalizeEnqueueRequest(contentBearing); !IsValidationError(err) {
		t.Fatalf("NormalizeEnqueueRequest(content-bearing config) = %v; want validation error", err)
	}
}

func TestNormalizeEnqueueRequestAcceptsCanonicalSandboxJobsAndRequiresExplicitBudget(t *testing.T) {
	ws := workspace.ID("ws_sandbox_queue_shapes")
	sessionID := "sesn_sandbox_queue_shapes"
	threadID := "thrd_sandbox_queue_shapes"
	toolUseEventID := "sevt_sandbox_queue_shapes"
	logicalSandboxID := "sbox_sandbox_queue_shapes"
	finishIdleWriteID := "write_sandbox_queue_shapes"
	taskID := "task_sandbox_queue_shapes"
	tests := []struct {
		name    string
		request EnqueueRequest
	}{
		{name: KindSandboxToolExecute, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxToolExecute,
			PartitionKey: FormatSandboxExecutionPartitionKey(ws, sessionID, threadID, toolUseEventID),
			DedupeKey:    FormatSandboxToolExecuteDedupeKey(ws, sessionID, threadID, toolUseEventID, 1),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "session_thread_id": threadID, "tool_use_event_id": toolUseEventID}),
		}},
		{name: KindSandboxActivate, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxActivate,
			PartitionKey: FormatSandboxLifecyclePartitionKey(ws, logicalSandboxID),
			DedupeKey:    FormatSandboxLifecycleDedupeKey(KindSandboxActivate, ws, logicalSandboxID, "op_activate"),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "logical_sandbox_id": logicalSandboxID, "operation_id": "op_activate"}),
		}},
		{name: KindSandboxMaterialize, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxMaterialize,
			PartitionKey: FormatSandboxLifecyclePartitionKey(ws, logicalSandboxID),
			DedupeKey:    FormatSandboxLifecycleDedupeKey(KindSandboxMaterialize, ws, logicalSandboxID, "op_materialize"),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "logical_sandbox_id": logicalSandboxID, "operation_id": "op_materialize"}),
		}},
		{name: KindSandboxRelease, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxRelease,
			PartitionKey: FormatSandboxLifecyclePartitionKey(ws, logicalSandboxID),
			DedupeKey:    FormatSandboxLifecycleDedupeKey(KindSandboxRelease, ws, logicalSandboxID, "op_release"),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "logical_sandbox_id": logicalSandboxID, "operation_id": "op_release"}),
		}},
		{name: KindSandboxToolCancel, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxToolCancel,
			PartitionKey: FormatSandboxCancelPartitionKey(ws, sessionID, threadID, toolUseEventID),
			DedupeKey:    FormatSandboxToolCancelDedupeKey(ws, sessionID, threadID, toolUseEventID),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "session_thread_id": threadID, "tool_use_event_id": toolUseEventID}),
		}},
		{name: KindSandboxOutputCapture, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxOutputCapture,
			PartitionKey: FormatSandboxCapturePartitionKey(ws, sessionID, finishIdleWriteID),
			DedupeKey:    FormatSandboxOutputCaptureDedupeKey(ws, sessionID, finishIdleWriteID, 1),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "finish_idle_write_id": finishIdleWriteID, "capture_generation": 1}),
		}},
		{name: KindSandboxOutputCaptureCleanup, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxOutputCaptureCleanup,
			PartitionKey: FormatSandboxCapturePartitionKey(ws, sessionID, finishIdleWriteID),
			DedupeKey:    FormatSandboxOutputCaptureCleanupDedupeKey(ws, sessionID, finishIdleWriteID, 1, 2),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "finish_idle_write_id": finishIdleWriteID, "capture_generation": 1, "cleanup_generation": 2}),
		}},
		{name: KindSandboxMemoryProjection, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxMemoryProjection,
			PartitionKey: FormatSandboxMemoryPartitionKey(ws, "mem_sandbox_queue_shapes"),
			DedupeKey:    FormatSandboxMemoryProjectionDedupeKey(ws, "mem_sandbox_queue_shapes", "mwrite_sandbox_queue_shapes"),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "memory_store_id": "mem_sandbox_queue_shapes", "memory_write_id": "mwrite_sandbox_queue_shapes"}),
		}},
		{name: KindSandboxBackgroundCommand, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxBackgroundCommand,
			PartitionKey: FormatSandboxBackgroundPartitionKey(ws, sessionID, taskID),
			DedupeKey:    FormatSandboxBackgroundCommandDedupeKey(ws, sessionID, taskID, "request_sandbox_queue_shapes"),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "task_id": taskID, "request_id": "request_sandbox_queue_shapes"}),
		}},
		{name: KindSandboxBackgroundReconcile, request: EnqueueRequest{
			WorkspaceID: ws, Kind: KindSandboxBackgroundReconcile,
			PartitionKey: FormatSandboxBackgroundPartitionKey(ws, sessionID, taskID),
			DedupeKey:    FormatSandboxBackgroundReconcileDedupeKey(ws, sessionID, taskID, 3),
			PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": sessionID, "task_id": taskID, "reconcile_generation": 3}),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.request.MaxAttempts = 3
			if _, err := NormalizeEnqueueRequest(test.request); err != nil {
				t.Fatalf("NormalizeEnqueueRequest: %v", err)
			}
			withoutBudget := test.request
			withoutBudget.MaxAttempts = 0
			if _, err := NormalizeEnqueueRequest(withoutBudget); !IsValidationError(err) || !strings.Contains(err.Error(), "max_attempts") {
				t.Fatalf("NormalizeEnqueueRequest without explicit budget = %v; want max_attempts validation", err)
			}
			wrongPartition := test.request
			wrongPartition.PartitionKey += ":wrong"
			if _, err := NormalizeEnqueueRequest(wrongPartition); !IsValidationError(err) || !strings.Contains(err.Error(), "partition_key") {
				t.Fatalf("NormalizeEnqueueRequest wrong partition = %v; want partition validation", err)
			}
			wrongDedupe := test.request
			wrongDedupe.DedupeKey += ":wrong"
			if _, err := NormalizeEnqueueRequest(wrongDedupe); !IsValidationError(err) || !strings.Contains(err.Error(), "dedupe_key") {
				t.Fatalf("NormalizeEnqueueRequest wrong dedupe = %v; want dedupe validation", err)
			}
			var payload map[string]any
			if err := json.Unmarshal(test.request.PayloadJSON, &payload); err != nil {
				t.Fatalf("decode canonical payload: %v", err)
			}
			payload["content"] = "must stay in the durable business row"
			withContent := test.request
			withContent.PayloadJSON = queuePayload(t, payload)
			if _, err := NormalizeEnqueueRequest(withContent); !IsValidationError(err) || !strings.Contains(err.Error(), "unsupported field") {
				t.Fatalf("NormalizeEnqueueRequest content payload = %v; want refs-only validation", err)
			}
			for _, key := range sandboxStringReferenceFields(test.name) {
				var malformed map[string]any
				if err := json.Unmarshal(test.request.PayloadJSON, &malformed); err != nil {
					t.Fatalf("decode canonical payload: %v", err)
				}
				malformed[key] = float64(7)
				withNumericReference := test.request
				withNumericReference.PayloadJSON = queuePayload(t, malformed)
				if _, err := NormalizeEnqueueRequest(withNumericReference); !IsValidationError(err) {
					t.Fatalf("NormalizeEnqueueRequest numeric %s = %v; want validation error", key, err)
				}
			}
		})
	}
}

func sandboxStringReferenceFields(kind string) []string {
	switch kind {
	case KindSandboxToolExecute, KindSandboxToolCancel:
		return []string{"workspace_id", "session_id", "session_thread_id", "tool_use_event_id"}
	case KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease:
		return []string{"workspace_id", "session_id", "logical_sandbox_id", "operation_id"}
	case KindSandboxOutputCapture, KindSandboxOutputCaptureCleanup:
		return []string{"workspace_id", "session_id", "finish_idle_write_id"}
	case KindSandboxMemoryProjection:
		return []string{"workspace_id", "session_id", "memory_store_id", "memory_write_id"}
	case KindSandboxBackgroundCommand:
		return []string{"workspace_id", "session_id", "task_id", "request_id"}
	case KindSandboxBackgroundReconcile:
		return []string{"workspace_id", "session_id", "task_id"}
	default:
		return nil
	}
}

func TestNormalizeEnqueueRequestRejectsOversizedPayloadForEveryJobKind(t *testing.T) {
	kinds := []string{
		KindRuntimeInput,
		KindRuntimeConfigUpdate,
		KindCleanupSession,
		KindSessionDeleteCleanup,
		KindSessionPrepare,
		KindEnvironmentBuild,
		KindEnvironmentReadyFanout,
		KindEnvironmentFailedFanout,
		KindSandboxToolExecute,
		KindSandboxActivate,
		KindSandboxMaterialize,
		KindSandboxRelease,
		KindSandboxToolCancel,
		KindSandboxOutputCapture,
		KindSandboxOutputCaptureCleanup,
		KindSandboxMemoryProjection,
		KindSandboxBackgroundCommand,
		KindSandboxBackgroundReconcile,
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			_, err := NormalizeEnqueueRequest(EnqueueRequest{
				WorkspaceID:  workspace.ID("ws_payload_cap"),
				Kind:         kind,
				PartitionKey: "partition",
				DedupeKey:    "dedupe",
				PayloadJSON:  bytes.Repeat([]byte("x"), MaxQueueJobPayloadBytes+1),
			})
			if !IsValidationError(err) || !strings.Contains(err.Error(), "maximum queue job payload size") {
				t.Fatalf("NormalizeEnqueueRequest error = %v; want all-kind payload-cap rejection", err)
			}
		})
	}
}

func TestLeaseBatchCapacityIsDerivedFromScopedTransportConstants(t *testing.T) {
	if MaxQueueJobPayloadBytes != 64*1024 {
		t.Fatalf("max queue payload = %d; want 64 KiB reference fuse", MaxQueueJobPayloadBytes)
	}
	want := (sessionrpc.MaxQueueLeaseGRPCMessageBytes - QueueLeaseResponseFixedOverhead) /
		(MaxQueueJobPayloadBytes + QueueJobEnvelopeAllowance())
	if got := MaxQueueLeaseJobs(); got != want {
		t.Fatalf("max queue lease jobs = %d; want derived ceiling %d", got, want)
	}
	if err := ValidateLeaseBatchSize(MaxQueueLeaseJobs()); err != nil {
		t.Fatalf("ValidateLeaseBatchSize(maximum): %v", err)
	}
	if err := ValidateLeaseBatchSize(MaxQueueLeaseJobs() + 1); !IsValidationError(err) {
		t.Fatalf("ValidateLeaseBatchSize(over maximum) = %v; want validation error", err)
	}
	if err := ValidateLeaseRequest(LeaseRequest{
		WorkspaceID: "ws_batch_cap", Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge",
		MaxJobs: MaxQueueLeaseJobs() + 1, LeaseDuration: time.Minute,
	}); !IsValidationError(err) {
		t.Fatalf("ValidateLeaseRequest(over maximum) = %v; want queue-boundary validation error", err)
	}
}

func TestQueueAcceptBoundariesRejectEveryOversizedVariableRequestField(t *testing.T) {
	basePayload := []byte(`{"workspace_id":"ws_bounds","session_id":"sesn_bounds","cleanup_job_id":"cleanup_bounds"}`)
	base := EnqueueRequest{
		WorkspaceID:  "ws_bounds",
		Kind:         KindCleanupSession,
		PartitionKey: FormatSessionPartitionKey("ws_bounds", "sesn_bounds"),
		DedupeKey:    FormatCleanupSessionDedupeKey("ws_bounds", "sesn_bounds", "cleanup_bounds"),
		PayloadJSON:  basePayload,
	}
	for _, test := range []struct {
		name   string
		mutate func(*EnqueueRequest)
		want   string
	}{
		{name: "workspace", mutate: func(request *EnqueueRequest) {
			request.WorkspaceID = workspace.ID(strings.Repeat("w", workspace.MaxWorkspaceIDBytes+1))
		}, want: "workspace_id"},
		{name: "partition key", mutate: func(request *EnqueueRequest) {
			request.PartitionKey = strings.Repeat("p", MaxQueuePartitionKeyBytes+1)
		}, want: "partition_key"},
		{name: "dedupe key", mutate: func(request *EnqueueRequest) {
			request.DedupeKey = strings.Repeat("d", MaxQueueDedupeKeyBytes+1)
		}, want: "dedupe_key"},
		{name: "job id", mutate: func(request *EnqueueRequest) {
			request.ID = strings.Repeat("j", MaxQueueJobIDBytes+1)
		}, want: "id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := base
			test.mutate(&request)
			_, err := NormalizeEnqueueRequest(request)
			if !IsValidationError(err) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NormalizeEnqueueRequest error = %v; want %s bound", err, test.want)
			}
		})
	}

	for _, test := range []struct {
		name    string
		request LeaseRequest
		want    string
	}{
		{name: "workspace", request: LeaseRequest{
			WorkspaceID: workspace.ID(strings.Repeat("w", workspace.MaxWorkspaceIDBytes+1)), Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute,
		}, want: "workspace_id"},
		{name: "lease owner", request: LeaseRequest{
			WorkspaceID: "ws_bounds", Kinds: []string{KindRuntimeInput}, LeaseOwner: strings.Repeat("l", MaxQueueLeaseOwnerBytes+1), MaxJobs: 1, LeaseDuration: time.Minute,
		}, want: "lease_owner"},
	} {
		t.Run("lease "+test.name, func(t *testing.T) {
			if err := ValidateLeaseRequest(test.request); !IsValidationError(err) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateLeaseRequest error = %v; want %s bound", err, test.want)
			}
		})
	}
}
