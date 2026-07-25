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

func TestNormalizeEnqueueRequestRejectsRuntimeInputBeyondEventReferenceLimit(t *testing.T) {
	eventIDs := make([]string, MaxRuntimeInputEventRefsPerJob+1)
	for i := range eventIDs {
		eventIDs[i] = "sevt_event_reference"
	}
	payload, err := json.Marshal(map[string]any{
		"workspace_id":           "ws_event_reference_limit",
		"session_id":             "sesn_event_reference_limit",
		"session_thread_id":      "thrd_event_reference_limit",
		"runtime_input_id":       "rin_event_reference_limit",
		"event_ids":              eventIDs,
		"sequence_from":          1,
		"sequence_to":            len(eventIDs),
		"input_kind":             "messages",
		"preparation_attempt_id": "prep_event_reference_limit",
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
		"workspace_id":           "ws_agent_mail",
		"session_id":             "sesn_agent_mail",
		"session_thread_id":      "thrd_agent_mail_main",
		"runtime_input_id":       "agent_mail:delivery_agent_mail",
		"event_ids":              []string{},
		"sequence_from":          0,
		"sequence_to":            0,
		"input_kind":             "agent_mail",
		"preparation_attempt_id": "prep_agent_mail",
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
