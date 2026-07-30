package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeConfigRebuildsPreserveDataBytesWithinCommandFuses(t *testing.T) {
	buildInstalledPolicy := func(configCount int) string {
		configs := make([]map[string]any, configCount)
		for index := range configs {
			configs[index] = map[string]any{
				"name":    fmt.Sprintf("cfg_%06d_%s", index, strings.Repeat("&<", 55)),
				"enabled": true,
			}
		}
		body, err := json.Marshal(map[string]any{
			"tools": []any{
				map[string]any{"type": "tetral_agent_toolset", "family": "claude"},
				map[string]any{"type": "mcp_toolset", "mcp_server_name": "github", "configs": configs},
			},
			"mcp_servers": []any{map[string]any{"type": "url", "name": "github", "url": "https://api.githubcopilot.com/mcp/"}},
		})
		if err != nil {
			t.Fatalf("marshal installed policy: %v", err)
		}
		return string(body)
	}
	const targetPolicyBytes = (1 << 20) - 2048
	low, high := 1, 10_000
	for low < high {
		middle := low + (high-low+1)/2
		if len(buildInstalledPolicy(middle)) <= targetPolicyBytes {
			low = middle
		} else {
			high = middle - 1
		}
	}
	installedPolicy := buildInstalledPolicy(low)
	config, err := effectiveBridgeRuntimeAgentConfig("", installedPolicy)
	if err != nil {
		t.Fatalf("decode maximum policy: %v", err)
	}
	policyPayload, err := marshalBridgeDataJSON(map[string]any{
		"workspace_id":      "default",
		"session_id":        "sesn_policy_fuse",
		"config_generation": 1,
		"approval_mode":     "approve_for_me",
		"tool_policy":       bridgeRuntimeToolPolicy("approve_for_me", config),
	})
	if err != nil {
		t.Fatalf("marshal rebuilt policy: %v", err)
	}
	assertRuntimeCommandDataPayloadFits(t, policyPayload)

	const toolsPrefix = `[{"name":"search","description":"`
	const toolsSuffix = `","input_schema":{"type":"object"}}]`
	toolsJSON := toolsPrefix + strings.Repeat("&", queue.MaxMcpManifestBytes-len(toolsPrefix)-len(toolsSuffix)) + toolsSuffix
	manifestPayload, err := runtimeMCPManifestCommandPayload("default", "sesn_manifest_fuse", "github", mcpManifestRow{
		ToolsJSON:    sql.NullString{String: toolsJSON, Valid: true},
		ManifestETag: sql.NullString{String: "etag_manifest_fuse", Valid: true},
		Generation:   1,
		Readiness:    mcpManifestReadinessReady,
	})
	if err != nil {
		t.Fatalf("marshal rebuilt manifest: %v", err)
	}
	assertRuntimeCommandDataPayloadFits(t, manifestPayload)
}

func TestRuntimeMCPManifestCommandCarriesNoToolPolicyKeys(t *testing.T) {
	payload, err := runtimeMCPManifestCommandPayload("default", "sesn_manifest_policy", "github", mcpManifestRow{
		ToolsJSON:    sql.NullString{String: `[{"name":"github_search","description":"Search GitHub","input_schema":{"type":"object"}}]`, Valid: true},
		ManifestETag: sql.NullString{String: "etag_manifest_policy", Valid: true},
		Generation:   3,
		Readiness:    mcpManifestReadinessReady,
	})
	if err != nil {
		t.Fatalf("build MCP manifest command: %v", err)
	}
	var envelope struct {
		Manifest map[string]json.RawMessage `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
		t.Fatalf("decode MCP manifest command: %v", err)
	}
	for _, forbidden := range []string{"default_config", "configs"} {
		if _, exists := envelope.Manifest[forbidden]; exists {
			t.Fatalf("MCP manifest command carries policy key %q: %s", forbidden, payload)
		}
	}
}

func assertRuntimeCommandDataPayloadFits(t *testing.T, payload string) {
	t.Helper()
	for _, escaped := range []string{`\u0026`, `\u003c`} {
		if strings.Contains(payload, escaped) {
			t.Fatalf("rebuilt command payload contains HTML escape %q", escaped)
		}
	}
	if len(payload) > 2*1024*1024 {
		t.Fatalf("rebuilt payload bytes = %d; want at most 2 MiB", len(payload))
	}
	request := &agentruntimev1.RuntimeInputCommandRequest{
		RequestId:      "qjob_payload_fuse:lease_payload_fuse",
		WorkspaceId:    "default",
		SessionId:      "sesn_payload_fuse",
		RuntimeInputId: "runtime_config_update:payload_fuse:1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
		PayloadJson:    payload,
	}
	if got := proto.Size(request); got > sessionrpc.MaxRuntimeCommandGRPCMessageBytes {
		t.Fatalf("rebuilt command bytes = %d; want at most %d", got, sessionrpc.MaxRuntimeCommandGRPCMessageBytes)
	}
}

func TestRuntimePodDirectDelivererSendsPreparedRuntimeCommand(t *testing.T) {
	request := &agentruntimev1.RuntimeInputCommandRequest{
		RequestId:      "qjob_1:lease_1",
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
	}
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:  RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
		Request: request,
	}}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:         agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
	}}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery status = %s; want accepted", result.Status)
	}
	if len(sender.requests) != 1 || sender.requests[0] != request {
		t.Fatalf("sent requests = %#v; want prepared request", sender.requests)
	}
	if !reflect.DeepEqual(sender.targets[0], store.plan.Target) {
		t.Fatalf("sent target = %#v; want %#v", sender.targets[0], store.plan.Target)
	}
	if len(store.acceptedJobs) != 1 || store.acceptedJobs[0].RuntimeInputID != "rin_1" {
		t.Fatalf("accepted jobs = %#v; want rin_1 marked accepted", store.acceptedJobs)
	}
}

func TestBackgroundTaskProcessTerminalStatusFactsRemainDistinct(t *testing.T) {
	t.Parallel()

	for _, status := range []string{
		"completed",
		"failed",
		"cancelled",
		"expired",
	} {
		status := status
		t.Run(status, func(t *testing.T) {
			t.Parallel()
			if got := normalizeBackgroundTaskTerminalStatus(status); got != status {
				t.Fatalf("normalizeBackgroundTaskTerminalStatus(%q) = %q; want unchanged", status, got)
			}
		})
	}

	for _, status := range []string{"cancelled_by_cleanup", "stale", "unknown"} {
		if got := normalizeBackgroundTaskTerminalStatus(status); got != "" {
			t.Fatalf("normalizeBackgroundTaskTerminalStatus(%q) = %q; want rejection", status, got)
		}
		if _, err := terminalStatusFromResultJSON(`{"status":"` + status + `"}`); err == nil {
			t.Fatalf("terminalStatusFromResultJSON accepted process status %q", status)
		}
	}
}

func TestRuntimePodDirectDelivererStaleAcceptedSkipsRuntimeCommand(t *testing.T) {
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{StaleAccepted: true}}
	sender := &recordingRuntimeCommandSender{}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("delivery status = %s; want duplicate for stale accepted input", result.Status)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("sent stale command count = %d; want 0", len(sender.requests))
	}
}

func TestRuntimePodDirectDelivererRejectsWhenAcceptedMarkerFails(t *testing.T) {
	store := &recordingRuntimeDeliveryStore{
		plan: RuntimeCommandPlan{
			Target: RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			Request: &agentruntimev1.RuntimeInputCommandRequest{
				RequestId:      "qjob_1:lease_1",
				WorkspaceId:    "ws_bridge",
				SessionId:      "sesn_1",
				RuntimeInputId: "rin_1",
				CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			},
		},
		acceptedErr: runtimeDeliveryPrepareError{kind: "runtime_inbox_accept_missing", message: "runtime inbox row is missing", retryable: true},
	}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:         agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
	}}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "runtime_inbox_accept_missing" {
		t.Fatalf("delivery result = %#v; want retryable runtime_inbox_accept_missing rejection", result)
	}
}

func TestRuntimePodDirectDelivererRecordsNonRetryableRejectedRuntimeInput(t *testing.T) {
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target: RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
		Request: &agentruntimev1.RuntimeInputCommandRequest{
			RequestId:      "qjob_1:lease_1",
			WorkspaceId:    "ws_bridge",
			SessionId:      "sesn_1",
			RuntimeInputId: "rin_1",
			CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		},
	}}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:         agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
		Retryable:      false,
		ErrorCode:      agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_INPUT_IDENTITY_CONFLICT,
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
	}}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "runtime_input_identity_conflict" {
		t.Fatalf("delivery result = %#v; want terminal runtime_input_identity_conflict", result)
	}
	if len(store.acceptedJobs) != 0 {
		t.Fatalf("accepted jobs = %#v; want none for rejected input", store.acceptedJobs)
	}
	if len(store.rejectedJobs) != 1 || store.rejectedJobs[0].RuntimeInputID != "rin_1" {
		t.Fatalf("rejected jobs = %#v; want rin_1 recorded", store.rejectedJobs)
	}
	if len(store.rejectedResults) != 1 || store.rejectedResults[0].ErrorKind != "runtime_input_identity_conflict" {
		t.Fatalf("rejected results = %#v; want runtime_input_identity_conflict recorded", store.rejectedResults)
	}
}

func TestRuntimePodDirectDelivererRedeliversBoundedRejectionToTheLoop(t *testing.T) {
	originalRequest := &agentruntimev1.RuntimeInputCommandRequest{
		RequestId:      "qjob_1:lease_1",
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJson:    strings.Repeat("x", 1024),
	}
	rejectionRequest := proto.Clone(originalRequest).(*agentruntimev1.RuntimeInputCommandRequest)
	rejectionRequest.PayloadJson = `{"input_kind":"rejection","reason_code":"runtime_command_rejected"}`
	store := &recordingRuntimeDeliveryStore{
		plan: RuntimeCommandPlan{
			Target:  RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			Request: originalRequest,
		},
		rejectionPlan: &RuntimeCommandPlan{
			Target:  RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			Request: rejectionRequest,
		},
		convertRejection: true,
	}
	sender := &recordingRuntimeCommandSender{responses: []*agentruntimev1.RuntimeInputCommandResponse{{
		Status:    agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
		Retryable: false,
		ErrorCode: agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_REJECTED_INPUT,
	}, {
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery result = %#v; want bounded rejection accepted by loop", result)
	}
	if len(sender.requests) != 2 ||
		sender.requests[0].GetPayloadJson() != originalRequest.GetPayloadJson() ||
		sender.requests[1].GetPayloadJson() != rejectionRequest.GetPayloadJson() {
		t.Fatalf("runtime requests = %#v; want original then bounded rejection", sender.requests)
	}
	if len(store.acceptedJobs) != 1 {
		t.Fatalf("accepted jobs = %#v; want bounded rejection inbox acceptance", store.acceptedJobs)
	}
}

func TestRuntimePodDirectDelivererFinalizesCleanupWhenTargetIsProvenGone(t *testing.T) {
	store := &recordingRuntimeDeliveryStore{
		plan:          RuntimeCommandPlan{CleanupTargetGone: true},
		cleanupResult: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted},
	}

	result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), cleanupRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob cleanup gone: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("cleanup gone delivery status = %s; want accepted", result.Status)
	}
	if len(store.cleanupJobs) != 1 || store.cleanupJobs[0].CleanupJobID != "cleanup_1" {
		t.Fatalf("cleanup finalizer jobs = %#v; want cleanup_1", store.cleanupJobs)
	}
}

func TestRuntimePodDirectDelivererSendsTaskNotificationRuntimeCommand(t *testing.T) {
	prepared := &agentruntimev1.RuntimeInputCommandRequest{
		RequestId:      "qjob_task:lease_task",
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "task_notification:task_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
		PayloadJson:    `{"task_id":"task_1","source_tool_use_event_id":"sevt_1","status":"completed"}`,
	}
	store := &recordingRuntimeDeliveryStore{
		plan: RuntimeCommandPlan{
			Target:           RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			Request:          prepared,
			TaskNotification: &RuntimeTaskNotificationPlan{TaskID: "task_1", SourceToolUseEventID: "sevt_1", ResultJSON: prepared.PayloadJson},
		},
	}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:         agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:      "sesn_1",
		RuntimeInputId: "task_notification:task_1",
	}}
	job := runtimeInputRuntimeJob()
	job.RuntimeInputID = "task_notification:task_1"
	job.InputKind = "task_notification"
	job.CommandKind = agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION
	job.EventIDs = nil
	job.SequenceFrom = 0
	job.SequenceTo = 0

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob task notification: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery status = %s; want accepted", result.Status)
	}
	if len(sender.requests) != 1 || sender.requests[0].GetPayloadJson() != prepared.PayloadJson {
		t.Fatalf("sent task notification payload = %#v; want committed payload", sender.requests)
	}
	if len(store.acceptedJobs) != 1 || store.acceptedJobs[0].RuntimeInputID != "task_notification:task_1" {
		t.Fatalf("accepted jobs = %#v; want task notification marked accepted", store.acceptedJobs)
	}
}

func TestRuntimePodDirectDelivererMapsPrepareFailuresWithoutAck(t *testing.T) {
	store := &recordingRuntimeDeliveryStore{err: runtimeDeliveryPrepareError{
		kind:      "runtime_binding_unavailable",
		message:   "runtime binding is unavailable",
		retryable: true,
	}}
	sender := &recordingRuntimeCommandSender{}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "runtime_binding_unavailable" {
		t.Fatalf("delivery result = %#v; want retryable runtime_binding_unavailable", result)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("sent command after prepare failure = %d; want 0", len(sender.requests))
	}
}

func TestRuntimePodDirectDelivererPropagatesTransportUncertainty(t *testing.T) {
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:  RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
		Request: &agentruntimev1.RuntimeInputCommandRequest{CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES},
	}}
	senderErr := errors.New("connection refused")
	sender := &recordingRuntimeCommandSender{err: senderErr}

	_, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if !errors.Is(err, senderErr) {
		t.Fatalf("DeliverRuntimeJob error = %v; want transport uncertainty", err)
	}
}

func TestRuntimePodDirectDelivererClassifiesRuntimeCommandStatusErrors(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantRetryable bool
		wantKind      string
		wantErr       bool
	}{
		{
			name:     "invalid argument dead letters",
			err:      status.Error(codes.InvalidArgument, "invalid envelope"),
			wantKind: "runtime_command_invalid_argument",
		},
		{
			name:     "internal invariant dead letters",
			err:      status.Error(codes.Internal, "invariant failed"),
			wantKind: "runtime_command_internal_invariant",
		},
		{
			name:    "unavailable stays uncertain",
			err:     status.Error(codes.Unavailable, "pod unavailable"),
			wantErr: true,
		},
		{
			name:    "deadline exceeded stays uncertain",
			err:     status.Error(codes.DeadlineExceeded, "deadline"),
			wantErr: true,
		},
		{
			name:    "resource exhausted stays uncertain",
			err:     status.Error(codes.ResourceExhausted, "backpressure"),
			wantErr: true,
		},
		{
			name:    "failed precondition stays uncertain",
			err:     status.Error(codes.FailedPrecondition, "pod not ready"),
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
				Target:  RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
				Request: &agentruntimev1.RuntimeInputCommandRequest{CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES},
			}}
			sender := &recordingRuntimeCommandSender{err: test.err}

			result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
			if test.wantErr {
				if !errors.Is(err, test.err) {
					t.Fatalf("DeliverRuntimeJob error = %v; want original uncertainty %v", err, test.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeliverRuntimeJob: %v", err)
			}
			if result.Status != RuntimeDeliveryRejected || result.Retryable != test.wantRetryable || result.ErrorKind != test.wantKind {
				t.Fatalf("delivery result = %#v; want rejected retryable=%v kind=%s", result, test.wantRetryable, test.wantKind)
			}
			if len(store.rejectedJobs) != 1 || store.rejectedJobs[0].RuntimeInputID != "rin_1" {
				t.Fatalf("rejected jobs = %#v; want terminal runtime command rejection recorded", store.rejectedJobs)
			}
		})
	}
}

func TestRuntimePodDirectDelivererDeadLettersOversizedCommandBeforeTransport(t *testing.T) {
	tokenSource := &countingRuntimeCommandTokenSource{}
	request := &agentruntimev1.RuntimeInputCommandRequest{
		RequestId:      "qjob_oversized:lease_oversized",
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_oversized",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJson:    strings.Repeat("x", 4*1024*1024),
	}
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:  RuntimePodTarget{PodIP: "127.0.0.1", Port: 9090},
		Request: request,
	}}

	result, err := (RuntimePodDirectDeliverer{
		Store:  store,
		Sender: NewRuntimePodCommandClient(tokenSource),
	}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob oversized command: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "runtime_command_payload_too_large" {
		t.Fatalf("delivery result = %#v; want terminal runtime_command_payload_too_large", result)
	}
	if tokenSource.calls != 0 {
		t.Fatalf("token calls = %d; want zero before transport", tokenSource.calls)
	}
	if len(store.rejectedResults) != 1 || store.rejectedResults[0].ErrorKind != "runtime_command_payload_too_large" {
		t.Fatalf("recorded rejections = %#v; want deterministic oversized result", store.rejectedResults)
	}
}

func runtimeInputRuntimeJob() RuntimeJob {
	return RuntimeJob{
		JobID:           "qjob_1",
		LeaseToken:      "lease_1",
		Kind:            "runtime_input",
		WorkspaceID:     "ws_bridge",
		SessionID:       "sesn_1",
		SessionThreadID: "thr_1",
		RuntimeInputID:  "rin_1",
		EventIDs:        []string{"evt_1"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"ws_bridge","session_id":"sesn_1"}`,
	}
}

func cleanupRuntimeJob() RuntimeJob {
	return RuntimeJob{
		JobID:          "qjob_cleanup_1",
		LeaseToken:     "lease_cleanup_1",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "ws_bridge",
		SessionID:      "sesn_1",
		RuntimeInputID: "cleanup_session:cleanup_1",
		CleanupJobID:   "cleanup_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"ws_bridge","session_id":"sesn_1","cleanup_job_id":"cleanup_1"}`,
	}
}

type recordingRuntimeDeliveryStore struct {
	plan             RuntimeCommandPlan
	rejectionPlan    *RuntimeCommandPlan
	cleanupResult    RuntimeDeliveryResult
	err              error
	acceptedErr      error
	jobs             []RuntimeJob
	acceptedJobs     []RuntimeJob
	rejectedJobs     []RuntimeJob
	rejectedResults  []RuntimeDeliveryResult
	cleanupJobs      []RuntimeJob
	convertRejection bool
}

func (s *recordingRuntimeDeliveryStore) PrepareRuntimeCommand(_ context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	s.jobs = append(s.jobs, job)
	if len(s.jobs) > 1 && s.rejectionPlan != nil {
		return *s.rejectionPlan, s.err
	}
	return s.plan, s.err
}

func (s *recordingRuntimeDeliveryStore) MarkRuntimeInputAccepted(_ context.Context, job RuntimeJob, _ *agentruntimev1.RuntimeInputCommandRequest) error {
	s.acceptedJobs = append(s.acceptedJobs, job)
	return s.acceptedErr
}

func (s *recordingRuntimeDeliveryStore) PrepareRuntimeInputRejection(_ context.Context, job RuntimeJob, result RuntimeDeliveryResult) (bool, error) {
	s.rejectedJobs = append(s.rejectedJobs, job)
	s.rejectedResults = append(s.rejectedResults, result)
	converted := s.convertRejection
	s.convertRejection = false
	return converted, nil
}

func (s *recordingRuntimeDeliveryStore) FinalizeRuntimeCleanup(_ context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	s.cleanupJobs = append(s.cleanupJobs, job)
	if s.cleanupResult.Status == "" {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
	}
	return s.cleanupResult, nil
}

type recordingRuntimeCommandSender struct {
	response  *agentruntimev1.RuntimeInputCommandResponse
	responses []*agentruntimev1.RuntimeInputCommandResponse
	err       error
	targets   []RuntimePodTarget
	requests  []*agentruntimev1.RuntimeInputCommandRequest
}

type countingRuntimeCommandTokenSource struct {
	calls int
}

func (s *countingRuntimeCommandTokenSource) Token(context.Context) (string, error) {
	s.calls++
	return "test-token", nil
}

func (s *recordingRuntimeCommandSender) SendRuntimeCommand(_ context.Context, target RuntimePodTarget, request *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	s.targets = append(s.targets, target)
	s.requests = append(s.requests, request)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.responses) > 0 {
		response := proto.Clone(s.responses[0]).(*agentruntimev1.RuntimeInputCommandResponse)
		s.responses = s.responses[1:]
		if response.SessionId == "" {
			response.SessionId = request.GetSessionId()
		}
		if response.RuntimeInputId == "" {
			response.RuntimeInputId = request.GetRuntimeInputId()
		}
		if response.BindingId == "" {
			response.BindingId = request.GetBindingId()
		}
		if response.BindingGeneration == 0 {
			response.BindingGeneration = request.GetBindingGeneration()
		}
		return response, nil
	}
	if s.response == nil {
		return nil, nil
	}
	response := proto.Clone(s.response).(*agentruntimev1.RuntimeInputCommandResponse)
	if response.SessionId == "" {
		response.SessionId = request.GetSessionId()
	}
	if response.RuntimeInputId == "" {
		response.RuntimeInputId = request.GetRuntimeInputId()
	}
	if response.BindingId == "" {
		response.BindingId = request.GetBindingId()
	}
	if response.BindingGeneration == 0 {
		response.BindingGeneration = request.GetBindingGeneration()
	}
	return response, nil
}
