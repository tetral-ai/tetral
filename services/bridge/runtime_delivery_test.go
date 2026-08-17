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
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testRuntimeInterruptLeaseRef(runtimeInputID string) *agentruntimev1.InterruptLeaseRef {
	return &agentruntimev1.InterruptLeaseRef{
		JobId: "qjob_" + runtimeInputID, LeaseToken: "lease_" + runtimeInputID,
		PartitionKey: "session:ws_bridge:sesn_1", DedupeKey: "runtime_input:ws_bridge:sesn_1:" + runtimeInputID,
	}
}

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
	request := &agentruntimev1.ApplyRuntimeConfigRequest{
		WorkspaceId: "default",
		SessionId:   "sesn_payload_fuse",
		Config: &agentruntimev1.ApplyRuntimeConfigRequest_SessionConfig{SessionConfig: &agentruntimev1.RuntimeSessionConfig{
			Generation:  1,
			ContentJson: payload,
		}},
	}
	if got := proto.Size(request); got > sessionrpc.MaxRuntimeCommandGRPCMessageBytes {
		t.Fatalf("rebuilt command bytes = %d; want at most %d", got, sessionrpc.MaxRuntimeCommandGRPCMessageBytes)
	}
}

func TestRuntimePodDirectDelivererSendsPreparedRuntimeCommand(t *testing.T) {
	request := &agentruntimev1.AcceptInputRequest{
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
		Content:        &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: `{}`},
	}
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:      RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
		AcceptInput: request,
	}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}

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

func TestRuntimeDeliveryMapsSessionBarrierStaleWithoutRetryOrAcceptance(t *testing.T) {
	results := []RuntimeDeliveryResult{
		runtimeResultFromAcceptInput(&agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Rejected{Rejected: &agentruntimev1.AcceptInputRejected{Reason: agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_SESSION_INTERRUPT_BARRIER_STALE}}}),
		runtimeResultFromAcceptAgentMail(&agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Rejected{Rejected: &agentruntimev1.AcceptAgentMailRejected{Reason: agentruntimev1.AcceptAgentMailFailure_ACCEPT_AGENT_MAIL_FAILURE_SESSION_INTERRUPT_BARRIER_STALE}}}),
		runtimeResultFromAcceptTask(&agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Rejected{Rejected: &agentruntimev1.AcceptTaskNotificationRejected{Reason: agentruntimev1.AcceptTaskNotificationFailure_ACCEPT_TASK_NOTIFICATION_FAILURE_SESSION_INTERRUPT_BARRIER_STALE}}}),
		runtimeResultFromToolConfirmation(&agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Rejected{Rejected: &agentruntimev1.ResolveToolConfirmationRejected{Reason: agentruntimev1.ResolveToolConfirmationFailure_RESOLVE_TOOL_CONFIRMATION_FAILURE_SESSION_INTERRUPT_BARRIER_STALE}}}),
	}
	for index, result := range results {
		if result.Status != RuntimeDeliveryBarrierStale || result.Retryable {
			t.Fatalf("barrier-stale result %d = %+v; want private nonretryable barrier stale", index, result)
		}
	}
	ordinaryStale := runtimeResultFromToolConfirmation(&agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Stale{Stale: &agentruntimev1.ResolveToolConfirmationStale{}}})
	if ordinaryStale.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("ordinary Tool confirmation stale = %+v; want duplicate settlement", ordinaryStale)
	}
}

func TestRuntimePodDirectDelivererSendsFirstInterruptOnce(t *testing.T) {
	request := &agentruntimev1.InterruptRequest{
		WorkspaceId: "ws_bridge", SessionId: "sesn_1", SessionThreadId: "thr_1",
		RuntimeInputId: "rin_interrupt_1", InterruptLeaseRef: testRuntimeInterruptLeaseRef("rin_interrupt_1"),
	}
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:    RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
		Interrupt: request,
	}, replayFound: true, replayResult: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	job := runtimeInputRuntimeJob()
	job.RuntimeInputID = request.GetRuntimeInputId()
	job.InputKind = "interrupt_control"

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("DeliverRuntimeJob first interrupt = %#v/%v; want accepted", result, err)
	}
	if len(store.jobs) != 1 {
		t.Fatalf("interrupt durable preparations = %d; want one owning preparation", len(store.jobs))
	}
	if len(sender.requests) != 1 || sender.requests[0] != request {
		t.Fatalf("first interrupt Runtime requests = %#v; want one exact command", sender.requests)
	}
	if len(store.acceptedJobs) != 0 || len(store.replayJobs) != 1 {
		t.Fatalf("first interrupt accepted writes/replays = %d/%d; want 0/1 after durable closeout", len(store.acceptedJobs), len(store.replayJobs))
	}
}

func TestRuntimePodDirectDelivererRevalidatesInterruptLeaseAfterPreparation(t *testing.T) {
	request := &agentruntimev1.InterruptRequest{
		WorkspaceId: "ws_bridge", SessionId: "sesn_1", SessionThreadId: "thr_1",
		RuntimeInputId: "rin_interrupt_lost_after_prepare", InterruptLeaseRef: testRuntimeInterruptLeaseRef("rin_interrupt_lost_after_prepare"),
	}
	store := &recordingRuntimeDeliveryStore{
		plan: RuntimeCommandPlan{
			Target:    RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
			Interrupt: request,
		},
		interruptAuthorityLost: true,
	}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	job := runtimeInputRuntimeJob()
	job.RuntimeInputID = request.GetRuntimeInputId()
	job.InputKind = "interrupt_control"

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob after authority loss: %v", err)
	}
	if result.Status != RuntimeDeliveryDuplicate || !result.QueueLeaseSettled {
		t.Fatalf("post-preparation authority loss = %+v; want settled duplicate", result)
	}
	if len(store.jobs) != 1 || len(sender.requests) != 0 || len(store.acceptedJobs) != 0 {
		t.Fatalf("post-preparation authority loss preparations/sends/accepted writes = %d/%d/%d; want 1/0/0",
			len(store.jobs), len(sender.requests), len(store.acceptedJobs))
	}
}

func TestRuntimePodDirectDelivererDoesNotSettleInterruptOnRuntimeAcceptanceAlone(t *testing.T) {
	request := &agentruntimev1.InterruptRequest{
		WorkspaceId: "ws_bridge", SessionId: "sesn_1", SessionThreadId: "thr_1",
		RuntimeInputId: "rin_interrupt_pending", InterruptLeaseRef: testRuntimeInterruptLeaseRef("rin_interrupt_pending"),
	}
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:    RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
		Interrupt: request,
	}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	job := runtimeInputRuntimeJob()
	job.RuntimeInputID = request.GetRuntimeInputId()
	job.InputKind = "interrupt_control"

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "interrupt_closeout_pending" {
		t.Fatalf("delivery result = %#v; want outcome-unknown closeout retry", result)
	}
	if len(sender.requests) != 1 || len(store.acceptedJobs) != 0 || len(store.replayJobs) != 1 {
		t.Fatalf("Runtime sends/accepted writes/receipt reads = %d/%d/%d; want 1/0/1", len(sender.requests), len(store.acceptedJobs), len(store.replayJobs))
	}
}

func TestRuntimeCommandPlanBoundsOnlyInterruptDeliveryWait(t *testing.T) {
	request := &agentruntimev1.InterruptRequest{
		WorkspaceId: "ws_bridge", SessionId: "sesn_timeout", SessionThreadId: "thr_timeout",
		RuntimeInputId: "rin_timeout", InterruptLeaseRef: testRuntimeInterruptLeaseRef("rin_timeout"),
	}
	sender := &recordingRuntimeCommandSender{observeInterruptDeadline: true}
	started := time.Now()
	_, err := (RuntimeCommandPlan{Target: RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090}, Interrupt: request}).send(context.Background(), sender)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("interrupt send error = %v; want deadline exceeded", err)
	}
	remaining := sender.interruptDeadline.Sub(started)
	if remaining < runtimeInterruptDeliveryTimeout-time.Second || remaining > runtimeInterruptDeliveryTimeout+time.Second {
		t.Fatalf("interrupt deadline = %s; want %s bound", remaining, runtimeInterruptDeliveryTimeout)
	}
	if len(sender.requests) != 1 || sender.requests[0] != request {
		t.Fatalf("bounded interrupt requests = %#v; want one exact command", sender.requests)
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
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		StaleAccepted: true,
		Target:        RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
		Interrupt: &agentruntimev1.InterruptRequest{
			WorkspaceId: "ws_bridge", SessionId: "sesn_1", SessionThreadId: "thr_1", RuntimeInputId: "rin_1",
		},
	}}
	sender := &recordingRuntimeCommandSender{}
	job := runtimeInputRuntimeJob()
	job.InputKind = "interrupt_control"

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
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
			AcceptInput: &agentruntimev1.AcceptInputRequest{
				WorkspaceId:    "ws_bridge",
				SessionId:      "sesn_1",
				RuntimeInputId: "rin_1",
				Content:        &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: `{}`},
			},
		},
		acceptedErr: runtimeDeliveryPrepareError{kind: "runtime_inbox_accept_missing", message: "runtime inbox row is missing", retryable: true},
	}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}

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
		AcceptInput: &agentruntimev1.AcceptInputRequest{
			WorkspaceId:    "ws_bridge",
			SessionId:      "sesn_1",
			RuntimeInputId: "rin_1",
			Content:        &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: `{}`},
		},
	}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "identity_conflict"}}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "identity_conflict" {
		t.Fatalf("delivery result = %#v; want terminal identity_conflict", result)
	}
	if len(store.acceptedJobs) != 0 {
		t.Fatalf("accepted jobs = %#v; want none for rejected input", store.acceptedJobs)
	}
	if len(store.rejectedJobs) != 1 || store.rejectedJobs[0].RuntimeInputID != "rin_1" {
		t.Fatalf("rejected jobs = %#v; want rin_1 recorded", store.rejectedJobs)
	}
	if len(store.rejectedResults) != 1 || store.rejectedResults[0].ErrorKind != "identity_conflict" {
		t.Fatalf("rejected results = %#v; want identity_conflict recorded", store.rejectedResults)
	}
}

func TestRuntimePodDirectDelivererRedeliversBoundedRejectionToTheLoop(t *testing.T) {
	originalRequest := &agentruntimev1.AcceptInputRequest{
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_1",
		Content:        &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: strings.Repeat("x", 1024)},
	}
	rejectionRequest := proto.Clone(originalRequest).(*agentruntimev1.AcceptInputRequest)
	rejectionRequest.Content = &agentruntimev1.AcceptInputRequest_Rejection{Rejection: &agentruntimev1.AcceptInputRejection{
		Reason: agentruntimev1.AcceptInputRejectionReason_ACCEPT_INPUT_REJECTION_REASON_RUNTIME_REJECTED,
	}}
	store := &recordingRuntimeDeliveryStore{
		plan: RuntimeCommandPlan{
			Target:      RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			AcceptInput: originalRequest,
		},
		rejectionPlan: &RuntimeCommandPlan{
			Target:      RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			AcceptInput: rejectionRequest,
		},
		convertRejection: true,
	}
	sender := &recordingRuntimeCommandSender{results: []RuntimeDeliveryResult{
		{Status: RuntimeDeliveryRejected, ErrorKind: "rejected_input"},
		{Status: RuntimeDeliveryAccepted},
	}}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery result = %#v; want bounded rejection accepted by loop", result)
	}
	if len(sender.requests) != 2 ||
		sender.requests[0].(*agentruntimev1.AcceptInputRequest).GetMessagesJson() != originalRequest.GetMessagesJson() ||
		sender.requests[1].(*agentruntimev1.AcceptInputRequest).GetRejection() == nil {
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
	prepared := &agentruntimev1.AcceptTaskNotificationRequest{
		WorkspaceId:      "ws_bridge",
		SessionId:        "sesn_1",
		RuntimeInputId:   "task_notification:task_1",
		NotificationJson: `{"task_id":"task_1","source_tool_use_event_id":"sevt_1","status":"completed"}`,
	}
	store := &recordingRuntimeDeliveryStore{
		plan: RuntimeCommandPlan{
			Target:           RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090, PodName: "runtime-a", PodUID: "uid-a", Namespace: "tetral-agent-runtime"},
			AcceptTask:       prepared,
			TaskNotification: &RuntimeTaskNotificationPlan{TaskID: "task_1", SourceToolUseEventID: "sevt_1", ResultJSON: prepared.NotificationJson},
		},
	}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	job := runtimeInputRuntimeJob()
	job.RuntimeInputID = "task_notification:task_1"
	job.InputKind = "task_notification"
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
	if len(sender.requests) != 1 || sender.requests[0].(*agentruntimev1.AcceptTaskNotificationRequest).GetNotificationJson() != prepared.NotificationJson {
		t.Fatalf("sent task notification payload = %#v; want prepared transport payload", sender.requests)
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
		Target:      RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
		AcceptInput: &agentruntimev1.AcceptInputRequest{},
	}}
	senderErr := errors.New("connection refused")
	sender := &recordingRuntimeCommandSender{err: senderErr}

	_, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputRuntimeJob())
	if !errors.Is(err, senderErr) {
		t.Fatalf("DeliverRuntimeJob error = %v; want transport uncertainty", err)
	}
}

func TestRuntimePodDirectDelivererClassifiesRuntimeTransportErrors(t *testing.T) {
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
				Target:      RuntimePodTarget{PodIP: "10.0.0.1", Port: 9090},
				AcceptInput: &agentruntimev1.AcceptInputRequest{},
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
	request := &agentruntimev1.AcceptInputRequest{
		WorkspaceId:    "ws_bridge",
		SessionId:      "sesn_1",
		RuntimeInputId: "rin_oversized",
		Content:        &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: strings.Repeat("x", 4*1024*1024)},
	}
	store := &recordingRuntimeDeliveryStore{plan: RuntimeCommandPlan{
		Target:      RuntimePodTarget{PodIP: "127.0.0.1", Port: 9090},
		AcceptInput: request,
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
		PayloadJSON:    `{"workspace_id":"ws_bridge","session_id":"sesn_1","cleanup_job_id":"cleanup_1"}`,
	}
}

type recordingRuntimeDeliveryStore struct {
	plan                   RuntimeCommandPlan
	rejectionPlan          *RuntimeCommandPlan
	cleanupResult          RuntimeDeliveryResult
	err                    error
	acceptedErr            error
	jobs                   []RuntimeJob
	acceptedJobs           []RuntimeJob
	rejectedJobs           []RuntimeJob
	rejectedResults        []RuntimeDeliveryResult
	cleanupJobs            []RuntimeJob
	convertRejection       bool
	replayFound            bool
	replayResult           RuntimeDeliveryResult
	replayErr              error
	replayJobs             []RuntimeJob
	interruptAuthorityLost bool
}

func (s *recordingRuntimeDeliveryStore) ReplayRuntimeDeliveryFinalization(_ context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	s.replayJobs = append(s.replayJobs, job)
	return s.replayResult, s.replayFound, s.replayErr
}

func (s *recordingRuntimeDeliveryStore) PrepareRuntimeCommand(_ context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	s.jobs = append(s.jobs, job)
	if len(s.jobs) > 1 && s.rejectionPlan != nil {
		return *s.rejectionPlan, s.err
	}
	return s.plan, s.err
}

func (s *recordingRuntimeDeliveryStore) InterruptDeliveryAuthorityActive(_ context.Context, _ RuntimeJob) (bool, error) {
	return !s.interruptAuthorityLost, nil
}

func (s *recordingRuntimeDeliveryStore) MarkRuntimeInputAccepted(_ context.Context, job RuntimeJob, _ RuntimeAttemptedBinding) (bool, error) {
	s.acceptedJobs = append(s.acceptedJobs, job)
	return false, s.acceptedErr
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
	result                   RuntimeDeliveryResult
	results                  []RuntimeDeliveryResult
	err                      error
	targets                  []RuntimePodTarget
	requests                 []proto.Message
	observeInterruptDeadline bool
	interruptDeadline        time.Time
}

type countingRuntimeCommandTokenSource struct {
	calls int
}

func (s *countingRuntimeCommandTokenSource) Token(context.Context) (string, error) {
	s.calls++
	return "test-token", nil
}

func (s *recordingRuntimeCommandSender) record(target RuntimePodTarget, request proto.Message) (RuntimeDeliveryResult, error) {
	s.targets = append(s.targets, target)
	s.requests = append(s.requests, request)
	if s.err != nil {
		return RuntimeDeliveryResult{}, s.err
	}
	if len(s.results) > 0 {
		result := s.results[0]
		s.results = s.results[1:]
		return result, nil
	}
	return s.result, nil
}

func (s *recordingRuntimeCommandSender) AcceptInput(_ context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryAccepted {
		return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Accepted{Accepted: &agentruntimev1.AcceptInputAccepted{}}}, nil
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Duplicate{Duplicate: &agentruntimev1.AcceptInputDuplicate{}}}, nil
	}
	return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Rejected{Rejected: &agentruntimev1.AcceptInputRejected{Reason: agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT, Retryable: result.Retryable}}}, nil
}
func (s *recordingRuntimeCommandSender) AcceptAgentMail(_ context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Duplicate{Duplicate: &agentruntimev1.AcceptAgentMailDuplicate{}}}, nil
	}
	if result.Status == RuntimeDeliveryRejected {
		return &agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Rejected{Rejected: &agentruntimev1.AcceptAgentMailRejected{Reason: agentruntimev1.AcceptAgentMailFailure_ACCEPT_AGENT_MAIL_FAILURE_IDENTITY_CONFLICT, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.AcceptAgentMailResponse{Outcome: &agentruntimev1.AcceptAgentMailResponse_Accepted{Accepted: &agentruntimev1.AcceptAgentMailAccepted{}}}, nil
}
func (s *recordingRuntimeCommandSender) AcceptTaskNotification(_ context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Duplicate{Duplicate: &agentruntimev1.AcceptTaskNotificationDuplicate{}}}, nil
	}
	if result.Status == RuntimeDeliveryRejected {
		return &agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Rejected{Rejected: &agentruntimev1.AcceptTaskNotificationRejected{Reason: agentruntimev1.AcceptTaskNotificationFailure_ACCEPT_TASK_NOTIFICATION_FAILURE_CONTROL_BUSY, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.AcceptTaskNotificationResponse{Outcome: &agentruntimev1.AcceptTaskNotificationResponse_Accepted{Accepted: &agentruntimev1.AcceptTaskNotificationAccepted{}}}, nil
}
func (s *recordingRuntimeCommandSender) Interrupt(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
	if s.observeInterruptDeadline {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, errors.New("interrupt context has no deadline")
		}
		s.interruptDeadline = deadline
		s.targets = append(s.targets, target)
		s.requests = append(s.requests, request)
		return nil, context.DeadlineExceeded
	}
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Duplicate{Duplicate: &agentruntimev1.InterruptDuplicate{}}}, nil
	}
	if result.Status == RuntimeDeliveryRejected {
		return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Rejected{Rejected: &agentruntimev1.InterruptRejected{Reason: agentruntimev1.InterruptFailure_INTERRUPT_FAILURE_CONTROL_BUSY, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Accepted{Accepted: &agentruntimev1.InterruptAccepted{}}}, nil
}
func (s *recordingRuntimeCommandSender) ResolveToolConfirmation(_ context.Context, target RuntimePodTarget, request *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Duplicate{Duplicate: &agentruntimev1.ResolveToolConfirmationDuplicate{}}}, nil
	}
	if result.Status == RuntimeDeliveryRejected {
		return &agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Rejected{Rejected: &agentruntimev1.ResolveToolConfirmationRejected{Reason: agentruntimev1.ResolveToolConfirmationFailure_RESOLVE_TOOL_CONFIRMATION_FAILURE_CONTROL_CONFLICT, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.ResolveToolConfirmationResponse{Outcome: &agentruntimev1.ResolveToolConfirmationResponse_Accepted{Accepted: &agentruntimev1.ResolveToolConfirmationAccepted{}}}, nil
}
func (s *recordingRuntimeCommandSender) ApplyRuntimeConfig(_ context.Context, target RuntimePodTarget, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Duplicate{Duplicate: &agentruntimev1.ApplyRuntimeConfigDuplicate{}}}, nil
	}
	if result.Status == RuntimeDeliveryRejected {
		return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Rejected{Rejected: &agentruntimev1.ApplyRuntimeConfigRejected{Reason: agentruntimev1.ApplyRuntimeConfigFailure_APPLY_RUNTIME_CONFIG_FAILURE_BINDING_MISMATCH, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Applied{Applied: &agentruntimev1.ApplyRuntimeConfigApplied{}}}, nil
}
func (s *recordingRuntimeCommandSender) CleanupSession(_ context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	result, err := s.record(target, request)
	if err != nil || result.Status == "" {
		return nil, err
	}
	if result.Status == RuntimeDeliveryDuplicate {
		return &agentruntimev1.CleanupSessionResponse{Outcome: &agentruntimev1.CleanupSessionResponse_Duplicate{Duplicate: &agentruntimev1.CleanupSessionDuplicate{}}}, nil
	}
	if result.Status == RuntimeDeliveryRejected {
		return &agentruntimev1.CleanupSessionResponse{Outcome: &agentruntimev1.CleanupSessionResponse_Rejected{Rejected: &agentruntimev1.CleanupSessionRejected{Reason: agentruntimev1.CleanupSessionFailure_CLEANUP_SESSION_FAILURE_BINDING_MISMATCH, Retryable: result.Retryable}}}, nil
	}
	return &agentruntimev1.CleanupSessionResponse{Outcome: &agentruntimev1.CleanupSessionResponse_Completed{Completed: &agentruntimev1.CleanupSessionCompleted{}}}, nil
}
