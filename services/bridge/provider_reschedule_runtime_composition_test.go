package agentruntimebridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type providerRescheduleRecoveryComposition struct {
	ResultType          string          `json:"resultType"`
	ProviderInvocations int             `json:"providerInvocations"`
	ExecutorInvocations int             `json:"executorInvocations"`
	WaitedMS            []int64         `json:"waitedMs"`
	ProviderContext     json.RawMessage `json:"providerContext"`
	PreloadResult       json.RawMessage `json:"preloadResult"`
	LastSnapshot        json.RawMessage `json:"lastSnapshot"`
}

func TestPostgreSQLProviderRescheduleColdRecoversCommittedToolWithoutReexecution(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_provider_reschedule_recovery"
		threadID        = "sthr_provider_reschedule_recovery"
		oldBindingID    = "bind_provider_reschedule_old"
		oldPodUID       = "pod_provider_reschedule_old"
		newBindingID    = "bind_provider_reschedule_new"
		newPodUID       = "pod_provider_reschedule_new"
		modelRequestID  = "mreq_provider_reschedule_original"
		modelToolCallID = "call_provider_reschedule_original"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_provider_reschedule_user", "sevt_provider_reschedule_user", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"read the original file"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed provider reschedule user context: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("provider-reschedule-recovery-signing-key")
	acceptedAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	oldScope := bridgeAPIScope(sessionID, threadID, oldBindingID, 1, oldPodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, oldScope, "evt_provider_reschedule_durable_turn")
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_provider_reschedule_start", modelRequestID, requestKindAgentProviderRequest, 1)

	if message, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_partial", ModelRequestId: modelRequestID,
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"discarded partial text"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("discarded partial text"),
	}); err != nil || message.GetCommitted() == nil {
		t.Fatalf("write failed request partial text: response=%#v err=%v", message, err)
	}
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_tool", ModelRequestId: modelRequestID,
		EventType:                   "agent.tool_use",
		PayloadJson:                 `{"type":"agent.tool_use","name":"Read","input":{"path":"original.txt"},"evaluated_permission":"allow"}`,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest(modelToolCallID, "Read", `{"path":"original.txt"}`),
		CanonicalExecutionInputJson: `{"path":"original.txt"}`,
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write original Tool Use: response=%#v err=%v", toolUse, err)
	}
	if accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: oldScope, ToolUseEventId: toolUse.GetCommitted().GetEventId(),
	}); err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("accept original Tool execution: response=%#v err=%v", accepted, err)
	}
	endRequest := &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_provider_reschedule_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}
	if committed, err := store.WriteRequestEnd(context.Background(), endRequest); err != nil || committed.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit provider reschedule before lost acknowledgement: response=%#v err=%v", committed, err)
	}
	replayed, err := store.WriteRequestEnd(context.Background(), endRequest)
	if err != nil || replayed.GetDuplicate().GetRescheduled() == nil {
		t.Fatalf("replay provider reschedule after lost acknowledgement: response=%#v err=%v", replayed, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json = jsonb_set(data_json::jsonb, '{parts}',
			(data_json::jsonb -> 'parts') || '[{"type":"tool_call","modelToolCallId":"call_uncommitted_fragment","toolName":"Write","canonicalInput":{"path":"never.txt"}}]'::jsonb)::text
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID); err != nil {
		t.Fatalf("seed uncommitted sibling Tool fragment: %v", err)
	}

	if result, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET binding_id=$2, binding_generation=2, agent_runtime_pod_uid=$3, updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, sessionID, newBindingID, newPodUID); err != nil {
		t.Fatalf("install replacement Runtime binding: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("replacement Runtime binding rows = %d err=%v", rows, rowsErr)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for provider reschedule recovery: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	preloaded := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": listener.Addr().String(),
		"workspaceId":   "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": newBindingID, "bindingGeneration": 2, "targetPodUid": newPodUID,
		"now":         acceptedAt.Add(250 * time.Millisecond).Format(time.RFC3339Nano),
		"preloadOnly": true,
	})
	if preloaded.ResultType != "preloaded" || preloaded.ProviderInvocations != 0 || preloaded.ExecutorInvocations != 0 ||
		!strings.Contains(string(preloaded.PreloadResult), `"ok":true`) ||
		!strings.Contains(string(preloaded.LastSnapshot), `"observed":true`) ||
		!strings.Contains(string(preloaded.LastSnapshot), `"hasUnsettledToolOwner":true`) ||
		strings.Contains(string(preloaded.LastSnapshot), "discarded partial text") ||
		strings.Contains(string(preloaded.LastSnapshot), "call_uncommitted_fragment") {
		t.Fatalf("replacement Runtime nonterminal preload = %+v snapshot=%s", preloaded, preloaded.LastSnapshot)
	}
	newScope := bridgeAPIScope(sessionID, threadID, newBindingID, 2, newPodUID)
	settleSandboxExecutionForHotReceiptProof(t, runtimeDB, admin, newScope, toolUse.GetCommitted().GetEventId(),
		`{"status":"success","result":{"content":"original result"}}`)
	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		newScope,
		bridgeCompletedToolSettlementForTest(toolUse.GetCommitted().GetEventId(), "original result"),
	))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle original Tool Result through replacement Runtime identity: response=%#v err=%v", settled, err)
	}

	captureSettled := make(chan error, 1)
	go func() {
		captureSettled <- settleOutputCaptureGenerationForTest(admin, sessionID, "evt_provider_reschedule_durable_turn", 1, "staged")
	}()
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": listener.Addr().String(),
		"workspaceId":   "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": newBindingID, "bindingGeneration": 2, "targetPodUid": newPodUID,
		"now": acceptedAt.Add(500 * time.Millisecond).Format(time.RFC3339Nano),
	})
	if result.ResultType != "completed" || result.ProviderInvocations != 1 || result.ExecutorInvocations != 0 {
		t.Fatalf("replacement Runtime recovery = %+v", result)
	}
	if err := <-captureSettled; err != nil {
		t.Fatalf("stage provider reschedule closeout capture: %v", err)
	}
	if len(result.WaitedMS) == 0 || result.WaitedMS[0] != 500 {
		t.Fatalf("replacement Runtime accepted-deadline wait = %v; want first wait 500ms", result.WaitedMS)
	}
	providerContext := string(result.ProviderContext)
	expectedProviderContext := `[{"role":1,"content":[{"text":{"text":"read the original file"}}]},{"role":2,"content":[{"toolCall":{"modelToolCallId":"call_provider_reschedule_original","name":"Read","inputJson":"{\"path\":\"original.txt\"}"}},{"toolResult":{"modelToolCallId":"call_provider_reschedule_original","completed":{"outputJson":"{\"text\":\"original result\"}"}}}]}]`
	if providerContext != expectedProviderContext {
		t.Fatalf("recovered provider context did not preserve the exact narrow Tool pair: %s", providerContext)
	}

	var starts, ends, toolUses, toolResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result')`, sessionID).
		Scan(&starts, &ends, &toolUses, &toolResults); err != nil {
		t.Fatalf("read provider reschedule recovery census: %v", err)
	}
	if starts != 2 || ends != 2 || toolUses != 1 || toolResults != 1 {
		t.Fatalf("provider reschedule recovery census starts/ends/tools/results = %d/%d/%d/%d", starts, ends, toolUses, toolResults)
	}
}

func TestPostgreSQLProviderRescheduleColdCarriesCreatedSubagentWithoutRecreation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_subagent_reschedule_recovery"
		threadID        = "sthr_subagent_reschedule_recovery"
		oldBindingID    = "bind_subagent_reschedule_old"
		newBindingID    = "bind_subagent_reschedule_new"
		newPodUID       = "pod_subagent_reschedule_new"
		modelRequestID  = "mreq_subagent_reschedule_original"
		modelToolCallID = "call_subagent_reschedule_original"
		taskName        = "recovery-worker"
		prompt          = "finish the durable task"
	)
	oldBinding := runtimePodLostBinding(sessionID, oldBindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_subagent_reschedule_user", "sevt_subagent_reschedule_user", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"spawn the durable worker"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed subagent reschedule user context: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldBinding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("subagent-reschedule-recovery-signing-key")
	acceptedAt := time.Date(2026, 8, 19, 13, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	oldScope := bridgeAPIScope(sessionID, threadID, oldBindingID, 1, oldBinding.PodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, oldScope, "evt_subagent_reschedule_durable_turn")
	seedBridgeAPIRequestStart(t, store, oldScope, "rwrite_subagent_reschedule_start", modelRequestID, requestKindAgentProviderRequest, 1)
	canonicalInput := `{"task_name":"recovery-worker","prompt":"finish the durable task","agent_type":"worker","fork_turns":"all"}`
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_subagent_reschedule_tool", ModelRequestId: modelRequestID,
		EventType:                   "agent.tool_use",
		PayloadJson:                 `{"type":"agent.tool_use","name":"spawn_agent","input":` + canonicalInput + `,"evaluated_permission":"allow"}`,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest(modelToolCallID, "spawn_agent", canonicalInput),
		CanonicalExecutionInputJson: canonicalInput,
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write spawn Tool Use: response=%#v err=%v", toolUse, err)
	}
	created, err := store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: oldScope, SourceToolUseEventId: toolUse.GetCommitted().GetEventId(), TaskName: taskName, AgentType: "worker", ForkTurns: "all",
	})
	if err != nil || created.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("create subagent before Request End: response=%#v err=%v", created, err)
	}
	childID := created.GetCommitted().GetChildThreadId()
	deliveryID := agentMailDeliveryID(toolUse.GetCommitted().GetEventId(), childID)
	if delivered, err := store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: oldScope, DeliveryId: deliveryID, TargetThreadId: childID,
		SourceToolUseEventId: toolUse.GetCommitted().GetEventId(), Content: prompt,
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("commit subagent delivery before Tool Result: response=%#v err=%v", delivered, err)
	}
	if ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: oldScope, RuntimeWriteId: "rwrite_subagent_reschedule_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}); err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit subagent provider reschedule: response=%#v err=%v", ended, err)
	}
	var resultsBeforeRepair int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
		  AND payload_json::jsonb->>'tool_use_event_id'=$2`, sessionID, toolUse.GetCommitted().GetEventId()).Scan(&resultsBeforeRepair); err != nil {
		t.Fatalf("count pre-repair subagent results: %v", err)
	}
	if resultsBeforeRepair != 0 {
		t.Fatalf("pre-repair subagent Tool Results = %d; want nonterminal", resultsBeforeRepair)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtimeDB, sessionID, oldBinding, acceptedAt.Add(250*time.Millisecond)); err != nil {
			t.Fatalf("repair committed subagent delivery attempt %d: %v", attempt+1, err)
		}
	}
	var sessionStatus, threadStatus, runtimeStatus string
	var providerAttempts int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1 AND binding_id=$3 AND binding_generation=1),
		(SELECT provider_attempts FROM session_turn_retries WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2)`,
		sessionID, threadID, oldBindingID).Scan(&sessionStatus, &threadStatus, &runtimeStatus, &providerAttempts); err != nil {
		t.Fatalf("read repaired reschedule ownership: %v", err)
	}
	if sessionStatus != "rescheduling" || threadStatus != "rescheduling" || runtimeStatus != "idle" || providerAttempts != 1 {
		t.Fatalf("repaired reschedule ownership session/thread/runtime/attempts = %s/%s/%s/%d",
			sessionStatus, threadStatus, runtimeStatus, providerAttempts)
	}
	if result, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET binding_id=$2, binding_generation=2, agent_runtime_pod_uid=$3, updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, sessionID, newBindingID, newPodUID); err != nil {
		t.Fatalf("install replacement subagent Runtime binding: %v", err)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("replacement subagent Runtime binding rows = %d err=%v", rows, rowsErr)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for subagent reschedule recovery: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	captureSettled := make(chan error, 1)
	go func() {
		captureSettled <- settleOutputCaptureGenerationForTest(admin, sessionID, "evt_subagent_reschedule_durable_turn", 1, "staged")
	}()
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": listener.Addr().String(),
		"workspaceId":   "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": newBindingID, "bindingGeneration": 2, "targetPodUid": newPodUID,
		"now": acceptedAt.Add(500 * time.Millisecond).Format(time.RFC3339Nano),
	})
	if result.ResultType != "completed" || result.ProviderInvocations != 1 || result.ExecutorInvocations != 0 {
		t.Fatalf("replacement Runtime subagent recovery = %+v", result)
	}
	if err := <-captureSettled; err != nil {
		t.Fatalf("stage subagent reschedule closeout capture: %v", err)
	}
	if len(result.WaitedMS) == 0 || result.WaitedMS[0] != 500 {
		t.Fatalf("subagent reschedule accepted-deadline wait = %v; want first wait 500ms", result.WaitedMS)
	}
	providerContext := string(result.ProviderContext)
	expectedProviderContext := fmt.Sprintf(`[{"role":1,"content":[{"text":{"text":"spawn the durable worker"}}]},{"role":2,"content":[{"toolCall":{"modelToolCallId":"call_subagent_reschedule_original","name":"spawn_agent","inputJson":"{\"agent_type\":\"worker\",\"fork_turns\":\"all\",\"prompt\":\"finish the durable task\",\"task_name\":\"recovery-worker\"}"}},{"toolResult":{"modelToolCallId":"call_subagent_reschedule_original","completed":{"outputJson":"{\"text\":\"task_name: recovery-worker\\nsession_thread_id: %s\\nstatus: delivered\"}"}}}]}]`, childID)
	if providerContext != expectedProviderContext {
		t.Fatalf("recovered provider context did not preserve one exact spawn pair: %s", providerContext)
	}
	var children, createOperations, toolUses, toolResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
		  AND payload_json::jsonb->>'tool_use_event_id'=$3)`, sessionID, threadID, toolUse.GetCommitted().GetEventId()).
		Scan(&children, &createOperations, &toolUses, &toolResults); err != nil {
		t.Fatalf("read recovered subagent census: %v", err)
	}
	if children != 1 || createOperations != 1 || toolUses != 1 || toolResults != 1 {
		t.Fatalf("recovered subagent census children/operations/tool uses/results = %d/%d/%d/%d", children, createOperations, toolUses, toolResults)
	}
}

func TestPostgreSQLPodLossAfterRetryStartSettlesConsumedReschedule(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID   = "sesn_consumed_reschedule_pod_loss"
		threadID    = "sthr_consumed_reschedule_pod_loss"
		bindingID   = "bind_consumed_reschedule_pod_loss"
		originalID  = "mreq_consumed_reschedule_original"
		retryID     = "mreq_consumed_reschedule_retry"
		durableTurn = "evt_consumed_reschedule_turn"
	)
	binding := runtimePodLostBinding(sessionID, bindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	acceptedAt := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, binding.PodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, durableTurn)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_consumed_reschedule_original_start", originalID, requestKindAgentProviderRequest, 0)
	if ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_consumed_reschedule_original_end", ModelRequestId: originalID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	}); err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit original reschedule: response=%#v err=%v", ended, err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_consumed_reschedule_retry_start", retryID, requestKindAgentProviderRequest, 0)
	if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtimeDB, sessionID, binding, acceptedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("repair pod loss after retry start: %v", err)
	}
	var sessionStatus, threadStatus, runtimeStatus string
	var providerAttempts int64
	var retryEnds int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1 AND binding_id=$3 AND binding_generation=1),
		(SELECT provider_attempts FROM session_turn_retries WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='span.model_request_end' AND model_request_id=$4
		  AND payload_json::jsonb->>'error_kind'='runtime_pod_lost')`,
		sessionID, threadID, bindingID, retryID).Scan(&sessionStatus, &threadStatus, &runtimeStatus, &providerAttempts, &retryEnds); err != nil {
		t.Fatalf("read consumed reschedule closeout: %v", err)
	}
	if sessionStatus != "idle" || threadStatus != "idle" || runtimeStatus != "idle" || providerAttempts != 0 || retryEnds != 1 {
		t.Fatalf("consumed reschedule closeout session/thread/runtime/attempts/retry ends = %s/%s/%s/%d/%d",
			sessionStatus, threadStatus, runtimeStatus, providerAttempts, retryEnds)
	}
}

func runProviderRescheduleRecoveryComposition(t *testing.T, input map[string]any) providerRescheduleRecoveryComposition {
	t.Helper()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode provider reschedule recovery input: %v", err)
	}
	inputPath := t.TempDir() + "/input.json"
	if err := os.WriteFile(inputPath, inputJSON, 0o600); err != nil {
		t.Fatalf("write provider reschedule recovery input: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/provider-reschedule-recovery-composition.ts", inputPath) //nolint:gosec // Fixed Runtime composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run provider reschedule recovery composition: %v: %s", err, output)
	}
	var result providerRescheduleRecoveryComposition
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode provider reschedule recovery composition: %v: %s", err, output)
	}
	return result
}
