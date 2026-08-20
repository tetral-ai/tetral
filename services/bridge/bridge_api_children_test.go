package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestSelectForkEntriesUsesUserLedTurnBoundaries(t *testing.T) {
	entries := []bridgeRuntimeContextEntry{
		{MessageSequence: 1, ContextKind: "user"},
		{MessageSequence: 2, ContextKind: "assistant"},
		{MessageSequence: 3, ContextKind: "compaction"},
		{MessageSequence: 4, ContextKind: "runtime_notification"},
		{MessageSequence: 5, ContextKind: "assistant"},
	}
	kinds := []string{"user", "assistant", "compaction", "runtime_notification", "assistant"}

	selected := selectForkEntries(entries, kinds, "1")
	if len(selected) != 2 || selected[0].MessageSequence != 4 || selected[1].MessageSequence != 5 {
		t.Fatalf("last user-led turn = %#v; want sequences 4,5", selected)
	}
	all := selectForkEntries(entries, kinds, "2")
	if len(all) != len(entries) {
		t.Fatalf("two user-led turns selected %d entries; want %d", len(all), len(entries))
	}
}

func TestDurablePrefixIncludesAcknowledgedFailedAndRescheduledAssistantParts(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_prefix_sealed_only"
		threadID  = "thr_prefix_sealed_only"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
		workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,model_request_id,created_at,updated_at
	) VALUES
		('default',$1,$2,'msg_prefix_success',1,'assistant','{"parts":[{"type":"text","text":"success"}]}','mreq_prefix_success',now(),now()),
		('default',$1,$2,'msg_prefix_failed',2,'assistant','{"parts":[{"type":"text","text":"failed partial"}]}','mreq_prefix_failed',now(),now()),
		('default',$1,$2,'msg_prefix_rescheduled',3,'assistant','{"parts":[{"type":"text","text":"rescheduled partial"}]}','mreq_prefix_rescheduled',now(),now()),
		('default',$1,$2,'msg_prefix_user',4,'user','{"parts":[{"type":"text","text":"next input"}]}',NULL,now(),now())`, sessionID, threadID); err != nil {
		t.Fatalf("seed prefix messages: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,model_request_id,created_at,updated_at
	) VALUES
		('default',$1,$2,'evt_prefix_success_end',1,'span.model_request_end','{"is_error":false}','mreq_prefix_success',now(),now()),
		('default',$1,$2,'evt_prefix_failed_end',2,'span.model_request_end','{"is_error":true}','mreq_prefix_failed',now(),now()),
		('default',$1,$2,'evt_prefix_rescheduled_end',3,'span.model_request_end','{"is_error":false}','mreq_prefix_rescheduled',now(),now()),
		('default',$1,$2,'evt_prefix_rescheduled',4,'session.status_rescheduled','{}','mreq_prefix_rescheduled',now(),now())`, sessionID, threadID); err != nil {
		t.Fatalf("seed prefix request dispositions: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	var entries []bridgeRuntimeContextEntry
	var kinds []string
	err := client.WithWorkspaceTx(context.Background(), "default", "test.prefix.sealed_only", func(tx *dbconnect.Tx) error {
		var err error
		entries, kinds, err = loadDurablePrefixEntriesThroughTx(
			context.Background(), tx,
			&bridgev1.RuntimeScope{WorkspaceId: "default", SessionId: sessionID, SessionThreadId: threadID},
			4,
		)
		return err
	})
	if err != nil {
		t.Fatalf("load durable prefix: %v", err)
	}
	if len(entries) != 4 || entries[0].MessageSequence != 1 || entries[1].MessageSequence != 2 ||
		entries[2].MessageSequence != 3 || entries[3].MessageSequence != 4 || len(kinds) != 4 ||
		kinds[0] != "assistant" || kinds[1] != "assistant" || kinds[2] != "assistant" || kinds[3] != "user" {
		t.Fatalf("acknowledged prefix entries/kinds = %#v/%v", entries, kinds)
	}
}

func TestActorCreationBoundsMatchTheRuntimeContract(t *testing.T) {
	if !validActorTaskName(strings.Repeat("a", actorTaskNameMaxBytes)) {
		t.Fatal("exact 128-byte task name was rejected")
	}
	if validActorTaskName(strings.Repeat("a", actorTaskNameMaxBytes+1)) {
		t.Fatal("129-byte task name was accepted")
	}
	if !validActorTaskName(strings.Repeat("界", 42)) || validActorTaskName(strings.Repeat("界", 43)) {
		t.Fatal("task name bound was not applied to UTF-8 bytes")
	}
	if validActorTaskName(" padded ") || validActorTaskName(string([]byte{0xff})) {
		t.Fatal("non-canonical task name was accepted")
	}
	for _, value := range []string{"none", "all", "1", "1000"} {
		if !validForkTurns(value) {
			t.Fatalf("valid fork_turns %q was rejected", value)
		}
	}
	for _, value := range []string{"", "0", "01", "1001", "-1", "1.0"} {
		if validForkTurns(value) {
			t.Fatalf("invalid fork_turns %q was accepted", value)
		}
	}
}

func TestCreateSubagentThreadPreservesLiveToolAdmissionFences(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_live_spawn_fences"
		parentID  = "thr_live_spawn_fences_parent"
		otherID   = "thr_live_spawn_fences_other"
		bindingID = "bind_live_spawn_fences"
		podUID    = "pod_live_spawn_fences"
		requestID = "mreq_live_spawn_fences"
		validID   = "evt_live_spawn_fences_valid"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, otherID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, validID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"worker","agent_type":"worker","fork_turns":"all"},"evaluated_permission":"ask"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, requestID, validID); err != nil {
		t.Fatalf("authorize valid live spawn source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, requestID, validID, "call_live_spawn_fences", "spawn_agent")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES ('default',$1,$2,$3,'call_live_spawn_fences','spawn_agent',
			'{"task_name":"worker","agent_type":"worker","fork_turns":"all"}','pending',now(),now())`,
		sessionID, parentID, validID,
	); err != nil {
		t.Fatalf("seed pending spawn authorization: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_live_spawn_fences_wrong_name", 2, "agent.tool_use",
		`{"type":"agent.tool_use","name":"Read","input":{"task_name":"worker","agent_type":"worker","fork_turns":"all"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id='evt_live_spawn_fences_wrong_name'`, sessionID, requestID); err != nil {
		t.Fatalf("authorize wrong-name source: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_live_spawn_fences_wrong_type", 3, "agent.mcp_tool_use",
		`{"type":"agent.mcp_tool_use","name":"spawn_agent","input":{"task_name":"worker","agent_type":"worker","fork_turns":"all"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id='evt_live_spawn_fences_wrong_type'`, sessionID, requestID); err != nil {
		t.Fatalf("authorize wrong-type source: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, otherID, "evt_live_spawn_fences_other_thread", 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"worker","agent_type":"worker","fork_turns":"all"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id='evt_live_spawn_fences_other_thread'`, sessionID, requestID); err != nil {
		t.Fatalf("authorize other-thread source: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	request := func(sourceID, taskName string, candidateScope *bridgev1.RuntimeScope) *bridgev1.CreateSubagentThreadRequest {
		return &bridgev1.CreateSubagentThreadRequest{
			Scope: candidateScope, SourceToolUseEventId: sourceID, TaskName: taskName, AgentType: "worker", ForkTurns: "all", InitialPrompt: "perform the delegated task",
		}
	}
	for name, candidate := range map[string]*bridgev1.CreateSubagentThreadRequest{
		"wrong Tool name":            request("evt_live_spawn_fences_wrong_name", "worker", scope),
		"wrong Tool type":            request("evt_live_spawn_fences_wrong_type", "worker", scope),
		"stale scope":                request(validID, "worker", bridgeAPIScope(sessionID, parentID, bindingID, 2, podUID)),
		"source from another Thread": request("evt_live_spawn_fences_other_thread", "worker", scope),
	} {
		if response, err := store.CreateSubagentThread(context.Background(), candidate); err == nil || response != nil {
			t.Fatalf("%s admission = %#v/%v; want rejection", name, response, err)
		}
	}
	if response, err := store.CreateSubagentThread(context.Background(), request(validID, "worker", scope)); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("pending spawn authorization = %#v/%v; want FailedPrecondition", response, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses
		SET status='resolving',decision='deny',updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		sessionID, parentID, validID); err != nil {
		t.Fatalf("deny spawn authorization: %v", err)
	}
	if response, err := store.CreateSubagentThread(context.Background(), request(validID, "worker", scope)); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("denied spawn authorization = %#v/%v; want FailedPrecondition", response, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses
		SET status='cancelled',decision=NULL,updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		sessionID, parentID, validID); err != nil {
		t.Fatalf("cancel spawn authorization: %v", err)
	}
	if response, err := store.CreateSubagentThread(context.Background(), request(validID, "worker", scope)); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("cancelled spawn authorization = %#v/%v; want FailedPrecondition", response, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses
		SET status='resolving',decision='allow',updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND tool_use_event_id=$3`,
		sessionID, parentID, validID); err != nil {
		t.Fatalf("allow spawn authorization: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET projection_json=jsonb_set(projection_json::jsonb,'{provider_input}',
			'{"task_name":"other","agent_type":"worker","fork_turns":"all"}'::jsonb)::text
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, validID); err != nil {
		t.Fatalf("mutate provider-visible spawn declaration: %v", err)
	}
	created, err := store.CreateSubagentThread(context.Background(), request(validID, "worker", scope))
	if err != nil || created.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("Runtime-owned spawn declaration = %#v/%v; want committed", created, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET projection_json=jsonb_set(projection_json::jsonb,'{provider_input}',
			(payload_json::jsonb -> 'input'))::text
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, validID); err != nil {
		t.Fatalf("restore provider-visible spawn declaration: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, parentID); err != nil {
		t.Fatalf("close parent before admission: %v", err)
	}
	if replayed, err := store.CreateSubagentThread(context.Background(), request(validID, "worker", scope)); err != nil || replayed.GetDuplicate().GetChildThreadId() != created.GetCommitted().GetChildThreadId() {
		t.Fatalf("closed-parent exact creation replay = %#v/%v; want duplicate", replayed, err)
	}
	var children, operations, initialInputs, queueJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='subagent' AND task_name='worker'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1)`,
		sessionID).Scan(&children, &operations, &initialInputs, &queueJobs); err != nil {
		t.Fatalf("read spawn mutation census: %v", err)
	}
	if children != 1 || operations != 1 || initialInputs != 1 || queueJobs != 1 {
		t.Fatalf("spawn mutation census children/operations/inputs/queue = %d/%d/%d/%d", children, operations, initialInputs, queueJobs)
	}
}

func TestPostgreSQLSubagentPrefixExcludesSourceAssistantBeforeAndAfterRequestEnd(t *testing.T) {
	for _, requestEnded := range []bool{false, true} {
		name := "open_request"
		if requestEnded {
			name = "ended_request"
		}
		t.Run(name, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_spawn_prefix_" + name
			threadID := "thr_spawn_prefix_" + name
			bindingID := "bind_spawn_prefix_" + name
			podUID := "pod_spawn_prefix_" + name
			modelRequestID := "mreq_spawn_prefix_" + name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_spawn_prefix_"+name, "evt_spawn_prefix_user_"+name, 1)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_spawn_prefix_start_"+name, modelRequestID, requestKindAgentProviderRequest, 1)
			inputJSON := `{"task_name":"worker","agent_type":"worker","fork_turns":"all"}`
			toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_spawn_prefix_tool_" + name, ModelRequestId: modelRequestID,
				EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"spawn_agent","input":` + inputJSON + `,"evaluated_permission":"allow"}`,
				AssistantContextDelta:       bridgeToolCallContextDeltaForTest("call_spawn_prefix_"+name, "spawn_agent", inputJSON),
				CanonicalExecutionInputJson: inputJSON,
			})
			if err != nil || toolUse.GetCommitted() == nil {
				t.Fatalf("write source spawn Tool Use = %#v/%v", toolUse, err)
			}
			if requestEnded {
				if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
					Scope: scope, RuntimeWriteId: "rwrite_spawn_prefix_end_" + name, ModelRequestId: modelRequestID,
					FinishReason: "tool_calls", UsageJson: `{}`,
				}); err != nil {
					t.Fatalf("end source spawn request: %v", err)
				}
			}
			created, err := store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
				Scope: scope, SourceToolUseEventId: toolUse.GetCommitted().GetEventId(), TaskName: "worker", AgentType: "worker", ForkTurns: "all", InitialPrompt: "perform the delegated task",
			})
			if err != nil || created.GetCommitted().GetChildThreadId() == "" {
				t.Fatalf("create child from %s = %#v/%v", name, created, err)
			}
			var entriesJSON string
			if err := admin.QueryRowContext(context.Background(), `SELECT entries_json
				FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2`,
				sessionID, created.GetCommitted().GetChildThreadId()).Scan(&entriesJSON); err != nil {
				t.Fatalf("read child prefix: %v", err)
			}
			var entries []bridgeRuntimeContextEntry
			if err := json.Unmarshal([]byte(entriesJSON), &entries); err != nil {
				t.Fatalf("decode child prefix: %v", err)
			}
			if len(entries) != 1 || entries[0].MessageSequence != 1 || strings.Contains(entriesJSON, "spawn_agent") || strings.Contains(entriesJSON, "call_spawn_prefix_") {
				t.Fatalf("%s child prefix = %s; want prior user context without source Assistant", name, entriesJSON)
			}
		})
	}
}

func TestActorBoundaryDiagnosticsAreBoundedAndFailOpen(t *testing.T) {
	scope := bridgeAPIScope("sesn_actor_diagnostic", "thr_actor_diagnostic", "bind_actor_diagnostic", 1, "pod_actor_diagnostic")
	logActorBoundaryRejected(
		slog.New(panicSlogHandler{}), scope, "create_subagent_thread", strings.Repeat("x", 129), "validate",
		status.Error(codes.InvalidArgument, "private rejection detail"),
	)

	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logActorBoundaryRejected(
		logger, scope, "create_subagent_thread", strings.Repeat("x", 129), "validate",
		status.Error(codes.InvalidArgument, "private rejection detail"),
	)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode actor diagnostic: %v", err)
	}
	if record["event.kind"] != "actor_boundary_rejected" || record["operation.id"] != "invalid" ||
		record["phase"] != "validate" || record["reason"] != codes.InvalidArgument.String() {
		t.Fatalf("actor diagnostic = %#v; want bounded operation identity and stable phase/reason", record)
	}
	if strings.Contains(output.String(), "private rejection detail") || strings.Contains(output.String(), strings.Repeat("x", 129)) {
		t.Fatalf("actor diagnostic leaked rejected input: %s", output.String())
	}
}

func TestAdmitChildInterruptAssignsDurableControlOperationIdentity(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_control_operation_identity"
		parentID  = "thr_control_operation_parent"
		childID   = "thr_control_operation_child"
		sourceID  = "evt_control_operation_source"
		bindingID = "bind_control_operation_identity"
		podUID    = "pod_control_operation_identity"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"close_agent","input":{"task_name":"provider-owned-different"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public' WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make control source public: %v", err)
	}
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	request := &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: sourceID, TargetChildThreadId: childID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	}
	first, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil {
		t.Fatalf("first AdmitChildInterrupt: %v", err)
	}
	operationID := first.GetCommitted().GetControlOperationId()
	if operationID == "" || operationID == sourceID {
		t.Fatalf("control operation id = %q; want a distinct Bridge-owned identity", operationID)
	}
	replay, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil {
		t.Fatalf("replayed AdmitChildInterrupt: %v", err)
	}
	if replay.GetDuplicate().GetControlOperationId() != operationID {
		t.Fatalf("replayed control operation id = %q; want %q", replay.GetDuplicate().GetControlOperationId(), operationID)
	}
	conflict := *request
	conflict.Action = bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT
	if response, err := store.AdmitChildInterrupt(context.Background(), &conflict); status.Code(err) != codes.AlreadyExists || response != nil {
		t.Fatalf("conflicting child-control declaration = %#v/%v; want AlreadyExists", response, err)
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: scope, ControlOperationId: sourceID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AwaitChildInterrupt accepted source Tool identity: %v", err)
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: scope, ControlOperationId: operationID}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("AwaitChildInterrupt durable control lookup = %v; want pending census", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET status='parked', binding_id=$2, binding_generation=1, target_pod_uid=$3
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'`, sessionID, bindingID, podUID); err != nil {
		t.Fatalf("park child interrupt custody: %v", err)
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: scope, ControlOperationId: operationID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AwaitChildInterrupt parked interrupt = %v; want invalid-custody FailedPrecondition", err)
	}
}

func TestPostgreSQLDeliverInterAgentMailIsAtomicAcrossGeneratedGRPCAndConcurrentReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_atomic_agent_mail"
		parentID  = "thr_atomic_agent_mail_parent"
		childID   = "thr_atomic_agent_mail_child"
		sourceID  = "evt_atomic_agent_mail_source"
		bindingID = "bind_atomic_agent_mail"
		podUID    = "pod_atomic_agent_mail"
		content   = "run the isolated verification"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+childID+`","message":"`+content+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make mail Tool source public: %v", err)
	}
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///atomic-agent-mail", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatalf("dial generated Bridge client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	request := &bridgev1.DeliverInterAgentMailRequest{
		Scope:      bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID),
		DeliveryId: agentMailDeliveryID(sourceID, childID), TargetThreadId: childID,
		SourceToolUseEventId: sourceID, Content: content,
	}

	type result struct {
		response *bridgev1.DeliverInterAgentMailResponse
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			response, err := client.DeliverInterAgentMail(context.Background(), request)
			results <- result{response: response, err: err}
		}()
	}
	ready.Wait()
	close(start)
	committed, duplicate := 0, 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent generated DeliverInterAgentMail: %v", result.err)
		}
		if result.response.GetCommitted() != nil {
			committed++
		}
		if result.response.GetDuplicate() != nil {
			duplicate++
		}
	}
	if committed != 1 || duplicate != 1 {
		t.Fatalf("concurrent outcomes committed/duplicate = %d/%d; want 1/1", committed, duplicate)
	}
	replay, err := client.DeliverInterAgentMail(context.Background(), request)
	if err != nil || replay.GetDuplicate() == nil {
		t.Fatalf("lost-ACK replay = %#v/%v; want duplicate", replay, err)
	}
	var sent, received, inbox, queued, operation int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'runtime_input_id'=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation=$3 AND idempotency_key=$4)`,
		sessionID, completionRuntimeInputID(request.GetDeliveryId()), bridgeOpDeliverInterAgentMail, request.GetDeliveryId(),
	).Scan(&sent, &received, &inbox, &queued, &operation); err != nil {
		t.Fatalf("read atomic mail state: %v", err)
	}
	if sent != 1 || received != 1 || inbox != 1 || queued != 1 || operation != 1 {
		t.Fatalf("atomic mail rows sent/received/inbox/queue/receipt = %d/%d/%d/%d/%d; want all one", sent, received, inbox, queued, operation)
	}
}

func TestPostgreSQLDeliverInterAgentMailQueueFailureRollsBackAllMailState(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_atomic_agent_mail_rollback"
		parentID  = "thr_atomic_agent_mail_rollback_parent"
		childID   = "thr_atomic_agent_mail_rollback_child"
		sourceID  = "evt_atomic_agent_mail_rollback_source"
		bindingID = "bind_atomic_agent_mail_rollback"
		podUID    = "pod_atomic_agent_mail_rollback"
		content   = "this delivery must roll back"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+childID+`","message":"`+content+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make mail Tool source public: %v", err)
	}
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_atomic_agent_mail_queue_birth() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected atomic agent mail Queue failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_atomic_agent_mail_queue_birth BEFORE INSERT ON queue_jobs
		FOR EACH ROW WHEN (NEW.kind = 'runtime_input') EXECUTE FUNCTION fail_atomic_agent_mail_queue_birth()`); err != nil {
		t.Fatalf("install Queue failure: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	deliveryID := agentMailDeliveryID(sourceID, childID)
	if _, err := store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), DeliveryId: deliveryID,
		TargetThreadId: childID, SourceToolUseEventId: sourceID, Content: content,
	}); err == nil {
		t.Fatal("DeliverInterAgentMail succeeded despite injected Queue failure")
	}
	var sent, received, inbox, queued, operation int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'runtime_input_id'=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation=$3 AND idempotency_key=$4)`,
		sessionID, completionRuntimeInputID(deliveryID), bridgeOpDeliverInterAgentMail, deliveryID,
	).Scan(&sent, &received, &inbox, &queued, &operation); err != nil {
		t.Fatalf("read rolled-back atomic mail state: %v", err)
	}
	if sent != 0 || received != 0 || inbox != 0 || queued != 0 || operation != 0 {
		t.Fatalf("rolled-back mail rows sent/received/inbox/queue/receipt = %d/%d/%d/%d/%d; want all zero", sent, received, inbox, queued, operation)
	}
}

func TestPostgreSQLInterruptBarrierDistinguishesSiblingMailFromInterruptedEffects(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID     = "sesn_interrupt_actor_effects"
		mainID        = "thr_interrupt_actor_main"
		siblingID     = "thr_interrupt_actor_sibling"
		grandchildID  = "thr_interrupt_actor_grandchild"
		bindingID     = "bind_interrupt_actor_effects"
		podUID        = "pod_interrupt_actor_effects"
		interruptID   = "rin_interrupt_actor_control"
		preSourceID   = "evt_interrupt_actor_pre_mail"
		siblingSource = "evt_interrupt_actor_sibling_mail"
		lateSourceID  = "evt_interrupt_actor_late_mail"
		childSourceID = "evt_interrupt_actor_late_child"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, siblingID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, siblingID, grandchildID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, preSourceID, nextBridgeAPIEventSequenceForTest(t, admin, sessionID, mainID), "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+siblingID+`","message":"committed before interrupt"},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, siblingSource, nextBridgeAPIEventSequenceForTest(t, admin, sessionID, mainID), "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+siblingID+`","message":"external sibling mail waits"},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, siblingID, lateSourceID, nextBridgeAPIEventSequenceForTest(t, admin, sessionID, siblingID), "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+grandchildID+`","message":"must be rejected"},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, siblingID, childSourceID, nextBridgeAPIEventSequenceForTest(t, admin, sessionID, siblingID), "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"late-child","prompt":"must be rejected"},"evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, mainID, preSourceID)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, mainID, siblingSource)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, siblingID, lateSourceID)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, siblingID, childSourceID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	mainScope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	siblingScope := bridgeAPIScope(sessionID, siblingID, bindingID, 1, podUID)
	preRequest := &bridgev1.DeliverInterAgentMailRequest{
		Scope: mainScope, DeliveryId: agentMailDeliveryID(preSourceID, siblingID), TargetThreadId: siblingID,
		SourceToolUseEventId: preSourceID, Content: "committed before interrupt",
	}
	if response, err := store.DeliverInterAgentMail(context.Background(), preRequest); err != nil || response.GetCommitted() == nil {
		t.Fatalf("pre-interrupt mail = %#v/%v; want committed", response, err)
	}
	interruptSequence := nextBridgeAPIEventSequenceForTest(t, admin, sessionID, siblingID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, siblingID, "evt_interrupt_actor_control", interruptSequence, "user.interrupt", `{}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,
		sequence_from,sequence_to,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'interrupt_control','["evt_interrupt_actor_control"]',$4,$4,'queued',clock_timestamp(),clock_timestamp())`,
		sessionID, siblingID, interruptID, interruptSequence); err != nil {
		t.Fatalf("seed actor interrupt barrier: %v", err)
	}
	if response, err := store.DeliverInterAgentMail(context.Background(), preRequest); err != nil || response.GetDuplicate() == nil {
		t.Fatalf("pre-interrupt mail replay = %#v/%v; want duplicate", response, err)
	}
	siblingRequest := &bridgev1.DeliverInterAgentMailRequest{
		Scope: mainScope, DeliveryId: agentMailDeliveryID(siblingSource, siblingID), TargetThreadId: siblingID,
		SourceToolUseEventId: siblingSource, Content: "external sibling mail waits",
	}
	if response, err := store.DeliverInterAgentMail(context.Background(), siblingRequest); err != nil || response.GetCommitted() == nil {
		t.Fatalf("sibling mail behind barrier = %#v/%v; want committed", response, err)
	}
	lateRequest := &bridgev1.DeliverInterAgentMailRequest{
		Scope: siblingScope, DeliveryId: agentMailDeliveryID(lateSourceID, grandchildID), TargetThreadId: grandchildID,
		SourceToolUseEventId: lateSourceID, Content: "must be rejected",
	}
	if _, err := store.DeliverInterAgentMail(context.Background(), lateRequest); !isSessionInterruptBarrierStaleError(err) {
		t.Fatalf("interrupted-source mail error = %v; want interrupt barrier stale", err)
	}
	if _, err := store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: siblingScope, SourceToolUseEventId: childSourceID, TaskName: "late-child", AgentType: "worker", ForkTurns: "all", InitialPrompt: "must be stale",
	}); !isSessionInterruptBarrierStaleError(err) {
		t.Fatalf("interrupted-source child error = %v; want interrupt barrier stale", err)
	}
	var sent, received, inbox, queued, lateOperations, lateChildren int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key=$2 AND payload_json::jsonb->>'input_kind'='agent_mail'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND idempotency_key IN ($3,$4)),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND task_name='late-child')`,
		sessionID, queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID), lateRequest.GetDeliveryId(), childSourceID,
	).Scan(&sent, &received, &inbox, &queued, &lateOperations, &lateChildren); err != nil {
		t.Fatalf("read interrupt actor effects: %v", err)
	}
	if sent != 2 || received != 2 || inbox != 2 || queued != 2 || lateOperations != 0 || lateChildren != 0 {
		t.Fatalf("interrupt actor effects sent/received/inbox/queue/late_ops/late_children = %d/%d/%d/%d/%d/%d; want 2/2/2/2/0/0",
			sent, received, inbox, queued, lateOperations, lateChildren)
	}
}

func TestPostgreSQLDeclaredChildControlOwnsExactTargetActionAndExpansion(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_declared_child_control"
		parentID        = "thr_declared_control_parent"
		interruptRootID = "thr_declared_interrupt_root"
		interruptLeafID = "thr_declared_interrupt_leaf"
		closeRootID     = "thr_declared_close_root"
		closeLeafID     = "thr_declared_close_leaf"
		resumeTargetID  = "thr_declared_resume_target"
		reviewerID      = "thr_declared_control_reviewer"
		bindingID       = "bind_declared_child_control"
		podUID          = "pod_declared_child_control"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, interruptRootID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, interruptRootID, interruptLeafID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, closeRootID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, closeRootID, closeLeafID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, resumeTargetID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)

	seedSource := func(sourceID, toolName string, sequence int64) string {
		t.Helper()
		seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, sequence, "agent.tool_use",
			`{"type":"agent.tool_use","name":"`+toolName+`","input":{"task_name":"provider-owned-different"}}`)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
			t.Fatalf("publish declared child-control source: %v", err)
		}
		seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
		return sourceID
	}
	interruptSource := seedSource("evt_declared_interrupt", "interrupt_agent", 1)
	closeSource := seedSource("evt_declared_close", "close_agent", 2)
	capabilitySource := seedSource("evt_declared_capability", "close_agent", 3)
	wrongParentSource := seedSource("evt_declared_wrong_parent", "interrupt_agent", 4)
	wrongRoleSource := seedSource("evt_declared_wrong_role", "interrupt_agent", 5)
	interrupt, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: interruptSource, TargetChildThreadId: interruptRootID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT,
	})
	if err != nil || interrupt.GetCommitted().GetControlOperationId() == "" {
		t.Fatalf("Runtime-declared exact interrupt = %#v/%v", interrupt, err)
	}

	closed, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: closeSource, TargetChildThreadId: closeRootID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	})
	if err != nil || closed.GetCommitted().GetControlOperationId() == "" {
		t.Fatalf("Runtime-declared close subtree = %#v/%v", closed, err)
	}

	if response, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: capabilitySource, TargetChildThreadId: interruptRootID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT,
	}); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("child-control capability substitution = %#v/%v; want FailedPrecondition", response, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime',closed_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, resumeTargetID); err != nil {
		t.Fatalf("close capability-substitution resume target: %v", err)
	}
	if response, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope: scope, SourceToolUseEventId: capabilitySource, TargetChildThreadId: resumeTargetID,
	}); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("child-resume capability substitution = %#v/%v; want FailedPrecondition", response, err)
	}

	if response, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: wrongParentSource, TargetChildThreadId: interruptLeafID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT,
	}); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("non-direct child-control target = %#v/%v; want FailedPrecondition", response, err)
	}

	if response, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: wrongRoleSource, TargetChildThreadId: reviewerID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT,
	}); status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("non-public-subagent control target = %#v/%v; want FailedPrecondition", response, err)
	}

	var interruptTargets, closeTargets, rejectedTargets int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_interrupt_requested' AND payload_json::jsonb->>'source_tool_use_event_id'=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_interrupt_requested' AND payload_json::jsonb->>'source_tool_use_event_id'=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_interrupt_requested' AND payload_json::jsonb->>'source_tool_use_event_id' IN ($4,$5,$6))`,
		sessionID, interruptSource, closeSource, capabilitySource, wrongParentSource, wrongRoleSource).Scan(&interruptTargets, &closeTargets, &rejectedTargets); err != nil {
		t.Fatalf("read declared child-control target census: %v", err)
	}
	if interruptTargets != 1 || closeTargets != 2 || rejectedTargets != 0 {
		t.Fatalf("declared interrupt/close/rejected target census = %d/%d/%d; want 1/2/0", interruptTargets, closeTargets, rejectedTargets)
	}
}

func TestPostgreSQLMarkChildThreadActiveUsesRuntimeDeclaredTarget(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_durable_resume_target"
		parentID  = "thr_durable_resume_parent"
		childID   = "thr_durable_resume_child"
		sourceID  = "evt_durable_resume_source"
		bindingID = "bind_durable_resume"
		podUID    = "pod_durable_resume"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"provider-owned-different"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make resume Tool source public: %v", err)
	}
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime',closed_at='2026-01-01T00:00:00Z'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); err != nil {
		t.Fatalf("close resume target: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.MarkChildThreadActiveRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), SourceToolUseEventId: sourceID, TargetChildThreadId: childID,
	}
	response, err := store.MarkChildThreadActive(context.Background(), request)
	if err != nil || response.GetCommitted().GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED {
		t.Fatalf("MarkChildThreadActive durable target = %#v/%v; want committed resumed", response, err)
	}
	replay, err := store.MarkChildThreadActive(context.Background(), request)
	if err != nil || replay.GetDuplicate().GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED {
		t.Fatalf("MarkChildThreadActive lost-ACK replay = %#v/%v; want duplicate resumed", replay, err)
	}
	var statusValue string
	var operationCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1
		 AND session_thread_id=$2 AND operation='mark_child_thread_active')`, sessionID, childID).Scan(&statusValue, &operationCount); err != nil {
		t.Fatalf("read resumed child: %v", err)
	}
	if statusValue != "idle" || operationCount != 1 {
		t.Fatalf("resumed child status/receipt = %s/%d; want idle/1", statusValue, operationCount)
	}

	for index, test := range []struct {
		status      string
		disposition bridgev1.ChildLifecycleDisposition
	}{
		{status: "failed", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED},
		{status: "terminated", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED},
	} {
		sourceID := fmt.Sprintf("evt_durable_resume_%s_source", test.status)
		seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, int64(index+2), "agent.tool_use",
			`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"task_`+childID+`"}}`)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
			t.Fatalf("make %s resume Tool source public: %v", test.status, err)
		}
		seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status=$3,closed_at='2026-01-01T00:00:00Z'
			WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID, test.status); err != nil {
			t.Fatalf("set resume target %s: %v", test.status, err)
		}
		terminalRequest := &bridgev1.MarkChildThreadActiveRequest{Scope: request.GetScope(), SourceToolUseEventId: sourceID, TargetChildThreadId: childID}
		terminalResponse, err := store.MarkChildThreadActive(context.Background(), terminalRequest)
		if err != nil || terminalResponse.GetCommitted().GetDisposition() != test.disposition {
			t.Fatalf("MarkChildThreadActive %s target = %#v/%v; want committed %s", test.status, terminalResponse, err, test.disposition)
		}
		terminalReplay, err := store.MarkChildThreadActive(context.Background(), terminalRequest)
		if err != nil || terminalReplay.GetDuplicate().GetDisposition() != test.disposition {
			t.Fatalf("MarkChildThreadActive %s replay = %#v/%v; want duplicate %s", test.status, terminalReplay, err, test.disposition)
		}
		if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads
			WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID).Scan(&statusValue); err != nil {
			t.Fatalf("read preserved %s target: %v", test.status, err)
		}
		if statusValue != test.status {
			t.Fatalf("resume changed terminal child status = %q; want %q", statusValue, test.status)
		}
	}

	terminalSourceID := "evt_durable_resume_terminal_source"
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, terminalSourceID, 4, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"task_`+childID+`"}}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_durable_resume_terminal_result", 5, "agent.tool_result",
		`{"type":"agent.tool_result","tool_use_event_id":"`+terminalSourceID+`","result":{"status":"completed"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id IN ($2,$3)`, sessionID, terminalSourceID, "evt_durable_resume_terminal_result"); err != nil {
		t.Fatalf("make terminal resume source public: %v", err)
	}
	terminalRequest := &bridgev1.MarkChildThreadActiveRequest{
		Scope: request.GetScope(), SourceToolUseEventId: terminalSourceID, TargetChildThreadId: childID,
	}
	if _, err := store.MarkChildThreadActive(context.Background(), terminalRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("terminal resume Tool source err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLAdmitApprovalReviewInputSerializesConcurrentReplayWithoutQueue(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_reviewer_admission_replay"
		parentID   = "thr_reviewer_admission_parent"
		reviewerID = "thr_reviewer_admission_target"
		reviewID   = "arvw_reviewer_admission"
		bindingID  = "bind_reviewer_admission"
		podUID     = "pod_reviewer_admission"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), ReviewerThreadId: reviewerID, ReviewId: reviewID,
	}

	responses := make([]*bridgev1.AdmitApprovalReviewInputResponse, 2)
	errorsSeen := make([]error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range responses {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			responses[index], errorsSeen[index] = store.AdmitApprovalReviewInput(context.Background(), request)
		}()
	}
	close(start)
	wait.Wait()

	committed, duplicate := 0, 0
	var runtimeInputID string
	for index, response := range responses {
		if errorsSeen[index] != nil {
			t.Fatalf("concurrent reviewer admission %d: %v", index, errorsSeen[index])
		}
		switch {
		case response.GetCommitted() != nil:
			committed++
			runtimeInputID = response.GetCommitted().GetRuntimeInputId()
		case response.GetDuplicate() != nil:
			duplicate++
			if runtimeInputID == "" {
				runtimeInputID = response.GetDuplicate().GetRuntimeInputId()
			}
		default:
			t.Fatalf("concurrent reviewer admission %d outcome = %#v", index, response)
		}
	}
	if committed != 1 || duplicate != 1 || runtimeInputID == "" {
		t.Fatalf("concurrent reviewer admissions committed/duplicate/id = %d/%d/%q; want 1/1/non-empty", committed, duplicate, runtimeInputID)
	}
	for index, response := range responses {
		gotID := response.GetCommitted().GetRuntimeInputId()
		if response.GetDuplicate() != nil {
			gotID = response.GetDuplicate().GetRuntimeInputId()
		}
		if gotID != runtimeInputID {
			t.Fatalf("concurrent reviewer admission %d runtime input id = %q; want %q", index, gotID, runtimeInputID)
		}
	}

	var inboxCount, queueCount int
	var statusValue, targetThreadID string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT session_thread_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'runtime_input_id'=$2)`,
		sessionID, runtimeInputID,
	).Scan(&inboxCount, &statusValue, &targetThreadID, &queueCount); err != nil {
		t.Fatalf("read concurrent reviewer admission authority: %v", err)
	}
	if inboxCount != 1 || statusValue != "accepted" || targetThreadID != reviewerID || queueCount != 0 {
		t.Fatalf("reviewer admission authority count/status/target/queue = %d/%q/%q/%d; want 1/accepted/%q/0",
			inboxCount, statusValue, targetThreadID, queueCount, reviewerID)
	}
}

func TestPostgreSQLAdmitApprovalReviewInputResolvesCommittedReplayBeforeInterruptBarrier(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_reviewer_admission_barrier_replay"
		parentID   = "thr_reviewer_admission_barrier_parent"
		siblingID  = "thr_reviewer_admission_barrier_sibling"
		reviewerID = "thr_reviewer_admission_barrier_target"
		conflictID = "thr_reviewer_admission_barrier_conflict"
		reviewID   = "arvw_reviewer_admission_barrier_replay"
		bindingID  = "bind_reviewer_admission_barrier_replay"
		podUID     = "pod_reviewer_admission_barrier_replay"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, siblingID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), ReviewerThreadId: reviewerID, ReviewId: reviewID,
	}

	committed, err := store.AdmitApprovalReviewInput(context.Background(), request)
	if err != nil || committed.GetCommitted().GetRuntimeInputId() == "" {
		t.Fatalf("initial Reviewer admission = %#v/%v; want committed", committed, err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, created_at, updated_at
	) VALUES ('default', $1, $2, 'rin_reviewer_admission_barrier_interrupt', 'interrupt_control',
		'[]', 1, 1, 'queued', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, sessionID, parentID); err != nil {
		t.Fatalf("seed interrupt barrier after Reviewer admission: %v", err)
	}

	replay, err := store.AdmitApprovalReviewInput(context.Background(), request)
	if err != nil || replay.GetDuplicate().GetRuntimeInputId() != committed.GetCommitted().GetRuntimeInputId() {
		t.Fatalf("committed Reviewer admission replay behind barrier = %#v/%v; want duplicate %q",
			replay, err, committed.GetCommitted().GetRuntimeInputId())
	}
	wrongSourceRequest := &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: bridgeAPIScope(sessionID, siblingID, bindingID, 1, podUID), ReviewerThreadId: reviewerID, ReviewId: reviewID,
	}
	if wrongSource, err := store.AdmitApprovalReviewInput(context.Background(), wrongSourceRequest); status.Code(err) != codes.FailedPrecondition || wrongSource != nil {
		t.Fatalf("Reviewer admission replay from sibling source = %#v/%v; want FailedPrecondition with no receipt", wrongSource, err)
	}
	conflictRequest := &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: request.GetScope(), ReviewerThreadId: conflictID, ReviewId: reviewID,
	}
	if conflict, err := store.AdmitApprovalReviewInput(context.Background(), conflictRequest); status.Code(err) != codes.AlreadyExists || conflict != nil {
		t.Fatalf("conflicting Reviewer admission replay behind barrier = %#v/%v; want AlreadyExists with no receipt", conflict, err)
	}
}

func TestPostgreSQLAdmitApprovalReviewInputRejectsInterruptFirstWithoutCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_reviewer_admission_interrupt_first"
		parentID   = "thr_reviewer_admission_interrupt_parent"
		reviewerID = "thr_reviewer_admission_interrupt_target"
		bindingID  = "bind_reviewer_admission_interrupt"
		podUID     = "pod_reviewer_admission_interrupt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, created_at, updated_at
	) VALUES ('default', $1, $2, 'rin_reviewer_admission_interrupt', 'interrupt_control',
		'["evt_reviewer_admission_interrupt"]', 1, 1, 'queued',
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, sessionID, parentID); err != nil {
		t.Fatalf("seed interrupt-first barrier: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope:            bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID),
		ReviewerThreadId: reviewerID, ReviewId: "arvw_reviewer_admission_interrupt_first",
	})
	if err != nil || response.GetStale() == nil {
		t.Fatalf("interrupt-first Reviewer admission = %#v/%v; want typed stale", response, err)
	}
	var reviewerInboxRows int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='approval_review'`, sessionID).Scan(&reviewerInboxRows); err != nil {
		t.Fatalf("count interrupt-first Reviewer custody: %v", err)
	}
	if reviewerInboxRows != 0 {
		t.Fatalf("interrupt-first Reviewer custody rows = %d; want zero", reviewerInboxRows)
	}
}

func TestPostgreSQLReviewerEnsureRejectsInterruptFirstWithTypedStale(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID   = "sesn_reviewer_ensure_interrupt_first"
		parentID    = "thr_reviewer_ensure_interrupt_parent"
		bindingID   = "bind_reviewer_ensure_interrupt"
		podUID      = "pod_reviewer_ensure_interrupt"
		interruptID = "rin_reviewer_ensure_interrupt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, created_at, updated_at
	) VALUES ('default', $1, $2, $3, 'interrupt_control', '[]', 1, 1, 'queued',
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, sessionID, parentID, interruptID); err != nil {
		t.Fatalf("seed interrupt-first reviewer ensure barrier: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	server := BridgeAPIServer{store: store}
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	trunk, err := server.EnsureApprovalReviewerTrunk(context.Background(), &bridgev1.EnsureApprovalReviewerTrunkRequest{
		Scope: scope, EnsureOperationId: "aprv_ensure_interrupt_first",
	})
	if err != nil || trunk.GetStale() == nil {
		t.Fatalf("interrupt-first Reviewer trunk ensure = %#v/%v; want typed stale", trunk, err)
	}
	sidecar, err := server.EnsureApprovalReviewerSidecar(context.Background(), &bridgev1.EnsureApprovalReviewerSidecarRequest{
		Scope: scope, ReviewId: "arvw_ensure_interrupt_first",
	})
	if err != nil || sidecar.GetStale() == nil {
		t.Fatalf("interrupt-first Reviewer sidecar ensure = %#v/%v; want typed stale", sidecar, err)
	}

	var reviewerThreads, operations, reviewerInputs int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='approval_review')`,
		sessionID,
	).Scan(&reviewerThreads, &operations, &reviewerInputs); err != nil {
		t.Fatalf("read interrupt-first Reviewer ensure residue: %v", err)
	}
	if reviewerThreads != 0 || operations != 0 || reviewerInputs != 0 {
		t.Fatalf("interrupt-first Reviewer ensure residue threads/operations/inputs = %d/%d/%d; want zero", reviewerThreads, operations, reviewerInputs)
	}
}

func TestPostgreSQLInterruptCommitCancelsAdmissionFirstReviewerCustodyAtomically(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_reviewer_admission_first_interrupt"
		parentID         = "thr_reviewer_admission_first_parent"
		reviewerID       = "thr_reviewer_admission_first_target"
		bindingID        = "bind_reviewer_admission_first"
		podUID           = "pod_reviewer_admission_first"
		interruptID      = "rin_reviewer_admission_first_interrupt"
		interruptEventID = "evt_reviewer_admission_first_interrupt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	admissionRequest := &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: scope, ReviewerThreadId: reviewerID, ReviewId: "arvw_reviewer_admission_first_interrupt",
	}
	admitted, err := store.AdmitApprovalReviewInput(context.Background(), admissionRequest)
	if err != nil || admitted.GetCommitted() == nil {
		t.Fatalf("admission-first Reviewer custody = %#v/%v; want committed", admitted, err)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, interruptEventID, 1, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, parentID, interruptID, "interrupt_control",
		`["`+interruptEventID+`"]`, "accepted", bindingID, podUID, 1, 1)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, parentID, interruptID, "interrupt_control", interruptEventID, 1, queue.DefaultMaxAttempts, time.Now().UTC())
	interruptLease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "reviewer-admission-first-interrupt",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	committed, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: interruptID, InterruptLeaseRef: bridgeInterruptLeaseRef(interruptLease),
	})
	if err != nil || committed.GetCommitted().GetInterrupt() == nil {
		t.Fatalf("interrupt closeout after Reviewer admission = %#v/%v; want committed", committed, err)
	}

	var reviewerStatus, interruptStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$4)`,
		sessionID, admitted.GetCommitted().GetRuntimeInputId(), interruptID, interruptLease.ID,
	).Scan(&reviewerStatus, &interruptStatus, &queueStatus); err != nil {
		t.Fatalf("read admission-first interrupt atomic authority: %v", err)
	}
	if reviewerStatus != "cancelled" || interruptStatus != "committed" || queueStatus != "leased" {
		t.Fatalf("admission-first interrupt statuses = Reviewer %q interrupt %q queue %q; want cancelled/committed/exact delivery lease retained",
			reviewerStatus, interruptStatus, queueStatus)
	}
	replayed, err := store.AdmitApprovalReviewInput(context.Background(), admissionRequest)
	if err != nil || replayed.GetDuplicate().GetRuntimeInputId() != admitted.GetCommitted().GetRuntimeInputId() {
		t.Fatalf("cancelled Reviewer admission replay = %#v/%v; want duplicate %q", replayed, err, admitted.GetCommitted().GetRuntimeInputId())
	}
}

func TestPostgreSQLTargetedInterruptCancelsOnlyItsReviewerCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_reviewer_targeted_interrupt"
		mainID          = "thr_reviewer_targeted_main"
		childID         = "thr_reviewer_targeted_child"
		mainReviewerID  = "thr_reviewer_targeted_main_reviewer"
		childReviewerID = "thr_reviewer_targeted_child_reviewer"
		bindingID       = "bind_reviewer_targeted_interrupt"
		podUID          = "pod_reviewer_targeted_interrupt"
		interruptID     = "rin_reviewer_targeted_interrupt"
		interruptEvent  = "evt_reviewer_targeted_interrupt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, mainReviewerID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, childID, childReviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	mainAdmission, err := store.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID), ReviewerThreadId: mainReviewerID, ReviewId: "arvw_reviewer_targeted_main",
	})
	if err != nil || mainAdmission.GetCommitted() == nil {
		t.Fatalf("admit main Reviewer custody: %#v/%v", mainAdmission, err)
	}
	childAdmission, err := store.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: bridgeAPIScope(sessionID, childID, bindingID, 1, podUID), ReviewerThreadId: childReviewerID, ReviewId: "arvw_reviewer_targeted_child",
	})
	if err != nil || childAdmission.GetCommitted() == nil {
		t.Fatalf("admit child Reviewer custody: %#v/%v", childAdmission, err)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, interruptEvent, 1, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, childID, interruptID, "interrupt_control",
		`["`+interruptEvent+`"]`, "accepted", bindingID, podUID, 1, 1)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, childID, interruptID, "interrupt_control", interruptEvent, 1, queue.DefaultMaxAttempts, time.Now().UTC())
	interruptLease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "reviewer-targeted-interrupt",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	committed, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: bridgeAPIScope(sessionID, childID, bindingID, 1, podUID), RuntimeInputId: interruptID,
		InterruptLeaseRef: bridgeInterruptLeaseRef(interruptLease),
	})
	if err != nil || committed.GetCommitted().GetInterrupt() == nil {
		t.Fatalf("targeted child interrupt closeout = %#v/%v", committed, err)
	}

	var mainStatus, childStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		mainAdmission.GetCommitted().GetRuntimeInputId(), childAdmission.GetCommitted().GetRuntimeInputId(),
	).Scan(&mainStatus, &childStatus); err != nil {
		t.Fatalf("read targeted Reviewer custody: %v", err)
	}
	if mainStatus != "accepted" || childStatus != "cancelled" {
		t.Fatalf("targeted Reviewer custody = main %q child %q; want accepted/cancelled", mainStatus, childStatus)
	}
}

func TestActorResponsesExposeOnlyOperationSpecificResults(t *testing.T) {
	created := &bridgev1.CreateSubagentThreadResponse{Outcome: &bridgev1.CreateSubagentThreadResponse_Committed{
		Committed: &bridgev1.CreateSubagentThreadCommitted{ChildThreadId: "thr_bridge_owned"},
	}}
	if created.GetCommitted().GetChildThreadId() != "thr_bridge_owned" {
		t.Fatal("create response lost Bridge-owned child identity")
	}
	delivered := &bridgev1.DeliverInterAgentMailResponse{Outcome: &bridgev1.DeliverInterAgentMailResponse_Committed{
		Committed: &bridgev1.DeliverInterAgentMailCommitted{},
	}}
	if delivered.GetCommitted() == nil || delivered.GetDuplicate() != nil {
		t.Fatal("delivery response is not a closed committed result")
	}
	resumed := &bridgev1.MarkChildThreadActiveResponse{Outcome: &bridgev1.MarkChildThreadActiveResponse_Committed{
		Committed: &bridgev1.MarkChildThreadActiveCommitted{Disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED},
	}}
	if resumed.GetCommitted() == nil || resumed.GetDuplicate() != nil || resumed.GetStale() != nil {
		t.Fatal("resume response is not a closed echo-free result")
	}
	resolved := &bridgev1.ResolveChildThreadResponse{Outcome: &bridgev1.ResolveChildThreadResponse_Resolved{
		Resolved: &bridgev1.ResolveChildThreadResolved{Child: &bridgev1.ChildThreadFact{ChildThreadId: "thr_child"}},
	}}
	if resolved.GetResolved().GetChild().GetChildThreadId() != "thr_child" {
		t.Fatal("child read response lost its typed child fact")
	}
	listed := &bridgev1.ListChildThreadsResponse{Outcome: &bridgev1.ListChildThreadsResponse_Completed{
		Completed: &bridgev1.ListChildThreadsCompleted{Children: []*bridgev1.ChildThreadFact{{ChildThreadId: "thr_child"}}},
	}}
	if len(listed.GetCompleted().GetChildren()) != 1 {
		t.Fatal("child list response lost its typed child facts")
	}
	reviewerClosed := &bridgev1.CloseApprovalReviewerResponse{Outcome: &bridgev1.CloseApprovalReviewerResponse_Committed{
		Committed: &bridgev1.CloseApprovalReviewerCommitted{},
	}}
	if reviewerClosed.GetCommitted() == nil || reviewerClosed.GetDuplicate() != nil || reviewerClosed.GetStale() != nil {
		t.Fatal("reviewer close response is not a closed operation-specific result")
	}
}
