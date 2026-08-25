package agentruntimebridge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLMemoryEffectRequiresExactExecutableRoute(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_memory_route_gate"
		threadID  = "thr_memory_route_gate"
		bindingID = "bind_memory_route_gate"
		podUID    = "pod_memory_route_gate"
		storeID   = "memstore_memory_route_gate"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_memory_route_gate", "call_memory_route_gate", "memory",
		`{"action":"create","path":"route.md","content":"owned"}`)
	request := &bridgev1.RunMemoryRequest{Scope: scope, ToolUseEventId: toolUseID}

	assertRejectedWithoutEffect := func(label string, candidateScope *bridgev1.RuntimeScope) {
		t.Helper()
		response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{Scope: candidateScope, ToolUseEventId: toolUseID})
		if label == "stale binding" {
			if err != nil || response.GetStale() == nil {
				t.Fatalf("%s memory route = %#v/%v; want typed stale", label, response, err)
			}
		} else if status.Code(err) != codes.FailedPrecondition || response != nil {
			t.Fatalf("%s memory route = %#v/%v; want FailedPrecondition", label, response, err)
		}
		var memories, receipts int
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM memories WHERE workspace_id='default' AND memory_store_id=$1),
			(SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$2 AND tool_use_event_id=$3)`,
			storeID, sessionID, toolUseID).Scan(&memories, &receipts); err != nil {
			t.Fatalf("%s memory effect census: %v", label, err)
		}
		if memories != 0 || receipts != 0 {
			t.Fatalf("%s memory effects = memories:%d receipts:%d; want zero", label, memories, receipts)
		}
	}

	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='pending',decision=NULL WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
		t.Fatalf("make pending route: %v", err)
	}
	assertRejectedWithoutEffect("pending", scope)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='resolving',decision='deny' WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
		t.Fatalf("make denied route: %v", err)
	}
	assertRejectedWithoutEffect("denied", scope)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='cancelled',decision='allow' WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
		t.Fatalf("make cancelled route: %v", err)
	}
	assertRejectedWithoutEffect("cancelled", scope)
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
		t.Fatalf("delete route: %v", err)
	}
	assertRejectedWithoutEffect("missing", scope)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, threadID, toolUseID)
	assertRejectedWithoutEffect("stale binding", bridgeAPIScope(sessionID, threadID, bindingID, 2, podUID))
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='resolved',result_event_id='evt_conflicting_route_result' WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
		t.Fatalf("make conflicting resolved route: %v", err)
	}
	assertRejectedWithoutEffect("conflicting", scope)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='resolving',decision='allow',result_event_id=NULL,resolved_at=NULL WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
		t.Fatalf("restore executable route: %v", err)
	}
	committed, err := store.RunMemory(context.Background(), request)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("executable memory route = %#v/%v; want committed", committed, err)
	}
	replayed, err := store.RunMemory(context.Background(), request)
	if err != nil || replayed.GetDuplicate() == nil {
		t.Fatalf("resolved-route memory replay = %#v/%v; want exact duplicate", replayed, err)
	}
}

func TestPostgreSQLActorEffectsUseExecutableRouteGate(t *testing.T) {
	for _, toolName := range []string{"send_message", "close_agent", "resume_agent"} {
		t.Run(toolName, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(toolName, "_", "")
			sessionID := "sesn_actor_route_" + suffix
			parentID := "thr_actor_route_parent_" + suffix
			childID := "thr_actor_route_child_" + suffix
			bindingID := "bind_actor_route_" + suffix
			podUID := "pod_actor_route_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, parentID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
			input := fmt.Sprintf(`{"task_name":"task_%s"}`, childID)
			if toolName == "send_message" {
				input = fmt.Sprintf(`{"task_name":"task_%s","message":"blocked"}`, childID)
			}
			toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_actor_route_"+suffix, "call_actor_route_"+suffix, toolName, input)
			if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET decision='deny' WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
				t.Fatalf("deny actor route: %v", err)
			}
			switch toolName {
			case "send_message":
				response, err := store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
					Scope: scope, DeliveryId: agentMailDeliveryID(toolUseID, childID), TargetThreadId: childID,
					SourceToolUseEventId: toolUseID, Content: "blocked",
				})
				if status.Code(err) != codes.FailedPrecondition || response != nil {
					t.Fatalf("denied mail = %#v/%v", response, err)
				}
			case "close_agent":
				response, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
					Scope: scope, SourceToolUseEventId: toolUseID, TargetChildThreadId: childID,
					Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
				})
				if status.Code(err) != codes.FailedPrecondition || response != nil {
					t.Fatalf("denied child interrupt = %#v/%v", response, err)
				}
			case "resume_agent":
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime',closed_at=clock_timestamp() WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); err != nil {
					t.Fatalf("close resume target: %v", err)
				}
				response, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{Scope: scope, SourceToolUseEventId: toolUseID, TargetChildThreadId: childID})
				if status.Code(err) != codes.FailedPrecondition || response != nil {
					t.Fatalf("denied child resume = %#v/%v", response, err)
				}
			}
			var actorEvents, operations int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('agent.thread_message_sent','agent.thread_interrupt_requested')),
				(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation IN ('deliver_inter_agent_mail','mark_child_thread_active'))`,
				sessionID).Scan(&actorEvents, &operations); err != nil {
				t.Fatalf("actor effect census: %v", err)
			}
			if actorEvents != 0 || operations != 0 {
				t.Fatalf("denied %s effects = events:%d operations:%d; want zero", toolName, actorEvents, operations)
			}
		})
	}
}

func TestPostgreSQLActorEffectsRejectExecutableCapabilitySubstitution(t *testing.T) {
	for _, endpoint := range []string{"spawn_agent", "send_message", "close_agent", "resume_agent"} {
		t.Run(endpoint, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(endpoint, "_", "")
			sessionID := "sesn_actor_capability_" + suffix
			parentID := "thr_actor_capability_parent_" + suffix
			childID := "thr_actor_capability_child_" + suffix
			bindingID := "bind_actor_capability_" + suffix
			podUID := "pod_actor_capability_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, parentID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
			toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_actor_capability_"+suffix, "call_actor_capability_"+suffix, "list_agents", `{}`)
			var responseNonNil bool
			var err error
			switch endpoint {
			case "spawn_agent":
				var createResponse *bridgev1.CreateSubagentThreadResponse
				createResponse, err = store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
					Scope: scope, SourceToolUseEventId: toolUseID, TaskName: "capability-worker", AgentType: "worker", InitialPrompt: "do not create",
				})
				responseNonNil = createResponse != nil
			case "send_message":
				var mailResponse *bridgev1.DeliverInterAgentMailResponse
				mailResponse, err = store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
					Scope: scope, DeliveryId: agentMailDeliveryID(toolUseID, childID), TargetThreadId: childID,
					SourceToolUseEventId: toolUseID, Content: "do not deliver",
				})
				responseNonNil = mailResponse != nil
			case "close_agent":
				var interruptResponse *bridgev1.AdmitChildInterruptResponse
				interruptResponse, err = store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
					Scope: scope, SourceToolUseEventId: toolUseID, TargetChildThreadId: childID,
					Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
				})
				responseNonNil = interruptResponse != nil
			case "resume_agent":
				if _, updateErr := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime',closed_at=clock_timestamp()
					WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); updateErr != nil {
					t.Fatalf("close capability target: %v", updateErr)
				}
				var resumeResponse *bridgev1.MarkChildThreadActiveResponse
				resumeResponse, err = store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
					Scope: scope, SourceToolUseEventId: toolUseID, TargetChildThreadId: childID,
				})
				responseNonNil = resumeResponse != nil
			}
			if status.Code(err) != codes.FailedPrecondition || responseNonNil {
				t.Fatalf("%s capability substitution response=%t err=%v; want FailedPrecondition", endpoint, responseNonNil, err)
			}
			var children, actorEvents, operations int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='subagent'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('agent.thread_message_sent','agent.thread_interrupt_requested')),
				(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation IN ('create_child_thread','deliver_inter_agent_mail','mark_child_thread_active'))`,
				sessionID).Scan(&children, &actorEvents, &operations); err != nil {
				t.Fatalf("read capability substitution census: %v", err)
			}
			if children != 1 || actorEvents != 0 || operations != 0 {
				t.Fatalf("%s capability substitution effects children/events/operations = %d/%d/%d", endpoint, children, actorEvents, operations)
			}
		})
	}
}

func TestPostgreSQLSandboxEffectRejectsExecutableCapabilitySubstitution(t *testing.T) {
	for _, toolName := range []string{"web", "memory", "spawn_agent", "write_stdin"} {
		t.Run(toolName, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(toolName, "_", "")
			sessionID := "sesn_sandbox_capability_" + suffix
			threadID := "thr_sandbox_capability_" + suffix
			bindingID := "bind_sandbox_capability_" + suffix
			podUID := "pod_sandbox_capability_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_sandbox_capability_"+suffix, "call_sandbox_capability_"+suffix, toolName, `{}`)

			response, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
				Scope: scope, ToolUseEventId: toolUseID,
			})
			if status.Code(err) != codes.FailedPrecondition || response != nil {
				t.Fatalf("Sandbox capability substitution response=%#v err=%v; want FailedPrecondition", response, err)
			}
			var executions, jobs int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key LIKE '%' || $1 || '%')`, sessionID,
			).Scan(&executions, &jobs); err != nil {
				t.Fatalf("read rejected Sandbox effect census: %v", err)
			}
			if executions != 0 || jobs != 0 {
				t.Fatalf("rejected Sandbox capability effects executions/jobs = %d/%d; want zero", executions, jobs)
			}
		})
	}
}

func TestPostgreSQLToolSettlementClosesExactRouteAtomically(t *testing.T) {
	for _, decision := range []string{"allow", "deny"} {
		t.Run(decision, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_settlement_route_" + decision
			threadID := "thr_settlement_route_" + decision
			bindingID := "bind_settlement_route_" + decision
			podUID := "pod_settlement_route_" + decision
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_settlement_route_"+decision, "call_settlement_route_"+decision, "Read", `{"path":"owned.txt"}`)
			if decision == "deny" {
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET decision='deny' WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
					t.Fatalf("deny settlement route: %v", err)
				}
			}
			settlement := bridgeCompletedToolSettlementForTest(toolUseID, "done")
			if decision == "deny" {
				settlement = bridgeErrorToolSettlementForTest(toolUseID, "policy denied")
			}
			request := bridgeToolSettlementRequestForTest(scope, settlement)
			if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_exact_route_settlement() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
				CREATE TRIGGER fail_exact_route_settlement BEFORE UPDATE ON session_pending_tool_uses
				FOR EACH ROW EXECUTE FUNCTION fail_exact_route_settlement()`); err != nil {
				t.Fatalf("install settlement rollback trigger: %v", err)
			}
			if response, err := store.SettleToolResult(context.Background(), request); err == nil || response != nil {
				t.Fatalf("settlement route-update failure = %#v/%v; want rollback", response, err)
			}
			var resultCount int
			var routeStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$2),
				(SELECT status FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2)`,
				sessionID, toolUseID).Scan(&resultCount, &routeStatus); err != nil {
				t.Fatalf("read rolled-back settlement: %v", err)
			}
			if resultCount != 0 || routeStatus != "resolving" {
				t.Fatalf("rolled-back settlement = results:%d route:%s", resultCount, routeStatus)
			}
			if _, err := admin.ExecContext(context.Background(), `DROP TRIGGER fail_exact_route_settlement ON session_pending_tool_uses; DROP FUNCTION fail_exact_route_settlement()`); err != nil {
				t.Fatalf("remove settlement rollback trigger: %v", err)
			}

			type result struct {
				response *bridgev1.SettleToolResultResponse
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
					response, err := store.SettleToolResult(context.Background(), request)
					results <- result{response: response, err: err}
				}()
			}
			ready.Wait()
			close(start)
			committed, duplicate := 0, 0
			for range 2 {
				result := <-results
				if result.err != nil {
					t.Fatalf("competing settlement: %v", result.err)
				}
				if result.response.GetCommitted() != nil {
					committed++
				}
				if result.response.GetDuplicate() != nil {
					duplicate++
				}
			}
			if committed != 1 || duplicate != 1 {
				t.Fatalf("competing settlement outcomes = committed:%d duplicate:%d", committed, duplicate)
			}
			var resultEventID sql.NullString
			var resultEventType, resultToolUseID sql.NullString
			if err := admin.QueryRowContext(context.Background(), `SELECT route.status,route.result_event_id,result.type,result.payload_json::jsonb->>'tool_use_id'
				FROM session_pending_tool_uses route
				LEFT JOIN session_events result ON result.workspace_id=route.workspace_id AND result.session_id=route.session_id AND result.event_id=route.result_event_id
				WHERE route.workspace_id='default' AND route.session_id=$1 AND route.tool_use_event_id=$2`, sessionID, toolUseID).Scan(&routeStatus, &resultEventID, &resultEventType, &resultToolUseID); err != nil {
				t.Fatalf("read committed settlement route: %v", err)
			}
			if routeStatus != "resolved" || !resultEventID.Valid || resultEventType.String != "agent.tool_result" || resultToolUseID.String != toolUseID {
				t.Fatalf("committed settlement route = %s/%v/%v/%v; want exact linked terminal result", routeStatus, resultEventID, resultEventType, resultToolUseID)
			}
		})
	}
}

func TestPostgreSQLToolSettlementRejectsNonsettleableRoutesWithoutResult(t *testing.T) {
	for _, routeState := range []string{"missing", "pending", "cancelled"} {
		t.Run(routeState, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_nonsettleable_" + routeState
			threadID := "thr_nonsettleable_" + routeState
			bindingID := "bind_nonsettleable_" + routeState
			podUID := "pod_nonsettleable_" + routeState
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_nonsettleable_"+routeState, "call_nonsettleable_"+routeState, "Read", `{"path":"owned.txt"}`)
			switch routeState {
			case "missing":
				if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
					t.Fatalf("delete settlement route: %v", err)
				}
			case "pending":
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='pending',decision=NULL WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
					t.Fatalf("make pending settlement route: %v", err)
				}
			case "cancelled":
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='cancelled' WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID); err != nil {
					t.Fatalf("make cancelled settlement route: %v", err)
				}
			}
			response, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, bridgeCompletedToolSettlementForTest(toolUseID, "unowned")))
			if status.Code(err) != codes.FailedPrecondition || response != nil {
				t.Fatalf("%s settlement = %#v/%v; want FailedPrecondition", routeState, response, err)
			}
			var results int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$2`, sessionID, toolUseID).Scan(&results); err != nil {
				t.Fatalf("count rejected settlement results: %v", err)
			}
			if results != 0 {
				t.Fatalf("%s settlement results = %d; want zero", routeState, results)
			}
		})
	}
}

func TestPostgreSQLRuntimeTerminationClosesMainAndSiblingToolRoutesAtomically(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "termination_tool_routes")
	const (
		childID          = "thr_termination_tool_routes_child"
		requestChildID   = "thr_termination_open_request_child"
		requestID        = "mreq_termination_open_request_child"
		requestStartID   = "evt_termination_open_request_child"
		interruptChildID = "thr_termination_interrupt_child"
		interruptInputID = "rin_termination_interrupt_child"
		interruptEventID = "evt_termination_interrupt_child"
	)
	seedBridgeAPIChildThread(t, fixture.admin, "default", fixture.sessionID, fixture.threadID, childID)
	seedBridgeAPIChildThread(t, fixture.admin, "default", fixture.sessionID, fixture.threadID, requestChildID)
	seedBridgeAPIChildThread(t, fixture.admin, "default", fixture.sessionID, fixture.threadID, interruptChildID)
	mainScope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
	childScope := bridgeAPIScope(fixture.sessionID, childID, fixture.bindingID, 1, fixture.podUID)
	mainToolID := writeDurableOrdinaryToolUseForTest(t, fixture.store, mainScope, "mreq_termination_main_tool", "call_termination_main_tool", "Read", `{"path":"main.txt"}`)
	childToolID := writeDurableOrdinaryToolUseForTest(t, fixture.store, childScope, "mreq_termination_child_tool", "call_termination_child_tool", "Read", `{"path":"child.txt"}`)
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE session_pending_tool_uses SET status='pending',decision=NULL WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, fixture.sessionID, childToolID); err != nil {
		t.Fatalf("make sibling pending/ask route: %v", err)
	}
	seedBridgeAPIEvent(t, fixture.admin, "default", fixture.sessionID, requestChildID, requestStartID, 1,
		"span.model_request_start", `{"type":"span.model_request_start","model_request_id":"`+requestID+`"}`)
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE session_events
		SET model_request_id=$3, projection_json='{"request_kind":"agent_provider_request","context_through_message_sequence":0}'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, fixture.sessionID, requestStartID, requestID); err != nil {
		t.Fatalf("seed sibling open Request: %v", err)
	}
	seedBridgeAPIEvent(t, fixture.admin, "default", fixture.sessionID, interruptChildID, interruptEventID, 1,
		"user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, fixture.admin, RuntimeJob{
		WorkspaceID: "default", SessionID: fixture.sessionID, SessionThreadID: interruptChildID,
		RuntimeInputID: interruptInputID, InputKind: "interrupt_control", EventIDs: []string{interruptEventID},
		SequenceFrom: 1, SequenceTo: 1,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(fixture.runtime))
	enqueueInterruptExhaustionJob(t, queueStore, fixture.sessionID, interruptChildID, interruptInputID,
		"interrupt_control", interruptEventID, 1, queue.DefaultMaxAttempts, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	running, err := fixture.store.WriteEvent(context.Background(), closeoutWriteEventRequest(mainScope, "rwrite_termination_tool_routes"))
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("open termination owner Turn = %#v/%v", running, err)
	}
	request := &bridgev1.CommitRuntimeTerminationRequest{
		Scope: mainScope, RuntimeWriteId: running.GetCommitted().GetEventId(),
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
	}
	if _, err := fixture.admin.ExecContext(context.Background(), `CREATE FUNCTION fail_terminal_sibling_route_close() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected terminal route failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_terminal_sibling_route_close BEFORE UPDATE ON session_pending_tool_uses
		FOR EACH ROW WHEN (OLD.session_thread_id = 'thr_termination_tool_routes_child') EXECUTE FUNCTION fail_terminal_sibling_route_close()`); err != nil {
		t.Fatalf("install terminal route rollback trigger: %v", err)
	}
	if response, err := fixture.server.CommitRuntimeTermination(context.Background(), request); err == nil || response != nil {
		t.Fatalf("terminal sibling route failure = %#v/%v; want atomic rollback", response, err)
	}
	var terminalEvents, results, nonterminalRoutes, requestEnds int
	var interruptInboxStatus, interruptQueueStatus string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('session.status_terminated','session.thread_status_terminated')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id' IN ($2,$3)),
		(SELECT count(*) FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id IN ($2,$3) AND status IN ('pending','resolving'))`,
		fixture.sessionID, mainToolID, childToolID).Scan(&terminalEvents, &results, &nonterminalRoutes); err != nil {
		t.Fatalf("read rolled-back termination routes: %v", err)
	}
	if terminalEvents != 0 || results != 0 || nonterminalRoutes != 2 {
		t.Fatalf("rolled-back termination = terminal:%d results:%d nonterminal:%d", terminalEvents, results, nonterminalRoutes)
	}
	if _, err := fixture.admin.ExecContext(context.Background(), `DROP TRIGGER fail_terminal_sibling_route_close ON session_pending_tool_uses; DROP FUNCTION fail_terminal_sibling_route_close()`); err != nil {
		t.Fatalf("remove terminal route rollback trigger: %v", err)
	}
	committed, err := fixture.server.CommitRuntimeTermination(context.Background(), request)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("terminate all Tool routes = %#v/%v", committed, err)
	}
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('session.status_terminated','session.thread_status_terminated')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id' IN ($2,$3)),
		(SELECT count(*) FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id IN ($2,$3) AND status IN ('pending','resolving')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end' AND model_request_id=$4),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$5),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$6)`,
		fixture.sessionID, mainToolID, childToolID, requestID, interruptInputID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, interruptInputID),
	).Scan(&terminalEvents, &results, &nonterminalRoutes, &requestEnds, &interruptInboxStatus, &interruptQueueStatus); err != nil {
		t.Fatalf("read committed termination routes: %v", err)
	}
	if terminalEvents < 4 || results != 2 || nonterminalRoutes != 0 || requestEnds != 1 ||
		interruptInboxStatus != "cancelled" || interruptQueueStatus != queue.StatusCancelled {
		t.Fatalf("committed termination = terminal:%d results:%d nonterminal:%d requestEnds:%d interrupt:%s/%s; want all siblings closed",
			terminalEvents, results, nonterminalRoutes, requestEnds, interruptInboxStatus, interruptQueueStatus)
	}
	replayed, err := fixture.server.CommitRuntimeTermination(context.Background(), request)
	if err != nil || replayed.GetDuplicate() == nil {
		t.Fatalf("replay termination = %#v/%v; want duplicate", replayed, err)
	}
}
