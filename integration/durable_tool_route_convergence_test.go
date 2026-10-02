package integration

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

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
	seedRuntimeInboxBirthForJob(t, fixture.admin, jobrunner.RuntimeJob{
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
