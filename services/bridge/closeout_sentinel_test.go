package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestCloseoutSentinelTaxonomyPreservesGRPCStatus(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		wantCode string
	}{
		{name: "superseded", err: scopeSupersededError(status.Error(codes.FailedPrecondition, "stale")), wantCode: closeoutScopeSupersededCode},
		{name: "unrepairable", err: closeoutUnrepairableError(status.Error(codes.Internal, "invalid")), wantCode: closeoutUnrepairableCode},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotCode, ok := closeoutSentinelCode(test.err)
			if !ok || gotCode != test.wantCode {
				t.Fatalf("closeoutSentinelCode() = (%q, %t); want (%q, true)", gotCode, ok, test.wantCode)
			}
			if status.Code(test.err) == codes.Unknown {
				t.Fatalf("sentinel lost wrapped gRPC status: %v", test.err)
			}
		})
	}
	if _, ok := closeoutSentinelCode(status.Error(codes.Unavailable, "transient")); ok {
		t.Fatal("ordinary gRPC error was classified as a closeout sentinel")
	}
}

func TestBridgeAPIServerReturnsClosedStaleResultsForSupersededCloseouts(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "typed_stale")
	scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
	modelRequestID := "mreq_" + fixture.sessionID
	seedBridgeAPIRequestStart(t, fixture.store, scope, "rwrite_start_"+fixture.sessionID, modelRequestID, requestKindAgentProviderRequest, 0)
	if _, err := fixture.admin.ExecContext(context.Background(),
		`DELETE FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`,
		fixture.sessionID,
	); err != nil {
		t.Fatalf("delete runtime binding: %v", err)
	}

	written, err := fixture.server.WriteEvent(context.Background(), closeoutWriteEventRequest(scope, "rwrite_"+fixture.sessionID))
	if err != nil || written.GetStale() == nil {
		t.Fatalf("WriteEvent stale result = %#v err=%v", written, err)
	}
	idle, err := fixture.server.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
		Scope: scope, DurableTurnId: "rwrite_start_" + fixture.sessionID, StopReasonJson: `{"type":"end_turn"}`,
	})
	if err != nil || idle.GetStale() == nil {
		t.Fatalf("FinishIdle stale result = %#v err=%v", idle, err)
	}
	ended, err := fixture.server.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_end_" + fixture.sessionID, ModelRequestId: modelRequestID,
		FinishReason: "stop", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"},
	})
	if err != nil || ended.GetStale() == nil {
		t.Fatalf("WriteRequestEnd stale result = %#v err=%v", ended, err)
	}
	terminated, err := fixture.server.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: "rwrite_termination_" + fixture.sessionID,
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
	})
	if err != nil || terminated.GetStale() == nil {
		t.Fatalf("CommitRuntimeTermination stale result = %#v err=%v", terminated, err)
	}
}

func TestPostgreSQLRuntimeTerminationReceiptOwnsReplayAfterUnbinding(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "termination_replay_binding")
	scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
	const committedInputID = "rin_termination_replay_binding"
	seedBridgeAPIEvent(t, fixture.admin, "default", fixture.sessionID, fixture.threadID,
		"evt_termination_replay_input", 1, "user.message", `{"content":[{"type":"text","text":"settled"}]}`)
	seedBridgeAPIRuntimeInbox(t, fixture.admin, "default", fixture.sessionID, fixture.threadID,
		committedInputID, "messages", `["evt_termination_replay_input"]`, "accepted",
		fixture.bindingID, fixture.podUID, 1, 1)
	commitInputsRequest := &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: committedInputID}
	if committed, err := fixture.server.CommitInputs(context.Background(), commitInputsRequest); err != nil || committed.GetCommitted() == nil {
		t.Fatalf("CommitInputs before termination = %#v/%v; want committed", committed, err)
	}
	oldRunningRequest := closeoutWriteEventRequest(scope, "rwrite_termination_replay_old_running")
	oldRunning, err := fixture.store.WriteEvent(context.Background(), oldRunningRequest)
	if err != nil || oldRunning.GetCommitted() == nil {
		t.Fatalf("open old durable Runtime turn = %#v/%v", oldRunning, err)
	}
	modelRequestID := "mreq_termination_replay_binding"
	seedBridgeAPIRequestStart(t, fixture.store, scope, "rwrite_termination_replay_request_start", modelRequestID, requestKindAgentProviderRequest, 1)
	requestEndRequest := &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_termination_replay_request_end", ModelRequestId: modelRequestID,
		FinishReason: "stop", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"},
	}
	if ended, err := fixture.server.WriteRequestEnd(context.Background(), requestEndRequest); err != nil || ended.GetCommitted() == nil {
		t.Fatalf("WriteRequestEnd before termination = %#v/%v; want committed", ended, err)
	}
	finishIdleRequest := &bridgev1.FinishIdleRequest{
		Scope: scope, DurableTurnId: oldRunning.GetCommitted().GetEventId(), StopReasonJson: `{"type":"end_turn"}`,
	}
	if idle, err := finishIdleWithStagedCaptureForTest(t, fixture.admin, fixture.store, finishIdleRequest); err != nil || idle.GetCommitted() == nil {
		t.Fatalf("FinishIdle before termination = %#v/%v; want committed", idle, err)
	}
	running, err := fixture.store.WriteEvent(context.Background(), closeoutWriteEventRequest(scope, "rwrite_termination_replay_binding"))
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("open durable Runtime turn = %#v/%v", running, err)
	}
	const deliveryID = "mail_termination_replay_binding"
	seedAgentMailCustody(t, fixture.admin, fixture.sessionID, fixture.threadID, deliveryID, time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC))
	const cleanupJobID = "cleanup_termination_replay_binding"
	if _, err := fixture.admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_after='2026-01-01T00:30:00Z', cleanup_job_id=$2,
		        cleanup_enqueued_at='2026-01-01T00:30:00Z', cleanup_claimed_at='2026-01-01T00:31:00Z'
		  WHERE workspace_id='default' AND session_id=$1`, fixture.sessionID, cleanupJobID,
	); err != nil {
		t.Fatalf("seed claimed cleanup custody: %v", err)
	}
	runtimeWriteID := running.GetCommitted().GetEventId()
	request := &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: runtimeWriteID,
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
	}
	committed, err := fixture.server.CommitRuntimeTermination(context.Background(), request)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("CommitRuntimeTermination = %#v/%v; want committed", committed, err)
	}
	var eventsBefore, operationsBefore, capturesBefore, queueBefore int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM sandbox_output_capture_operations WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key LIKE '%' || $1 || '%')`, fixture.sessionID,
	).Scan(&eventsBefore, &operationsBefore, &capturesBefore, &queueBefore); err != nil {
		t.Fatalf("read post-terminal mutation baseline: %v", err)
	}
	if replayed, err := fixture.server.CommitInputs(context.Background(), commitInputsRequest); err != nil || replayed.GetStale() == nil {
		t.Fatalf("post-terminal CommitInputs = %#v/%v; want stale", replayed, err)
	}
	if replayed, err := fixture.server.WriteEvent(context.Background(), oldRunningRequest); err != nil || replayed.GetStale() == nil {
		t.Fatalf("post-terminal WriteEvent = %#v/%v; want stale", replayed, err)
	}
	if replayed, err := fixture.server.WriteRequestEnd(context.Background(), requestEndRequest); err != nil || replayed.GetStale() == nil {
		t.Fatalf("post-terminal WriteRequestEnd = %#v/%v; want stale", replayed, err)
	}
	if replayed, err := fixture.server.FinishIdle(context.Background(), finishIdleRequest); err != nil || replayed.GetStale() == nil {
		t.Fatalf("post-terminal FinishIdle = %#v/%v; want stale", replayed, err)
	}
	var eventsAfter, operationsAfter, capturesAfter, queueAfter int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM sandbox_output_capture_operations WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key LIKE '%' || $1 || '%')`, fixture.sessionID,
	).Scan(&eventsAfter, &operationsAfter, &capturesAfter, &queueAfter); err != nil {
		t.Fatalf("read post-terminal mutation result: %v", err)
	}
	if eventsAfter != eventsBefore || operationsAfter != operationsBefore || capturesAfter != capturesBefore || queueAfter != queueBefore {
		t.Fatalf("ordinary post-terminal replay mutated events/operations/captures/queue = %d/%d/%d/%d -> %d/%d/%d/%d",
			eventsBefore, operationsBefore, capturesBefore, queueBefore,
			eventsAfter, operationsAfter, capturesAfter, queueAfter)
	}
	var runtimeStatus, statusEventID string
	var liveBindings int
	var runningSince, idleSince, cleanupAfter, cleanupEnqueuedAt, cleanupClaimedAt sql.NullTime
	var bindingID, storedCleanupJobID sql.NullString
	var bindingGeneration sql.NullInt64
	if err := fixture.admin.QueryRowContext(context.Background(),
		`SELECT status, status_event_id, running_since, idle_since, cleanup_after,
		        cleanup_job_id, cleanup_enqueued_at, cleanup_claimed_at,
		        binding_id, binding_generation,
		        (SELECT count(*) FROM session_runtime_bindings
		          WHERE workspace_id='default' AND session_id=$1)
		   FROM session_runtime_status
		  WHERE workspace_id='default' AND session_id=$1`, fixture.sessionID,
	).Scan(&runtimeStatus, &statusEventID, &runningSince, &idleSince, &cleanupAfter,
		&storedCleanupJobID, &cleanupEnqueuedAt, &cleanupClaimedAt,
		&bindingID, &bindingGeneration, &liveBindings); err != nil {
		t.Fatalf("read Runtime status after termination: %v", err)
	}
	if runtimeStatus != "idle" || statusEventID != committed.GetCommitted().GetCloseoutEventId() ||
		runningSince.Valid || !idleSince.Valid || cleanupAfter.Valid || storedCleanupJobID.Valid ||
		cleanupEnqueuedAt.Valid || cleanupClaimedAt.Valid || bindingID.Valid ||
		bindingGeneration.Valid || liveBindings != 0 {
		t.Fatalf("Runtime status after termination = %q event=%q running=%+v idle=%+v cleanup=%+v/%+v/%+v/%+v binding=%+v/%+v live=%d; want receipt-owned unbound terminal residency", runtimeStatus, statusEventID, runningSince, idleSince, cleanupAfter, storedCleanupJobID, cleanupEnqueuedAt, cleanupClaimedAt, bindingID, bindingGeneration, liveBindings)
	}
	inputID := completionRuntimeInputID(deliveryID)
	var inboxStatus, queueStatus string
	if err := fixture.admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox
		  WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, fixture.sessionID, inputID,
	).Scan(&inboxStatus); err != nil {
		t.Fatalf("read queued Inbox after termination: %v", err)
	}
	if err := fixture.admin.QueryRowContext(context.Background(),
		`SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$1`,
		queue.FormatRuntimeInputDedupeKey("default", fixture.sessionID, inputID),
	).Scan(&queueStatus); err != nil {
		t.Fatalf("read queued job after termination: %v", err)
	}
	if inboxStatus != "cancelled" || queueStatus != "cancelled" {
		t.Fatalf("queued termination custody = Inbox %q Queue %q; want cancelled/cancelled", inboxStatus, queueStatus)
	}
	if _, err := fixture.store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("LoadContext after termination error = %v; want FailedPrecondition", err)
	}
	if _, err := fixture.store.RefreshRuntimeBindingToken(context.Background(), &bridgev1.RefreshRuntimeBindingTokenRequest{Scope: scope}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RefreshRuntimeBindingToken after termination error = %v; want FailedPrecondition", err)
	}
	duplicate, err := fixture.server.CommitRuntimeTermination(context.Background(), request)
	if err != nil || duplicate.GetDuplicate() == nil ||
		duplicate.GetDuplicate().GetFailureEventId() != committed.GetCommitted().GetFailureEventId() ||
		duplicate.GetDuplicate().GetCloseoutEventId() != committed.GetCommitted().GetCloseoutEventId() {
		t.Fatalf("same-binding termination replay = %#v/%v; want exact duplicate", duplicate, err)
	}
	cleanupStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(fixture.runtime), 9090)
	cleanupPlan, err := cleanupStore.PrepareRuntimeCommand(context.Background(), RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID: "qjob_" + cleanupJobID, LeaseToken: "lease_" + cleanupJobID,
		Kind: queue.KindCleanupSession, WorkspaceID: "default", SessionID: fixture.sessionID,
		RuntimeInputID: "cleanup_session:" + cleanupJobID, CleanupJobID: cleanupJobID,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + fixture.sessionID + `","cleanup_job_id":"` + cleanupJobID + `"}`,
	})
	if err != nil || !cleanupPlan.StaleAccepted || cleanupPlan.CleanupSession != nil {
		t.Fatalf("retired cleanup custody plan = %#v/%v; want stale without Runtime command", cleanupPlan, err)
	}
}

func TestPostgreSQLRuntimeTerminationRejectsAbsentResidencyRowAtomically(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "termination_absent_residency")
	scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
	running, err := fixture.store.WriteEvent(context.Background(), closeoutWriteEventRequest(scope, "rwrite_termination_absent_residency"))
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("open durable Runtime turn = %#v/%v", running, err)
	}
	if _, err := fixture.admin.ExecContext(context.Background(),
		`DELETE FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, fixture.sessionID,
	); err != nil {
		t.Fatalf("remove optional Runtime status row: %v", err)
	}
	committed, err := fixture.server.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: running.GetCommitted().GetEventId(),
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
	})
	if committed != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CommitRuntimeTermination without residency row = %#v/%v; want FailedPrecondition", committed, err)
	}
	var rows, terminalEvents, operations int
	var sessionStatus string
	if err := fixture.admin.QueryRowContext(context.Background(),
		`SELECT
		   (SELECT count(*) FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		   (SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		   (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1
		      AND type IN ('session.error','session.status_terminated')),
		   (SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1
		      AND operation='commit_runtime_termination')`, fixture.sessionID,
	).Scan(&rows, &sessionStatus, &terminalEvents, &operations); err != nil {
		t.Fatalf("read absent-row termination rollback: %v", err)
	}
	if rows != 0 || sessionStatus == "terminated" || terminalEvents != 0 || operations != 0 {
		t.Fatalf("absent-row termination rollback = rows %d status %q events %d operations %d; want no terminal mutation",
			rows, sessionStatus, terminalEvents, operations)
	}
}

func TestBridgeAPIServerKeepsStructuralFailureOutOfStaleUnion(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "structural")
	response, err := fixture.server.WriteEvent(context.Background(), closeoutWriteEventRequest(
		bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID),
		"",
	))
	if response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid WriteEvent response=%#v err=%v; want InvalidArgument", response, err)
	}
}

func TestBridgeAPIServerMapsInputSettlementScopeSupersessionToTypedStale(t *testing.T) {
	for _, rpc := range []string{"commit_inputs", "commit_task_notification_result"} {
		for _, staleKind := range []string{"binding_generation", "pod_uid", "session_deleted"} {
			t.Run(rpc+"/"+staleKind, func(t *testing.T) {
				fixture := newCloseoutSentinelFixture(t, rpc+"_"+staleKind)
				scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
				inputID := "rin_" + fixture.sessionID
				if rpc == "commit_inputs" {
					seedBridgeAPIEvent(t, fixture.admin, "default", fixture.sessionID, fixture.threadID, "evt_"+fixture.sessionID, 1, "user.message", `{"content":[{"type":"text","text":"stale"}]}`)
					seedBridgeAPIRuntimeInbox(t, fixture.admin, "default", fixture.sessionID, fixture.threadID, inputID, "messages",
						`["evt_`+fixture.sessionID+`"]`, "accepted", fixture.bindingID, fixture.podUID, 1, 1)
				} else {
					inputID = "task_notification:task_" + fixture.sessionID
					seedBridgeAPITaskNotificationInbox(t, fixture.admin, "default", fixture.sessionID, fixture.threadID, inputID, fixture.bindingID, fixture.podUID)
				}
				switch staleKind {
				case "binding_generation":
					scope.Binding.BindingGeneration++
				case "pod_uid":
					scope.Binding.TargetPodUid = "pod_superseded"
				case "session_deleted":
					if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted'
						WHERE workspace_id='default' AND id=$1`, fixture.sessionID); err != nil {
						t.Fatalf("delete superseded Session: %v", err)
					}
				}
				if rpc == "commit_inputs" {
					response, err := fixture.server.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: inputID})
					if err != nil || response.GetStale() == nil {
						t.Fatalf("CommitInputs superseded scope = %#v/%v; want typed stale", response, err)
					}
				} else {
					response, err := fixture.server.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{Scope: scope, RuntimeInputId: inputID})
					if err != nil || response.GetStale() == nil {
						t.Fatalf("CommitTaskNotificationResult superseded scope = %#v/%v; want typed stale", response, err)
					}
				}
				var inboxStatus string
				var operations int
				if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
					(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
					(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`,
					fixture.sessionID, inputID,
				).Scan(&inboxStatus, &operations); err != nil {
					t.Fatalf("read typed stale input facts: %v", err)
				}
				if inboxStatus != "accepted" || operations != 0 {
					t.Fatalf("typed stale mutated input facts = Inbox %s operations %d", inboxStatus, operations)
				}
			})
		}
	}
}

func TestBridgeAPIServerDoesNotClassifyMalformedInputSettlementsAsStale(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "input_structural_control")
	scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 2, fixture.podUID)
	if response, err := fixture.server.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{Scope: scope}); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed CommitInputs = %#v/%v; want InvalidArgument", response, err)
	}
	if response, err := fixture.server.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{Scope: scope}); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed CommitTaskNotificationResult = %#v/%v; want InvalidArgument", response, err)
	}
}

func TestCloseoutTerminalChildSentinel(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "terminal_child")
	client := dbconnect.NewClientForTesting(fixture.runtime)
	childID := "thr_closeout_terminal_child_subagent"
	if _, err := fixture.admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, agent_type, task_name, status,
			created_at, last_active_at, updated_at
		) VALUES ('default', $1, $2, $3, 'subagent', 'public', 'worker', 'worker', 'terminated',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		childID, fixture.sessionID, fixture.threadID,
	); err != nil {
		t.Fatalf("seed terminal child: %v", err)
	}
	err := client.WithWorkspaceTx(context.Background(), "default", "closeout.terminal_child", func(tx *dbconnect.Tx) error {
		return updateChildThreadStatusTx(
			context.Background(), tx,
			bridgeAPIScope(fixture.sessionID, childID, fixture.bindingID, 1, fixture.podUID),
			"idle", fixture.store.now(),
		)
	})
	if code, ok := closeoutSentinelCode(err); !ok || code != closeoutScopeSupersededCode {
		t.Fatalf("terminal child error = %v; want %q sentinel", err, closeoutScopeSupersededCode)
	}
}

type closeoutSentinelFixture struct {
	runtime   *sql.DB
	admin     *sql.DB
	sessionID string
	threadID  string
	bindingID string
	podUID    string
	store     *PostgreSQLBridgeAPIStore
	server    BridgeAPIServer
}

func newCloseoutSentinelFixture(t *testing.T, suffix string) closeoutSentinelFixture {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	suffix = strings.ReplaceAll(suffix, " ", "_")
	sessionID := "sesn_closeout_" + suffix
	threadID := "thr_closeout_" + suffix
	bindingID := "bind_closeout_" + suffix
	podUID := "pod_uid_closeout_" + suffix
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	return closeoutSentinelFixture{
		runtime: runtimeDB, admin: admin, sessionID: sessionID, threadID: threadID,
		bindingID: bindingID, podUID: podUID, store: store, server: BridgeAPIServer{store: store},
	}
}

func closeoutWriteEventRequest(scope *bridgev1.RuntimeScope, writeID string) *bridgev1.WriteEventRequest {
	return &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: writeID, EventType: "session.status_running",
		PayloadJson: `{"type":"session.status_running"}`,
	}
}
