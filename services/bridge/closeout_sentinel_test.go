package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
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

func TestPostgreSQLRuntimeTerminationReplayRevalidatesCurrentBinding(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "termination_replay_binding")
	scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
	running, err := fixture.store.WriteEvent(context.Background(), closeoutWriteEventRequest(scope, "rwrite_termination_replay_binding"))
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("open durable Runtime turn = %#v/%v", running, err)
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
	if _, err := fixture.admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id='bind_termination_replacement', binding_generation=2,
		        agent_runtime_pod_uid='pod_termination_replacement'
		  WHERE workspace_id='default' AND session_id=$1`, fixture.sessionID,
	); err != nil {
		t.Fatalf("replace Runtime binding: %v", err)
	}
	replay, err := fixture.server.CommitRuntimeTermination(context.Background(), request)
	if err != nil || replay.GetStale() == nil {
		t.Fatalf("old-Pod termination replay = %#v/%v; want stale", replay, err)
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
