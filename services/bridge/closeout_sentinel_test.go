package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestCloseoutWriteRejectionTaxonomy(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		wantCode string
		wantOK   bool
	}{
		{name: "superseded", err: scopeSupersededError(status.Error(codes.FailedPrecondition, "stale")), wantCode: closeoutScopeSupersededCode, wantOK: true},
		{name: "unrepairable", err: closeoutUnrepairableError(status.Error(codes.Internal, "invalid")), wantCode: closeoutUnrepairableCode, wantOK: true},
		{name: "validation", err: status.Error(codes.InvalidArgument, "invalid"), wantCode: closeoutUnrepairableCode, wantOK: true},
		{name: "idempotency conflict", err: status.Error(codes.AlreadyExists, "conflict"), wantCode: closeoutUnrepairableCode, wantOK: true},
		{name: "transient failed precondition", err: status.Error(codes.FailedPrecondition, "preparation not ready"), wantOK: false},
		{name: "transport", err: status.Error(codes.Unavailable, "database unavailable"), wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotCode, gotOK := closeoutWriteRejectionCode(test.err)
			if gotOK != test.wantOK || gotCode != test.wantCode {
				t.Fatalf("closeoutWriteRejectionCode() = (%q, %t); want (%q, %t)", gotCode, gotOK, test.wantCode, test.wantOK)
			}
			if status.Code(test.err) == codes.Unknown && test.wantOK {
				t.Fatalf("sentinel lost wrapped gRPC status: %v", test.err)
			}
		})
	}
}

func TestBridgeAPIServerWriteEventReturnsSupersededForEveryInitialCustodyEnd(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, fixture closeoutSentinelFixture)
		scope  func(fixture closeoutSentinelFixture) *bridgev1.RuntimeScope
		ctx    func(fixture closeoutSentinelFixture) context.Context
	}{
		{
			name: "binding absent",
			mutate: func(t *testing.T, fixture closeoutSentinelFixture) {
				if _, err := fixture.admin.ExecContext(context.Background(),
					`DELETE FROM session_runtime_bindings WHERE workspace_id = 'default' AND session_id = $1`, fixture.sessionID); err != nil {
					t.Fatalf("delete runtime binding: %v", err)
				}
			},
		},
		{
			name: "binding generation replaced",
			scope: func(fixture closeoutSentinelFixture) *bridgev1.RuntimeScope {
				return bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 2, fixture.podUID)
			},
		},
		{
			name: "session deleted",
			mutate: func(t *testing.T, fixture closeoutSentinelFixture) {
				if _, err := fixture.admin.ExecContext(context.Background(),
					`UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = 'default' AND id = $1`, fixture.sessionID); err != nil {
					t.Fatalf("mark session deleted: %v", err)
				}
			},
		},
		{
			name: "session not found",
			scope: func(fixture closeoutSentinelFixture) *bridgev1.RuntimeScope {
				return bridgeAPIScope("sesn_closeout_missing", "thr_closeout_missing", fixture.bindingID, 1, fixture.podUID)
			},
		},
		{
			name: "caller pod uid mismatch",
			ctx: func(fixture closeoutSentinelFixture) context.Context {
				return internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
					ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
					KubernetesPodUID: "pod_uid_other",
				})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCloseoutSentinelFixture(t, test.name)
			modelRequestID := "mreq_" + fixture.sessionID
			start, err := fixture.store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope:                         bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID),
				RuntimeWriteId:                "rwrite_start_" + fixture.sessionID,
				ModelRequestId:                modelRequestID,
				EventType:                     "span.model_request_start",
				PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
				ContextThroughMessageSequence: bridgeAPIInt64(0),
				RequestKind:                   "agent_provider_request",
				SessionVisible:                false,
			})
			if err != nil {
				t.Fatalf("seed request start: %v", err)
			}
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			scope := bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID)
			if test.scope != nil {
				scope = test.scope(fixture)
			}
			ctx := context.Background()
			if test.ctx != nil {
				ctx = test.ctx(fixture)
			}
			response, err := fixture.server.WriteEvent(ctx, closeoutWriteEventRequest(scope, "rwrite_"+fixture.sessionID))
			if err != nil {
				t.Fatalf("WriteEvent error = %v; want rejected ACK", err)
			}
			if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
				response.GetAck().GetErrorCode() != closeoutScopeSupersededCode {
				t.Fatalf("WriteEvent ack = %+v; want rejected %q", response.GetAck(), closeoutScopeSupersededCode)
			}
			assertNoCloseoutBridgeOperation(t, fixture, "rwrite_"+fixture.sessionID)
			idleResponse, err := fixture.server.FinishIdle(ctx, &bridgev1.FinishIdleRequest{
				Scope:          scope,
				DurableTurnId:  "rwrite_start_" + fixture.sessionID,
				StopReasonJson: `{"type":"end_turn"}`,
			})
			if err != nil {
				t.Fatalf("FinishIdle error = %v; want rejected ACK", err)
			}
			if idleResponse.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
				idleResponse.GetAck().GetErrorCode() != closeoutScopeSupersededCode {
				t.Fatalf("FinishIdle ack = %+v; want rejected %q", idleResponse.GetAck(), closeoutScopeSupersededCode)
			}
			assertNoCloseoutBridgeOperation(t, fixture, "rwrite_idle_"+fixture.sessionID)
			requestEndResponse, err := fixture.server.WriteRequestEnd(ctx, &bridgev1.WriteRequestEndRequest{
				Scope:                    scope,
				RuntimeWriteId:           "rwrite_request_end_" + fixture.sessionID,
				ModelRequestId:           modelRequestID,
				ModelRequestStartEventId: start.GetEventId(),
				FinishReason:             "stop",
				UsageJson:                `{}`,
			})
			if err != nil {
				t.Fatalf("WriteRequestEnd error = %v; want rejected ACK", err)
			}
			if requestEndResponse.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
				requestEndResponse.GetAck().GetErrorCode() != closeoutScopeSupersededCode {
				t.Fatalf("WriteRequestEnd ack = %+v; want rejected %q", requestEndResponse.GetAck(), closeoutScopeSupersededCode)
			}
			assertNoCloseoutBridgeOperation(t, fixture, "mreq_"+fixture.sessionID+":rwrite_request_end_"+fixture.sessionID)
			terminationResponse, err := fixture.server.CommitRuntimeTermination(ctx, &bridgev1.CommitRuntimeTerminationRequest{
				Scope:          scope,
				RuntimeWriteId: "rwrite_termination_" + fixture.sessionID,
				FailureJson:    `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
			})
			if err != nil {
				t.Fatalf("CommitRuntimeTermination error = %v; want rejected ACK", err)
			}
			if terminationResponse.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
				terminationResponse.GetAck().GetErrorCode() != closeoutScopeSupersededCode {
				t.Fatalf("CommitRuntimeTermination ack = %+v; want rejected %q", terminationResponse.GetAck(), closeoutScopeSupersededCode)
			}
			assertNoCloseoutBridgeOperation(t, fixture, "session:rwrite_termination_"+fixture.sessionID)
		})
	}
}

func TestBridgeAPIServerCloseoutUnrepairableAndTransientArms(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "unrepairable")
	invalid, err := fixture.server.WriteEvent(context.Background(), closeoutWriteEventRequest(
		bridgeAPIScope(fixture.sessionID, fixture.threadID, fixture.bindingID, 1, fixture.podUID),
		"",
	))
	if err != nil {
		t.Fatalf("invalid WriteEvent error = %v; want rejected ACK", err)
	}
	if invalid.GetAck().GetErrorCode() != closeoutUnrepairableCode {
		t.Fatalf("invalid WriteEvent errorCode = %q; want %q", invalid.GetAck().GetErrorCode(), closeoutUnrepairableCode)
	}

	missingThreadScope := bridgeAPIScope(fixture.sessionID, "thr_missing_closeout", fixture.bindingID, 1, fixture.podUID)
	missing, err := fixture.server.WriteEvent(context.Background(), closeoutWriteEventRequest(missingThreadScope, "rwrite_missing_thread"))
	if err != nil {
		t.Fatalf("missing-thread WriteEvent error = %v; want rejected ACK", err)
	}
	if missing.GetAck().GetErrorCode() != closeoutUnrepairableCode {
		t.Fatalf("missing-thread WriteEvent errorCode = %q; want %q", missing.GetAck().GetErrorCode(), closeoutUnrepairableCode)
	}
	missingIdle, err := fixture.server.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
		Scope:          missingThreadScope,
		DurableTurnId:  "rwrite_missing_thread_idle",
		StopReasonJson: `{"type":"end_turn"}`,
	})
	if err != nil {
		t.Fatalf("missing-thread FinishIdle error = %v; want rejected ACK", err)
	}
	if missingIdle.GetAck().GetErrorCode() != closeoutUnrepairableCode {
		t.Fatalf("missing-thread FinishIdle errorCode = %q; want %q", missingIdle.GetAck().GetErrorCode(), closeoutUnrepairableCode)
	}
}

func TestCloseoutTerminalChildSentinel(t *testing.T) {
	fixture := newCloseoutSentinelFixture(t, "lower_layers")
	client := dbconnect.NewClientForTesting(fixture.runtime)

	childID := "thr_closeout_terminal_child"
	if _, err := fixture.admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, agent_type, task_name, status,
			created_at, last_active_at, updated_at
		) VALUES ('default', $1, $2, $3, 'subagent', 'public', 'worker', 'worker', 'terminated',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		childID, fixture.sessionID, fixture.threadID); err != nil {
		t.Fatalf("seed terminal child: %v", err)
	}
	err := client.WithWorkspaceTx(context.Background(), "default", "closeout.terminal_child", func(tx *dbconnect.Tx) error {
		return updateChildThreadStatusTx(
			context.Background(),
			tx,
			bridgeAPIScope(fixture.sessionID, childID, fixture.bindingID, 1, fixture.podUID),
			"idle",
			fixture.store.now(),
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
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	suffix = strings.ReplaceAll(suffix, " ", "_")
	sessionID := "sesn_closeout_" + suffix
	threadID := "thr_closeout_" + suffix
	bindingID := "bind_closeout_" + suffix
	podUID := "pod_uid_closeout_" + suffix
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	return closeoutSentinelFixture{
		runtime:   runtime,
		admin:     admin,
		sessionID: sessionID,
		threadID:  threadID,
		bindingID: bindingID,
		podUID:    podUID,
		store:     store,
		server:    BridgeAPIServer{store: store},
	}
}

func closeoutWriteEventRequest(scope *bridgev1.RuntimeScope, writeID string) *bridgev1.WriteEventRequest {
	return &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: writeID,
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
	}
}

func assertNoCloseoutBridgeOperation(t *testing.T, fixture closeoutSentinelFixture, writeID string) {
	t.Helper()
	var count int
	if err := fixture.admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND idempotency_key = $1`,
		writeID,
	).Scan(&count); err != nil {
		t.Fatalf("count closeout bridge operations: %v", err)
	}
	if count != 0 {
		t.Fatalf("bridge operation count = %d; want 0", count)
	}
}
