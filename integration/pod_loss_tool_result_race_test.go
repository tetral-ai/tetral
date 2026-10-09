package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

// Pod-loss repair settles an unanchored send_message Tool Use itself through
// the shared terminal writer, while its allowed route stays settleable by the
// Runtime. Both owners run under their installed roles and queue behind one
// held Session row lock; PostgreSQL's lock queue, not elapsed time, selects
// the first committer. Exactly one result references the Tool Use, the
// Sandbox execution recorded on it is consumed once by that winner, and the
// loser is a typed stale settlement or a repair that writes no second result.
func TestPostgreSQLSeparatedOwnersPodLossToolResultRace(t *testing.T) {
	for _, first := range []string{"settlement", "recovery"} {
		t.Run(first+"_first", func(t *testing.T) {
			f := newSeparatedOwners(t, "pod_loss_result_"+first, false)
			scope := f.declare(t)
			separatedActiveFence(t, f, scope)
			seedBridgeAPIRequestStart(t, f.bridge, scope, "rwrite_race_start", "mreq_race", runtimecontrol.RequestKindAgentProviderRequest, 0)
			declared, err := f.bridge.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_race_delivery", ModelRequestId: "mreq_race",
				ToolDeclaration: sessionfixture.BridgeToolDeclarationWithRouteForTest("call_race_delivery", "send_message",
					`{"task_name":"worker","message":"hello"}`, "allow"),
			})
			if err != nil || declared.GetCommitted() == nil {
				t.Fatalf("declare send_message = %v/%v", declared, err)
			}
			toolID := declared.GetCommitted().GetEventId()
			// A finished, unconsumed Sandbox execution recorded on the Tool Use
			// observes the one consumption step both terminal writers share.
			const resultJSON = `{"status":"success","stdout":"done"}`
			f.sql(t, `INSERT INTO session_runtime_tool_results (
				workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
				normalized_input_hash, tool_name, input_json, ack_status, result_json,
				model_tool_call_id, execution_state, execution_attempt_generation,
				result_digest, created_at, updated_at
			) VALUES ('default', $1, $2, $3, 'sandbox_tool', $4, 'send_message', $5, 'committed', $6,
				'call_race_delivery', 'terminal_unconsumed', 1, $7, now(), now())`,
				f.sessionID, scope.SessionThreadId, toolID, runtimecontrol.Sha256Hex(`{"message":"hello","task_name":"worker"}`),
				`{"message":"hello","task_name":"worker"}`, resultJSON, runtimecontrol.Sha256Hex(resultJSON))

			confirmationStarted, allowConfirmation := make(chan struct{}), make(chan struct{})
			var confirmationOnce sync.Once
			releaseConfirmation := func() { confirmationOnce.Do(func() { close(allowConfirmation) }) }
			t.Cleanup(releaseConfirmation)
			f.runner.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{GetPod: func(ctx context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
				close(confirmationStarted)
				select {
				case <-allowConfirmation:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return fixtureConfirmedMissingRuntimePod(ctx, namespace, name)
			}, LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
			}}
			type outcome struct {
				owner      string
				settlement *bridgev1.SettleToolResultResponse
				repaired   int
				err        error
			}
			results := make(chan outcome, 2)
			settle := func(ctx context.Context) error {
				response, err := f.bridge.SettleToolResult(ctx, sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "delivered-by-runtime")))
				results <- outcome{owner: "settlement", settlement: response, err: err}
				return nil
			}
			recover := func(ctx context.Context) error {
				count, err := repairRuntimePodLoss(ctx, f.runner)
				results <- outcome{owner: "recovery", repaired: count, err: err}
				return nil
			}
			// Pod confirmation runs outside the Session lock; the mutation
			// transaction then queues on the held Session row.
			separatedStartRun(f.ctx, t, recover)
			select {
			case <-confirmationStarted:
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}
			lock, err := f.admin.BeginTx(f.ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lock.Rollback() })
			var blocker int
			if err := lock.QueryRowContext(f.ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
				t.Fatal(err)
			}
			if _, err := lock.ExecContext(f.ctx, `SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, f.sessionID); err != nil {
				t.Fatal(err)
			}
			if first == "settlement" {
				separatedStartRun(f.ctx, t, settle)
			} else {
				releaseConfirmation()
			}
			firstPIDs := separatedBlockedPIDs(t, f, blocker, 1)
			if first == "settlement" {
				releaseConfirmation()
			} else {
				separatedStartRun(f.ctx, t, settle)
			}
			if pids := separatedBlockedPIDs(t, f, blocker, 2); len(firstPIDs) != 1 || len(pids) != 2 {
				t.Fatalf("two-owner Session queue = %v/%v", firstPIDs, pids)
			}
			if err := lock.Commit(); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				value := separatedWait(f.ctx, t, results)
				if value.err != nil {
					t.Fatalf("%s returned %v", value.owner, value.err)
				}
				if value.owner == "recovery" {
					if value.repaired != 1 {
						t.Fatalf("recovery repaired %d bindings; want 1", value.repaired)
					}
					continue
				}
				if first == "settlement" && value.settlement.GetCommitted() == nil {
					t.Fatalf("winning settlement = %v", value.settlement)
				}
				if first == "recovery" && value.settlement.GetStale() == nil {
					t.Fatalf("losing settlement = %v; want typed stale", value.settlement)
				}
			}
			var referencing int
			var resultID, payload, consumedBy, consumptionReason, executionState string
			if err := f.admin.QueryRowContext(f.ctx, `SELECT
				(SELECT count(*) FROM session_events WHERE session_id=$1 AND tool_use_event_id=$2),
				(SELECT event_id FROM session_events WHERE session_id=$1 AND tool_use_event_id=$2),
				(SELECT payload_json FROM session_events WHERE session_id=$1 AND tool_use_event_id=$2),
				(SELECT COALESCE(consumed_by_terminal_event_id,'') FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2),
				(SELECT COALESCE(consumption_reason,'') FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2),
				(SELECT execution_state FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2)`,
				f.sessionID, toolID).Scan(&referencing, &resultID, &payload, &consumedBy, &consumptionReason, &executionState); err != nil {
				t.Fatalf("read one Tool result for %s: %v", toolID, err)
			}
			wantReason, wantPayload := "conversation_tool_result", `"delivered-by-runtime"`
			if first == "recovery" {
				wantReason, wantPayload = "pod_lost", `"runtime_pod_lost_delivery_failed"`
			}
			if referencing != 1 || !strings.Contains(payload, wantPayload) || executionState != "consumed" || consumedBy != resultID || consumptionReason != wantReason {
				t.Fatalf("after %s first: results %d payload %s execution %s consumed by %q (%s), result %s",
					first, referencing, payload, executionState, consumedBy, consumptionReason, resultID)
			}
			sessionfixture.RequireToolRelationFactsForTest(t, f.admin, "default", f.sessionID)
			if first == "recovery" {
				// The stale outcome above came from the retired binding. A
				// replacement Runtime that settles the same Tool Use is stale
				// because its result already exists.
				replacement := declareReplacementScope(t, f.bridge.Client, f.runner.Client, scope)
				late, err := f.bridge.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(replacement, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "late-replacement")))
				if err != nil || late.GetStale() == nil {
					t.Fatalf("replacement settlement after Pod-loss result = %v/%v; want typed stale", late, err)
				}
				if f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1 AND tool_use_event_id=$2`, f.sessionID, toolID) != 1 {
					t.Fatal("replacement settlement added a second result")
				}
			}
			if parts := f.count(t, `SELECT count(*) FROM session_message_parts WHERE session_id=$1 AND part_kind='tool_result' AND model_tool_call_id='call_race_delivery'`, f.sessionID); parts != 1 {
				t.Fatalf("after %s first: stored result parts = %d; want exactly one", first, parts)
			}
			t.Logf("winner=%s result=%s consumption=%s", first, resultID, consumptionReason)
		})
	}
}
