package agentruntimebridge

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLMCPExecutionBudgetPreservesCommitRecovery(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		for _, variant := range []string{"held-response-execution-deadline", "shorter-grpc-caller-deadline", "commit-ack-drop-second-bridge"} {
			t.Run(adapter+"/"+variant, func(t *testing.T) {
				options := map[string]any{"executionTimeoutMs": 1000}
				if variant == "commit-ack-drop-second-bridge" {
					options = map[string]any{"secondBridgeReplica": true}
				}
				h := newMCPDurableCompositionWithOptions(t, options)
				nonce := "budget-" + adapter + "-" + variant
				event, call := h.declare("work-"+adapter, nonce, "allow")
				action := map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce}
				if variant == "commit-ack-drop-second-bridge" {
					committed := make(chan struct{}, 1)
					h.bridge.mu.Lock()
					h.bridge.commitACKBarrier = func(ctx context.Context, _ *bridgev1.CommitMcpToolResultRequest, response *bridgev1.CommitMcpToolResultResponse) error {
						if response.GetCommitted() == nil {
							return fmt.Errorf("first actual Bridge failed to commit")
						}
						committed <- struct{}{}
						<-ctx.Done()
						return status.Error(codes.DeadlineExceeded, "controlled committed ACK loss until first-commit deadline")
					}
					h.bridge.mu.Unlock()
					pending := h.startAction(action)
					select {
					case <-committed:
					case <-h.ctx.Done():
						t.Fatal(h.ctx.Err())
					}
					var state string
					var results, publicResults int
					if err := h.admin.QueryRow(`SELECT mcp_claim_status,(SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1),(SELECT count(*) FROM session_events WHERE payload_json::jsonb->>'mcp_tool_use_id'=$1 AND type='agent.mcp_tool_result') FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, event).Scan(&state, &results, &publicResults); err != nil {
						t.Fatal(err)
					}
					if state != "stored" || results != 1 || publicResults != 0 {
						t.Fatalf("actual SQL before lost ACK=(%s,%d,%d)", state, results, publicResults)
					}
					output := h.joinAction(pending)
					want := fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce)
					h.assertMCPOriginalSettlement(event, output, 1, 0, want)
					h.bridge.mu.Lock()
					firstRequests := append([]*bridgev1.CommitMcpToolResultRequest(nil), h.bridge.commitRequests...)
					firstTimes := append([]time.Time(nil), h.bridge.commitTimes...)
					h.bridge.mu.Unlock()
					h.bridgeReplicaTwo.mu.Lock()
					secondRequests := append([]*bridgev1.CommitMcpToolResultRequest(nil), h.bridgeReplicaTwo.commitRequests...)
					secondTimes := append([]time.Time(nil), h.bridgeReplicaTwo.commitTimes...)
					h.bridgeReplicaTwo.mu.Unlock()
					if len(firstRequests) != 1 || len(secondRequests) != 1 || !proto.Equal(firstRequests[0], secondRequests[0]) || secondTimes[0].Sub(firstTimes[0]) < 10*time.Second {
						t.Fatal("receipt recovery did not reuse the frozen first commit on another Bridge after its reserve")
					}
					h.assertOAuthCounts(adapter, 0, 1, 1, 1, 1)
					action["replica"] = 1
					replay := h.action(action)
					h.assertMCPOriginalSettlement(event, replay, 1, 0, want)
					h.assertOAuthCounts(adapter, 0, 1, 1, 1, 1)
					h.evidence("execution-deadline-and-commit-recovery", adapter+"/"+variant, output, map[string]any{"execution_budget_ms": 170000, "first_commit_reserve_ms": 10000, "actual_sql_stored_before_ack_drop": true, "first_bridge_commits": 1, "second_bridge_commits": 1, "recovery_after_first_commit_ms": secondTimes[0].Sub(firstTimes[0]).Milliseconds(), "frozen_commit_request_equal": true, "stored_results": 1, "original_public_result": 1, "original_settlement_receipt": 1, "external_calls": 1, "accepted_effects": 1})
					return
				}
				originInitialize, originList := 1, 1
				var readinessSetup json.RawMessage
				if variant == "shorter-grpc-caller-deadline" {
					// Complete SDK readiness before testing cancellation of an accepted
					// external call. The separate execution-deadline case retains cold
					// preparation inside its shared budget.
					listed := h.action(map[string]any{"kind": "list", "adapter": adapter})
					var setup struct {
						Tools  []struct{ Name string }
						Counts struct{ Initialize, List, Call, Effects int }
					}
					if err := json.Unmarshal(listed, &setup); err != nil {
						t.Fatal(err)
					}
					if len(setup.Tools) != 1 || setup.Tools[0].Name != "read_echo" || setup.Counts.Initialize != 1 || setup.Counts.List != 1 || setup.Counts.Call != 0 || setup.Counts.Effects != 0 {
						t.Fatalf("SDK readiness setup did not complete without execution: %s", listed)
					}
					readinessSetup = h.action(map[string]any{"kind": "observe", "adapter": adapter})
					var ready struct {
						Connections []int
						IssuerCalls int
					}
					if err := json.Unmarshal(readinessSetup, &ready); err != nil {
						t.Fatal(err)
					}
					if len(ready.Connections) != 2 || ready.Connections[0] != 1 || ready.Connections[1] != 0 || ready.IssuerCalls != 0 {
						t.Fatal("SDK readiness setup did not retain exactly its prepared connection")
					}
					// Keep setup traffic separate from the timed call and its replay.
					h.action(map[string]any{"kind": "reset"})
					originInitialize, originList = 0, 0
				}
				h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": "tools/call"})
				if variant == "shorter-grpc-caller-deadline" {
					action["callerDeadlineMs"], action["settle"] = 500, false
				}
				pending := h.startAction(action)
				h.awaitMCPHeld(adapter, "tools/call")
				output := h.joinAction(pending)
				h.awaitMCPStoredResult(event)
				// Local completion and storage do not acknowledge the peer's HTTP abort.
				// Observe that event while its accepted response remains held.
				h.action(map[string]any{"kind": "wait-call-cancelled", "adapter": adapter})
				observed := h.assertOAuthCounts(adapter, 0, originInitialize, originList, 1, 1)
				var proof struct {
					Counts  struct{ CancelledCalls int }
					Records []map[string]any
				}
				if err := json.Unmarshal(observed, &proof); err != nil {
					t.Fatal(err)
				}
				if proof.Counts.CancelledCalls != 1 {
					t.Fatal("actual held HTTP request did not observe caller abort before release")
				}
				// The endpoint accepted the effect before holding its response. A timed
				// out response does not imply cancellation undid that external effect.
				h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "tools/call"})
				if variant == "held-response-execution-deadline" {
					h.assertMCPOriginalSettlement(event, output, 2, 5, "MCP tool call timed out.")
				}
				replayAction := map[string]any{"kind": "execute", "replica": 1, "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce}
				replay := h.action(replayAction)
				h.assertMCPOriginalSettlement(event, replay, 2, 5, "MCP tool call timed out.")
				h.assertOAuthCounts(adapter, 0, originInitialize, originList, 1, 1)
				remaining, elapsed, lastRemaining, lastElapsed := 0, 0, 1000.0, 0.0
				for _, record := range proof.Records {
					r, rok := record["timeout.remaining_ms"].(float64)
					e, eok := record["timeout.elapsed_ms"].(float64)
					if rok {
						remaining++
						if r < 0 || r > lastRemaining {
							t.Fatal("execution remaining budget reset across owner phases")
						}
						lastRemaining = r
					}
					if eok {
						elapsed++
						if e < lastElapsed {
							t.Fatal("execution elapsed budget moved backward")
						}
						lastElapsed = e
					}
				}
				if remaining < 3 || elapsed < 3 {
					t.Fatal("actual shared execution phase budget observations are missing")
				}
				h.evidence("execution-deadline-and-commit-recovery", adapter+"/"+variant, replay, map[string]any{"original_caller_observation": json.RawMessage(output), "readiness_setup": readinessSetup, "origin_initialize": originInitialize, "origin_list": originList, "injected_execution_budget_ms": 1000, "grpc_caller_deadline_ms": func() int {
					if variant == "shorter-grpc-caller-deadline" {
						return 500
					}
					return 180000
				}(), "remaining_budget_observations": remaining, "elapsed_budget_observations": elapsed, "actual_http_abort_before_release": true, "effect_accepted_before_held_response": true, "stored_status": 2, "stored_error_kind": 5, "stored_results": 1, "original_public_error": 1, "original_settlement_receipt": 1, "replay_external_calls": 0})
			})
		}
	}
}
