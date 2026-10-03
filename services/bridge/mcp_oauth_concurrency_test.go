package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type mcpAsyncActionResult struct {
	output json.RawMessage
	err    error
}

func (h *mcpDurableComposition) startAction(action map[string]any) <-chan mcpAsyncActionResult {
	result := make(chan mcpAsyncActionResult, 1)
	go func() { output, err := h.tryAction(action); result <- mcpAsyncActionResult{output, err} }()
	return result
}
func (h *mcpDurableComposition) joinAction(result <-chan mcpAsyncActionResult) json.RawMessage {
	h.t.Helper()
	select {
	case joined := <-result:
		if joined.err != nil {
			h.t.Fatal(joined.err)
		}
		return joined.output
	case <-h.ctx.Done():
		h.t.Fatal(h.ctx.Err())
		return nil
	}
}
func (h *mcpDurableComposition) assertOAuthCounts(adapter string, f, i, l, c, e int) json.RawMessage {
	h.t.Helper()
	output := h.action(map[string]any{"kind": "observe", "adapter": adapter})
	h.assertNoMCPOAuthSecrets(adapter, string(output))
	var observed struct {
		IssuerCalls     int
		Counts          struct{ Effects int }
		Requests        []struct{ Origin, Method, CredentialLabel string }
		RemainingFaults map[string]int
	}
	if err := json.Unmarshal(output, &observed); err != nil {
		h.t.Fatal(err)
	}
	oi, ol, oc, vl := 0, 0, 0, 0
	for _, r := range observed.Requests {
		if r.CredentialLabel == "unrecognized" {
			h.t.Fatal("unrecognized scoped OAuth token")
		}
		if r.Origin == "origin-execution" {
			switch r.Method {
			case "initialize":
				oi++
			case "tools/list":
				ol++
			case "tools/call":
				oc++
			}
		} else if r.Origin == "bridge-verification" && r.Method == "tools/list" {
			vl++
		} else {
			h.t.Fatalf("unidentified request origin=%s", output)
		}
	}
	if observed.IssuerCalls != f || oi != i || ol != l || oc != c || observed.Counts.Effects != e {
		h.t.Fatalf("OAuth origin F/I/L/C/effects=%d/%d/%d/%d/%d want%d/%d/%d/%d/%d (verification L%d): %s", observed.IssuerCalls, oi, ol, oc, observed.Counts.Effects, f, i, l, c, e, vl, output)
	}
	for key, n := range observed.RemainingFaults {
		if n != 0 {
			h.t.Fatalf("unreached fault %q remaining%d", key, n)
		}
	}
	return output
}
func testMCPOAuthConcurrentOwners(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		for _, variant := range []string{"two-managers-row-lock", "two-initial-waiters", "exact-two-late-waiters", "cancel-one-survivor-and-independent", "all-waiters-gone"} {
			t.Run(adapter+"/"+variant, func(t *testing.T) {
				h := newMCPDurableComposition(t)
				before := h.seedMCPOAuth(adapter, variant == "two-managers-row-lock")
				events, calls, nonces := make([]string, 2), make([]string, 2), make([]string, 2)
				for n := range events {
					nonces[n] = fmt.Sprintf("oauth-%s-%s-%d", adapter, variant, n)
					events[n], calls[n] = h.declare("work-"+adapter, nonces[n], "allow")
				}
				action := func(n, replica int) map[string]any {
					return map[string]any{"kind": "execute", "adapter": adapter, "eventId": events[n], "callId": calls[n], "nonce": nonces[n], "replica": replica}
				}
				if variant == "two-managers-row-lock" {
					h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": "issuer"})
					first := h.startAction(action(0, 0))
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "issuer"})
					second := h.startAction(action(1, 1))
					// Independently observe the losing installed-role transaction blocked
					// on the production credential FOR UPDATE; no timing guess proves this.
					deadline := time.NewTimer(5 * time.Second)
					defer deadline.Stop()
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					waiting := false
					for !waiting {
						var count int
						if err := h.admin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=$1 AND wait_event_type='Lock' AND query ILIKE '%FOR UPDATE%' AND query ILIKE '%credentials%'`, h.gatewayUser).Scan(&count); err != nil {
							t.Fatal(err)
						}
						waiting = count == 1
						if !waiting {
							select {
							case <-ticker.C:
							case <-deadline.C:
								t.Fatal("second installed Gateway resolver did not wait on the credential row lock")
							}
						}
					}
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "issuer"})
					for n, result := range []<-chan mcpAsyncActionResult{first, second} {
						output := h.joinAction(result)
						h.assertMCPOriginalSettlement(events[n], output, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonces[n]))
					}
					output := h.assertOAuthCounts(adapter, 1, 2, 2, 2, 2)
					h.assertMCPOAuthRotation(adapter, before, 1)
					var proof struct {
						Records  []struct{ Event, Outcome, DurableWrite string }
						Requests []struct{ CredentialLabel string }
					}
					if err := json.Unmarshal(output, &proof); err != nil {
						t.Fatal(err)
					}
					winner, reused := 0, 0
					for _, r := range proof.Records {
						if r.Event == "oauth_refresh_completed" {
							if r.Outcome == "refreshed" && r.DurableWrite == "committed" {
								winner++
							}
							if r.Outcome == "concurrent_winner_reused" && r.DurableWrite == "not_needed" {
								reused++
							}
						}
					}
					if winner != 1 || reused != 1 {
						t.Fatalf("row-lock outcomes winner%d reused%d", winner, reused)
					}
					for _, r := range proof.Requests {
						if r.CredentialLabel != adapter+"-rotation-1" {
							t.Fatal("manager used pre-rotation bearer")
						}
					}
					h.evidence("oauth-refresh", adapter+"/"+variant, output, map[string]any{"installed_role_row_lock_waiter": 1, "independent_managers": 2, "committed_rotations": 1, "concurrent_winner_reused": 1, "original_receipts": 2, "origin_F": 1, "origin_I": 2, "origin_L": 2, "origin_C": 2, "accepted_effects": 2})
					return
				}
				h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": "tools/list", "faultOrigin": "origin-execution", "faults": []int{401, 0}})
				h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": "tools/call", "faultOrigin": "origin-execution", "faults": func() []int {
					if variant == "all-waiters-gone" {
						return []int{}
					}
					if variant == "cancel-one-survivor-and-independent" {
						return []int{401}
					}
					return []int{401, 401}
				}()})
				h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": "tools/list"})
				if variant != "exact-two-late-waiters" {
					h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": "initialize"})
				}
				first := h.startAction(action(0, 0))
				if variant == "exact-two-late-waiters" {
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "tools/list"})
				} else {
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "initialize"})
				}
				second := h.startAction(action(1, 0))
				h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 2})
				if variant != "exact-two-late-waiters" {
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "initialize"})
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "tools/list"})
				}
				if variant == "all-waiters-gone" {
					for _, event := range events {
						h.action(map[string]any{"kind": "cancel", "eventId": event})
					}
					h.action(map[string]any{"kind": "wait-resources-closed", "adapter": adapter})
					for n, result := range []<-chan mcpAsyncActionResult{first, second} {
						output := h.joinAction(result)
						var proof struct{ Result struct{ Type string } }
						if err := json.Unmarshal(output, &proof); err != nil {
							t.Fatal(err)
						}
						if proof.Result.Type != "cancelled" {
							t.Fatalf("cancelled owner returned %s", output)
						}
						var receipts int
						if err := h.admin.QueryRow(`SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND operation='settle_tool_result' AND idempotency_key=$1 AND ack_status='committed'`, events[n]).Scan(&receipts); err != nil {
							t.Fatal(err)
						}
						if receipts != 1 {
							t.Fatal("cancelled original Tool Use did not settle once")
						}
					}
					output := h.assertOAuthCounts(adapter, 1, 2, 2, 0, 0)
					h.assertMCPOAuthRotation(adapter, before, 1)
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "tools/list"})
					h.evidence("oauth-refresh", adapter+"/"+variant, output, map[string]any{"cancelled_owners": 2, "original_settlement_receipts": 2, "owned_openings": 0, "owned_connections": 0, "notification_streams": 0, "origin_F": 1, "origin_I": 2, "origin_L": 2, "origin_C": 0, "accepted_effects": 0})
					return
				}
				if variant == "cancel-one-survivor-and-independent" {
					h.action(map[string]any{"kind": "cancel", "eventId": events[1]})
					h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 1})
				}
				h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "tools/list"})
				outputs := []json.RawMessage{h.joinAction(first), h.joinAction(second)}
				h.assertMCPOriginalSettlement(events[0], outputs[0], 3, 4, "MCP authentication failed after refresh.")
				if variant != "cancel-one-survivor-and-independent" {
					h.assertMCPOriginalSettlement(events[1], outputs[1], 3, 4, "MCP authentication failed after refresh.")
					output := h.assertOAuthCounts(adapter, 1, 2, 2, 2, 0)
					h.assertMCPOAuthRotation(adapter, before, 1)
					h.evidence("oauth-refresh", adapter+"/"+variant, output, map[string]any{"original_authentication_error_receipts": 2, "joined_waiters": 2, "origin_F": 1, "origin_I": 2, "origin_L": 2, "origin_C": 2, "accepted_effects": 0})
					return
				}
				// The cancelled owner dispatches nothing. The survivor cannot reclaim
				// the allowance spent by their shared readiness refresh.
				var cancelled struct{ Result struct{ Type string } }
				if err := json.Unmarshal(outputs[1], &cancelled); err != nil {
					t.Fatal(err)
				}
				if cancelled.Result.Type != "cancelled" {
					t.Fatalf("cancelled Runtime owner=%s", outputs[1])
				}
				baseline := h.assertOAuthCounts(adapter, 1, 2, 2, 1, 0)
				h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": "tools/call", "faultOrigin": "origin-execution", "faults": []int{401, 0}})
				nonce := "later-independent-" + adapter
				event, call := h.declare("work-"+adapter, nonce, "allow")
				output := h.action(map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce})
				h.assertMCPOriginalSettlement(event, output, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce))
				output = h.assertOAuthCounts(adapter, 2, 3, 3, 3, 1)
				h.assertMCPOAuthRotation(adapter, before, 2)
				h.evidence("oauth-refresh", adapter+"/"+variant, output, map[string]any{"cancelled_owner_dispatched": false, "survivor_authentication_receipt": 1, "later_independent_success_receipt": 1, "before_independent": json.RawMessage(baseline), "origin_F": 2, "origin_I": 3, "origin_L": 3, "origin_C": 3, "accepted_effects": 1})
			})
		}
	}
}
func testMCPOAuthIssuerRollback(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		for _, failure := range []string{"ok-false", "http-503", "malformed", "write-back"} {
			t.Run(adapter+"/"+failure, func(t *testing.T) {
				h := newMCPDurableComposition(t)
				before := h.seedMCPOAuth(adapter, true)
				if failure == "write-back" {
					if _, err := h.admin.Exec(`CREATE FUNCTION reject_fixture_rotation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled credential write failure'; END $$; CREATE TRIGGER reject_fixture_rotation BEFORE UPDATE ON credentials FOR EACH ROW EXECUTE FUNCTION reject_fixture_rotation()`); err != nil {
						t.Fatal(err)
					}
				} else {
					h.action(map[string]any{"kind": "issuer", "adapter": adapter, "issuerFailures": []string{failure}})
				}
				nonce := "issuer-rollback-" + adapter + "-" + failure
				event, call := h.declare("work-"+adapter, nonce, "allow")
				output := h.action(map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce})
				h.assertMCPOriginalSettlement(event, output, 3, 3, "MCP credential refresh is temporarily unavailable.")
				var after []byte
				if err := h.admin.QueryRow(`SELECT encrypted_auth FROM credentials WHERE workspace_id='default' AND id=$1`, "cred_mcp_"+adapter).Scan(&after); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("failed refresh overwrote encrypted credential")
				}
				observed := h.assertOAuthCounts(adapter, 1, 0, 0, 0, 0)
				var proof struct {
					Records     []struct{ Event, Outcome, FailureKind, DurableWrite string }
					IssuerCalls int
				}
				if err := json.Unmarshal(observed, &proof); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, r := range proof.Records {
					if r.Event == "oauth_refresh_completed" && r.Outcome == "failed" {
						found = true
						if failure == "write-back" && (r.FailureKind != "write_back" || r.DurableWrite != "failed") {
							t.Fatalf("issuer-side rotation not distinguished from failed write: %+v", r)
						}
					}
				}
				if !found {
					t.Fatal("refresh owner exit missing")
				}
				h.assertNoMCPOAuthSecrets(adapter, string(observed))
				replay := h.action(map[string]any{"kind": "execute", "replica": 1, "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce})
				h.assertMCPOriginalSettlement(event, replay, 3, 3, "MCP credential refresh is temporarily unavailable.")
				h.assertOAuthCounts(adapter, 1, 0, 0, 0, 0)
				h.evidence("oauth-refresh", adapter+"/"+failure, observed, map[string]any{"encrypted_row_preserved": true, "committed_rotations": 0, "issuer_side_rotation_not_rolled_back": failure == "write-back", "original_error_receipt": 1, "replay_external_calls": 0})
			})
		}
	}
}

func testMCPOAuthWaiterDeadlinesAndRetry(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		for _, variant := range []string{"short-initialize-owner-long-survivor", "short-list-owner-long-survivor", "already-retrying-and-eligible"} {
			t.Run(adapter+"/"+variant, func(t *testing.T) {
				h := newMCPDurableComposition(t)
				before := h.seedMCPOAuth(adapter, false)
				events, calls, nonces := make([]string, 2), make([]string, 2), make([]string, 2)
				for n := range events {
					nonces[n] = fmt.Sprintf("oauth-%s-%s-%d", adapter, variant, n)
					events[n], calls[n] = h.declare("work-"+adapter, nonces[n], "allow")
				}
				action := func(n int) map[string]any {
					return map[string]any{"kind": "execute", "adapter": adapter, "eventId": events[n], "callId": calls[n], "nonce": nonces[n]}
				}
				if variant == "already-retrying-and-eligible" {
					h.action(map[string]any{"kind": "list", "adapter": adapter})
					h.action(map[string]any{"kind": "reset"})
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": "tools/call", "faultOrigin": "origin-execution", "faults": []int{401, 0}})
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": "tools/list", "faultOrigin": "origin-execution", "faults": []int{401, 0}})
					h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": "initialize"})
					first := h.startAction(action(0))
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "initialize"})
					second := h.startAction(action(1))
					h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 2})
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "initialize"})
					firstOutput, secondOutput := h.joinAction(first), h.joinAction(second)
					h.assertMCPOriginalSettlement(events[0], firstOutput, 3, 4, "MCP authentication failed after refresh.")
					h.assertMCPOriginalSettlement(events[1], secondOutput, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonces[1]))
					output := h.assertOAuthCounts(adapter, 2, 2, 2, 2, 1)
					h.assertMCPOAuthRotation(adapter, before, 2)
					h.evidence("oauth-refresh", adapter+"/"+variant, output, map[string]any{"already_retrying_authentication_receipt": 1, "eligible_independent_success_receipt": 1, "joined_waiters": 2, "origin_F": 2, "origin_I": 2, "origin_L": 2, "origin_C": 2, "accepted_effects": 1})
					return
				}
				phase := "initialize"
				if variant == "short-list-owner-long-survivor" {
					phase = "tools/list"
				}
				h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": phase})
				short, long := action(0), action(1)
				short["callerDeadlineMs"], short["settle"] = 1500, false
				long["callerDeadlineMs"] = 5000
				first := h.startAction(short)
				h.awaitMCPHeld(adapter, phase)
				second := h.startAction(long)
				h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 2})
				shortOutput := h.joinAction(first)
				h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 1})
				select {
				case premature := <-second:
					t.Fatalf("longer deadline waiter completed before held phase release: %s/%v", premature.output, premature.err)
				default:
				}
				h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": phase})
				longOutput := h.joinAction(second)
				h.assertMCPOriginalSettlement(events[1], longOutput, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonces[1]))
				// The transport deadline loses its original ACK. The subsequent receipt
				// replay must expose the stored timeout, without a second dispatch.
				h.awaitMCPStoredResult(events[0])
				replay := action(0)
				replay["replica"] = 1
				replayed := h.action(replay)
				h.assertMCPOriginalSettlement(events[0], replayed, 2, 5, "MCP tool call timed out.")
				output := h.assertOAuthCounts(adapter, 0, 1, 1, 1, 1)
				h.evidence("oauth-refresh", adapter+"/"+variant, output, map[string]any{"short_caller_deadline_ms": 1500, "long_caller_deadline_ms": 5000, "short_caller_observation": json.RawMessage(shortOutput), "short_stored_timeout_receipt": 1, "long_success_receipt": 1, "pending_survivor_before_release": true, "origin_F": 0, "origin_I": 1, "origin_L": 1, "origin_C": 1, "accepted_effects": 1})
			})
		}
	}
}

func (h *mcpDurableComposition) awaitMCPStoredResult(event string) {
	h.t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		var stored bool
		if err := h.admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM session_runtime_tool_results WHERE tool_use_event_id=$1 AND result_json IS NOT NULL AND mcp_claim_status='stored')`, event).Scan(&stored); err != nil {
			h.t.Fatal(err)
		}
		if stored {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			h.t.Fatal("original Connector did not store its result before receipt replay")
		}
	}
}

func (h *mcpDurableComposition) awaitMCPHeld(adapter, phase string) {
	h.t.Helper()
	if _, err := h.tryAction(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": phase}); err != nil {
		observed, _ := h.tryAction(map[string]any{"kind": "observe", "adapter": adapter})
		h.t.Fatalf("actual phase %s not reached: %v; safe observations=%s", phase, err, observed)
	}
}
