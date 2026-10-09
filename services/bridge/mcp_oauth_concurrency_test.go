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
	kind   any
}

func (h *mcpDurableComposition) startAction(action map[string]any) <-chan mcpAsyncActionResult {
	result := make(chan mcpAsyncActionResult, 1)
	kind := action["kind"]
	go func() { output, err := h.tryAction(action); result <- mcpAsyncActionResult{output, err, kind} }()
	return result
}
func (h *mcpDurableComposition) joinAction(result <-chan mcpAsyncActionResult) json.RawMessage {
	h.t.Helper()
	select {
	case joined := <-result:
		if joined.err != nil {
			h.t.Fatal(joined.err)
		}
		// Barriers are recorded on the test goroutine, after the join.
		h.reachedAction(map[string]any{"kind": joined.kind})
		return joined.output
	case <-h.ctx.Done():
		h.t.Fatal(h.ctx.Err())
		return nil
	}
}

// mcpOAuthCounts holds the values assertOAuthCounts compared: issuer calls (F),
// origin-execution initialize, tools/list and tools/call requests (I, L, C),
// bridge-verification tools/list requests and accepted effects.
type mcpOAuthCounts struct {
	F, I, L, C, VerificationL, Effects int
}

// facts adds the compared counts to an evidence record under the keys the
// OAuth refresh table records use.
func (o mcpOAuthCounts) facts(record map[string]any) map[string]any {
	record["origin_F"], record["origin_I"], record["origin_L"], record["origin_C"] = o.F, o.I, o.L, o.C
	record["verification_L"], record["accepted_effects"] = o.VerificationL, o.Effects
	return record
}

func (h *mcpDurableComposition) assertOAuthCounts(adapter string, f, i, l, c, e int) (json.RawMessage, mcpOAuthCounts) {
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
		h.t.Fatalf("OAuth origin F/I/L/C/effects=%d/%d/%d/%d/%d want %d/%d/%d/%d/%d (verification L %d): %s", observed.IssuerCalls, oi, ol, oc, observed.Counts.Effects, f, i, l, c, e, vl, output)
	}
	for key, n := range observed.RemainingFaults {
		if n != 0 {
			h.t.Fatalf("unreached fault %q remaining %d", key, n)
		}
	}
	return output, mcpOAuthCounts{F: observed.IssuerCalls, I: oi, L: ol, C: oc, VerificationL: vl, Effects: observed.Counts.Effects}
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
					waiting, lockWaiters := false, 0
					for !waiting {
						var count int
						if err := h.admin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=$1 AND wait_event_type='Lock' AND query ILIKE '%FOR UPDATE%' AND query ILIKE '%credentials%'`, h.gatewayUser).Scan(&count); err != nil {
							t.Fatal(err)
						}
						waiting, lockWaiters = count == 1, count
						if !waiting {
							select {
							case <-ticker.C:
							case <-deadline.C:
								t.Fatal("second installed Gateway resolver did not wait on the credential row lock")
							}
						}
					}
					h.reached("installed-role-row-lock-waiter-before-issuer-release")
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "issuer"})
					settlements := make([]mcpSettlementObservation, 0, 2)
					for n, result := range []<-chan mcpAsyncActionResult{first, second} {
						output := h.joinAction(result)
						settlements = append(settlements, h.assertMCPOriginalSettlement(events[n], output, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonces[n])))
					}
					output, counts := h.assertOAuthCounts(adapter, 1, 2, 2, 2, 2)
					lifetime := h.assertMCPOAuthRotation(adapter, before, 1)
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
						t.Fatalf("row-lock outcomes winner %d reused %d", winner, reused)
					}
					for _, r := range proof.Requests {
						if r.CredentialLabel != adapter+"-rotation-1" {
							t.Fatal("manager used pre-rotation bearer")
						}
					}
					h.evidence("oauth-refresh", adapter+"/"+variant, output, counts.facts(map[string]any{"installed_role_row_lock_waiters": lockWaiters, "committed_rotations": winner, "concurrent_winner_reused": reused, "original_settlements": settlements, "rotated_credential_lifetime_s": int(lifetime.Seconds())}))
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
				joinedWaiters := h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 2})
				if variant != "exact-two-late-waiters" {
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "initialize"})
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "tools/list"})
				}
				if variant == "all-waiters-gone" {
					for _, event := range events {
						h.action(map[string]any{"kind": "cancel", "eventId": event})
					}
					closed := h.action(map[string]any{"kind": "wait-resources-closed", "adapter": adapter})
					cancelledOwners, settledReceipts := 0, 0
					for n, result := range []<-chan mcpAsyncActionResult{first, second} {
						output := h.joinAction(result)
						var proof struct{ Result struct{ Type string } }
						if err := json.Unmarshal(output, &proof); err != nil {
							t.Fatal(err)
						}
						if proof.Result.Type != "cancelled" {
							t.Fatalf("cancelled owner returned %s", output)
						}
						cancelledOwners++
						var receipts int
						if err := h.admin.QueryRow(`SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND operation='settle_tool_result' AND idempotency_key=$1 AND ack_status='committed'`, events[n]).Scan(&receipts); err != nil {
							t.Fatal(err)
						}
						if receipts != 1 {
							t.Fatal("cancelled original Tool Use did not settle once")
						}
						settledReceipts += receipts
					}
					output, counts := h.assertOAuthCounts(adapter, 1, 2, 2, 0, 0)
					lifetime := h.assertMCPOAuthRotation(adapter, before, 1)
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "tools/list"})
					h.evidence("oauth-refresh", adapter+"/"+variant, output, counts.facts(map[string]any{"cancelled_owners": cancelledOwners, "original_settlement_receipts": settledReceipts, "resources_after_cancellation": closed, "joined_waiters_observation": joinedWaiters, "rotated_credential_lifetime_s": int(lifetime.Seconds())}))
					return
				}
				if variant == "cancel-one-survivor-and-independent" {
					h.action(map[string]any{"kind": "cancel", "eventId": events[1]})
					h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 1})
				}
				h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "tools/list"})
				outputs := []json.RawMessage{h.joinAction(first), h.joinAction(second)}
				survivor := h.assertMCPOriginalSettlement(events[0], outputs[0], 3, 4, "MCP authentication failed after refresh.")
				if variant != "cancel-one-survivor-and-independent" {
					settlements := []mcpSettlementObservation{survivor, h.assertMCPOriginalSettlement(events[1], outputs[1], 3, 4, "MCP authentication failed after refresh.")}
					output, counts := h.assertOAuthCounts(adapter, 1, 2, 2, 2, 0)
					lifetime := h.assertMCPOAuthRotation(adapter, before, 1)
					h.evidence("oauth-refresh", adapter+"/"+variant, output, counts.facts(map[string]any{"original_settlements": settlements, "joined_waiters_observation": joinedWaiters, "rotated_credential_lifetime_s": int(lifetime.Seconds())}))
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
				baseline, _ := h.assertOAuthCounts(adapter, 1, 2, 2, 1, 0)
				h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": "tools/call", "faultOrigin": "origin-execution", "faults": []int{401, 0}})
				nonce := "later-independent-" + adapter
				event, call := h.declare("work-"+adapter, nonce, "allow")
				output := h.action(map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce})
				independent := h.assertMCPOriginalSettlement(event, output, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce))
				output, counts := h.assertOAuthCounts(adapter, 2, 3, 3, 3, 1)
				lifetime := h.assertMCPOAuthRotation(adapter, before, 2)
				h.evidence("oauth-refresh", adapter+"/"+variant, output, counts.facts(map[string]any{"cancelled_owner_result": cancelled.Result.Type, "survivor_settlement": survivor, "later_independent_settlement": independent, "before_independent": json.RawMessage(baseline), "joined_waiters_observation": joinedWaiters, "rotated_credential_lifetime_s": int(lifetime.Seconds())}))
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
				original := h.assertMCPOriginalSettlement(event, output, 3, 3, "MCP credential refresh is temporarily unavailable.")
				var after []byte
				if err := h.admin.QueryRow(`SELECT encrypted_auth FROM credentials WHERE workspace_id='default' AND id=$1`, "cred_mcp_"+adapter).Scan(&after); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("failed refresh overwrote encrypted credential")
				}
				observed, _ := h.assertOAuthCounts(adapter, 1, 0, 0, 0, 0)
				var proof struct {
					Records     []struct{ Event, Outcome, FailureKind, DurableWrite string }
					IssuerCalls int
				}
				if err := json.Unmarshal(observed, &proof); err != nil {
					t.Fatal(err)
				}
				found := false
				var failedRefresh map[string]string
				for _, r := range proof.Records {
					if r.Event == "oauth_refresh_completed" && r.Outcome == "failed" {
						found = true
						if failure == "write-back" && (r.FailureKind != "write_back" || r.DurableWrite != "failed") {
							t.Fatalf("issuer-side rotation not distinguished from failed write: %+v", r)
						}
						failedRefresh = map[string]string{"failure_kind": r.FailureKind, "durable_write": r.DurableWrite}
					}
				}
				if !found {
					t.Fatal("refresh owner exit missing")
				}
				h.assertNoMCPOAuthSecrets(adapter, string(observed))
				replay := h.action(map[string]any{"kind": "execute", "replica": 1, "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce})
				replayed := h.assertMCPOriginalSettlement(event, replay, 3, 3, "MCP credential refresh is temporarily unavailable.")
				afterReplay, _ := h.assertOAuthCounts(adapter, 1, 0, 0, 0, 0)
				var replayCounts struct {
					IssuerCalls int
					Counts      struct{ Call, Effects int }
				}
				if err := json.Unmarshal(afterReplay, &replayCounts); err != nil {
					t.Fatal(err)
				}
				h.evidence("oauth-refresh", adapter+"/"+failure, observed, map[string]any{"encrypted_row_preserved": bytes.Equal(before, after), "failed_refresh_record": failedRefresh, "original_settlement": original, "replay_settlement": replayed, "replay_issuer_calls": replayCounts.IssuerCalls, "replay_calls": replayCounts.Counts.Call, "replay_effects": replayCounts.Counts.Effects})
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
					joinedWaiters := h.action(map[string]any{"kind": "wait-waiters", "expectedWaiters": 2})
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "initialize"})
					firstOutput, secondOutput := h.joinAction(first), h.joinAction(second)
					retrying := h.assertMCPOriginalSettlement(events[0], firstOutput, 3, 4, "MCP authentication failed after refresh.")
					eligible := h.assertMCPOriginalSettlement(events[1], secondOutput, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonces[1]))
					output, counts := h.assertOAuthCounts(adapter, 2, 2, 2, 2, 1)
					lifetime := h.assertMCPOAuthRotation(adapter, before, 2)
					h.evidence("oauth-refresh", adapter+"/"+variant, output, counts.facts(map[string]any{"already_retrying_settlement": retrying, "eligible_independent_settlement": eligible, "joined_waiters_observation": joinedWaiters, "rotated_credential_lifetime_s": int(lifetime.Seconds())}))
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
				survivorPending := false
				select {
				case premature := <-second:
					t.Fatalf("longer deadline waiter completed before held phase release: %s/%v", premature.output, premature.err)
				default:
					survivorPending = true
				}
				h.reached("long-readiness-owner-pending-before-release")
				h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": phase})
				longOutput := h.joinAction(second)
				survivor := h.assertMCPOriginalSettlement(events[1], longOutput, 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonces[1]))
				// The transport deadline loses its original ACK. The subsequent receipt
				// replay must expose the stored timeout, without a second dispatch.
				h.awaitMCPStoredResult(events[0])
				replay := action(0)
				replay["replica"] = 1
				replayed := h.action(replay)
				shortReplay := h.assertMCPOriginalSettlement(events[0], replayed, 2, 5, "MCP tool call timed out.")
				output, counts := h.assertOAuthCounts(adapter, 0, 1, 1, 1, 1)
				h.evidence("oauth-refresh", adapter+"/"+variant, output, counts.facts(map[string]any{"configuration": map[string]any{"short_caller_deadline_ms": short["callerDeadlineMs"], "long_caller_deadline_ms": long["callerDeadlineMs"]}, "short_caller_observation": json.RawMessage(shortOutput), "short_stored_timeout_settlement": shortReplay, "long_success_settlement": survivor, "survivor_pending_before_release": survivorPending}))
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
	action := map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": phase}
	if _, err := h.tryAction(action); err != nil {
		observed, _ := h.tryAction(map[string]any{"kind": "observe", "adapter": adapter})
		h.t.Fatalf("actual phase %s not reached: %v; safe observations=%s", phase, err, observed)
	}
	h.reachedAction(action)
}
