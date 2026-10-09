package integration

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

// The response-only fault runs after the actual Bridge transaction. The
// caller's original write key, database receipt and content stay untouched.
func TestPostgreSQLContentACKIdentity(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	for _, eventType := range []string{"agent.message", "agent.thinking"} {
		for _, mode := range []string{"wrong-committed", "wrong-duplicate", "correct"} {
			t.Run(eventType+"/"+mode, func(t *testing.T) {
				c := startContentE2EWithOptions(t, "durable-interleaved", false, false, contentE2EOptions{ACKMode: mode, ACKEventType: eventType, Runtime: map[string]any{"controlCommands": true, "observeColdLoad": true}, Gateway: map[string]any{"holdFinish": false}})
				c.provider.finish()
				c.sdk.control(t, "send", map[string]any{"sessionId": c.session, "text": "start-content-fixture"})
				if mode == "correct" {
					waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, c.session, 1)
					assertContentSDKTexts(t, c, []string{"alpha", "beta", "done"})
					if c.provider.calls.Load() != 1 {
						t.Fatal("correct ACK did not dispatch exactly once")
					}
				} else {
					waitContentACKResidencyDiscard(t, c)
					var expectedTexts []string
					if eventType == "agent.message" {
						expectedTexts = []string{"alpha"}
					}
					assertContentSDKTextHistory(t, c, expectedTexts)
					waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_use'`, c.session, 0)
					if c.provider.calls.Load() != 0 {
						t.Fatal("wrong ACK allowed dependent dispatch")
					}
				}
				c.lost.mu.Lock()
				original, receipt, attempts, altered := c.lost.original, c.lost.receipt, c.lost.attempts, c.lost.altered
				c.lost.mu.Unlock()
				wantAttempts, wantAltered := 1, 1
				if mode == "wrong-duplicate" {
					wantAttempts = 2
				}
				if mode == "correct" {
					wantAltered = 0
				}
				if original == nil || receipt.GetCommitted() == nil || attempts != wantAttempts || altered != wantAltered || receipt.GetCommitted().GetEventId() != original.GetPreallocatedEventId() {
					t.Fatalf("actual/altered receipt observations attempts=%d altered=%d", attempts, altered)
				}
				var storedID, storedPayload string
				if err := c.db.QueryRow(`SELECT event_id,payload_json FROM session_events WHERE session_id=$1 AND event_id=$2`, c.session, original.GetPreallocatedEventId()).Scan(&storedID, &storedPayload); err != nil {
					t.Fatal(err)
				}
				if storedID != receipt.GetCommitted().GetEventId() || storedPayload != original.GetPayloadJson() {
					t.Fatal("wrong ACK changed the committed event")
				}
				waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id='evt_ffffffffffffffffffffffffffffffff'`, c.session, 0)
				if mode != "correct" {
					// A new public input reloads the absent hot owner. The next provider
					// script returns done; it cannot regenerate or declare the refused member.
					c.sdk.control(t, "send", map[string]any{"sessionId": c.session, "text": "resume-content-fixture"})
					waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, c.session, 1)
					cold := c.runtime.marker(t, "cold-context-loaded-2")
					var actualMessages any
					if err := json.Unmarshal(cold["messages"], &actualMessages); err != nil {
						t.Fatal(err)
					}
					resumeSequence := 3
					if eventType == "agent.thinking" {
						resumeSequence = 2
					}
					expectedRaw, _ := json.Marshal([]any{map[string]any{"messageSequence": 1, "contextKind": "user", "parts": []any{map[string]any{"type": "text", "text": "start-content-fixture"}}}, map[string]any{"messageSequence": resumeSequence, "contextKind": "user", "parts": []any{map[string]any{"type": "text", "text": "resume-content-fixture"}}}})
					var expectedMessages any
					if err := json.Unmarshal(expectedRaw, &expectedMessages); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(actualMessages, expectedMessages) || string(cold["currentRequestMessage"]) != "null" {
						t.Fatalf("failed request leaked into eligible cold context: messages=%s current=%s", cold["messages"], cold["currentRequestMessage"])
					}
					// Failed content remains durable audit history; the owning
					// abnormal-End policy deliberately excludes it from provider history.
					if eventType == "agent.message" {
						var preserved int
						err := c.db.QueryRow(`SELECT count(*) FROM session_messages m WHERE session_id=$1 AND model_request_id=$2 AND sequence=$3 AND (`+sessionfixture.MessageContentSQL+`)::jsonb='{"parts":[{"type":"reasoning","text":"reason-before-text","providerMetadata":{"anthropic":{"signature":"fixture-signature-text"}}},{"type":"text","text":"alpha"}]}'::jsonb`, c.session, original.GetModelRequestId(), receipt.GetCommitted().GetAssignedMessageSequence()).Scan(&preserved)
						if err != nil || preserved != 1 {
							t.Fatalf("committed audit Assistant count=%d error=%v", preserved, err)
						}
					}
					var session string
					if json.Unmarshal(cold["sessionId"], &session) != nil || session != c.session {
						t.Fatal("fresh input did not load the original Session")
					}
					expected := []string{"done"}
					if eventType == "agent.message" {
						expected = []string{"alpha", "done"}
					}
					assertContentSDKTexts(t, c, expected)
					if c.provider.calls.Load() != 0 {
						t.Fatal("cold recovery dispatched an unacknowledged dependent Tool")
					}
					var count int
					if err := c.db.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id=$2 AND payload_json=$3`, c.session, storedID, storedPayload).Scan(&count); err != nil || count != 1 {
						t.Fatalf("cold committed event preservation count=%d error=%v", count, err)
					}
				}
				observed := c.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
				var requests int
				if json.Unmarshal(observed["providerCalls"], &requests) != nil || requests != 2 {
					t.Fatalf("ACK recovery provider requests=%d want2", requests)
				}
				t.Logf("actual ACK %s event=%s attempts=%d altered=%d", mode, eventType, attempts, altered)
			})
		}
	}
}

func waitContentACKResidencyDiscard(t *testing.T, c *contentE2E) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		c.lost.mu.Lock()
		altered := c.lost.altered
		c.lost.mu.Unlock()
		if altered > 0 {
			reply := c.runtimeControl(t, "inspect", c.session)
			var thread struct {
				Observed *bool `json:"observed"`
			}
			if err := json.Unmarshal(reply["thread"], &thread); err != nil || thread.Observed == nil {
				t.Fatal("invalid actual Runtime owner inspection")
			}
			if !*thread.Observed {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("wrong ACK did not discard residency (altered=%d)", altered)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
