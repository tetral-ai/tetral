package jobrunner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestPostgreSQLRuntimePodLossDeliverySettlementMatrix(t *testing.T) {
	tests := []struct {
		name            string
		toolName        string
		childStatus     string
		withSent        bool
		withReceived    bool
		withTerminal    bool
		withRequestEnd  bool
		wantReceived    int
		wantWake        int
		wantResultError bool
		wantResultText  string
	}{
		{
			name:           "received spawn after request end becomes delivered",
			toolName:       "spawn_agent",
			childStatus:    "idle",
			withSent:       true,
			withReceived:   true,
			withRequestEnd: true,
			wantReceived:   1,
			wantWake:       1,
			wantResultText: "status: delivered",
		},
		{
			name:           "receivable spawn redrives same delivery",
			toolName:       "spawn_agent",
			childStatus:    "idle",
			withSent:       true,
			wantWake:       1,
			wantResultText: "status: delivered",
		},
		{
			name:           "receivable send redrives same delivery",
			toolName:       "send_message",
			childStatus:    "running",
			withSent:       true,
			wantWake:       1,
			wantResultText: "status: delivered",
		},
		{
			name:            "closed child becomes parent terminal error",
			toolName:        "send_message",
			childStatus:     "closed_for_runtime",
			withSent:        true,
			wantWake:        1,
			wantResultError: true,
			wantResultText:  "not receivable",
		},
		{
			name:            "missing anchor becomes parent terminal error",
			toolName:        "spawn_agent",
			childStatus:     "idle",
			wantResultError: true,
			wantResultText:  "delivery anchor",
		},
		{
			name:           "existing terminal result is skipped",
			toolName:       "spawn_agent",
			childStatus:    "idle",
			withSent:       true,
			withTerminal:   true,
			wantWake:       1,
			wantResultText: "existing terminal",
		},
	}

	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			fixture := seedRuntimePodLostDeliveryFixture(t, admin, index, tc.toolName, tc.childStatus, tc.withSent, tc.withReceived, tc.withTerminal, tc.withRequestEnd, false)

			for attempt := 0; attempt < 2; attempt++ {
				if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, fixture.sessionID, fixture.binding, fixture.now); err != nil {
					t.Fatalf("repair attempt %d: %v", attempt+1, err)
				}
			}

			var sentCount int
			var receivedCount int
			var childCount int
			var wakeCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1
				    AND type = 'agent.thread_message_sent'
				    AND payload_json::jsonb ->> 'delivery_id' = $2`, fixture.sessionID, fixture.deliveryID).Scan(&sentCount); err != nil {
				t.Fatalf("count sent events: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1
				    AND type = 'agent.thread_message_received'
				    AND payload_json::jsonb ->> 'delivery_id' = $2`, fixture.sessionID, fixture.deliveryID).Scan(&receivedCount); err != nil {
				t.Fatalf("count received events: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_threads
				  WHERE workspace_id = 'default' AND session_id = $1 AND parent_thread_id = $2 AND role = 'subagent'`,
				fixture.sessionID, fixture.parentThreadID).Scan(&childCount); err != nil {
				t.Fatalf("count child threads: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*)
				   FROM queue_jobs
				  WHERE workspace_id = 'default'
				    AND kind = 'runtime_input'
				    AND payload_json::jsonb ->> 'runtime_input_id' = $1`,
				runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID),
			).Scan(&wakeCount); err != nil {
				t.Fatalf("count durable agent-mail wakes: %v", err)
			}
			if sentCount != boolCount(tc.withSent) || receivedCount != tc.wantReceived || childCount != 1 {
				t.Fatalf("delivery facts sent=%d received=%d children=%d; want %d/%d/1", sentCount, receivedCount, childCount, boolCount(tc.withSent), tc.wantReceived)
			}
			if wakeCount != tc.wantWake {
				t.Fatalf("durable agent-mail wakes = %d; want %d", wakeCount, tc.wantWake)
			}

			var resultCount int
			var resultPayload string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*), COALESCE(max(payload_json), '')
				   FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1
				    AND type = 'agent.tool_result'
				    AND payload_json::jsonb ->> 'tool_use_event_id' = $2`, fixture.sessionID, fixture.toolUseEventID).Scan(&resultCount, &resultPayload); err != nil {
				t.Fatalf("read parent terminal result: %v", err)
			}
			if resultCount != 1 || !strings.Contains(resultPayload, tc.wantResultText) {
				t.Fatalf("parent results=%d payload=%s; want exactly one containing %q", resultCount, resultPayload, tc.wantResultText)
			}
			got, ok := testJSONPathValue(t, resultPayload, "is_error").(bool)
			if !ok || got != tc.wantResultError {
				t.Fatalf("parent result is_error=%v payload=%s; want %v", got, resultPayload, tc.wantResultError)
			}
			if !tc.wantResultError && strings.Contains(resultPayload, `"reason":"runtime_pod_lost"`) {
				t.Fatalf("delivered sub-agent tool was generically closed: %s", resultPayload)
			}
			if tc.withSent && !tc.withReceived && !tc.withTerminal && tc.childStatus != "closed_for_runtime" {
				var messageCount int
				if err := admin.QueryRowContext(context.Background(),
					`SELECT count(*) FROM session_messages
					  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND kind = 'user'`,
					fixture.sessionID, fixture.childThreadID).Scan(&messageCount); err != nil {
					t.Fatalf("count re-driven child messages: %v", err)
				}
				if messageCount != 0 {
					t.Fatalf("pre-delivery child messages = %d; want none before the child commits input", messageCount)
				}
			}
		})
	}
}

func TestPostgreSQLRuntimePodLossAllowsErroredRequestWithDeliveredSpawn(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 20, "spawn_agent", "idle", true, true, false, false, true)

	if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, fixture.sessionID, fixture.binding, fixture.now); err != nil {
		t.Fatalf("repair mixed outcome: %v", err)
	}
	var requestEndCount int
	var deliveredResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'span.model_request_end' AND model_request_id = $2
		    AND payload_json::jsonb ->> 'error_kind' = 'runtime_pod_lost'`, fixture.sessionID, fixture.modelRequestID).Scan(&requestEndCount); err != nil {
		t.Fatalf("count repaired request end: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = $2
		    AND COALESCE((payload_json::jsonb ->> 'is_error')::boolean, false) = false`, fixture.sessionID, fixture.toolUseEventID).Scan(&deliveredResultCount); err != nil {
		t.Fatalf("count delivered spawn result: %v", err)
	}
	if requestEndCount != 1 || deliveredResultCount != 1 {
		t.Fatalf("mixed outcome request_ends=%d delivered_spawns=%d; want 1/1", requestEndCount, deliveredResultCount)
	}
}

func TestPostgreSQLRuntimePodLossDeliveryStaleBindingFencePreventsSettlement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 22, "send_message", "idle", true, true, false, true, false)
	stale := fixture.binding
	stale.BindingGeneration--
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	err := store.repairLostRuntimeBinding(context.Background(), "default", fixture.sessionID, stale, fixture.now)
	assertRuntimePodLostRetryableError(t, err, "runtime_pod_lost_claim_stale")
	var resultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = $2`, fixture.sessionID, fixture.toolUseEventID).Scan(&resultCount); err != nil {
		t.Fatalf("count stale-fenced result: %v", err)
	}
	if resultCount != 0 {
		t.Fatalf("stale-fenced results = %d; want 0", resultCount)
	}
}

type runtimePodLostDeliveryFixture struct {
	sessionID      string
	parentThreadID string
	childThreadID  string
	modelRequestID string
	toolUseEventID string
	deliveryID     string
	binding        runtimecontrol.Binding
	now            time.Time
}

func seedRuntimePodLostDeliveryFixture(
	t *testing.T,
	db *sql.DB,
	index int,
	toolName string,
	childStatus string,
	withSent bool,
	withReceived bool,
	withTerminal bool,
	withRequestEnd bool,
	withOpenRequest bool,
) runtimePodLostDeliveryFixture {
	t.Helper()
	fixture := runtimePodLostDeliveryFixture{
		sessionID:      fmt.Sprintf("sesn_pod_loss_delivery_%d", index),
		parentThreadID: fmt.Sprintf("thr_pod_loss_delivery_parent_%d", index),
		childThreadID:  fmt.Sprintf("thr_pod_loss_delivery_child_%d", index),
		modelRequestID: fmt.Sprintf("mrq_pod_loss_delivery_%d", index),
		toolUseEventID: fmt.Sprintf("evt_pod_loss_delivery_tool_%d", index),
		deliveryID:     fmt.Sprintf("delivery_pod_loss_%d", index),
		now:            time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	}
	bindingID := fmt.Sprintf("bind_pod_loss_delivery_%d", index)
	fixture.binding = runtimePodLostBinding(fixture.sessionID, bindingID, int64(index+2))
	seedBridgeAPISession(t, db, "default", fixture.sessionID, fixture.parentThreadID)
	seedBridgeAPIRuntimeBinding(t, db, "default", fixture.sessionID, bindingID, fixture.binding.BindingGeneration, fixture.binding.PodUID)
	seedRuntimePodLostStatusFence(t, db, fixture.sessionID, bindingID, fixture.binding.BindingGeneration)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			agent_type, title, task_name, is_trunk, created_at, last_active_at, updated_at
		) VALUES ('default', $1, $2, $3, 'subagent', 'public', $4,
			'worker', 'worker', 'worker', false, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		fixture.childThreadID, fixture.sessionID, fixture.parentThreadID, childStatus); err != nil {
		t.Fatalf("seed delivery child: %v", err)
	}

	sequence := int64(1)
	if withOpenRequest || withRequestEnd {
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_start_"+fixture.toolUseEventID, sequence, "span.model_request_start",
			fmt.Sprintf(`{"type":"span.model_request_start","model_request_id":%q,"request_kind":"agent_provider_request"}`, fixture.modelRequestID),
			"internal", false)
		sequence++
	}
	toolPayload := fmt.Sprintf(`{"type":"agent.tool_use","name":%q,"input":{"task_name":"worker"},"evaluated_permission":"allow"}`, toolName)
	seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, fixture.toolUseEventID, sequence, "agent.tool_use", toolPayload, "public", true)
	seedBridgeAPIDurableToolMessage(
		t,
		db,
		"default",
		fixture.sessionID,
		fixture.parentThreadID,
		fixture.modelRequestID,
		fixture.toolUseEventID,
		"call_"+fixture.toolUseEventID,
		toolName,
	)
	seedBridgeAPIAllowedToolRoute(t, db, "default", fixture.sessionID, fixture.parentThreadID, fixture.toolUseEventID)
	sequence++
	if withSent {
		messageJSON := bridgePublicMessageJSONForTest(t, "hello worker")
		sentPayload := bridgeInterAgentSentEventJSON(t, fixture.deliveryID, fixture.parentThreadID, fixture.childThreadID, "worker", fixture.toolUseEventID, messageJSON)
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_sent_"+fixture.toolUseEventID, sequence, "agent.thread_message_sent", sentPayload, "public", true)
		seedAgentMailCustody(t, db, fixture.sessionID, fixture.childThreadID, fixture.deliveryID, fixture.now)
		sequence++
		if withReceived {
			receivedPayload := bridgeInterAgentMessageJSON(t, fixture.deliveryID, fixture.parentThreadID, fixture.toolUseEventID, messageJSON)
			receivedEventID := runtimecontrol.StableRuntimeID("agent_mail_received_event", "default", fixture.sessionID, fixture.childThreadID, fixture.deliveryID)
			seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.childThreadID, receivedEventID, 1, "agent.thread_message_received", receivedPayload, "public", true)
			if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox
				SET status='accepted', event_ids_json=jsonb_build_array($3::text)::text,
				    sequence_from=1, sequence_to=1, binding_id=$4, binding_generation=$5,
				    target_pod_uid=$6, updated_at=$7
				WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`,
				fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID), receivedEventID,
				fixture.binding.BindingID, fixture.binding.BindingGeneration, fixture.binding.PodUID, fixture.now); err != nil {
				t.Fatalf("accept received agent-mail custody: %v", err)
			}
		}
	}
	if withTerminal {
		payload := fmt.Sprintf(`{"type":"agent.tool_result","tool_use_event_id":%q,"content":[{"type":"text","text":"existing terminal"}],"is_error":false}`, fixture.toolUseEventID)
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_result_"+fixture.toolUseEventID, sequence, "agent.tool_result", payload, "public", true)
		if _, err := db.ExecContext(context.Background(), `UPDATE session_pending_tool_uses
			SET status='resolved',result_event_id=$5,resolved_at=$6,updated_at=$6
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
			"default", fixture.sessionID, fixture.parentThreadID, fixture.toolUseEventID,
			"evt_result_"+fixture.toolUseEventID, fixture.now); err != nil {
			t.Fatalf("resolve existing delivery Tool route: %v", err)
		}
		sequence++
	}
	if withRequestEnd {
		payload := fmt.Sprintf(`{"type":"span.model_request_end","model_request_id":%q,"model_request_start_id":%q,"request_kind":"agent_provider_request","is_error":false,"finish_reason":"stop","usage":{}}`, fixture.modelRequestID, "evt_start_"+fixture.toolUseEventID)
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_end_"+fixture.toolUseEventID, sequence, "span.model_request_end", payload, "internal", false)
	}
	return fixture
}

func TestPostgreSQLRuntimePodLossDeliveryRequiresExactInboxCustody(t *testing.T) {
	tests := []struct {
		name          string
		withReceived  bool
		mutate        func(*testing.T, *sql.DB, runtimePodLostDeliveryFixture)
		wantDelivered bool
	}{
		{name: "exact queued Inbox", wantDelivered: true},
		{
			name: "committed Inbox is durable delivery truth", withReceived: true, wantDelivered: true,
			mutate: runtimePodLostDeliveryInboxStatusMutation("committed"),
		},
		{
			name: "accepted Inbox wrong binding", withReceived: true,
			mutate: runtimePodLostDeliveryInboxBindingMutation("bind_wrong", 0, ""),
		},
		{
			name: "accepted Inbox wrong generation", withReceived: true,
			mutate: runtimePodLostDeliveryInboxBindingMutation("", 999, ""),
		},
		{
			name: "delivering Inbox wrong pod uid", withReceived: true,
			mutate: func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
				runtimePodLostDeliveryInboxStatusMutation("delivering")(t, db, fixture)
				runtimePodLostDeliveryInboxBindingMutation("", 0, "pod_wrong")(t, db, fixture)
			},
		},
		{
			name: "missing Inbox",
			mutate: func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `DELETE FROM session_runtime_inbox
					WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID)); err != nil {
					t.Fatalf("delete agent-mail Inbox: %v", err)
				}
			},
		},
		{
			name:   "dead-lettered Inbox",
			mutate: runtimePodLostDeliveryInboxStatusMutation("dead_lettered"),
		},
		{
			name:   "cancelled Inbox",
			mutate: runtimePodLostDeliveryInboxStatusMutation("cancelled"),
		},
		{
			name: "wrong delivery identity",
			mutate: func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET runtime_input_id='agent_mail:delivery_other'
					WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID)); err != nil {
					t.Fatalf("change agent-mail delivery identity: %v", err)
				}
			},
		},
		{
			name: "wrong target thread",
			mutate: func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET session_thread_id=$3
					WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID), fixture.parentThreadID); err != nil {
					t.Fatalf("change agent-mail target thread: %v", err)
				}
			},
		},
		{
			name: "wrong received event",
			mutate: func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox
					SET event_ids_json='["evt_wrong_received"]', sequence_from=1, sequence_to=1
					WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID)); err != nil {
					t.Fatalf("change agent-mail received event: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			fixture := seedRuntimePodLostDeliveryFixture(t, admin, 100+index, "spawn_agent", "idle", true, test.withReceived, false, true, false)
			if test.mutate != nil {
				test.mutate(t, admin, fixture)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, fixture.sessionID, fixture.binding, fixture.now); err != nil {
					t.Fatalf("repair attempt %d: %v", attempt+1, err)
				}
			}
			var delivered, failed int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				count(*) FILTER (WHERE COALESCE((payload_json::jsonb ->> 'is_error')::boolean, false) = false),
				count(*) FILTER (WHERE payload_json::jsonb ->> 'reason' = 'runtime_pod_lost_delivery_failed')
				FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
				AND payload_json::jsonb ->> 'tool_use_event_id'=$2`, fixture.sessionID, fixture.toolUseEventID).Scan(&delivered, &failed); err != nil {
				t.Fatalf("read delivery settlement: %v", err)
			}
			if test.wantDelivered {
				if delivered != 1 || failed != 0 {
					t.Fatalf("delivery settlement delivered=%d failed=%d; want 1/0", delivered, failed)
				}
			} else if delivered != 0 || failed != 1 {
				t.Fatalf("delivery settlement delivered=%d failed=%d; want 0/1", delivered, failed)
			}
		})
	}
}

func runtimePodLostDeliveryInboxBindingMutation(bindingID string, generation int64, podUID string) func(*testing.T, *sql.DB, runtimePodLostDeliveryFixture) {
	return func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
		t.Helper()
		if bindingID == "" {
			bindingID = fixture.binding.BindingID
		}
		if generation == 0 {
			generation = fixture.binding.BindingGeneration
		}
		if podUID == "" {
			podUID = fixture.binding.PodUID
		}
		if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox
			SET binding_id=$3,binding_generation=$4,target_pod_uid=$5
			WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`,
			fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID), bindingID, generation, podUID); err != nil {
			t.Fatalf("change agent-mail Inbox binding: %v", err)
		}
	}
}

func runtimePodLostDeliveryInboxStatusMutation(status string) func(*testing.T, *sql.DB, runtimePodLostDeliveryFixture) {
	return func(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture) {
		t.Helper()
		if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status=$3
			WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, fixture.sessionID, runtimecontrol.CompletionRuntimeInputID(fixture.deliveryID), status); err != nil {
			t.Fatalf("set agent-mail Inbox status: %v", err)
		}
	}
}

func seedRuntimePodLostDeliveryEvent(t *testing.T, db *sql.DB, fixture runtimePodLostDeliveryFixture, threadID string, eventID string, sequence int64, eventType string, payloadJSON string, visibility string, sessionVisible bool) {
	t.Helper()
	projectionJSON := `{}`
	if eventType == "span.model_request_start" {
		projectionJSON = `{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}`
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES ('default', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		fixture.sessionID, threadID, eventID, sequence, eventType, payloadJSON, visibility, sessionVisible, fixture.modelRequestID, projectionJSON); err != nil {
		t.Fatalf("seed %s event %s: %v", eventType, eventID, err)
	}
}

func runRuntimePodLostRepairTransaction(ctx context.Context, runtime *sql.DB, sessionID string, binding runtimecontrol.Binding, now time.Time) (int, error) {
	client := dbconnect.NewClientForTesting(runtime)
	repaired := 0
	err := client.WithWorkspaceTx(ctx, "default", "test.runtime_pod_lost_delivery_repair", func(tx *dbconnect.Tx) error {
		var err error
		repaired, _, err = repairLostRuntimeBindingDetailedTx(ctx, tx, "default", sessionID, binding, now)
		return err
	})
	return repaired, err
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
