package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
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
				completionRuntimeInputID(fixture.deliveryID),
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
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
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

func TestPostgreSQLRuntimePodLossReexecutedWaitReservesCurrentCompletionAndRefusesItAfterSupersession(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 25, "wait_agent", "idle", false, false, false, false, true)
	const completionDeliveryID = "delivery_pod_loss_wait_completion"
	seedRuntimePodLostDeliveryEvent(t, admin, fixture, fixture.childThreadID, "evt_pod_loss_wait_opener", 1,
		"agent.thread_message_received", `{"type":"agent.thread_message_received"}`, "public", true)
	messageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		fixture.sessionID,
		"msg_pod_loss_wait_completion",
		completionMailEnvelope("main", "worker", "current completion"),
	)
	sentPayload := bridgeInterAgentSentEventJSON(
		t,
		completionDeliveryID,
		fixture.childThreadID,
		fixture.parentThreadID,
		"",
		"sevt_pod_loss_wait_spawn",
		messageJSON,
	)
	seedRuntimePodLostDeliveryEvent(t, admin, fixture, fixture.childThreadID, "evt_pod_loss_wait_completion", 2,
		"agent.thread_message_sent", sentPayload, "public", true)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("pod-loss-wait-currency-key")
	scope := bridgeAPIScope(
		fixture.sessionID,
		fixture.parentThreadID,
		fixture.binding.BindingID,
		fixture.binding.BindingGeneration,
		fixture.binding.PodUID,
	)
	if _, err := store.CommitInputs(context.Background(), bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		scope,
		"agent_mail:"+completionDeliveryID,
		completionDeliveryID,
		fixture.childThreadID,
		"sevt_pod_loss_wait_spawn",
		messageJSON,
	)); err != nil {
		t.Fatalf("commit pre-loss wait receipt: %v", err)
	}
	if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, fixture.sessionID, fixture.binding, fixture.now); err != nil {
		t.Fatalf("repair lost wait request: %v", err)
	}
	var waitFailureCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id'=$2
		    AND payload_json::jsonb ->> 'reason'='runtime_pod_lost'`,
		fixture.sessionID,
		fixture.toolUseEventID,
	).Scan(&waitFailureCount); err != nil {
		t.Fatalf("count repaired wait result: %v", err)
	}
	if waitFailureCount != 1 {
		t.Fatalf("repaired wait results = %d; want 1", waitFailureCount)
	}

	current, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_pod_loss_wait_current",
	})
	if err != nil {
		t.Fatalf("load current completion after pod loss: %v", err)
	}
	var currentPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(current.GetContextJson()), &currentPayload); err != nil {
		t.Fatalf("decode current completion after pod loss: %v", err)
	}
	if len(currentPayload.PendingAgentMail) != 0 {
		t.Fatalf("current completion after committed receipt = %#v; want none", currentPayload.PendingAgentMail)
	}

	seedRuntimePodLostDeliveryEvent(t, admin, fixture, fixture.childThreadID, "evt_pod_loss_wait_newer_opener", 3,
		"agent.thread_message_received", `{"type":"agent.thread_message_received"}`, "public", true)
	superseded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_pod_loss_wait_superseded",
	})
	if err != nil {
		t.Fatalf("load superseded completion after pod loss: %v", err)
	}
	var supersededPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(superseded.GetContextJson()), &supersededPayload); err != nil {
		t.Fatalf("decode superseded completion after pod loss: %v", err)
	}
	if len(supersededPayload.PendingAgentMail) != 0 {
		t.Fatalf("superseded completion after pod loss = %#v; want status-only", supersededPayload.PendingAgentMail)
	}
	var receiptCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		    AND type='agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id'=$3`,
		fixture.sessionID,
		fixture.parentThreadID,
		completionDeliveryID,
	).Scan(&receiptCount); err != nil {
		t.Fatalf("count wait completion receipts: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("wait completion receipts after repair and reads = %d; want original receipt only", receiptCount)
	}
}

func TestPostgreSQLRuntimePodLossDeliveryChildCloseSerializesBeforeRedrive(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 24, "send_message", "idle", true, false, false, true, false)

	closeTx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin child close: %v", err)
	}
	defer func() { _ = closeTx.Rollback() }()
	if _, err := closeTx.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'closed_for_runtime', closed_at = '2026-01-01T00:04:59Z', updated_at = '2026-01-01T00:04:59Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`, fixture.sessionID, fixture.childThreadID); err != nil {
		t.Fatalf("lock child close: %v", err)
	}

	repairDone := make(chan error, 1)
	go func() {
		_, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, fixture.sessionID, fixture.binding, fixture.now)
		repairDone <- err
	}()
	if err := closeTx.Commit(); err != nil {
		t.Fatalf("commit child close: %v", err)
	}
	select {
	case err := <-repairDone:
		if err != nil {
			t.Fatalf("repair after serialized child close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("repair remained blocked after child close committed")
	}

	var receivedCount int
	var errorResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $2`, fixture.sessionID, fixture.deliveryID).Scan(&receivedCount); err != nil {
		t.Fatalf("count close-raced receipts: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = $2
		    AND (payload_json::jsonb ->> 'is_error')::boolean`, fixture.sessionID, fixture.toolUseEventID).Scan(&errorResultCount); err != nil {
		t.Fatalf("count close-raced parent errors: %v", err)
	}
	if receivedCount != 0 || errorResultCount != 1 {
		t.Fatalf("close-raced delivery receipts=%d errors=%d; want 0/1", receivedCount, errorResultCount)
	}
}

type runtimePodLostDeliveryFixture struct {
	sessionID      string
	parentThreadID string
	childThreadID  string
	modelRequestID string
	toolUseEventID string
	deliveryID     string
	binding        runtimeBindingForDelivery
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
	sequence++
	if withSent {
		messageJSON := bridgeRuntimeUserMessageJSON(t, fixture.sessionID, "msg_"+fixture.deliveryID, "hello worker")
		sentPayload := bridgeInterAgentSentEventJSON(t, fixture.deliveryID, fixture.parentThreadID, fixture.childThreadID, "worker", fixture.toolUseEventID, messageJSON)
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_sent_"+fixture.toolUseEventID, sequence, "agent.thread_message_sent", sentPayload, "public", true)
		sequence++
		if withReceived {
			receivedPayload := bridgeInterAgentMessageJSON(t, fixture.deliveryID, fixture.parentThreadID, fixture.toolUseEventID, messageJSON)
			seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.childThreadID, "evt_received_"+fixture.toolUseEventID, 1, "agent.thread_message_received", receivedPayload, "public", true)
		}
	}
	if withTerminal {
		payload := fmt.Sprintf(`{"type":"agent.tool_result","tool_use_event_id":%q,"content":[{"type":"text","text":"existing terminal"}],"is_error":false}`, fixture.toolUseEventID)
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_result_"+fixture.toolUseEventID, sequence, "agent.tool_result", payload, "public", true)
		sequence++
	}
	if withRequestEnd {
		payload := fmt.Sprintf(`{"type":"span.model_request_end","model_request_id":%q,"model_request_start_id":%q,"request_kind":"agent_provider_request","is_error":false,"finish_reason":"stop","usage":{}}`, fixture.modelRequestID, "evt_start_"+fixture.toolUseEventID)
		seedRuntimePodLostDeliveryEvent(t, db, fixture, fixture.parentThreadID, "evt_end_"+fixture.toolUseEventID, sequence, "span.model_request_end", payload, "internal", false)
	}
	return fixture
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

func runRuntimePodLostRepairTransaction(ctx context.Context, runtime *sql.DB, sessionID string, binding runtimeBindingForDelivery, now time.Time) (int, error) {
	client := dbconnect.NewClientForTesting(runtime)
	repaired := 0
	err := client.WithWorkspaceTx(ctx, "default", "test.runtime_pod_lost_delivery_repair", func(tx *dbconnect.Tx) error {
		var err error
		repaired, err = repairLostRuntimeBindingTx(ctx, tx, "default", sessionID, binding, now)
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
