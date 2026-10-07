package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestPostgreSQLRuntimePodLossReexecutedWaitReservesCurrentCompletionAndRefusesItAfterSupersession(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 25, "wait_agent", "idle", false, false, false, false, true)
	const completionDeliveryID = "delivery_pod_loss_wait_completion"
	seedRuntimePodLostDeliveryEvent(t, admin, fixture, fixture.childThreadID, "evt_pod_loss_wait_opener", 1,
		"agent.thread_message_received", `{"type":"agent.thread_message_received"}`, "public", true)
	messageJSON := bridgePublicMessageJSONForTest(t, completionMailEnvelope("main", "worker", "current completion"))
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

	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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
	scope = declareReplacementScope(t, store.Client, store.Client, scope)
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
	if waitFailureCount != 0 {
		t.Fatalf("repaired wait results = %d; want no synthetic Result", waitFailureCount)
	}

	current, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope,
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
		Scope: scope,
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
	// repairLostBindingThroughProduction installs the confirmed-loss visibility
	// for its repair; the store resolves no other target.
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, nil)
	err := repairLostBindingThroughProduction(ctx, store, "default", sessionID, binding, now)
	return 0, err
}
