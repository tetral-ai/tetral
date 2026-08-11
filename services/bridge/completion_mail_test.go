package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func completionMailEnvelope(taskName string, sender string, payload string) string {
	return "Message Type: FINAL_ANSWER\nTask name: " + taskName + "\nSender: " + sender + "\nPayload:\n" + payload
}

func TestCompletionDeliveryIdentityIsScopedToTheSettlingChild(t *testing.T) {
	first := completionDeliveryID("thr_child_a", "rwrite_shared")
	second := completionDeliveryID("thr_child_b", "rwrite_shared")
	if first == second {
		t.Fatalf("sender-scoped completion delivery ids collided: %q", first)
	}
	if first != completionDeliveryID("thr_child_a", "rwrite_shared") {
		t.Fatal("completion delivery identity is not deterministic")
	}
}

func TestPostgreSQLCompletionMailPersistsDeclaredEnvelopeVerbatim(t *testing.T) {
	const suffix = "declared_verbatim"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	store := completionMailTestStore(t, runtime)
	request := bridgeAPIChildFinishIdleFailureRequest(suffix)
	wantEnvelope := completionMailEnvelope(
		"main",
		"task_"+completionTestChildID(suffix),
		"the loop authored this body\nverbatim",
	)
	request.CompletionMailCreate = bridgeCompletionMailCreateForTest(request.GetScope(), request.GetDurableTurnId(), wantEnvelope)
	request.CompletionMailCreate.MessageInfoJson = `{"role":"user","origin":"runtime","status":"completed","finishReason":"stop","responseId":"completion_response","usage":{"inputTokens":1,"outputTokens":2,"reasoningTokens":3,"cacheReadTokens":4,"cacheWriteTokens":5}}`
	request.CompletionMailCreate.Parts[0].PartJson = fmt.Sprintf(`{"type":"text","text":%q,"truncated":true,"status":"failed","startedAt":"2026-01-01T00:00:40Z","completedAt":"2026-01-01T00:00:41Z"}`, wantEnvelope)

	response, err := finishIdleWithStagedCaptureForTest(t, admin, store, request)
	if err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	replay, err := store.FinishIdle(context.Background(), request)
	if err != nil {
		t.Fatalf("FinishIdle replay: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("FinishIdle ACKs = %s/%s; want committed/duplicate", response.GetAck().GetStatus(), replay.GetAck().GetStatus())
	}
	mailCount, jobCount, envelope := completionMailRows(t, admin, completionTestSessionID(suffix))
	if mailCount != 1 || jobCount != 1 || envelope != wantEnvelope {
		t.Fatalf("completion rows = mail %d job %d envelope %q; want 1/1/%q", mailCount, jobCount, envelope, wantEnvelope)
	}
	var inboxCount, queuedInboxCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*), count(*) FILTER (WHERE status='queued') FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'`, completionTestSessionID(suffix)).Scan(&inboxCount, &queuedInboxCount); err != nil {
		t.Fatalf("read completion mail Inbox custody: %v", err)
	}
	if inboxCount != 1 || queuedInboxCount != 1 {
		t.Fatalf("completion mail Inbox rows = %d queued = %d; want exactly one queued row", inboxCount, queuedInboxCount)
	}
	var semantics struct {
		Message struct {
			Origin       string         `json:"origin"`
			Status       string         `json:"status"`
			FinishReason string         `json:"finishReason"`
			ResponseID   string         `json:"responseId"`
			Usage        map[string]any `json:"usage"`
			Parts        []struct {
				Truncated   bool   `json:"truncated"`
				Status      string `json:"status"`
				StartedAt   string `json:"startedAt"`
				CompletedAt string `json:"completedAt"`
			} `json:"parts"`
		} `json:"message"`
	}
	var raw string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`,
		completionTestSessionID(suffix),
	).Scan(&raw); err != nil {
		t.Fatalf("read completion declaration: %v", err)
	}
	if err := json.Unmarshal([]byte(raw), &semantics); err != nil {
		t.Fatalf("decode completion declaration: %v", err)
	}
	part := semantics.Message.Parts[0]
	if semantics.Message.Origin != "runtime" || semantics.Message.Status != "completed" ||
		semantics.Message.FinishReason != "stop" || semantics.Message.ResponseID != "completion_response" ||
		semantics.Message.Usage["reasoningTokens"] != float64(3) || !part.Truncated || part.Status != "failed" ||
		part.StartedAt != "2026-01-01T00:00:40Z" || part.CompletedAt != "2026-01-01T00:00:41Z" {
		t.Fatalf("completion declaration semantics changed: %#v", semantics.Message)
	}
}

func TestPostgreSQLCompletionMailBirthRollsBackSourceInboxAndQueueTogether(t *testing.T) {
	const suffix = "birth_rollback"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_completion_mail_queue_birth() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected completion mail queue failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_completion_mail_queue_birth BEFORE INSERT ON queue_jobs
		FOR EACH ROW WHEN (NEW.kind = 'runtime_input') EXECUTE FUNCTION fail_completion_mail_queue_birth()`); err != nil {
		t.Fatalf("install completion mail queue failure: %v", err)
	}
	store := completionMailTestStore(t, runtime)
	request := bridgeAPIChildFinishIdleFailureRequest(suffix)
	request.CompletionMailCreate = bridgeCompletionMailCreateForTest(
		request.GetScope(), request.GetDurableTurnId(),
		completionMailEnvelope("main", "task_"+completionTestChildID(suffix), "rollback"),
	)
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, store, request); err == nil {
		t.Fatal("FinishIdle succeeded despite injected completion mail Queue failure")
	}
	var sent, inbox, jobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb ->> 'session_id'=$1 AND kind='runtime_input')`,
		completionTestSessionID(suffix),
	).Scan(&sent, &inbox, &jobs); err != nil {
		t.Fatalf("read rolled-back completion custody: %v", err)
	}
	if sent != 0 || inbox != 0 || jobs != 0 {
		t.Fatalf("rolled-back completion custody = sent %d Inbox %d Queue %d; want 0/0/0", sent, inbox, jobs)
	}
}

func TestPostgreSQLCompletionMailRequiresActionThenSameChildCompletesNextTurn(t *testing.T) {
	const suffix = "requires_action_then_complete"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	store := completionMailTestStore(t, runtime)

	first := bridgeAPIChildFinishIdleFailureRequest(suffix)
	first.StopReasonJson = `{"type":"requires_action","event_ids":["evt_pending"]}`
	first.CompletionMailCreate = nil
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, store, first); err != nil {
		t.Fatalf("FinishIdle requires_action: %v", err)
	}
	mailCount, jobCount, _ := completionMailRows(t, admin, completionTestSessionID(suffix))
	if mailCount != 0 || jobCount != 0 {
		t.Fatalf("requires_action completion rows = mail %d job %d; want 0/0", mailCount, jobCount)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status='running'
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		completionTestSessionID(suffix),
		completionTestChildID(suffix),
	); err != nil {
		t.Fatalf("start next child turn: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", completionTestSessionID(suffix), completionTestChildID(suffix),
		"evt_completion_running_second_"+suffix, 4, "session.thread_status_running", `{"type":"session.thread_status_running"}`)

	second := bridgeAPIChildFinishIdleFailureRequest(suffix)
	second.DurableTurnId = "evt_completion_running_second_" + suffix
	second.CompletionMailCreate = bridgeCompletionMailCreateForTest(
		second.GetScope(),
		second.GetDurableTurnId(),
		completionMailEnvelope("main", "task_"+completionTestChildID(suffix), "completed after action"),
	)
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, store, second); err != nil {
		t.Fatalf("FinishIdle next clean turn: %v", err)
	}
	mailCount, jobCount, envelope := completionMailRows(t, admin, completionTestSessionID(suffix))
	wantEnvelope := completionMailEnvelope("main", "task_"+completionTestChildID(suffix), "completed after action")
	if mailCount != 1 || jobCount != 1 || envelope != wantEnvelope {
		t.Fatalf("next-turn completion rows = mail %d job %d envelope %q; want 1/1/%q", mailCount, jobCount, envelope, wantEnvelope)
	}
}

func TestPostgreSQLCompletionMailNeverLeavesApprovalReviewerThreads(t *testing.T) {
	t.Run("finish idle", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID  = "sesn_completion_reviewer_idle"
			mainID     = "thrd_completion_reviewer_idle_main"
			reviewerID = "thrd_completion_reviewer_idle"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewerID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_completion_reviewer_idle", 1, "pod_completion_reviewer_idle")
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE sessions SET status='running' WHERE workspace_id='default' AND id=$1`,
			sessionID,
		); err != nil {
			t.Fatalf("mark reviewer session running: %v", err)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_threads SET status='running'
			  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
			sessionID, reviewerID,
		); err != nil {
			t.Fatalf("mark reviewer thread running: %v", err)
		}
		store := completionMailTestStore(t, runtime)
		scope := bridgeAPIScope(sessionID, reviewerID, "bind_completion_reviewer_idle", 1, "pod_completion_reviewer_idle")
		request := &bridgev1.FinishIdleRequest{
			Scope:          scope,
			DurableTurnId:  "evt_completion_reviewer_running",
			StopReasonJson: `{"type":"end_turn"}`,
		}
		seedBridgeAPIOpenDurableTurn(t, admin, scope, request.GetDurableTurnId())
		if _, err := finishIdleWithStagedCaptureForTest(t, admin, store, request); err != nil {
			t.Fatalf("FinishIdle reviewer: %v", err)
		}
		mailCount, jobCount, _ := completionMailRows(t, admin, sessionID)
		if mailCount != 0 || jobCount != 0 {
			t.Fatalf("reviewer FinishIdle completion rows = %d/%d; want zero", mailCount, jobCount)
		}
	})

	t.Run("runtime termination", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID  = "sesn_completion_reviewer_terminate"
			mainID     = "thrd_completion_reviewer_terminate_main"
			reviewerID = "thrd_completion_reviewer_terminate"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewerID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_completion_reviewer_terminate", 1, "pod_completion_reviewer_terminate")
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		scope := bridgeAPIScope(sessionID, reviewerID, "bind_completion_reviewer_terminate", 1, "pod_completion_reviewer_terminate")
		seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_completion_reviewer_terminate")
		if _, err := store.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
			Scope:          scope,
			RuntimeWriteId: "rwrite_completion_reviewer_terminate",
			FailureJson:    `{"type":"provider","code":"provider_invalid_request","message":"Reviewer failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"}}`,
		}); err != nil {
			t.Fatalf("CommitRuntimeTermination reviewer: %v", err)
		}
		mailCount, jobCount, _ := completionMailRows(t, admin, sessionID)
		if mailCount != 0 || jobCount != 0 {
			t.Fatalf("reviewer termination completion rows = %d/%d; want zero", mailCount, jobCount)
		}
	})
}

func TestBridgeAPIServerLateFinishIdleOnFailedChildIsSupersededWithoutCompletionMail(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const suffix = "late_failed"
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status='failed'
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		completionTestSessionID(suffix),
		completionTestChildID(suffix),
	); err != nil {
		t.Fatalf("mark child failed: %v", err)
	}
	store := completionMailTestStore(t, runtime)
	response, err := (BridgeAPIServer{store: store}).FinishIdle(
		context.Background(),
		bridgeAPIChildFinishIdleFailureRequest(suffix),
	)
	if err != nil {
		t.Fatalf("late FinishIdle on failed child: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
		response.GetAck().GetErrorCode() != closeoutScopeSupersededCode {
		t.Fatalf("late failed-child FinishIdle ack = %+v; want rejected %q", response.GetAck(), closeoutScopeSupersededCode)
	}
	var childStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		completionTestSessionID(suffix),
		completionTestChildID(suffix),
	).Scan(&childStatus); err != nil {
		t.Fatalf("read failed child status: %v", err)
	}
	if childStatus != "failed" {
		t.Fatalf("late FinishIdle child status = %q; want failed", childStatus)
	}
	mailCount, jobCount, _ := completionMailRows(t, admin, completionTestSessionID(suffix))
	if mailCount != 0 || jobCount != 0 {
		t.Fatalf("late failed-child completion rows = mail %d job %d; want 0/0", mailCount, jobCount)
	}
}

func completionMailTestStore(t *testing.T, runtime *sql.DB) *PostgreSQLBridgeAPIStore {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	return store
}

func completionTestSessionID(suffix string) string {
	return "sesn_bridge_child_finish_idle_" + suffix
}

func completionTestChildID(suffix string) string {
	return "thr_bridge_child_finish_idle_" + suffix
}

func completionMailRows(t *testing.T, db *sql.DB, sessionID string) (int, int, string) {
	t.Helper()
	var mailCount, jobCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`,
		sessionID,
	).Scan(&mailCount); err != nil {
		t.Fatalf("count completion mail events: %v", err)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs
		  WHERE workspace_id='default' AND payload_json::jsonb ->> 'input_kind'='agent_mail'
		    AND payload_json::jsonb ->> 'session_id'=$1`,
		sessionID,
	).Scan(&jobCount); err != nil {
		t.Fatalf("count completion mail jobs: %v", err)
	}
	if mailCount == 0 {
		return mailCount, jobCount, ""
	}
	var raw string
	if err := db.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`,
		sessionID,
	).Scan(&raw); err != nil {
		t.Fatalf("read completion mail event: %v", err)
	}
	var payload struct {
		Message struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode completion mail event: %v", err)
	}
	if len(payload.Message.Parts) != 1 {
		t.Fatalf("completion mail parts = %d; want 1", len(payload.Message.Parts))
	}
	return mailCount, jobCount, payload.Message.Parts[0].Text
}
