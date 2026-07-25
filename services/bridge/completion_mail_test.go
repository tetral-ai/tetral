package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	"github.com/tetral-ai/tetral/services/bridge/internal/outputcapture"
)

func TestCompletionMailEnvelopeUsesStatusInvariantWrapper(t *testing.T) {
	payload := "completed child output"
	got := completionMailEnvelope("main", "worker", payload)
	want := "Message Type: FINAL_ANSWER\nTask name: main\nSender: worker\nPayload:\ncompleted child output"
	if got != want {
		t.Fatalf("completion envelope = %q; want %q", got, want)
	}
}

func TestCompletionMailErrorPayloadMiddleTruncatesRawReasonBeforeWrapping(t *testing.T) {
	reason := strings.Repeat("头", 700) + strings.Repeat("middle", 600) + strings.Repeat("尾", 700)
	got := completionMailErrorPayload(reason)
	const guidance = "This agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task."
	if !strings.HasPrefix(got, "Agent errored: "+strings.Repeat("头", 600)) {
		t.Fatalf("errored payload lost UTF-8-safe head: %q", got[:128])
	}
	if !strings.Contains(got, "…1050 tokens truncated…") {
		t.Fatalf("errored payload lacks byte-derived truncation marker: %q", got)
	}
	if !strings.Contains(got, strings.Repeat("尾", 600)+"\n\n"+guidance) {
		t.Fatalf("errored payload lost UTF-8-safe tail or guidance: %q", got[len(got)-512:])
	}
	if strings.Contains(got, strings.Repeat("middle", 600)) {
		t.Fatal("errored payload retained the elided middle")
	}
}

func TestCompletionMailErrorPayloadLeavesShortReasonVerbatim(t *testing.T) {
	const reason = "provider stream failed"
	got := completionMailErrorPayload(reason)
	want := "Agent errored: provider stream failed\n\nThis agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task."
	if got != want {
		t.Fatalf("short errored payload = %q; want %q", got, want)
	}
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

func TestPostgreSQLCompletionMailFinishIdleDiscriminator(t *testing.T) {
	tests := []struct {
		name          string
		stopReason    string
		seedEvidence  func(*testing.T, *sql.DB, string)
		wantMail      bool
		wantPayload   string
		payloadPrefix string
	}{
		{
			name:       "clean completion",
			stopReason: `{"type":"end_turn"}`,
			seedEvidence: func(t *testing.T, db *sql.DB, suffix string) {
				seedCompletionFinalAssistantMessage(t, db, suffix, "completed child output")
			},
			wantMail:    true,
			wantPayload: "completed child output",
		},
		{
			name:       "empty completion",
			stopReason: `{"type":"end_turn"}`,
			seedEvidence: func(t *testing.T, db *sql.DB, suffix string) {
				seedCompletionFinalAssistantMessage(t, db, suffix, "")
			},
			wantMail:    true,
			wantPayload: "",
		},
		{
			name:       "large completion",
			stopReason: `{"type":"end_turn"}`,
			seedEvidence: func(t *testing.T, db *sql.DB, suffix string) {
				seedCompletionFinalAssistantMessage(t, db, suffix, strings.Repeat("verbatim-success-", 300))
			},
			wantMail:    true,
			wantPayload: strings.Repeat("verbatim-success-", 300),
		},
		{
			name:       "terminal error",
			stopReason: `{"type":"end_turn"}`,
			seedEvidence: func(t *testing.T, db *sql.DB, suffix string) {
				seedBridgeAPIEvent(t, db, "default", completionTestSessionID(suffix), completionTestChildID(suffix),
					"evt_completion_terminal_"+suffix, 3, "session.error",
					`{"type":"session.error","error":{"type":"unknown_error","message":"terminal child failure","retry_status":{"type":"terminal"}}}`)
			},
			wantMail:      true,
			payloadPrefix: "Agent errored: terminal child failure\n\n" + completionMailErrorGuidance,
		},
		{
			name:       "processed interrupt",
			stopReason: `{"type":"end_turn"}`,
			seedEvidence: func(t *testing.T, db *sql.DB, suffix string) {
				eventID := "evt_completion_interrupt_" + suffix
				seedBridgeAPIEvent(t, db, "default", completionTestSessionID(suffix), completionTestChildID(suffix),
					eventID, 3, "user.interrupt", `{"type":"user.interrupt"}`)
				if _, err := db.ExecContext(context.Background(),
					`UPDATE session_events SET processed_at='2026-01-01T00:00:01Z'
					  WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`,
					completionTestSessionID(suffix), eventID,
				); err != nil {
					t.Fatalf("mark completion interrupt processed: %v", err)
				}
			},
		},
		{
			name:       "requires action",
			stopReason: `{"type":"requires_action","event_ids":["evt_pending"]}`,
		},
		{
			name:          "retry exhaustion",
			stopReason:    `{"type":"retries_exhausted"}`,
			wantMail:      true,
			payloadPrefix: "Agent errored: Runtime retries exhausted.\n\n" + completionMailErrorGuidance,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			suffix := strings.ReplaceAll(test.name, " ", "_")
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
			seedBridgeAPIEvent(t, admin, "default", completionTestSessionID(suffix), completionTestChildID(suffix),
				"evt_completion_running_"+suffix, 2, "session.thread_status_running", `{"type":"session.thread_status_running"}`)
			if test.seedEvidence != nil {
				test.seedEvidence(t, admin, suffix)
			}
			store := completionMailTestStore(t, runtime)
			request := bridgeAPIChildFinishIdleFailureRequest(suffix)
			request.StopReasonJson = test.stopReason

			response, err := store.FinishIdle(context.Background(), request)
			if err != nil {
				t.Fatalf("FinishIdle %s: %v", test.name, err)
			}
			if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
				t.Fatalf("FinishIdle %s ack = %s; want committed", test.name, response.GetAck().GetStatus())
			}
			mailCount, jobCount, envelope := completionMailRows(t, admin, completionTestSessionID(suffix))
			wantCount := 0
			if test.wantMail {
				wantCount = 1
			}
			if mailCount != wantCount || jobCount != wantCount {
				t.Fatalf("%s completion rows = mail %d job %d; want %d/%d", test.name, mailCount, jobCount, wantCount, wantCount)
			}
			if !test.wantMail {
				return
			}
			wantEnvelope := completionMailEnvelope("main", "task_"+completionTestChildID(suffix), test.wantPayload)
			if test.payloadPrefix != "" {
				wantEnvelope = completionMailEnvelope("main", "task_"+completionTestChildID(suffix), test.payloadPrefix)
			}
			if envelope != wantEnvelope {
				t.Fatalf("%s completion envelope = %q; want %q", test.name, envelope, wantEnvelope)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreChildFinishIdleRearmsPendingCompletionMail(t *testing.T) {
	tests := []struct {
		name       string
		stopReason string
		interrupt  bool
	}{
		{name: "end_turn", stopReason: `{"type":"end_turn"}`},
		{name: "requires_action", stopReason: `{"type":"requires_action","event_ids":["evt_pending"]}`},
		{name: "retries_exhausted", stopReason: `{"type":"retries_exhausted"}`},
		{name: "processed_interrupt", stopReason: `{"type":"end_turn"}`, interrupt: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := "inbound_rearm_" + test.name
			sessionID := completionTestSessionID(suffix)
			childID := completionTestChildID(suffix)
			grandchildID := childID + "_sender"
			deliveryID := "delivery_" + suffix
			seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, childID, grandchildID)
			seedCompletionMailSentAt(t, admin, sessionID, childID, grandchildID, deliveryID, 1, "2026-01-01T00:00:01Z")
			if test.interrupt {
				eventID := "evt_interrupt_" + suffix
				seedBridgeAPIEvent(t, admin, "default", sessionID, childID, eventID, 2, "user.interrupt", `{"type":"user.interrupt"}`)
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_events
					    SET processed_at = '2026-01-01T00:00:02Z'
					  WHERE workspace_id = 'default'
					    AND session_id = $1
					    AND event_id = $2`,
					sessionID,
					eventID,
				); err != nil {
					t.Fatalf("mark interrupt processed: %v", err)
				}
			}

			store := completionMailTestStore(t, runtime)
			request := bridgeAPIChildFinishIdleFailureRequest(suffix)
			request.StopReasonJson = test.stopReason
			if _, err := store.FinishIdle(context.Background(), request); err != nil {
				t.Fatalf("FinishIdle %s: %v", test.name, err)
			}
			assertActiveCompletionWake(t, admin, sessionID, deliveryID, true)
			if test.interrupt {
				var outboundCount int
				if err := admin.QueryRowContext(context.Background(),
					`SELECT count(*)
					   FROM session_events
					  WHERE workspace_id = 'default'
					    AND session_id = $1
					    AND type = 'agent.thread_message_sent'
					    AND payload_json::jsonb ->> 'source_thread_id' = $2`,
					sessionID,
					childID,
				).Scan(&outboundCount); err != nil {
					t.Fatalf("count interrupted child outbound completion: %v", err)
				}
				if outboundCount != 0 {
					t.Fatalf("interrupted child outbound completion count = %d; want 0", outboundCount)
				}
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreMainFinishIdleDrainsCompletionMailAcrossBoundedPages(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_main_finish_idle_mail_pages"
		mainID    = "thr_main_finish_idle_mail_pages"
		bindingID = "bind_main_finish_idle_mail_pages"
		podUID    = "pod_main_finish_idle_mail_pages"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_main_finish_idle_mail_pages")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, running_since, active_seconds_total,
			binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', $1, 'running', '2026-01-01T00:00:00Z', 0,
			$2, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`,
		sessionID,
		bindingID,
	); err != nil {
		t.Fatalf("seed running runtime status: %v", err)
	}
	deliveries := make([]string, MailFetchMaxEnvelopes+1)
	for index := range deliveries {
		childID := fmt.Sprintf("thr_main_finish_idle_mail_child_%d", index)
		deliveries[index] = fmt.Sprintf("delivery_main_finish_idle_mail_%d", index)
		seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
		seedCompletionMailSentAt(
			t,
			admin,
			sessionID,
			mainID,
			childID,
			deliveries[index],
			int64(index+1),
			fmt.Sprintf("2026-01-01T00:00:0%dZ", index+1),
		)
	}

	store := completionMailTestStore(t, runtime)
	first := &bridgev1.FinishIdleRequest{
		Scope:          bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		RuntimeWriteId: "rwrite_main_finish_idle_mail_page_1",
		IdleSince:      "2026-01-01T00:00:45Z",
		StopReasonJson: `{"type":"end_turn"}`,
	}
	if _, err := store.FinishIdle(context.Background(), first); err != nil {
		t.Fatalf("FinishIdle first page: %v", err)
	}
	for index, deliveryID := range deliveries {
		assertActiveCompletionWake(t, admin, sessionID, deliveryID, index < MailFetchMaxEnvelopes)
	}

	var nextSequence int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`,
		sessionID,
		mainID,
	).Scan(&nextSequence); err != nil {
		t.Fatalf("read next main-thread sequence: %v", err)
	}
	for index := 0; index < MailFetchMaxEnvelopes; index++ {
		seedBridgeAPIEvent(
			t,
			admin,
			"default",
			sessionID,
			mainID,
			fmt.Sprintf("evt_main_finish_idle_mail_received_%d", index),
			nextSequence+int64(index),
			"agent.thread_message_received",
			`{"delivery_id":"`+deliveries[index]+`"}`,
		)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET status='running',
		        running_since='2026-01-01T00:01:00Z',
		        idle_since=NULL,
		        cleanup_after=NULL,
		        updated_at='2026-01-01T00:01:00Z'
		  WHERE workspace_id='default' AND session_id=$1`,
		sessionID,
	); err != nil {
		t.Fatalf("restart main-thread runtime status: %v", err)
	}
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 45, 0, time.UTC) }
	second := proto.Clone(first).(*bridgev1.FinishIdleRequest)
	second.RuntimeWriteId = "rwrite_main_finish_idle_mail_page_2"
	second.IdleSince = "2026-01-01T00:01:45Z"
	if _, err := store.FinishIdle(context.Background(), second); err != nil {
		t.Fatalf("FinishIdle tail page: %v", err)
	}
	assertActiveCompletionWake(t, admin, sessionID, deliveries[MailFetchMaxEnvelopes], true)
}

func TestPostgreSQLCompletionMailRequiresActionThenSameChildCompletesNextTurn(t *testing.T) {
	const suffix = "requires_action_then_complete"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	seedBridgeAPIEvent(t, admin, "default", completionTestSessionID(suffix), completionTestChildID(suffix),
		"evt_completion_running_first_"+suffix, 2, "session.thread_status_running", `{"type":"session.thread_status_running"}`)
	store := completionMailTestStore(t, runtime)

	first := bridgeAPIChildFinishIdleFailureRequest(suffix)
	first.StopReasonJson = `{"type":"requires_action","event_ids":["evt_pending"]}`
	if _, err := store.FinishIdle(context.Background(), first); err != nil {
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
	seedCompletionFinalAssistantMessageAtSequence(t, admin, suffix, "completed after action", 5, "second")

	second := bridgeAPIChildFinishIdleFailureRequest(suffix)
	second.RuntimeWriteId = "rwrite_bridge_child_finish_idle_second_" + suffix
	if _, err := store.FinishIdle(context.Background(), second); err != nil {
		t.Fatalf("FinishIdle next clean turn: %v", err)
	}
	mailCount, jobCount, envelope := completionMailRows(t, admin, completionTestSessionID(suffix))
	wantEnvelope := completionMailEnvelope("main", "task_"+completionTestChildID(suffix), "completed after action")
	if mailCount != 1 || jobCount != 1 || envelope != wantEnvelope {
		t.Fatalf("next-turn completion rows = mail %d job %d envelope %q; want 1/1/%q", mailCount, jobCount, envelope, wantEnvelope)
	}
}

func TestPostgreSQLCompletionMailRollsBackWithSettlementAndRetriesExactlyOnce(t *testing.T) {
	const suffix = "atomic_retry"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET superseded_at='2026-01-01T00:00:30Z'
		  WHERE workspace_id='default' AND session_id=$1`,
		completionTestSessionID(suffix),
	); err != nil {
		t.Fatalf("hide active preparation: %v", err)
	}
	store := completionMailTestStore(t, runtime)
	request := bridgeAPIChildFinishIdleFailureRequest(suffix)

	if _, err := store.FinishIdle(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("FinishIdle without active preparation err = %v; want FailedPrecondition", err)
	}
	mailCount, jobCount, _ := completionMailRows(t, admin, completionTestSessionID(suffix))
	var idleCount, operationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='session.thread_status_idle'`,
		completionTestSessionID(suffix),
	).Scan(&idleCount); err != nil {
		t.Fatalf("count rolled-back idle events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_bridge_operations
		  WHERE workspace_id='default' AND session_id=$1 AND operation='finish_idle'`,
		completionTestSessionID(suffix),
	).Scan(&operationCount); err != nil {
		t.Fatalf("count rolled-back finish operations: %v", err)
	}
	if mailCount != 0 || jobCount != 0 || idleCount != 0 || operationCount != 0 {
		t.Fatalf("rolled-back settlement rows = mail %d job %d idle %d operation %d; want all zero", mailCount, jobCount, idleCount, operationCount)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET superseded_at=NULL
		  WHERE workspace_id='default' AND session_id=$1`,
		completionTestSessionID(suffix),
	); err != nil {
		t.Fatalf("restore active preparation: %v", err)
	}
	first, err := store.FinishIdle(context.Background(), request)
	if err != nil {
		t.Fatalf("FinishIdle retry: %v", err)
	}
	replay, err := store.FinishIdle(context.Background(), request)
	if err != nil {
		t.Fatalf("FinishIdle replay: %v", err)
	}
	if first.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("retry/replay ACKs = %s/%s; want committed/duplicate", first.GetAck().GetStatus(), replay.GetAck().GetStatus())
	}
	mailCount, jobCount, _ = completionMailRows(t, admin, completionTestSessionID(suffix))
	if mailCount != 1 || jobCount != 1 {
		t.Fatalf("retried settlement rows = mail %d job %d; want 1/1", mailCount, jobCount)
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
		seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_completion_reviewer_idle")
		seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
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
		if _, err := store.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
			Scope:          bridgeAPIScope(sessionID, reviewerID, "bind_completion_reviewer_idle", 1, "pod_completion_reviewer_idle"),
			RuntimeWriteId: "rwrite_completion_reviewer_idle",
			IdleSince:      "2026-01-01T00:00:45Z",
			StopReasonJson: `{"type":"end_turn"}`,
		}); err != nil {
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
		if _, err := store.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
			Scope:          bridgeAPIScope(sessionID, reviewerID, "bind_completion_reviewer_terminate", 1, "pod_completion_reviewer_terminate"),
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
	store.SandboxStatusFreshnessWindow = 5 * time.Minute
	store.OutputCapturer = outputcapture.NewCapturer(blob.NewFakeBlobStore(), &recordingOutputScanner{})
	return store
}

func completionTestSessionID(suffix string) string {
	return "sesn_bridge_child_finish_idle_" + suffix
}

func completionTestChildID(suffix string) string {
	return "thr_bridge_child_finish_idle_" + suffix
}

func seedCompletionFinalAssistantMessage(t *testing.T, db *sql.DB, suffix string, text string) {
	seedCompletionFinalAssistantMessageAtSequence(t, db, suffix, text, 3, "first")
}

func seedCompletionFinalAssistantMessageAtSequence(t *testing.T, db *sql.DB, suffix string, text string, eventSequence int64, identitySuffix string) {
	t.Helper()
	sessionID := completionTestSessionID(suffix)
	childID := completionTestChildID(suffix)
	modelRequestID := "mreq_completion_" + suffix + "_" + identitySuffix
	payloadJSON, err := json.Marshal(map[string]any{
		"type":             "span.model_request_end",
		"model_request_id": modelRequestID,
		"request_kind":     "agent_provider_request",
	})
	if err != nil {
		t.Fatalf("marshal completion request end: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, model_request_id, created_at, updated_at, processed_at
		) VALUES (
			'default', $1, $2, $3, $4, 'span.model_request_end',
			$5, $6, '2026-01-01T00:00:02Z', '2026-01-01T00:00:02Z', '2026-01-01T00:00:02Z'
		)`,
		sessionID, childID, "evt_completion_end_"+suffix+"_"+identitySuffix, eventSequence, string(payloadJSON), modelRequestID,
	); err != nil {
		t.Fatalf("seed completion request end: %v", err)
	}
	messageJSON, err := json.Marshal(map[string]any{
		"parts": []map[string]any{{"type": "text", "text": text}},
	})
	if err != nil {
		t.Fatalf("marshal completion assistant message: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, model_request_id, created_at, updated_at
		) VALUES (
			'default', $1, $2, $3, 1, 'assistant',
			$4, $5, '2026-01-01T00:00:02Z', '2026-01-01T00:00:02Z'
		)`,
		sessionID, childID, "msg_completion_"+suffix+"_"+identitySuffix, string(messageJSON), modelRequestID,
	); err != nil {
		t.Fatalf("seed completion assistant message: %v", err)
	}
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
