package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func seedCompletionMailSentAt(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	targetThreadID string,
	sourceThreadID string,
	deliveryID string,
	sequence int64,
	createdAt string,
) {
	t.Helper()
	eventID := "evt_" + deliveryID
	messageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_"+deliveryID,
		completionMailEnvelope("main", "sender", deliveryID),
	)
	seedBridgeAPIEvent(
		t,
		db,
		"default",
		sessionID,
		sourceThreadID,
		eventID,
		sequence,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(
			t,
			deliveryID,
			sourceThreadID,
			targetThreadID,
			"",
			"sevt_"+deliveryID,
			messageJSON,
		),
	)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events
		    SET created_at = $3,
		        updated_at = $3
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND event_id = $2`,
		sessionID,
		eventID,
		createdAt,
	); err != nil {
		t.Fatalf("set completion mail creation time: %v", err)
	}
	created, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		t.Fatalf("parse completion mail creation time: %v", err)
	}
	seedAgentMailCustody(t, db, sessionID, targetThreadID, deliveryID, created)
}

func assertActiveCompletionWake(t *testing.T, db *sql.DB, sessionID string, deliveryID string, want bool) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND dedupe_key = $1
		    AND status IN ('pending', 'leased')`,
		queue.FormatRuntimeInputDedupeKey(
			workspace.ID("default"),
			sessionID,
			completionRuntimeInputID(deliveryID),
		),
	).Scan(&count); err != nil {
		t.Fatalf("count active completion wake: %v", err)
	}
	wantCount := 0
	if want {
		wantCount = 1
	}
	if count != wantCount {
		t.Fatalf("active wake for %s = %d; want %d", deliveryID, count, wantCount)
	}
}

func TestPostgreSQLCompletionMailWakeAcceptsEveryStaleRecipientArm(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, string, string)
		absent bool
	}{
		{name: "absent session", absent: true},
		{
			name: "deleted session",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, _ string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sessions SET lifecycle_state='deleted' WHERE workspace_id='default' AND id=$1`,
					sessionID,
				); err != nil {
					t.Fatalf("delete session fixture: %v", err)
				}
			},
		},
		{
			name: "terminated session",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, _ string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sessions SET status='terminated' WHERE workspace_id='default' AND id=$1`,
					sessionID,
				); err != nil {
					t.Fatalf("terminate session fixture: %v", err)
				}
			},
		},
		{
			name: "closed recipient",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, threadID string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_threads SET status='closed_for_runtime'
					  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
					sessionID,
					threadID,
				); err != nil {
					t.Fatalf("close recipient fixture: %v", err)
				}
			},
		},
		{
			name: "terminated recipient",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, threadID string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_threads SET status='terminated'
					  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
					sessionID,
					threadID,
				); err != nil {
					t.Fatalf("terminate recipient fixture: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_completion_mail_stale_" + suffix
			threadID := "thrd_completion_mail_stale_" + suffix
			if test.absent {
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO workspaces (id, type, name, created_at)
					 VALUES ('default', 'workspace', 'default', '2026-01-01T00:00:00Z')
					 ON CONFLICT (id) DO NOTHING`,
				); err != nil {
					t.Fatalf("seed absent-session workspace: %v", err)
				}
			} else {
				seedBridgeAPISession(t, admin, "default", sessionID, threadID)
				test.mutate(t, admin, sessionID, threadID)
			}
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			plan, err := store.PrepareRuntimeCommand(context.Background(),
				completionMailRuntimeJob(sessionID, threadID, "agent_mail:delivery_stale_"+suffix))
			if err != nil {
				t.Fatalf("prepare stale completion-mail wake: %v", err)
			}
			if !plan.StaleAccepted || plan.hasCommand() {
				t.Fatalf("stale completion-mail plan = %#v; want accepted no-op", plan)
			}
		})
	}
}

func TestPostgreSQLCompletionMailFinalizationRechecksTerminalRecipientFences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, string, string)
	}{
		{
			name: "session terminates after prepare",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, _ string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sessions SET status='terminated' WHERE workspace_id='default' AND id=$1`,
					sessionID,
				); err != nil {
					t.Fatalf("terminate session after prepare: %v", err)
				}
			},
		},
		{
			name: "recipient closes after prepare",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, threadID string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_threads SET status='closed_for_runtime'
					  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
					sessionID,
					threadID,
				); err != nil {
					t.Fatalf("close recipient after prepare: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_completion_mail_finalize_stale_" + suffix
			threadID := "thrd_completion_mail_finalize_stale_" + suffix
			childID := "thrd_completion_mail_finalize_stale_child_" + suffix
			deliveryID := "delivery_completion_mail_finalize_stale_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, childID)
			messageJSON := bridgeRuntimeNotificationMessageJSON(
				t,
				sessionID,
				"msg_completion_mail_finalize_stale_"+suffix,
				completionMailEnvelope("main", "task_"+childID, "completion"),
			)
			seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_completion_mail_finalize_stale_sent_"+suffix, 1,
				"agent.thread_message_sent",
				bridgeInterAgentSentEventJSON(t, deliveryID, childID, threadID, "", "sevt_completion_mail_finalize_stale_"+suffix, messageJSON))
			seedAgentMailCustody(t, admin, sessionID, threadID, deliveryID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_"+suffix, 1, "pod_"+suffix)
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
			job := completionMailRuntimeJob(
				sessionID,
				threadID,
				"agent_mail:"+deliveryID,
			)

			plan, err := store.PrepareRuntimeCommand(context.Background(), job)
			if err != nil || plan.AcceptAgentMail == nil || plan.StaleAccepted {
				t.Fatalf("prepare completion-mail race fixture = %#v/%v; want live request", plan, err)
			}
			test.mutate(t, admin, sessionID, threadID)
			exhaustion := runtimeDeliveryResultWithAttemptedBinding(retryableExhaustionResult(), plan.AttemptedBinding)

			for attempt := 0; attempt < 2; attempt++ {
				finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, exhaustion)
				if err != nil || finalized.Status != RuntimeDeliveryAccepted {
					t.Fatalf("finalize stale completion mail attempt %d = %#v/%v; want accepted no-op", attempt, finalized, err)
				}
			}
			var errorEvents int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`,
				sessionID,
			).Scan(&errorEvents); err != nil {
				t.Fatalf("count stale finalization errors: %v", err)
			}
			if errorEvents != 0 {
				t.Fatalf("stale completion-mail finalization errors = %d; want 0", errorEvents)
			}
			assertActiveCompletionWake(t, admin, sessionID, deliveryID, true)
		})
	}
}

func TestPostgreSQLAgentMailPrepareLocksSessionBeforeInbox(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_agent_mail_lock_order"
		mainID    = "thrd_agent_mail_lock_order_main"
		childID   = "thrd_agent_mail_lock_order_child"
		bindingID = "bind_agent_mail_lock_order"
		podUID    = "pod_agent_mail_lock_order"
		delivery  = "delivery_agent_mail_lock_order"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:00Z")
	seedAgentMailCustody(t, admin, sessionID, mainID, delivery, now)

	blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`,
		sessionID,
	)
	defer func() { _ = blocker.Rollback() }()
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now }
	type prepareResult struct {
		plan RuntimeCommandPlan
		err  error
	}
	results := make(chan prepareResult, 1)
	go func() {
		plan, err := store.PrepareRuntimeCommand(context.Background(), completionMailRuntimeJob(
			sessionID,
			mainID,
			completionRuntimeInputID(delivery),
		))
		results <- prepareResult{plan: plan, err: err}
	}()
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
	var lockedInputID string
	probeErr := admin.QueryRowContext(context.Background(),
		`SELECT runtime_input_id
		   FROM session_runtime_inbox
		  WHERE workspace_id='default' AND runtime_input_id=$1
		  FOR UPDATE NOWAIT`,
		completionRuntimeInputID(delivery),
	).Scan(&lockedInputID)
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release session lock-order fence: %v", err)
	}
	result := <-results
	if probeErr != nil {
		t.Fatalf("agent-mail prepare locked inbox before session: %v", probeErr)
	}
	if lockedInputID != completionRuntimeInputID(delivery) {
		t.Fatalf("lock-order probe input = %q; want %q", lockedInputID, completionRuntimeInputID(delivery))
	}
	if result.err != nil || result.plan.AcceptAgentMail == nil {
		t.Fatalf("prepare after lock-order probe = %#v/%v; want Runtime command", result.plan, result.err)
	}
}

func completionMailRuntimeJob(sessionID string, threadID string, runtimeInputID string) RuntimeJob {
	return RuntimeJob{
		JobID:           "qjob_" + runtimeInputID,
		LeaseToken:      "lease_" + runtimeInputID,
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       sessionID,
		SessionThreadID: threadID,
		RuntimeInputID:  runtimeInputID,
		InputKind:       "agent_mail",
		PayloadJSON:     "{}",
	}
}
