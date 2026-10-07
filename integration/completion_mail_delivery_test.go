package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
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
	messageJSON := bridgePublicMessageJSONForTest(t, completionMailEnvelope("main", "sender", deliveryID))
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

	blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`,
		sessionID,
	)
	defer func() { _ = blocker.Rollback() }()
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return now }
	job := leaseCompletionMailRuntimeJob(t, runtime)
	type prepareResult struct {
		plan jobrunner.RuntimeCommandPlan
		err  error
	}
	results := make(chan prepareResult, 1)
	go func() {
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		results <- prepareResult{plan: plan, err: err}
	}()
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
	var lockedInputID string
	probeErr := admin.QueryRowContext(context.Background(),
		`SELECT runtime_input_id
		   FROM session_runtime_inbox
		  WHERE workspace_id='default' AND runtime_input_id=$1
		  FOR UPDATE NOWAIT`,
		runtimecontrol.CompletionRuntimeInputID(delivery),
	).Scan(&lockedInputID)
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release session lock-order fence: %v", err)
	}
	result := <-results
	if probeErr != nil {
		t.Fatalf("agent-mail prepare locked inbox before session: %v", probeErr)
	}
	if lockedInputID != runtimecontrol.CompletionRuntimeInputID(delivery) {
		t.Fatalf("lock-order probe input = %q; want %q", lockedInputID, runtimecontrol.CompletionRuntimeInputID(delivery))
	}
	if result.err != nil || result.plan.AcceptAgentMail == nil {
		t.Fatalf("prepare after lock-order probe = %#v/%v; want Runtime command", result.plan, result.err)
	}
}

func leaseCompletionMailRuntimeJob(t *testing.T, runtime *sql.DB) jobrunner.RuntimeJob {
	t.Helper()
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "completion-mail-production-test",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease completion-mail Queue custody = %#v/%v", leased, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode completion-mail Queue custody: %v", err)
	}
	return job
}
