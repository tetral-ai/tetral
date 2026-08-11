package agentruntimebridge

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLInputCommitAndRuntimeTerminationSerializeAtSessionBoundary(t *testing.T) {
	for _, commitFirst := range []bool{true, false} {
		name := "input_commit_first"
		if !commitFirst {
			name = "runtime_termination_first"
		}
		t.Run(name, func(t *testing.T) {
			runInputCommitTerminationRace(t, commitFirst)
		})
	}
}

func runInputCommitTerminationRace(t *testing.T, commitFirst bool) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_input_termination_race"
		threadID  = "thr_input_termination_race"
		bindingID = "bind_input_termination_race"
		podUID    = "pod_input_termination_race"
		inputID   = "rin_input_termination_race"
		eventID   = "evt_input_termination_race"
		turnID    = "rwrite_input_termination_race"
	)
	now := time.Date(2026, 8, 11, 11, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"race input"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, inputID, "messages", `["evt_input_termination_race"]`, "accepted", bindingID, podUID, 1, 1)
	seedBridgeAPIOpenDurableTurn(t, admin, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), turnID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_status (
		workspace_id,session_id,status,running_since,active_seconds_total,binding_id,binding_generation,created_at,updated_at
	) VALUES ('default',$1,'running',$3,0,$2,1,$3,$3)`, sessionID, bindingID, now); err != nil {
		t.Fatalf("seed running Runtime status: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	request, err := lostRuntimeInputEnqueueRequest("default", sessionID, runtimePodLostAcceptedInput{
		SessionThreadID: threadID, RuntimeInputID: inputID, InputKind: "messages", EventIDsJSON: `["` + eventID + `"]`,
		SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true},
	}, now)
	if err != nil {
		t.Fatalf("build accepted input Queue lineage: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatalf("enqueue accepted input Queue lineage: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-input-termination-race",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease accepted input Queue lineage = %#v, %v; want one", leased, err)
	}
	if acked, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(2 * time.Second),
	}); err != nil || !acked {
		t.Fatalf("ACK accepted input Queue lineage = %t, %v; want true", acked, err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return now.Add(3 * time.Second) }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	commitRequest := &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: inputID, InputKind: "messages",
		EventIds: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
		MessageCreates: []*bridgev1.RuntimeMessageCreate{
			bridgeUserInputCreateForTest("default", sessionID, threadID, inputID, eventID, "race input"),
		},
	}
	terminationRequest := &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: turnID,
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
	}
	blocker, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin Session lock blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var lockedSession string
	if err := blocker.QueryRow(`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID).Scan(&lockedSession); err != nil {
		t.Fatalf("hold Session lock: %v", err)
	}
	var blockerPID int
	if err := blocker.QueryRow(`SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read Session blocker pid: %v", err)
	}
	type result struct {
		kind        string
		commit      *bridgev1.CommitInputsResponse
		termination *bridgev1.CommitRuntimeTerminationResponse
		err         error
	}
	results := make(chan result, 2)
	startCommit := func() {
		go func() {
			response, err := store.CommitInputs(context.Background(), commitRequest)
			results <- result{kind: "commit", commit: response, err: err}
		}()
	}
	startTermination := func() {
		go func() {
			response, err := store.CommitRuntimeTermination(context.Background(), terminationRequest)
			results <- result{kind: "termination", termination: response, err: err}
		}()
	}
	if commitFirst {
		startCommit()
		waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
		startTermination()
	} else {
		startTermination()
		waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
		startCommit()
	}
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release Session lock: %v", err)
	}
	var commitResult result
	var terminationResult result
	for range 2 {
		value := <-results
		if value.kind == "commit" {
			commitResult = value
		} else {
			terminationResult = value
		}
	}
	if terminationResult.err != nil || terminationResult.termination.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("termination result = %#v/%v; want committed", terminationResult.termination, terminationResult.err)
	}
	if commitFirst {
		if commitResult.err != nil || commitResult.commit.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
			t.Fatalf("commit-first input result = %#v/%v; want committed", commitResult.commit, commitResult.err)
		}
	} else if commitResult.err == nil {
		t.Fatalf("termination-first input commit = %#v; want fenced rejection", commitResult.commit)
	}
	var messageCount, inboxCount, activeJobs int
	var inboxStatus, queueStatus, sessionStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND source_event_id=$3),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$4),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$4),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$5),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key='session:default:' || $1 AND status IN ('pending','leased')),
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1)`,
		sessionID, threadID, eventID, inputID, queued.ID,
	).Scan(&messageCount, &inboxCount, &inboxStatus, &queueStatus, &activeJobs, &sessionStatus); err != nil {
		t.Fatalf("read commit/termination winner: %v", err)
	}
	wantMessages, wantInboxStatus := 0, "cancelled"
	if commitFirst {
		wantMessages, wantInboxStatus = 1, "committed"
	}
	if messageCount != wantMessages || inboxCount != 1 || inboxStatus != wantInboxStatus || queueStatus != queue.StatusAcknowledged || activeJobs != 0 || sessionStatus != "terminated" {
		t.Fatalf("durable winner = messages %d Inbox %d/%s Queue %s active %d session %s; want %d/1/%s/acknowledged/0/terminated",
			messageCount, inboxCount, inboxStatus, queueStatus, activeJobs, sessionStatus, wantMessages, wantInboxStatus)
	}
}
