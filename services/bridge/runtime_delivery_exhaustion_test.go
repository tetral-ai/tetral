package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestRuntimeDeliveryExhaustionEventIDMatchesDatabaseDerivation(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	job := RuntimeJob{
		WorkspaceID:    "default",
		SessionID:      "sesn_exhaustion_id_parity",
		RuntimeInputID: "agent_mail:delivery_exhaustion_id_parity",
	}
	var databaseID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT 'evt_runtime_exhausted_' || substr(encode(sha256(
			convert_to($1, 'UTF8') ||
			decode('00', 'hex') ||
			convert_to($2, 'UTF8') ||
			decode('00', 'hex') ||
			convert_to($3, 'UTF8') ||
			decode('00', 'hex') ||
			convert_to('runtime_delivery_exhausted', 'UTF8')
		), 'hex'), 1, 24)`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&databaseID); err != nil {
		t.Fatalf("derive runtime delivery exhaustion event id in PostgreSQL: %v", err)
	}
	if got := runtimeDeliveryExhaustionEventID(job); got != databaseID {
		t.Fatalf("runtime delivery exhaustion event id = %q; database derivation = %q", got, databaseID)
	}
}

func TestRuntimeDeliveryExhaustionDoesNotProjectMessageOrAdvanceRequestBoundary(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_exhaustion_boundary"
		threadID  = "thr_exhaustion_boundary"
		bindingID = "bind_exhaustion_boundary"
		podUID    = "pod_exhaustion_boundary"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIRuntimeInput(
		t, admin, "default", sessionID, threadID,
		"rin_exhaustion_message", bindingID, podUID, "evt_exhaustion_message",
	)

	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.RuntimeBindingTokenHMACKey = []byte("bridge-exhaustion-boundary-key!")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	committed, err := apiStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_exhaustion_message", InputKind: "messages",
		EventIds: []string{"evt_exhaustion_message"}, SequenceFrom: 1, SequenceTo: 1,
		MessageCreates: []*bridgev1.RuntimeMessageCreate{bridgeUserInputCreateForTest(
			"default", sessionID, threadID, "rin_exhaustion_message", "evt_exhaustion_message", "hello",
		)},
	})
	if err != nil {
		t.Fatalf("commit exhaustion boundary input: %v", err)
	}
	messageSequence := committed.GetDeclaration().GetReceipts()[0].GetMessages()[0].GetMessageSequence()

	seedBridgeAPIEvent(
		t, admin, "default", sessionID, threadID,
		"evt_exhaustion_delivery", 2, "user.message", `{"content":[{"type":"text","text":"exhaust"}]}`,
	)
	seedBridgeAPIRuntimeInbox(
		t, admin, "default", sessionID, threadID,
		"rin_exhaustion_delivery", "messages", `["evt_exhaustion_delivery"]`,
		"accepted", bindingID, podUID, 2, 2,
	)
	job := exhaustionRuntimeJob(sessionID, threadID, "rin_exhaustion_delivery", "messages", []string{"evt_exhaustion_delivery"})
	job.SequenceFrom = 2
	job.SequenceTo = 2
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	if _, err := deliveryStore.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResultForBinding(bindingID, 1, podUID)); err != nil {
		t.Fatalf("finalize exhausted delivery: %v", err)
	}

	var messageCount int
	var maximumSequence int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(MAX(sequence), 0)
		   FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2`,
		sessionID, threadID,
	).Scan(&messageCount, &maximumSequence); err != nil {
		t.Fatalf("read exhaustion message boundary: %v", err)
	}
	if messageCount != 1 || maximumSequence != messageSequence {
		t.Fatalf("exhaustion messages count/max sequence = %d/%d; want 1/%d", messageCount, maximumSequence, messageSequence)
	}
	if _, err := apiStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_exhaustion_cold_load",
	}); err != nil {
		t.Fatalf("LoadContext after exhaustion: %v", err)
	}
	boundary := messageSequence
	if _, err := apiStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_exhaustion_boundary", ModelRequestId: "mreq_exhaustion_boundary",
		EventType:                     "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_exhaustion_boundary"}`,
		ContextThroughMessageSequence: &boundary, RequestKind: "agent_provider_request",
	}); err != nil {
		t.Fatalf("write Request Start at unchanged boundary: %v", err)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreConcurrentQueuedInboxFinalizationLinearizes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_exhaust_concurrent_event"
	const threadID = "thr_exhaust_concurrent_event"
	const eventID = "evt_exhaust_concurrent_event"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"type":"user.message"}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 1, 30, 0, time.UTC) }
	job := exhaustionRuntimeJob(sessionID, threadID, "rin_exhaust_concurrent_event", "messages", []string{eventID})
	seedRuntimeInboxBirthForJob(t, admin, job)
	locker, lockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2 FOR UPDATE`,
		sessionID,
		threadID,
	)
	defer func() { _ = locker.Rollback() }()

	results := startConcurrentRuntimeFinalizations(store, job, retryableExhaustionResult())
	waitForPostgreSQLLockWaiters(t, admin, lockerPID, 2)
	if err := locker.Commit(); err != nil {
		t.Fatalf("release thread finalization fence: %v", err)
	}
	for index := 0; index < 2; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent event finalization %d: %v", index, result.err)
		}
		assertExhaustedRuntimeDeliveryResult(t, result.result)
	}
	assertRuntimeExhaustionRows(t, admin, job, "dead_lettered", 1)
}

func TestPostgreSQLRuntimeDeliveryStoreExhaustionFinalizesDeliveringInbox(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_exhaust_delivering", "thr_exhaust_delivering")
	seedBridgeAPIEvent(t, admin, "default", "sesn_exhaust_delivering", "thr_exhaust_delivering", "evt_exhaust_delivering", 1, "user.message", `{"type":"user.message"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_exhaust_delivering", "thr_exhaust_delivering", "rin_exhaust_delivering", "messages", `["evt_exhaust_delivering"]`, "delivering", "bind_exhaust_delivering", "pod_exhaust_delivering", 1, 1)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	job := exhaustionRuntimeJob("sesn_exhaust_delivering", "thr_exhaust_delivering", "rin_exhaust_delivering", "messages", []string{"evt_exhaust_delivering"})

	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResultForBinding("bind_exhaust_delivering", 1, "pod_exhaust_delivering"))
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery: %v", err)
	}
	assertExhaustedRuntimeDeliveryResult(t, result)
	assertRuntimeExhaustionRows(t, admin, job, "dead_lettered", 1)
}

func TestPostgreSQLRuntimeDeliveryStoreExhaustionCancelledBeforeTransactionWritesNothing(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_exhaust_cancelled", "thr_exhaust_cancelled")
	seedBridgeAPIEvent(t, admin, "default", "sesn_exhaust_cancelled", "thr_exhaust_cancelled", "evt_exhaust_cancelled", 1, "user.message", `{"type":"user.message"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_exhaust_cancelled", "thr_exhaust_cancelled", "rin_exhaust_cancelled", "messages", `["evt_exhaust_cancelled"]`, "accepted", "bind_exhaust_cancelled", "pod_exhaust_cancelled", 1, 1)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	job := exhaustionRuntimeJob("sesn_exhaust_cancelled", "thr_exhaust_cancelled", "rin_exhaust_cancelled", "messages", []string{"evt_exhaust_cancelled"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.FinalizeRuntimeDelivery(ctx, job, retryableExhaustionResult()); err == nil {
		t.Fatal("FinalizeRuntimeDelivery succeeded with cancelled context")
	}
	var inboxStatus string
	var processedAt sql.NullString
	var errorsCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read cancelled finalization inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT processed_at FROM session_events WHERE workspace_id='default' AND event_id=$1`, job.EventIDs[0]).Scan(&processedAt); err != nil {
		t.Fatalf("read cancelled finalization event: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, job.SessionID).Scan(&errorsCount); err != nil {
		t.Fatalf("count cancelled finalization errors: %v", err)
	}
	if inboxStatus != "accepted" || processedAt.Valid || errorsCount != 0 {
		t.Fatalf("cancelled finalization inbox=%q processed=%v errors=%d; want recoverable accepted/unprocessed/no-error", inboxStatus, processedAt.Valid, errorsCount)
	}
}

func TestPostgreSQLJobRunnerExhaustionCrashWindowsConvergeAcrossDatabases(t *testing.T) {
	t.Run("failure before Bridge transaction leaves Queue lease recoverable", func(t *testing.T) {
		bridgeRuntime, bridgeAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		job := seedCrossDatabaseExhaustionFixture(t, bridgeAdmin, "before_bridge", true)
		bridgeStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(bridgeRuntime), 9090)
		queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
		leased := enqueueAndLeaseExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 1, 59, 0, 0, time.UTC))
		job.JobID = leased.ID
		job.LeaseToken = leased.LeaseToken
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := bridgeStore.FinalizeRuntimeDelivery(ctx, job, retryableExhaustionResult()); err == nil {
			t.Fatal("FinalizeRuntimeDelivery before-transaction fault succeeded")
		}
		var inboxStatus string
		var processedAt sql.NullString
		var errorsCount int
		if err := bridgeAdmin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
			t.Fatalf("read pre-transaction inbox: %v", err)
		}
		if err := bridgeAdmin.QueryRowContext(context.Background(), `SELECT processed_at FROM session_events WHERE workspace_id='default' AND event_id=$1`, job.EventIDs[0]).Scan(&processedAt); err != nil {
			t.Fatalf("read pre-transaction event: %v", err)
		}
		if err := bridgeAdmin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, job.SessionID).Scan(&errorsCount); err != nil {
			t.Fatalf("count pre-transaction errors: %v", err)
		}
		var queueStatus string
		var attemptCount int
		var maxAttempts int
		if err := queueAdmin.QueryRowContext(context.Background(), `SELECT status, attempt_count, max_attempts FROM queue_jobs WHERE workspace_id='default' AND id=$1`, job.JobID).Scan(&queueStatus, &attemptCount, &maxAttempts); err != nil {
			t.Fatalf("read recoverable Queue lease: %v", err)
		}
		if inboxStatus != "queued" || processedAt.Valid || errorsCount != 0 || queueStatus != queue.StatusLeased || attemptCount != 1 || maxAttempts != 1 {
			t.Fatalf("pre-transaction Bridge/Queue state inbox=%q processed=%v errors=%d queue=%q attempts=%d/%d; want queued/unprocessed/zero/leased/1/1", inboxStatus, processedAt.Valid, errorsCount, queueStatus, attemptCount, maxAttempts)
		}
	})

	t.Run("Bridge commit before Queue dead-letter", func(t *testing.T) {
		for _, arm := range []struct {
			name      string
			wantInbox string
		}{
			{name: "queued inbox", wantInbox: "dead_lettered"},
		} {
			t.Run(arm.name, func(t *testing.T) {
				bridgeRuntime, bridgeAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
				queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
				suffix := strings.NewReplacer("-", "_", " ", "_").Replace(arm.name)
				job := seedCrossDatabaseExhaustionFixture(t, bridgeAdmin, suffix, true)
				bridgeStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(bridgeRuntime), 9090)
				bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC) }
				queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
				leased := enqueueAndLeaseExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC))
				if _, err := queueAdmin.ExecContext(context.Background(), `UPDATE queue_jobs
					SET max_attempts=2,
					    leased_until=clock_timestamp() - interval '1 millisecond'
				  WHERE workspace_id='default' AND id=$1`, leased.ID); err != nil {
					t.Fatalf("extend Queue attempt bound for real reclaim: %v", err)
				}
				job.JobID = leased.ID
				job.LeaseToken = leased.LeaseToken
				job.AttemptCount = 2
				job.MaxAttempts = 2

				if _, err := bridgeStore.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult()); err != nil {
					t.Fatalf("commit Bridge exhaustion fence: %v", err)
				}
				reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
					WorkspaceID: workspace.DefaultID,
					Kind:        queue.KindRuntimeInput,
					Limit:       1,
				})
				if err != nil || reclaimed != 1 {
					t.Fatalf("reclaim expired Queue lease = %d/%v; want one", reclaimed, err)
				}
				deliverer := &postgresFinalizingDeliverer{store: bridgeStore, result: retryableExhaustionResult()}
				queueServer := tetralqueue.NewServer(queueStore, nil)
				runner := &JobRunner{Queue: queueServer, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}

				if err := runner.RunOnce(context.Background()); err != nil {
					t.Fatalf("RunOnce after Bridge crash boundary: %v", err)
				}
				if deliverer.deliveries != 0 {
					t.Fatalf("Runtime deliveries after Bridge fence = %d; want zero", deliverer.deliveries)
				}
				assertCrossDatabaseExhaustionConverged(t, bridgeAdmin, queueAdmin, bridgeStore, job, arm.wantInbox)
			})
		}
	})

	t.Run("Queue dead-letter commit with response loss", func(t *testing.T) {
		bridgeRuntime, bridgeAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		job := seedCrossDatabaseExhaustionFixture(t, bridgeAdmin, "queue_response_loss", true)
		bridgeStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(bridgeRuntime), 9090)
		bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 2, 1, 0, 0, time.UTC) }
		queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
		enqueueExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 2, 1, 0, 0, time.UTC))
		deliverer := &postgresFinalizingDeliverer{store: bridgeStore, result: retryableExhaustionResult()}
		queueClient := &deadLetterResponseLossQueueClient{QueueClient: tetralqueue.NewServer(queueStore, nil)}
		runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}

		if err := runner.RunOnce(context.Background()); err == nil || !errors.Is(err, errSyntheticQueueResponseLoss) {
			t.Fatalf("RunOnce error = %v; want synthetic post-commit response loss", err)
		}
		if deliverer.deliveries != 1 {
			t.Fatalf("Runtime deliveries before Queue response loss = %d; want one", deliverer.deliveries)
		}
		job.JobID = queueClient.deadLetteredJobID
		job.MaxAttempts = 1
		assertCrossDatabaseExhaustionConverged(t, bridgeAdmin, queueAdmin, bridgeStore, job, "dead_lettered")
		if err := runner.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce after terminal Queue row: %v", err)
		}
		if deliverer.deliveries != 1 {
			t.Fatalf("Runtime deliveries after terminal Queue row = %d; want unchanged one", deliverer.deliveries)
		}
	})
}

func TestPostgreSQLJobRunnerInvalidRuntimeCustodyDeadLettersQueueWithoutBridgeMutation(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *sql.DB, *queue.PostgreSQLQueueStore, time.Time) (string, string)
		check func(*testing.T, *sql.DB)
	}{
		{
			name: "missing generic Inbox",
			setup: func(t *testing.T, admin *sql.DB, queueStore *queue.PostgreSQLQueueStore, now time.Time) (string, string) {
				job := seedCrossDatabaseExhaustionFixture(t, admin, "missing_generic", false)
				queued := enqueueExhaustionJob(t, queueStore, job, now)
				return job.SessionID, queued.ID
			},
			check: func(t *testing.T, admin *sql.DB) {
				assertRuntimeSourceFactsUnchanged(t, admin, "sesn_cross_missing_generic", []string{"evt_cross_missing_generic"}, "rin_cross_missing_generic", false)
			},
		},
		{
			name: "conflicting generic Inbox",
			setup: func(t *testing.T, admin *sql.DB, queueStore *queue.PostgreSQLQueueStore, now time.Time) (string, string) {
				const (
					sessionID = "sesn_invalid_generic_conflict"
					threadID  = "thr_invalid_generic_conflict"
				)
				seedBridgeAPISession(t, admin, "default", sessionID, threadID)
				seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_inbox_generic_conflict", 1, "user.message", `{"type":"user.message"}`)
				seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_queue_generic_conflict", 2, "user.message", `{"type":"user.message"}`)
				inboxJob := exhaustionRuntimeJob(sessionID, threadID, "rin_invalid_generic_conflict", "messages", []string{"evt_inbox_generic_conflict"})
				seedRuntimeInboxBirthForJob(t, admin, inboxJob)
				queueJob := exhaustionRuntimeJob(sessionID, threadID, inboxJob.RuntimeInputID, "messages", []string{"evt_queue_generic_conflict"})
				queueJob.SequenceFrom, queueJob.SequenceTo = 2, 2
				payload := fmt.Sprintf(`{"workspace_id":"default","session_id":%q,"session_thread_id":%q,"runtime_input_id":%q,"event_ids":[%q],"sequence_from":2,"sequence_to":2,"input_kind":"messages"}`, sessionID, threadID, queueJob.RuntimeInputID, queueJob.EventIDs[0])
				queued, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
					WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
					PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
					DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, queueJob.RuntimeInputID),
					PayloadVersion: 1, PayloadJSON: []byte(payload), MaxAttempts: 1, Now: now,
				})
				if err != nil {
					t.Fatalf("enqueue conflicting generic job: %v", err)
				}
				return sessionID, queued.ID
			},
			check: func(t *testing.T, admin *sql.DB) {
				assertRuntimeSourceFactsUnchanged(t, admin, "sesn_invalid_generic_conflict", []string{"evt_inbox_generic_conflict", "evt_queue_generic_conflict"}, "rin_invalid_generic_conflict", true)
				var queuedJobs int
				var eventIDsJSON string
				if err := admin.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(max(payload_json::jsonb ->> 'event_ids'), '')
					FROM queue_jobs WHERE workspace_id='default' AND status=$1
					AND payload_json::jsonb ->> 'runtime_input_id'='rin_invalid_generic_conflict'`, queue.StatusPending).Scan(&queuedJobs, &eventIDsJSON); err != nil {
					t.Fatalf("read recreated canonical Queue custody: %v", err)
				}
				if queuedJobs != 1 || eventIDsJSON != `["evt_inbox_generic_conflict"]` {
					t.Fatalf("recreated Queue custody = %d/%s; want one canonical Inbox payload", queuedJobs, eventIDsJSON)
				}
			},
		},
		{
			name: "missing agent mail Inbox",
			setup: func(t *testing.T, admin *sql.DB, queueStore *queue.PostgreSQLQueueStore, now time.Time) (string, string) {
				const (
					sessionID = "sesn_invalid_missing_mail"
					mainID    = "thr_invalid_missing_mail_main"
					childID   = "thr_invalid_missing_mail_child"
					delivery  = "delivery_invalid_missing_mail"
				)
				seedBridgeAPISession(t, admin, "default", sessionID, mainID)
				seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
				messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_invalid_missing_mail", completionMailEnvelope("main", "task_child", "done"))
				seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_invalid_missing_mail", 1, "agent.thread_message_sent",
					bridgeInterAgentSentEventJSON(t, delivery, childID, mainID, "", "sevt_invalid_missing_mail", messageJSON))
				request, _, err := agentMailWakeEnqueueRequest("default", sessionID, mainID, delivery, now)
				if err != nil {
					t.Fatalf("build missing agent-mail Queue custody: %v", err)
				}
				queued, err := queueStore.Enqueue(context.Background(), request)
				if err != nil {
					t.Fatalf("enqueue missing agent-mail custody: %v", err)
				}
				return sessionID, queued.ID
			},
			check: func(t *testing.T, admin *sql.DB) {
				assertRuntimeSourceFactsUnchanged(t, admin, "sesn_invalid_missing_mail", []string{"evt_invalid_missing_mail"}, "agent_mail:delivery_invalid_missing_mail", false)
			},
		},
		{
			name: "missing task notification Inbox",
			setup: func(t *testing.T, admin *sql.DB, queueStore *queue.PostgreSQLQueueStore, now time.Time) (string, string) {
				const (
					sessionID = "sesn_invalid_missing_task"
					threadID  = "thr_invalid_missing_task"
					taskID    = "task_invalid_missing_task"
					sourceID  = "sevt_invalid_missing_task"
				)
				seedBridgeAPISession(t, admin, "default", sessionID, threadID)
				seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_invalid_missing_task", 1, "pod_invalid_missing_task")
				seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, "bind_invalid_missing_task", taskID, sourceID)
				settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
				request, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, threadID, taskID, now)
				if err != nil {
					t.Fatalf("build missing task-notification Queue custody: %v", err)
				}
				queued, err := queueStore.Enqueue(context.Background(), request)
				if err != nil {
					t.Fatalf("enqueue missing task-notification custody: %v", err)
				}
				return sessionID, queued.ID
			},
			check: func(t *testing.T, admin *sql.DB) {
				assertRuntimeSourceFactsUnchanged(t, admin, "sesn_invalid_missing_task", []string{"sevt_invalid_missing_task"}, "task_notification:task_invalid_missing_task", false)
				var terminalEvent sql.NullString
				if err := admin.QueryRowContext(context.Background(), `SELECT terminal_event_id FROM session_background_tasks
					WHERE workspace_id='default' AND session_id='sesn_invalid_missing_task' AND task_id='task_invalid_missing_task'`).Scan(&terminalEvent); err != nil {
					t.Fatalf("read missing task-notification source: %v", err)
				}
				if terminalEvent.Valid {
					t.Fatalf("missing task-notification source terminal event = %q; want unchanged empty", terminalEvent.String)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			now := time.Now().UTC().Add(-time.Second)
			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			sessionID, jobID := test.setup(t, admin, queueStore, now)
			deliverer := &postgresFinalizingDeliverer{
				store:  NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090),
				result: retryableExhaustionResult(),
			}
			runner := &JobRunner{
				Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
				Config: JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("run invalid custody job: %v", err)
			}
			var queueStatus, errorKind string
			if err := admin.QueryRowContext(context.Background(), `SELECT status, last_error_kind FROM queue_jobs
				WHERE workspace_id='default' AND id=$1`, jobID).Scan(&queueStatus, &errorKind); err != nil {
				t.Fatalf("read invalid custody Queue disposition: %v", err)
			}
			var sessionErrors int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
				WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, sessionID).Scan(&sessionErrors); err != nil {
				t.Fatalf("count invalid custody Session errors: %v", err)
			}
			if queueStatus != queue.StatusDeadLettered || errorKind != "invalid_runtime_job_payload" || sessionErrors != 0 || deliverer.deliveries != 0 {
				t.Fatalf("invalid custody disposition = Queue %s/%s Session errors %d Runtime calls %d; want dead_lettered/invalid_runtime_job_payload/0/0", queueStatus, errorKind, sessionErrors, deliverer.deliveries)
			}
			test.check(t, admin)
		})
	}
}

func TestPostgreSQLCompletionMailProducerAndJobRunnerTerminalizeQueuedInbox(t *testing.T) {
	const suffix = "queued_exhaustion"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPIChildFinishIdleFailureFixture(t, admin, suffix)
	producer := completionMailTestStore(t, runtime)
	request := bridgeAPIChildFinishIdleFailureRequest(suffix)
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, producer, request); err != nil {
		t.Fatalf("produce completion mail custody: %v", err)
	}
	sessionID := completionTestSessionID(suffix)
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_runtime_bindings
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("remove Runtime target before first delivery: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET max_attempts=1, available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND kind='runtime_input'
		AND payload_json::jsonb ->> 'session_id'=$1
		AND payload_json::jsonb ->> 'input_kind'='agent_mail'`, sessionID); err != nil {
		t.Fatalf("configure final completion-mail attempt: %v", err)
	}
	var sentProcessedBefore sql.NullTime
	if err := admin.QueryRowContext(context.Background(), `SELECT processed_at FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`, sessionID).Scan(&sentProcessedBefore); err != nil {
		t.Fatalf("read completion-mail source before exhaustion: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: manifestCompositionDeliverer{direct: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}},
		Config:    JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run final completion-mail attempt: %v", err)
	}
	var inboxStatus, queueStatus, errorKind string
	var sessionErrors, liveInbox int
	var sentProcessed sql.NullTime
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_input'
		  AND payload_json::jsonb ->> 'session_id'=$1 AND payload_json::jsonb ->> 'input_kind'='agent_mail'),
		(SELECT last_error_kind FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_input'
		  AND payload_json::jsonb ->> 'session_id'=$1 AND payload_json::jsonb ->> 'input_kind'='agent_mail'),
		(SELECT processed_at FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1
		  AND status IN ('queued','delivering','accepted','parked'))`, sessionID).Scan(
		&inboxStatus, &queueStatus, &errorKind, &sentProcessed, &sessionErrors, &liveInbox,
	); err != nil {
		t.Fatalf("read completion-mail exhaustion settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || errorKind != "runtime_delivery_exhausted" ||
		sentProcessed.Valid != sentProcessedBefore.Valid || (sentProcessed.Valid && !sentProcessed.Time.Equal(sentProcessedBefore.Time)) ||
		sessionErrors != 1 || liveInbox != 0 || len(sender.requests) != 0 {
		t.Fatalf("completion-mail settlement = Inbox %s Queue %s/%s sent processed %v errors %d live %d Runtime calls %d",
			inboxStatus, queueStatus, errorKind, sentProcessed.Valid, sessionErrors, liveInbox, len(sender.requests))
	}
}

func assertRuntimeSourceFactsUnchanged(t *testing.T, db *sql.DB, sessionID string, eventIDs []string, runtimeInputID string, wantInbox bool) {
	t.Helper()
	for _, eventID := range eventIDs {
		var processedAt sql.NullTime
		if err := db.QueryRowContext(context.Background(), `SELECT processed_at FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, eventID).Scan(&processedAt); err != nil {
			t.Fatalf("read unchanged source event %s: %v", eventID, err)
		}
		if processedAt.Valid {
			t.Fatalf("source event %s processed at %v; want unchanged", eventID, processedAt.Time)
		}
	}
	var inboxCount int
	var inboxStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(max(status), '') FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).Scan(&inboxCount, &inboxStatus); err != nil {
		t.Fatalf("read unchanged Inbox custody: %v", err)
	}
	wantCount := 0
	if wantInbox {
		wantCount = 1
	}
	if inboxCount != wantCount || (wantInbox && inboxStatus != "queued") {
		t.Fatalf("unchanged Inbox = %d/%s; want %d/queued", inboxCount, inboxStatus, wantCount)
	}
}

func exhaustionRuntimeJob(sessionID string, threadID string, runtimeInputID string, inputKind string, eventIDs []string) RuntimeJob {
	return RuntimeJob{
		JobID:           "qjob_" + runtimeInputID,
		LeaseToken:      "lease_" + runtimeInputID,
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       sessionID,
		SessionThreadID: threadID,
		RuntimeInputID:  runtimeInputID,
		EventIDs:        append([]string(nil), eventIDs...),
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       inputKind,
		AttemptCount:    2,
		MaxAttempts:     2,
	}
}

func retryableExhaustionResult() RuntimeDeliveryResult {
	return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy", ErrorMessage: "runtime busy"}
}

func retryableExhaustionResultForBinding(bindingID string, bindingGeneration int64, podUID string) RuntimeDeliveryResult {
	result := retryableExhaustionResult()
	result.AttemptedBindingID = bindingID
	result.AttemptedBindingGeneration = bindingGeneration
	result.AttemptedTargetPodUID = podUID
	return result
}

func assertExhaustedRuntimeDeliveryResult(t *testing.T, result RuntimeDeliveryResult) {
	t.Helper()
	if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("finalized result = %#v; want terminal runtime_delivery_exhausted", result)
	}
}

func assertRuntimeExhaustionRows(t *testing.T, db *sql.DB, job RuntimeJob, inboxStatus string, wantErrors int) {
	t.Helper()
	var inboxCount int
	var storedInboxStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(MAX(status), '') FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2`, job.WorkspaceID, job.RuntimeInputID).Scan(&inboxCount, &storedInboxStatus); err != nil {
		t.Fatalf("read exhaustion inbox: %v", err)
	}
	if (inboxStatus == "" && inboxCount != 0) || (inboxStatus != "" && (inboxCount != 1 || storedInboxStatus != inboxStatus)) {
		t.Fatalf("exhaustion inbox count/status = %d/%q; want %q", inboxCount, storedInboxStatus, inboxStatus)
	}
	var errorCount int
	var errorType string
	var retryStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(MAX(payload_json::jsonb #>> '{error,type}'), ''), COALESCE(MAX(payload_json::jsonb #>> '{error,retry_status,type}'), '') FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type='session.error'`, job.WorkspaceID, job.SessionID).Scan(&errorCount, &errorType, &retryStatus); err != nil {
		t.Fatalf("read exhaustion errors: %v", err)
	}
	if errorCount != wantErrors || errorType != "unknown_error" || retryStatus != "exhausted" {
		t.Fatalf("exhaustion errors count/type/retry = %d/%q/%q; want %d/unknown_error/exhausted", errorCount, errorType, retryStatus, wantErrors)
	}
	var errorStreamChanges int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_event_stream_changes changes JOIN session_events event ON event.workspace_id=changes.workspace_id AND event.event_id=changes.event_id WHERE event.workspace_id=$1 AND event.session_id=$2 AND event.type='session.error'`, job.WorkspaceID, job.SessionID).Scan(&errorStreamChanges); err != nil {
		t.Fatalf("count exhaustion error stream changes: %v", err)
	}
	if errorStreamChanges != wantErrors {
		t.Fatalf("exhaustion error stream changes = %d; want %d", errorStreamChanges, wantErrors)
	}
	for _, eventID := range job.EventIDs {
		var processedAt sql.NullString
		var revision int64
		if err := db.QueryRowContext(context.Background(), `SELECT processed_at, revision FROM session_events WHERE workspace_id=$1 AND event_id=$2`, job.WorkspaceID, eventID).Scan(&processedAt, &revision); err != nil {
			t.Fatalf("read exhaustion input event %s: %v", eventID, err)
		}
		if !processedAt.Valid || revision != 2 {
			t.Fatalf("exhaustion input event %s processed=%v revision=%d; want true/2", eventID, processedAt.Valid, revision)
		}
		var processingChanges int
		if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_event_stream_changes WHERE workspace_id=$1 AND event_id=$2 AND revision=2`, job.WorkspaceID, eventID).Scan(&processingChanges); err != nil {
			t.Fatalf("count exhaustion input stream changes %s: %v", eventID, err)
		}
		if processingChanges != 1 {
			t.Fatalf("exhaustion input stream changes %s = %d; want one revision-2 change", eventID, processingChanges)
		}
	}
	var runtimeStatusRows int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_status WHERE workspace_id=$1 AND session_id=$2`, job.WorkspaceID, job.SessionID).Scan(&runtimeStatusRows); err != nil {
		t.Fatalf("count exhaustion runtime status rows: %v", err)
	}
	if runtimeStatusRows != 0 {
		t.Fatalf("exhaustion runtime status rows = %d; want no status mutation", runtimeStatusRows)
	}
	var exhaustionMessageCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id=$1 AND source_event_id=$2`,
		job.WorkspaceID, runtimeDeliveryExhaustionEventID(job),
	).Scan(&exhaustionMessageCount); err != nil {
		t.Fatalf("count exhaustion message projections: %v", err)
	}
	if exhaustionMessageCount != 0 {
		t.Fatalf("exhaustion message projections = %d; want 0", exhaustionMessageCount)
	}
}

type postgresFinalizingDeliverer struct {
	store      *PostgreSQLRuntimeDeliveryStore
	result     RuntimeDeliveryResult
	deliveries int
}

type concurrentRuntimeFinalizationResult struct {
	result RuntimeDeliveryResult
	err    error
}

func startConcurrentRuntimeFinalizations(store *PostgreSQLRuntimeDeliveryStore, job RuntimeJob, result RuntimeDeliveryResult) <-chan concurrentRuntimeFinalizationResult {
	start := make(chan struct{})
	results := make(chan concurrentRuntimeFinalizationResult, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, result)
			results <- concurrentRuntimeFinalizationResult{result: finalized, err: err}
		}()
	}
	close(start)
	return results
}

func lockPostgreSQLFinalizationFence(t *testing.T, db *sql.DB, query string, args ...any) (*sql.Tx, int) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin finalization fence transaction: %v", err)
	}
	var lockedID string
	if err := tx.QueryRowContext(context.Background(), query, args...).Scan(&lockedID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock finalization fence row: %v", err)
	}
	var backendPID int
	if err := tx.QueryRowContext(context.Background(), `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("read finalization fence backend: %v", err)
	}
	return tx, backendPID
}

func waitForPostgreSQLLockWaiters(t *testing.T, db *sql.DB, blockerPID int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		if err := db.QueryRowContext(context.Background(),
			`WITH RECURSIVE waiters(pid) AS (
				SELECT pid
				  FROM pg_stat_activity
				 WHERE $1 = ANY(pg_blocking_pids(pid))
				UNION
				SELECT activity.pid
				  FROM pg_stat_activity activity
				  JOIN waiters blocker ON blocker.pid = ANY(pg_blocking_pids(activity.pid))
			)
			SELECT count(*) FROM waiters`,
			blockerPID,
		).Scan(&waiters); err != nil {
			t.Fatalf("read PostgreSQL finalization waiters: %v", err)
		}
		if waiters >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PostgreSQL finalization waiters did not reach %d for blocker %d", want, blockerPID)
}

func (d *postgresFinalizingDeliverer) DeliverRuntimeJob(context.Context, RuntimeJob) (RuntimeDeliveryResult, error) {
	d.deliveries++
	return d.result, nil
}

func (d *postgresFinalizingDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	return d.store.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d *postgresFinalizingDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.store.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d *postgresFinalizingDeliverer) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	return d.store.ReplaceMalformedRuntimeInputCustody(ctx, job)
}

var errSyntheticQueueResponseLoss = errors.New("synthetic queue response loss")

type deadLetterResponseLossQueueClient struct {
	QueueClient
	deadLetteredJobID string
}

func (c *deadLetterResponseLossQueueClient) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	response, err := c.QueueClient.DeadLetter(ctx, request)
	if err != nil {
		return response, err
	}
	c.deadLetteredJobID = request.GetJobId()
	return nil, errSyntheticQueueResponseLoss
}

func seedCrossDatabaseExhaustionFixture(t *testing.T, bridgeAdmin *sql.DB, suffix string, existingInbox bool) RuntimeJob {
	t.Helper()
	sessionID := "sesn_cross_" + suffix
	threadID := "thr_cross_" + suffix
	runtimeInputID := "rin_cross_" + suffix
	eventID := "evt_cross_" + suffix
	seedBridgeAPISession(t, bridgeAdmin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, bridgeAdmin, "default", sessionID, threadID, eventID, 1, "user.message", `{"type":"user.message"}`)
	if existingInbox {
		job := exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "messages", []string{eventID})
		seedRuntimeInboxBirthForJob(t, bridgeAdmin, job)
	}
	return exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "messages", []string{eventID})
}

func enqueueExhaustionJob(t *testing.T, store *queue.PostgreSQLQueueStore, job RuntimeJob, now time.Time) *queue.Job {
	t.Helper()
	payload := fmt.Sprintf(`{"workspace_id":"default","session_id":%q,"session_thread_id":%q,"runtime_input_id":%q,"event_ids":[%q],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`, job.SessionID, job.SessionThreadID, job.RuntimeInputID, job.EventIDs[0])
	queued, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID:    workspace.DefaultID,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, job.SessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, job.SessionID, job.RuntimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(payload),
		MaxAttempts:    1,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("enqueue exhaustion job: %v", err)
	}
	return queued
}

func enqueueAndLeaseExhaustionJob(t *testing.T, store *queue.PostgreSQLQueueStore, job RuntimeJob, now time.Time) *queue.Job {
	t.Helper()
	enqueueExhaustionJob(t, store, job, now)
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.DefaultID,
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "bridge-test",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease exhaustion job = %d/%v; want one", len(leased), err)
	}
	return leased[0]
}

func queueJobProto(job *queue.Job) *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:             job.ID,
		WorkspaceId:    string(job.WorkspaceID),
		Kind:           job.Kind,
		PartitionKey:   job.PartitionKey,
		DedupeKey:      job.DedupeKey,
		PayloadVersion: int32(job.PayloadVersion),
		PayloadJson:    string(job.PayloadJSON),
		LeaseToken:     job.LeaseToken,
		AttemptCount:   int32(job.AttemptCount),
		MaxAttempts:    int32(job.MaxAttempts),
	}
}

func assertCrossDatabaseExhaustionConverged(t *testing.T, bridgeAdmin *sql.DB, queueAdmin *sql.DB, store *PostgreSQLRuntimeDeliveryStore, job RuntimeJob, wantInbox string) {
	t.Helper()
	assertRuntimeExhaustionRows(t, bridgeAdmin, job, wantInbox, 1)
	assertIndependentQueueExhausted(t, queueAdmin, job.JobID, int(job.MaxAttempts))
	for iteration := 0; iteration < 3; iteration++ {
		if _, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult()); err != nil {
			t.Fatalf("replay finalization %d: %v", iteration, err)
		}
	}
	assertRuntimeExhaustionRows(t, bridgeAdmin, job, wantInbox, 1)
}

func assertIndependentQueueExhausted(t *testing.T, queueAdmin *sql.DB, jobID string, wantAttempts int) {
	t.Helper()
	var queueStatus string
	var attemptCount int
	var maxAttempts int
	if err := queueAdmin.QueryRowContext(context.Background(), `SELECT status, attempt_count, max_attempts FROM queue_jobs WHERE workspace_id='default' AND id=$1`, jobID).Scan(&queueStatus, &attemptCount, &maxAttempts); err != nil {
		t.Fatalf("read independent Queue row: %v", err)
	}
	if queueStatus != queue.StatusDeadLettered || attemptCount != wantAttempts || maxAttempts != wantAttempts {
		t.Fatalf("Queue status/attempts = %q/%d/%d; want dead_lettered/%d/%d", queueStatus, attemptCount, maxAttempts, wantAttempts, wantAttempts)
	}
}
