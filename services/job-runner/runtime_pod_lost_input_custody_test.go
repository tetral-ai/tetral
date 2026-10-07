package jobrunner

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLRuntimePodLossPreservesActiveQueueCustody(t *testing.T) {
	for _, test := range []struct {
		name            string
		inboxStatus     string
		leaseJob        bool
		wantInboxStatus string
		wantQueueStatus string
		wantHandedOff   int
	}{
		{name: "accepted pending", inboxStatus: "accepted", wantInboxStatus: "queued", wantQueueStatus: queue.StatusPending, wantHandedOff: 1},
		{name: "accepted leased", inboxStatus: "accepted", leaseJob: true, wantInboxStatus: "queued", wantQueueStatus: queue.StatusPending, wantHandedOff: 1},
		{name: "delivering leased", inboxStatus: "delivering", leaseJob: true, wantInboxStatus: "delivering", wantQueueStatus: queue.StatusLeased},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			const (
				sessionID      = "sesn_pod_loss_queue_active"
				threadID       = "thr_pod_loss_queue_active"
				runtimeInputID = "rin_pod_loss_queue_active"
				bindingID      = "bind_pod_loss_queue_active"
				podUID         = "pod_pod_loss_queue_active"
				jobID          = "qjob_pod_loss_active"
			)
			now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_pod_loss_queue_active", 1, "user.message", `{"type":"user.message"}`)
			seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages", `["evt_pod_loss_queue_active"]`, test.inboxStatus, bindingID, podUID, 1, 1)

			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			request, err := runtimecontrol.RuntimeInputEnqueueRequest("default", sessionID, runtimecontrol.AcceptedRuntimeInput{
				SessionThreadID: threadID,
				RuntimeInputID:  runtimeInputID,
				InputKind:       "messages",
				EventIDsJSON:    `["evt_pod_loss_queue_active"]`,
				SequenceFrom:    sql.NullInt64{Int64: 1, Valid: true},
				SequenceTo:      sql.NullInt64{Int64: 1, Valid: true},
			}, now)
			if err != nil {
				t.Fatalf("build original Queue request: %v", err)
			}
			request.ID = jobID
			if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
				t.Fatalf("enqueue original Queue job: %v", err)
			}
			var leasedRuntimeJob RuntimeJob
			if test.leaseJob {
				leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
					WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-active-custody",
					MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
				})
				if err != nil || len(leased) != 1 || leased[0].ID != jobID {
					t.Fatalf("lease original Queue job = %#v, %v; want %s", leased, err, jobID)
				}
				leasedRuntimeJob, err = DecodeRuntimeJob(queueJobProto(leased[0]))
				if err != nil {
					t.Fatalf("decode original Queue job: %v", err)
				}
			}

			handedOff := handOffLostRuntimeInputsForTest(t, runtime, sessionID, bindingID, podUID, now.Add(2*time.Second))
			if handedOff != test.wantHandedOff {
				t.Fatalf("handed off inputs = %d; want %d", handedOff, test.wantHandedOff)
			}
			var inboxStatus, queueStatus string
			var leaseTokenValid bool
			if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status, job.status, job.lease_token IS NOT NULL
				FROM session_runtime_inbox inbox
				JOIN queue_jobs job ON job.workspace_id = inbox.workspace_id AND job.id = $2
				WHERE inbox.workspace_id = 'default' AND inbox.runtime_input_id = $1`, runtimeInputID, jobID).Scan(&inboxStatus, &queueStatus, &leaseTokenValid); err != nil {
				t.Fatalf("read post-loss custody: %v", err)
			}
			if inboxStatus != test.wantInboxStatus || queueStatus != test.wantQueueStatus {
				t.Fatalf("post-loss custody = inbox %q / Queue %q; want %q / %q", inboxStatus, queueStatus, test.wantInboxStatus, test.wantQueueStatus)
			}
			if test.inboxStatus == "accepted" && leaseTokenValid {
				t.Fatal("reclaimed accepted input retained a stale Queue lease")
			}
			if test.name == "accepted leased" {
				staleAttempt := retryableExhaustionResultForBinding(bindingID, 1, podUID)
				deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
				if _, err := deliveryStore.FinalizeRuntimeDelivery(context.Background(), leasedRuntimeJob, staleAttempt); !invalidRuntimeJobPayload(err) {
					t.Fatalf("stale finalization error = %v; want invalid_runtime_job_payload", err)
				}
				var exhaustionEvents int
				if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
					WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`,
					sessionID, runtimeDeliveryExhaustionEventID(leasedRuntimeJob),
				).Scan(&exhaustionEvents); err != nil {
					t.Fatalf("count stale exhaustion events: %v", err)
				}
				if exhaustionEvents != 0 {
					t.Fatalf("stale finalization exhaustion events = %d; want zero", exhaustionEvents)
				}
			}
			var jobCount int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
				WHERE workspace_id = 'default' AND dedupe_key = $1`, request.DedupeKey).Scan(&jobCount); err != nil {
				t.Fatalf("count Queue lineage: %v", err)
			}
			if jobCount != 1 {
				t.Fatalf("Queue lineage jobs = %d; want original job only", jobCount)
			}
			if second := handOffLostRuntimeInputsForTest(t, runtime, sessionID, bindingID, podUID, now.Add(3*time.Second)); second != 0 {
				t.Fatalf("repeated handoff = %d; want idempotent zero", second)
			}
		})
	}
}

func TestPostgreSQLRuntimePodLossLeavesExhaustedInterruptForCurrentLeaseTerminalization(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_lost_interrupt_barrier"
		threadID       = "thr_lost_interrupt_barrier"
		runtimeInputID = "rin_lost_interrupt_barrier"
		eventID        = "evt_lost_interrupt_barrier"
		oldBindingID   = "bind_lost_interrupt_old"
		oldPodUID      = "pod_lost_interrupt_old"
		jobID          = "qjob_lost_interrupt"
	)
	now := time.Date(2026, 8, 10, 12, 30, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "interrupt_control", `["evt_lost_interrupt_barrier"]`, "delivering", oldBindingID, oldPodUID, 1, 1)

	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), queue.RetryPolicy{
		BaseDelay: time.Second, MaxDelay: time.Second, MaxAttempts: 2,
		RandomInt64: func(bound int64) int64 { return bound - 1 },
	})
	request, err := runtimecontrol.RuntimeInputEnqueueRequest("default", sessionID, runtimecontrol.AcceptedRuntimeInput{
		SessionThreadID: threadID, RuntimeInputID: runtimeInputID, InputKind: "interrupt_control",
		EventIDsJSON: `["evt_lost_interrupt_barrier"]`, SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true},
	}, now)
	if err != nil {
		t.Fatalf("build interrupt Queue request: %v", err)
	}
	request.ID = jobID
	request.MaxAttempts = 2
	if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
		t.Fatalf("enqueue interrupt barrier: %v", err)
	}
	leaseRequest := queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-old", MaxJobs: 1, LeaseDuration: time.Minute, Now: now}
	first := mustLeaseBridgeQueueJob(t, queueStore, leaseRequest)
	if ok, err := queueStore.Retry(context.Background(), queue.RetryRequest{WorkspaceID: workspace.DefaultID, JobID: jobID, LeaseToken: first.LeaseToken, ErrorKind: "runtime_transport_error", Now: now.Add(time.Second)}); err != nil || !ok {
		t.Fatalf("retry first interrupt attempt = %t/%v", ok, err)
	}
	leaseRequest.Now = now.Add(3 * time.Second)
	second := mustLeaseBridgeQueueJob(t, queueStore, leaseRequest)
	if second.AttemptCount != 2 {
		t.Fatalf("second interrupt attempt = %d; want 2", second.AttemptCount)
	}
	if ok, err := queueStore.Retry(context.Background(), queue.RetryRequest{WorkspaceID: workspace.DefaultID, JobID: jobID, LeaseToken: second.LeaseToken, ErrorKind: "interrupt_closeout_pending", Now: now.Add(4 * time.Second)}); err != nil || !ok {
		t.Fatalf("retain exhausted interrupt = %t/%v", ok, err)
	}

	if handedOff := handOffLostRuntimeInputsForTest(t, runtime, sessionID, oldBindingID, oldPodUID, now.Add(5*time.Second)); handedOff != 1 {
		t.Fatalf("proven-loss handoff = %d; want 1", handedOff)
	}
	var inboxStatus, queueStatus string
	var attempts, lineage int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3)`,
		runtimeInputID, jobID, request.DedupeKey).Scan(&inboxStatus, &queueStatus, &attempts, &lineage); err != nil {
		t.Fatalf("read replacement handoff: %v", err)
	}
	if inboxStatus != "queued" || queueStatus != queue.StatusPending || attempts != 2 || lineage != 1 {
		t.Fatalf("exhausted handoff = inbox:%s queue:%s attempts:%d lineage:%d; want queued/pending/2/1", inboxStatus, queueStatus, attempts, lineage)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliverer := &postgresFinalizingDeliverer{store: deliveryStore}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "bridge-current-terminal-owner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("terminalize exhausted interrupt through JobRunner: %v", err)
	}
	if deliverer.deliveries != 0 {
		t.Fatalf("exhausted interrupt replacement Runtime calls = %d; want zero", deliverer.deliveries)
	}
	var finalQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, jobID).Scan(&finalQueueStatus); err != nil {
		t.Fatalf("read final Queue status: %v", err)
	}
	if finalQueueStatus != queue.StatusCancelled {
		t.Fatalf("terminal winner Queue status = %s; want cancelled", finalQueueStatus)
	}
}

func TestPostgreSQLRuntimePodLossReplacesOnlyAcknowledgedQueueCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_pod_loss_queue_acked"
		threadID       = "thr_pod_loss_queue_acked"
		runtimeInputID = "rin_pod_loss_queue_acked"
		bindingID      = "bind_pod_loss_queue_acked"
		podUID         = "pod_pod_loss_queue_acked"
		originalJobID  = "qjob_pod_loss_acked"
	)
	now := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages", `["evt_pod_loss_queue_acked"]`, "accepted", bindingID, podUID, 1, 1)

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	request, err := runtimecontrol.RuntimeInputEnqueueRequest("default", sessionID, runtimecontrol.AcceptedRuntimeInput{
		SessionThreadID: threadID, RuntimeInputID: runtimeInputID, InputKind: "messages",
		EventIDsJSON: `["evt_pod_loss_queue_acked"]`, SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true},
	}, now)
	if err != nil {
		t.Fatalf("build original Queue request: %v", err)
	}
	request.ID = originalJobID
	if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
		t.Fatalf("enqueue original Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-acked-custody",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease original Queue job = %#v, %v; want one", leased, err)
	}
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: originalJobID, LeaseToken: leased[0].LeaseToken, Now: now.Add(2 * time.Second),
	}); err != nil || !updated {
		t.Fatalf("ACK original Queue job = %t, %v; want true, nil", updated, err)
	}

	if handedOff := handOffLostRuntimeInputsForTest(t, runtime, sessionID, bindingID, podUID, now.Add(3*time.Second)); handedOff != 1 {
		t.Fatalf("handed off inputs = %d; want 1", handedOff)
	}
	var replacementID, replacementStatus, inboxStatus string
	var lineageCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT id, status FROM queue_jobs
		WHERE workspace_id = 'default' AND dedupe_key = $1 AND status IN ('pending','leased')`, request.DedupeKey).Scan(&replacementID, &replacementStatus); err != nil {
		t.Fatalf("read replacement Queue job: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'default' AND dedupe_key = $1`, request.DedupeKey).Scan(&lineageCount); err != nil {
		t.Fatalf("count replacement lineage: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read replacement inbox state: %v", err)
	}
	if replacementID == originalJobID || replacementStatus != queue.StatusPending || inboxStatus != "queued" || lineageCount != 2 {
		t.Fatalf("replacement custody = id %q status %q inbox %q lineage %d; want new pending/queued lineage 2", replacementID, replacementStatus, inboxStatus, lineageCount)
	}
	if second := handOffLostRuntimeInputsForTest(t, runtime, sessionID, bindingID, podUID, now.Add(4*time.Second)); second != 0 {
		t.Fatalf("repeated replacement handoff = %d; want zero", second)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'default' AND dedupe_key = $1`, request.DedupeKey).Scan(&lineageCount); err != nil {
		t.Fatalf("recount replacement lineage: %v", err)
	}
	if lineageCount != 2 {
		t.Fatalf("Queue lineage after repeated handoff = %d; want 2", lineageCount)
	}
}

func TestPostgreSQLRuntimeDeliveryAcknowledgesReclaimedJobAlreadyAcceptedByLiveBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_reclaimed_live_binding"
		threadID       = "thr_reclaimed_live_binding"
		runtimeInputID = "rin_reclaimed_live_binding"
		bindingID      = "bind_reclaimed_live_binding"
		podUID         = "pod_reclaimed_live_binding"
		jobID          = "qjob_reclaimed_live"
	)
	now := time.Date(2026, 8, 10, 13, 30, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://example.test/mcp"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json =
		'{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://example.test/mcp"}]}'
		WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("seed unready manifest configuration: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_reclaimed_live_binding", 1, "user.message", `{"content":[{"type":"text","text":"accepted"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages", `["evt_reclaimed_live_binding"]`, "accepted", bindingID, podUID, 1, 1)

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	request, err := runtimecontrol.RuntimeInputEnqueueRequest("default", sessionID, runtimecontrol.AcceptedRuntimeInput{
		SessionThreadID: threadID, RuntimeInputID: runtimeInputID, InputKind: "messages",
		EventIDsJSON: `["evt_reclaimed_live_binding"]`, SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true},
	}, now)
	if err != nil {
		t.Fatalf("build reclaimed Queue request: %v", err)
	}
	request.ID = jobID
	if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
		t.Fatalf("enqueue reclaimed Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-reclaimed-live",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease reclaimed Queue job = %#v, %v; want one", leased, err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now.Add(2 * time.Second) }
	lister := &recordingMCPManifestLister{err: errors.New("manifest readiness must not be consulted")}
	resolver := &recordingRuntimeTargetResolver{err: errors.New("target availability must not be consulted")}
	store.MCPManifestLister = lister
	store.TargetResolver = resolver
	sender := &recordingRuntimeCommandSender{}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), RuntimeJob{
		JobID: jobID, LeaseToken: leased[0].LeaseToken, Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: runtimeInputID,
		EventIDs: []string{"evt_reclaimed_live_binding"}, SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		PayloadJSON: string(request.PayloadJSON),
	})
	if err != nil {
		t.Fatalf("deliver reclaimed Queue job: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted || !result.QueueLeaseSettled || len(sender.requests) != 0 {
		t.Fatalf("reclaimed delivery = %#v sender requests %d; want accepted settled without Runtime RPC", result, len(sender.requests))
	}
	if len(lister.requests) != 0 || len(resolver.jobs) != 0 {
		t.Fatalf("accepted replay consulted readiness/target resolution: lister=%d resolver=%d", len(lister.requests), len(resolver.jobs))
	}
	var queueStatus, inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT job.status, inbox.status
		FROM queue_jobs job JOIN session_runtime_inbox inbox
		  ON inbox.workspace_id = job.workspace_id AND inbox.runtime_input_id = $2
		WHERE job.workspace_id = 'default' AND job.id = $1`, jobID, runtimeInputID).Scan(&queueStatus, &inboxStatus); err != nil {
		t.Fatalf("read reclaimed settlement: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged || inboxStatus != "accepted" {
		t.Fatalf("reclaimed settlement = Queue %q / inbox %q; want acknowledged / accepted", queueStatus, inboxStatus)
	}
}

func TestPostgreSQLRuntimePodLossRejectsDeliveringInputWithoutActiveQueueCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_pod_loss_queue_missing"
		threadID       = "thr_pod_loss_queue_missing"
		runtimeInputID = "rin_pod_loss_queue_missing"
		bindingID      = "bind_pod_loss_queue_missing"
		podUID         = "pod_pod_loss_queue_missing"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages", `["evt_pod_loss_queue_missing"]`, "delivering", bindingID, podUID, 1, 1)

	client := dbconnect.NewClientForTesting(runtime)
	err := client.WithWorkspaceTx(context.Background(), "default", "test.runtime_pod_loss_missing_queue_custody", func(tx *dbconnect.Tx) error {
		_, err := runtimecontrol.HandBackRuntimeInputsTx(context.Background(), tx, "default", sessionID,
			runtimecontrol.Binding{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID},
			time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC))
		return err
	})
	var prepareErr runtimecontrol.PreparationError
	if !errors.As(err, &prepareErr) || prepareErr.Kind != "runtime_inbox_invariant" || prepareErr.Retryable {
		t.Fatalf("missing Queue custody error = %#v; want non-retryable runtime_inbox_invariant", err)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read invariant-fenced inbox: %v", err)
	}
	if inboxStatus != "delivering" {
		t.Fatalf("invariant-fenced inbox = %q; want delivering", inboxStatus)
	}
}

func handOffLostRuntimeInputsForTest(t *testing.T, runtime *sql.DB, sessionID string, bindingID string, podUID string, now time.Time) int {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtime)
	handedOff := 0
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.runtime_pod_loss_queue_custody", func(tx *dbconnect.Tx) error {
		var err error
		handedOff, err = runtimecontrol.HandBackRuntimeInputsTx(context.Background(), tx, "default", sessionID,
			runtimecontrol.Binding{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID}, now)
		return err
	}); err != nil {
		t.Fatalf("hand off lost Runtime inputs: %v", err)
	}
	return handedOff
}

func mustLeaseBridgeQueueJob(t *testing.T, store *queue.PostgreSQLQueueStore, request queue.LeaseRequest) *queue.Job {
	t.Helper()
	jobs, err := store.Lease(context.Background(), request)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease Queue job = %#v/%v; want exactly one", jobs, err)
	}
	return jobs[0]
}
