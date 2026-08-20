package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLRuntimePodLossAgentMailHandoffReclaimsExistingProjection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_pod_loss_agent_mail"
		mainID     = "thr_pod_loss_agent_mail_main"
		childID    = "thr_pod_loss_agent_mail_child"
		deliveryID = "delivery_pod_loss_agent_mail"
		oldBinding = "bind_pod_loss_agent_mail_old"
		oldPodUID  = "pod_pod_loss_agent_mail_old"
		newBinding = "bind_pod_loss_agent_mail_new"
		newPodUID  = "pod_pod_loss_agent_mail_new"
		runtimeID  = "agent_mail:delivery_pod_loss_agent_mail"
	)
	now := time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBinding, 1, oldPodUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, deliveryID, 1, "2026-08-11T09:00:00Z")

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	originalLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-agent-mail-old",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(originalLease) != 1 {
		t.Fatalf("lease original agent mail = %#v, %v; want one", originalLease, err)
	}
	originalJob, err := DecodeRuntimeJob(queueJobProto(originalLease[0]))
	if err != nil {
		t.Fatalf("decode original agent mail: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	visiblePodUID := oldPodUID
	visiblePodName := "runtime-pod-0"
	visiblePodIP := "10.0.0.10"
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: visiblePodName, PodUID: visiblePodUID, PodIP: visiblePodIP,
		}})
	}}
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), originalJob)
	if err != nil || plan.AcceptAgentMail == nil {
		t.Fatalf("prepare original agent mail = %#v, %v; want Runtime request", plan, err)
	}
	settled, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), originalJob, plan.AttemptedBinding)
	if err != nil || settled {
		t.Fatalf("accept original agent mail = %t, %v; want accepted Inbox with Queue lease still owned by runner", settled, err)
	}
	if handedOff := handOffLostRuntimeInputsForTest(t, runtime, sessionID, oldBinding, oldPodUID, now.Add(3*time.Second)); handedOff != 1 {
		t.Fatalf("agent mail handoff = %d; want one", handedOff)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET binding_id=$2,binding_generation=2,agent_runtime_pod_uid=$3,agent_runtime_pod_name='runtime-pod-1',agent_runtime_pod_ip='10.0.0.11',updated_at=$4
		WHERE workspace_id='default' AND session_id=$1`, sessionID, newBinding, newPodUID, now.Add(4*time.Second)); err != nil {
		t.Fatalf("install replacement Runtime binding: %v", err)
	}
	visiblePodUID = newPodUID
	visiblePodName = "runtime-pod-1"
	visiblePodIP = "10.0.0.11"
	deliveryStore.Clock = func() time.Time { return now.Add(6 * time.Second) }
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue:      tetralqueue.NewServer(queueStore, nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer:  RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:     JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run replacement agent-mail delivery: %v", err)
	}
	if len(sender.requests) != 1 {
		t.Fatalf("replacement agent-mail Runtime requests = %d; want one", len(sender.requests))
	}

	var sentEvents, receivedEvents, inboxRows, errorEvents int
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent' AND payload_json::jsonb ->> 'delivery_id'=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received' AND payload_json::jsonb ->> 'delivery_id'=$2),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$4)`,
		sessionID, deliveryID, runtimeID, originalLease[0].ID,
	).Scan(&sentEvents, &receivedEvents, &inboxRows, &errorEvents, &inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read replacement agent mail custody: %v", err)
	}
	if sentEvents != 1 || receivedEvents != 1 || inboxRows != 1 || errorEvents != 0 || inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("replacement agent mail = sent %d received %d Inbox %d/%s errors %d Queue %s; want 1/1/1 accepted/0/acknowledged",
			sentEvents, receivedEvents, inboxRows, inboxStatus, errorEvents, queueStatus)
	}
}

func TestPostgreSQLMalformedAgentMailReplacementPassesReplayAndDelivers(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_agent_mail_replacement_replay"
		mainID     = "thr_agent_mail_replacement_main"
		childID    = "thr_agent_mail_replacement_child"
		deliveryID = "delivery_agent_mail_replacement"
		bindingID  = "bind_agent_mail_replacement"
		podUID     = "pod_agent_mail_replacement"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, deliveryID, 1, now.Format(time.RFC3339Nano))

	var malformedJobID string
	if err := admin.QueryRowContext(context.Background(), `UPDATE queue_jobs
		SET payload_json=jsonb_set(payload_json::jsonb, '{session_thread_id}', to_jsonb($2::text), false)::text
		WHERE workspace_id='default' AND kind='runtime_input'
		  AND payload_json::jsonb ->> 'runtime_input_id'=$1
		RETURNING id`, "agent_mail:"+deliveryID, childID).Scan(&malformedJobID); err != nil {
		t.Fatalf("corrupt agent-mail Queue thread: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now }
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "10.0.0.10",
		}})
	}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue:      tetralqueue.NewServer(queueStore, nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer:  RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:     JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("replace malformed agent-mail custody: %v", err)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("Runtime requests after malformed lease = %d; want zero", len(sender.requests))
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("deliver canonical agent-mail replacement: %v", err)
	}
	if len(sender.requests) != 1 {
		t.Fatalf("Runtime requests after canonical replacement = %d; want one", len(sender.requests))
	}
	var oldStatus string
	var deadLetters, pending, acknowledged int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		count(*) FILTER (WHERE status='dead_lettered'),
		count(*) FILTER (WHERE status='pending'),
		count(*) FILTER (WHERE status='acknowledged')
		FROM queue_jobs WHERE workspace_id='default'
		  AND payload_json::jsonb ->> 'runtime_input_id'=$2`, malformedJobID, "agent_mail:"+deliveryID,
	).Scan(&oldStatus, &deadLetters, &pending, &acknowledged); err != nil {
		t.Fatalf("read agent-mail replacement lifecycle: %v", err)
	}
	if oldStatus != queue.StatusDeadLettered || deadLetters != 1 || pending != 0 || acknowledged != 1 {
		t.Fatalf("agent-mail replacement lifecycle = old %s dead/pending/ack %d/%d/%d; want dead_lettered 1/0/1", oldStatus, deadLetters, pending, acknowledged)
	}
}

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
			request, err := lostRuntimeInputEnqueueRequest("default", sessionID, runtimePodLostAcceptedInput{
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
	request, err := lostRuntimeInputEnqueueRequest("default", sessionID, runtimePodLostAcceptedInput{
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
	request, err := lostRuntimeInputEnqueueRequest("default", sessionID, runtimePodLostAcceptedInput{
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

func TestPostgreSQLRuntimePodLossReplacementQueueCustodyPreservesInboxOrder(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_pod_loss_queue_order"
		threadID  = "thr_pod_loss_queue_order"
		bindingID = "bind_pod_loss_queue_order"
		podUID    = "pod_pod_loss_queue_order"
		earlyID   = "rin_z_pod_loss_early"
		middleID  = "task_notification:task_m_pod_loss_middle"
		lateID    = "agent_mail:a_pod_loss_late"
		taskID    = "task_m_pod_loss_middle"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_pod_loss_early", 1, "user.message", `{"content":[{"type":"text","text":"first"}]}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_pod_loss_late", 2, "agent.thread_message_received", bridgeInterAgentMessageJSON(
		t, "a_pod_loss_late", threadID, "evt_pod_loss_mail_source", bridgePublicMessageJSONForTest(t, "third"),
	))
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "evt_pod_loss_task_source")
	taskResult := `{"task_id":"task_m_pod_loss_middle","source_tool_use_event_id":"evt_pod_loss_task_source","status":"completed","stdout":{"text":"second","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":0}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", taskResult)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, earlyID, "messages", `["evt_pod_loss_early"]`, "accepted", bindingID, podUID, 1, 1)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, middleID, "task_notification", `[]`, "accepted", bindingID, podUID, 0, 0)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, lateID, "agent_mail", `["evt_pod_loss_late"]`, "accepted", bindingID, podUID, 2, 2)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET created_at = CASE runtime_input_id
		  WHEN $1 THEN '2026-08-10T10:00:00Z'::timestamptz
		  WHEN $2 THEN '2026-08-10T10:00:01Z'::timestamptz
		  ELSE '2026-08-10T10:00:02Z'::timestamptz
		END
		WHERE workspace_id = 'default' AND runtime_input_id IN ($1, $2, $3)`, earlyID, middleID, lateID); err != nil {
		t.Fatalf("stamp Inbox creation order: %v", err)
	}

	if handedOff := handOffLostRuntimeInputsForTest(
		t,
		runtime,
		sessionID,
		bindingID,
		podUID,
		time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC),
	); handedOff != 3 {
		t.Fatalf("ordered handoff count = %d; want 3", handedOff)
	}
	rows, err := admin.QueryContext(context.Background(), `SELECT payload_json::jsonb ->> 'runtime_input_id'
		FROM queue_jobs
		WHERE workspace_id = 'default'
		  AND kind = 'runtime_input'
		  AND payload_json::jsonb ->> 'session_id' = $1
		ORDER BY queue_partition_sequence`, sessionID)
	if err != nil {
		t.Fatalf("read replacement Queue order: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ordered []string
	for rows.Next() {
		var runtimeInputID string
		if err := rows.Scan(&runtimeInputID); err != nil {
			t.Fatalf("scan replacement Queue order: %v", err)
		}
		ordered = append(ordered, runtimeInputID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate replacement Queue order: %v", err)
	}
	if len(ordered) != 3 || ordered[0] != earlyID || ordered[1] != middleID || ordered[2] != lateID {
		t.Fatalf("replacement Queue order = %v; want mixed-kind Inbox creation order [%s %s %s]", ordered, earlyID, middleID, lateID)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET status='accepted',binding_id=$2,binding_generation=1,target_pod_uid=$3
		WHERE workspace_id='default' AND session_id=$1`, sessionID, bindingID, podUID); err != nil {
		t.Fatalf("accept replacement inputs in Queue order: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("pod-loss-context-order-test-key-32")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: earlyID,
	}); err != nil {
		t.Fatalf("commit first replacement input: %v", err)
	}
	if response, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(t, scope, middleID)); err != nil || response.GetCommitted() == nil {
		t.Fatalf("commit middle replacement input: %v", err)
	}
	if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: lateID,
	}); err != nil {
		t.Fatalf("commit final replacement input: %v", err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("load replacement Context: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode replacement Context: %v", err)
	}
	if len(payload.ContextEntries) != 3 {
		t.Fatalf("replacement Context messages = %s; want three mixed-kind inputs", loaded.GetContextJson())
	}
	var contextTexts []string
	for _, entry := range payload.ContextEntries {
		var part struct {
			Text string `json:"text"`
		}
		if len(entry.Parts) != 1 {
			t.Fatalf("replacement Context entry parts = %#v; want one", entry.Parts)
		}
		if err := json.Unmarshal(entry.Parts[0], &part); err != nil {
			t.Fatalf("decode replacement Context part: %v; part=%s", err, entry.Parts[0])
		}
		contextTexts = append(contextTexts, part.Text)
	}
	if len(contextTexts) != 3 || contextTexts[0] != "first" || !strings.Contains(contextTexts[1], taskID) || contextTexts[2] != "third" {
		t.Fatalf("replacement Context order = %v; want user/task/agent-mail creation order; context=%s", contextTexts, loaded.GetContextJson())
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
	request, err := lostRuntimeInputEnqueueRequest("default", sessionID, runtimePodLostAcceptedInput{
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

func TestPostgreSQLRuntimePodLossReplacementCommitsTheSameAcceptedInputOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_pod_loss_commit"
		threadID       = "thr_pod_loss_commit"
		runtimeInputID = "rin_pod_loss_commit"
		eventID        = "evt_pod_loss_commit"
		oldBindingID   = "bind_pod_loss_commit_old"
		oldPodUID      = "pod_pod_loss_commit_old"
		originalJobID  = "qjob_pl_commit"
	)
	now := time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET status='idle'
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("seed pre-running Runtime status: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"continue after replacement"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages", `["`+eventID+`"]`, "accepted", oldBindingID, oldPodUID, 1, 1)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	request, err := lostRuntimeInputEnqueueRequest("default", sessionID, runtimePodLostAcceptedInput{
		SessionThreadID: threadID, RuntimeInputID: runtimeInputID, InputKind: "messages",
		EventIDsJSON: `["` + eventID + `"]`, SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true},
	}, now)
	if err != nil {
		t.Fatalf("build original input custody: %v", err)
	}
	request.ID = originalJobID
	if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
		t.Fatalf("enqueue original input custody: %v", err)
	}
	originalLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-pod-loss-original",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(originalLease) != 1 {
		t.Fatalf("lease original input custody = %#v, %v; want one", originalLease, err)
	}
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: originalJobID, LeaseToken: originalLease[0].LeaseToken, Now: now.Add(2 * time.Second),
	}); err != nil || !updated {
		t.Fatalf("ACK original input custody = %t, %v; want true", updated, err)
	}

	oldPod := enginekubernetes.BoundRuntimePod{
		Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: oldPodUID, PodIP: "10.0.0.10",
	}
	replacement := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime", PodName: "runtime-pod-1", PodUID: "pod_pod_loss_commit_new", PodIP: "10.0.0.11",
	}
	store := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
			true, oldPod, enginekubernetes.BindingVisibilityDeleted, []enginekubernetes.BindingCandidate{replacement},
		)
	})
	if repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 1 {
		t.Fatalf("repair accepted input after proven Pod loss = %d, %v; want one", repaired, err)
	}
	replacementLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-pod-loss-replacement",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(3 * time.Second),
	})
	if err != nil || len(replacementLease) != 1 || replacementLease[0].ID == originalJobID {
		t.Fatalf("lease replacement input custody = %#v, %v; want one new job", replacementLease, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(replacementLease[0]))
	if err != nil {
		t.Fatalf("decode replacement input custody: %v", err)
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.AcceptInput == nil || plan.AcceptInput.GetTargetPodUid() != replacement.PodUID {
		t.Fatalf("prepare replacement input = %#v, %v; want new Pod", plan, err)
	}
	if settled, err := store.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding); err != nil || settled {
		t.Fatalf("mark replacement input accepted = %t, %v; want accepted without deferral", settled, err)
	}
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: replacementLease[0].ID, LeaseToken: replacementLease[0].LeaseToken, Now: now.Add(4 * time.Second),
	}); err != nil || !updated {
		t.Fatalf("ACK replacement delivery custody = %t, %v; want true", updated, err)
	}
	commit := &bridgev1.CommitInputsRequest{
		Scope: runtimeScopeFromAttempt(job, plan.AttemptedBinding), RuntimeInputId: runtimeInputID,
	}
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	first, err := apiStore.CommitInputs(context.Background(), commit)
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("commit replacement input = %#v, %v; want committed", first, err)
	}
	replay, err := apiStore.CommitInputs(context.Background(), commit)
	if err != nil || replay.GetCommitted() == nil {
		t.Fatalf("replay replacement input commit = %#v, %v; want committed", replay, err)
	}
	var inboxStatus string
	var processed, messages, lineage int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND event_id=$2 AND processed_at IS NOT NULL),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND source_event_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3)`,
		runtimeInputID, eventID, request.DedupeKey,
	).Scan(&inboxStatus, &processed, &messages, &lineage); err != nil {
		t.Fatalf("read replacement commit evidence: %v", err)
	}
	if inboxStatus != "committed" || processed != 1 || messages != 1 || lineage != 2 {
		t.Fatalf("replacement commit = Inbox %q processed %d messages %d lineage %d; want committed/1/1/2", inboxStatus, processed, messages, lineage)
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
		_, err := handOffLostRuntimeAcceptedInputsTx(context.Background(), tx, "default", sessionID,
			runtimeBindingForDelivery{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID},
			time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC))
		return err
	})
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_invariant" || prepareErr.retryable {
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
		handedOff, err = handOffLostRuntimeAcceptedInputsTx(context.Background(), tx, "default", sessionID,
			runtimeBindingForDelivery{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID}, now)
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
