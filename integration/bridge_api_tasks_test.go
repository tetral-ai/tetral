package integration

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
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func settleBridgeAPIBackgroundTask(t *testing.T, admin *sql.DB, sessionID string, taskID string, terminalStatus string, resultJSON string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status=$3, terminal_result_json=$4, terminal_result_digest=$5,
		    terminal_at='2026-01-01T00:00:30Z', next_poll_at=NULL,
		    reconcile_generation=reconcile_generation+1, updated_at='2026-01-01T00:00:30Z'
		WHERE workspace_id='default' AND session_id=$1 AND task_id=$2 AND status='running'`,
		sessionID, taskID, terminalStatus, resultJSON, runtimecontrol.RequestHash(resultJSON)); err != nil {
		t.Fatalf("settle background task: %v", err)
	}
}

type taskNotificationReplayOnlyDeliverer struct {
	store      *jobrunner.PostgreSQLRuntimeDeliveryStore
	deliveries int
}

func (d *taskNotificationReplayOnlyDeliverer) DeliverRuntimeJob(context.Context, jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, error) {
	d.deliveries++
	return jobrunner.RuntimeDeliveryResult{}, errors.New("Runtime must not be contacted after durable rejection")
}

func (d *taskNotificationReplayOnlyDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, bool, error) {
	return d.store.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func TestPostgreSQLJobRunnerReclaimsRejectedTaskNotificationAndACKsWithoutRuntime(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_rejection_reclaim"
		threadID  = "thr_task_rejection_reclaim"
		bindingID = "bind_task_rejection_reclaim"
		podUID    = "pod_task_rejection_reclaim"
		taskID    = "task_rejection_reclaim"
		inputID   = "task_notification:task_rejection_reclaim"
	)
	now := time.Now().UTC()
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "sevt_task_rejection_reclaim")
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET type='agent.message', model_tool_call_id=NULL
		WHERE workspace_id='default' AND session_id=$1 AND event_id='sevt_task_rejection_reclaim'`, sessionID); err != nil {
		t.Fatalf("corrupt durable task source: %v", err)
	}
	sessionfixture.SeedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, threadID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue job: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue task notification Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "rejection-before-crash",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease task notification Queue job = %#v/%v", leased, err)
	}
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope: sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
	}
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := bridgeStore.CommitTaskNotificationResult(context.Background(), request)
	if err != nil || response.GetRejected().GetReason() != bridgev1.TaskNotificationRejectionReason_TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID {
		t.Fatalf("commit durable task notification rejection = %#v/%v", response, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND status='leased'`, queued.ID); err != nil {
		t.Fatalf("expire rejected task notification lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim rejected task notification lease = %d/%v; want one", reclaimed, err)
	}
	deliverer := &taskNotificationReplayOnlyDeliverer{store: fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)}
	runner := &jobrunner.JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Deliverer: deliverer,
		Config: jobrunner.JobRunnerConfig{LeaseOwner: "rejection-after-crash", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := acquireAndJoinJobRunnerActive(context.Background(), runner); err != nil || !active {
		t.Fatalf("replay rejected task notification after reclaim = active:%t err:%v", active, err)
	}
	var queueStatus, inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		queued.ID, inputID,
	).Scan(&queueStatus, &inboxStatus); err != nil {
		t.Fatalf("read reclaimed rejection custody: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged || inboxStatus != "dead_lettered" || deliverer.deliveries != 0 {
		t.Fatalf("reclaimed rejection = Queue:%s Inbox:%s Runtime calls:%d", queueStatus, inboxStatus, deliverer.deliveries)
	}
}

func TestPostgreSQLTaskNotificationRejectionBeforeAcceptanceFinalizationACKsOwnedQueueLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_rejection_acceptance"
		threadID  = "thr_task_rejection_acceptance"
		bindingID = "bind_task_rejection_acceptance"
		podUID    = "pod_task_rejection_acceptance"
		taskID    = "task_rejection_acceptance"
		inputID   = "task_notification:task_rejection_acceptance"
	)
	now := time.Now().UTC()
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "sevt_task_rejection_acceptance")
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET type='agent.message', model_tool_call_id=NULL
		WHERE workspace_id='default' AND session_id=$1 AND event_id='sevt_task_rejection_acceptance'`, sessionID); err != nil {
		t.Fatalf("corrupt durable task source: %v", err)
	}
	sessionfixture.SeedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, threadID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue job: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue task notification Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "task-rejection-acceptance",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease task notification Queue job = %#v/%v", leased, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode task notification Queue job: %v", err)
	}
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope: sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
	}
	apiStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := apiStore.CommitTaskNotificationResult(context.Background(), request)
	if err != nil || response.GetRejected().GetReason() != bridgev1.TaskNotificationRejectionReason_TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID {
		t.Fatalf("commit terminal notification rejection = %#v/%v", response, err)
	}
	deliveryStore := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	attemptedBinding := jobrunner.RuntimeAttemptedBinding{
		BindingID: bindingID, Generation: 1, TargetPodUID: podUID, RuntimeProcessID: "process_" + podUID,
	}
	if settled, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), job, attemptedBinding); err != nil || settled {
		t.Fatalf("MarkRuntimeInputAccepted after rejection = settled:%t err:%v; want replayed terminal Inbox", settled, err)
	}
	if acked, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(2 * time.Second),
	}); err != nil || !acked {
		t.Fatalf("ACK rejection Queue lease = %t/%v", acked, err)
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status
		FROM session_runtime_inbox inbox JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id AND job.id=$2
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, inputID, queued.ID).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read converged rejected custody: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("rejected custody = Inbox:%s Queue:%s", inboxStatus, queueStatus)
	}
}
