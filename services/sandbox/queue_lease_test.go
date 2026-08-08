package tetralsandbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type blockingHeartbeatQueue struct {
	recordingSandboxQueue
	started chan struct{}
}

type renewingHeartbeatQueue struct {
	recordingSandboxQueue
	called      chan struct{}
	leasedUntil time.Time
}

func (q *renewingHeartbeatQueue) Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	select {
	case q.called <- struct{}{}:
	default:
	}
	return &queuev1.HeartbeatResponse{Updated: true, LeasedUntil: q.leasedUntil.Format(time.RFC3339Nano)}, nil
}

type blockingTransitionQueue struct {
	recordingSandboxQueue
}

func (q *blockingTransitionQueue) Ack(ctx context.Context, _ *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *blockingHeartbeatQueue) Heartbeat(ctx context.Context, _ *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	close(q.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestQueueLeaseGuardUsesTheWireRoundedDuration(t *testing.T) {
	got := wireRoundedQueueLeaseDuration(40*time.Millisecond + 999*time.Microsecond)
	if got != 40*time.Millisecond {
		t.Fatalf("wire-rounded Queue lease = %s; want 40ms", got)
	}
}

func TestQueueHeartbeatDeadlineIsDerivedFromOneAuthoritySnapshot(t *testing.T) {
	now := time.Now()
	expiry := now.Add(9 * time.Millisecond)
	deadline, ok := queueHeartbeatDeadline(now, expiry, 50*time.Millisecond)
	if !ok {
		t.Fatal("queueHeartbeatDeadline rejected a live authority window")
	}
	if !deadline.Equal(now.Add(4*time.Millisecond + 500*time.Microsecond)) {
		t.Fatalf("heartbeat deadline = %s; want half of the sampled authority window", deadline.Sub(now))
	}
	if !deadline.Before(expiry) {
		t.Fatalf("heartbeat deadline %s is not before local expiry %s", deadline, expiry)
	}
}

func TestQueueLeaseGuardCancelsWorkBeforeAHungHeartbeatCanOutliveTheLease(t *testing.T) {
	leaseDuration := 500 * time.Millisecond
	heartbeatInterval := 50 * time.Millisecond
	startedAt := time.Now()
	queueClient := &blockingHeartbeatQueue{started: make(chan struct{})}
	job := &queuev1.QueueJob{
		Id: "qjob_lease_guard", WorkspaceId: "ws_lease_guard", LeaseToken: "lease_guard",
		LeasedUntil: startedAt.Add(leaseDuration).UTC().Format(time.RFC3339Nano),
	}
	workCtx, finish, err := startQueueLeaseGuard(context.Background(), queueClient, job, startedAt.Add(leaseDuration), heartbeatInterval, leaseDuration)
	if err != nil {
		t.Fatalf("startQueueLeaseGuard: %v", err)
	}
	defer func() { _ = finish() }()
	select {
	case <-queueClient.started:
	case <-time.After(heartbeatInterval * 2):
		t.Fatal("heartbeat did not start")
	}
	select {
	case <-workCtx.Done():
	case <-time.After(leaseDuration):
		t.Fatal("work context remained live through the Queue lease expiry")
	}
	if elapsed := time.Since(startedAt); elapsed >= leaseDuration {
		t.Fatalf("work cancellation took %s; want before %s lease expiry", elapsed, leaseDuration)
	}
	if err := finish(); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("finish error = %v; want Queue lease loss", err)
	}
}

func TestQueueLeaseGuardCarriesHeartbeatExpiryIntoLifecycleClaim(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	setupCtx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(setupCtx, execution, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	job := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
	if _, err := adminDB.Exec(`UPDATE queue_jobs
		SET status=$1, lease_token=NULL, leased_by=NULL, leased_at=NULL, leased_until=NULL
		WHERE workspace_id=$2 AND id=$3`, queue.StatusPending, job.WorkspaceID, job.JobID); err != nil {
		t.Fatalf("return activation Queue job to pending: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: "ws_execution_store", Kinds: []string{queue.KindSandboxActivate},
		LeaseOwner: "sandbox", MaxJobs: 1, LeaseDuration: 5 * time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != job.JobID || leased[0].LeasedUntil == nil {
		t.Fatalf("lease activation Queue job = %#v, %v; want %s", leased, err, job.JobID)
	}
	job.LeaseOwner = leased[0].LeasedBy
	job.LeaseToken = leased[0].LeaseToken
	job.LeaseExpiresAt = *leased[0].LeasedUntil
	job.AttemptCount = leased[0].AttemptCount
	queueJob := &queuev1.QueueJob{
		Id: job.JobID, WorkspaceId: job.WorkspaceID, LeasedBy: job.LeaseOwner,
		LeaseToken: job.LeaseToken, AttemptCount: int32(job.AttemptCount),
		LeasedUntil: now.Add(500 * time.Millisecond).Format(time.RFC3339Nano),
	}
	refreshedExpiry := now.Add(2 * time.Minute)
	queueClient := &renewingHeartbeatQueue{called: make(chan struct{}, 1), leasedUntil: refreshedExpiry}
	workCtx, finish, err := startQueueLeaseGuard(context.Background(), queueClient, queueJob, now.Add(500*time.Millisecond), 20*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("startQueueLeaseGuard: %v", err)
	}
	defer func() { _ = finish() }()
	select {
	case <-queueClient.called:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not run")
	}
	deadline := time.Now().Add(time.Second)
	for !sandboxQueueAuthorityFromContext(workCtx).currentLeasedUntil().Equal(refreshedExpiry) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := sandboxQueueAuthorityFromContext(workCtx).currentLeasedUntil(); !got.Equal(refreshedExpiry) {
		t.Fatalf("carried lifecycle expiry = %s; want %s", got, refreshedExpiry)
	}
	store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	if _, disposition, err := store.ClaimActivation(workCtx, job, now); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation with heartbeat expiry = %s, %v; want applied", disposition, err)
	}
}

func TestQueueLeaseGuardCancelsFinalTransitionAtLocalExpiry(t *testing.T) {
	now := time.Now()
	queueClient := &blockingTransitionQueue{}
	job := &queuev1.QueueJob{
		Id: "qjob_transition_expiry", WorkspaceId: "ws_transition_expiry", LeaseToken: "lease_transition_expiry",
		LeasedUntil: now.Add(150 * time.Millisecond).UTC().Format(time.RFC3339Nano),
	}
	workCtx, finish, err := startQueueLeaseGuard(context.Background(), queueClient, job, now.Add(150*time.Millisecond), 50*time.Millisecond, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("startQueueLeaseGuard: %v", err)
	}
	if err := stopQueueLeaseGuard(workCtx); err != nil {
		t.Fatalf("stopQueueLeaseGuard: %v", err)
	}
	if _, err := queueClient.Ack(workCtx, &queuev1.AckRequest{WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked transition error = %v; want context cancellation", err)
	}
	if err := finish(); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("finish error = %v; want Queue lease loss", err)
	}
}
