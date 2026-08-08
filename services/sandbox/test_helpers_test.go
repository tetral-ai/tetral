package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type recordingSandboxQueue struct {
	leased        []*queuev1.QueueJob
	transitions   []string
	heartbeatLost bool
	heartbeatErr  error
	leaseDelay    time.Duration
}

type observingSandboxFinalizerQueue struct {
	recordingSandboxQueue
	finalizing        <-chan struct{}
	heartbeatObserved chan<- struct{}
}

func (q *observingSandboxFinalizerQueue) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	select {
	case <-q.finalizing:
		select {
		case q.heartbeatObserved <- struct{}{}:
		default:
		}
	default:
	}
	return q.recordingSandboxQueue.Heartbeat(ctx, request)
}

func requireHeartbeatDuringFinalizer(started chan struct{}, heartbeatObserved <-chan struct{}) error {
	close(started)
	select {
	case <-heartbeatObserved:
		return nil
	case <-time.After(time.Second):
		return errors.New("queue heartbeat stopped before sandbox finalization")
	}
}

func (q *recordingSandboxQueue) Lease(ctx context.Context, _ *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	if q.leaseDelay > 0 {
		timer := time.NewTimer(q.leaseDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	jobs := q.leased
	q.leased = nil
	for _, job := range jobs {
		if job.GetLeasedUntil() == "" {
			job.LeasedUntil = testSandboxLeaseExpiry()
		}
	}
	return &queuev1.LeaseResponse{Jobs: jobs}, nil
}

func (q *recordingSandboxQueue) Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	if q.heartbeatErr != nil {
		return nil, q.heartbeatErr
	}
	return &queuev1.HeartbeatResponse{Updated: !q.heartbeatLost, LeasedUntil: time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)}, nil
}

func (q *recordingSandboxQueue) Ack(_ context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "ack:"+request.GetJobId())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSandboxQueue) Retry(_ context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "retry:"+request.GetJobId()+":"+request.GetErrorKind())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSandboxQueue) Defer(_ context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "defer:"+request.GetJobId())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSandboxQueue) DeadLetter(_ context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "dead:"+request.GetJobId()+":"+request.GetErrorKind())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func newSandboxServiceTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func testSandboxLeaseExpiry() string {
	return time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
}

func withLifecycleJobQueueAuthority(ctx context.Context, job SandboxLifecycleJob) context.Context {
	return withSandboxQueueAuthority(ctx, &sandboxQueueAuthority{
		workspaceID: job.WorkspaceID,
		jobID:       job.JobID,
		leaseToken:  job.LeaseToken,
		leasedUntil: job.LeaseExpiresAt,
	})
}

func withEnvironmentBuildQueueAuthority(ctx context.Context, job EnvironmentBuildJob) context.Context {
	return withSandboxQueueAuthority(ctx, &sandboxQueueAuthority{
		workspaceID: job.WorkspaceID,
		jobID:       job.JobID,
		leaseToken:  job.LeaseToken,
	})
}

func withTransportQueueAuthority(ctx context.Context, job *queuev1.QueueJob) context.Context {
	leasedUntil, _ := time.Parse(time.RFC3339Nano, job.GetLeasedUntil())
	return withSandboxQueueAuthority(ctx, &sandboxQueueAuthority{
		workspaceID: job.GetWorkspaceId(),
		jobID:       job.GetId(),
		leaseToken:  job.GetLeaseToken(),
		leasedUntil: leasedUntil,
	})
}

func sandboxTestQueueContext(t *testing.T, runtimeDB *sql.DB) context.Context {
	t.Helper()
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	jobID := queue.NewJobID()
	sessionID := "sesn_" + jobID
	memoryStoreID := "memstore_" + jobID
	memoryWriteID := "memwrite_" + jobID
	queued, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: jobID, WorkspaceID: "ws_execution_store", Kind: queue.KindSandboxMemoryProjection,
		PartitionKey:   queue.FormatSandboxMemoryPartitionKey("ws_execution_store", memoryStoreID),
		DedupeKey:      queue.FormatSandboxMemoryProjectionDedupeKey("ws_execution_store", memoryStoreID, memoryWriteID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","session_id":"` + sessionID + `","memory_store_id":"` + memoryStoreID + `","memory_write_id":"` + memoryWriteID + `"}`),
		MaxAttempts:    5,
	})
	if err != nil {
		t.Fatalf("enqueue test Queue authority: %v", err)
	}
	jobs, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: "ws_execution_store", Kinds: []string{queue.KindSandboxMemoryProjection},
		LeaseOwner: "sandbox-test", MaxJobs: 1, LeaseDuration: time.Hour,
	})
	if err != nil || len(jobs) != 1 || jobs[0].ID != queued.ID || jobs[0].LeasedUntil == nil {
		t.Fatalf("lease test Queue authority = %#v, %v; want %s", jobs, err, queued.ID)
	}
	return withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
		workspaceID: string(jobs[0].WorkspaceID), jobID: jobs[0].ID,
		leaseToken: jobs[0].LeaseToken, leasedUntil: *jobs[0].LeasedUntil,
	})
}

func leaseSandboxMaterializationJobForTest(t *testing.T, runtimeDB *sql.DB, adminDB *sql.DB, job *SandboxLifecycleJob) context.Context {
	t.Helper()
	if job == nil {
		t.Fatal("sandbox materialization job is required")
	}
	payload, err := sandboxLifecycleQueuePayload(SandboxExecutionRef{
		WorkspaceID: job.WorkspaceID,
		SessionID:   job.SessionID,
	}, job.LogicalSandboxID, job.OperationID)
	if err != nil {
		t.Fatalf("encode sandbox materialization Queue payload: %v", err)
	}
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	queued, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID:             job.JobID,
		WorkspaceID:    workspace.ID(job.WorkspaceID),
		Kind:           queue.KindSandboxMaterialize,
		PartitionKey:   queue.FormatSandboxLifecyclePartitionKey(workspace.ID(job.WorkspaceID), job.LogicalSandboxID),
		DedupeKey:      queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, workspace.ID(job.WorkspaceID), job.LogicalSandboxID, job.OperationID),
		PayloadVersion: 1,
		PayloadJSON:    payload,
		MaxAttempts:    sandboxMaterializationMaxAttempts,
	})
	if err != nil {
		t.Fatalf("enqueue sandbox materialization test job: %v", err)
	}
	if queued.ID != job.JobID {
		t.Fatalf("sandbox materialization test Queue job = %q; want %q", queued.ID, job.JobID)
	}
	job.AttemptCount = 1
	job.LeaseOwner = "sandbox-materialization-test"
	job.LeaseToken = "lease_" + job.JobID
	if err := adminDB.QueryRow(`UPDATE queue_jobs
		SET status='leased', leased_by=$3, lease_token=$4,
		    leased_at=clock_timestamp(), leased_until=clock_timestamp()+interval '1 hour',
		    attempt_count=GREATEST(attempt_count, 1), updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND id=$2 AND status IN ('pending', 'leased')
		RETURNING leased_until`, job.WorkspaceID, job.JobID, job.LeaseOwner, job.LeaseToken).Scan(&job.LeaseExpiresAt); err != nil {
		t.Fatalf("lease sandbox materialization test job: %v", err)
	}
	return withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
		workspaceID: job.WorkspaceID,
		jobID:       job.JobID,
		leaseToken:  job.LeaseToken,
		leasedUntil: job.LeaseExpiresAt,
	})
}

func acknowledgeSandboxLifecycleJobForTest(t *testing.T, adminDB *sql.DB, job SandboxLifecycleJob) {
	t.Helper()
	result, err := adminDB.Exec(`UPDATE queue_jobs
		SET status='acknowledged', acknowledged_at=clock_timestamp(),
		    leased_by=NULL, lease_token=NULL, leased_at=NULL, leased_until=NULL,
		    updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND id=$2 AND status IN ('pending', 'leased')`, job.WorkspaceID, job.JobID)
	if err != nil {
		t.Fatalf("acknowledge sandbox lifecycle test job: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		t.Fatalf("acknowledge sandbox lifecycle test job rows = %d, %v; want 1", rows, err)
	}
}

func supersedeSandboxQueueLease(t *testing.T, runtimeDB *sql.DB, adminDB *sql.DB, request queue.EnqueueRequest) (context.Context, context.Context, *queuev1.QueueJob, *queuev1.QueueJob) {
	return supersedeSandboxQueueLeaseAfter(t, runtimeDB, adminDB, request, nil)
}

func supersedeSandboxQueueLeaseAfter(t *testing.T, runtimeDB *sql.DB, adminDB *sql.DB, request queue.EnqueueRequest, beforeTakeover func(context.Context, *queuev1.QueueJob)) (context.Context, context.Context, *queuev1.QueueJob, *queuev1.QueueJob) {
	t.Helper()
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	queued, err := store.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatalf("enqueue source Queue job: %v", err)
	}
	return supersedeExistingSandboxQueueLease(t, store, adminDB, request.WorkspaceID, request.Kind, queued.ID, beforeTakeover)
}

func supersedeExistingSandboxQueueLease(t *testing.T, store *queue.PostgreSQLQueueStore, adminDB *sql.DB, workspaceID workspace.ID, kind string, jobID string, beforeTakeover func(context.Context, *queuev1.QueueJob)) (context.Context, context.Context, *queuev1.QueueJob, *queuev1.QueueJob) {
	t.Helper()
	toProto := func(job *queue.Job) *queuev1.QueueJob {
		return &queuev1.QueueJob{
			Id: job.ID, WorkspaceId: string(job.WorkspaceID), Kind: job.Kind,
			PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			PayloadVersion: int32(job.PayloadVersion), PayloadJson: string(job.PayloadJSON),
			LeasedBy: job.LeasedBy, LeaseToken: job.LeaseToken,
			LeasedUntil:  job.LeasedUntil.UTC().Format(time.RFC3339Nano),
			AttemptCount: int32(job.AttemptCount), MaxAttempts: int32(job.MaxAttempts),
		}
	}
	toContext := func(job *queue.Job) context.Context {
		return withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
			workspaceID: string(job.WorkspaceID), jobID: job.ID, leaseToken: job.LeaseToken, leasedUntil: *job.LeasedUntil,
		})
	}
	lease := func(owner string) *queue.Job {
		t.Helper()
		jobs, err := store.Lease(context.Background(), queue.LeaseRequest{
			WorkspaceID: workspaceID, Kinds: []string{kind}, LeaseOwner: owner,
			MaxJobs: 1, LeaseDuration: time.Minute,
		})
		if err != nil || len(jobs) != 1 || jobs[0].ID != jobID || jobs[0].LeasedUntil == nil {
			t.Fatalf("lease source Queue job = %#v, %v; want %s", jobs, err, jobID)
		}
		return jobs[0]
	}
	first := lease("sandbox-worker-a")
	firstProto := toProto(first)
	firstCtx := toContext(first)
	if beforeTakeover != nil {
		beforeTakeover(firstCtx, firstProto)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, workspaceID, jobID); err != nil {
		t.Fatalf("expire source Queue job: %v", err)
	}
	if reclaimed, err := store.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspaceID, Limit: 1}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim source Queue job = %d, %v; want 1,nil", reclaimed, err)
	}
	second := lease("sandbox-worker-b")
	secondProto := toProto(second)
	secondCtx := toContext(second)
	return firstCtx, secondCtx, firstProto, secondProto
}

func seedSandboxLifecycleLockRow(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		observed_binding_revision, target_provider_resource_id, created_at, updated_at, completed_at
	) VALUES (
		'ws_execution_store', 'sop_lock_order', 'sesn_execution_store', 'sbox_execution_store', 'start', 'completed',
		1, 'provider_execution_store', $1, $1, $1
	)`, now); err != nil {
		t.Fatalf("seed lifecycle lock row: %v", err)
	}
}

func waitForSandboxLifecycleLockWait(t *testing.T, db *sql.DB) {
	waitForSandboxLockWait(t, db, "sandbox_lifecycle_operations")
}

func waitForSandboxLockWait(t *testing.T, db *sql.DB, queryFragment string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := db.QueryRow(`SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type='Lock' AND query LIKE $1
		)`, "%"+queryFragment+"%").Scan(&waiting); err != nil {
			t.Fatalf("inspect %s lock wait: %v", queryFragment, err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sandbox writer did not block on %s", queryFragment)
}
