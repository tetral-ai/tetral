package tetralsandbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLSandboxOutputCaptureStoreClosesBlobStageBeforePublishingResult(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 17, 0, 0, 0, time.UTC)
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation, retain_until, created_at, updated_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_capture_stage',1,
		'running','bind_capture_stage',1,$1,$2,$2)`, now.Add(time.Hour), now); err != nil {
		t.Fatalf("seed capture operation: %v", err)
	}
	store := NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtimeDB))
	work := SandboxOutputCaptureWork{SandboxOutputCaptureJob: SandboxOutputCaptureJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", FinishIdleWriteID: "rwrite_capture_stage", CaptureGeneration: 1,
	}}
	if err := store.StageCapture(ctx, work, nil, nil, nil, false, "", "", now); err != nil {
		t.Fatalf("StageCapture: %v", err)
	}
	if _, err := store.EnsureBlobStage(ctx, work, SandboxOutputCaptureManifestEntry{
		SourcePath: "/mnt/session/outputs/late.txt", BlobPointer: "output-captures/late", SizeBytes: 1,
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, now); err == nil {
		t.Fatal("EnsureBlobStage after parent stage succeeded; want the parent fence to reject late custody")
	}
	transport := &queuev1.QueueJob{
		LeasedUntil: testSandboxLeaseExpiry(),
		Id:          "qjob_capture_stage", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxOutputCapture,
		PartitionKey: queue.FormatSandboxCapturePartitionKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_stage"),
		DedupeKey:    queue.FormatSandboxOutputCaptureDedupeKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_stage", 1),
	}
	if err := store.FinalizeCaptureExhaustion(ctx, transport, now.Add(time.Minute)); err != nil {
		t.Fatalf("FinalizeCaptureExhaustion after stage: %v", err)
	}
	var state string
	var childCount int
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_stage'`).Scan(&state); err != nil {
		t.Fatalf("read capture state: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_output_capture_blobs
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_stage'`).Scan(&childCount); err != nil {
		t.Fatalf("count capture children: %v", err)
	}
	if state != "staged" || childCount != 0 {
		t.Fatalf("capture after late worker/exhaustion = %s with %d children; want staged with no late child", state, childCount)
	}
}

func TestPostgreSQLSandboxOutputCaptureStoreReadsAndFencesCurrentBinding(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 17, 30, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation, retain_until, created_at, updated_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_capture_binding',1,
		'pending','bind_capture_binding',1,$1,$2,$2)`, now.Add(time.Hour), now); err != nil {
		t.Fatalf("seed capture operation: %v", err)
	}
	store := NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtimeDB))
	job := SandboxOutputCaptureJob{WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", FinishIdleWriteID: "rwrite_capture_binding", CaptureGeneration: 1}
	work, current, err := store.LoadCapture(ctx, job, now.Add(time.Minute))
	if err != nil || !current || !work.ProviderAvailable || work.ProviderResourceID != "provider_execution_store" || work.BindingRevision != 1 {
		t.Fatalf("LoadCapture = current %t work %#v err %v; want current binding revision 1", current, work, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at=$1, release_reason='session_delete', updated_at=$1
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("fence binding: %v", err)
	}
	_, current, err = store.LoadCapture(ctx, job, now.Add(3*time.Minute))
	if err != nil || current {
		t.Fatalf("LoadCapture after release fence = current %t err %v; want failed generation", current, err)
	}
	var state, failureKind string
	if err := adminDB.QueryRow(`SELECT state, failure_kind FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_binding'`).Scan(&state, &failureKind); err != nil {
		t.Fatalf("read fenced capture: %v", err)
	}
	if state != "failed" || failureKind != "sandbox_binding_changed" {
		t.Fatalf("fenced capture = %s/%s; want failed/sandbox_binding_changed", state, failureKind)
	}
}

func TestPostgreSQLSandboxOutputCaptureRejectsSupersededSettlement(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation, retain_until, created_at, updated_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_capture_fence',1,
		'pending','bind_capture_fence',1,$1,$2,$2)`, now.Add(time.Hour), now); err != nil {
		t.Fatalf("seed capture operation: %v", err)
	}
	store := NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtimeDB))
	var work SandboxOutputCaptureWork
	firstCtx, secondCtx, _, secondTransport := supersedeSandboxQueueLeaseAfter(t, runtimeDB, adminDB, queue.EnqueueRequest{
		ID: "qjob_capture_fence", WorkspaceID: "ws_execution_store", Kind: queue.KindSandboxOutputCapture,
		PartitionKey:   queue.FormatSandboxCapturePartitionKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_fence"),
		DedupeKey:      queue.FormatSandboxOutputCaptureDedupeKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_fence", 1),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","session_id":"sesn_execution_store","finish_idle_write_id":"rwrite_capture_fence","capture_generation":1}`),
		MaxAttempts:    5,
	}, func(ctx context.Context, transport *queuev1.QueueJob) {
		job, err := DecodeSandboxOutputCaptureJob(transport)
		if err != nil {
			t.Fatalf("DecodeSandboxOutputCaptureJob: %v", err)
		}
		var current bool
		work, current, err = store.LoadCapture(ctx, job, now.Add(time.Minute))
		if err != nil || !current {
			t.Fatalf("LoadCapture = current %t work %#v err %v", current, work, err)
		}
	})
	if err := store.StageCapture(firstCtx, work, nil, nil, nil, false, "", "", now.Add(2*time.Minute)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded StageCapture error = %v; want Queue authority loss", err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_fence'`).Scan(&state); err != nil {
		t.Fatalf("read capture after stale settlement: %v", err)
	}
	if state != "running" {
		t.Fatalf("capture after stale settlement = %q; want running", state)
	}
	secondJob, err := DecodeSandboxOutputCaptureJob(secondTransport)
	if err != nil {
		t.Fatalf("decode successor capture job: %v", err)
	}
	work, current, err := store.LoadCapture(secondCtx, secondJob, now.Add(3*time.Minute))
	if err != nil || !current {
		t.Fatalf("successor LoadCapture = current %t work %#v err %v", current, work, err)
	}
	if err := store.StageCapture(secondCtx, work, nil, nil, nil, false, "", "", now.Add(4*time.Minute)); err != nil {
		t.Fatalf("successor StageCapture: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_fence'`).Scan(&state); err != nil {
		t.Fatalf("read successor capture settlement: %v", err)
	}
	if state != "staged" {
		t.Fatalf("successor capture settlement = %q; want staged", state)
	}
}

func TestPostgreSQLSandboxOutputCaptureLoadAndSettlementDoNotDeadlock(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation, retain_until, created_at, updated_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_capture_interleave',1,
		'pending','bind_capture_interleave',1,$1,$2,$2)`, now.Add(time.Hour), now); err != nil {
		t.Fatalf("seed capture operation: %v", err)
	}
	store := NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtimeDB))
	job := SandboxOutputCaptureJob{WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", FinishIdleWriteID: "rwrite_capture_interleave", CaptureGeneration: 1}
	work, current, err := store.LoadCapture(ctx, job, now)
	if err != nil || !current {
		t.Fatalf("initial LoadCapture = current %t work %#v err %v", current, work, err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		_, _, err := store.LoadCapture(ctx, job, now.Add(time.Second))
		errs <- err
	}()
	go func() {
		defer wait.Done()
		<-start
		errs <- store.StageCapture(ctx, work, nil, nil, nil, false, "", "", now.Add(time.Second))
	}()
	close(start)
	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LoadCapture and StageCapture deadlocked")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("capture interleaving: %v", err)
		}
	}
}

func TestPostgreSQLSandboxOutputCaptureStoreSweepsAndCleansExpiredBlobCustody(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation,
		outcome_state, outcome_digest, retain_until, created_at, updated_at, staged_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_capture_cleanup',1,
		'staged','bind_capture_cleanup',1,
		'staged',$1,$2,$3,$3,$3)`, digest, now.Add(-time.Second), now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed capture operation: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_blobs (
		workspace_id, session_id, finish_idle_write_id, capture_generation, source_path,
		blob_pointer, size_bytes, sha256, state, created_at, updated_at, uploaded_at
	) VALUES ('ws_execution_store','sesn_execution_store','rwrite_capture_cleanup',1,
		'/mnt/session/outputs/result.txt','output-captures/staged-cleanup',1,$1,'uploaded',$2,$2,$2)`, digest, now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed capture blob: %v", err)
	}
	store := NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtimeDB))
	count, err := store.SweepExpiredCaptures(context.Background(), "ws_execution_store", now, 10)
	if err != nil || count != 1 {
		t.Fatalf("SweepExpiredCaptures = (%d,%v); want 1,nil", count, err)
	}
	var state string
	var cleanupGeneration int64
	if err := adminDB.QueryRow(`SELECT state, cleanup_generation FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_cleanup'`).Scan(&state, &cleanupGeneration); err != nil {
		t.Fatalf("read swept capture: %v", err)
	}
	if state != "cleanup_pending" || cleanupGeneration != 1 {
		t.Fatalf("capture cleanup state = %s/%d; want cleanup_pending/1", state, cleanupGeneration)
	}
	var queueStatus string
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id='ws_execution_store' AND kind=$1 AND dedupe_key=$2`,
		queue.KindSandboxOutputCaptureCleanup,
		queue.FormatSandboxOutputCaptureCleanupDedupeKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_cleanup", 1, 1),
	).Scan(&queueStatus); err != nil {
		t.Fatalf("read cleanup Queue job: %v", err)
	}
	if queueStatus != queue.StatusPending {
		t.Fatalf("cleanup Queue status = %q; want pending", queueStatus)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	var jobID string
	if err := adminDB.QueryRow(`SELECT id FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='sandbox_output_capture_cleanup' AND status='pending'`).Scan(&jobID); err != nil {
		t.Fatalf("read cleanup Queue job id: %v", err)
	}
	var work SandboxOutputCaptureCleanupWork
	firstCtx, secondCtx, _, secondTransport := supersedeExistingSandboxQueueLease(t, queueStore, adminDB,
		"ws_execution_store", queue.KindSandboxOutputCaptureCleanup, jobID,
		func(ctx context.Context, transport *queuev1.QueueJob) {
			job, err := DecodeSandboxOutputCaptureCleanupJob(transport)
			if err != nil {
				t.Fatalf("DecodeSandboxOutputCaptureCleanupJob: %v", err)
			}
			var current bool
			work, current, err = store.LoadCaptureCleanup(ctx, job)
			if err != nil || !current || len(work.BlobPointers) != 1 || work.BlobPointers[0] != "output-captures/staged-cleanup" {
				t.Fatalf("LoadCaptureCleanup = current %t work %#v err %v", current, work, err)
			}
		})
	if err := store.CompleteCaptureCleanup(firstCtx, work, now.Add(time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded CompleteCaptureCleanup error = %v; want Queue authority loss", err)
	}
	var retainedBlobs int
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_output_capture_blobs
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_cleanup'`).Scan(&retainedBlobs); err != nil {
		t.Fatalf("count retained Blob custody: %v", err)
	}
	if retainedBlobs != 1 {
		t.Fatalf("Blob custody after stale cleanup = %d; want 1", retainedBlobs)
	}
	secondJob, err := DecodeSandboxOutputCaptureCleanupJob(secondTransport)
	if err != nil {
		t.Fatalf("decode successor cleanup job: %v", err)
	}
	work, current, err := store.LoadCaptureCleanup(secondCtx, secondJob)
	if err != nil || !current {
		t.Fatalf("successor LoadCaptureCleanup = current %t work %#v err %v", current, work, err)
	}
	if err := store.CompleteCaptureCleanup(secondCtx, work, now.Add(time.Second)); err != nil {
		t.Fatalf("successor CompleteCaptureCleanup: %v", err)
	}
	var outcomeState, outcomeDigest string
	if err := adminDB.QueryRow(`SELECT state, outcome_state, outcome_digest FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_cleanup'`).Scan(&state, &outcomeState, &outcomeDigest); err != nil {
		t.Fatalf("read cleaned capture: %v", err)
	}
	if state != "cleaned" || outcomeState != "staged" || outcomeDigest != digest {
		t.Fatalf("cleaned capture = %s/%s/%s; want cleaned/staged/original digest", state, outcomeState, outcomeDigest)
	}
	var blobRows int
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_output_capture_blobs
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND finish_idle_write_id='rwrite_capture_cleanup'`).Scan(&blobRows); err != nil {
		t.Fatalf("count cleaned blob stages: %v", err)
	}
	if blobRows != 0 {
		t.Fatalf("cleaned blob stage count = %d; want zero", blobRows)
	}
}

func TestPostgreSQLSandboxOutputCleanupLocksOperationBeforeSuccessorQueue(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation, outcome_state, outcome_digest,
		retain_until, created_at, updated_at, staged_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_cleanup_lock',1,
		'staged','bind_cleanup_lock',1,'staged','digest_cleanup_lock',$1,$2,$2,$2)`, now.Add(-time.Second), now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed capture cleanup: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLSandboxOutputCaptureStore(client)
	if count, err := store.SweepExpiredCaptures(context.Background(), "ws_execution_store", now, 10); err != nil || count != 1 {
		t.Fatalf("SweepExpiredCaptures = %d, %v; want 1,nil", count, err)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	var jobID, partition string
	if err := adminDB.QueryRow(`SELECT id, partition_key FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_output_capture_cleanup' AND status='pending'`).Scan(&jobID, &partition); err != nil {
		t.Fatalf("read cleanup Queue source: %v", err)
	}
	_, authorityCtx, _, transport := supersedeExistingSandboxQueueLease(t, queueStore, adminDB,
		"ws_execution_store", queue.KindSandboxOutputCaptureCleanup, jobID, nil)
	blocker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var state string
	if err := blocker.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND finish_idle_write_id='rwrite_cleanup_lock' AND capture_generation=1 FOR UPDATE`).Scan(&state); err != nil {
		t.Fatalf("lock cleanup operation: %v", err)
	}
	finalized := make(chan error, 1)
	go func() {
		finalized <- store.FinalizeCaptureCleanupExhaustion(authorityCtx, transport, now.Add(time.Minute))
	}()
	time.Sleep(200 * time.Millisecond)
	lockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sequence int64
	if err := blocker.QueryRowContext(lockCtx, `SELECT last_sequence FROM queue_partition_counters
		WHERE workspace_id='ws_execution_store' AND partition_key=$1 FOR UPDATE`, partition).Scan(&sequence); err != nil {
		t.Fatalf("lock cleanup successor Queue partition while operation is held: %v", err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release cleanup operation: %v", err)
	}
	select {
	case err := <-finalized:
		if err != nil {
			t.Fatalf("FinalizeCaptureCleanupExhaustion after operation release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FinalizeCaptureCleanupExhaustion did not complete after operation release")
	}
	var successors int
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='sandbox_output_capture_cleanup' AND dedupe_key LIKE '%:2'`).Scan(&successors); err != nil {
		t.Fatalf("count cleanup successor: %v", err)
	}
	if successors != 1 {
		t.Fatalf("cleanup successor jobs = %d; want 1", successors)
	}
}

func TestSandboxOutputCleanupRunnerExhaustionQueuesOneSuccessor(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation, outcome_state, outcome_digest,
		retain_until, created_at, updated_at, staged_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','rwrite_cleanup_runner',1,
		'staged','bind_cleanup_runner',1,'staged','digest_cleanup_runner',$1,$2,$2,$2)`, now.Add(-time.Second), now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed capture cleanup: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO sandbox_output_capture_blobs (
		workspace_id, session_id, finish_idle_write_id, capture_generation, source_path,
		blob_pointer, size_bytes, sha256, state, created_at, updated_at, uploaded_at
	) VALUES ('ws_execution_store','sesn_execution_store','rwrite_cleanup_runner',1,
		'/mnt/session/outputs/result.txt','output-captures/cleanup-runner',1,
		'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','uploaded',$1,$1,$1)`, now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed capture cleanup blob: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLSandboxOutputCaptureStore(client)
	if count, err := store.SweepExpiredCaptures(context.Background(), "ws_execution_store", now, 10); err != nil || count != 1 {
		t.Fatalf("SweepExpiredCaptures = %d, %v; want 1,nil", count, err)
	}
	var sourceJobID string
	if err := adminDB.QueryRow(`SELECT id FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_output_capture_cleanup' AND status='pending'`).Scan(&sourceJobID); err != nil {
		t.Fatalf("read cleanup Queue source: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET attempt_count=$3
		WHERE workspace_id=$1 AND id=$2`, "ws_execution_store", sourceJobID, queue.SandboxOutputCaptureCleanupMaxAttempts-1); err != nil {
		t.Fatalf("advance cleanup attempt count: %v", err)
	}
	runner := &SandboxOutputCaptureCleanupRunner{
		Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client)), Store: store,
		BlobStore: &failingDeleteBlobStore{FakeBlobStore: blob.NewFakeBlobStore()},
		Config:    SandboxOutputCaptureRunnerConfig{WorkspaceID: "ws_execution_store", LeaseOwner: "sandbox-cleanup-test", MaxJobs: 1, LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if _, err := runner.RunOnceWithActivity(context.Background()); err != nil {
		t.Fatalf("RunOnceWithActivity: %v", err)
	}
	var sourceStatus string
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id='ws_execution_store' AND id=$1`, sourceJobID).Scan(&sourceStatus); err != nil {
		t.Fatalf("read exhausted cleanup source: %v", err)
	}
	if sourceStatus != queue.StatusDeadLettered {
		t.Fatalf("cleanup source status = %q; want dead_lettered", sourceStatus)
	}
	var successors int
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='sandbox_output_capture_cleanup' AND status='pending' AND dedupe_key LIKE '%:2'`).Scan(&successors); err != nil {
		t.Fatalf("count cleanup successor: %v", err)
	}
	if successors != 1 {
		t.Fatalf("cleanup successor jobs = %d; want 1", successors)
	}
}
