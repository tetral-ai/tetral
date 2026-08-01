package tetralsandbox

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLSandboxOutputCaptureStoreClosesBlobStageBeforePublishingResult(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
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
	if err := store.StageCapture(context.Background(), work, nil, nil, nil, false, "", "", now); err != nil {
		t.Fatalf("StageCapture: %v", err)
	}
	if _, err := store.EnsureBlobStage(context.Background(), work, SandboxOutputCaptureManifestEntry{
		SourcePath: "/mnt/session/outputs/late.txt", BlobPointer: "output-captures/late", SizeBytes: 1,
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, now); err == nil {
		t.Fatal("EnsureBlobStage after parent stage succeeded; want the parent fence to reject late custody")
	}
	transport := &queuev1.QueueJob{
		Id: "qjob_capture_stage", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxOutputCapture,
		PartitionKey: queue.FormatSandboxCapturePartitionKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_stage"),
		DedupeKey:    queue.FormatSandboxOutputCaptureDedupeKey("ws_execution_store", "sesn_execution_store", "rwrite_capture_stage", 1),
	}
	if err := store.FinalizeCaptureExhaustion(context.Background(), transport, now.Add(time.Minute)); err != nil {
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
	work, current, err := store.LoadCapture(context.Background(), job, now.Add(time.Minute))
	if err != nil || !current || !work.ProviderAvailable || work.ProviderResourceID != "provider_execution_store" || work.BindingRevision != 1 {
		t.Fatalf("LoadCapture = current %t work %#v err %v; want current binding revision 1", current, work, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at=$1, release_reason='session_delete', updated_at=$1
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("fence binding: %v", err)
	}
	_, current, err = store.LoadCapture(context.Background(), job, now.Add(3*time.Minute))
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
	work, current, err := store.LoadCaptureCleanup(context.Background(), SandboxOutputCaptureCleanupJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", FinishIdleWriteID: "rwrite_capture_cleanup",
		CaptureGeneration: 1, CleanupGeneration: 1,
	})
	if err != nil || !current || len(work.BlobPointers) != 1 || work.BlobPointers[0] != "output-captures/staged-cleanup" {
		t.Fatalf("LoadCaptureCleanup = current %t work %#v err %v", current, work, err)
	}
	if err := store.CompleteCaptureCleanup(context.Background(), work, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteCaptureCleanup: %v", err)
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
