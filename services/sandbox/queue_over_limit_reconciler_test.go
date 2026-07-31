package tetralsandbox

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxQueueOverLimitReconcilerUsesBoundedCensusAndBusinessFinalizer(t *testing.T) {
	now := time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)
	candidates := []queue.PendingAtOrOverBudgetJob{
		{WorkspaceID: workspace.ID("ws_over_limit"), JobID: "qjob_execution", Kind: queue.KindSandboxToolExecute, AttemptCount: 5, MaxAttempts: 5},
		{WorkspaceID: workspace.ID("ws_over_limit"), JobID: "qjob_activation", Kind: queue.KindSandboxActivate, AttemptCount: 6, MaxAttempts: 5},
	}
	reader := &recordingOverLimitReader{candidates: candidates}
	finalizer := &recordingOverLimitFinalizer{results: []bool{true, false}}
	reconciler := &SandboxQueueOverLimitReconciler{Queue: reader, Finalizer: finalizer, Clock: func() time.Time { return now }}

	processed, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d; want one conditional close", processed)
	}
	if !reflect.DeepEqual(reader.limits, []int{SandboxQueueOverLimitBatchSize}) {
		t.Fatalf("census limits = %v; want bounded pass", reader.limits)
	}
	if !reflect.DeepEqual(finalizer.candidates, candidates) {
		t.Fatalf("finalized candidates = %#v; want %#v", finalizer.candidates, candidates)
	}
	if !reflect.DeepEqual(finalizer.times, []time.Time{now, now}) {
		t.Fatalf("finalizer times = %v; want one DB-clock boundary per pass", finalizer.times)
	}
}

func TestPostgreSQLSandboxQueueOverLimitFinalizerCommitsBusinessResultWithDeadLetter(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 16, 30, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	candidate := seedOverLimitSandboxExecution(t, queueStore, "evt_execution_a", now)
	candidate.PayloadJSON = []byte(`{"poison":`)

	finalizer := NewPostgreSQLSandboxQueueOverLimitFinalizer(client)
	updated, err := finalizer.FinalizePendingAtOrOverBudget(ctx, candidate, now.Add(3*time.Second))
	if err != nil || !updated {
		t.Fatalf("FinalizePendingAtOrOverBudget = (%t,%v); want true,nil", updated, err)
	}

	var executionState, resultJSON, queueStatus string
	if err := adminDB.QueryRow(`SELECT execution_state, result_json
		FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND session_thread_id = 'thr_execution_store' AND tool_use_event_id = 'evt_execution_a'`).Scan(&executionState, &resultJSON); err != nil {
		t.Fatalf("read execution result: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id = $1 AND id = $2`, candidate.WorkspaceID, candidate.JobID).Scan(&queueStatus); err != nil {
		t.Fatalf("read Queue status: %v", err)
	}
	if executionState != "terminal_unconsumed" || queueStatus != queue.StatusDeadLettered {
		t.Fatalf("execution/Queue state = %s/%s; want terminal_unconsumed/dead_lettered", executionState, queueStatus)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	errorBody, _ := result["error"].(map[string]any)
	if errorBody["kind"] != "sandbox_execution_unavailable" {
		t.Fatalf("result = %s; want sandbox_execution_unavailable", resultJSON)
	}
}

func TestPostgreSQLSandboxExecutionExhaustionPreservesCommittedDependencyHandoff(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state = 'waiting_activation'
		WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = 'evt_execution_a'`); err != nil {
		t.Fatalf("seed dependency handoff: %v", err)
	}
	job := &queuev1.QueueJob{
		Id: "qjob_old_execution", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxToolExecute,
		PartitionKey: queue.FormatSandboxExecutionPartitionKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a"),
		DedupeKey:    queue.FormatSandboxToolExecuteDedupeKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a", 1),
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	if err := coordinator.FinalizeExhaustedExecution(context.Background(), job); err != nil {
		t.Fatalf("FinalizeExhaustedExecution: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "waiting_activation", 1)
}

func TestPostgreSQLSandboxQueueOverLimitFinalizerRollsBackWhenQueueObservationChanges(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 17, 0, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	candidate := seedOverLimitSandboxExecution(t, queueStore, "evt_execution_b", now)
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET attempt_count = attempt_count + 1
		WHERE workspace_id = $1 AND id = $2`, candidate.WorkspaceID, candidate.JobID); err != nil {
		t.Fatalf("advance Queue observation: %v", err)
	}

	updated, err := NewPostgreSQLSandboxQueueOverLimitFinalizer(client).FinalizePendingAtOrOverBudget(ctx, candidate, now.Add(3*time.Second))
	if err != nil || updated {
		t.Fatalf("FinalizePendingAtOrOverBudget stale = (%t,%v); want false,nil", updated, err)
	}
	var executionState, queueStatus string
	if err := adminDB.QueryRow(`SELECT execution_state FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND session_thread_id = 'thr_execution_store' AND tool_use_event_id = 'evt_execution_b'`).Scan(&executionState); err != nil {
		t.Fatalf("read execution state: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id = $1 AND id = $2`, candidate.WorkspaceID, candidate.JobID).Scan(&queueStatus); err != nil {
		t.Fatalf("read Queue status: %v", err)
	}
	if executionState != "pending" || queueStatus != queue.StatusPending {
		t.Fatalf("execution/Queue state = %s/%s; want pending/pending after rollback", executionState, queueStatus)
	}
}

func seedOverLimitSandboxExecution(t *testing.T, queueStore *queue.PostgreSQLQueueStore, eventID string, now time.Time) queue.PendingAtOrOverBudgetJob {
	t.Helper()
	ctx := context.Background()
	ws := workspace.ID("ws_execution_store")
	payload, err := json.Marshal(map[string]string{
		"workspace_id": ws.String(), "session_id": "sesn_execution_store",
		"session_thread_id": "thr_execution_store", "tool_use_event_id": eventID,
	})
	if err != nil {
		t.Fatalf("encode Queue payload: %v", err)
	}
	jobID := queue.NewJobID()
	job, err := queueStore.Enqueue(ctx, queue.EnqueueRequest{
		ID: jobID, WorkspaceID: ws, Kind: queue.KindSandboxToolExecute,
		PartitionKey:   queue.FormatSandboxExecutionPartitionKey(ws, "sesn_execution_store", "thr_execution_store", eventID),
		DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey(ws, "sesn_execution_store", "thr_execution_store", eventID, 1),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: 1, Now: now,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	leased, err := queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: ws, Kinds: []string{queue.KindSandboxToolExecute}, LeaseOwner: "sandbox-test",
		MaxJobs: 1, LeaseDuration: time.Second, Now: now,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != job.ID {
		t.Fatalf("Lease = %#v err %v; want %s", leased, err, job.ID)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: ws, Limit: 1, Now: now.Add(2 * time.Second),
	}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = (%d,%v); want 1,nil", reclaimed, err)
	}
	candidates, err := queueStore.ListPendingAtOrOverBudget(ctx, queue.ListPendingAtOrOverBudgetRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListPendingAtOrOverBudget: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.JobID == job.ID {
			return candidate
		}
	}
	t.Fatalf("over-limit candidate %s not found in %#v", job.ID, candidates)
	return queue.PendingAtOrOverBudgetJob{}
}

type recordingOverLimitReader struct {
	candidates []queue.PendingAtOrOverBudgetJob
	limits     []int
}

func (r *recordingOverLimitReader) ListPendingAtOrOverBudget(_ context.Context, request queue.ListPendingAtOrOverBudgetRequest) ([]queue.PendingAtOrOverBudgetJob, error) {
	r.limits = append(r.limits, request.Limit)
	return append([]queue.PendingAtOrOverBudgetJob(nil), r.candidates...), nil
}

type recordingOverLimitFinalizer struct {
	results    []bool
	candidates []queue.PendingAtOrOverBudgetJob
	times      []time.Time
}

func (f *recordingOverLimitFinalizer) FinalizePendingAtOrOverBudget(_ context.Context, candidate queue.PendingAtOrOverBudgetJob, now time.Time) (bool, error) {
	f.candidates = append(f.candidates, candidate)
	f.times = append(f.times, now)
	result := f.results[0]
	f.results = f.results[1:]
	return result, nil
}
