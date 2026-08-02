package tetralsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
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

func TestSandboxQueueOverLimitReconcilerContinuesAfterCandidateFailure(t *testing.T) {
	now := time.Date(2026, 7, 31, 16, 15, 0, 0, time.UTC)
	candidates := []queue.PendingAtOrOverBudgetJob{
		{WorkspaceID: workspace.ID("ws_over_limit"), JobID: "qjob_poisoned", Kind: queue.KindSandboxToolExecute, AttemptCount: 5, MaxAttempts: 5},
		{WorkspaceID: workspace.ID("ws_over_limit"), JobID: "qjob_healthy", Kind: queue.KindSandboxActivate, AttemptCount: 5, MaxAttempts: 5},
	}
	reader := &recordingOverLimitReader{candidates: candidates}
	finalizer := &recordingOverLimitFinalizer{results: []bool{false, true}, errors: []error{errors.New("poisoned candidate"), nil}}
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	reconciler := &SandboxQueueOverLimitReconciler{
		Queue: reader, Finalizer: finalizer, Clock: func() time.Time { return now },
	}

	processed, err := reconciler.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "poisoned candidate") {
		t.Fatalf("RunOnce error = %v; want aggregated poisoned-candidate error", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d; want healthy candidate finalized", processed)
	}
	if !reflect.DeepEqual(finalizer.candidates, candidates) {
		t.Fatalf("finalized candidates = %#v; want both candidates", finalizer.candidates)
	}
	if !strings.Contains(logs.String(), `"msg":"sandbox.queue_over_limit.candidate_failed"`) ||
		!strings.Contains(logs.String(), `"queue.job.id":"qjob_poisoned"`) ||
		strings.Contains(logs.String(), "poisoned candidate") {
		t.Fatalf("candidate failure log = %s; want identity-only safe failure", logs.String())
	}
}

func TestPostgreSQLSandboxQueueOverLimitFinalizerCommitsBusinessResultWithDeadLetter(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 16, 30, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	candidate := seedOverLimitSandboxExecution(t, adminDB, queueStore, "evt_execution_a", now)
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
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state = 'waiting_activation'
		WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = 'evt_execution_a'`); err != nil {
		t.Fatalf("seed dependency handoff: %v", err)
	}
	job := &queuev1.QueueJob{
		LeasedUntil: testSandboxLeaseExpiry(),
		Id:          "qjob_old_execution", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxToolExecute,
		PartitionKey: queue.FormatSandboxExecutionPartitionKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a"),
		DedupeKey:    queue.FormatSandboxToolExecuteDedupeKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a", 1),
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	if err := coordinator.FinalizeExhaustedExecution(ctx, job); err != nil {
		t.Fatalf("FinalizeExhaustedExecution: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "waiting_activation", 1)
}

func TestPostgreSQLSandboxExecutionExhaustionHonorsPreProviderFences(t *testing.T) {
	for _, test := range []struct {
		name        string
		mutate      string
		wantKind    string
		wantMessage string
	}{
		{
			name: "accepted cancellation",
			mutate: `UPDATE session_runtime_tool_results SET cancel_requested_at=CURRENT_TIMESTAMP
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`,
			wantKind: "cancelled", wantMessage: "sandbox execution was cancelled",
		},
		{
			name: "session release",
			mutate: `UPDATE session_sandbox_bindings SET release_requested_at=CURRENT_TIMESTAMP, release_reason='session_delete'
				WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`,
			wantKind: "session_deleted", wantMessage: "sandbox execution is no longer available",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			ctx := sandboxTestQueueContext(t, runtimeDB)
			now := time.Now().UTC()
			seedReadySandboxBinding(t, adminDB, now)
			if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
				SET execution_state='preparing', preparation_deadline=$1
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`, now.Add(-time.Minute)); err != nil {
				t.Fatalf("seed expired preparation: %v", err)
			}
			if _, err := adminDB.Exec(test.mutate); err != nil {
				t.Fatalf("seed fence: %v", err)
			}
			job := &queuev1.QueueJob{
				LeasedUntil: testSandboxLeaseExpiry(),
				Id:          "qjob_execution_fenced", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxToolExecute,
				PartitionKey: queue.FormatSandboxExecutionPartitionKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a"),
				DedupeKey:    queue.FormatSandboxToolExecuteDedupeKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a", 1),
			}
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
			if err := coordinator.FinalizeExhaustedExecution(ctx, job); err != nil {
				t.Fatalf("FinalizeExhaustedExecution: %v", err)
			}
			var resultJSON string
			if err := adminDB.QueryRow(`SELECT result_json FROM session_runtime_tool_results
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`).Scan(&resultJSON); err != nil {
				t.Fatalf("read result: %v", err)
			}
			var result struct {
				Error struct {
					Kind    string `json:"kind"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
				t.Fatalf("decode result: %v", err)
			}
			if result.Error.Kind != test.wantKind || result.Error.Message != test.wantMessage {
				t.Fatalf("result = %s; want %s/%s", resultJSON, test.wantKind, test.wantMessage)
			}
		})
	}
}

func TestPostgreSQLSandboxQueueOverLimitFinalizerRollsBackWhenQueueObservationChanges(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 17, 0, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	candidate := seedOverLimitSandboxExecution(t, adminDB, queueStore, "evt_execution_b", now)
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

func TestPostgreSQLSandboxQueueOverLimitFinalizerAdvancesBackgroundReconcileWithDeadLetter(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	now := time.Now().UTC().Add(time.Minute)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	candidate := reclaimBackgroundQueueJobAtBudget(t, adminDB, queueStore, queue.KindSandboxBackgroundReconcile, now)

	updated, err := NewPostgreSQLSandboxQueueOverLimitFinalizer(client).FinalizePendingAtOrOverBudget(context.Background(), candidate, now)
	if err != nil || !updated {
		t.Fatalf("FinalizePendingAtOrOverBudget = (%t,%v); want true,nil", updated, err)
	}

	var generation int64
	var nextPoll time.Time
	if err := adminDB.QueryRow(`SELECT reconcile_generation, next_poll_at FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&generation, &nextPoll); err != nil {
		t.Fatalf("read background task: %v", err)
	}
	if generation != 2 || nextPoll.Sub(now.Add(SandboxBackgroundOutageRetryBackoff)).Abs() >= time.Microsecond {
		t.Fatalf("background task generation/next poll = %d/%v; want 2/%v", generation, nextPoll, now.Add(SandboxBackgroundOutageRetryBackoff))
	}
	var oldStatus string
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, candidate.WorkspaceID, candidate.JobID).Scan(&oldStatus); err != nil {
		t.Fatalf("read exhausted Queue job: %v", err)
	}
	if oldStatus != queue.StatusDeadLettered {
		t.Fatalf("exhausted Queue status = %q; want dead_lettered", oldStatus)
	}
	var createdAt, availableAt time.Time
	if err := adminDB.QueryRow(`SELECT created_at, available_at FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind=$1 AND dedupe_key=$2`,
		queue.KindSandboxBackgroundReconcile,
		queue.FormatSandboxBackgroundReconcileDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", 2),
	).Scan(&createdAt, &availableAt); err != nil {
		t.Fatalf("read successor Queue job: %v", err)
	}
	if createdAt.Sub(now).Abs() >= time.Microsecond || availableAt.Sub(now.Add(SandboxBackgroundOutageRetryBackoff)).Abs() >= time.Microsecond {
		t.Fatalf("successor Queue times = created %v available %v; want %v/%v", createdAt, availableAt, now, now.Add(SandboxBackgroundOutageRetryBackoff))
	}
}

func TestPostgreSQLSandboxQueueOverLimitFinalizerSettlesSubmittedBackgroundCommandUnknown(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	now := time.Now().UTC().Add(time.Minute)
	requestID := "request_background_submitted"
	seedBackgroundCommandReceipt(t, adminDB, requestID, "stdin", "submitted", now.Add(-time.Minute))
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET status='acknowledged', acknowledged_at=$1, updated_at=$1
		WHERE workspace_id='ws_execution_store' AND kind=$2`, now, queue.KindSandboxBackgroundReconcile); err != nil {
		t.Fatalf("close seed reconcile job: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	job, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: "ws_execution_store", Kind: queue.KindSandboxBackgroundCommand,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey("ws_execution_store", "sesn_execution_store", "task_execution"),
		DedupeKey:      queue.FormatSandboxBackgroundCommandDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", requestID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","session_id":"sesn_execution_store","task_id":"task_execution","request_id":"request_background_submitted"}`),
		MaxAttempts:    1, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue background command: %v", err)
	}
	candidate := reclaimQueueJobAtBudget(t, adminDB, queueStore, job.ID, queue.KindSandboxBackgroundCommand, now)

	updated, err := NewPostgreSQLSandboxQueueOverLimitFinalizer(client).FinalizePendingAtOrOverBudget(context.Background(), candidate, now.Add(3*time.Second))
	if err != nil || !updated {
		t.Fatalf("FinalizePendingAtOrOverBudget = (%t,%v); want true,nil", updated, err)
	}
	var operationState, resultJSON, queueStatus string
	if err := adminDB.QueryRow(`SELECT background_operation_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND session_thread_id='thr_execution_store' AND tool_use_event_id='evt_background_submitted'`).Scan(&operationState, &resultJSON); err != nil {
		t.Fatalf("read background operation: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, candidate.WorkspaceID, candidate.JobID).Scan(&queueStatus); err != nil {
		t.Fatalf("read Queue job: %v", err)
	}
	if operationState != "terminal" || queueStatus != queue.StatusDeadLettered || !strings.Contains(resultJSON, `"kind":"sandbox_background_outcome_unknown"`) {
		t.Fatalf("background operation/Queue = %s/%s result %s; want terminal/dead_lettered unknown outcome", operationState, queueStatus, resultJSON)
	}
	var taskStatus string
	if err := adminDB.QueryRow(`SELECT status FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read background task: %v", err)
	}
	if taskStatus != "running" {
		t.Fatalf("background task status = %q; explicit command exhaustion must not terminalize it", taskStatus)
	}
}

func TestPostgreSQLSandboxQueueOverLimitFinalizerSettlesMemoryProjection(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	const (
		workspaceID = "ws_execution_store"
		sessionID   = "sesn_execution_store"
		threadID    = "thr_execution_store"
		storeID     = "memstore_projection_exhausted"
		writeID     = "evt_memory_projection_exhausted"
	)
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		memory_projection_state, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 'memory', 'hash_projection_exhausted', 'memory',
		'{"action":"create","path":"note.md","content":"x"}', 'committed',
		'{"status":"completed","action":"create","path":"/note.md"}', 'pending', $5, $5)`,
		workspaceID, sessionID, threadID, writeID, now); err != nil {
		t.Fatalf("seed memory projection: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	payload, err := json.Marshal(map[string]string{
		"workspace_id": workspaceID, "session_id": sessionID,
		"memory_store_id": storeID, "memory_write_id": writeID,
	})
	if err != nil {
		t.Fatalf("encode projection payload: %v", err)
	}
	job, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxMemoryProjection,
		PartitionKey:   queue.FormatSandboxMemoryPartitionKey(workspaceID, storeID),
		DedupeKey:      queue.FormatSandboxMemoryProjectionDedupeKey(workspaceID, storeID, writeID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: 1, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue projection: %v", err)
	}
	candidate := reclaimQueueJobAtBudget(t, adminDB, queueStore, job.ID, queue.KindSandboxMemoryProjection, now)
	updated, err := NewPostgreSQLSandboxQueueOverLimitFinalizer(client).FinalizePendingAtOrOverBudget(context.Background(), candidate, now.Add(3*time.Second))
	if err != nil || !updated {
		t.Fatalf("FinalizePendingAtOrOverBudget = (%t,%v); want true,nil", updated, err)
	}
	var projectionState, resultJSON, queueStatus string
	if err := adminDB.QueryRow(`SELECT memory_projection_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND tool_use_event_id=$2`, workspaceID, writeID).Scan(&projectionState, &resultJSON); err != nil {
		t.Fatalf("read memory projection result: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, workspaceID, job.ID).Scan(&queueStatus); err != nil {
		t.Fatalf("read projection Queue job: %v", err)
	}
	if projectionState != "failed" || queueStatus != queue.StatusDeadLettered || !strings.Contains(resultJSON, `"error_code":"projection_refresh_failed"`) {
		t.Fatalf("projection/Queue = %s/%s result %s; want failed/dead_lettered projection_refresh_failed", projectionState, queueStatus, resultJSON)
	}
}

func TestPostgreSQLSandboxMemoryProjectionRejectsSupersededSettlement(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	const (
		workspaceID = "ws_execution_store"
		sessionID   = "sesn_execution_store"
		threadID    = "thr_execution_store"
		storeID     = "memstore_projection_fence"
		writeID     = "evt_memory_projection_fence"
	)
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		memory_projection_state, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 'memory', 'hash_projection_fence', 'memory',
		'{"action":"create","path":"note.md","content":"x"}', 'committed',
		'{"status":"completed","action":"create","path":"/note.md"}', 'pending', $5, $5)`,
		workspaceID, sessionID, threadID, writeID, now); err != nil {
		t.Fatalf("seed memory projection: %v", err)
	}
	payload, err := json.Marshal(map[string]string{
		"workspace_id": workspaceID, "session_id": sessionID,
		"memory_store_id": storeID, "memory_write_id": writeID,
	})
	if err != nil {
		t.Fatalf("encode projection payload: %v", err)
	}
	firstCtx, secondCtx, _, _ := supersedeSandboxQueueLease(t, runtimeDB, adminDB, queue.EnqueueRequest{
		ID: "qjob_mem_fence", WorkspaceID: workspaceID, Kind: queue.KindSandboxMemoryProjection,
		PartitionKey:   queue.FormatSandboxMemoryPartitionKey(workspaceID, storeID),
		DedupeKey:      queue.FormatSandboxMemoryProjectionDedupeKey(workspaceID, storeID, writeID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: 5,
	})
	store := NewPostgreSQLSandboxMemoryProjectionStore(dbconnect.NewClientForTesting(runtimeDB))
	work := SandboxMemoryProjectionWork{WorkspaceID: workspaceID, SessionID: sessionID, MemoryStoreID: storeID, MemoryWriteID: writeID}
	if err := store.SettleProjection(firstCtx, work, "refreshed", "", now); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded SettleProjection error = %v; want Queue authority loss", err)
	}
	var state, resultJSON string
	if err := adminDB.QueryRow(`SELECT memory_projection_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND tool_use_event_id=$2`, workspaceID, writeID).Scan(&state, &resultJSON); err != nil {
		t.Fatalf("read projection after stale settlement: %v", err)
	}
	if state != "pending" || strings.Contains(resultJSON, "projection_refreshed") {
		t.Fatalf("projection after stale settlement = %s/%s; want unchanged pending result", state, resultJSON)
	}
	if err := store.SettleProjection(secondCtx, work, "refreshed", "", now); err != nil {
		t.Fatalf("successor SettleProjection: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT memory_projection_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND tool_use_event_id=$2`, workspaceID, writeID).Scan(&state, &resultJSON); err != nil {
		t.Fatalf("read successor projection: %v", err)
	}
	if state != "refreshed" || !strings.Contains(resultJSON, `"projection_refreshed":true`) {
		t.Fatalf("successor projection = %s/%s; want refreshed result", state, resultJSON)
	}
}

func reclaimBackgroundQueueJobAtBudget(t *testing.T, adminDB *sql.DB, queueStore *queue.PostgreSQLQueueStore, kind string, now time.Time) queue.PendingAtOrOverBudgetJob {
	t.Helper()
	var jobID string
	if err := adminDB.QueryRow(`UPDATE queue_jobs SET max_attempts=1, available_at=$2, updated_at=$2
		WHERE workspace_id='ws_execution_store' AND kind=$1 RETURNING id`, kind, now).Scan(&jobID); err != nil {
		t.Fatalf("prepare background Queue job: %v", err)
	}
	return reclaimQueueJobAtBudget(t, adminDB, queueStore, jobID, kind, now)
}

func reclaimQueueJobAtBudget(t *testing.T, adminDB *sql.DB, queueStore *queue.PostgreSQLQueueStore, jobID string, kind string, now time.Time) queue.PendingAtOrOverBudgetJob {
	t.Helper()
	ctx := context.Background()
	leased, err := queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: "ws_execution_store", Kinds: []string{kind}, LeaseOwner: "sandbox-background-test",
		MaxJobs: 1, LeaseDuration: time.Second, Now: now,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != jobID {
		t.Fatalf("Lease = %#v err %v; want %s", leased, err, jobID)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, leased[0].WorkspaceID, jobID); err != nil {
		t.Fatalf("expire Queue lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: "ws_execution_store", Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = (%d,%v); want 1,nil", reclaimed, err)
	}
	candidates, err := queueStore.ListPendingAtOrOverBudget(ctx, queue.ListPendingAtOrOverBudgetRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListPendingAtOrOverBudget: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.JobID == jobID {
			return candidate
		}
	}
	t.Fatalf("over-limit candidate %s not found", jobID)
	return queue.PendingAtOrOverBudgetJob{}
}

func seedBackgroundCommandReceipt(t *testing.T, adminDB *sql.DB, requestID string, kind string, state string, now time.Time) {
	t.Helper()
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, background_write_sequence,
		created_at, updated_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','evt_background_submitted','sandbox_background',
		'hash_background_submitted','write_stdin','{}','committed',$1,$2,$3,'task_execution',0,1,$4,$4)`,
		kind, state, requestID, now); err != nil {
		t.Fatalf("seed background command receipt: %v", err)
	}
}

func seedOverLimitSandboxExecution(t *testing.T, adminDB *sql.DB, queueStore *queue.PostgreSQLQueueStore, eventID string, now time.Time) queue.PendingAtOrOverBudgetJob {
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
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, ws, job.ID); err != nil {
		t.Fatalf("expire Queue lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: ws, Limit: 1,
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
	errors     []error
	candidates []queue.PendingAtOrOverBudgetJob
	times      []time.Time
}

func (f *recordingOverLimitFinalizer) FinalizePendingAtOrOverBudget(_ context.Context, candidate queue.PendingAtOrOverBudgetJob, now time.Time) (bool, error) {
	f.candidates = append(f.candidates, candidate)
	f.times = append(f.times, now)
	result := f.results[0]
	f.results = f.results[1:]
	var err error
	if len(f.errors) > 0 {
		err = f.errors[0]
		f.errors = f.errors[1:]
	}
	return result, err
}
