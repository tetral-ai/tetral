package tetralsandbox

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxruntime "github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// This file owns the acceptance evidence for the Sandbox execution-result
// notification producers: ordinary settlement (settleSandboxExecutionTx) and
// Session-deletion settlement (release settleWaitersTx) emit one refs-only
// hint per execution they actually terminalize, in the same transaction.

type executionResultNotificationCapture struct {
	mu       sync.Mutex
	payloads []string
	ready    chan struct{}
}

func startExecutionResultNotificationCapture(t *testing.T, client *dbconnect.Client) *executionResultNotificationCapture {
	t.Helper()
	capture := &executionResultNotificationCapture{ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	readyOnce := sync.Once{}
	go func() {
		// The capture listens exactly once: a disconnect fails the test's
		// readiness or payload expectations instead of silently re-listening.
		_ = client.Listen(ctx, "sandbox.test.capture_execution_result", sandboxruntime.ExecutionResultNotificationChannel,
			func() { readyOnce.Do(func() { close(capture.ready) }) },
			func(payload string) {
				capture.mu.Lock()
				capture.payloads = append(capture.payloads, payload)
				capture.mu.Unlock()
			})
	}()
	t.Cleanup(cancel)
	select {
	case <-capture.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("execution result notification capture did not become ready")
	}
	return capture
}

func (c *executionResultNotificationCapture) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.payloads...)
}

func (c *executionResultNotificationCapture) waitForCount(t *testing.T, count int) []string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		payloads := c.snapshot()
		if len(payloads) >= count {
			return payloads
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("execution result notifications = %v; want at least %d", c.snapshot(), count)
	return nil
}

// requireQuiet asserts no notification beyond the baseline count is
// observable at the checkpoint or arrives during the window.
func (c *executionResultNotificationCapture) requireQuiet(t *testing.T, baseline int, window time.Duration) {
	t.Helper()
	if payloads := c.snapshot(); len(payloads) != baseline {
		t.Fatalf("execution result notifications before checkpoint = %v; want exactly %d", payloads, baseline)
	}
	time.Sleep(window)
	if payloads := c.snapshot(); len(payloads) != baseline {
		t.Fatalf("execution result notifications during quiet window = %v; want exactly %d", payloads, baseline)
	}
}

func requireExecutionResultHint(t *testing.T, payload string, workspaceID string, sessionID string, threadID string, toolUseEventID string) {
	t.Helper()
	hint, ok := sandboxruntime.ParseExecutionResultHint(payload)
	if !ok {
		t.Fatalf("execution result notification payload is malformed: %q", payload)
	}
	want := sandboxruntime.ExecutionResultHint{
		WorkspaceID: workspaceID, SessionID: sessionID, SessionThreadID: threadID, ToolUseEventID: toolUseEventID,
	}
	if hint != want {
		t.Fatalf("execution result hint = %+v; want %+v", hint, want)
	}
	if payload != `{"workspace_id":"`+workspaceID+`","session_id":"`+sessionID+`","session_thread_id":"`+threadID+`","tool_use_event_id":"`+toolUseEventID+`"}` {
		t.Fatalf("execution result notification carries more than refs-only identity: %q", payload)
	}
}

func TestSandboxExecutionSettlementEmitsResultNotificationOnCommit(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedSandboxExecutionStoreRows(t, adminDB, "evt_execution_c")
	client := dbconnect.NewClientForTesting(runtimeDB)
	capture := startExecutionResultNotificationCapture(t, client)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)

	settle := func(eventID string, settlement SandboxExecutionSettlement) {
		t.Helper()
		work := loadSandboxExecutionWork(t, coordinator, eventID)
		// Settlement asserts live Queue lease authority; settle through the
		// same leased-job context the production runner holds.
		_, settleCtx, _, _ := supersedeSandboxQueueLease(t, runtimeDB, adminDB, queue.EnqueueRequest{
			ID: queue.NewJobID(), WorkspaceID: workspace.ID(work.Ref.WorkspaceID), Kind: queue.KindSandboxToolExecute,
			PartitionKey:   queue.FormatSandboxExecutionPartitionKey(workspace.ID(work.Ref.WorkspaceID), work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID),
			DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(work.Ref.WorkspaceID), work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID, 1),
			PayloadVersion: 1,
			PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","session_id":"sesn_execution_store","session_thread_id":"thr_execution_store","tool_use_event_id":"` + eventID + `"}`),
			MaxAttempts:    5,
		})
		if err := coordinator.SettleExecution(settleCtx, work, settlement); err != nil {
			t.Fatalf("SettleExecution(%s): %v", eventID, err)
		}
	}
	settle("evt_execution_a", SandboxExecutionSettlement{
		Kind: SandboxExecutionCompleted, ResultJSON: `{"status":"success","result":{"exit_code":0}}`,
	})
	settle("evt_execution_b", SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: "provider_failed", SafeMessage: "provider failed",
	})
	settle("evt_execution_c", SandboxExecutionSettlement{
		Kind: SandboxExecutionUnknownOutcome, ErrorKind: "sandbox_execution_outcome_unknown", SafeMessage: "sandbox execution outcome is unknown",
	})

	payloads := capture.waitForCount(t, 3)
	requireExecutionResultHint(t, payloads[0], "ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a")
	requireExecutionResultHint(t, payloads[1], "ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_b")
	requireExecutionResultHint(t, payloads[2], "ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_c")
}

func TestSettleSandboxExecutionNotificationFollowsTransactionOutcome(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	capture := startExecutionResultNotificationCapture(t, client)
	now := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	settlement := SandboxExecutionSettlement{
		Kind: SandboxExecutionCompleted, ResultJSON: `{"status":"success","result":{"exit_code":0}}`,
	}

	beginDirectTx := func(t *testing.T) (*sql.Tx, *dbconnect.Tx) {
		t.Helper()
		sqlTx, err := runtimeDB.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin direct settlement transaction: %v", err)
		}
		// The runtime role reads and writes through workspace RLS, so the
		// direct transaction scopes itself exactly like the production
		// workspace transaction wrapper does.
		if _, err := sqlTx.ExecContext(context.Background(),
			`SELECT set_config('tetral.workspace_id', 'ws_execution_store', true)`); err != nil {
			t.Fatalf("scope direct settlement transaction: %v", err)
		}
		return sqlTx, dbconnect.NewTxForTesting(sqlTx, client, "sandbox.test.settle")
	}

	// Committed settlement emits exactly one hint, and never before commit.
	committedSQLTx, committedTx := beginDirectTx(t)
	if err := settleSandboxExecutionTx(context.Background(), committedTx, ref, 1, settlement, now); err != nil {
		t.Fatalf("settle in open transaction: %v", err)
	}
	capture.requireQuiet(t, 0, 300*time.Millisecond)
	if err := committedSQLTx.Commit(); err != nil {
		t.Fatalf("commit settlement: %v", err)
	}
	payloads := capture.waitForCount(t, 1)
	requireExecutionResultHint(t, payloads[0], ref.WorkspaceID, ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID)

	// A replayed settlement against the terminal row affects nothing and emits
	// nothing, even though the write transaction commits.
	replaySQLTx, replayTx := beginDirectTx(t)
	if err := settleSandboxExecutionTx(context.Background(), replayTx, ref, 1, settlement, now); err != nil {
		t.Fatalf("replay settlement: %v", err)
	}
	if err := replaySQLTx.Commit(); err != nil {
		t.Fatalf("commit replayed settlement: %v", err)
	}
	capture.waitForCount(t, 1)
	capture.requireQuiet(t, 1, 300*time.Millisecond)

	// A stale generation is fenced before any row changes: no notification.
	seedSandboxExecutionStoreRows(t, adminDB, "evt_execution_c")
	staleSQLTx, staleTx := beginDirectTx(t)
	staleRef := ref
	staleRef.ToolUseEventID = "evt_execution_c"
	if err := settleSandboxExecutionTx(context.Background(), staleTx, staleRef, 99, settlement, now); err != nil {
		t.Fatalf("stale-generation settlement: %v", err)
	}
	if err := staleSQLTx.Commit(); err != nil {
		t.Fatalf("commit stale-generation settlement: %v", err)
	}
	capture.requireQuiet(t, 1, 300*time.Millisecond)

	// A rolled-back settlement publishes neither the result nor the hint.
	seedSandboxExecutionStoreRows(t, adminDB, "evt_execution_d")
	rollbackSQLTx, rollbackTx := beginDirectTx(t)
	rollbackRef := ref
	rollbackRef.ToolUseEventID = "evt_execution_d"
	if err := settleSandboxExecutionTx(context.Background(), rollbackTx, rollbackRef, 1, settlement, now); err != nil {
		t.Fatalf("rollback settlement: %v", err)
	}
	if err := rollbackSQLTx.Rollback(); err != nil {
		t.Fatalf("rollback settlement transaction: %v", err)
	}
	capture.requireQuiet(t, 1, 300*time.Millisecond)
	var state string
	if err := adminDB.QueryRowContext(context.Background(), `SELECT execution_state FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		rollbackRef.WorkspaceID, rollbackRef.SessionID, rollbackRef.SessionThreadID, rollbackRef.ToolUseEventID,
	).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("rolled-back execution state = %q/%v; want pending", state, err)
	}
}

func TestSessionDeleteSettlementEmitsResultNotificationPerSettledWaiter(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
	client := dbconnect.NewClientForTesting(runtimeDB)
	capture := startExecutionResultNotificationCapture(t, client)

	beginRelease := func() *sql.Tx {
		t.Helper()
		sqlTx, err := runtimeDB.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin release transaction: %v", err)
		}
		t.Cleanup(func() { _ = sqlTx.Rollback() })
		if _, err := sqlTx.ExecContext(context.Background(),
			`SELECT set_config('tetral.workspace_id', 'ws_execution_store', true)`); err != nil {
			t.Fatalf("scope release transaction: %v", err)
		}
		releaseTx := dbconnect.NewTxForTesting(sqlTx, client, "sandbox.test.release")
		if _, _, err := EnsureSandboxReleaseTx(
			context.Background(), releaseTx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		); err != nil {
			t.Fatalf("EnsureSandboxReleaseTx: %v", err)
		}
		return sqlTx
	}

	// Exercise the deletion writer's rollback through its owning release
	// boundary. This is not a claim that the public API can delete a running
	// Session; that API has separate running/rescheduling guards.
	rollbackTx := beginRelease()
	var terminalRows int
	if err := rollbackTx.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results
		 WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		   AND execution_state='terminal_unconsumed' AND result_json IS NOT NULL`,
	).Scan(&terminalRows); err != nil || terminalRows != 2 {
		t.Fatalf("deletion results inside transaction = %d/%v; want 2", terminalRows, err)
	}
	capture.requireQuiet(t, 0, 300*time.Millisecond)
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("rollback session-delete release: %v", err)
	}
	capture.requireQuiet(t, 0, 300*time.Millisecond)
	var pendingRows int
	if err := adminDB.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results
		 WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		   AND execution_state='pending' AND result_json IS NULL`,
	).Scan(&pendingRows); err != nil || pendingRows != 2 {
		t.Fatalf("deletion results after rollback = %d pending/%v; want 2 with no result", pendingRows, err)
	}

	// The same release can now commit: both results and both notifications
	// become visible, proving that the rolled-back attempt left no terminal rows.
	committedTx := beginRelease()
	capture.requireQuiet(t, 0, 300*time.Millisecond)
	if err := committedTx.Commit(); err != nil {
		t.Fatalf("commit session-delete release: %v", err)
	}
	payloads := capture.waitForCount(t, 2)
	requireExecutionResultHint(t, payloads[0], "ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a")
	requireExecutionResultHint(t, payloads[1], "ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_b")

	// Replaying the release settles nothing: the waiters are already terminal,
	// so the state fence keeps the repeat silent.
	replayTx := beginRelease()
	if err := replayTx.Commit(); err != nil {
		t.Fatalf("commit session-delete release replay: %v", err)
	}
	capture.requireQuiet(t, 2, 300*time.Millisecond)
}
