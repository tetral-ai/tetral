package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the AwaitSandboxExecution notification acceptance evidence:
// real PostgreSQL LISTEN/NOTIFY for the wake path, explicit query-tracer
// barriers for read/wait race placement, and per-statement accounting that
// separates verification transactions from RPC entry validation and
// transaction/setup commands.

const (
	awaitTraceVerificationRead = "FROM session_runtime_tool_results"
	awaitTraceEntryValidation  = "FROM session_runtime_bindings"
	awaitTraceListen           = "LISTEN " + sandboxmodel.ExecutionResultNotificationChannel
)

type bridgeTraceEntry struct {
	sql string
	at  time.Time
}

// bridgeExecutionQueryTracer records every completed statement on a traced
// runtime pool and can park one matched statement at its completion barrier so
// a test can commit between exact await-loop steps.
type bridgeExecutionQueryTracer struct {
	mu      sync.Mutex
	entries []bridgeTraceEntry
	barrier *bridgeTraceBarrier
}

type bridgeTraceBarrier struct {
	mu         sync.Mutex
	armed      bool
	after      string
	afterSeen  bool
	pattern    string
	occurrence int
	seen       int
	fired      chan struct{}
	release    chan struct{}
	firedOnce  sync.Once
}

type bridgeTraceSQLContextKey struct{}

func (tr *bridgeExecutionQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, bridgeTraceSQLContextKey{}, data.SQL)
}

func (tr *bridgeExecutionQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sqlText, _ := ctx.Value(bridgeTraceSQLContextKey{}).(string)
	tr.mu.Lock()
	tr.entries = append(tr.entries, bridgeTraceEntry{sql: sqlText, at: time.Now()})
	barrier := tr.barrier
	tr.mu.Unlock()
	if barrier == nil {
		return
	}
	barrier.mu.Lock()
	fire := false
	if barrier.armed {
		if !barrier.afterSeen && barrier.after != "" && strings.Contains(sqlText, barrier.after) {
			barrier.afterSeen = true
		}
		if (barrier.after == "" || barrier.afterSeen) && barrier.seen < barrier.occurrence && strings.Contains(sqlText, barrier.pattern) {
			barrier.seen++
			fire = barrier.seen == barrier.occurrence
		}
	}
	barrier.mu.Unlock()
	if fire {
		barrier.firedOnce.Do(func() { close(barrier.fired) })
		<-barrier.release
	}
}

func (tr *bridgeExecutionQueryTracer) reset() {
	tr.mu.Lock()
	tr.entries = nil
	tr.mu.Unlock()
}

func (tr *bridgeExecutionQueryTracer) countSQL(match string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	count := 0
	for _, entry := range tr.entries {
		if strings.Contains(entry.sql, match) {
			count++
		}
	}
	return count
}

func (tr *bridgeExecutionQueryTracer) entriesSQL(match string) []bridgeTraceEntry {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var entries []bridgeTraceEntry
	for _, entry := range tr.entries {
		if strings.Contains(entry.sql, match) {
			entries = append(entries, entry)
		}
	}
	return entries
}

// countStatementsWithPrefix counts completed statements by case-insensitive
// prefix, so read-only transactions ("begin read only") count as BEGIN.
func (tr *bridgeExecutionQueryTracer) countStatementsWithPrefix(prefix string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	count := 0
	for _, entry := range tr.entries {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(entry.sql)), prefix) {
			count++
		}
	}
	return count
}

func (tr *bridgeExecutionQueryTracer) waitForSQLCount(t *testing.T, match string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if tr.countSQL(match) >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("statements containing %q = %d; want at least %d", match, tr.countSQL(match), want)
}

func (tr *bridgeExecutionQueryTracer) armBarrier(after string, pattern string, occurrence int) (fired chan struct{}, release chan struct{}) {
	barrier := &bridgeTraceBarrier{
		armed: true, after: after, pattern: pattern, occurrence: occurrence,
		fired: make(chan struct{}), release: make(chan struct{}),
	}
	tr.mu.Lock()
	tr.barrier = barrier
	tr.mu.Unlock()
	return barrier.fired, barrier.release
}

// seedAwaitExecutionNotificationFixture builds one accepted Sandbox execution
// through the production Bridge RPCs and returns its await target.
func seedAwaitExecutionNotificationFixture(t *testing.T, store *PostgreSQLBridgeAPIStore, admin *sql.DB, suffix string) (*bridgev1.RuntimeScope, string) {
	t.Helper()
	const workspaceID = "default"
	sessionID := "sesn_exec_notify_" + suffix
	threadID := "thr_exec_notify_" + suffix
	bindingID := "bind_exec_notify_" + suffix
	podUID := "pod_exec_notify_" + suffix
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableOrdinaryToolUseForTest(
		t, store, scope, "mreq_exec_notify_"+suffix, "call_exec_notify_"+suffix,
		"exec_command", `{"cmd":"printf ok"}`,
	)
	accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUseEventID,
	})
	if err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("AcceptSandboxExecution = %#v/%v; want committed", accepted, err)
	}
	return scope, toolUseEventID
}

// commitAwaitExecutionSettlement terminalizes one accepted execution through
// a fixture transaction. With notify set it emits the hint through the real
// production emitter in the same transaction; with notify clear it simulates
// a write no listener could have observed.
func commitAwaitExecutionSettlement(t *testing.T, admin *sql.DB, scope *bridgev1.RuntimeScope, toolUseEventID string, resultJSON string, notify bool) {
	t.Helper()
	tx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin settlement fixture: %v", err)
	}
	result, err := tx.ExecContext(context.Background(),
		`UPDATE session_runtime_tool_results
		    SET execution_state='terminal_unconsumed', result_json=$5, result_digest=$6, updated_at=now()
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND tool_use_event_id=$4 AND execution_state NOT IN ('terminal_unconsumed','consumed')`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
		resultJSON, sha256Hex(resultJSON),
	)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("settle execution fixture: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("settlement fixture rows = %d/%v; want 1", affected, err)
	}
	if notify {
		if err := sandboxmodel.NotifyExecutionResultTx(context.Background(),
			dbconnect.NewTxForTesting(tx, dbconnect.NewClientForTesting(admin), "bridge.test.settle_execution"),
			sandboxmodel.ExecutionResultHint{
				WorkspaceID:     scope.GetWorkspaceId(),
				SessionID:       scope.GetSessionId(),
				SessionThreadID: scope.GetSessionThreadId(),
				ToolUseEventID:  toolUseEventID,
			}); err != nil {
			_ = tx.Rollback()
			t.Fatalf("emit settlement fixture notification: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit settlement fixture: %v", err)
	}
}

func emitRawExecutionResultPayload(t *testing.T, admin *sql.DB, payload string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`SELECT pg_notify($1, $2)`, sandboxmodel.ExecutionResultNotificationChannel, payload,
	); err != nil {
		t.Fatalf("emit raw execution result payload: %v", err)
	}
}

type awaitExecutionOutcome struct {
	response *bridgev1.AwaitSandboxExecutionResponse
	err      error
	elapsed  time.Duration
}

func startAwaitSandboxExecution(store *PostgreSQLBridgeAPIStore, ctx context.Context, scope *bridgev1.RuntimeScope, toolUseEventID string) chan awaitExecutionOutcome {
	done := make(chan awaitExecutionOutcome, 1)
	go func() {
		started := time.Now()
		response, err := store.AwaitSandboxExecution(ctx, &bridgev1.AwaitSandboxExecutionRequest{
			Scope: scope, ToolUseEventId: toolUseEventID,
		})
		done <- awaitExecutionOutcome{response: response, err: err, elapsed: time.Since(started)}
	}()
	return done
}

func requireAwaitCompleted(t *testing.T, done chan awaitExecutionOutcome, resultJSON string) awaitExecutionOutcome {
	t.Helper()
	select {
	case outcome := <-done:
		if outcome.err != nil {
			t.Fatalf("AwaitSandboxExecution error = %v; want completed", outcome.err)
		}
		if outcome.response.GetCompleted().GetResultJson() != resultJSON {
			t.Fatalf("AwaitSandboxExecution result = %q; want %q", outcome.response.GetCompleted().GetResultJson(), resultJSON)
		}
		return outcome
	case <-time.After(10 * time.Second):
		t.Fatal("AwaitSandboxExecution did not return")
	}
	return awaitExecutionOutcome{}
}

func requireAwaitBlocked(t *testing.T, done chan awaitExecutionOutcome, window time.Duration) {
	t.Helper()
	select {
	case outcome := <-done:
		t.Fatalf("AwaitSandboxExecution returned while still blocked: %#v/%v", outcome.response, outcome.err)
	case <-time.After(window):
	}
}

// startAwaitExecutionResultListener runs the production listener for store and
// waits until its LISTEN is visible on the traced pool.
func startAwaitExecutionResultListener(t *testing.T, store *PostgreSQLBridgeAPIStore, tracer *bridgeExecutionQueryTracer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- store.RunExecutionResultListener(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("RunExecutionResultListener: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("execution result listener did not stop")
		}
	})
	tracer.waitForSQLCount(t, awaitTraceListen, 1)
}

func newAwaitNotificationTracedStore(t *testing.T, runtime *sql.DB, tracer *bridgeExecutionQueryTracer) *PostgreSQLBridgeAPIStore {
	t.Helper()
	traced := storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(traced))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	return store
}

const awaitNotificationTerminalResult = `{"status":"success","result":{"exit_code":0,"stdout":"ok"}}`

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionWakesOnResultNotification(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "wake")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("notification wake latency = %s; want well below the 1s fallback", latency)
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads = %d; want exactly 2 (initial + terminal)", reads)
	}
	if validations := tracer.countSQL(awaitTraceEntryValidation); validations != 1 {
		t.Fatalf("entry validation transactions = %d; want 1", validations)
	}
	if waiters := store.executionResultWake().waiterCount(); waiters != 0 {
		t.Fatalf("live waiters after completion = %d; want 0", waiters)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionReadsPreCommittedResult(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "precommit")

	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 1 {
		t.Fatalf("verification reads = %d; want 1 (committed result returns on the initial read)", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionCommitDuringInitialRead(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "duringread")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	// Park the waiter at the completion of the first verification read's
	// result statement: the statement observed the pending row, and the
	// settlement commits while the read transaction is still open.
	fired, release := tracer.armBarrier("", awaitTraceVerificationRead, 1)
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("initial verification read did not reach the barrier")
	}
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	close(release)

	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("in-window commit wake latency = %s; want notification path, not fallback", latency)
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads = %d; want 2 (stale pending read + post-wake terminal)", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionCommitBetweenReadAndWait(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "betweenread")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	// Park the waiter at the COMMIT closing the first verification read: the
	// pending read is durable, and the settlement commits before the waiter
	// can block. The wake snapshot taken before the read must make the wait
	// return immediately instead of sleeping into the fallback.
	fired, release := tracer.armBarrier(awaitTraceVerificationRead, "commit", 1)
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("verification read transaction did not reach the commit barrier")
	}
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	close(release)

	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("post-read commit wake latency = %s; want notification path, not fallback", latency)
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads = %d; want 2 (pending read + immediate woken read)", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionRepeatedCyclesVerifyDurably(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "cycles")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	// A hint without a durable transition wakes a verification read that
	// observes pending and blocks again; duplicates of it behave identically.
	hint := sandboxmodel.ExecutionResultHint{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(),
		SessionThreadID: scope.GetSessionThreadId(), ToolUseEventID: toolUseEventID,
	}
	payload, err := marshalBridgeJSON(hint)
	if err != nil {
		t.Fatalf("marshal hint fixture: %v", err)
	}
	emitRawExecutionResultPayload(t, admin, payload)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 2)
	emitRawExecutionResultPayload(t, admin, payload)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 3)
	requireAwaitBlocked(t, done, 150*time.Millisecond)

	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 4 {
		t.Fatalf("verification reads = %d; want 4 (initial + 2 spurious + terminal)", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionIgnoresUnrelatedAndMalformedHints(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "isolated")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	emitRawExecutionResultPayload(t, admin, "not-json")
	emitRawExecutionResultPayload(t, admin, `{"workspace_id":"default"}`)
	emitRawExecutionResultPayload(t, admin, `{"workspace_id":"default","session_id":"`+scope.GetSessionId()+
		`","session_thread_id":"`+scope.GetSessionThreadId()+`","tool_use_event_id":"evt_unrelated_tool"}`)
	otherWorkspaceHint := sandboxmodel.ExecutionResultHint{
		WorkspaceID: "ws_other", SessionID: scope.GetSessionId(),
		SessionThreadID: scope.GetSessionThreadId(), ToolUseEventID: toolUseEventID,
	}
	otherPayload, err := marshalBridgeJSON(otherWorkspaceHint)
	if err != nil {
		t.Fatalf("marshal other-workspace hint: %v", err)
	}
	emitRawExecutionResultPayload(t, admin, otherPayload)

	// The window stays well under the 1 s fallback so any extra read can only
	// come from a misrouted hint.
	requireAwaitBlocked(t, done, 300*time.Millisecond)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 1 {
		t.Fatalf("verification reads after unrelated hints = %d; want 1", reads)
	}

	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads = %d; want 2", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionFallbackCoversMissedNotification(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "fallback")

	// No listener runs: the settlement is invisible to the wake path and only
	// the one-second fallback can observe it.
	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, false)
	outcome := requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if outcome.elapsed < 800*time.Millisecond || outcome.elapsed > 4*time.Second {
		t.Fatalf("fallback wake elapsed = %s; want about one fallback interval", outcome.elapsed)
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads = %d; want 2 (initial + fallback)", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionReconnectTriggersCatchUp(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "reconnect")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	terminateListener := func() {
		t.Helper()
		var terminated int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_stat_activity
			  WHERE datname = current_database() AND pid <> pg_backend_pid()
			    AND query = $1`, awaitTraceListen,
		).Scan(&terminated); err != nil {
			t.Fatalf("count execution result listener backends: %v", err)
		}
		if terminated != 1 {
			t.Fatalf("execution result listener backends = %d; want 1", terminated)
		}
		if _, err := admin.ExecContext(context.Background(),
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			  WHERE datname = current_database() AND pid <> pg_backend_pid()
			    AND query = $1`, awaitTraceListen,
		); err != nil {
			t.Fatalf("terminate execution result listener: %v", err)
		}
	}

	// Two disconnects: each reconnect re-LISTENs and its readiness catch-up
	// wakes the waiter without waiting for a notification or the fallback.
	for kill := 1; kill <= 2; kill++ {
		terminateListener()
		// The tracer was reset after the initial LISTEN, so the first
		// reconnect is the first post-reset LISTEN entry.
		tracer.waitForSQLCount(t, awaitTraceListen, kill)
		tracer.waitForSQLCount(t, awaitTraceVerificationRead, kill+1)
		relisten := tracer.entriesSQL(awaitTraceListen)
		catchUp := tracer.entriesSQL(awaitTraceVerificationRead)
		if gap := catchUp[kill].at.Sub(relisten[kill-1].at); gap > 2*time.Second {
			t.Fatalf("catch-up read after reconnect %d came %s after LISTEN; want prompt", kill, gap)
		}
	}
	var listenBackends int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM pg_stat_activity
		  WHERE datname = current_database() AND query = $1`, awaitTraceListen,
	).Scan(&listenBackends); err != nil {
		t.Fatalf("count listener backends: %v", err)
	}
	if listenBackends != 1 {
		t.Fatalf("listener backends after repeated reconnects = %d; want exactly 1 (no leak)", listenBackends)
	}

	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("post-reconnect notification latency = %s; want notification path", latency)
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 4 {
		t.Fatalf("verification reads = %d; want 4 (initial + 2 catch-ups + terminal)", reads)
	}
	if waiters := store.executionResultWake().waiterCount(); waiters != 0 {
		t.Fatalf("live waiters after completion = %d; want 0", waiters)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionBoundedIdleLoad(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "idleload")

	tracer.reset()
	started := time.Now()
	_, err := store.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUseEventID,
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("AwaitSandboxExecution idle wait error = %v; want DeadlineExceeded", err)
	}
	elapsed := time.Since(started)
	if elapsed < sandboxExecutionWaitTimeout || elapsed > sandboxExecutionWaitTimeout+5*time.Second {
		t.Fatalf("idle wait elapsed = %s; want the existing 30s internal deadline", elapsed)
	}
	reads := tracer.countSQL(awaitTraceVerificationRead)
	validations := tracer.countSQL(awaitTraceEntryValidation)
	begins := tracer.countStatementsWithPrefix("begin")
	commits := tracer.countStatementsWithPrefix("commit")
	setConfigs := tracer.countSQL("set_config")
	t.Logf("idle wait accounting: verification reads=%d entry validations=%d BEGIN=%d COMMIT=%d set_config=%d elapsed=%s",
		reads, validations, begins, commits, setConfigs, elapsed)
	if reads > 31 {
		t.Fatalf("verification transactions over a 30s idle wait = %d; want at most 31 with the 1s fallback", reads)
	}
	if reads < 28 {
		t.Fatalf("verification transactions over a 30s idle wait = %d; want the 1s fallback cadence", reads)
	}
	if validations != 1 {
		t.Fatalf("entry validation transactions = %d; want 1", validations)
	}
	if begins != reads+validations || commits != reads+validations {
		t.Fatalf("transaction commands = %d/%d; want one BEGIN+COMMIT per verification/validation (%d)", begins, commits, reads+validations)
	}
	if waiters := store.executionResultWake().waiterCount(); waiters != 0 {
		t.Fatalf("live waiters after deadline = %d; want 0", waiters)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionFanOutAcrossBridgeInstances(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracerOne := &bridgeExecutionQueryTracer{}
	tracerTwo := &bridgeExecutionQueryTracer{}
	storeOne := newAwaitNotificationTracedStore(t, runtime, tracerOne)
	storeTwo := newAwaitNotificationTracedStore(t, runtime, tracerTwo)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, storeOne, admin, "fanout")

	// A second accepted execution on the same session (same model request)
	// gives the hub a sibling identity that must stay asleep while the target
	// advances.
	otherWritten, err := storeOne.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_exec_notify_fanout_other_tool", ModelRequestId: "mreq_exec_notify_fanout",
		ToolDeclaration: bridgeToolDeclarationWithRouteForTest("call_exec_notify_fanout_other", "exec_command", `{"cmd":"printf other"}`, "allow"),
	})
	if err != nil || otherWritten.GetCommitted() == nil {
		t.Fatalf("write sibling durable Tool use: response=%#v err=%v", otherWritten, err)
	}
	otherToolUseEventID := otherWritten.GetCommitted().GetEventId()
	acceptedOther, err := storeOne.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: otherToolUseEventID,
	})
	if err != nil || acceptedOther.GetCommitted() == nil {
		t.Fatalf("AcceptSandboxExecution(other) = %#v/%v; want committed", acceptedOther, err)
	}

	startAwaitExecutionResultListener(t, storeOne, tracerOne)
	startAwaitExecutionResultListener(t, storeTwo, tracerTwo)

	tracerOne.reset()
	tracerTwo.reset()
	targetOne := startAwaitSandboxExecution(storeOne, context.Background(), scope, toolUseEventID)
	targetTwo := startAwaitSandboxExecution(storeTwo, context.Background(), scope, toolUseEventID)
	otherCtx, cancelOther := context.WithCancel(context.Background())
	other := startAwaitSandboxExecution(storeOne, otherCtx, scope, otherToolUseEventID)
	tracerOne.waitForSQLCount(t, awaitTraceVerificationRead, 2)
	tracerTwo.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	// A hint naming the sibling execution wakes only its waiters, on only the
	// instance that hosts them.
	otherHint := sandboxmodel.ExecutionResultHint{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(),
		SessionThreadID: scope.GetSessionThreadId(), ToolUseEventID: otherToolUseEventID,
	}
	otherPayload, err := marshalBridgeJSON(otherHint)
	if err != nil {
		t.Fatalf("marshal sibling hint: %v", err)
	}
	emitRawExecutionResultPayload(t, admin, otherPayload)
	tracerOne.waitForSQLCount(t, awaitTraceVerificationRead, 3)
	time.Sleep(300 * time.Millisecond)
	if reads := tracerTwo.countSQL(awaitTraceVerificationRead); reads != 1 {
		t.Fatalf("instance-two reads after sibling hint = %d; want 1 (unaffected)", reads)
	}
	if reads := tracerOne.countSQL(awaitTraceVerificationRead); reads != 3 {
		t.Fatalf("instance-one reads after sibling hint = %d; want 3 (only the sibling re-read)", reads)
	}
	requireAwaitBlocked(t, targetOne, 0)
	requireAwaitBlocked(t, targetTwo, 0)
	requireAwaitBlocked(t, other, 0)

	// One terminal settlement reaches every local waiter across both Bridge
	// instances through their own listeners.
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	requireAwaitCompleted(t, targetOne, awaitNotificationTerminalResult)
	requireAwaitCompleted(t, targetTwo, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("fan-out wake latency = %s; want notification path on both instances", latency)
	}
	requireAwaitBlocked(t, other, 150*time.Millisecond)

	cancelOther()
	select {
	case outcome := <-other:
		if outcome.err == nil {
			t.Fatalf("cancelled sibling await = %#v; want an error", outcome.response)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled sibling await did not return")
	}
	if waiters := storeOne.executionResultWake().waiterCount(); waiters != 0 {
		t.Fatalf("instance-one live waiters = %d; want 0", waiters)
	}
	if waiters := storeTwo.executionResultWake().waiterCount(); waiters != 0 {
		t.Fatalf("instance-two live waiters = %d; want 0", waiters)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionRejectsInvalidResultDespiteHint(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "invalid")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	done := startAwaitSandboxExecution(store, context.Background(), scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	// The hint only schedules a re-read: an invalid stored result is rejected
	// by the durable checks, never accepted because a hint arrived.
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, "not-json", true)
	select {
	case outcome := <-done:
		if status.Code(outcome.err) != codes.FailedPrecondition {
			t.Fatalf("AwaitSandboxExecution = %#v/%v; want FailedPrecondition for an invalid stored result", outcome.response, outcome.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AwaitSandboxExecution did not reject the invalid stored result")
	}
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("invalid-result rejection latency = %s; want notification path", latency)
	}
}

// scriptedExecutionResultListener is a queue.NotificationListener double that
// connects once per call, fires readiness, and fails the first connection on
// demand; it needs no database.
type scriptedExecutionResultListener struct {
	mu              sync.Mutex
	count           int
	calls           chan int
	allowDisconnect chan struct{}
}

func (l *scriptedExecutionResultListener) Listen(ctx context.Context, _ string, onReady func(), _ func(string)) error {
	l.mu.Lock()
	l.count++
	call := l.count
	l.mu.Unlock()
	select {
	case l.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
	onReady()
	if call == 1 {
		select {
		case <-l.allowDisconnect:
			return context.DeadlineExceeded
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestExecutionResultListenerCatchUpWakesCurrentWaitersWithoutDatabase(t *testing.T) {
	store := NewPostgreSQLBridgeAPIStore(nil)
	hub := store.executionResultWake()
	key := sandboxmodel.ExecutionResultHint{
		WorkspaceID: "default", SessionID: "sesn_catchup", SessionThreadID: "thr_catchup", ToolUseEventID: "evt_catchup",
	}
	wake := hub.register(key)
	defer hub.unregister(key)

	listener := &scriptedExecutionResultListener{calls: make(chan int, 2), allowDisconnect: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runExecutionResultListener: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("listener did not stop")
		}
	})
	go func() { done <- store.runExecutionResultListener(ctx, listener) }()

	// Initial LISTEN readiness is itself a catch-up for already-registered
	// waiters.
	beforeReady := wake.Snapshot()
	select {
	case call := <-listener.calls:
		if call != 1 {
			t.Fatalf("first listen call = %d", call)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not connect")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := wake.Wait(waitCtx, time.Hour, beforeReady); err != nil {
		t.Fatalf("wait for initial readiness catch-up: %v", err)
	}

	// Reconnect must broadcast a catch-up immediately rather than waiting for
	// another notification.
	beforeReconnect := wake.Snapshot()
	close(listener.allowDisconnect)
	select {
	case call := <-listener.calls:
		if call != 2 {
			t.Fatalf("reconnect listen call = %d", call)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not reconnect")
	}
	if err := wake.Wait(waitCtx, time.Hour, beforeReconnect); err != nil {
		t.Fatalf("wait for reconnect catch-up: %v", err)
	}
}
