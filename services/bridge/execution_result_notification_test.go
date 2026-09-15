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
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
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

func startAwaitSandboxExecution(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope, toolUseEventID string) chan awaitExecutionOutcome {
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

// Count both registrations and retained keys: zero counts alone would miss
// a leak of per-execution signals after the last waiter leaves.
func requireExecutionResultWaiters(t *testing.T, hub *sandboxExecutionResultWakeHub, wantWaiters, wantKeys int) {
	t.Helper()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	waiters := 0
	for _, set := range hub.waiters {
		waiters += set.count
	}
	if waiters != wantWaiters || len(hub.waiters) != wantKeys {
		t.Fatalf("execution result hub registrations/keys = %d/%d; want %d/%d", waiters, len(hub.waiters), wantWaiters, wantKeys)
	}
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
// waits for its initial readiness broadcast after LISTEN succeeds.
func startAwaitExecutionResultListener(t *testing.T, store *PostgreSQLBridgeAPIStore, tracer *bridgeExecutionQueryTracer) {
	startAwaitExecutionResultListenerRun(t, store, tracer, store.RunExecutionResultListener)
}

func startAwaitExecutionResultListenerRun(t *testing.T, store *PostgreSQLBridgeAPIStore, tracer *bridgeExecutionQueryTracer, run func(context.Context) error) {
	t.Helper()
	hub := store.executionResultWake()
	readyKey := sandboxmodel.ExecutionResultHint{}
	ready := hub.register(readyKey)
	defer hub.unregister(readyKey)
	snapshot := ready.Snapshot()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
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
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	if err := ready.WaitForWake(readyCtx, snapshot); err != nil {
		t.Fatalf("execution result listener readiness: %v", err)
	}
	if tracer != nil {
		tracer.waitForSQLCount(t, awaitTraceListen, 1)
	}
}

// gatedExecutionResultReconnect holds the second reconnect before LISTEN so
// a test can commit a result while the actual PostgreSQL listener is absent.
type gatedExecutionResultReconnect struct {
	delegate queue.NotificationListener
	reached  chan struct{}
	resume   chan struct{}
	calls    int
}

func (l *gatedExecutionResultReconnect) Listen(ctx context.Context, channel string, onReady func(), onPayload func(string)) error {
	l.calls++
	if l.calls == 3 {
		close(l.reached)
		select {
		case <-l.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return l.delegate.Listen(ctx, channel, onReady, onPayload)
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
	seedReadySandboxForSharedToolExecution(t, admin, scope.GetWorkspaceId(), scope.GetSessionId())
	startAwaitExecutionResultListener(t, store, tracer)

	// Drive the accepted Queue job through the real runner and terminal writer.
	// Only provider execution is simulated; the gate keeps the result pending
	// until Bridge has registered and completed its first verification read.
	producerClient := dbconnect.NewClientForTesting(runtime)
	queueConnection := startBackgroundNotificationQueueServer(t, queue.NewPostgreSQLStore(producerClient))
	provider := newGatedBridgeToolProvider()
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: provider,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(producerClient, 30*time.Minute),
		Providers:   registry,
		Media:       backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: scope.GetWorkspaceId(), LeaseOwner: "notification-composition", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	runnerDone := make(chan struct{})
	var runnerActive bool
	var runnerErr error
	go func() {
		defer close(runnerDone)
		runnerActive, runnerErr = runner.RunOnceWithActivity(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runnerDone:
		case <-time.After(5 * time.Second):
			t.Error("Sandbox execution runner did not stop")
		}
	})
	select {
	case <-provider.started:
	case <-ctx.Done():
		t.Fatal("Sandbox execution did not reach provider")
	}

	tracer.reset()
	done := startAwaitSandboxExecution(ctx, store, scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	released := time.Now()
	close(provider.release)
	requireAwaitCompleted(t, done, `{"status":"success","result":{"text":"done"}}`)
	if latency := time.Since(released); latency >= 800*time.Millisecond {
		t.Fatalf("notification wake latency = %s; want prompt notification delivery", latency)
	}
	select {
	case <-runnerDone:
		if runnerErr != nil || !runnerActive {
			t.Fatalf("Sandbox runner = active %v, error %v; want true,nil", runnerActive, runnerErr)
		}
	case <-ctx.Done():
		t.Fatal("Sandbox execution runner did not finish")
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads = %d; want exactly 2 (initial + terminal)", reads)
	}
	if validations := tracer.countSQL(awaitTraceEntryValidation); validations != 1 {
		t.Fatalf("entry validation transactions = %d; want 1", validations)
	}
	requireExecutionResultWaiters(t, store.executionResultWake(), 0, 0)
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionReadsPreCommittedResult(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "precommit")

	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	tracer.reset()
	done := startAwaitSandboxExecution(context.Background(), store, scope, toolUseEventID)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 1 {
		t.Fatalf("verification reads = %d; want 1 (committed result returns on the initial read)", reads)
	}
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionWithMinimumConnectionPool(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	traced := storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer)
	traced.SetMaxOpenConns(2)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(traced))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "minimum_pool")
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	startAwaitExecutionResultListener(t, store, tracer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := startAwaitSandboxExecution(ctx, store, scope, toolUseEventID)
	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionCommitDuringInitialRead(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "duringread")
	startAwaitExecutionResultListener(t, store, tracer)

	// Observe the same signal as the real waiter. Wait for the listener's
	// broadcast before releasing the SQL barrier, forcing the lost-wake race.
	hub := store.executionResultWake()
	key := sandboxExecutionResultKey(scope, toolUseEventID)
	probe := hub.register(key)
	defer hub.unregister(key)
	snapshot := probe.Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tracer.reset()
	// Park the waiter at the completion of the first verification read's
	// result statement: the statement observed the pending row, and the
	// settlement commits while the read transaction is still open.
	fired, release := tracer.armBarrier("", awaitTraceVerificationRead, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	done := startAwaitSandboxExecution(ctx, store, scope, toolUseEventID)
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("initial verification read did not reach the barrier")
	}
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	if err := probe.WaitForWake(ctx, snapshot); err != nil {
		t.Fatalf("result hint did not reach the hub while the read was parked: %v", err)
	}
	unblock()

	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("in-window commit wake latency = %s; want prompt notification delivery", latency)
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

	// Observe the same signal as the real waiter. Wait for the listener's
	// broadcast before releasing the SQL barrier, forcing the lost-wake race.
	hub := store.executionResultWake()
	key := sandboxExecutionResultKey(scope, toolUseEventID)
	probe := hub.register(key)
	defer hub.unregister(key)
	snapshot := probe.Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tracer.reset()
	// Park the waiter at the COMMIT closing the first verification read: the
	// pending read is durable, and the settlement commits before the waiter
	// can block. The wake snapshot taken before the read must make the wait
	// return immediately instead of blocking for another notification.
	fired, release := tracer.armBarrier(awaitTraceVerificationRead, "commit", 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	done := startAwaitSandboxExecution(ctx, store, scope, toolUseEventID)
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("verification read transaction did not reach the commit barrier")
	}
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	if err := probe.WaitForWake(ctx, snapshot); err != nil {
		t.Fatalf("result hint did not reach the hub while the read was parked: %v", err)
	}
	unblock()

	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if latency := time.Since(committed); latency >= 800*time.Millisecond {
		t.Fatalf("post-read commit wake latency = %s; want prompt notification delivery", latency)
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
	done := startAwaitSandboxExecution(context.Background(), store, scope, toolUseEventID)
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
	done := startAwaitSandboxExecution(context.Background(), store, scope, toolUseEventID)
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

	// With no periodic verification, any extra read comes from a misrouted hint.
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

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionMissedNotificationRecoversOnRejoin(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "rejoin")

	// No listener runs. The result notification has no receiver, so this RPC
	// expires without another read. Use an earlier caller deadline here; the
	// idle-load test separately proves the unchanged 30-second internal limit.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	tracer.reset()
	done := startAwaitSandboxExecution(ctx, store, scope, toolUseEventID)
	tracer.waitForSQLCount(t, awaitTraceVerificationRead, 1)
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	select {
	case outcome := <-done:
		if status.Code(outcome.err) != codes.DeadlineExceeded {
			t.Fatalf("wait without notification = %v; want DeadlineExceeded", outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not honor caller deadline")
	}
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 1 {
		t.Fatalf("verification reads before rejoin = %d; want 1", reads)
	}
	requireExecutionResultWaiters(t, store.executionResultWake(), 0, 0)

	// Rejoin the same accepted execution: the first read returns its durable
	// result without another acceptance or provider execution. Runtime's
	// DEADLINE_EXCEEDED rejoin and identity are covered in tool-runner.test.ts.
	requireAwaitCompleted(t, startAwaitSandboxExecution(context.Background(), store, scope, toolUseEventID), awaitNotificationTerminalResult)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
		t.Fatalf("verification reads across both waits = %d; want 2", reads)
	}
	requireExecutionResultWaiters(t, store.executionResultWake(), 0, 0)
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionReconnectTriggersCatchUp(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "reconnect")
	listener := &gatedExecutionResultReconnect{
		delegate: queue.PostgreSQLNotificationListener{Client: store.Client, Operation: "agentruntimebridge.listen_sandbox_execution_result"},
		reached:  make(chan struct{}), resume: make(chan struct{}),
	}
	startAwaitExecutionResultListenerRun(t, store, tracer, func(ctx context.Context) error {
		return store.runExecutionResultListener(ctx, listener)
	})

	tracer.reset()
	done := startAwaitSandboxExecution(context.Background(), store, scope, toolUseEventID)
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

	// Two disconnects: each reconnect re-LISTENs and wakes the waiter. The
	// second catches a result committed while no listener could receive its hint.
	for kill := 1; kill <= 2; kill++ {
		terminateListener()
		if kill == 2 {
			select {
			case <-listener.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("listener did not reach the gated reconnect")
			}
			commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
			requireAwaitBlocked(t, done, 1200*time.Millisecond)
			if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 2 {
				t.Fatalf("verification reads while disconnected = %d; want initial + first catch-up", reads)
			}
			close(listener.resume)
		}
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

	requireAwaitCompleted(t, done, awaitNotificationTerminalResult)
	if reads := tracer.countSQL(awaitTraceVerificationRead); reads != 3 {
		t.Fatalf("verification reads = %d; want 3 (initial + 2 catch-ups, second terminal)", reads)
	}
	requireExecutionResultWaiters(t, store.executionResultWake(), 0, 0)
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionBoundedIdleLoad(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "idleload")

	startAwaitExecutionResultListener(t, store, tracer)
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
	if reads != 1 {
		t.Fatalf("verification transactions over a 30s idle wait = %d; want exactly 1 without periodic verification", reads)
	}
	if validations != 1 {
		t.Fatalf("entry validation transactions = %d; want 1", validations)
	}
	if setConfigs != reads+validations {
		t.Fatalf("workspace scope setup commands = %d; want one per verification/validation (%d)", setConfigs, reads+validations)
	}
	if begins != reads+validations || commits != reads+validations {
		t.Fatalf("transaction commands = %d/%d; want one BEGIN+COMMIT per verification/validation (%d)", begins, commits, reads+validations)
	}
	requireExecutionResultWaiters(t, store.executionResultWake(), 0, 0)
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionFanOutAcrossAndWithinBridgeInstances(t *testing.T) {
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
	targetOne := startAwaitSandboxExecution(context.Background(), storeOne, scope, toolUseEventID)
	targetOneDuplicate := startAwaitSandboxExecution(context.Background(), storeOne, scope, toolUseEventID)
	cancelledCtx, cancelTarget := context.WithCancel(context.Background())
	defer cancelTarget()
	cancelledTarget := startAwaitSandboxExecution(cancelledCtx, storeOne, scope, toolUseEventID)
	targetTwo := startAwaitSandboxExecution(context.Background(), storeTwo, scope, toolUseEventID)
	otherCtx, cancelOther := context.WithCancel(context.Background())
	other := startAwaitSandboxExecution(otherCtx, storeOne, scope, otherToolUseEventID)
	tracerOne.waitForSQLCount(t, awaitTraceVerificationRead, 4)
	tracerTwo.waitForSQLCount(t, awaitTraceVerificationRead, 1)

	// Cancelling one of three local waiters for the same execution must keep
	// the other two registered and routable, alongside the unrelated waiter.
	cancelTarget()
	select {
	case outcome := <-cancelledTarget:
		if outcome.err == nil {
			t.Fatal("cancelled same-key waiter returned a result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled same-key waiter did not return")
	}
	requireExecutionResultWaiters(t, storeOne.executionResultWake(), 3, 2)

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
	tracerOne.waitForSQLCount(t, awaitTraceVerificationRead, 5)
	time.Sleep(300 * time.Millisecond)
	if reads := tracerTwo.countSQL(awaitTraceVerificationRead); reads != 1 {
		t.Fatalf("instance-two reads after sibling hint = %d; want 1 (unaffected)", reads)
	}
	if reads := tracerOne.countSQL(awaitTraceVerificationRead); reads != 5 {
		t.Fatalf("instance-one reads after sibling hint = %d; want 5 (only the sibling re-read)", reads)
	}
	requireAwaitBlocked(t, targetOne, 0)
	requireAwaitBlocked(t, targetOneDuplicate, 0)
	requireAwaitBlocked(t, targetTwo, 0)
	requireAwaitBlocked(t, other, 0)

	// One terminal settlement reaches every local waiter across both Bridge
	// instances through their own listeners.
	committed := time.Now()
	commitAwaitExecutionSettlement(t, admin, scope, toolUseEventID, awaitNotificationTerminalResult, true)
	requireAwaitCompleted(t, targetOne, awaitNotificationTerminalResult)
	requireAwaitCompleted(t, targetOneDuplicate, awaitNotificationTerminalResult)
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
	requireExecutionResultWaiters(t, storeOne.executionResultWake(), 0, 0)
	requireExecutionResultWaiters(t, storeTwo.executionResultWake(), 0, 0)
}

func TestPostgreSQLBridgeAPIStoreAwaitSandboxExecutionRejectsInvalidResultDespiteHint(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &bridgeExecutionQueryTracer{}
	store := newAwaitNotificationTracedStore(t, runtime, tracer)
	scope, toolUseEventID := seedAwaitExecutionNotificationFixture(t, store, admin, "invalid")
	startAwaitExecutionResultListener(t, store, tracer)

	tracer.reset()
	done := startAwaitSandboxExecution(context.Background(), store, scope, toolUseEventID)
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
	onReady()
	// Report the connection only after its readiness callback has completed,
	// so the test observes a wake that happened before it calls Wait.
	select {
	case l.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
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
	// Initial LISTEN readiness is itself a catch-up for already-registered
	// waiters. Snapshot before starting the producer so its first broadcast
	// cannot become part of the baseline we are waiting to advance.
	beforeReady := wake.Snapshot()
	go func() { done <- store.runExecutionResultListener(ctx, listener) }()
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
