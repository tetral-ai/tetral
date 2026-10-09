package integration

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
)

const awaitTraceListen = "LISTEN " + sandboxmodel.ExecutionResultNotificationChannel

type bridgeTraceEntry struct {
	sql string
	at  time.Time
}

// bridgeExecutionQueryTracer records every completed statement on a traced
// runtime pool for the cross-owner integration fixtures.
type bridgeExecutionQueryTracer struct {
	mu      sync.Mutex
	entries []bridgeTraceEntry
	errors  []error
}

type bridgeTraceSQLContextKey struct{}

func (tr *bridgeExecutionQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, bridgeTraceSQLContextKey{}, data.SQL)
}

func (tr *bridgeExecutionQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sqlText, _ := ctx.Value(bridgeTraceSQLContextKey{}).(string)
	tr.mu.Lock()
	tr.entries = append(tr.entries, bridgeTraceEntry{sql: sqlText, at: time.Now()})
	if data.Err != nil {
		tr.errors = append(tr.errors, data.Err)
	}
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

// startAwaitExecutionResultListener runs the production listener for store and
// waits for its initial readiness broadcast after LISTEN succeeds.
func startAwaitExecutionResultListener(t *testing.T, store *agentruntimebridge.PostgreSQLBridgeAPIStore, tracer *bridgeExecutionQueryTracer) {
	startAwaitExecutionResultListenerRun(t, store, tracer, store.RunExecutionResultListener)
}

func startAwaitExecutionResultListenerRun(t *testing.T, store *agentruntimebridge.PostgreSQLBridgeAPIStore, tracer *bridgeExecutionQueryTracer, run func(context.Context) error) {
	t.Helper()
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
	if tracer == nil {
		t.Fatal("listener readiness requires actual completed LISTEN trace")
	}
	tracer.waitForSQLCount(t, awaitTraceListen, 1)
}
