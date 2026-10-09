package workload

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type processGuardKey struct{}
type processGuard struct {
	mu       sync.Mutex
	started  time.Time
	budget   time.Duration
	owner    *ProcessLogger
	timer    *time.Timer
	finished bool
}

// RunProcess is the executable boundary. Reusable service runners cannot exit
// their caller: only this explicit entry installs a fatal shutdown deadline.
// Completion includes all deferred cleanup in run.
func RunProcess(run func(context.Context) error) error {
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	guard := &processGuard{}
	ctx := context.WithValue(signalCtx, processGuardKey{}, guard)
	done := make(chan struct{})
	defer close(done)
	defer guard.finish()
	go func() {
		select {
		case <-signalCtx.Done():
			BeginProcessShutdown(ctx)
		case <-done:
		}
	}()
	return run(ctx)
}

// ConfigureProcessShutdown receives the owning command's validated application
// drain and join allocation. Configuration cannot restart a shutdown deadline.
func ConfigureProcessShutdown(ctx context.Context, budget time.Duration, owner *ProcessLogger) {
	if ctx == nil {
		return
	}
	guard, _ := ctx.Value(processGuardKey{}).(*processGuard)
	if guard == nil {
		return
	}
	if budget <= 0 {
		panic("process shutdown budget must be positive")
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.budget != 0 {
		panic("process shutdown budget already configured")
	}
	guard.budget, guard.owner = budget, owner
	guard.arm()
}

// BeginProcessShutdown starts a single absolute deadline shared by all listeners
// and cleanup owners. It is a no-op outside RunProcess.
func BeginProcessShutdown(ctx context.Context) {
	if ctx == nil {
		return
	}
	guard, _ := ctx.Value(processGuardKey{}).(*processGuard)
	if guard == nil {
		return
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.started.IsZero() {
		guard.started = time.Now()
	}
	guard.arm()
}

// ProcessCleanup arms the process boundary before a potentially blocking close.
// Use as defer ProcessCleanup(ctx, closeResources), preserving ownership order.
func ProcessCleanup(ctx context.Context, cleanup func()) {
	BeginProcessShutdown(ctx)
	cleanup()
}

func (g *processGuard) arm() {
	if g.finished || g.timer != nil || g.started.IsZero() || g.budget == 0 {
		return
	}
	g.timer = time.AfterFunc(time.Until(g.started.Add(g.budget)), func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.finished {
			return
		}
		// ProcessLogger has a bounded nonblocking producer. A blocked or panicking
		// sink cannot delay the exit; delivery of this final record is best effort.
		if g.owner != nil {
			g.owner.Logger.Error("workload.shutdown.incomplete", "operation", "workload.shutdown", "event.kind", "shutdown", "component", "workload", "error.class", "shutdown_error", "error.code", "process_shutdown_timeout", "duration.ms", time.Since(g.started).Milliseconds())
		}
		os.Exit(1)
	})
}

func (g *processGuard) finish() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.finished = true
	if g.timer != nil {
		g.timer.Stop()
	}
}
