package tetralqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

type MaintenanceStore interface {
	ReclaimExpiredLeases(context.Context, queue.ReclaimExpiredLeasesRequest) (int, error)
	SweepSandboxTerminalJobs(context.Context, queue.SandboxTerminalSweepRequest) (int, error)
	SweepEmptyPartitionCounters(context.Context, queue.EmptyPartitionCounterSweepRequest) (int, error)
	PruneJobRunnerTerminalJobs(context.Context, queue.JobRunnerTerminalRetentionRequest) (queue.JobRunnerTerminalRetentionResult, error)
}

type MaintenanceConfig struct {
	Interval time.Duration
	Limit    int
	Logger   *slog.Logger
	Metrics  *RetentionMetrics
}

// jobRunnerRetentionDeadline bounds the whole Job Runner terminal-retention
// phase of one maintenance tick, across all three terminal states.
const jobRunnerRetentionDeadline = 2 * time.Second

// maintenanceAdmission closes cycle admission without cancelling a cycle that
// already owns database work. The service later cancels that work at its drain
// deadline and joins this loop before returning database ownership to command.
type maintenanceAdmission struct {
	mu           sync.Mutex
	stopping     bool
	stop         chan struct{}
	admissionCtx context.Context
}

func (a *maintenanceAdmission) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.stopping {
		a.stopping = true
		close(a.stop)
	}
}

func (a *maintenanceAdmission) admit() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Signal cancellation closes new-cycle admission even before the service's
	// shutdown goroutine runs. The separate work context still lets the current
	// cycle complete within its drain window.
	return !a.stopping && a.admissionCtx.Err() == nil
}

func runStalledLeaseMaintenance(ctx context.Context, store MaintenanceStore, cfg MaintenanceConfig, admission *maintenanceAdmission) {
	if store == nil {
		return
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = time.Duration(defaultLeaseReclaimIntervalSeconds) * time.Second
	}
	limit := cfg.Limit
	if limit <= 0 {
		limit = defaultLeaseReclaimLimit
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var stop <-chan struct{}
	if admission != nil {
		stop = admission.stop
	}
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if ctx.Err() != nil || (admission != nil && !admission.admit()) {
				return
			}
			cfg.Limit = limit
			runMaintenanceTick(ctx, store, cfg, now)
		}
	}
}

func runMaintenanceTick(ctx context.Context, store MaintenanceStore, cfg MaintenanceConfig, now time.Time) {
	started := time.Now()
	reclaimed, err := store.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{
		Limit:        cfg.Limit,
		ErrorKind:    "lease_expired",
		ErrorMessage: "queue lease expired",
	})
	logLeaseReclaimResult(cfg.Logger, reclaimed, err, time.Since(started))
	if err != nil {
		return
	}

	started = time.Now()
	deletedJobs, err := store.SweepSandboxTerminalJobs(ctx, queue.SandboxTerminalSweepRequest{
		Now:   now,
		Limit: queue.SandboxMaintenanceBatchLimit,
	})
	logSandboxRetentionResult(cfg.Logger, "queue.sandbox_job_retention", "queue.jobs.deleted", deletedJobs, err, time.Since(started))
	if err != nil && !queue.IsIntegrityError(err) {
		return
	}

	started = time.Now()
	deletedCounters, err := store.SweepEmptyPartitionCounters(ctx, queue.EmptyPartitionCounterSweepRequest{
		Limit: queue.SandboxMaintenanceBatchLimit,
	})
	logSandboxRetentionResult(cfg.Logger, "queue.partition_counter_retention", "queue.partition_counters.deleted", deletedCounters, err, time.Since(started))

	// Job Runner terminal retention runs last, under its own deadline. The
	// phase counts once when any state spent its 256-row page while another
	// eligible row remained.
	started = time.Now()
	retentionCtx, cancel := context.WithTimeout(ctx, jobRunnerRetentionDeadline)
	result, err := store.PruneJobRunnerTerminalJobs(retentionCtx, queue.JobRunnerTerminalRetentionRequest{Now: now})
	cancel()
	budgetExhausted := result.ExhaustedStates > 0
	if budgetExhausted {
		cfg.Metrics.observeBudgetExhausted()
	}
	logJobRunnerRetentionResult(cfg.Logger, result, budgetExhausted, err, time.Since(started))
}

// logJobRunnerRetentionResult records one Job Runner retention pass with
// aggregate counts only. Retained rows with a NULL terminal timestamp are
// reported as an integrity count; a failure carries no driver text.
func logJobRunnerRetentionResult(logger *slog.Logger, result queue.JobRunnerTerminalRetentionResult, budgetExhausted bool, err error, duration time.Duration) {
	if logger == nil {
		return
	}
	const operation = "queue.job_runner_retention"
	counts := []any{
		slog.Int("deleted.count", result.Deleted),
		slog.Int("malformed.count", result.Malformed),
		slog.Bool("budget.exhausted", budgetExhausted),
		slog.Int("target.count", result.ExhaustedStates),
	}
	if err != nil {
		logger.Warn(operation+".failed", append([]any{
			slog.String("operation", operation),
			slog.String("event.kind", operation+".failed"),
			slog.String("component", "queue"),
			slog.Int64("duration.ms", duration.Milliseconds()),
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
			slog.String("error.class", "queue_maintenance_error"),
			slog.String("error.code", "queue_retention_failed"),
			slog.String("error.message_safe", "queue retention failed"),
		}, counts...)...)
		return
	}
	if result.Deleted == 0 && result.Malformed == 0 && !budgetExhausted {
		return
	}
	level := slog.LevelInfo
	if result.Malformed > 0 {
		level = slog.LevelWarn
	}
	logger.Log(context.Background(), level, operation+".completed", append([]any{
		slog.String("operation", operation),
		slog.String("event.kind", operation+".completed"),
		slog.String("component", "queue"),
		slog.Int64("duration.ms", duration.Milliseconds()),
	}, counts...)...)
}

func logLeaseReclaimResult(logger *slog.Logger, reclaimed int, err error, duration time.Duration) {
	if logger == nil {
		return
	}
	if err != nil {
		logger.Warn("queue.lease_reclaim.failed",
			slog.String("operation", "queue.lease_reclaim"),
			slog.String("event.kind", "queue.lease_reclaim.failed"),
			slog.String("component", "queue"),
			slog.Int64("duration.ms", duration.Milliseconds()),
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
			slog.String("error.class", "queue_maintenance_error"),
			slog.String("error.code", "lease_reclaim_failed"),
			slog.String("error.message_safe", "queue lease reclaim failed"),
		)
		return
	}
	if reclaimed > 0 {
		logger.Info("queue.lease_reclaim.completed",
			slog.String("operation", "queue.lease_reclaim"),
			slog.String("event.kind", "queue.lease_reclaim.completed"),
			slog.String("component", "queue"),
			slog.Int64("duration.ms", duration.Milliseconds()),
			slog.Int("queue.jobs.reclaimed", reclaimed),
		)
	}
}

func logSandboxRetentionResult(logger *slog.Logger, operation string, countField string, deleted int, err error, duration time.Duration) {
	if logger == nil {
		return
	}
	if err != nil {
		logger.Warn(operation+".failed",
			slog.String("operation", operation),
			slog.String("event.kind", operation+".failed"),
			slog.String("component", "queue"),
			slog.Int64("duration.ms", duration.Milliseconds()),
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
			slog.String("error.class", "queue_maintenance_error"),
			slog.String("error.code", "queue_retention_failed"),
			slog.String("error.message_safe", "queue retention failed"),
		)
		return
	}
	if deleted > 0 {
		logger.Info(operation+".completed",
			slog.String("operation", operation),
			slog.String("event.kind", operation+".completed"),
			slog.String("component", "queue"),
			slog.Int64("duration.ms", duration.Milliseconds()),
			slog.Int(countField, deleted),
		)
	}
}
