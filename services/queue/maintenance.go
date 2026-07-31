package tetralqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

type MaintenanceStore interface {
	ReclaimExpiredLeases(context.Context, queue.ReclaimExpiredLeasesRequest) (int, error)
	SweepSandboxTerminalJobs(context.Context, queue.SandboxTerminalSweepRequest) (int, error)
	SweepEmptyPartitionCounters(context.Context, queue.EmptyPartitionCounterSweepRequest) (int, error)
}

type MaintenanceConfig struct {
	Interval time.Duration
	Limit    int
	Logger   *slog.Logger
}

func RunStalledLeaseMaintenance(ctx context.Context, store MaintenanceStore, cfg MaintenanceConfig) {
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
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
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
		Now:          now,
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
	if err != nil {
		return
	}

	started = time.Now()
	deletedCounters, err := store.SweepEmptyPartitionCounters(ctx, queue.EmptyPartitionCounterSweepRequest{
		Limit: queue.SandboxMaintenanceBatchLimit,
	})
	logSandboxRetentionResult(cfg.Logger, "queue.partition_counter_retention", "queue.partition_counters.deleted", deletedCounters, err, time.Since(started))
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
