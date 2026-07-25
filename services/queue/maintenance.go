package tetralqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

type LeaseReclaimer interface {
	ReclaimExpiredLeases(context.Context, queue.ReclaimExpiredLeasesRequest) (int, error)
}

type MaintenanceConfig struct {
	Interval time.Duration
	Limit    int
	Logger   *slog.Logger
}

func RunStalledLeaseMaintenance(ctx context.Context, reclaimer LeaseReclaimer, cfg MaintenanceConfig) {
	if reclaimer == nil {
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
			started := time.Now()
			reclaimed, err := reclaimer.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{
				Limit:        limit,
				ErrorKind:    "lease_expired",
				ErrorMessage: "queue lease expired",
				Now:          now,
			})
			logLeaseReclaimResult(cfg.Logger, reclaimed, err, time.Since(started))
		}
	}
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
