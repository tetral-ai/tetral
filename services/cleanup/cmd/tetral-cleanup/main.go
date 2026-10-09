package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

var openDatabase = dbconnect.OpenProtectedDSNFromEnv
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }

var newMetricsExporter = func(endpoint string) tetralcleanup.MetricsExporter {
	if endpoint == "" {
		return nil
	}
	return tetralcleanup.OpenMetricsHTTPExporter{Endpoint: endpoint}
}

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env tetralcleanup.Env) error {
	diagnostics, diagnosticErr := workload.DiagnosticConfigFromEnv(env.Getenv)
	diagnosticOwner := workload.NewProcessLogger(os.Stderr, "cleanup", env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), diagnostics)
	defer diagnosticOwner.CloseWithBudget()
	logger := diagnosticOwner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if diagnosticErr != nil {
		return workload.LogStartupFailure(logger, "cleanup", diagnosticErr)
	}
	cfg, err := tetralcleanup.ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "cleanup", err)
	}
	openResult, err := openDatabase(ctx)
	if err != nil {
		return workload.LogStartupFailure(logger, "cleanup", err)
	}
	defer func() { _ = openResult.Client.Close() }()
	if err := verifySchema(ctx, openResult.Client); err != nil {
		return workload.LogStartupFailure(logger, "cleanup", err)
	}
	if err := openResult.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, "cleanup", err)
	}
	metrics := tetralcleanup.NewSchedulerMetrics()
	err = runPhases(ctx, logger, openResult.Client, cfg, metrics)
	// Every phase has run or been cancelled; their joined errors are returned
	// only after this invocation's metrics are exported.
	exportCleanupMetrics(ctx, logger, newMetricsExporter(cfg.MetricsExportURL), metrics, cfg.MetricsExportTimeout)
	if err != nil {
		return workload.LogStartupFailure(logger, "cleanup", err)
	}
	return nil
}

// runPhases runs the three independent phases once each, in order: Session
// scheduling, idempotency-receipt retention, then change retention. A phase
// failure, or scheduling exhausting its own 45 s budget, is recorded and the
// next phase still runs; only cancellation of the process context stops the
// remaining phases. Both retention phases share one 24-hour cutoff read from
// the database clock at invocation, and each derives its own batch deadlines
// from the process context.
func runPhases(ctx context.Context, logger *slog.Logger, client *dbconnect.Client, cfg tetralcleanup.Config, metrics *tetralcleanup.SchedulerMetrics) error {
	retention := tetralcleanup.NewRetention(client)
	cutoff, cutoffErr := retention.Cutoff(ctx)
	var errs []error

	// The scheduling phase derives its own 45 s budget from the process context
	// and releases its election before returning.
	scheduler := tetralcleanup.NewScheduler(client, cfg.ClaimLimit)
	started := time.Now()
	result, err := scheduler.RunSchedulingPhase(ctx)
	duration := time.Since(started)
	metrics.ObserveClaimDue(result.Claimed, duration, err)
	logCleanupClaimDue(logger, result, err, cfg.ClaimLimit, duration)
	errs = append(errs, err)

	if cutoffErr != nil {
		return errors.Join(append(errs, cutoffErr)...)
	}
	for _, phase := range []func(context.Context, time.Time) (tetralcleanup.RetentionResult, error){
		retention.PruneIdempotencyReceipts,
		retention.PruneStreamChanges,
	} {
		if ctx.Err() != nil {
			return errors.Join(append(errs, ctx.Err())...)
		}
		started := time.Now()
		result, err := phase(ctx, cutoff)
		metrics.ObserveRetention(result)
		logCleanupRetention(logger, result, err, time.Since(started))
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func exportCleanupMetrics(
	ctx context.Context,
	logger *slog.Logger,
	exporter tetralcleanup.MetricsExporter,
	metrics *tetralcleanup.SchedulerMetrics,
	timeout time.Duration,
) {
	if exporter == nil || metrics == nil {
		return
	}
	started := time.Now()
	exportCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	samples, err := metrics.Collector()(exportCtx)
	if err == nil {
		diagnostics, _ := workload.DiagnosticMetrics(logger)(exportCtx)
		samples = append(samples, diagnostics...)
		err = exporter.Export(exportCtx, samples)
	}
	if err == nil || logger == nil {
		return
	}
	logger.Error("cleanup.metrics_export.failed",
		slog.String("operation", "cleanup.metrics_export"),
		slog.String("event.kind", "cleanup.metrics_export.failed"),
		slog.String("component", tetralcleanup.ServiceName),
		slog.Int64("duration.ms", time.Since(started).Milliseconds()),
		slog.Bool("retryable", false),
		slog.Bool("terminal", true),
		slog.String("error.class", "metrics_export_error"),
		slog.String("error.code", "cleanup_metrics_export_failed"),
		slog.String("error.message_safe", "cleanup metrics export failed"),
	)
}

// logCleanupRetention records one retention phase with aggregate counts only;
// a failed phase's error is reported separately without its raw text.
func logCleanupRetention(logger *slog.Logger, result tetralcleanup.RetentionResult, err error, duration time.Duration) {
	if logger == nil {
		return
	}
	outcome := "completed"
	switch {
	case err != nil:
		outcome = "failed"
	case result.BudgetExhausted:
		outcome = "budget_exhausted"
	}
	logger.Info("cleanup.retention.completed",
		slog.String("operation", "cleanup.retention"),
		slog.String("event.kind", "cleanup.retention.completed"),
		slog.String("component", tetralcleanup.ServiceName),
		slog.String("phase", result.Phase),
		slog.String("outcome", outcome),
		slog.Int64("duration.ms", duration.Milliseconds()),
		slog.Int("page.count", result.Batches),
		slog.Int("candidate.count", result.Examined),
		slog.Int("deleted.count", result.Deleted),
	)
}

// logCleanupClaimDue records one scheduling phase with aggregate counts only;
// a failed phase's error is reported separately without its raw text.
func logCleanupClaimDue(logger *slog.Logger, result tetralcleanup.SchedulingResult, err error, limit int, duration time.Duration) {
	if logger == nil {
		return
	}
	outcome := "completed"
	switch {
	case err != nil:
		outcome = "failed"
	case !result.Elected:
		outcome = "not_elected"
	}
	logger.Info("cleanup.claim_due.completed",
		slog.String("operation", "cleanup.claim_due"),
		slog.String("event.kind", "cleanup.claim_due.completed"),
		slog.String("component", tetralcleanup.ServiceName),
		slog.String("outcome", outcome),
		slog.Int64("duration.ms", duration.Milliseconds()),
		slog.Int("candidate.count", result.Attempted),
		slog.Int("cleanup.jobs.claimed", result.Claimed),
		slog.Int("failed.count", result.Failed),
		slog.Int("cleanup.claim.limit", limit),
	)
}
