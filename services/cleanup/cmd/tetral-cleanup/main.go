package main

import (
	"context"
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
	scheduler := tetralcleanup.NewScheduler(openResult.Client, cfg.ClaimLimit)
	metrics := tetralcleanup.NewSchedulerMetrics()
	defer exportCleanupMetrics(ctx, logger, newMetricsExporter(cfg.MetricsExportURL), metrics, cfg.MetricsExportTimeout)
	// The scheduling phase derives its own 45 s budget from the process context
	// and releases its election before returning.
	started := time.Now()
	result, err := scheduler.RunSchedulingPhase(ctx)
	duration := time.Since(started)
	metrics.ObserveClaimDue(result.Claimed, duration, err)
	logCleanupClaimDue(logger, result, err, cfg.ClaimLimit, duration)
	if err != nil {
		return workload.LogStartupFailure(logger, "cleanup", err)
	}
	return nil
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
