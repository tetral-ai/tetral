package main

import (
	"context"
	"net"
	"os"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

var openDatabase = dbconnect.OpenPlainDSNFromEnv
var runQueueService = tetralqueue.Run
var listenTCP = net.Listen
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env tetralqueue.Env) error {
	logger := workload.NewLogger(os.Stderr, "queue", env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	cfg, err := tetralqueue.ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "queue", workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	openResult, err := openDatabase(ctx)
	if err != nil {
		return workload.LogStartupFailure(logger, "queue", workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer func() { _ = openResult.Client.Close() }()
	if err := verifySchema(ctx, openResult.Client); err != nil {
		return workload.LogStartupFailure(logger, "queue", workload.WithStartupFailureCause(workload.StartupFailureCauseSchema, err))
	}
	if err := openResult.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, "queue", workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	store := queue.NewPostgreSQLStoreWithRetryPolicy(openResult.Client, queue.RetryPolicy{
		BaseDelay:   cfg.RetryBaseDelay,
		MaxDelay:    cfg.RetryMaxDelay,
		MaxAttempts: cfg.RetryMaxAttempts,
	})
	if err := store.VerifyReady(ctx); err != nil {
		return workload.LogStartupFailure(logger, "queue", workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	maintenanceCtx, cancelMaintenance := context.WithCancel(ctx)
	defer cancelMaintenance()
	go tetralqueue.RunStalledLeaseMaintenance(maintenanceCtx, store, tetralqueue.MaintenanceConfig{
		Interval: cfg.LeaseReclaimInterval,
		Limit:    cfg.LeaseReclaimBatchLimit,
		Logger:   logger,
	})
	return runQueueService(ctx, cfg, store, tetralqueue.RuntimeConfig{Listen: listenTCP, Logger: logger, DBStatsProvider: openResult.Client})
}
