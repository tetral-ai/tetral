package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workload"
	computerhub "github.com/tetral-ai/tetral/services/computer-hub"
)

var openDatabase = dbconnect.OpenProtectedDSN
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }
var loadBlobConfig = blob.LoadConfig
var newBlobStore = func(ctx context.Context, cfg *blob.Config) (io.Closer, error) {
	return blob.NewProtectedS3BlobStore(ctx, cfg)
}
var runHub = computerhub.Run
var listenTCP = net.Listen

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := workload.RunProcess(func(ctx context.Context) error { return run(ctx, osEnv{}) }); err != nil {
		os.Exit(1)
	}
}

// run opens the process's dependencies in order (configuration, database
// with schema and role checks, blob store) and serves until ctx ends. Each
// startup failure is one redacted record.
func run(ctx context.Context, env computerhub.Env) error {
	diagnostics, diagnosticErr := workload.DiagnosticConfigFromEnv(env.Getenv)
	diagnosticOwner := workload.NewProcessLogger(os.Stderr, computerhub.ServiceName, env.Getenv(workload.EnvDeploymentEnvironment), env.Getenv(workload.EnvServiceVersion), diagnostics)
	defer diagnosticOwner.CloseWithBudget()
	logger := diagnosticOwner.Logger
	defer workload.InstallDefaultLogger(logger)()
	coldStart := workload.BeginServiceColdStart(ctx)
	fail := func(err error) error { return workload.LogStartupFailure(logger, computerhub.ServiceName, err) }
	if diagnosticErr != nil {
		return fail(diagnosticErr)
	}
	cfg, err := computerhub.ConfigFromEnv(env)
	if err != nil {
		return fail(err)
	}
	workload.ConfigureProcessShutdown(ctx, cfg.StreamDrainWindow+computerhub.ShutdownTimeout+5*time.Second, diagnosticOwner)
	coldStart.Advance(ctx)
	opened, err := openDatabase(ctx, cfg.DatabaseURL, cfg.DatabaseTLSCAPath, cfg.DatabaseTLSServerName)
	if err != nil {
		return fail(err)
	}
	defer workload.ProcessCleanup(ctx, func() { _ = opened.Client.Close() })
	if err := verifySchema(ctx, opened.Client); err != nil {
		return fail(err)
	}
	if err := opened.Client.VerifyRuntimeRole(ctx); err != nil {
		return fail(err)
	}
	blobConfig, err := loadBlobConfig()
	if err == nil {
		err = blobConfig.AssertProductionReady()
	}
	if err != nil {
		return fail(workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	blobStore, err := newBlobStore(ctx, blobConfig)
	if err != nil {
		return fail(workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, fmt.Errorf("blob store: %w", err)))
	}
	defer workload.ProcessCleanup(ctx, func() { _ = blobStore.Close() })
	coldStart.Advance(ctx)
	return runHub(ctx, cfg, computerhub.Runtime{Database: opened.Client, Logger: logger, Listen: listenTCP})
}
