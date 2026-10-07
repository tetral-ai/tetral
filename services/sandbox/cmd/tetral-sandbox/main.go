package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var runWorkload = workload.Run
var openDatabase = func(ctx context.Context, _ string, dsn string) (dbconnect.OpenResult, error) {
	return dbconnect.OpenProtectedDSN(ctx, dsn, os.Getenv("TETRAL_DATABASE_TLS_CA_PATH"), os.Getenv("TETRAL_DATABASE_TLS_SERVER_NAME"))
}
var listenTCP = net.Listen
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }

type envReader interface {
	Getenv(string) string
}

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := workload.RunProcess(func(ctx context.Context) error { return run(ctx, osEnv{}) }); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env envReader) (runErr error) {
	ctx, stopSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	resourcesCtx, cancelResources := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelResources()
	diagnostics, diagnosticErr := workload.DiagnosticConfigFromEnv(env.Getenv)
	cfg, cfgErr := tetralsandbox.ConfigFromEnv(env)
	if cfgErr == nil && cfg.DebugLogging {
		// The Sandbox debug switch selects Debug regardless of TETRAL_LOG_LEVEL;
		// both are boot settings applied once when the process logger is built.
		diagnostics.Level = slog.LevelDebug
	}
	diagnosticOwner := workload.NewProcessLogger(os.Stderr, tetralsandbox.ServiceName, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), diagnostics)
	defer diagnosticOwner.CloseWithBudget()
	logger := diagnosticOwner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if diagnosticErr != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, diagnosticErr)
	}
	if cfgErr != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, cfgErr))
	}
	workload.ConfigureProcessShutdown(ctx, cfg.DrainTimeout+cfg.CancelJoinTimeout, diagnosticOwner)
	openResult, err := openDatabase(resourcesCtx, tetralsandbox.EnvPostgresDSN, cfg.PostgresDSN)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer workload.ProcessCleanup(ctx, func() { _ = openResult.Client.Close() })
	if err := verifySchema(ctx, openResult.Client); err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseSchema, err))
	}
	if err := openResult.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	dialOptions := append([]grpc.DialOption{}, internalgrpc.QueueRPCDialOptions()...)
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	queueConn, err := grpc.NewClient(cfg.QueueGRPCAddress, dialOptions...)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer workload.ProcessCleanup(ctx, func() { _ = queueConn.Close() })
	providerAdapter, err := tetralsandbox.NewDaytonaAdapter(resourcesCtx, cfg, openResult.Client, logger)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	if closer, ok := providerAdapter.BlobStore.(interface{ Close() error }); ok {
		defer func() {
			workload.BeginProcessShutdown(ctx)
			if closeErr := closer.Close(); closeErr != nil {
				logger.Error("shutdown.resource_close_failed", "operation", "close_blob_store", "error.class", "resource_shutdown", "error.code", "blob_store_close_failed")
			}
		}()
	}
	providerRegistry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": providerAdapter,
	})
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	store := sandbox.NewPostgreSQLStore(openResult.Client)
	queueClient := sandboxQueueClient(queueConn)
	queueStore := queue.NewPostgreSQLStore(openResult.Client)
	workspaceStore := workspace.NewStore(openResult.RawDatabaseForExcludedStores)
	executionCoordinator := tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(openResult.Client, cfg.ResourceCredentialRefreshMargin)
	mediaMaterializer := tetralsandbox.NewPostgreSQLSandboxMediaMaterializer(openResult.Client, providerAdapter.BlobStore)
	lifecycleStore := tetralsandbox.NewPostgreSQLSandboxLifecycleStore(openResult.Client, store, cfg.ResourceCredentialRefreshMargin)
	backgroundCommandStore := tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(openResult.Client)
	memoryProjectionStore := tetralsandbox.NewPostgreSQLSandboxMemoryProjectionStore(openResult.Client)
	outputCaptureStore := tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(openResult.Client)
	overLimitFinalizer := tetralsandbox.NewPostgreSQLSandboxQueueOverLimitFinalizer(openResult.Client)
	environmentStore := tetralsandbox.NewEnvironmentArtifactStore(openResult.Client)
	workerPool, err := tetralsandbox.NewWorkspaceConsumerPool(cfg.WorkerConcurrency)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	if err := transportsecurity.WaitForRoutingProxy(ctx, env.Getenv(transportsecurity.EnvRoutingProxyRequired) == "true"); err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	acquisitionCtx, closeAcquisition := context.WithCancel(ctx)
	defer closeAcquisition()
	workerCtx, cancelWorkers := context.WithCancel(resourcesCtx)
	defer cancelWorkers()
	workerCtx = tetralsandbox.WithAcquisitionContext(workerCtx, acquisitionCtx)

	queueWake := queue.NewWakeSignal()
	shutdownJoined := make(chan error, 1)
	allWorkersDone, err := launchSandboxWorkers(workerCtx, acquisitionCtx, sandboxWorkerDependencies{
		cfg: cfg, loops: defaultSandboxWorkerLoops(), queueClient: queueClient, queueStore: queueStore, workspaceStore: workspaceStore,
		client: openResult.Client, providerAdapter: providerAdapter, providerRegistry: providerRegistry, executionCoordinator: executionCoordinator,
		mediaMaterializer: mediaMaterializer, lifecycleStore: lifecycleStore, backgroundCommandStore: backgroundCommandStore, memoryProjectionStore: memoryProjectionStore,
		outputCaptureStore: outputCaptureStore, overLimitFinalizer: overLimitFinalizer, environmentStore: environmentStore, workerPool: workerPool, queueWake: queueWake, logger: logger,
	})
	if err != nil {
		return err
	}
	go func() {
		<-acquisitionCtx.Done()
		shutdownJoined <- tetralsandbox.JoinSandboxWorkers(allWorkersDone, cancelWorkers, cfg.DrainTimeout, cfg.CancelJoinTimeout)
	}()
	defer func() {
		workload.BeginProcessShutdown(ctx)
		closeAcquisition()
		if err := <-shutdownJoined; err != nil && runErr == nil {
			runErr = err
		}
	}()
	readiness := workload.NewReadiness()
	readiness.MarkReady()
	return runWorkload(ctx, workload.Config{
		ServiceName:           tetralsandbox.ServiceName,
		DeploymentEnvironment: env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"),
		ServiceVersion:        env.Getenv("TETRAL_SERVICE_VERSION"),
		ListenAddress:         cfg.HTTPAddress,
		ListenConfigKey:       tetralsandbox.EnvHTTPAddress,
		Listen:                listenTCP,
		Handler: workload.HealthRouter(readiness,
			workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger)),
			workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", openResult.Client)),
		),
		Readiness:       readiness,
		ShutdownTimeout: cfg.DrainTimeout,
		Logger:          logger,
	})
}
