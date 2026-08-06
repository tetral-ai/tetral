package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var runWorkload = workload.Run
var openDatabase = dbconnect.OpenPlainDSN
var listenTCP = net.Listen
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }

type envReader interface {
	Getenv(string) string
}

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env envReader) error {
	logger := workload.NewLogger(os.Stderr, tetralsandbox.ServiceName, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	cfg, err := tetralsandbox.ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	if cfg.DebugLogging {
		logger = workload.NewLoggerWithLevel(os.Stderr, tetralsandbox.ServiceName, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), slog.LevelDebug)
	}
	openResult, err := openDatabase(ctx, tetralsandbox.EnvPostgresDSN, cfg.PostgresDSN)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer func() { _ = openResult.Client.Close() }()
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
	defer func() { _ = queueConn.Close() }()
	providerAdapter, err := tetralsandbox.NewDaytonaAdapter(ctx, cfg, openResult.Client, logger)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	providerRegistry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": providerAdapter,
	})
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	store := sandbox.NewPostgreSQLStore(openResult.Client)
	queueClient := tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConn))
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
	queueWakeCtx, cancelQueueWake := context.WithCancel(ctx)
	defer cancelQueueWake()
	queueWake := queue.NewWakeSignal()
	go func() {
		_ = queue.RunNotificationListener(queueWakeCtx, queue.PostgreSQLNotificationListener{Client: openResult.Client}, queue.ConsumerClassSandbox, queueWake, logger)
	}()
	overLimitLoopCtx, cancelOverLimitLoop := context.WithCancel(ctx)
	defer cancelOverLimitLoop()
	go tetralsandbox.RunSandboxQueueOverLimitLoop(overLimitLoopCtx, &tetralsandbox.SandboxQueueOverLimitReconciler{
		Queue: queueStore, Finalizer: overLimitFinalizer,
	}, tetralsandbox.SandboxQueueOverLimitInterval)
	environmentBuildLoopCtx, cancelEnvironmentBuildLoop := context.WithCancel(ctx)
	defer cancelEnvironmentBuildLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(environmentBuildLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.EnvironmentBuildJobRunner{
				Queue:     queueClient,
				Store:     environmentStore,
				Providers: providerRegistry,
				Config: tetralsandbox.EnvironmentRunnerConfig{
					WorkspaceID:       workspaceID.String(),
					LeaseOwner:        tetralsandbox.ServiceName,
					MaxJobs:           cfg.EnvironmentBuildConcurrency,
					LeaseDuration:     tetralsandbox.EnvironmentQueueLeaseDuration(cfg),
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	outputCaptureLoopCtx, cancelOutputCaptureLoop := context.WithCancel(ctx)
	defer cancelOutputCaptureLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(outputCaptureLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxOutputCaptureJobRunner{
				Queue: queueClient, Store: outputCaptureStore, Providers: providerRegistry, BlobStore: providerAdapter.BlobStore, Logger: logger,
				Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	outputCaptureCleanupLoopCtx, cancelOutputCaptureCleanupLoop := context.WithCancel(ctx)
	defer cancelOutputCaptureCleanupLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(outputCaptureCleanupLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxOutputCaptureCleanupRunner{
				Queue: queueClient, Store: outputCaptureStore, BlobStore: providerAdapter.BlobStore,
				Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	outputCaptureSweepLoopCtx, cancelOutputCaptureSweepLoop := context.WithCancel(ctx)
	defer cancelOutputCaptureSweepLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(outputCaptureSweepLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			count, err := outputCaptureStore.SweepExpiredCaptures(cycleCtx, workspaceID.String(), storage.Now(), tetralsandbox.SandboxOutputCaptureCleanupBatchSize)
			return count > 0, err
		}, nil, logger)
	}()
	executionLoopCtx, cancelExecutionLoop := context.WithCancel(ctx)
	defer cancelExecutionLoop()
	go func() {
		_ = tetralsandbox.RunSandboxToolExecutionConsumerGroup(
			executionLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval,
			queueClient, executionCoordinator, providerRegistry, mediaMaterializer,
			tetralsandbox.SandboxToolExecutionRunnerConfig{
				LeaseOwner: tetralsandbox.ServiceName, MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
				HeartbeatInterval: cfg.LeaseHeartbeatInterval, PreparationTimeout: cfg.ProviderCommandTimeout,
				LateCommandMargin: cfg.LateCommandMargin,
			},
			queueWake,
			logger,
		)
	}()
	cancellationLoopCtx, cancelCancellationLoop := context.WithCancel(ctx)
	defer cancelCancellationLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(cancellationLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxToolCancelJobRunner{
				Queue: queueClient, Store: executionCoordinator, Providers: providerRegistry,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	backgroundReconcileLoopCtx, cancelBackgroundReconcileLoop := context.WithCancel(ctx)
	defer cancelBackgroundReconcileLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(backgroundReconcileLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxBackgroundReconcileJobRunner{
				Queue: queueClient, Store: backgroundCommandStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxBackgroundRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	backgroundCommandLoopCtx, cancelBackgroundCommandLoop := context.WithCancel(ctx)
	defer cancelBackgroundCommandLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(backgroundCommandLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxBackgroundCommandJobRunner{
				Queue: queueClient, Store: backgroundCommandStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxBackgroundRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	memoryProjectionLoopCtx, cancelMemoryProjectionLoop := context.WithCancel(ctx)
	defer cancelMemoryProjectionLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(memoryProjectionLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxMemoryProjectionJobRunner{
				Queue: queueClient, Store: memoryProjectionStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxMemoryProjectionRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	activationLoopCtx, cancelActivationLoop := context.WithCancel(ctx)
	defer cancelActivationLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(activationLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxActivationJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	materializationLoopCtx, cancelMaterializationLoop := context.WithCancel(ctx)
	defer cancelMaterializationLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(materializationLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxMaterializationJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	releaseLoopCtx, cancelReleaseLoop := context.WithCancel(ctx)
	defer cancelReleaseLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerGroup(releaseLoopCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxReleaseJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	environmentReadyFanoutLoopCtx, cancelEnvironmentReadyFanoutLoop := context.WithCancel(ctx)
	defer cancelEnvironmentReadyFanoutLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(environmentReadyFanoutLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.EnvironmentReadyFanoutJobRunner{
				Queue: queueClient,
				Store: environmentStore,
				Config: tetralsandbox.EnvironmentRunnerConfig{
					WorkspaceID:       workspaceID.String(),
					LeaseOwner:        tetralsandbox.ServiceName,
					MaxJobs:           cfg.EnvironmentReadyFanoutConcurrency,
					LeaseDuration:     tetralsandbox.EnvironmentQueueLeaseDuration(cfg),
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	}()
	resourcePrefixGCLoopCtx, cancelResourcePrefixGCLoop := context.WithCancel(ctx)
	defer cancelResourcePrefixGCLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(resourcePrefixGCLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			jobs, err := (&tetralsandbox.ResourcePrefixGCRunner{
				Client: openResult.Client,
				Blobs:  providerAdapter.BlobStore,
				Config: tetralsandbox.ResourcePrefixGCRunnerConfig{
					WorkspaceID: workspaceID.String(),
					RetryAfter:  cfg.JobPollInterval,
				},
			}).RunOnce(cycleCtx)
			return len(jobs) > 0, err
		}, nil, logger)
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
			workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", openResult.Client)),
		),
		Readiness: readiness,
		Logger:    logger,
	})
}
