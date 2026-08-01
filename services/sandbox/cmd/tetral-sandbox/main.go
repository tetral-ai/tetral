package main

import (
	"context"
	"net"
	"os"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var runWorkload = workload.Run
var runInternalGRPC = internalgrpc.Run
var openDatabase = dbconnect.OpenPlainDSN
var newAuthenticator func(envReader, grpcauth.Config) (internalgrpc.Authenticator, error) = func(env envReader, cfg grpcauth.Config) (internalgrpc.Authenticator, error) {
	return grpcauth.NewStaticBearerAuthenticatorFromFile(env.Getenv(tetralsandbox.EnvInternalGRPCTokenPath), cfg)
}
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
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	openResult, err := openDatabase(ctx, tetralsandbox.EnvPostgresDSN, cfg.PostgresDSN)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	defer func() { _ = openResult.Client.Close() }()
	if err := verifySchema(ctx, openResult.Client); err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	if err := openResult.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	dialOptions := append([]grpc.DialOption{}, internalgrpc.QueueRPCDialOptions()...)
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	queueConn, err := grpc.NewClient(cfg.QueueGRPCAddress, dialOptions...)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	defer func() { _ = queueConn.Close() }()
	providerAdapter, err := tetralsandbox.NewDaytonaAdapter(ctx, cfg, openResult.Client)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	provider := providerAdapter.Lifecycle
	artifactBuilder, err := sandboxdriver.NewDaytonaArtifactBuilder(cfg.Daytona)
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	memoryMaterializer, ok := providerAdapter.Tools.(*sandboxdriver.DaytonaHelperExecutor)
	if !ok {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, &tetralsandbox.ConfigError{Message: "daytona tool executor is unavailable"})
	}
	resourceMaterializer, ok := providerAdapter.Resources.(*tetralsandbox.DaytonaResourceMaterializer)
	if !ok {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, &tetralsandbox.ConfigError{Message: "daytona resource materializer is unavailable"})
	}
	resourcePreparer, ok := resourceMaterializer.Projection.(*tetralsandbox.ResourceProjectionPreparer)
	if !ok {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, &tetralsandbox.ConfigError{Message: "daytona resource projection is unavailable"})
	}
	providerRegistry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: providerAdapter,
	})
	if err != nil {
		return workload.LogStartupFailure(logger, tetralsandbox.ServiceName, err)
	}
	store := sandbox.NewPostgreSQLStore(openResult.Client)
	serviceOptions := tetralsandbox.SandboxServiceOptionsWithDatabase(cfg, openResult.Client, memoryMaterializer)
	serviceOptions = append(serviceOptions,
		sandbox.WithSessionResourcePreparer(resourcePreparer),
		sandbox.WithMemoryProjection(store, memoryMaterializer),
		sandbox.WithSessionPreparationStore(store),
	)
	service := sandbox.NewService(store, provider, serviceOptions...)
	queueClient := tetralsandbox.SessionPrepareQueueFromGRPC(queuev1.NewQueueServiceClient(queueConn))
	queueStore := queue.NewPostgreSQLStore(openResult.Client)
	workspaceStore := workspace.NewStore(openResult.RawDatabaseForExcludedStores)
	executionCoordinator := tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(openResult.Client, cfg.ResourceCredentialRefreshMargin)
	lifecycleStore := tetralsandbox.NewPostgreSQLSandboxLifecycleStore(openResult.Client, store, cfg.ResourceCredentialRefreshMargin)
	backgroundCommandStore := tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(openResult.Client)
	memoryProjectionStore := tetralsandbox.NewPostgreSQLSandboxMemoryProjectionStore(openResult.Client)
	outputCaptureStore := tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(openResult.Client)
	overLimitFinalizer := tetralsandbox.NewPostgreSQLSandboxQueueOverLimitFinalizer(openResult.Client)
	environmentStore := tetralsandbox.NewEnvironmentArtifactStore(openResult.Client)
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
				Queue:   queueClient,
				Store:   environmentStore,
				Builder: artifactBuilder,
				Config: tetralsandbox.EnvironmentRunnerConfig{
					WorkspaceID:       workspaceID.String(),
					LeaseOwner:        tetralsandbox.ServiceName,
					MaxJobs:           cfg.EnvironmentBuildConcurrency,
					LeaseDuration:     tetralsandbox.EnvironmentQueueLeaseDuration(cfg),
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	outputCaptureLoopCtx, cancelOutputCaptureLoop := context.WithCancel(ctx)
	defer cancelOutputCaptureLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(outputCaptureLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxOutputCaptureJobRunner{
				Queue: queueClient, Store: outputCaptureStore, Providers: providerRegistry, BlobStore: providerAdapter.BlobStore, Logger: logger,
				Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	outputCaptureCleanupLoopCtx, cancelOutputCaptureCleanupLoop := context.WithCancel(ctx)
	defer cancelOutputCaptureCleanupLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(outputCaptureCleanupLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxOutputCaptureCleanupRunner{
				Queue: queueClient, Store: outputCaptureStore, BlobStore: providerAdapter.BlobStore,
				Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	outputCaptureSweepLoopCtx, cancelOutputCaptureSweepLoop := context.WithCancel(ctx)
	defer cancelOutputCaptureSweepLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(outputCaptureSweepLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			count, err := outputCaptureStore.SweepExpiredCaptures(cycleCtx, workspaceID.String(), storage.Now(), tetralsandbox.SandboxOutputCaptureCleanupBatchSize)
			return count > 0, err
		})
	}()
	executionLoopCtx, cancelExecutionLoop := context.WithCancel(ctx)
	defer cancelExecutionLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(executionLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxToolExecutionJobRunner{
				Queue: queueClient, Coordinator: executionCoordinator, Providers: providerRegistry,
				Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval, PreparationTimeout: cfg.PreparationCommandTimeout,
					LateCommandMargin: cfg.LateCommandMargin,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	backgroundReconcileLoopCtx, cancelBackgroundReconcileLoop := context.WithCancel(ctx)
	defer cancelBackgroundReconcileLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(backgroundReconcileLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxBackgroundReconcileJobRunner{
				Queue: queueClient, Store: backgroundCommandStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxBackgroundRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	backgroundCommandLoopCtx, cancelBackgroundCommandLoop := context.WithCancel(ctx)
	defer cancelBackgroundCommandLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(backgroundCommandLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxBackgroundCommandJobRunner{
				Queue: queueClient, Store: backgroundCommandStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxBackgroundRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	memoryProjectionLoopCtx, cancelMemoryProjectionLoop := context.WithCancel(ctx)
	defer cancelMemoryProjectionLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(memoryProjectionLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxMemoryProjectionJobRunner{
				Queue: queueClient, Store: memoryProjectionStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxMemoryProjectionRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	activationLoopCtx, cancelActivationLoop := context.WithCancel(ctx)
	defer cancelActivationLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(activationLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxActivationJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	materializationLoopCtx, cancelMaterializationLoop := context.WithCancel(ctx)
	defer cancelMaterializationLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(materializationLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxMaterializationJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: cfg.SessionPrepareConcurrency, LeaseDuration: cfg.SessionPrepareLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
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
		})
	}()
	environmentFailedFanoutLoopCtx, cancelEnvironmentFailedFanoutLoop := context.WithCancel(ctx)
	defer cancelEnvironmentFailedFanoutLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(environmentFailedFanoutLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.EnvironmentFailedFanoutJobRunner{
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
		})
	}()
	prepareLoopCtx, cancelPrepareLoop := context.WithCancel(ctx)
	defer cancelPrepareLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(prepareLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SessionPrepareJobRunner{
				Queue:   queueClient,
				Handler: service,
				Config: tetralsandbox.SessionPrepareRunnerConfig{
					WorkspaceID:       workspaceID.String(),
					LeaseOwner:        tetralsandbox.ServiceName,
					MaxJobs:           cfg.SessionPrepareConcurrency,
					LeaseDuration:     tetralsandbox.SessionPrepareQueueLeaseDuration(cfg),
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		})
	}()
	resourcePrefixGCLoopCtx, cancelResourcePrefixGCLoop := context.WithCancel(ctx)
	defer cancelResourcePrefixGCLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(resourcePrefixGCLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			jobs, err := (&tetralsandbox.ResourcePrefixGCRunner{
				Client:   openResult.Client,
				Preparer: resourcePreparer,
				Config: tetralsandbox.ResourcePrefixGCRunnerConfig{
					WorkspaceID: workspaceID.String(),
					RetryAfter:  cfg.JobPollInterval,
				},
			}).RunOnce(cycleCtx)
			return len(jobs) > 0, err
		})
	}()
	staleCreatingLoopCtx, cancelStaleCreatingLoop := context.WithCancel(ctx)
	defer cancelStaleCreatingLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(staleCreatingLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			sandboxIDs, err := (&tetralsandbox.StaleCreatingReconciler{
				Client:  openResult.Client,
				Handler: service,
				Config: tetralsandbox.StaleCreatingReconcilerConfig{
					WorkspaceID: workspaceID.String(),
					StaleAfter:  cfg.StatusFreshnessWindow,
				},
			}).RunOnce(cycleCtx)
			return len(sandboxIDs) > 0, err
		})
	}()
	startupCleanupLoopCtx, cancelStartupCleanupLoop := context.WithCancel(ctx)
	defer cancelStartupCleanupLoop()
	go func() {
		_ = tetralsandbox.RunWorkspaceConsumerLoop(startupCleanupLoopCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			sandboxIDs, err := (&tetralsandbox.StartupCleanupReconciler{
				Client:  openResult.Client,
				Handler: service,
				Config: tetralsandbox.StartupCleanupReconcilerConfig{
					WorkspaceID: workspaceID.String(),
				},
			}).RunOnce(cycleCtx)
			return len(sandboxIDs) > 0, err
		})
	}()
	handler := tetralsandbox.NewReleaseHandler(openResult.Client, service, store)
	return internalgrpc.RunGRPCWorkload(ctx, env, internalgrpc.GRPCWorkloadParams{
		ServiceName:       tetralsandbox.ServiceName,
		HTTPListenEnvKey:  tetralsandbox.EnvHTTPAddress,
		HTTPListenDefault: cfg.HTTPAddress,
		GRPCListenEnvKey:  tetralsandbox.EnvGRPCAddress,
		GRPCListenDefault: cfg.GRPCAddress,
		Register:          func(server *grpc.Server) { tetralsandbox.Register(server, handler) },
		MethodAuthorizer:  tetralsandbox.SandboxServiceMethodAuthorizer,
		DBStatsProvider:   openResult.Client,
		RunWorkload:       runWorkload,
		RunInternalGRPC:   runInternalGRPC,
		NewAuthenticator:  func(cfg grpcauth.Config) (internalgrpc.Authenticator, error) { return newAuthenticator(env, cfg) },
		Listen:            listenTCP,
	})
}
