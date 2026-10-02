package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var runWorkload = workload.Run
var listenTCP = net.Listen
var openDatabase = func(ctx context.Context, _ string, dsn string) (dbconnect.OpenResult, error) {
	return dbconnect.OpenProtectedDSN(ctx, dsn, os.Getenv("TETRAL_DATABASE_TLS_CA_PATH"), os.Getenv("TETRAL_DATABASE_TLS_SERVER_NAME"))
}
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env jobrunner.Env) error {
	ctx, stopSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	resourcesCtx, cancelResources := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelResources()
	diagnosticConfig, err := workload.DiagnosticConfigFromEnv(env.Getenv)
	owner := workload.NewProcessLogger(os.Stderr, jobrunner.ServiceNameJobRunner, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), diagnosticConfig)
	defer owner.CloseWithBudget()
	logger := owner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	cfg, err := jobrunner.JobRunnerConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	database, err := openDatabase(ctx, jobrunner.EnvDatabaseURL, cfg.DatabaseURL)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer func() { _ = database.Client.Close() }()
	if err := verifySchema(ctx, database.Client); err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseSchema, err))
	}
	// Workspace isolation is enforced by row-level policies that a superuser or
	// BYPASSRLS role silently defeats. The Job Runner sweeps every workspace, so
	// it refuses to serve on a role that would bypass them.
	if err := database.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	workspaceStore := workspace.NewStore(database.RawDatabaseForExcludedStores)
	if err := jobrunner.ValidateRuntimeInboxEventRefBounds(ctx, database.Client, workspaceStore); err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseSchema, err))
	}
	if err := transportsecurity.WaitForRoutingProxy(ctx, env.Getenv(transportsecurity.EnvRoutingProxyRequired) == "true"); err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, err)
	}
	var directTLS *transportsecurity.Owner
	var directDial []grpc.DialOption
	if cfg.TransportProfile == "hardened" {
		directTLS, err = transportsecurity.OpenFromEnv(ctx, env.Getenv, "runtime-direct")
		if err != nil {
			return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, err)
		}
		defer func() {
			if closeErr := directTLS.Close(); closeErr != nil {
				logger.Error("shutdown.resource_close_failed", "operation", "close_runtime_direct_tls", "error.class", "resource_shutdown", "error.code", "runtime_direct_tls_close_failed")
			}
		}()
		credentials, credentialsErr := directTLS.GRPCClientCredentials(cfg.RuntimeDirectServerName, cfg.RuntimeDirectPeerURI)
		if credentialsErr != nil {
			return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, credentialsErr)
		}
		directDial = append(directDial, grpc.WithTransportCredentials(credentials))
	}
	commandClient := jobrunner.NewRuntimePodCommandClient(internalgrpcauth.FileTokenSource{Path: cfg.RuntimePodTokenPath}, directDial...)
	commandClient.Policy = cfg.CommandPolicy
	if directTLS != nil {
		if err := directTLS.SetTrustActivationObserver(commandClient.RetireChannels); err != nil {
			return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, err)
		}
	}
	defer func() {
		if closeErr := commandClient.Close(); closeErr != nil {
			logger.Error("shutdown.resource_close_failed", "operation", "close_runtime_command_channels", "error.class", "resource_shutdown", "error.code", "runtime_command_channels_close_failed")
		}
	}()
	listener, err := listenTCP("tcp", cfg.HTTPAddress)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseListener, err))
	}
	defer func() { _ = listener.Close() }()
	dialOptions := append([]grpc.DialOption{}, internalgrpc.QueueRPCDialOptions()...)
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	queueConn, err := grpc.NewClient(cfg.QueueGRPCAddress, dialOptions...)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer func() { _ = queueConn.Close() }()
	visibilityConfig, err := enginekubernetes.LoadConfig(env)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	kubernetesCache := enginekubernetes.NewWatcherCache(cfg.KubernetesNamespace, enginekubernetes.WithLogger(
		logger,
	))
	kubernetesClient, err := enginekubernetes.NewInClusterVisibilityClient()
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	watchHandles, err := enginekubernetes.SyncAndWatch(resourcesCtx, kubernetesClient, enginekubernetes.Config{
		Namespace:                 visibilityConfig.Namespace,
		AgentRuntimeLabelSelector: visibilityConfig.AgentRuntimeLabelSelector,
		AgentRuntimeServiceName:   visibilityConfig.AgentRuntimeServiceName,
	}, kubernetesCache)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer watchHandles.Stop()
	readiness := workload.NewReadiness().WithReadinessDependency(kubernetesCache.Ready)
	readiness.MarkReady()
	blobConfig, err := blob.LoadConfig()
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	if err := blobConfig.AssertProductionReady(); err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	blobStore, err := blob.NewProtectedS3BlobStore(ctx, blobConfig)
	if err != nil {
		return workload.LogStartupFailure(logger, jobrunner.ServiceNameJobRunner, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer func() {
		if closeErr := blobStore.Close(); closeErr != nil {
			logger.Error("shutdown.resource_close_failed", "operation", "close_blob_store", "error.class", "resource_shutdown", "error.code", "blob_store_close_failed")
		}
	}()
	deliveryStore := jobrunner.NewJobRunnerRuntimeDeliveryStore(
		database.Client,
		logger,
		cfg,
		kubernetesCache.BindingVisibilitySnapshot,
	)
	if closer, ok := deliveryStore.MCPManifestLister.(interface{ Close() error }); ok {
		defer func() {
			if closeErr := closer.Close(); closeErr != nil {
				logger.Error("shutdown.resource_close_failed", "operation", "close_mcp_manifest_channel", "error.class", "resource_shutdown", "error.code", "mcp_manifest_channel_close_failed")
			}
		}()
	}
	resolver := deliveryStore.TargetResolver.(jobrunner.KubernetesRuntimeTargetResolver)
	resolver.GetPod = kubernetesClient.GetPod
	deliveryStore.TargetResolver = resolver
	deliveryStore.AttachmentBlobStore = blobStore
	acquisitionCtx, closeAcquisition := context.WithCancel(ctx)
	defer closeAcquisition()
	var loopWorkers sync.WaitGroup
	defer func() { closeAcquisition(); loopWorkers.Wait() }()
	queueWake := queue.NewWakeSignal()
	loopWorkers.Add(1)
	go func() {
		defer loopWorkers.Done()
		_ = queue.RunNotificationListener(acquisitionCtx, queue.PostgreSQLNotificationListener{Client: database.Client}, queue.ConsumerClassJobRunner, queueWake, logger)
	}()
	loopWorkers.Add(1)
	go func() {
		defer loopWorkers.Done()
		_ = jobrunner.RunJobRunnerLoop(acquisitionCtx, &jobrunner.JobRunner{
			Queue:      jobrunner.QueueClientFromGRPC(queuev1.NewQueueServiceClient(queueConn)),
			Workspaces: workspaceStore,
			Deliverer: jobrunner.RuntimePodDirectDeliverer{
				Store:  deliveryStore,
				Sender: commandClient,
			},
			Config: cfg,
		}, logger, queueWake)
	}()
	httpMetrics := workload.NewHTTPMetrics()
	return runWorkload(ctx, workload.Config{
		ServiceName:           jobrunner.ServiceNameJobRunner,
		DeploymentEnvironment: cfg.DeploymentEnvironment,
		ServiceVersion:        cfg.ServiceVersion,
		ListenAddress:         cfg.HTTPAddress,
		ListenConfigKey:       jobrunner.EnvJobRunnerHTTPAddress,
		Listener:              listener,
		Handler: workload.HealthRouter(readiness,
			workload.WithHTTPMetrics(httpMetrics),
			workload.WithMetricsCollector("http", httpMetrics.Collector()),
			workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger)),
			workload.WithMetricsCollector("placement", deliveryStore.PlacementMetrics.Collector()),
			workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", database.Client)),
		),
		Readiness: readiness,
		Logger:    logger,
	})
}
