package main

import (
	"context"
	"net"
	"os"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var runWorkload = workload.Run
var listenTCP = net.Listen
var openDatabase = dbconnect.OpenPlainDSN
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env agentruntimebridge.Env) error {
	logger := workload.NewLogger(os.Stderr, agentruntimebridge.ServiceNameJobRunner, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	cfg, err := agentruntimebridge.JobRunnerConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	database, err := openDatabase(ctx, agentruntimebridge.EnvDatabaseURL, cfg.DatabaseURL)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	defer func() { _ = database.Client.Close() }()
	if err := verifySchema(ctx, database.Client); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	// Workspace isolation is enforced by row-level policies that a superuser or
	// BYPASSRLS role silently defeats. The Job Runner sweeps every workspace, so
	// it refuses to serve on a role that would bypass them.
	if err := database.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	workspaceStore := workspace.NewStore(database.RawDatabaseForExcludedStores)
	if err := agentruntimebridge.ValidateRuntimeInboxEventRefBounds(ctx, database.Client, workspaceStore); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	listener, err := listenTCP("tcp", cfg.HTTPAddress)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	defer func() { _ = listener.Close() }()
	dialOptions := append([]grpc.DialOption{}, internalgrpc.QueueRPCDialOptions()...)
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	queueConn, err := grpc.NewClient(cfg.QueueGRPCAddress, dialOptions...)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	defer func() { _ = queueConn.Close() }()
	visibilityConfig, err := enginekubernetes.LoadConfig(env)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	kubernetesCache := enginekubernetes.NewWatcherCache(cfg.KubernetesNamespace, enginekubernetes.WithLogger(
		logger,
	))
	kubernetesClient, err := enginekubernetes.NewInClusterVisibilityClient()
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	watchHandles, err := enginekubernetes.SyncAndWatch(ctx, kubernetesClient, enginekubernetes.Config{
		Namespace:                 visibilityConfig.Namespace,
		AgentRuntimeLabelSelector: visibilityConfig.AgentRuntimeLabelSelector,
		AgentRuntimeServiceName:   visibilityConfig.AgentRuntimeServiceName,
	}, kubernetesCache)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameJobRunner, err)
	}
	defer watchHandles.Stop()
	readiness := workload.NewReadiness().WithReadinessDependency(kubernetesCache.Ready)
	readiness.MarkReady()
	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	go func() {
		_ = agentruntimebridge.RunJobRunnerLoop(loopCtx, &agentruntimebridge.JobRunner{
			Queue:      agentruntimebridge.QueueClientFromGRPC(queuev1.NewQueueServiceClient(queueConn)),
			Workspaces: workspaceStore,
			Deliverer: agentruntimebridge.RuntimePodDirectDeliverer{
				Store: agentruntimebridge.NewJobRunnerRuntimeDeliveryStore(
					database.Client,
					logger,
					cfg,
					kubernetesCache.BindingVisibilitySnapshot,
				),
				Sender: agentruntimebridge.NewRuntimePodCommandClient(internalgrpcauth.FileTokenSource{
					Path: cfg.RuntimePodTokenPath,
				}),
			},
			Config: cfg,
		}, logger)
	}()
	httpMetrics := workload.NewHTTPMetrics()
	return runWorkload(ctx, workload.Config{
		ServiceName:           agentruntimebridge.ServiceNameJobRunner,
		DeploymentEnvironment: cfg.DeploymentEnvironment,
		ServiceVersion:        cfg.ServiceVersion,
		ListenAddress:         cfg.HTTPAddress,
		ListenConfigKey:       agentruntimebridge.EnvJobRunnerHTTPAddress,
		Listener:              listener,
		Handler: workload.HealthRouter(readiness,
			workload.WithHTTPMetrics(httpMetrics),
			workload.WithMetricsCollector("http", httpMetrics.Collector()),
			workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", database.Client)),
		),
		Readiness: readiness,
		Logger:    logger,
	})
}
