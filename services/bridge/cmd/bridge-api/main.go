package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"

	"google.golang.org/grpc"
)

var runWorkload = workload.Run
var runInternalGRPC = internalgrpc.Run
var openDatabase = func(ctx context.Context, _ string, dsn string) (dbconnect.OpenResult, error) {
	return dbconnect.OpenProtectedDSN(ctx, dsn, os.Getenv("TETRAL_DATABASE_TLS_CA_PATH"), os.Getenv("TETRAL_DATABASE_TLS_SERVER_NAME"))
}
var newBlobStore = blob.NewProtectedS3BlobStore
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }
var newTokenReviewClient func(envReader) (grpcauth.TokenReviewClient, error) = func(env envReader) (grpcauth.TokenReviewClient, error) {
	return grpcauth.NewTokenReviewClientFromEnv(env)
}
var listenTCP = net.Listen

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

func run(ctx context.Context, env envReader) error {
	ctx, stopSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	resourceCtx, cancelResources := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelResources()
	diagnosticConfig, err := workload.DiagnosticConfigFromEnv(env.Getenv)
	owner := workload.NewProcessLogger(os.Stderr, agentruntimebridge.ServiceNameBridgeAPI, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), diagnosticConfig)
	defer owner.CloseWithBudget()
	logger := owner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	bridgeConfig, err := agentruntimebridge.BridgeAPIConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	workload.ConfigureProcessShutdown(ctx, bridgeConfig.LifecyclePolicy.DrainTimeout+bridgeConfig.LifecyclePolicy.CancelJoinTimeout, owner)
	database, err := openDatabase(resourceCtx, agentruntimebridge.EnvDatabaseURL, env.Getenv(agentruntimebridge.EnvDatabaseURL))
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	defer workload.ProcessCleanup(ctx, func() { _ = database.Client.Close() })
	if err := verifySchema(ctx, database.Client); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseSchema, err))
	}
	// Workspace isolation is enforced by row-level policies that a superuser or
	// BYPASSRLS role silently defeats. Bridge is the widest writer in the
	// system, so it refuses to serve on a role that would bypass them.
	if err := database.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, err))
	}
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(database.Client)
	store.Logger = logger
	tokenKey, err := agentruntimebridge.RuntimeBindingTokenHMACKeyFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	store.RuntimeBindingTokenHMACKey = tokenKey
	store.ProcessPolicy = bridgeConfig.ProcessPolicy
	store.LifecyclePolicy = bridgeConfig.LifecyclePolicy
	if err := transportsecurity.WaitForRoutingProxy(ctx, env.Getenv(transportsecurity.EnvRoutingProxyRequired) == "true"); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	store.ProviderRescheduleBudget = bridgeConfig.ProviderRescheduleBudget
	store.CompactionRescheduleBudget = bridgeConfig.CompactionRescheduleBudget
	blobConfig, err := blob.LoadConfig()
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	if err := blobConfig.AssertProductionReady(); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseConfiguration, err))
	}
	blobStore, err := newBlobStore(resourceCtx, blobConfig)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, fmt.Errorf("blob store: %w", err)))
	}
	defer func() {
		workload.BeginProcessShutdown(ctx)
		if closeErr := blobStore.Close(); closeErr != nil {
			logger.Error("shutdown.resource_close_failed", "operation", "close_blob_store", "error.class", "resource_shutdown", "error.code", "blob_store_close_failed")
		}
	}()
	store.AttachmentBlobStore = blobStore
	store.FileBlobStore = blobStore
	manifestLister := mcpmanifest.NewConnectorLister(bridgeConfig.MCPConnectorGRPCAddress, grpcauth.FileTokenSource{
		Path: bridgeConfig.GatewayTokenPath,
	})
	store.MCPManifestLister = manifestLister
	defer func() {
		workload.BeginProcessShutdown(ctx)
		if closeErr := manifestLister.Close(); closeErr != nil {
			logger.Error("shutdown.resource_close_failed", "operation", "close_mcp_manifest_channel", "error.class", "resource_shutdown", "error.code", "mcp_manifest_channel_close_failed")
		}
	}()
	// One process-local LISTEN connection feeds AwaitSandboxExecution waiters
	// their wake hints; initial readiness and reconnect trigger catch-up reads.
	executionResultListenerCtx, cancelExecutionResultListener := context.WithCancel(resourceCtx)
	executionResultListenerDone := make(chan struct{})
	defer func() {
		workload.BeginProcessShutdown(ctx)
		cancelExecutionResultListener()
		<-executionResultListenerDone
	}()
	go func() {
		defer close(executionResultListenerDone)
		if err := store.RunExecutionResultListener(executionResultListenerCtx); err != nil && executionResultListenerCtx.Err() == nil {
			logger.Error("bridge.execution_result_listener.stopped",
				"operation", "agentruntimebridge.listen_sandbox_execution_result",
				"error.class", "bridge_execution_result_listener_error",
				"error.message_safe", "sandbox execution result listener stopped",
				"terminal", true,
			)
		}
	}()
	stopAttachmentGC := agentruntimebridge.StartTransientAttachmentGC(resourceCtx, store, logger, time.Minute, 100)
	defer workload.ProcessCleanup(ctx, stopAttachmentGC)
	return internalgrpc.RunGRPCWorkload(ctx, env, internalgrpc.GRPCWorkloadParams{
		ServiceName:       agentruntimebridge.ServiceNameBridgeAPI,
		ShutdownTimeout:   bridgeConfig.LifecyclePolicy.DrainTimeout,
		CancelJoinTimeout: bridgeConfig.LifecyclePolicy.CancelJoinTimeout,
		Logger:            logger,
		HTTPListenEnvKey:  agentruntimebridge.EnvBridgeAPIHTTPAddress,
		HTTPListenDefault: ":8080",
		GRPCListenEnvKey:  agentruntimebridge.EnvBridgeAPIGRPCAddress,
		GRPCListenDefault: ":9090",
		Register:          func(server *grpc.Server) { agentruntimebridge.RegisterBridgeAPI(server, store) },
		MethodAuthorizer:  agentruntimebridge.BridgeAPIMethodAuthorizer,
		// UPDATE-WITH: internal/sessionrpc/bounds.go
		// (MaxBridgeAPIGRPCMessageBytes); services/agent-runtime/packages/
		// runtime-pod/src/bounds.ts (MaxBridgeDurableContextGrpcMessageBytes).
		ServerOptions: []grpc.ServerOption{
			grpc.MaxRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
			grpc.MaxSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
		},
		DBStatsProvider:      database.Client,
		RunWorkload:          runWorkload,
		RunInternalGRPC:      runInternalGRPC,
		NewTokenReviewClient: func() (grpcauth.TokenReviewClient, error) { return newTokenReviewClient(env) },
		Listen:               listenTCP,
	})
}
