package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/workload"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"

	"google.golang.org/grpc"
)

var runWorkload = workload.Run
var runInternalGRPC = internalgrpc.Run
var openDatabase = dbconnect.OpenPlainDSN
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
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env envReader) error {
	logger := workload.NewLogger(os.Stderr, agentruntimebridge.ServiceNameBridgeAPI, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	database, err := openDatabase(ctx, agentruntimebridge.EnvDatabaseURL, env.Getenv(agentruntimebridge.EnvDatabaseURL))
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	defer func() { _ = database.Client.Close() }()
	if err := verifySchema(ctx, database.Client); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	// Workspace isolation is enforced by row-level policies that a superuser or
	// BYPASSRLS role silently defeats. Bridge is the widest writer in the
	// system, so it refuses to serve on a role that would bypass them.
	if err := database.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(database.Client)
	store.Logger = logger
	bridgeConfig, err := agentruntimebridge.BridgeAPIConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	tokenKey, err := agentruntimebridge.RuntimeBindingTokenHMACKeyFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	store.RuntimeBindingTokenHMACKey = tokenKey
	store.ProviderRescheduleBudget = bridgeConfig.ProviderRescheduleBudget
	store.CompactionRescheduleBudget = bridgeConfig.CompactionRescheduleBudget
	blobConfig, err := blob.LoadConfig()
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	if err := blobConfig.AssertProductionReady(); err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, err)
	}
	blobStore, err := blob.NewS3BlobStore(ctx, blobConfig)
	if err != nil {
		return workload.LogStartupFailure(logger, agentruntimebridge.ServiceNameBridgeAPI, fmt.Errorf("blob store: %w", err))
	}
	store.AttachmentBlobStore = blobStore
	store.FileBlobStore = blobStore
	store.MCPManifestLister = agentruntimebridge.NewGatewayMCPManifestLister(bridgeConfig.MCPConnectorGRPCAddress, grpcauth.FileTokenSource{
		Path: bridgeConfig.GatewayTokenPath,
	})
	agentruntimebridge.StartTransientAttachmentGC(ctx, store, logger, time.Minute, 100)
	return internalgrpc.RunGRPCWorkload(ctx, env, internalgrpc.GRPCWorkloadParams{
		ServiceName:       agentruntimebridge.ServiceNameBridgeAPI,
		HTTPListenEnvKey:  agentruntimebridge.EnvBridgeAPIHTTPAddress,
		HTTPListenDefault: ":8080",
		GRPCListenEnvKey:  agentruntimebridge.EnvBridgeAPIGRPCAddress,
		GRPCListenDefault: ":9090",
		Register:          func(server *grpc.Server) { agentruntimebridge.RegisterBridgeAPI(server, store) },
		MethodAuthorizer:  agentruntimebridge.BridgeAPIMethodAuthorizer,
		ServerOptions: []grpc.ServerOption{
			grpc.MaxRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
			grpc.MaxSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
		},
		DBStatsProvider:      database.Client,
		RunWorkload:          runWorkload,
		RunInternalGRPC:      runInternalGRPC,
		NewTokenReviewClient: func() (grpcauth.TokenReviewClient, error) { return newTokenReviewClient(env) },
		Listen:               listenTCP,
	})
}
