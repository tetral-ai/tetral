package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/workload"
	webconnector "github.com/tetral-ai/tetral/services/web-connector"
)

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env webconnector.Env) error {
	logger := workload.NewLogger(os.Stderr, webconnector.ServiceName, env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	cfg, err := webconnector.LoadConfig(env)
	if err != nil {
		return workload.LogStartupFailure(logger, webconnector.ServiceName, err)
	}
	blobCfg, err := blob.LoadConfig()
	if err != nil {
		return workload.LogStartupFailure(logger, webconnector.ServiceName, err)
	}
	if err = blobCfg.AssertProductionReady(); err != nil {
		return workload.LogStartupFailure(logger, webconnector.ServiceName, err)
	}
	blobStore, err := blob.NewS3BlobStore(ctx, blobCfg)
	if err != nil {
		return workload.LogStartupFailure(logger, webconnector.ServiceName, err)
	}
	authCfg, err := grpcauth.LoadConfig(env)
	if err != nil {
		return workload.LogStartupFailure(logger, webconnector.ServiceName, err)
	}
	reviewer, err := grpcauth.NewTokenReviewClientFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, webconnector.ServiceName, err)
	}
	authenticator := grpcauth.NewTokenReviewAuthenticator(reviewer, authCfg)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	metrics := webconnector.NewMetrics()
	backend := webconnector.NewJinaBackend(client, cfg.SearchEndpoint, cfg.ReaderEndpoint, cfg.APIKeys, time.Now).WithMetrics(metrics).WithLogger(logger)
	service := webconnector.NewService(blobStore, backend, webconnector.NewBindingVerifier(cfg.BindingHMACKey, time.Now), metrics, time.Now, nil).WithLogger(logger)
	return webconnector.Run(ctx, cfg, service, metrics, webconnector.RuntimeConfig{Authenticator: authenticator, Logger: logger})
}
