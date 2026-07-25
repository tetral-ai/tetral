package main

import (
	gitproxy "github.com/tetral-ai/tetral/services/git-proxy"

	"context"
	"net"
	"os"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workload"
)

var openDatabase = dbconnect.OpenPlainDSN
var verifySchema = func(ctx context.Context, client *dbconnect.Client) error { return client.VerifySchema(ctx) }
var newEncryptor = vault.NewEncryptor
var runGitProxy = gitproxy.Run
var listenTCP = net.Listen

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env gitproxy.Env) error {
	logger := workload.NewLogger(os.Stderr, gitproxy.ServiceName, env.Getenv(gitproxy.EnvDeploymentEnvironment), env.Getenv(gitproxy.EnvServiceVersion))
	cfg, err := gitproxy.ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, gitproxy.ServiceName, err)
	}
	openResult, err := openDatabase(ctx, gitproxy.EnvDatabaseURL, cfg.DatabaseURL)
	if err != nil {
		return workload.LogStartupFailure(logger, gitproxy.ServiceName, err)
	}
	defer func() { _ = openResult.Client.Close() }()
	if err := verifySchema(ctx, openResult.Client); err != nil {
		return workload.LogStartupFailure(logger, gitproxy.ServiceName, err)
	}
	if err := openResult.Client.VerifyRuntimeRole(ctx); err != nil {
		return workload.LogStartupFailure(logger, gitproxy.ServiceName, err)
	}
	encryptor, err := newEncryptor(cfg.VaultKey)
	if err != nil {
		return workload.LogStartupFailure(logger, gitproxy.ServiceName, err)
	}
	return runGitProxy(ctx, cfg, openResult.Client, encryptor, gitproxy.RuntimeConfig{
		Listen:          listenTCP,
		Logger:          logger,
		DBStatsProvider: openResult.Client,
	})
}
