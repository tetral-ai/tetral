package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/workload"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

var runWorkload = workload.Run

const (
	defaultShutdownTimeout        = 10 * time.Second
	envHTTPAddress                = "TETRAL_EVENT_STREAM_HTTP_ADDR"
	envMetricsAddress             = "TETRAL_EVENT_STREAM_METRICS_ADDR"
	envEventStreamDatabaseURL     = "TETRAL_EVENT_STREAM_DATABASE_URL"
	envInternalPrincipalPublicKey = "TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64"
)

type envReader interface {
	Getenv(string) string
}

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

type commandConfig struct {
	ListenAddress         string
	MetricsAddress        string
	DeploymentEnvironment string
	ServiceVersion        string
	PrincipalVerifier     *auth.InternalPrincipalVerifier
}

func main() {
	if err := run(context.Background(), osEnv{}, nil); err != nil {
		os.Exit(1)
	}
}

func configFromEnv(env envReader) (commandConfig, error) {
	listenAddress := env.Getenv(envHTTPAddress)
	if listenAddress == "" {
		listenAddress = ":8080"
	}
	metricsAddress := env.Getenv(envMetricsAddress)
	if metricsAddress == "" {
		metricsAddress = ":8081"
	}
	if metricsAddress == listenAddress {
		return commandConfig{}, workload.NewConfigError(envMetricsAddress + " must not equal " + envHTTPAddress)
	}
	deploymentEnvironment := env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT")
	if deploymentEnvironment == "" {
		deploymentEnvironment = "local"
	}
	serviceVersion := env.Getenv("TETRAL_SERVICE_VERSION")
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}
	principalVerifier, err := loadInternalPrincipalVerifierFromEnv(env)
	if err != nil {
		return commandConfig{}, err
	}
	return commandConfig{
		ListenAddress:         listenAddress,
		MetricsAddress:        metricsAddress,
		DeploymentEnvironment: deploymentEnvironment,
		ServiceVersion:        serviceVersion,
		PrincipalVerifier:     principalVerifier,
	}, nil
}

type startupReadinessClient interface {
	VerifySchema(context.Context) error
	VerifyRuntimeRole(context.Context) error
}

type startupDatabase struct {
	runtimeClient   *dbconnect.Client
	readinessClient startupReadinessClient
}

type openStartupFunc func(context.Context) (startupDatabase, error)

func openStartupDatabaseFromEnv(ctx context.Context) (startupDatabase, error) {
	openResult, err := dbconnect.OpenPlainDSN(ctx, envEventStreamDatabaseURL, os.Getenv(envEventStreamDatabaseURL))
	if err != nil {
		return startupDatabase{}, err
	}
	return startupDatabase{
		runtimeClient:   openResult.Client,
		readinessClient: openResult.Client,
	}, nil
}

func run(ctx context.Context, env envReader, open openStartupFunc) error {
	logger := workload.NewLogger(os.Stderr, "event-stream", env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	cfg, err := configFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "event-stream", err)
	}
	if open == nil {
		open = openStartupDatabaseFromEnv
	}
	database, err := prepareStartupDatabase(ctx, open)
	if err != nil {
		return logStartupFailure(logger, err)
	}
	defer func() { _ = closeStartupDatabase(database) }()
	readiness := workload.NewReadiness()
	httpMetrics := workload.NewHTTPMetrics()
	reader := internaleventstream.NewPostgreSQLReader(database.runtimeClient)
	handler := buildHTTPHandler(readiness, eventstream.NewRouter(reader, cfg.PrincipalVerifier, eventstream.WithLogger(logger), eventstream.WithRequestMetrics(httpMetrics)))
	metricsHandler := workload.HealthRouter(readiness,
		workload.WithHTTPMetrics(httpMetrics),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", database.runtimeClient)),
	)
	readiness.MarkReady()
	return runPublicAndMetricsHTTP(ctx, cfg, readiness, logger, handler, metricsHandler)
}

func runPublicAndMetricsHTTP(
	ctx context.Context,
	cfg commandConfig,
	readiness *workload.Readiness,
	logger *slog.Logger,
	publicHandler http.Handler,
	metricsHandler http.Handler,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- runWorkload(runCtx, workload.Config{
			ServiceName:           "event-stream",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.ListenAddress,
			ListenConfigKey:       envHTTPAddress,
			Handler:               publicHandler,
			Readiness:             readiness,
			ShutdownTimeout:       defaultShutdownTimeout,
			Logger:                logger,
		})
	}()
	go func() {
		results <- runWorkload(runCtx, workload.Config{
			ServiceName:           "event-stream",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.MetricsAddress,
			ListenConfigKey:       envMetricsAddress,
			Handler:               metricsHandler,
			Readiness:             readiness,
			ShutdownTimeout:       defaultShutdownTimeout,
			Logger:                logger,
		})
	}()

	var firstErr error
	for range 2 {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
		cancel()
	}
	return firstErr
}

func prepareStartupDatabase(ctx context.Context, open openStartupFunc) (startupDatabase, error) {
	database, err := open(ctx)
	if err != nil {
		return startupDatabase{}, err
	}
	if database.readinessClient == nil {
		_ = closeStartupDatabase(database)
		return startupDatabase{}, fmt.Errorf("startup database readiness client is required")
	}
	if err := database.readinessClient.VerifySchema(ctx); err != nil {
		_ = closeStartupDatabase(database)
		return startupDatabase{}, err
	}
	if err := database.readinessClient.VerifyRuntimeRole(ctx); err != nil {
		_ = closeStartupDatabase(database)
		return startupDatabase{}, err
	}
	return database, nil
}

func loadInternalPrincipalVerifierFromEnv(env envReader) (*auth.InternalPrincipalVerifier, error) {
	raw := env.Getenv(envInternalPrincipalPublicKey)
	if raw == "" {
		return nil, workload.NewConfigError(envInternalPrincipalPublicKey + " is required")
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(raw)
	if err != nil {
		return nil, workload.NewConfigError(err.Error())
	}
	return verifier, nil
}

func closeStartupDatabase(database startupDatabase) error {
	if database.runtimeClient == nil {
		return nil
	}
	return database.runtimeClient.Close()
}

func buildHTTPHandler(readiness *workload.Readiness, eventRouter http.Handler) http.Handler {
	healthRouter := workload.HealthRouter(readiness)
	notFound := http.HandlerFunc(http.NotFound)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/ready" {
			healthRouter.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/metrics" || r.URL.Path == "/metrics/" {
			notFound.ServeHTTP(w, r)
			return
		}
		eventRouter.ServeHTTP(w, r)
	})
}

// logStartupFailure logs a redacted startup-failure line. Config parsing errors
// use safe static messages; dependency/bootstrap errors remain class-only.
func logStartupFailure(logger *slog.Logger, err error) error {
	return workload.LogStartupFailure(logger, "event-stream", err)
}
