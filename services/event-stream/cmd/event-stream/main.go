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
	"github.com/tetral-ai/tetral/internal/transportsecurity"
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
	HTTPTransport         transportsecurity.HTTPConfig
	ListenAddress         string
	MetricsAddress        string
	DeploymentEnvironment string
	ServiceVersion        string
	PrincipalVerifier     *auth.InternalPrincipalVerifier
	StreamConfig          eventstream.StreamConfig
	NATSConfig            eventstream.NATSConfig
}

func main() {
	if err := workload.RunProcess(func(ctx context.Context) error { return run(ctx, osEnv{}, nil) }); err != nil {
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
	resource := workload.ResourceConfigFromEnv(env.Getenv)
	httpTransport, err := transportsecurity.HTTPConfigFromEnv(env.Getenv)
	if err != nil {
		return commandConfig{}, workload.NewConfigError(err.Error())
	}
	principalVerifier, err := loadInternalPrincipalVerifierFromEnv(env)
	if err != nil {
		return commandConfig{}, err
	}
	streamConfig, err := eventstream.StreamConfigFromEnv(env.Getenv)
	if err != nil {
		return commandConfig{}, workload.NewConfigError(err.Error())
	}
	natsConfig, err := eventstream.NATSConfigFromEnv(env.Getenv)
	if err != nil {
		return commandConfig{}, err
	}
	return commandConfig{
		HTTPTransport:         httpTransport,
		ListenAddress:         listenAddress,
		MetricsAddress:        metricsAddress,
		DeploymentEnvironment: resource.DeploymentEnvironment,
		ServiceVersion:        resource.ServiceVersion,
		PrincipalVerifier:     principalVerifier,
		StreamConfig:          streamConfig,
		NATSConfig:            natsConfig,
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
	openResult, err := dbconnect.OpenProtectedDSN(ctx, os.Getenv(envEventStreamDatabaseURL), os.Getenv("TETRAL_DATABASE_TLS_CA_PATH"), os.Getenv("TETRAL_DATABASE_TLS_SERVER_NAME"))
	if err != nil {
		return startupDatabase{}, err
	}
	return startupDatabase{
		runtimeClient:   openResult.Client,
		readinessClient: openResult.Client,
	}, nil
}

func run(ctx context.Context, env envReader, open openStartupFunc) error {
	diagnostics, diagnosticErr := workload.DiagnosticConfigFromEnv(env.Getenv)
	diagnosticOwner := workload.NewProcessLogger(os.Stderr, "event-stream", env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), diagnostics)
	defer diagnosticOwner.CloseWithBudget()
	logger := diagnosticOwner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if diagnosticErr != nil {
		return workload.LogStartupFailure(logger, "event-stream", diagnosticErr)
	}
	cfg, err := configFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "event-stream", err)
	}
	workload.ConfigureProcessShutdown(ctx, defaultShutdownTimeout+5*time.Second, diagnosticOwner)
	if open == nil {
		open = openStartupDatabaseFromEnv
	}
	database, err := prepareStartupDatabase(ctx, open)
	if err != nil {
		return logStartupFailure(logger, err)
	}
	defer workload.ProcessCleanup(ctx, func() { _ = closeStartupDatabase(database) })
	readiness := workload.NewReadiness()
	httpMetrics := workload.NewHTTPMetrics("event-stream")
	reader := internaleventstream.NewPostgreSQLReader(database.runtimeClient)
	previewMetrics := eventstream.NewPreviewMetrics()
	var previewHub *eventstream.PreviewHub
	if cfg.NATSConfig.Enabled() {
		transport, err := eventstream.NewNATSPreviewTransport(ctx, cfg.NATSConfig, cfg.StreamConfig, previewMetrics, logger)
		if err != nil {
			return logStartupFailure(logger, err)
		}
		defer workload.ProcessCleanup(ctx, transport.Close)
		previewHub, err = eventstream.NewPreviewHub(transport, cfg.StreamConfig, previewMetrics)
		if err != nil {
			return logStartupFailure(logger, err)
		}
		defer workload.ProcessCleanup(ctx, previewHub.Close)
	}
	handler := buildHTTPHandler(readiness, eventstream.NewRouter(reader, cfg.PrincipalVerifier, eventstream.WithLogger(logger), eventstream.WithRequestMetrics(httpMetrics), eventstream.WithStreamConfig(cfg.StreamConfig), eventstream.WithStreamShutdownContext(ctx), eventstream.WithPreviewHub(previewHub), eventstream.WithPreviewMetrics(previewMetrics)))
	metricsHandler := workload.HealthRouter(readiness,
		workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger)),
		workload.WithMetricsCollector("previews", previewMetrics.Collector()),
		workload.WithHTTPMetrics(httpMetrics),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", database.runtimeClient)),
	)
	readiness.MarkReady()
	return runPublicAndMetricsHTTP(ctx, cfg, readiness, logger, handler, metricsHandler, httpMetrics.Operations)
}

func runPublicAndMetricsHTTP(
	ctx context.Context,
	cfg commandConfig,
	readiness *workload.Readiness,
	logger *slog.Logger,
	publicHandler http.Handler,
	metricsHandler http.Handler,
	operationMetrics ...*workload.OperationMetrics,
) error {
	var metrics *workload.OperationMetrics
	if len(operationMetrics) != 0 {
		metrics = operationMetrics[0]
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tlsOwner, tlsConfig, err := cfg.HTTPTransport.Open(ctx)
	if err != nil {
		return workload.NewConfigError("native HTTP credential preparation failed")
	}
	if tlsOwner != nil {
		defer func() { _ = tlsOwner.Close() }()
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
			TLSConfig:             tlsConfig,
			Metrics:               metrics,
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
			Metrics:               metrics,
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
