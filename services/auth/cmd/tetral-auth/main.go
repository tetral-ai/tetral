package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	tetralauth "github.com/tetral-ai/tetral/services/auth"

	"github.com/tetral-ai/tetral/internal/workload"
)

var runWorkload = workload.Run

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := workload.RunProcess(func(ctx context.Context) error { return run(ctx, osEnv{}) }); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env tetralauth.Env) error {
	diagnostics, diagnosticErr := workload.DiagnosticConfigFromEnv(env.Getenv)
	diagnosticOwner := workload.NewProcessLogger(os.Stderr, "auth", env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"), diagnostics)
	defer diagnosticOwner.CloseWithBudget()
	logger := diagnosticOwner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if diagnosticErr != nil {
		return workload.LogStartupFailure(logger, "auth", diagnosticErr)
	}
	cfg, err := tetralauth.ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	workload.ConfigureProcessShutdown(ctx, tetralauth.DefaultShutdownTimeout+5*time.Second, diagnosticOwner)
	httpMetrics := workload.NewHTTPMetrics("auth")
	app, err := tetralauth.BuildApplication(ctx, cfg, nil, tetralauth.WithLogger(logger), tetralauth.WithRequestMetrics(httpMetrics))
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	defer workload.ProcessCleanup(ctx, func() { _ = app.Close() })
	readiness := workload.NewReadiness()
	handler := buildHTTPHandler(readiness, app.Handler)
	metricsHandler := workload.HealthRouter(readiness,
		workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger)),
		workload.WithHTTPMetrics(httpMetrics),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", app.Client)),
		workload.WithMetricsCollector("auth_token_pruning", app.PruningMetrics),
	)
	httpCredentials, httpTLS, err := cfg.HTTPTransport.Open(ctx)
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	if httpCredentials != nil {
		defer workload.ProcessCleanup(ctx, func() { _ = httpCredentials.Close() })
	}
	grpcServer, err := tetralauth.OpenExternalAuthorizationServer(ctx, cfg, app.ExternalAuthorization, logger, httpMetrics.Operations)
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	defer func() { _ = grpcServer.Close() }()
	publicListener, err := net.Listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	defer func() { _ = publicListener.Close() }()
	metricsListener, err := net.Listen("tcp", cfg.MetricsAddress)
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	defer func() { _ = metricsListener.Close() }()
	readiness.MarkReady()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- grpcServer.Run(runCtx, readiness) }()
	go func() {
		results <- runPublicAndMetricsHTTP(runCtx, cfg, readiness, logger, handler, metricsHandler, boundHTTPListeners{public: publicListener, metrics: metricsListener, tls: httpTLS, operationMetrics: httpMetrics.Operations})
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

type boundHTTPListeners struct {
	public, metrics  net.Listener
	operationMetrics *workload.OperationMetrics
	tls              *tls.Config
}

func runPublicAndMetricsHTTP(
	ctx context.Context,
	cfg tetralauth.Config,
	readiness *workload.Readiness,
	logger *slog.Logger,
	publicHandler http.Handler,
	metricsHandler http.Handler,
	listeners boundHTTPListeners,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		publicConfig := tetralauth.WorkloadConfig(cfg, publicHandler, readiness, logger)
		publicConfig.Listener = listeners.public
		publicConfig.TLSConfig = listeners.tls
		publicConfig.Metrics = listeners.operationMetrics
		results <- runWorkload(runCtx, publicConfig)
	}()
	go func() {
		metricsConfig := workload.Config{
			ServiceName:           "auth",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.MetricsAddress,
			ListenConfigKey:       tetralauth.EnvMetricsAddress,
			Handler:               metricsHandler,
			Readiness:             readiness,
			ShutdownTimeout:       tetralauth.DefaultShutdownTimeout,
			Logger:                logger,
			Listener:              listeners.metrics,
			Metrics:               listeners.operationMetrics,
		}
		results <- runWorkload(runCtx, metricsConfig)
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

func buildHTTPHandler(readiness *workload.Readiness, router http.Handler) http.Handler {
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
		router.ServeHTTP(w, r)
	})
}
