package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	tetralauth "github.com/tetral-ai/tetral/services/auth"

	"github.com/tetral-ai/tetral/internal/workload"
)

var runWorkload = workload.Run

const defaultShutdownTimeout = 10 * time.Second

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

func main() {
	if err := run(context.Background(), osEnv{}); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, env tetralauth.Env) error {
	logger := workload.NewLogger(os.Stderr, "auth", env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), env.Getenv("TETRAL_SERVICE_VERSION"))
	cfg, err := tetralauth.ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	httpMetrics := workload.NewHTTPMetrics()
	app, err := tetralauth.BuildApplication(ctx, cfg, nil, tetralauth.WithLogger(logger), tetralauth.WithRequestMetrics(httpMetrics))
	if err != nil {
		return workload.LogStartupFailure(logger, "auth", err)
	}
	defer func() { _ = app.Close() }()
	readiness := workload.NewReadiness()
	handler := buildHTTPHandler(readiness, app.Handler)
	metricsHandler := workload.HealthRouter(readiness,
		workload.WithHTTPMetrics(httpMetrics),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", app.Client)),
	)
	readiness.MarkReady()
	return runPublicAndMetricsHTTP(ctx, cfg, readiness, logger, handler, metricsHandler)
}

func runPublicAndMetricsHTTP(
	ctx context.Context,
	cfg tetralauth.Config,
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
		results <- runWorkload(runCtx, tetralauth.WorkloadConfig(cfg, publicHandler, readiness, logger))
	}()
	go func() {
		results <- runWorkload(runCtx, workload.Config{
			ServiceName:           "auth",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.MetricsAddress,
			ListenConfigKey:       tetralauth.EnvMetricsAddress,
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
