package tetralapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

const defaultShutdownTimeout = 10 * time.Second

type RunWorkloadFunc func(context.Context, workload.Config) error

// Run is the service-local api process bootstrap. cmd/tetral-api is
// intentionally thin: it supplies os env/stderr and exits with the mapped code.
func Run(ctx context.Context, env Env, stderr io.Writer, buildApplication BuildApplicationFunc, runWorkload RunWorkloadFunc) error {
	logger := workload.NewLogger(stderr, "api", env.Getenv(EnvDeploymentEnvironment), env.Getenv(EnvServiceVersion))
	cfg, err := ConfigFromEnv(env)
	if err != nil {
		return workload.LogStartupFailure(logger, "api", err)
	}
	if buildApplication == nil {
		buildApplication = BuildProductionApplication
	}
	httpMetrics := workload.NewHTTPMetrics()
	application, err := buildApplication(ctx, ApplicationConfig(cfg, env, logger, httpMetrics))
	if err != nil {
		return workload.LogStartupFailure(logger, "api", err)
	}
	defer func() { _ = application.Close() }()
	readiness := workload.NewReadiness()
	handler := BuildHTTPHandler(readiness, application.Handler)
	metricsHandler := workload.HealthRouter(readiness,
		workload.WithHTTPMetrics(httpMetrics),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", application.Client)),
	)
	readiness.MarkReady()
	if runWorkload == nil {
		runWorkload = workload.Run
	}
	return runPublicAndMetricsHTTP(ctx, runWorkload, cfg, readiness, logger, handler, metricsHandler)
}

func runPublicAndMetricsHTTP(
	ctx context.Context,
	runWorkload RunWorkloadFunc,
	cfg Config,
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
			ServiceName:           "api",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.ListenAddress,
			ListenConfigKey:       EnvHTTPAddress,
			Handler:               publicHandler,
			Readiness:             readiness,
			ShutdownTimeout:       defaultShutdownTimeout,
			Logger:                logger,
		})
	}()
	go func() {
		results <- runWorkload(runCtx, workload.Config{
			ServiceName:           "api",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.MetricsAddress,
			ListenConfigKey:       EnvMetricsAddress,
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

// BuildHTTPHandler installs health/readiness probes around the public API router.
func BuildHTTPHandler(readiness *workload.Readiness, apiRouter http.Handler) http.Handler {
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
		apiRouter.ServeHTTP(w, r)
	})
}
