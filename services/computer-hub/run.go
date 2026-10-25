package computerhub

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workload"
)

type ListenFunc func(network, address string) (net.Listener, error)
type RunHTTPFunc func(context.Context, workload.Config) error

// Runtime holds the dependencies the command opens before Run: the verified
// database client and the process logger.
type Runtime struct {
	Database *dbconnect.Client
	Logger   *slog.Logger
	Listen   ListenFunc
	RunHTTP  RunHTTPFunc
}

// Run serves the public listener and the metrics listener until ctx ends or
// one of them fails, then drains both.
func Run(ctx context.Context, cfg Config, runtime Runtime) error {
	if runtime.Database == nil {
		return workload.NewConfigError("runtime database client is required")
	}
	logger := runtime.Logger
	if logger == nil {
		logger = workload.ComponentLogger(ServiceName)
	}
	runHTTP := runtime.RunHTTP
	if runHTTP == nil {
		runHTTP = workload.Run
	}
	readiness := workload.NewReadiness()
	httpMetrics := workload.NewHTTPMetrics(ServiceName)
	events := NewInstanceEvents(runtime.Database, logger)
	instances := InstanceRoutes{Database: runtime.Database, MaxCredentialLifetime: cfg.MaxCredentialLifetime, PageTokenKey: cfg.PageTokenKey, Events: events}
	public := BuildHTTPHandler(readiness, NewRouter(cfg.PrincipalVerifier, WithLogger(logger), WithRequestMetrics(httpMetrics), WithInstanceRoutes(instances)))
	metrics := workload.HealthRouter(readiness,
		workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger)),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", runtime.Database)),
	)
	tlsOwner, tlsConfig, err := cfg.HTTPTransport.Open(ctx)
	if err != nil {
		return workload.NewConfigError("native HTTP credential preparation failed")
	}
	if tlsOwner != nil {
		defer func() { _ = tlsOwner.Close() }()
	}
	readiness.MarkReady()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	eventsDone := make(chan struct{})
	go func() { _ = events.Run(runCtx); close(eventsDone) }()
	defer func() { <-eventsDone }()
	listener := func(address, key string, handler http.Handler, tls bool) workload.Config {
		config := workload.Config{
			Metrics:               httpMetrics.Operations,
			ServiceName:           ServiceName,
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         address,
			ListenConfigKey:       key,
			Listen:                runtime.Listen,
			Handler:               handler,
			Readiness:             readiness,
			ShutdownTimeout:       ShutdownTimeout,
			Logger:                logger,
		}
		if tls {
			config.TLSConfig = tlsConfig
		}
		return config
	}
	results := make(chan error, 2)
	go func() { results <- runHTTP(runCtx, listener(cfg.HTTPAddress, EnvHTTPAddress, public, true)) }()
	go func() { results <- runHTTP(runCtx, listener(cfg.MetricsAddress, EnvMetricsAddress, metrics, false)) }()
	var firstErr error
	for range 2 {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
		cancel()
	}
	return firstErr
}

// BuildHTTPHandler serves health and readiness on the public listener too and
// keeps the metrics paths off it.
func BuildHTTPHandler(readiness *workload.Readiness, router http.Handler) http.Handler {
	health := workload.HealthRouter(readiness)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health", "/ready":
			health.ServeHTTP(w, r)
		case "/metrics", "/metrics/":
			http.NotFound(w, r)
		default:
			router.ServeHTTP(w, r)
		}
	})
}
