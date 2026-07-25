package gitproxy

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workload"
)

type ListenFunc func(network string, address string) (net.Listener, error)
type RunHTTPFunc func(context.Context, workload.Config) error

type RuntimeConfig struct {
	Listen          ListenFunc
	RunHTTP         RunHTTPFunc
	Logger          *slog.Logger
	DBStatsProvider workload.DBStatsProvider
}

func Run(ctx context.Context, cfg Config, client *dbconnect.Client, encryptor vault.CredentialEncryptor, runtime RuntimeConfig) error {
	if client == nil {
		return workload.NewConfigError("runtime database client is required")
	}
	if encryptor == nil {
		return workload.NewConfigError("credential decryptor is required")
	}
	cfg.HTTPAddress = valueOrDefault(cfg.HTTPAddress, DefaultHTTPAddress)
	cfg.MetricsAddress = valueOrDefault(cfg.MetricsAddress, DefaultMetricsAddress)
	if cfg.HTTPAddress == cfg.MetricsAddress {
		return workload.NewConfigError(EnvMetricsAddress + " must not equal " + EnvHTTPAddress)
	}
	readiness := workload.NewReadiness()
	metrics := NewGitProxyMetrics()
	proxyHandler := BuildHTTPHandler(readiness, NewHTTPHandler(
		TicketValidator{
			Store:         gitticket.NewPostgreSQLStore(client),
			RotationGrace: cfg.DrainGrace,
		},
		NewRepositoryPolicyAuthorizer(
			NewPostgreSQLRepositoryTokenResolver(client, encryptor),
		),
		HandlerOptions{
			PublicBaseURL:     cfg.PublicBaseURL,
			LegacyPathCutover: cfg.LegacyPathCutover,
			AccessLogger:      NewJSONAccessLogger(os.Stderr, WithAccessLogResource(cfg.DeploymentEnvironment, cfg.ServiceVersion)),
			Metrics:           metrics,
		},
	))
	metricsHandler := BuildMetricsHTTPHandler(readiness, metrics, runtime.DBStatsProvider)
	readiness.MarkReady()
	runHTTP := runtime.RunHTTP
	if runHTTP == nil {
		runHTTP = workload.Run
	}
	logger := runtime.Logger
	if logger == nil {
		logger = workload.NewLogger(nil, ServiceName, cfg.DeploymentEnvironment, cfg.ServiceVersion)
	}
	return runHTTPPair(ctx, runHTTP, runtime.Listen, logger, readiness, cfg, proxyHandler, metricsHandler)
}

func runHTTPPair(
	ctx context.Context,
	runHTTP RunHTTPFunc,
	listen ListenFunc,
	logger *slog.Logger,
	readiness *workload.Readiness,
	cfg Config,
	proxyHandler http.Handler,
	metricsHandler http.Handler,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- runHTTP(runCtx, workload.Config{
			ServiceName:           ServiceName,
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.HTTPAddress,
			ListenConfigKey:       EnvHTTPAddress,
			Listen:                listen,
			Handler:               proxyHandler,
			Readiness:             readiness,
			ReadHeaderTimeout:     HeaderReadTimeout,
			ShutdownTimeout:       cfg.DrainGrace,
			Logger:                logger,
		})
	}()
	go func() {
		results <- runHTTP(runCtx, workload.Config{
			ServiceName:           ServiceName,
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.MetricsAddress,
			ListenConfigKey:       EnvMetricsAddress,
			Listen:                listen,
			Handler:               metricsHandler,
			Readiness:             readiness,
			ReadHeaderTimeout:     HeaderReadTimeout,
			ShutdownTimeout:       cfg.DrainGrace,
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

func BuildHTTPHandler(readiness *workload.Readiness, proxy http.Handler) http.Handler {
	health := workload.HealthRouter(readiness)
	notFound := http.HandlerFunc(http.NotFound)
	mux := http.NewServeMux()
	mux.Handle("/health", health)
	mux.Handle("/ready", health)
	mux.Handle("/metrics", notFound)
	mux.Handle("/metrics/", notFound)
	mux.Handle("/", proxy)
	return mux
}

func BuildMetricsHTTPHandler(readiness *workload.Readiness, metrics *GitProxyMetrics, dbStatsProvider workload.DBStatsProvider) http.Handler {
	if metrics == nil {
		metrics = NewGitProxyMetrics()
	}
	health := workload.HealthRouter(readiness)
	mux := http.NewServeMux()
	mux.Handle("/health", health)
	mux.Handle("/ready", health)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		extra := []workload.Metric{}
		if dbStatsProvider != nil {
			collected, err := workload.DBStatsMetrics("runtime", dbStatsProvider)(r.Context())
			if err != nil {
				extra = append(extra, workload.Metric{
					Name:   "tetral_metrics_collector_errors_total",
					Help:   "Metrics collector failures.",
					Type:   "counter",
					Labels: []workload.MetricLabel{{Name: "collector", Value: "database"}},
					Value:  1,
				})
			} else {
				extra = append(extra, collected...)
			}
		}
		_, _ = w.Write([]byte(workload.RuntimeMetricsTextWith(extra)))
		_, _ = w.Write([]byte(metrics.render()))
	})
	return mux
}
