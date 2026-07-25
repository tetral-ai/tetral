package tetralqueue

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type ListenFunc func(network string, address string) (net.Listener, error)
type RunHTTPFunc func(context.Context, workload.Config) error

type RuntimeConfig struct {
	Listen          ListenFunc
	RunHTTP         RunHTTPFunc
	Logger          *slog.Logger
	DBStatsProvider workload.DBStatsProvider
}

func Run(ctx context.Context, cfg Config, store Store, runtime RuntimeConfig) error {
	if store == nil {
		return fmt.Errorf("queue store is required")
	}
	listen := runtime.Listen
	if listen == nil {
		listen = net.Listen
	}
	runHTTP := runtime.RunHTTP
	if runHTTP == nil {
		runHTTP = workload.Run
	}
	logger := runtime.Logger
	if logger == nil {
		logger = workload.NewLogger(nil, "queue", cfg.DeploymentEnvironment, cfg.ServiceVersion)
	}
	grpcListener, err := listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return err
	}
	defer func() { _ = grpcListener.Close() }()
	httpListener, err := listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return err
	}
	defer func() { _ = httpListener.Close() }()

	if ctx == nil {
		ctx = context.Background()
	}
	serverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	readiness := workload.NewReadiness()
	httpMetrics := workload.NewHTTPMetrics()
	grpcMetrics := workload.NewGRPCMetrics()
	grpcOptions := append(internalgrpc.QueueRPCServerOptions(),
		grpc.ChainUnaryInterceptor(internalgrpc.MetricsUnaryInterceptor(grpcMetrics)),
		grpc.ChainStreamInterceptor(internalgrpc.MetricsStreamInterceptor(grpcMetrics)),
	)
	grpcServer := grpc.NewServer(grpcOptions...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	Register(grpcServer, store)

	grpcErr := make(chan error, 1)
	go func() {
		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
		readiness.MarkReady()
		if err := grpcServer.Serve(grpcListener); err != nil {
			grpcErr <- err
		}
		close(grpcErr)
	}()

	httpErr := make(chan error, 1)
	go func() {
		metricsOptions := []workload.HealthRouterOption{}
		if runtime.DBStatsProvider != nil {
			metricsOptions = append(metricsOptions, workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", runtime.DBStatsProvider)))
		}
		metricsOptions = append(metricsOptions,
			workload.WithHTTPMetrics(httpMetrics),
			workload.WithMetricsCollector("http", httpMetrics.Collector()),
			workload.WithMetricsCollector("grpc", grpcMetrics.Collector()),
		)
		if metricsStore, ok := store.(interface {
			Metrics(context.Context, time.Time) ([]queue.MetricsSnapshot, error)
		}); ok {
			metricsOptions = append(metricsOptions, workload.WithMetricsCollector("queue", queueMetricsCollector(metricsStore, time.Now)))
		}
		httpErr <- runHTTP(serverCtx, workload.Config{
			ServiceName:           "queue",
			DeploymentEnvironment: cfg.DeploymentEnvironment,
			ServiceVersion:        cfg.ServiceVersion,
			ListenAddress:         cfg.HTTPAddress,
			ListenConfigKey:       EnvHTTPAddress,
			Listener:              httpListener,
			Handler:               workload.HealthRouter(readiness, metricsOptions...),
			Readiness:             readiness,
			Logger:                logger,
		})
	}()

	var errOut error
	select {
	case errOut = <-httpErr:
	case errOut = <-grpcErr:
	case <-serverCtx.Done():
		errOut = serverCtx.Err()
	}
	readiness.BeginShutdown()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
	cancel()
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		grpcServer.Stop()
	}
	select {
	case httpRunErr := <-httpErr:
		if errOut == nil {
			errOut = httpRunErr
		}
	default:
	}
	return errOut
}

type queueMetricsStore interface {
	Metrics(context.Context, time.Time) ([]queue.MetricsSnapshot, error)
}

func queueMetricsCollector(store queueMetricsStore, now func() time.Time) workload.MetricsCollector {
	return func(ctx context.Context) ([]workload.Metric, error) {
		if now == nil {
			now = time.Now
		}
		snapshots, err := store.Metrics(ctx, now().UTC())
		if err != nil {
			return nil, err
		}
		metrics := make([]workload.Metric, 0, len(snapshots)*5)
		for _, snapshot := range snapshots {
			labels := []workload.MetricLabel{{Name: "kind", Value: snapshot.Kind}}
			metrics = append(metrics,
				workload.Metric{Name: "queue_pending_jobs", Help: "Pending queue jobs.", Type: "gauge", Labels: labels, Value: float64(snapshot.PendingJobs)},
				workload.Metric{Name: "queue_leased_jobs", Help: "Leased queue jobs.", Type: "gauge", Labels: labels, Value: float64(snapshot.LeasedJobs)},
				workload.Metric{Name: "queue_retry_pending_jobs", Help: "Pending queue jobs that have retried at least once.", Type: "gauge", Labels: labels, Value: float64(snapshot.RetryPendingJobs)},
				workload.Metric{Name: "queue_dead_lettered_jobs", Help: "Dead-lettered queue jobs.", Type: "gauge", Labels: labels, Value: float64(snapshot.DeadLetteredJobs)},
				workload.Metric{Name: "queue_ready_lag_seconds", Help: "Maximum ready pending queue lag in seconds.", Type: "gauge", Labels: labels, Value: snapshot.ReadyLagSeconds},
			)
		}
		return metrics, nil
	}
}
