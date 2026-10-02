package tetralqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
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
	Listen           ListenFunc
	RunHTTP          RunHTTPFunc
	Logger           *slog.Logger
	DBStatsProvider  workload.DBStatsProvider
	MaintenanceStore MaintenanceStore
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
		owner := workload.NewProcessLogger(nil, "queue", cfg.DeploymentEnvironment, cfg.ServiceVersion, workload.DefaultDiagnosticConfig())
		defer owner.CloseWithBudget()
		logger = owner.Logger
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
	serverCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	readiness := workload.NewReadiness()
	httpMetrics := workload.NewHTTPMetrics()
	grpcMetrics := workload.NewGRPCMetrics()
	grpcOptions := append(internalgrpc.QueueRPCServerOptions(),
		grpc.WaitForHandlers(true),
		grpc.ChainUnaryInterceptor(internalgrpc.MetricsUnaryInterceptor(grpcMetrics)),
		grpc.ChainStreamInterceptor(internalgrpc.MetricsStreamInterceptor(grpcMetrics)),
	)
	grpcServer := grpc.NewServer(grpcOptions...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	Register(grpcServer, store, logger)

	drainTimeout := cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 10 * time.Second
	}
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(serverCtx))
	defer cancelWork()
	maintenanceAdmission := &maintenanceAdmission{stop: make(chan struct{}), admissionCtx: serverCtx}
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		runStalledLeaseMaintenance(workCtx, runtime.MaintenanceStore, MaintenanceConfig{
			Interval: cfg.LeaseReclaimInterval, Limit: cfg.LeaseReclaimBatchLimit, Logger: logger,
		}, maintenanceAdmission)
	}()
	httpUsers := &httpRequestOwner{}

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
		defer close(httpErr)
		metricsOptions := []workload.HealthRouterOption{workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger))}
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
			Handler:               httpUsers.handler(workCtx, workload.HealthRouter(readiness, metricsOptions...)),
			ShutdownTimeout:       drainTimeout,
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
	workload.BeginProcessShutdown(ctx)
	readiness.BeginShutdown()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
	maintenanceAdmission.close()
	httpUsers.closeAdmission()
	cancel()
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	joined := make(chan struct{})
	var httpRunErr error
	go func() {
		defer close(joined)
		<-stopped
		<-grpcErr
		httpRunErr = <-httpErr
		<-maintenanceDone
		httpUsers.users.Wait()
	}()
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case <-joined:
	case <-timer.C:
		// All store and metrics operations receive cancellation before force-stop.
		// WaitForHandlers plus the explicit joins keep their database pool alive
		// until every admitted user has returned, including a cancelled SQL call.
		cancelWork()
		grpcServer.Stop()
		<-joined
		errOut = errors.Join(errOut, context.DeadlineExceeded)
	}
	return errors.Join(errOut, httpRunErr)
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

// HTTP metrics can own database work too. Closing admission under the same
// mutex as Add makes the final Wait safe even when a new request races drain.
type httpRequestOwner struct {
	mu       sync.Mutex
	stopping bool
	users    sync.WaitGroup
}

func (o *httpRequestOwner) closeAdmission() { o.mu.Lock(); o.stopping = true; o.mu.Unlock() }
func (o *httpRequestOwner) handler(workCtx context.Context, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		if o.stopping {
			o.mu.Unlock()
			http.Error(w, "queue is draining", http.StatusServiceUnavailable)
			return
		}
		o.users.Add(1)
		o.mu.Unlock()
		defer o.users.Done()
		ctx, cancel := context.WithCancel(r.Context())
		stop := context.AfterFunc(workCtx, cancel)
		defer stop()
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
