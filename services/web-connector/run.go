package webconnector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/internalgrpc"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

type RuntimeConfig struct {
	Authenticator internalgrpc.Authenticator
	Listen        func(string, string) (net.Listener, error)
	Logger        *slog.Logger
}

func Register(server grpc.ServiceRegistrar, service *Service) {
	providergatewayv1.RegisterProviderGatewayServiceServer(server, service)
}

func Run(ctx context.Context, cfg Config, service *Service, metrics *Metrics, runtime RuntimeConfig) error {
	if service == nil {
		return fmt.Errorf("web service is required")
	}
	if runtime.Authenticator == nil {
		return fmt.Errorf("authenticator is required")
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	listen := runtime.Listen
	if listen == nil {
		listen = net.Listen
	}
	grpcListener, err := listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return err
	}
	defer func() { _ = grpcListener.Close() }()
	metricsListener, err := listen("tcp", cfg.MetricsAddress)
	if err != nil {
		return err
	}
	defer func() { _ = metricsListener.Close() }()
	if runtime.Logger != nil {
		runtime.Logger.Info("workload.started",
			slog.String("operation", "workload.lifecycle"),
			slog.String("event.kind", "started"),
			slog.String("component", "workload"),
			slog.String("listener.transport", "tcp"),
			slog.String("readiness.state", "not ready"),
		)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	serverCtx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var ready atomic.Bool
	var draining atomic.Bool
	var admission sync.Mutex
	var active atomic.Int64
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(serverCtx))
	defer cancelWork()
	server, healthServer, err := internalgrpc.NewServerWithHealth(internalgrpc.Config{ServiceName: ServiceName, Listener: grpcListener, Authenticator: runtime.Authenticator, MethodAuthorizer: MethodAuthorizer, Register: func(server *grpc.Server) { Register(server, service) }, Logger: runtime.Logger, ServerOptions: []grpc.ServerOption{
		grpc.WaitForHandlers(true), grpc.MaxRecvMsgSize(maxRunWebRequestGRPCMessageBytes), grpc.MaxSendMsgSize(maxRunWebResponseGRPCMessageBytes), grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 5 * time.Minute, MaxConnectionAgeGrace: 30 * time.Minute}),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			admission.Lock()
			if draining.Load() {
				admission.Unlock()
				return nil, status.Error(codes.Unavailable, "web connector draining")
			}
			active.Add(1)
			admission.Unlock()
			defer active.Add(-1)
			requestCtx, cancelRequest := context.WithCancel(ctx)
			stop := context.AfterFunc(workCtx, cancelRequest)
			defer stop()
			defer cancelRequest()
			return handler(requestCtx, request)
		}),
	}})
	if err != nil {
		return err
	}
	grpcErr := make(chan error, 1)
	go func() {
		admission.Lock()
		if !draining.Load() {
			ready.Store(true)
		}
		admission.Unlock()
		grpcErr <- server.Serve(grpcListener)
		close(grpcErr)
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.Handler().ServeHTTP(w, r)
		_, _ = fmt.Fprintf(w, "# TYPE web_requests_active gauge\nweb_requests_active %d\n# TYPE web_draining gauge\nweb_draining %d\n", active.Load(), boolInt(draining.Load()))
		_, _ = w.Write([]byte(workload.DiagnosticMetricsText(runtime.Logger)))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: DefaultOpsReadHeaderTimeout}
	httpErr := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(metricsListener)
		if serveErr == http.ErrServerClosed {
			serveErr = nil
		}
		httpErr <- serveErr
	}()

	var runErr error
	grpcConsumed, httpConsumed := false, false
	select {
	case runErr = <-grpcErr:
		grpcConsumed = true
	case runErr = <-httpErr:
		httpConsumed = true
	case <-serverCtx.Done():
	}
	workload.BeginProcessShutdown(ctx)
	admission.Lock()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
	draining.Store(true)
	ready.Store(false)
	admission.Unlock()
	if runtime.Logger != nil {
		runtime.Logger.Info("web.drain.started", slog.String("operation", "workload.shutdown"), slog.String("shutdown.phase", "draining"), slog.Int64("closeout.active_count", active.Load()))
	}
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	drain := cfg.DrainTimeout
	if drain <= 0 {
		drain = DefaultListenerShutdownTimeout
	}
	timer := time.NewTimer(drain)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		cancelWork()
		server.Stop()
		<-stopped
	}
	if !grpcConsumed {
		err := <-grpcErr
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			runErr = errors.Join(runErr, err)
		}
	}
	joinTimeout := cfg.CancelJoinTimeout
	if joinTimeout <= 0 {
		joinTimeout = 5 * time.Second
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), joinTimeout)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		_ = httpServer.Close()
		runErr = errors.Join(runErr, err)
	}
	if !httpConsumed {
		runErr = errors.Join(runErr, <-httpErr)
	}
	if runtime.Logger != nil {
		runtime.Logger.Info("web.drain.joined", slog.String("operation", "workload.shutdown"), slog.String("shutdown.phase", "joined"), slog.Int64("closeout.active_count", active.Load()))
	}
	return runErr
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
