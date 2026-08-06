package webconnector

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

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
	serverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ready atomic.Bool
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- internalgrpc.Run(serverCtx, internalgrpc.Config{ServiceName: ServiceName, Listener: grpcListener, Authenticator: runtime.Authenticator, MethodAuthorizer: MethodAuthorizer, Register: func(server *grpc.Server) { Register(server, service) }, OnServing: func() { ready.Store(true) }, ShutdownTimeout: 10 * time.Second, Logger: runtime.Logger, ServerOptions: []grpc.ServerOption{grpc.MaxRecvMsgSize(maxRunWebRequestGRPCMessageBytes), grpc.MaxSendMsgSize(maxRunWebResponseGRPCMessageBytes), grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 5 * time.Minute, MaxConnectionAgeGrace: 30 * time.Minute})}})
	}()
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
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
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	httpErr := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(metricsListener)
		if serveErr == http.ErrServerClosed {
			serveErr = nil
		}
		httpErr <- serveErr
	}()
	var runErr error
	select {
	case runErr = <-grpcErr:
	case runErr = <-httpErr:
	case <-serverCtx.Done():
		runErr = nil
	}
	ready.Store(false)
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	return runErr
}
