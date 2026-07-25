package internalgrpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/workload"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type Authenticator interface {
	Authenticate(context.Context, string) (auth.Identity, error)
}

type MethodAuthorizer func(auth.Identity, string) error

type Config struct {
	ServiceName           string
	DeploymentEnvironment string
	ServiceVersion        string
	ListenAddress         string
	ListenConfigKey       string
	Listener              net.Listener
	Listen                func(network string, address string) (net.Listener, error)
	Authenticator         Authenticator
	MethodAuthorizer      MethodAuthorizer
	Register              func(*grpc.Server)
	OnServing             func()
	ShutdownTimeout       time.Duration
	Logger                *slog.Logger
	Metrics               *workload.GRPCMetrics
	ServerOptions         []grpc.ServerOption
}

func Run(ctx context.Context, cfg Config) error {
	server, listener, healthServer, err := buildServer(cfg)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	go func() {
		if cfg.OnServing != nil {
			cfg.OnServing()
		}
		if err := server.Serve(listener); err != nil {
			done <- err
		}
		close(done)
	}()
	select {
	case <-ctx.Done():
		// Report NOT_SERVING before GracefulStop begins so health watchers and the
		// readiness layer observe the drain honestly: GracefulStop keeps accepting the
		// already-open Watch streams, so a stale SERVING status would otherwise linger
		// for the entire drain window.
		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()
		timeout := cfg.ShutdownTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-stopped:
		case <-timer.C:
			server.Stop()
		}
		return nil
	case err := <-done:
		return err
	}
}

func NewServer(cfg Config) (*grpc.Server, error) {
	server, _, _, err := buildServer(cfg)
	return server, err
}

func buildServer(cfg Config) (*grpc.Server, net.Listener, *health.Server, error) {
	if cfg.ServiceName == "" {
		return nil, nil, nil, fmt.Errorf("service name is required")
	}
	if cfg.Authenticator == nil {
		return nil, nil, nil, fmt.Errorf("authenticator is required")
	}
	if cfg.Register == nil {
		return nil, nil, nil, fmt.Errorf("registration callback is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(os.Stderr, nil)).With(
			slog.String("service.name", cfg.ServiceName),
			slog.String("deployment.environment", deploymentEnvironmentOrDefault(cfg.DeploymentEnvironment)),
			slog.String("service.version", serviceVersionOrDefault(cfg.ServiceVersion)),
		)
	}
	options := append(SessionRPCServerOptions(),
		grpc.ChainUnaryInterceptor(recoveryUnaryInterceptor(cfg.Logger, cfg.Metrics), authUnaryInterceptor(cfg)),
		grpc.ChainStreamInterceptor(recoveryStreamInterceptor(cfg.Logger, cfg.Metrics), authStreamInterceptor(cfg)),
	)
	options = append(options, cfg.ServerOptions...)
	server := grpc.NewServer(options...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(server, healthServer)
	cfg.Register(server)
	listener := cfg.Listener
	if listener == nil {
		listen := cfg.Listen
		if listen == nil {
			listen = net.Listen
		}
		address := cfg.ListenAddress
		if address == "" {
			address = ":9090"
		}
		created, err := listen("tcp", address)
		if err != nil {
			return nil, nil, nil, err
		}
		listener = created
	}
	return server, listener, healthServer, nil
}

func deploymentEnvironmentOrDefault(value string) string {
	if value == "" {
		return "local"
	}
	return value
}

func serviceVersionOrDefault(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func authUnaryInterceptor(cfg Config) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		token, err := auth.TokenFromIncomingContext(ctx)
		if err != nil {
			logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, "", codes.Unauthenticated, time.Since(started))
			return nil, err
		}
		identity, err := cfg.Authenticator.Authenticate(ctx, token)
		if err != nil {
			logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, "", status.Code(err), time.Since(started))
			return nil, err
		}
		if err := authorizeMethod(cfg, identity, info.FullMethod); err != nil {
			logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, identity.ServiceAccount.String(), status.Code(err), time.Since(started))
			return nil, err
		}
		response, err := handler(auth.ContextWithIdentity(ctx, identity), req)
		logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, identity.ServiceAccount.String(), status.Code(err), time.Since(started))
		return response, err
	}
}

func authStreamInterceptor(cfg Config) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		token, err := auth.TokenFromIncomingContext(stream.Context())
		if err != nil {
			logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, "", codes.Unauthenticated, time.Since(started))
			return err
		}
		identity, err := cfg.Authenticator.Authenticate(stream.Context(), token)
		if err != nil {
			logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, "", status.Code(err), time.Since(started))
			return err
		}
		if err := authorizeMethod(cfg, identity, info.FullMethod); err != nil {
			logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, identity.ServiceAccount.String(), status.Code(err), time.Since(started))
			return err
		}
		err = handler(srv, &identityServerStream{
			ServerStream: stream,
			ctx:          auth.ContextWithIdentity(stream.Context(), identity),
		})
		logBoundary(cfg.Logger, cfg.Metrics, info.FullMethod, identity.ServiceAccount.String(), status.Code(err), time.Since(started))
		return err
	}
}

func authorizeMethod(cfg Config, identity auth.Identity, method string) error {
	if cfg.MethodAuthorizer == nil {
		return nil
	}
	return cfg.MethodAuthorizer(identity, method)
}

func MetricsUnaryInterceptor(metrics *workload.GRPCMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, req)
		if metrics != nil {
			metrics.ObserveGRPCRequest(info.FullMethod, status.Code(err).String(), time.Since(started))
		}
		return response, err
	}
}

func MetricsStreamInterceptor(metrics *workload.GRPCMetrics) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		err := handler(srv, stream)
		if metrics != nil {
			metrics.ObserveGRPCRequest(info.FullMethod, status.Code(err).String(), time.Since(started))
		}
		return err
	}
}

func recoveryUnaryInterceptor(logger *slog.Logger, metrics *workload.GRPCMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		started := time.Now()
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = debug.Stack()
				if metrics != nil {
					metrics.ObserveGRPCRequest(info.FullMethod, codes.Internal.String(), time.Since(started))
				}
				if logger != nil {
					elapsed := time.Since(started)
					logger.Error("internal.grpc.recovered",
						slog.String("operation", "internal.grpc.request"),
						slog.String("event.kind", "panic"),
						slog.String("component", "internal-grpc"),
						slog.String("grpc.method", info.FullMethod),
						slog.String("grpc.code", codes.Internal.String()),
						slog.Int64("duration.ms", elapsed.Milliseconds()),
						slog.Bool("retryable", false),
						slog.Bool("terminal", true),
						slog.String("error.class", "panic"),
						slog.String("error.code", "internal_error"),
						slog.String("error.message_safe", "internal gRPC error"),
					)
				}
				response = nil
				err = status.Error(codes.Internal, "internal gRPC error")
			}
		}()
		return handler(ctx, req)
	}
}

func recoveryStreamInterceptor(logger *slog.Logger, metrics *workload.GRPCMetrics) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		started := time.Now()
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = debug.Stack()
				if metrics != nil {
					metrics.ObserveGRPCRequest(info.FullMethod, codes.Internal.String(), time.Since(started))
				}
				if logger != nil {
					elapsed := time.Since(started)
					logger.Error("internal.grpc.recovered",
						slog.String("operation", "internal.grpc.request"),
						slog.String("event.kind", "panic"),
						slog.String("component", "internal-grpc"),
						slog.String("grpc.method", info.FullMethod),
						slog.String("grpc.code", codes.Internal.String()),
						slog.Int64("duration.ms", elapsed.Milliseconds()),
						slog.Bool("retryable", false),
						slog.Bool("terminal", true),
						slog.String("error.class", "panic"),
						slog.String("error.code", "internal_error"),
						slog.String("error.message_safe", "internal gRPC error"),
					)
				}
				err = status.Error(codes.Internal, "internal gRPC error")
			}
		}()
		return handler(srv, stream)
	}
}

type identityServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *identityServerStream) Context() context.Context {
	return s.ctx
}

func logBoundary(logger *slog.Logger, metrics *workload.GRPCMetrics, method string, caller string, code codes.Code, elapsed time.Duration) {
	if metrics != nil {
		metrics.ObserveGRPCRequest(method, code.String(), elapsed)
	}
	if logger != nil {
		attrs := []slog.Attr{
			slog.String("operation", "internal.grpc.request"),
			slog.String("event.kind", "grpc_request"),
			slog.String("component", "internal-grpc"),
			slog.String("grpc.method", method),
			slog.String("grpc.code", code.String()),
			slog.String("caller.service_account", caller),
			slog.Int64("duration.ms", elapsed.Milliseconds()),
		}
		if code != codes.OK {
			retryable := retryableInternalGRPCCode(code)
			attrs = append(attrs,
				slog.Bool("retryable", retryable),
				slog.Bool("terminal", !retryable),
				slog.String("error.class", "grpc_error"),
				slog.String("error.code", strings.ToLower(code.String())),
				slog.String("error.message_safe", "internal gRPC request failed"),
			)
		}
		logger.LogAttrs(context.Background(), slog.LevelInfo, "internal.grpc.request", attrs...)
	}
}

func retryableInternalGRPCCode(code codes.Code) bool {
	switch code {
	case codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Unavailable:
		return true
	default:
		return false
	}
}
