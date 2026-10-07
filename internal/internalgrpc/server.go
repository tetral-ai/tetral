package internalgrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	// CancelJoinTimeout reports a forced-cancellation overrun. Run still joins
	// handlers before returning so callers can safely close their dependencies.
	CancelJoinTimeout time.Duration
	Logger            *slog.Logger
	Metrics           *workload.GRPCMetrics
	ServerOptions     []grpc.ServerOption
}

// ErrCancelJoinTimeout reports a forced drain that joined after its budget.
var ErrCancelJoinTimeout = errors.New("internal grpc cancellation join exceeded its budget")

func Run(ctx context.Context, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = workload.ComponentLogger(cfg.ServiceName)
	}
	server, healthServer, err := buildServer(cfg)
	if err != nil {
		return err
	}
	listener, err := listenerFor(cfg)
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
		workload.BeginProcessShutdown(ctx)
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
		var joinErr error
		select {
		case <-stopped:
		case <-timer.C:
			joinErr = forceStopAndJoin(server, cfg)
		}
		<-stopped
		<-done
		return joinErr
	case err := <-done:
		workload.BeginProcessShutdown(ctx)
		return errors.Join(err, forceStopAndJoin(server, cfg))
	}
}

// Stop cancels transports and, with WaitForHandlers, waits for every handler.
// Budget exhaustion is observable while ownership stays with Run until join.
func forceStopAndJoin(server *grpc.Server, cfg Config) error {
	stopped := make(chan struct{})
	go func() { server.Stop(); close(stopped) }()
	timeout := cfg.CancelJoinTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	started := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-stopped:
		return nil
	case <-timer.C:
		cfg.Logger.Error("internal.grpc.cancellation_join_timeout",
			"operation", "internal.grpc.shutdown", "event.kind", "shutdown", "component", "internal-grpc",
			"error.class", "shutdown_error", "error.code", "cancellation_join_timeout",
			"duration.ms", time.Since(started).Milliseconds())
		<-stopped
		return ErrCancelJoinTimeout
	}
}

// NewServer builds the authenticated server without binding; the caller serves
// its own listener. Run is the composition that binds and serves.
func NewServer(cfg Config) (*grpc.Server, error) {
	server, _, err := NewServerWithHealth(cfg)
	return server, err
}

// NewServerWithHealth exposes the readiness owner to services that coordinate
// admission and resource joins themselves. Health changes before graceful drain.
// It never binds; the caller serves its own listener.
func NewServerWithHealth(cfg Config) (*grpc.Server, *health.Server, error) {
	return buildServer(cfg)
}

func buildServer(cfg Config) (*grpc.Server, *health.Server, error) {
	if cfg.ServiceName == "" {
		return nil, nil, fmt.Errorf("service name is required")
	}
	if cfg.Authenticator == nil {
		return nil, nil, fmt.Errorf("authenticator is required")
	}
	if cfg.Register == nil {
		return nil, nil, fmt.Errorf("registration callback is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = workload.ComponentLogger(cfg.ServiceName)
	}
	options := append(SessionRPCServerOptions(),
		grpc.WaitForHandlers(true),
		grpc.ChainUnaryInterceptor(recoveryUnaryInterceptor(cfg.Logger, cfg.Metrics), authUnaryInterceptor(cfg)),
		grpc.ChainStreamInterceptor(recoveryStreamInterceptor(cfg.Logger, cfg.Metrics), authStreamInterceptor(cfg)),
	)
	options = append(options, cfg.ServerOptions...)
	server := grpc.NewServer(options...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(server, healthServer)
	cfg.Register(server)
	return server, healthServer, nil
}

// listenerFor binds only for Run, which serves and closes what it binds.
func listenerFor(cfg Config) (net.Listener, error) {
	if cfg.Listener != nil {
		return cfg.Listener, nil
	}
	listen := cfg.Listen
	if listen == nil {
		listen = net.Listen
	}
	address := cfg.ListenAddress
	if address == "" {
		address = ":9090"
	}
	return listen("tcp", address)
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
		level := slog.LevelInfo
		switch code {
		case codes.OK, codes.Canceled:
			level = slog.LevelDebug
		case codes.Internal, codes.DataLoss:
			level = slog.LevelError
		case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
			level = slog.LevelWarn
		}
		logger.LogAttrs(context.Background(), level, "internal.grpc.request", attrs...)
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
