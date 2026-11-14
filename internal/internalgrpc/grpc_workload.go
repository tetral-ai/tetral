package internalgrpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/workload"

	"google.golang.org/grpc"
)

// RunGRPCWorkload owns the dual-listener orchestration shared by internal gRPC
// workloads: a Kubernetes-probed HTTP listener for /health and /ready beside an
// authenticated internal gRPC listener. Workload-specific service behavior stays
// behind the Register callback supplied by each command.

// GRPCWorkloadParams is the thin parameter block each gRPC command fills in.
// The fields are the only things that differ between the two workloads (service
// name, listen env keys/defaults, and the service Register callback) plus the
// test seams the command layer stubs.
type GRPCWorkloadParams struct {
	ServiceName         string
	HTTPListenEnvKey    string
	HTTPListenDefault   string
	GRPCListenEnvKey    string
	GRPCListenDefault   string
	Register            func(*grpc.Server)
	MethodAuthorizer    MethodAuthorizer
	ReadinessDependency func() bool
	ShutdownTimeout     time.Duration
	CancelJoinTimeout   time.Duration
	DBStatsProvider     workload.DBStatsProvider
	ServerOptions       []grpc.ServerOption
	Logger              *slog.Logger

	// Seams. Production wiring fills these from the real implementations; command
	// tests stub them to drive startup/shutdown without a real cluster. Any nil
	// field falls back to the real implementation.
	RunWorkload          func(context.Context, workload.Config) error
	RunInternalGRPC      func(context.Context, Config) error
	NewAuthenticator     func(grpcauth.Config) (Authenticator, error)
	NewTokenReviewClient func() (grpcauth.TokenReviewClient, error)
	Listen               func(network string, address string) (net.Listener, error)
}

// EnvReader reads command configuration from the environment.
type EnvReader interface {
	Getenv(string) string
}

// RunGRPCWorkload validates internal-auth/listen config, opens the separate gRPC
// and HTTP probe listeners, starts the authenticated internal gRPC server, and
// runs the HTTP probe workload alongside it with shared graceful shutdown. Invalid
// config and dependency bootstrap failures fail before serving, each with one
// startup-failure record. It returns nil only on a clean drain.
func RunGRPCWorkload(ctx context.Context, env EnvReader, params GRPCWorkloadParams) error {
	runWorkload := params.RunWorkload
	if runWorkload == nil {
		runWorkload = workload.Run
	}
	runInternalGRPC := params.RunInternalGRPC
	if runInternalGRPC == nil {
		runInternalGRPC = Run
	}
	newTokenReviewClient := params.NewTokenReviewClient
	if newTokenReviewClient == nil {
		newTokenReviewClient = func() (grpcauth.TokenReviewClient, error) {
			return grpcauth.NewInClusterTokenReviewClient()
		}
	}
	listen := params.Listen
	if listen == nil {
		listen = net.Listen
	}
	shutdownTimeout := params.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 10 * time.Second
	}
	resource := workload.ResourceConfigFromEnv(env.Getenv)
	deploymentEnvironment, serviceVersion := resource.DeploymentEnvironment, resource.ServiceVersion
	logger := params.Logger
	if logger == nil {
		logger = workload.ComponentLogger(params.ServiceName)
	}
	httpMetrics := workload.NewHTTPMetrics(params.ServiceName)
	grpcMetrics := workload.NewGRPCMetrics(params.ServiceName)

	authConfig, err := grpcauth.LoadConfig(env)
	if err != nil {
		return logGRPCStartupFailure(params.ServiceName, logger, workload.StartupFailureCauseConfiguration, err)
	}
	var authenticator Authenticator
	if params.NewAuthenticator != nil {
		authenticator, err = params.NewAuthenticator(authConfig)
		if err != nil {
			return logGRPCStartupFailure(params.ServiceName, logger, workload.StartupFailureCauseDependencyReadiness, err)
		}
	} else {
		client, err := newTokenReviewClient()
		if err != nil {
			return logGRPCStartupFailure(params.ServiceName, logger, workload.StartupFailureCauseDependencyReadiness, err)
		}
		authenticator = grpcauth.NewTokenReviewAuthenticator(client, authConfig)
	}
	httpAddress := valueOrDefault(env.Getenv(params.HTTPListenEnvKey), params.HTTPListenDefault)
	grpcAddress := valueOrDefault(env.Getenv(params.GRPCListenEnvKey), params.GRPCListenDefault)
	grpcListener, err := workload.ListenTCP(listen, grpcAddress)
	if err != nil {
		return logGRPCStartupFailure(params.ServiceName, logger, workload.StartupFailureCauseListener, err)
	}
	defer func() { _ = grpcListener.Close() }()
	httpListener, err := workload.ListenTCP(listen, httpAddress)
	if err != nil {
		return logGRPCStartupFailure(params.ServiceName, logger, workload.StartupFailureCauseListener, err)
	}
	defer func() { _ = httpListener.Close() }()

	readiness := workload.NewReadiness()
	if params.ReadinessDependency != nil {
		readiness = readiness.WithReadinessDependency(params.ReadinessDependency)
	}
	serverCtx, cancelServers := context.WithCancel(ctx)
	defer cancelServers()
	grpcCtx, cancelGRPC := context.WithCancel(serverCtx)
	defer cancelGRPC()
	grpcServing := make(chan struct{})
	var markGRPCServing sync.Once
	grpcErr := make(chan error, 1)
	go func() {
		runErr := runInternalGRPC(grpcCtx, Config{
			ServiceName:           params.ServiceName,
			DeploymentEnvironment: deploymentEnvironment,
			ServiceVersion:        serviceVersion,
			Listener:              grpcListener,
			ListenConfigKey:       params.GRPCListenEnvKey,
			Authenticator:         authenticator,
			MethodAuthorizer:      params.MethodAuthorizer,
			Register:              params.Register,
			OnServing: func() {
				markGRPCServing.Do(func() { close(grpcServing) })
			},
			ShutdownTimeout:   shutdownTimeout,
			CancelJoinTimeout: params.CancelJoinTimeout,
			Logger:            logger,
			Metrics:           grpcMetrics,
			ServerOptions:     params.ServerOptions,
		})
		if grpcCtx.Err() == nil {
			if runErr == nil {
				runErr = fmt.Errorf("internal grpc stopped unexpectedly")
			}
			readiness.BeginShutdown()
			cancelServers()
		}
		grpcErr <- runErr
	}()
	select {
	case <-grpcServing:
		readiness.MarkReady()
	case err = <-grpcErr:
		workload.BeginProcessShutdown(ctx)
		cancelGRPC()
		if err == nil {
			return fmt.Errorf("internal grpc stopped before serving")
		}
		return err
	case <-serverCtx.Done():
		readiness.BeginShutdown()
		workload.BeginProcessShutdown(ctx)
		cancelGRPC()
		// The listener owner must finish before callers close its dependencies,
		// even when cancellation arrives before the serving callback.
		if runErr := <-grpcErr; runErr != nil {
			return runErr
		}
		return serverCtx.Err()
	}
	metricsOptions := []workload.HealthRouterOption{
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("grpc", grpcMetrics.Collector()),
		workload.WithMetricsCollector("diagnostics", workload.DiagnosticMetrics(logger)),
	}
	if params.DBStatsProvider != nil {
		metricsOptions = append(metricsOptions, workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", params.DBStatsProvider)))
	}
	err = runWorkload(serverCtx, workload.Config{
		ServiceName:           params.ServiceName,
		DeploymentEnvironment: deploymentEnvironment,
		ServiceVersion:        serviceVersion,
		ListenAddress:         httpAddress,
		ListenConfigKey:       params.HTTPListenEnvKey,
		Listener:              httpListener,
		Metrics:               httpMetrics.Operations,
		Handler:               workload.HealthRouter(readiness, metricsOptions...),
		Readiness:             readiness,
		ShutdownTimeout:       shutdownTimeout,
		Logger:                logger,
	})
	workload.BeginProcessShutdown(ctx)
	readiness.BeginShutdown()
	cancelGRPC()
	// Run owns the drain deadline and forced cancellation. An outer timeout
	// cannot safely release pools while its handlers are still joining.
	if grpcRunErr := <-grpcErr; err == nil {
		err = grpcRunErr
	}
	return err
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// logGRPCStartupFailure writes the startup-failure record of an internal gRPC
// workload, as every workload does (workload.LogStartupFailure): a
// workload.ConfigError (bad audience, malformed allowlist) is config_error,
// any other failure (TokenReview client construction, listener bind) is
// startup_error, and both carry the error as the record's cause.
func logGRPCStartupFailure(serviceName string, logger *slog.Logger, cause workload.StartupFailureCause, err error) error {
	return workload.LogStartupFailure(logger, serviceName, workload.WithStartupFailureCause(cause, err))
}
