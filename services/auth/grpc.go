package tetralauth

import (
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"time"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

// ExternalAuthorizationServer owns a separately bound Check listener and its
// credential observer. Open completes before process readiness is published.
type ExternalAuthorizationServer struct {
	server      *grpc.Server
	listener    net.Listener
	credentials *transportsecurity.Owner
	health      *health.Server
	logger      *slog.Logger
	metrics     *workload.OperationMetrics
}

const externalAuthorizationCheckMethod = "/envoy.service.auth.v3.Authorization/Check"

func OpenExternalAuthorizationServer(ctx context.Context, cfg Config, adapter *ExternalAuthorization, logger *slog.Logger, metrics ...*workload.OperationMetrics) (*ExternalAuthorizationServer, error) {
	if adapter == nil {
		return nil, workload.NewConfigError("external authorization adapter is required")
	}
	if logger == nil {
		logger = workload.ComponentLogger("auth")
	}
	owner := &ExternalAuthorizationServer{logger: logger, health: health.NewServer()}
	if len(metrics) != 0 {
		owner.metrics = metrics[0]
	}
	if owner.metrics == nil {
		owner.metrics = workload.NewOperationMetrics("auth")
	}
	owner.metrics.SetOperations([]string{externalAuthorizationCheckMethod})
	validatePeer := func(ctx context.Context) error {
		if owner.credentials != nil {
			if err := verifyExternalAuthorizationPeer(ctx, owner.credentials, cfg.GRPCTLSEdgeClientURI); err != nil {
				return status.Error(codes.Unavailable, "authentication transport unavailable")
			}
		}
		return nil
	}
	options := []grpc.ServerOption{grpc.MaxRecvMsgSize(128 << 10), grpc.WaitForHandlers(true), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (response any, transportErr error) {
		if info.FullMethod == externalAuthorizationCheckMethod {
			started := time.Now()
			defer func() {
				owner.metrics.Observe(externalAuthorizationCheckMethod, externalAuthorizationOutcome(ctx, response, transportErr), time.Since(started))
			}()
		}
		if err := validatePeer(ctx); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}), grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if err := validatePeer(stream.Context()); err != nil {
			return err
		}
		return next(service, stream)
	})}
	switch cfg.GRPCTransport {
	case "plaintext":
		if cfg.GRPCTLSCAPath != "" || cfg.GRPCTLSCertPath != "" || cfg.GRPCTLSKeyPath != "" || cfg.GRPCTLSEdgeClientURI != "" {
			return nil, workload.NewConfigError("plaintext Check transport cannot carry TLS material")
		}
	case "native-mtls":
		uri, uriErr := url.Parse(cfg.GRPCTLSEdgeClientURI)
		if cfg.GRPCTLSCAPath == "" || cfg.GRPCTLSCertPath == "" || cfg.GRPCTLSKeyPath == "" || uriErr != nil || uri.Scheme != "spiffe" || uri.Host == "" || uri.Path != "/ns/envoy-gateway-system/sa/tetral-public-edge" || uri.User != nil || uri.RawQuery != "" || uri.Fragment != "" {
			return nil, workload.NewConfigError("native Check transport requires complete credentials and edge role")
		}
		var err error
		owner.credentials, err = transportsecurity.Open(ctx, transportsecurity.Config{CAPath: cfg.GRPCTLSCAPath, CertPath: cfg.GRPCTLSCertPath, KeyPath: cfg.GRPCTLSKeyPath, Purpose: "edge-check"})
		if err != nil {
			return nil, err
		}
		creds, err := owner.credentials.GRPCServerCredentials(cfg.GRPCTLSEdgeClientURI)
		if err != nil {
			_ = owner.credentials.Close()
			return nil, err
		}
		options = append(options, grpc.Creds(creds))
	default:
		return nil, workload.NewConfigError(EnvGRPCTransport + " requires an explicit supported transport")
	}
	owner.server = grpc.NewServer(options...)
	adapter.Register(owner.server)
	healthv1.RegisterHealthServer(owner.server, owner.health)
	if cfg.GRPCAddress == "" {
		_ = owner.Close()
		return nil, workload.NewConfigError(EnvGRPCAddress + " is required")
	}
	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	owner.listener = listener
	return owner, nil
}

// Validate each newly admitted RPC against current trust even on an established
// HTTP/2 connection. CA overlap preserves admitted work and the live listener.
// The explicit CA replacement procedure drains old connections before removing
// old trust; a missed drain cannot admit a fresh Check through retired trust.
func verifyExternalAuthorizationPeer(ctx context.Context, owner *transportsecurity.Owner, edgeURI string) error {
	config, err := owner.ServerTLSConfig(edgeURI)
	if err != nil {
		return err
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return errors.New("native Check peer unavailable")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return errors.New("native Check TLS peer unavailable")
	}
	intermediates := x509.NewCertPool()
	for _, cert := range info.State.PeerCertificates[1:] {
		intermediates.AddCert(cert)
	}
	if _, err := info.State.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: config.ClientCAs, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return errors.New("native Check peer trust unavailable")
	}
	return config.VerifyConnection(info.State)
}

func (s *ExternalAuthorizationServer) Close() error {
	s.server.Stop()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.credentials != nil {
		return s.credentials.Close()
	}
	return nil
}

func (s *ExternalAuthorizationServer) Run(ctx context.Context, readiness *workload.Readiness) error {
	s.health.SetServingStatus("envoy.service.auth.v3.Authorization", healthv1.HealthCheckResponse_SERVING)
	s.logger.Info("auth.grpc.started", "component", "auth", "operation", "auth.external_authorization", "listener.transport", "grpc")
	served := make(chan error, 1)
	go func() { served <- s.server.Serve(s.listener) }()
	var serveErr error
	servedCompleted := false
	select {
	case <-ctx.Done():
	case serveErr = <-served:
		servedCompleted = true
	}
	if readiness != nil {
		readiness.BeginShutdown()
	}
	s.health.Shutdown()
	workload.BeginProcessShutdown(ctx)
	drainStarted := time.Now()
	drained := make(chan struct{})
	go func() { s.server.GracefulStop(); close(drained) }()
	timer := time.NewTimer(DefaultShutdownTimeout)
	defer timer.Stop()
	select {
	case <-drained:
		s.metrics.ObserveShutdown(s.logger, "shutdown_grpc_drain", "success", time.Since(drainStarted))
	case <-timer.C:
		s.metrics.ObserveShutdown(s.logger, "shutdown_grpc_drain", "timeout", time.Since(drainStarted))
		joinStarted := time.Now()
		s.server.Stop()
		<-drained
		s.metrics.ObserveShutdown(s.logger, "shutdown_grpc_cancel_join", "success", time.Since(joinStarted))
	}
	// Serve returns before admitted handlers have joined; GracefulStop/Stop joins
	// them before the application's resolver, database, and signer owners close.
	if !servedCompleted {
		serveErr = <-served
	}
	s.logger.Info("auth.grpc.shutdown.complete", "component", "auth", "operation", "auth.external_authorization", "listener.transport", "grpc")
	if errors.Is(serveErr, grpc.ErrServerStopped) {
		return nil
	}
	if serveErr != nil {
		s.logger.Error("auth.grpc.failed", workload.StartupFailureAttrs(serveErr, "component", "auth", "listener.transport", "grpc")...)
	}
	return serveErr
}

// An authorization denial is a successful gRPC exchange containing a typed
// DeniedHttpResponse. Transport status alone cannot identify that decision.
func externalAuthorizationOutcome(ctx context.Context, response any, err error) string {
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled || ctx.Err() == context.Canceled {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded || ctx.Err() == context.DeadlineExceeded {
		return "timeout"
	}
	if err != nil {
		return "error"
	}
	check, ok := response.(*authv3.CheckResponse)
	if !ok || check == nil {
		return "error"
	}
	if check.GetOkResponse() != nil && check.GetStatus() != nil && check.GetStatus().GetCode() == int32(codes.OK) {
		return "success"
	}
	if denied := check.GetDeniedResponse(); denied != nil {
		if code := int(denied.GetStatus().GetCode()); code >= 400 && code < 500 {
			return "rejected"
		}
	}
	return "error"
}
