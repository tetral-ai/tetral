package internalgrpc

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/workload"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// newTestLogger captures JSON log lines into buffer through the production handler.
func newTestLogger(buffer *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buffer, nil)).With(slog.String("service.name", "test-service"))
}

const testMethod = "/tetral.test.v1.TestService/Call"

func TestInternalGRPCRegistersCallbackAndHealthService(t *testing.T) {
	var called bool
	server, err := NewServer(Config{
		ServiceName:   "test-service",
		Authenticator: &allowingAuthenticator{},
		Register: func(server *grpc.Server) {
			called = true
			registerTestService(server, func(context.Context) error { return nil })
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if !called {
		t.Fatal("registration callback was not called")
	}
	if _, ok := server.GetServiceInfo()["grpc.health.v1.Health"]; !ok {
		t.Fatal("gRPC health service was not registered")
	}
}

func TestInternalGRPCAuthenticatesBeforeDispatchAndLogsSafely(t *testing.T) {
	var buffer bytes.Buffer
	metrics := workload.NewGRPCMetrics()
	dispatches := 0
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName:   "test-service",
		Authenticator: &allowingAuthenticator{},
		Logger:        newTestLogger(&buffer),
		Metrics:       metrics,
		Register: func(server *grpc.Server) {
			registerTestService(server, func(ctx context.Context) error {
				dispatches++
				identity, ok := auth.IdentityFromContext(ctx)
				if !ok || identity.ServiceAccount.String() != "runtime/agent" {
					t.Fatalf("identity = %#v ok=%v; want runtime/agent", identity, ok)
				}
				return status.Error(codes.Unimplemented, "not implemented")
			})
		},
	})
	defer cleanup()

	err := client.Invoke(context.Background(), testMethod, &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated call error = %v; want Unauthenticated", err)
	}
	if dispatches != 0 {
		t.Fatal("handler dispatched before auth")
	}

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret-token-sentinel"))
	err = client.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("authenticated call error = %v; want Unimplemented", err)
	}
	if dispatches != 1 {
		t.Fatalf("dispatches = %d; want 1", dispatches)
	}
	logOutput := buffer.String()
	if strings.Contains(logOutput, "secret-token-sentinel") {
		t.Fatalf("gRPC boundary log leaked bearer token: %s", logOutput)
	}
	for _, want := range []string{
		`"msg":"internal.grpc.request"`,
		`"operation":"internal.grpc.request"`,
		`"event.kind":"grpc_request"`,
		`"component":"internal-grpc"`,
		`"grpc.method":"` + testMethod + `"`,
		`"grpc.code":"Unimplemented"`,
		`"caller.service_account":"runtime/agent"`,
		`"retryable":false`,
		`"terminal":true`,
		`"error.class":"grpc_error"`,
		`"error.code":"unimplemented"`,
		`"error.message_safe":"internal gRPC request failed"`,
		`"duration.ms":`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("gRPC boundary log missing %s: %s", want, logOutput)
		}
	}
	assertGRPCMetricCount(t, metrics, testMethod, codes.Unauthenticated.String(), 1)
	assertGRPCMetricCount(t, metrics, testMethod, codes.Unimplemented.String(), 1)
}

func TestInternalGRPCOKBoundaryLogOmitsFailureClassification(t *testing.T) {
	var buffer bytes.Buffer
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName:   "test-service",
		Authenticator: &allowingAuthenticator{},
		Logger:        newTestLogger(&buffer),
		Register: func(server *grpc.Server) {
			registerTestService(server, func(context.Context) error { return nil })
		},
	})
	defer cleanup()

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer ok-token"))
	if err := client.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
		t.Fatalf("authenticated OK call: %v", err)
	}
	logOutput := buffer.String()
	for _, want := range []string{
		`"msg":"internal.grpc.request"`,
		`"operation":"internal.grpc.request"`,
		`"event.kind":"grpc_request"`,
		`"component":"internal-grpc"`,
		`"grpc.method":"` + testMethod + `"`,
		`"grpc.code":"OK"`,
		`"caller.service_account":"runtime/agent"`,
		`"duration.ms":`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("OK gRPC boundary log missing %s: %s", want, logOutput)
		}
	}
	for _, forbidden := range []string{`"retryable":`, `"terminal":`, `"error.class":`, `"error.code":`, `"error.message_safe":`} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("OK gRPC boundary log contains failure field %s: %s", forbidden, logOutput)
		}
	}
}

func TestInternalGRPCAuthorizesMethodBeforeDispatchAndLogsSafely(t *testing.T) {
	var buffer bytes.Buffer
	dispatches := 0
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName:   "test-service",
		Authenticator: &allowingAuthenticator{},
		Logger:        newTestLogger(&buffer),
		MethodAuthorizer: func(identity auth.Identity, method string) error {
			if identity.ServiceAccount.String() != "runtime/agent" {
				t.Fatalf("identity = %s; want runtime/agent", identity.ServiceAccount.String())
			}
			if method != testMethod {
				t.Fatalf("method = %s; want %s", method, testMethod)
			}
			return status.Error(codes.PermissionDenied, "method not allowed")
		},
		Register: func(server *grpc.Server) {
			registerTestService(server, func(context.Context) error {
				dispatches++
				return nil
			})
		},
	})
	defer cleanup()

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret-token-sentinel"))
	err := client.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("authorized call error = %v; want PermissionDenied", err)
	}
	if dispatches != 0 {
		t.Fatalf("dispatches = %d; want 0", dispatches)
	}
	logOutput := buffer.String()
	if strings.Contains(logOutput, "secret-token-sentinel") {
		t.Fatalf("gRPC boundary log leaked bearer token: %s", logOutput)
	}
	for _, want := range []string{
		`"msg":"internal.grpc.request"`,
		`"operation":"internal.grpc.request"`,
		`"event.kind":"grpc_request"`,
		`"component":"internal-grpc"`,
		`"grpc.method":"` + testMethod + `"`,
		`"grpc.code":"PermissionDenied"`,
		`"caller.service_account":"runtime/agent"`,
		`"retryable":false`,
		`"terminal":true`,
		`"error.class":"grpc_error"`,
		`"error.code":"permissiondenied"`,
		`"error.message_safe":"internal gRPC request failed"`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("gRPC boundary log missing %s: %s", want, logOutput)
		}
	}
}

func TestInternalGRPCAuthenticatesStreamingHealthWatch(t *testing.T) {
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName:   "test-service",
		Authenticator: &allowingAuthenticator{},
		Register: func(server *grpc.Server) {
			registerTestService(server, func(context.Context) error { return nil })
		},
	})
	defer cleanup()
	healthClient := healthv1.NewHealthClient(client)

	stream, err := healthClient.Watch(context.Background(), &healthv1.HealthCheckRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated health Watch error = %v; want Unauthenticated", err)
	}

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer health-token"))
	stream, err = healthClient.Watch(ctx, &healthv1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("authenticated health Watch: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("authenticated health Watch Recv: %v", err)
	}
	if response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
		t.Fatalf("health Watch status = %s; want SERVING", response.GetStatus())
	}
}

func TestInternalGRPCRecoveryCoversAuthenticatorPanics(t *testing.T) {
	var buffer bytes.Buffer
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName:   "test-service",
		Authenticator: panicAuthenticator{},
		Logger:        newTestLogger(&buffer),
		Register: func(server *grpc.Server) {
			registerTestService(server, func(context.Context) error { return nil })
		},
	})
	defer cleanup()

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer panic-token-sentinel"))
	err := client.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic auth error = %v; want Internal", err)
	}
	logOutput := buffer.String()
	if strings.Contains(logOutput, "panic-token-sentinel") {
		t.Fatalf("recovery log leaked bearer token: %s", logOutput)
	}
	if strings.Contains(logOutput, "auth panic with raw token material") {
		t.Fatalf("recovery log leaked panic text: %s", logOutput)
	}
	for _, want := range []string{
		`"msg":"internal.grpc.recovered"`,
		`"operation":"internal.grpc.request"`,
		`"event.kind":"panic"`,
		`"component":"internal-grpc"`,
		`"grpc.method":"` + testMethod + `"`,
		`"grpc.code":"Internal"`,
		`"duration.ms":`,
		`"retryable":false`,
		`"terminal":true`,
		`"error.class":"panic"`,
		`"error.code":"internal_error"`,
		`"error.message_safe":"internal gRPC error"`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("recovery log missing %s: %s", want, logOutput)
		}
	}
}

func TestInternalGRPCHealthReportsNotServingBeforeGracefulStopCompletes(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	releaseHandler := make(chan struct{})
	handlerStarted := make(chan struct{})
	var startedOnce sync.Once
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- Run(runCtx, Config{
			ServiceName:   "test-service",
			Authenticator: &allowingAuthenticator{},
			Listener:      listener,
			Register: func(server *grpc.Server) {
				registerTestService(server, func(ctx context.Context) error {
					startedOnce.Do(func() { close(handlerStarted) })
					select {
					case <-releaseHandler:
					case <-ctx.Done():
					}
					return nil
				})
			},
		})
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()

	authCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer health-token"))
	// A health Watch opened before shutdown stays open through GracefulStop and
	// receives status transitions, so it can observe NOT_SERVING during the drain.
	watchStream, err := healthv1.NewHealthClient(conn).Watch(authCtx, &healthv1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health Watch: %v", err)
	}
	first, err := watchStream.Recv()
	if err != nil || first.GetStatus() != healthv1.HealthCheckResponse_SERVING {
		t.Fatalf("initial health status = %v err=%v; want SERVING", first.GetStatus(), err)
	}

	// Keep one unary RPC in flight so GracefulStop cannot complete until released.
	callDone := make(chan error, 1)
	go func() {
		callDone <- conn.Invoke(authCtx, testMethod, &emptypb.Empty{}, &emptypb.Empty{})
	}()
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight handler did not start")
	}

	// Begin shutdown: Run must flip health NOT_SERVING before GracefulStop blocks on the drain.
	cancelRun()
	for {
		next, recvErr := watchStream.Recv()
		if recvErr != nil {
			t.Fatalf("health Watch Recv during drain: %v", recvErr)
		}
		if next.GetStatus() == healthv1.HealthCheckResponse_NOT_SERVING {
			break
		}
	}

	// GracefulStop is still draining: the in-flight call has not completed yet.
	select {
	case err := <-callDone:
		t.Fatalf("in-flight call completed before release; GracefulStop already finished: %v", err)
	default:
	}

	close(releaseHandler)
	if err := <-callDone; err != nil {
		t.Fatalf("in-flight call error after release: %v", err)
	}
	// Close the client so the long-lived health Watch stream ends; GracefulStop
	// also waits on open streams, so leaving it open would pin the drain open.
	_ = conn.Close()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after drain completed")
	}
}

func newInternalGRPCClient(t *testing.T, cfg Config) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	cfg.Listener = listener
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg)
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		cancel()
		err := <-done
		if err != nil && !strings.Contains(err.Error(), "closed") {
			t.Fatalf("Run returned error: %v", err)
		}
	}
	return conn, cleanup
}

type allowingAuthenticator struct{}

func (a *allowingAuthenticator) Authenticate(context.Context, string) (auth.Identity, error) {
	return auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "runtime", Name: "agent"}}, nil
}

type panicAuthenticator struct{}

func (panicAuthenticator) Authenticate(context.Context, string) (auth.Identity, error) {
	panic("auth panic with raw token material")
}

type testService interface{}

func registerTestService(server *grpc.Server, handler func(context.Context) error) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "tetral.test.v1.TestService",
		HandlerType: (*testService)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Call",
			Handler: func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				request := &emptypb.Empty{}
				if err := decode(request); err != nil {
					return nil, err
				}
				call := func(ctx context.Context, _ any) (any, error) {
					return &emptypb.Empty{}, handler(ctx)
				}
				if interceptor == nil {
					return call(ctx, request)
				}
				return interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: testMethod}, call)
			},
		}},
	}, struct{}{})
}

func assertGRPCMetricCount(t *testing.T, metrics *workload.GRPCMetrics, method string, code string, want float64) {
	t.Helper()
	collected, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatalf("collect gRPC metrics: %v", err)
	}
	for _, metric := range collected {
		if metric.Name != "grpc_request_duration_seconds_count" || metric.Value != want {
			continue
		}
		if hasMetricLabel(metric.Labels, "grpc_method", method) && hasMetricLabel(metric.Labels, "grpc_code", code) {
			return
		}
	}
	t.Fatalf("missing grpc_request_duration_seconds_count for method=%s code=%s value=%g in %+v", method, code, want, collected)
}

func hasMetricLabel(labels []workload.MetricLabel, name string, value string) bool {
	for _, label := range labels {
		if label.Name == name && label.Value == value {
			return true
		}
	}
	return false
}

var _ io.Writer = (*bytes.Buffer)(nil)
