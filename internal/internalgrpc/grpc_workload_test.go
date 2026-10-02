package internalgrpc

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/workload"

	"google.golang.org/grpc"
)

func TestRunGRPCWorkloadMetricsIncludeDatabaseStats(t *testing.T) {
	checkedMetrics := false
	err := RunGRPCWorkload(context.Background(), grpcWorkloadEnv{
		"TETRAL_INTERNAL_GRPC_AUDIENCE":            grpcauth.Audience,
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral/runtime",
	}, GRPCWorkloadParams{
		ServiceName:       "test-grpc-workload",
		HTTPListenEnvKey:  "TEST_HTTP_ADDR",
		HTTPListenDefault: "127.0.0.1:0",
		GRPCListenEnvKey:  "TEST_GRPC_ADDR",
		GRPCListenDefault: "127.0.0.1:0",
		Register:          func(*grpc.Server) {},
		CancelJoinTimeout: 73 * time.Millisecond,
		DBStatsProvider:   grpcWorkloadDBStatsProvider{stats: sql.DBStats{OpenConnections: 4}},
		Listen: func(string, string) (net.Listener, error) {
			return net.Listen("tcp", "127.0.0.1:0")
		},
		NewAuthenticator: func(grpcauth.Config) (Authenticator, error) {
			return grpcWorkloadAuthenticator{}, nil
		},
		RunInternalGRPC: func(ctx context.Context, cfg Config) error {
			if cfg.CancelJoinTimeout != 73*time.Millisecond {
				t.Errorf("cancel/join configuration = %s", cfg.CancelJoinTimeout)
			}
			cfg.Metrics.ObserveGRPCRequest("/tetral.test.v1.Service/Call", "OK", time.Second)
			cfg.OnServing()
			<-ctx.Done()
			return nil
		},
		RunWorkload: func(_ context.Context, cfg workload.Config) error {
			recorder := httptest.NewRecorder()
			cfg.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := recorder.Body.String()
			for _, want := range []string{
				"go_goroutines",
				"grpc_request_duration_seconds",
				`db_pool_open_connections{pool="runtime"} 4`,
				"tetral_diagnostic_sink_failures_total 0",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("/metrics body missing %q:\n%s", want, body)
				}
			}
			checkedMetrics = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RunGRPCWorkload: %v", err)
	}
	if !checkedMetrics {
		t.Fatal("RunWorkload seam did not inspect /metrics")
	}
}

type grpcWorkloadEnv map[string]string

func (e grpcWorkloadEnv) Getenv(key string) string { return e[key] }

type grpcWorkloadDBStatsProvider struct {
	stats sql.DBStats
}

func (p grpcWorkloadDBStatsProvider) Stats() sql.DBStats { return p.stats }

type grpcWorkloadAuthenticator struct{}

func (grpcWorkloadAuthenticator) Authenticate(context.Context, string) (grpcauth.Identity, error) {
	return grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral", Name: "runtime"}}, nil
}

func TestRunGRPCWorkloadJoinsCancelledServer(t *testing.T) {
	for _, beforeServing := range []bool{true, false} {
		name := "after_serving"
		if beforeServing {
			name = "before_serving"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			draining := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			defer func() { cancel(); close(release); <-done }()
			go func() {
				done <- RunGRPCWorkload(ctx, grpcWorkloadEnv{
					"TETRAL_INTERNAL_GRPC_AUDIENCE":            grpcauth.Audience,
					"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral/runtime",
				}, GRPCWorkloadParams{
					ServiceName: "test-join", HTTPListenDefault: "127.0.0.1:0", GRPCListenDefault: "127.0.0.1:0",
					ShutdownTimeout:  5 * time.Millisecond,
					NewAuthenticator: func(grpcauth.Config) (Authenticator, error) { return grpcWorkloadAuthenticator{}, nil },
					RunInternalGRPC: func(ctx context.Context, cfg Config) error {
						if !beforeServing {
							cfg.OnServing()
						}
						close(started)
						<-ctx.Done()
						close(draining)
						<-release
						return nil
					},
					RunWorkload: func(ctx context.Context, _ workload.Config) error { <-ctx.Done(); return nil },
				})
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("server not started")
			}
			cancel()
			select {
			case <-draining:
			case <-time.After(time.Second):
				t.Fatal("server not cancelled")
			}
			select {
			case err := <-done:
				done <- err
				t.Fatalf("workload returned before server joined: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
		})
	}
}
