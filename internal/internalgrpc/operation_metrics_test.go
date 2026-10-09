package internalgrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tetral-ai/tetral/internal/workload"
)

// Exercise descriptor installation, interceptor completion and the real Run join.
func TestInternalGRPCOperationDurationsFollowRegisteredRequestsAndShutdown(t *testing.T) {
	metrics := workload.NewGRPCMetrics("queue")
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName: "queue", Authenticator: &allowingAuthenticator{}, Metrics: metrics,
		Register: func(server *grpc.Server) { registerTestService(server, func(context.Context) error { return nil }) },
	})
	t.Cleanup(func() { cleanup() })
	if err := client.Invoke(context.Background(), testMethod, &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("denial: %v", err)
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer owning-operation-test"))
	if err := client.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
		t.Fatalf("success: %v", err)
	}
	// Joining Run makes the interceptor population stable, without sleeping or
	// treating local handler completion as a validated Agent turn.
	cleanup()
	cleanup = func() {}
	response := httptest.NewRecorder()
	workload.HealthRouter(workload.NewReadiness(), workload.WithMetricsCollector("grpc", metrics.Collector())).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	for _, expected := range []string{
		`tetral_operation_duration_seconds_count{operation="` + testMethod + `",outcome="success",service="queue"} 1`,
		`tetral_operation_duration_seconds_count{operation="` + testMethod + `",outcome="rejected",service="queue"} 1`,
		`tetral_operation_duration_seconds_count{operation="shutdown_grpc_drain",outcome="success",service="queue"} 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s\n%s", expected, body)
		}
	}
	if strings.Contains(body, `operation="unknown_method"`) {
		t.Fatalf("registered method lost its descriptor domain: %s", body)
	}
}

type shutdownRecordWriter struct {
	mutex sync.Mutex
	bytes.Buffer
}

func (w *shutdownRecordWriter) Write(b []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.Buffer.Write(b)
}
func (w *shutdownRecordWriter) records() []byte {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return append([]byte(nil), w.Bytes()...)
}

type throwingShutdownWriter struct{}

func (throwingShutdownWriter) Write([]byte) (int, error) { panic("diagnostic sink") }

type blockedShutdownWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedShutdownWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(b), nil
}

// The final sample belongs to the draining process, even after its metrics HTTP
// listener has gone. Hostile diagnostic writers cannot acquire join ownership.
func TestInternalGRPCShutdownRecordsAfterMetricsListenerClosesAndSinkFailures(t *testing.T) {
	for _, kind := range []string{"central_record", "throwing_sink", "blocked_sink"} {
		t.Run(kind, func(t *testing.T) {
			var collected shutdownRecordWriter
			blocked := &blockedShutdownWriter{entered: make(chan struct{}), release: make(chan struct{})}
			var sink io.Writer = &collected
			if kind == "throwing_sink" {
				sink = throwingShutdownWriter{}
			}
			if kind == "blocked_sink" {
				sink = blocked
			}
			owner := workload.NewProcessLogger(sink, "queue", "test", "unit", workload.DefaultDiagnosticConfig())
			var releaseSink sync.Once
			t.Cleanup(func() { releaseSink.Do(func() { close(blocked.release) }); owner.CloseWithBudget() })
			metrics := workload.NewGRPCMetrics("queue")
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			joined := make(chan error, 1)
			go func() {
				joined <- Run(ctx, Config{ServiceName: "queue", Listener: listener, Authenticator: &allowingAuthenticator{}, Logger: owner.Logger, Metrics: metrics, ShutdownTimeout: 2 * time.Second, Register: func(server *grpc.Server) {
					registerTestService(server, func(context.Context) error { close(entered); <-release; return nil })
				}})
			}()
			client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			requestDone := make(chan error, 1)
			callCtx, callCancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer owning-shutdown-test")), 5*time.Second)
			defer callCancel()
			go func() { requestDone <- client.Invoke(callCtx, testMethod, &emptypb.Empty{}, &emptypb.Empty{}) }()
			select {
			case <-entered:
			case <-callCtx.Done():
				t.Fatal("held owner did not enter")
			}
			watchCtx, stopWatch := context.WithCancel(callCtx)
			defer stopWatch()
			watch, err := healthv1.NewHealthClient(client).Watch(watchCtx, &healthv1.HealthCheckRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if response, err := watch.Recv(); err != nil || response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
				t.Fatalf("health watch did not observe serving:%v/%v", response, err)
			}
			metricsHTTP := httptest.NewServer(workload.HealthRouter(workload.NewReadiness(), workload.WithMetricsCollector("grpc", metrics.Collector())))
			response, err := http.Get(metricsHTTP.URL + "/metrics")
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			metricsHTTP.Close()
			if response, err := http.Get(metricsHTTP.URL + "/metrics"); err == nil {
				_ = response.Body.Close()
				t.Fatal("metrics listener remained available")
			}
			cancel()
			// The real NOT_SERVING transition proves Run has entered its graceful
			// phase while this admitted worker is still held, without a sleep.
			if response, err := watch.Recv(); err != nil || response.GetStatus() != healthv1.HealthCheckResponse_NOT_SERVING {
				t.Fatalf("held owner did not observe graceful shutdown:%v/%v", response, err)
			}
			stopWatch()
			select {
			case err := <-joined:
				t.Fatalf("Run joined held owner early:%v", err)
			default:
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-requestDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("admitted RPC did not join")
			}
			select {
			case err := <-joined:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("diagnostic sink acquired shutdown ownership")
			}
			if kind == "blocked_sink" {
				select {
				case <-blocked.entered:
				case <-time.After(time.Second):
					t.Fatal("production diagnostic worker did not reach blocked writer")
				}
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				owner.Close(closeCtx)
				closeCancel()
				releaseSink.Do(func() { close(blocked.release) })
			}
			owner.CloseWithBudget()
			text := metrics.Operations.Text()
			if !strings.Contains(text, `tetral_operation_duration_seconds_count{operation="shutdown_grpc_drain",outcome="success",service="queue"} 1`) {
				t.Fatalf("actual drain histogram missing:%s", text)
			}
			if kind == "throwing_sink" && owner.Stats().SinkFailures == 0 {
				t.Fatal("missing diagnostic sink failure receipt")
			}
			if kind != "central_record" {
				return
			}
			var drain map[string]any
			for _, line := range bytes.Split(bytes.TrimSpace(collected.records()), []byte("\n")) {
				var record map[string]any
				if err := json.Unmarshal(line, &record); err != nil {
					t.Fatal(err)
				}
				if record["event"] == "workload.shutdown.phase_completed" && record["operation"] == "shutdown_grpc_drain" {
					drain = record
				}
			}
			if drain == nil || drain["metric.observation.count"] != float64(1) || drain["outcome"] != "success" || drain["service.instance.id"] == "" || drain["service.instance.id"] == nil {
				t.Fatalf("missing attributable final phase record:%v", drain)
			}
			samples, err := metrics.Operations.Collector()(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, sample := range samples {
				if sample.Name == "tetral_operation_duration_seconds_sum" {
					for _, label := range sample.Labels {
						if label.Name == "operation" && label.Value == "shutdown_grpc_drain" && sample.Value != drain["duration.seconds"] {
							t.Fatalf("log/histogram source differs:%v/%v", sample.Value, drain)
						}
					}
				}
			}
		})
	}
}
