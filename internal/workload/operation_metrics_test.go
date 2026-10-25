package workload

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestOperationDurationExpositionAndClosedMethods(t *testing.T) {
	httpMetrics := NewHTTPMetrics("queue")
	grpcMetrics := NewGRPCMetrics("queue")
	grpcMetrics.SetMethods([]string{"/tetral.queue.v1.QueueService/Lease"})
	grpcMetrics.ObserveGRPCRequest("/tetral.queue.v1.QueueService/Lease", "OK", 100*time.Millisecond)
	grpcMetrics.ObserveGRPCRequest("/tetral.queue.v1.QueueService/Lease", "OK", 100*time.Millisecond+time.Nanosecond)
	grpcMetrics.ObserveGRPCRequest("request_identity_one", "Canceled", 11*time.Second)
	grpcMetrics.ObserveGRPCRequest("request_identity_two", "Canceled", time.Second)
	httpMetrics.ObserveHTTPRoute("", "PROPFIND", 403, 0, false)
	httpMetrics.ObserveHTTPRoute("GET /v1/client/events", "GET", 200, 40*time.Second, true)
	// A service-owned registry collected after a non-histogram collector, as
	// Job Runner's placement registry follows its diagnostics collector.
	ownedMetrics := NewOperationMetrics("git-proxy", "refs-upload")
	ownedMetrics.Observe("refs-upload", "success", 20*time.Millisecond)
	ownedMetrics.ObserveShutdown(nil, "shutdown_grpc_drain", "success", 40*time.Second)
	httpMetrics.Operations.ObserveShutdown(nil, "shutdown_http_drain", "ok", time.Second)
	response := httptest.NewRecorder()
	HealthRouter(NewReadiness(),
		WithMetricsCollector("http", httpMetrics.Collector()),
		WithMetricsCollector("grpc", grpcMetrics.Collector()),
		WithMetricsCollector("gauge", func(context.Context) ([]Metric, error) {
			return []Metric{{Name: "queue_ready_jobs", Help: "Ready jobs.", Type: "gauge", Value: 3}}, nil
		}),
		WithMetricsCollector("owned", ownedMetrics.Collector()),
	).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, expected := range []string{
		`# TYPE grpc_server_request_duration_seconds histogram`,
		`grpc_server_request_duration_seconds_bucket{code="OK",le="0.1",method="/tetral.queue.v1.QueueService/Lease",service="queue"} 1`,
		`grpc_server_request_duration_seconds_bucket{code="OK",le="0.25",method="/tetral.queue.v1.QueueService/Lease",service="queue"} 2`,
		`grpc_server_request_duration_seconds_bucket{code="Canceled",le="+Inf",method="unknown_method",service="queue"} 2`,
		`grpc_server_request_duration_seconds_bucket{code="Canceled",le="10",method="unknown_method",service="queue"} 1`,
		`http_server_request_duration_seconds_count{method="_OTHER",route="unmatched",service="queue",status_class="4xx"} 1`,
		`http_server_stream_duration_seconds_bucket{le="60",method="GET",route="GET /v1/client/events",service="queue",status_class="2xx"} 1`,
		`tetral_operation_duration_seconds_count{operation="refs-upload",outcome="ok",service="git-proxy"} 1`,
		`tetral_long_operation_duration_seconds_bucket{le="60",operation="shutdown_grpc_drain",outcome="ok",service="git-proxy"} 1`,
		`queue_ready_jobs 3`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s\n%s", expected, body)
		}
	}
	lines := strings.Split(body, "\n")
	for _, family := range []string{"grpc_server_request_duration_seconds", "tetral_operation_duration_seconds", "tetral_long_operation_duration_seconds"} {
		if strings.Count(body, "# TYPE "+family+" histogram") != 1 {
			t.Fatalf("%s has duplicate family headers", family)
		}
		// The exposition format requires one contiguous group after the
		// header, even when several registries contribute to the family.
		typeLine := slices.Index(lines, "# TYPE "+family+" histogram")
		offset := 0
		for index, line := range lines {
			if strings.HasPrefix(line, family+"_") {
				if index != typeLine+1+offset {
					t.Fatalf("sample %q is outside the contiguous %s group\n%s", line, family, body)
				}
				offset++
			}
		}
	}
	for _, line := range lines {
		if strings.Contains(line, "request_identity") || strings.Contains(line, "PROPFIND") {
			t.Fatalf("unbounded label value in histogram: %s", line)
		}
		if strings.HasPrefix(line, "# TYPE grpc_request_duration_seconds") || strings.HasPrefix(line, "# TYPE http_request_duration_seconds") {
			t.Fatalf("grpc_request_duration_seconds or http_request_duration_seconds rendered: %s", line)
		}
	}
}

func TestOperationMetricsRejectServiceOutsideWorkloadDomain(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a service outside the closed workload domain constructed a registry")
		}
	}()
	NewOperationMetrics("bridge-api")
}
