package workload

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestOperationDurationExpositionAndClosedMethods(t *testing.T) {
	httpMetrics := NewHTTPMetrics("queue")
	grpcMetrics := NewGRPCMetrics("queue")
	grpcMetrics.Operations.SetOperations([]string{"/tetral.queue.v1.QueueService/Lease"})
	grpcMetrics.ObserveGRPCRequest("/tetral.queue.v1.QueueService/Lease", "OK", 100*time.Millisecond)
	grpcMetrics.ObserveGRPCRequest("/tetral.queue.v1.QueueService/Lease", "OK", 100*time.Millisecond+time.Nanosecond)
	grpcMetrics.ObserveGRPCRequest("request_identity_one", "Canceled", 1801*time.Second)
	grpcMetrics.ObserveGRPCRequest("request_identity_two", "Canceled", time.Second)
	httpMetrics.ObserveHTTPRequest("request_identity_three", 403, 0)
	// A service-owned registry collected after a non-histogram collector, as
	// Job Runner's placement registry follows its diagnostics collector.
	ownedMetrics := NewOperationMetrics("queue", "lease_reclaim")
	ownedMetrics.Observe("lease_reclaim", "success", 20*time.Millisecond)
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
		`# TYPE tetral_operation_duration_seconds histogram`,
		`tetral_operation_duration_seconds_bucket{le="0.1",operation="/tetral.queue.v1.QueueService/Lease",outcome="success",service="queue"} 1`,
		`tetral_operation_duration_seconds_bucket{le="0.25",operation="/tetral.queue.v1.QueueService/Lease",outcome="success",service="queue"} 2`,
		`tetral_operation_duration_seconds_bucket{le="+Inf",operation="unknown_method",outcome="cancelled",service="queue"} 2`,
		`tetral_operation_duration_seconds_bucket{le="1800",operation="unknown_method",outcome="cancelled",service="queue"} 1`,
		`tetral_operation_duration_seconds_count{operation="http_unknown_method",outcome="rejected",service="queue"} 1`,
		`grpc_request_duration_seconds_count{grpc_code="OK",grpc_method="/tetral.queue.v1.QueueService/Lease"} 2`,
		`tetral_operation_duration_seconds_count{operation="lease_reclaim",outcome="success",service="queue"} 1`,
		`queue_ready_jobs 3`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s\n%s", expected, body)
		}
	}
	if strings.Count(body, "# TYPE tetral_operation_duration_seconds histogram") != 1 {
		t.Fatal("histogram has duplicate family headers")
	}
	lines := strings.Split(body, "\n")
	typeLine := -1
	var familyLines []int
	for index, line := range lines {
		if line == "# TYPE tetral_operation_duration_seconds histogram" {
			typeLine = index
		}
		if strings.HasPrefix(line, "tetral_operation_duration_seconds") {
			familyLines = append(familyLines, index)
		}
	}
	// The exposition format requires one contiguous group after the header,
	// even though three registries contribute to this family.
	for offset, index := range familyLines {
		if typeLine < 0 || index != typeLine+1+offset {
			t.Fatalf("histogram sample %q is outside the contiguous family group\n%s", lines[index], body)
		}
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "tetral_operation_") && strings.Contains(line, "request_identity") {
			t.Fatalf("unbounded method in histogram: %s", line)
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

func TestOperationDurationBucketsAgreeAcrossLanguages(t *testing.T) {
	data, err := os.ReadFile("../ts-observability/src/operation-duration-buckets.json")
	if err != nil {
		t.Fatal(err)
	}
	var projected []float64
	if err := json.Unmarshal(data, &projected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(operationDurationBuckets[:], projected) {
		t.Fatalf("Go %v differs from TypeScript %v", operationDurationBuckets, projected)
	}
}
