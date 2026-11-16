package internalgrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workload/workloadtest"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type sharedRule struct {
	Rule         string
	Traceparents []string
	Status       int
	Want         map[string]any
}

// TestRulesSharedWithTypeScript runs the gRPC rows of the table the
// TypeScript library is tested with too. A row's want is what the contract
// gives, not what the other library does. The other rows run in
// internal/workload, which owns them.
func TestRulesSharedWithTypeScript(t *testing.T) {
	probes := map[string]func(*testing.T, sharedRule) map[string]any{
		"repeated_traceparent": probeRepeatedTraceparent,
		"unknown_grpc_status":  probeUnknownGRPCStatus,
	}
	elsewhere := map[string]bool{"debug_failure_rate_limit": true, "force_flush": true}
	data, err := os.ReadFile("../ts-observability/test-support/go-typescript-rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var table struct{ Rows []sharedRule }
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, row := range table.Rows {
		probe := probes[row.Rule]
		if probe == nil {
			if !elsewhere[row.Rule] {
				t.Fatalf("rule %q has no probe in Go", row.Rule)
			}
			continue
		}
		ran++
		t.Run(row.Rule, func(t *testing.T) {
			encoded, err := json.Marshal(probe(t, row))
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, row.Want) {
				t.Fatalf("observed %v; the contract gives %v", got, row.Want)
			}
		})
	}
	if ran != len(probes) {
		t.Fatalf("the table ran %d of this package's %d rules", ran, len(probes))
	}
}

// servedCall makes one call that handler answers and returns the records,
// the server span and the metrics the boundary left.
func servedCall(t *testing.T, traceparents []string, handler func(context.Context) error) ([]map[string]any, tracetest.SpanStub, *workload.GRPCMetrics) {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "test-service", SyncExporter: spans, SampleRatio: 1})))
	var buffer bytes.Buffer
	metrics := workload.NewGRPCMetrics("queue")
	client, cleanup := newInternalGRPCClient(t, Config{
		ServiceName:   "test-service",
		Authenticator: &allowingAuthenticator{},
		Logger:        newTestLogger(t, &buffer),
		Metrics:       metrics,
		Register:      func(server *grpc.Server) { registerTestService(server, handler) },
	})
	pairs := []string{"authorization", "Bearer token"}
	for _, traceparent := range traceparents {
		pairs = append(pairs, "traceparent", traceparent)
	}
	_ = client.Invoke(metadata.NewOutgoingContext(context.Background(), metadata.Pairs(pairs...)), testMethod, &emptypb.Empty{}, &emptypb.Empty{})
	cleanup()
	served := slices.IndexFunc(spans.GetSpans(), func(span tracetest.SpanStub) bool { return span.Name == testMethod })
	if served < 0 {
		t.Fatalf("no server span of %s among %v", testMethod, spans.GetSpans())
	}
	return workloadtest.Find(workloadtest.Records(t, buffer.Bytes()), "internal.grpc.request"), spans.GetSpans()[served], metrics
}

func probeRepeatedTraceparent(t *testing.T, row sharedRule) map[string]any {
	_, span, _ := servedCall(t, row.Traceparents, func(context.Context) error { return nil })
	sent := slices.ContainsFunc(row.Traceparents, func(traceparent string) bool {
		return workload.ParseTraceparent(traceparent).TraceID() == span.SpanContext.TraceID()
	})
	parent := ""
	if span.Parent.HasSpanID() {
		parent = span.Parent.SpanID().String()
	}
	return map[string]any{"continues_a_sent_trace": sent, "parent_span_id": parent}
}

func probeUnknownGRPCStatus(t *testing.T, row sharedRule) map[string]any {
	records, span, metrics := servedCall(t, nil, func(context.Context) error {
		return status.Error(codes.Code(row.Status), "handler failed") //nolint:gosec // G115: the row's status is a small number.
	})
	if len(records) != 1 {
		t.Fatalf("internal.grpc.request records = %v; want one", records)
	}
	got := map[string]any{"record_error_code": records[0]["error.code"]}
	for _, attribute := range span.Attributes {
		if attribute.Key == "error.code" {
			got["span_error_code"] = attribute.Value.AsString()
		}
	}
	collected, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range collected {
		for _, label := range metric.Labels {
			if metric.Name == "grpc_server_request_duration_seconds_count" && label.Name == "code" && metric.Value == 1 {
				got["metric_code_label"] = label.Value
			}
		}
	}
	return got
}
