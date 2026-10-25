package workload

import (
	"context"
	"encoding/hex"
	"net"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// otlpReceiver is an in-process OTLP/gRPC trace collector that hands on the
// trace ID of each span it receives.
type otlpReceiver struct {
	coltracepb.UnimplementedTraceServiceServer
	traces chan string
}

func (r *otlpReceiver) Export(_ context.Context, request *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, resource := range request.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			for _, span := range scope.GetSpans() {
				r.traces <- hex.EncodeToString(span.GetTraceId())
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (r *otlpReceiver) next(t *testing.T) string {
	t.Helper()
	select {
	case traceID := <-r.traces:
		return traceID
	case <-time.After(10 * time.Second):
		t.Fatal("the receiver got no span")
		return ""
	}
}

// startOTLPReceiver serves a receiver on a loopback port and names it in
// OTEL_EXPORTER_OTLP_ENDPOINT.
func startOTLPReceiver(t *testing.T) *otlpReceiver {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	receiver := &otlpReceiver{traces: make(chan string, 16)}
	coltracepb.RegisterTraceServiceServer(server, receiver)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+listener.Addr().String())
	return receiver
}

// processFromEnvironment starts the process logger and telemetry a service
// starts, from the diagnostic variables of the environment.
func processFromEnvironment(t *testing.T) *ProcessLogger {
	t.Helper()
	cfg, err := DiagnosticConfigFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	owner := NewProcessLogger(&recordedLines{}, "observe-test", "test", "unit", cfg)
	RegisterDeclarations("observe-test", []byte(observeTestTable))
	t.Cleanup(InstallDefaultLogger(owner.Logger))
	return owner
}

func TestEndpointExportsAnEndedSpanToTheOTLPReceiver(t *testing.T) {
	receiver := startOTLPReceiver(t)
	owner := processFromEnvironment(t)
	ctx, observation := Observe(context.Background(), "lease.renew")
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	observation.End(nil)
	owner.CloseWithBudget()
	if got := receiver.next(t); got != traceID {
		t.Fatalf("received trace %s; want %s", got, traceID)
	}
}

func TestSampleRatioZeroExportsOnlyTheSpanWithASampledParent(t *testing.T) {
	receiver := startOTLPReceiver(t)
	t.Setenv("TETRAL_TRACE_SAMPLE_RATIO", "0")
	owner := processFromEnvironment(t)
	_, root := Observe(context.Background(), "lease.renew")
	root.End(nil)
	parent := ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	_, child := Observe(trace.ContextWithRemoteSpanContext(context.Background(), parent), "lease.renew")
	child.End(nil)
	owner.CloseWithBudget()
	// One worker exports spans in the order they end, so an exported root
	// span would arrive first.
	if got := receiver.next(t); got != parent.TraceID().String() {
		t.Fatalf("received trace %s first; want only the sampled parent's %s", got, parent.TraceID())
	}
	select {
	case traceID := <-receiver.traces:
		t.Fatalf("received another span, of trace %s", traceID)
	default:
	}
}
