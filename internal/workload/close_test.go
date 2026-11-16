package workload

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// A collector that never answers costs the spans their export, not the
// records their place in the sink.
func TestCloseWritesQueuedRecordsBeforeASpanExportThatNeverEnds(t *testing.T) {
	RegisterDeclarations("failure-test", []byte(failureTestTable))
	lines := &recordedLines{}
	owner := NewProcessLogger(lines, "failure-test", "local", "unit", DefaultDiagnosticConfig())
	exporter := blockingExporter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(exporter.release) })
	owner.Telemetry = NewTelemetry(TelemetryOptions{Service: "failure-test", Logger: owner.Logger, Exporter: exporter, SampleRatio: 1})
	owner.state.telemetry = owner.Telemetry
	_, span := owner.Telemetry.tracing.tracer.Start(context.Background(), "queued")
	span.End()
	// With a burst of 1 the second and third failure wait in the limiter and
	// reach the queue only as the close's summary.
	for range 3 {
		WriteFailure(context.Background(), owner.Logger, "dependency.failed", "unavailable", CauseOf(errors.New("connection refused")))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	owner.Close(ctx)
	select {
	case <-exporter.entered:
	default:
		t.Fatal("the close never handed the span to the exporter")
	}
	var events []any
	for _, record := range lines.records(t) {
		events = append(events, record["event"], record["suppressed.count"])
	}
	if want := []any{"dependency.failed", nil, "diagnostic.suppressed", 2.0}; !reflect.DeepEqual(events, want) {
		t.Fatalf("records in the sink = %v; want %v", events, want)
	}
	if dropped := owner.Stats().Dropped; dropped != 0 {
		t.Fatalf("%d records were dropped at close", dropped)
	}
}
