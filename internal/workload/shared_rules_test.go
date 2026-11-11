package workload

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// TestRulesSharedWithTypeScript runs the rows the TypeScript library is
// tested with too. A row's want is what the contract gives, not what the
// other library does. The rows of the gRPC interceptors run in
// internal/internalgrpc, which owns them.
func TestRulesSharedWithTypeScript(t *testing.T) {
	probes := map[string]func(*testing.T) map[string]any{
		"debug_failure_rate_limit": probeDebugFailureRateLimit,
		"force_flush":              probeForceFlush,
	}
	elsewhere := map[string]bool{"repeated_traceparent": true, "unknown_grpc_status": true}
	data, err := os.ReadFile("../ts-observability/test-support/go-typescript-rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Rows []struct {
			Rule string
			Want map[string]any
		}
	}
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
			encoded, err := json.Marshal(probe(t))
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

func probeDebugFailureRateLimit(t *testing.T) map[string]any {
	RegisterDeclarations("failure-test", []byte(failureTestTable))
	cfg := DefaultDiagnosticConfig()
	cfg.Level = slog.LevelDebug
	lines := &recordedLines{}
	owner := NewProcessLogger(lines, "failure-test", "local", "unit", cfg)
	instant := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	owner.state.clock = func() time.Time { return instant }
	for range 3 {
		WriteFailure(context.Background(), owner.Logger, "dependency.probed", "unavailable", CauseOf(errors.New("connection refused")))
	}
	got := map[string]any{"written": 0, "suppressed": owner.Stats().Suppressed, "summaries": 0}
	owner.CloseWithBudget()
	for _, record := range lines.records(t) {
		switch record["event"] {
		case "dependency.probed":
			got["written"] = got["written"].(int) + 1
		case "diagnostic.suppressed":
			got["summaries"] = got["summaries"].(int) + 1
			got["summary_suppressed_count"] = record["suppressed.count"]
		}
	}
	return got
}

func probeForceFlush(t *testing.T) map[string]any {
	var exported atomic.Int64
	exporter := blockingExporter{entered: make(chan struct{}, 1), release: make(chan struct{}), spans: &exported}
	telemetry := NewTelemetry(TelemetryOptions{Service: "failure-test", Exporter: exporter, SampleRatio: 1})
	t.Cleanup(func() { telemetry.Shutdown(context.Background()) })
	_, span := telemetry.tracing.tracer.Start(context.Background(), "held_export")
	span.End()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		_ = telemetry.tracing.provider.ForceFlush(context.Background())
	}()
	select {
	case <-exporter.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the ended span never reached the exporter")
	}
	got := map[string]any{}
	// The exporter holds the span, so a flush that waits for it cannot have
	// returned; one that does not wait returned before the export began.
	select {
	case <-returned:
		got["returned_while_held"] = true
	default:
		got["returned_while_held"] = false
	}
	close(exporter.release)
	select {
	case <-returned:
		got["returned_after_release"] = true
	case <-time.After(10 * time.Second):
		got["returned_after_release"] = false
	}
	got["spans_exported"] = exported.Load()
	return got
}
