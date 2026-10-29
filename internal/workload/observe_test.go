package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const observeTestTable = `{
  "service": "observe-test",
  "events": {
    "lease.renew.failed": {"level": "WARN", "log_type": "app", "audience": "operators",
      "fields": {"error.class": "string", "error.code": "string", "error.message_safe": "string", "duration_ms": "integer"},
      "errors": {"lease_expired": {"class": "lease_error", "message_safe": "lease expired"}}}
  },
  "operations": {"lease.renew": {"long": false, "errors": ["lease_expired"]}},
  "metrics": {
    "observe_test_rejected_total": {"type": "counter", "audience": "operators", "labels": {"cause": ["capacity"]}},
    "pressure_test_rejected_total": {"type": "counter", "audience": "operators", "labels": {"cause": ["capacity"]}},
    "observe_test_wait_seconds": {"type": "histogram", "audience": "operators", "labels": {"kind": ["lease"], "service": "service names"}, "buckets": [1, 2]}
  },
  "spans": {}
}`

type codedTestError string

func (e codedTestError) Error() string     { return "raw text that must never be recorded" }
func (e codedTestError) ErrorCode() string { return string(e) }

type recordedLines struct{ bytes.Buffer }

func (r *recordedLines) records(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(r.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("record is not JSON: %q", line)
		}
		out = append(out, record)
	}
	return out
}

func installObserveTestTelemetry(t *testing.T) (*recordedLines, *tracetest.InMemoryExporter, *Telemetry) {
	t.Helper()
	RegisterDeclarations("observe-test", []byte(observeTestTable))
	lines := &recordedLines{}
	exporter := tracetest.NewInMemoryExporter()
	telemetry := NewTelemetry(TelemetryOptions{Service: "observe-test", Logger: NewLogger(lines, "observe-test", "test", "unit"), SampleRatio: 1, SyncExporter: exporter})
	t.Cleanup(InstallTelemetry(telemetry))
	return lines, exporter, telemetry
}

func histogramCount(t *testing.T, telemetry *Telemetry, family, operation, outcome string) float64 {
	t.Helper()
	metrics, _ := telemetry.Collector()(context.Background())
	for _, metric := range metrics {
		if metric.Name != family+"_count" {
			continue
		}
		labels := map[string]string{}
		for _, label := range metric.Labels {
			labels[label.Name] = label.Value
		}
		if labels["operation"] == operation && labels["outcome"] == outcome {
			return metric.Value
		}
	}
	return 0
}

func TestObserveRecordsOneSpanSampleAndOutcomeWithContextFields(t *testing.T) {
	lines, exporter, telemetry := installObserveTestTelemetry(t)
	ctx := WithFields(context.Background(), "connection.id", "conn_1", "workspace.id", "wrk_1")
	_, observation := Observe(ctx, "lease.renew")
	observation.End(nil)
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "lease.renew" {
		t.Fatalf("spans = %+v; want one lease.renew span", spans)
	}
	attributes := map[string]string{}
	for _, attribute := range spans[0].Attributes {
		attributes[string(attribute.Key)] = attribute.Value.String()
	}
	if attributes["connection.id"] != "conn_1" || attributes["workspace.id"] != "wrk_1" {
		t.Fatalf("span attributes = %v; want the context's fields", attributes)
	}
	if got := histogramCount(t, telemetry, "tetral_operation_duration_seconds", "lease.renew", "ok"); got != 1 {
		t.Fatalf("ok samples = %v; want 1", got)
	}
	if lines.Len() != 0 {
		t.Fatalf("a successful observation wrote a record: %s", lines.String())
	}
}

func TestObservationEndHandledWritesOneFailureRecordAndEndWritesNone(t *testing.T) {
	lines, _, telemetry := installObserveTestTelemetry(t)
	_, observation := Observe(context.Background(), "lease.renew")
	observation.End(codedTestError("lease_expired"))
	if lines.Len() != 0 {
		t.Fatalf("End wrote a record: %s", lines.String())
	}
	_, observation = Observe(context.Background(), "lease.renew")
	observation.EndHandled(codedTestError("lease_expired"))
	records := lines.records(t)
	if len(records) != 1 {
		t.Fatalf("records = %v; want one failure record", records)
	}
	record := records[0]
	if record["event"] != "lease.renew.failed" || record["level"] != "WARN" || record["error.code"] != "lease_expired" || record["error.class"] != "lease_error" || record["error.message_safe"] != "lease expired" {
		t.Fatalf("failure record = %v", record)
	}
	if traceID, _ := record["trace_id"].(string); len(traceID) != 32 || traceID == strings.Repeat("0", 32) {
		t.Fatalf("trace_id = %v", record["trace_id"])
	}
	if spanID, _ := record["span_id"].(string); len(spanID) != 16 {
		t.Fatalf("span_id = %v", record["span_id"])
	}
	if strings.Contains(lines.String(), "raw text") {
		t.Fatal("failure record carried the error's own text")
	}
	if got := histogramCount(t, telemetry, "tetral_operation_duration_seconds", "lease.renew", "lease_expired"); got != 2 {
		t.Fatalf("lease_expired samples = %v; want 2", got)
	}
}

// The TypeScript library's test reads the same list, so a handled failure has
// the same field names in both languages.
func TestHandledFailureRecordHasTheSharedFieldNames(t *testing.T) {
	RegisterDeclarations("record-shape-test", []byte(`{"service": "record-shape-test",
	  "events": {"lookup.failed": {"level": "ERROR", "log_type": "app", "audience": "operators", "fields": {},
	    "errors": {"not_found": {"class": "lookup", "message_safe": "item not found"}}}},
	  "operations": {"lookup": {"long": false, "errors": ["not_found"]}}, "metrics": {}, "spans": {}}`))
	lines := &recordedLines{}
	t.Cleanup(InstallTelemetry(NewTelemetry(TelemetryOptions{Service: "record-shape-test", Logger: NewLogger(lines, "record-shape-test", "test", "unit"), SampleRatio: 1})))
	_, observation := Observe(WithFields(context.Background(), "request.id", "req_1"), "lookup")
	observation.EndHandled(codedTestError("not_found"))
	var shared struct {
		Fields []string
		Values map[string]any
	}
	data, err := os.ReadFile("../ts-observability/test-support/handled-failure-record.json")
	if err != nil || json.Unmarshal(data, &shared) != nil {
		t.Fatalf("read shared record shape: %v", err)
	}
	records := lines.records(t)
	if len(records) != 1 {
		t.Fatalf("records = %v; want one failure record", records)
	}
	fields := slices.Sorted(maps.Keys(records[0]))
	if !slices.Equal(fields, shared.Fields) {
		t.Fatalf("failure record fields = %v; want %v", fields, shared.Fields)
	}
	for name, want := range shared.Values {
		if records[0][name] != want {
			t.Fatalf("failure record %s = %v; want %v", name, records[0][name], want)
		}
	}
}

func TestObservationCountsUndeclaredCodesAsErrorAndRejectionsAsRejected(t *testing.T) {
	_, _, telemetry := installObserveTestTelemetry(t)
	_, observation := Observe(context.Background(), "lease.renew")
	observation.End(codedTestError("disk_full"))
	_, observation = Observe(context.Background(), "lease.renew")
	observation.End(errors.New("plain"))
	point := NewPressurePoint(telemetry.Registry(), "observe_test", "capacity")
	_, observation = Observe(context.Background(), "lease.renew")
	observation.EndHandled(point.Reject(context.Background(), "capacity"))
	if got := histogramCount(t, telemetry, "tetral_operation_duration_seconds", "lease.renew", "error"); got != 2 {
		t.Fatalf("error samples = %v; want 2", got)
	}
	if got := histogramCount(t, telemetry, "tetral_operation_duration_seconds", "lease.renew", "rejected"); got != 1 {
		t.Fatalf("rejected samples = %v; want 1", got)
	}
}

func TestUndeclaredOperationFailsTestsAndRecordsInvalidEventInProduction(t *testing.T) {
	lines, _, telemetry := installObserveTestTelemetry(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("an undeclared operation did not fail the test")
			}
		}()
		Observe(context.Background(), "lease.undeclared")
	}()
	failOnUndeclared = false
	t.Cleanup(func() { failOnUndeclared = true })
	_, observation := Observe(context.Background(), "lease.undeclared")
	observation.End(nil)
	records := lines.records(t)
	if len(records) != 1 || records[0]["event"] != "diagnostic.invalid_event" {
		t.Fatalf("records = %v; want one diagnostic.invalid_event", records)
	}
	if got := histogramCount(t, telemetry, "tetral_operation_duration_seconds", "unknown_operation", "ok"); got != 1 {
		t.Fatalf("undeclared operation kept its own label: %v", got)
	}
}

// The registry takes a family's type, labels and buckets from its
// declaration: a name declared with another type or other labels is as
// undeclared as a missing one.
func TestRegistryCreatesOnlyDeclaredFamiliesWithTheirDeclaredShape(t *testing.T) {
	RegisterDeclarations("observe-test", []byte(observeTestTable))
	registry := NewRegistry("observe-test")
	for name, create := range map[string]func(){
		"undeclared name":  func() { registry.Counter("observe_test_undeclared_total", "Undeclared.", "cause") },
		"another type":     func() { registry.Gauge("observe_test_rejected_total", "Wrong type.", "cause") },
		"other label name": func() { registry.Counter("observe_test_rejected_total", "Wrong label.", "reason") },
		"a missing label":  func() { registry.Histogram("observe_test_wait_seconds", "Missing label.", "kind") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: creating the family did not fail the test", name)
				}
			}()
			create()
		}()
	}
	failOnUndeclared = false
	t.Cleanup(func() { failOnUndeclared = true })
	registry.Counter("observe_test_undeclared_total", "Undeclared.", "cause").Add(1, "capacity")
	wait := registry.Histogram("observe_test_wait_seconds", "Declared.", "service", "kind")
	wait.Observe(1.5, "observe-test", "lease")
	wait.Observe(1.5, "observe-test", "renew")
	want := strings.Join([]string{
		"# HELP observe_test_wait_seconds Declared.",
		"# TYPE observe_test_wait_seconds histogram",
		`observe_test_wait_seconds_bucket{kind="lease",le="1",service="observe-test"} 0`,
		`observe_test_wait_seconds_bucket{kind="lease",le="2",service="observe-test"} 1`,
		`observe_test_wait_seconds_bucket{kind="lease",le="+Inf",service="observe-test"} 1`,
		`observe_test_wait_seconds_count{kind="lease",service="observe-test"} 1`,
		`observe_test_wait_seconds_sum{kind="lease",service="observe-test"} 1.5`,
	}, "\n") + "\n"
	if got := registry.Text(); got != want {
		t.Fatalf("registry text =\n%s\nwant only the declared family, its declared buckets and its declared label value:\n%s", got, want)
	}
}

func TestUndeclaredEventFailsTestsAndBecomesInvalidEventInProduction(t *testing.T) {
	RegisterDeclarations("observe-test", []byte(observeTestTable))
	lines := &recordedLines{}
	logger := NewLogger(lines, "observe-test", "test", "unit")
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("an undeclared event did not fail the test")
			}
		}()
		logger.Warn("lease.undeclared_event")
	}()
	failOnUndeclared = false
	t.Cleanup(func() { failOnUndeclared = true })
	logger.Warn("lease.undeclared_event")
	records := lines.records(t)
	if len(records) != 1 || records[0]["event"] != "diagnostic.invalid_event" || records[0]["msg"] != "diagnostic.invalid_event" {
		t.Fatalf("records = %v; want one diagnostic.invalid_event", records)
	}
}

// An unsampled root (ratio 0) still gives its records a trace ID.
func TestRecordsWithoutExporterStillCarryATraceID(t *testing.T) {
	for _, ratio := range []float64{1, 0} {
		lines := &recordedLines{}
		cfg := DefaultDiagnosticConfig()
		cfg.TraceSampleRatio = ratio
		owner := NewProcessLogger(lines, "observe-test", "test", "unit", cfg)
		if owner.Telemetry.tracing.processor != nil {
			t.Fatal("an exporter exists without an endpoint")
		}
		RegisterDeclarations("observe-test", []byte(observeTestTable))
		restore := InstallDefaultLogger(owner.Logger)
		ctx, observation := Observe(context.Background(), "lease.renew")
		owner.Logger.WarnContext(ctx, "lease.renew.failed", "error.class", "lease_error", "error.code", "lease_expired", "error.message_safe", "lease expired")
		observation.End(nil)
		owner.CloseWithBudget()
		restore()
		records := lines.records(t)
		if len(records) != 1 {
			t.Fatalf("ratio %v: records = %v", ratio, records)
		}
		if traceID, _ := records[0]["trace_id"].(string); len(traceID) != 32 || traceID == strings.Repeat("0", 32) {
			t.Fatalf("ratio %v: trace_id = %v; want a trace ID that is not all zeros", ratio, records[0]["trace_id"])
		}
		if records[0]["time"].(string)[19:20] != "." || len(records[0]["time"].(string)) != len("2026-10-24T08:15:30.123456Z") {
			t.Fatalf("time %v lacks six fractional digits", records[0]["time"])
		}
	}
}

// blockingExporter holds the export worker inside ExportSpans until release
// closes; entered receives once the worker is inside.
type blockingExporter struct{ entered, release chan struct{} }

func (e blockingExporter) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	select {
	case e.entered <- struct{}{}:
	default:
	}
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	return nil
}
func (blockingExporter) Shutdown(context.Context) error { return nil }

func TestFullSpanQueueDropsTheNextSpanWithoutBlocking(t *testing.T) {
	exporter := blockingExporter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	telemetry := NewTelemetry(TelemetryOptions{Service: "span-queue-test", Exporter: exporter, SampleRatio: 1})
	t.Cleanup(InstallTelemetry(telemetry))
	t.Cleanup(func() {
		close(exporter.release)
		telemetry.Shutdown(context.Background())
	})
	// Once the export worker holds the first span inside ExportSpans, it takes
	// no further span from the queue, so the queue's capacity alone decides
	// which later span is dropped.
	_, first := StartSpan(context.Background(), "queued")
	first.End()
	select {
	case <-exporter.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the export worker never exported the first span")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range spanQueueCapacity + 1 {
			_, span := StartSpan(context.Background(), "queued")
			span.End()
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ending a span blocked on a full queue")
	}
	metrics, _ := telemetry.Collector()(context.Background())
	var dropped float64 = -1
	for _, metric := range metrics {
		if metric.Name == "tetral_diagnostic_spans_dropped_total" {
			dropped = metric.Value
		}
	}
	if dropped != 1 {
		t.Fatalf("tetral_diagnostic_spans_dropped_total = %v; want only the span after a full queue dropped", dropped)
	}
}

func TestServiceColdStartRecordsPhasesAndObservesTotalAndEngine(t *testing.T) {
	lines := &recordedLines{}
	telemetry := NewTelemetry(TelemetryOptions{Service: "cold-start-test", Logger: NewLogger(lines, "cold-start-test", "test", "unit")})
	t.Cleanup(InstallTelemetry(telemetry))
	ctx := context.Background()
	coldStart := BeginServiceColdStart(ctx)
	coldStart.Advance(ctx)
	time.Sleep(2 * time.Second)
	coldStart.Advance(ctx)
	NewReadiness().MarkReady()
	counts := map[string]int{}
	var completed map[string]any
	for _, record := range lines.records(t) {
		event := record["event"].(string)
		if phase, ok := record["cold_start.phase"].(string); ok {
			event += " " + phase
		}
		counts[event]++
		if event == "cold_start.completed" {
			completed = record
		}
	}
	want := map[string]int{"cold_start.completed": 1}
	for _, phase := range []string{"config", "dependencies", "ready"} {
		want["cold_start.phase_started "+phase] = 1
		want["cold_start.phase_completed "+phase] = 1
	}
	if !maps.Equal(counts, want) {
		t.Fatalf("cold start records = %v; want one started and one completed per phase and one completed start", counts)
	}
	if completed["cold_start.kind"] != "service_pod" || completed["cold_start.start_kind"] != "new" || completed["outcome"] != "ok" {
		t.Fatalf("cold_start.completed = %v", completed)
	}
	if ms, _ := completed["dependencies_ms"].(float64); ms < 2000 {
		t.Fatalf("dependencies_ms = %v; want at least 2000", completed["dependencies_ms"])
	}
	metrics, _ := telemetry.Collector()(ctx)
	scopes := map[string]float64{}
	for _, metric := range metrics {
		if metric.Name != "cold_start_seconds_count" {
			continue
		}
		labels := map[string]string{}
		for _, label := range metric.Labels {
			labels[label.Name] = label.Value
		}
		if labels["service"] == "cold-start-test" && labels["kind"] == "service_pod" {
			scopes[labels["scope"]] = metric.Value
		}
	}
	if scopes["total"] != 1 || scopes["engine"] != 1 {
		t.Fatalf("cold_start_seconds samples = %v; want one total and one engine", scopes)
	}
}

func TestStartupFailureEndsTheServiceColdStartWithItsCodeAndObservesNoSample(t *testing.T) {
	lines := &recordedLines{}
	telemetry := NewTelemetry(TelemetryOptions{Service: "cold-start-test", Logger: NewLogger(lines, "cold-start-test", "test", "unit")})
	t.Cleanup(InstallTelemetry(telemetry))
	BeginServiceColdStart(context.Background())
	_ = LogStartupFailure(telemetry.Logger(), "cold-start-test", errors.New("dependency refused"))
	var outcomes []any
	for _, record := range lines.records(t) {
		if record["event"] == "cold_start.completed" {
			outcomes = append(outcomes, record["outcome"])
		}
	}
	if !slices.Equal(outcomes, []any{"startup_error"}) {
		t.Fatalf("cold_start.completed outcomes = %v; want one startup_error", outcomes)
	}
	metrics, _ := telemetry.Collector()(context.Background())
	for _, metric := range metrics {
		if metric.Name == "cold_start_seconds_count" && metric.Value != 0 {
			t.Fatalf("a failed cold start observed %s %v", metric.Name, metric.Labels)
		}
	}
}

// TestPressurePointRecordsOneEpisodePerQuietGap rejects three times, one
// second apart: an admission 59 seconds after the last rejection keeps the
// episode, and one 60 seconds after it ends the episode with its count.
func TestPressurePointRecordsOneEpisodePerQuietGap(t *testing.T) {
	RegisterDeclarations("observe-test", []byte(observeTestTable))
	lines := &recordedLines{}
	telemetry := NewTelemetry(TelemetryOptions{Service: "pressure-test", Logger: NewLogger(lines, "pressure-test", "test", "unit")})
	t.Cleanup(InstallTelemetry(telemetry))
	now := time.Unix(0, 0)
	point := NewPressurePoint(telemetry.Registry(), "pressure_test", "capacity").WithClock(func() time.Time { return now })
	ctx := context.Background()
	for i := range 3 {
		if i > 0 {
			now = now.Add(time.Second)
		}
		var rejection *Rejection
		if err := point.Reject(ctx, "capacity"); !errors.As(err, &rejection) {
			t.Fatalf("Reject returned %v", err)
		}
	}
	now = now.Add(59 * time.Second)
	point.Admit(ctx)
	if records := lines.records(t); len(records) != 1 || records[0]["event"] != "pressure.rejecting_started" {
		t.Fatalf("records 59 seconds after the last rejection = %v; want only rejecting_started", records)
	}
	now = now.Add(time.Second)
	point.Admit(ctx)
	records := lines.records(t)
	if len(records) != 2 || records[0]["event"] != "pressure.rejecting_started" || records[1]["event"] != "pressure.rejecting_ended" || records[1]["pressure.rejected"] != float64(3) || records[1]["pressure.point"] != "pressure_test" || records[1]["pressure.cause"] != "capacity" {
		t.Fatalf("episode records = %v", records)
	}
	if !strings.Contains(telemetry.Registry().Text(), `pressure_test_rejected_total{cause="capacity"} 3`) {
		t.Fatalf("rejections not counted:\n%s", telemetry.Registry().Text())
	}
}
