package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiagnosticBootControlsAndSafeErrors(t *testing.T) {
	cfg, err := DiagnosticConfigFromEnv(func(string) string { return "" })
	if err != nil || cfg.Level != slog.LevelInfo || cfg.MaxRecordBytes != 16384 || cfg.SummaryInterval != 30*time.Second || cfg.Burst != 1 {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
	values := map[string]string{"TETRAL_LOG_LEVEL": "warn", "TETRAL_LOG_MAX_RECORD_BYTES": "4096", "TETRAL_LOG_SUMMARY_INTERVAL_MS": "500", "TETRAL_LOG_BURST": "3"}
	cfg, err = DiagnosticConfigFromEnv(func(key string) string { return values[key] })
	if err != nil || cfg.Level != slog.LevelWarn || cfg.MaxRecordBytes != 4096 || cfg.SummaryInterval != 500*time.Millisecond || cfg.Burst != 3 {
		t.Fatalf("overrides: %+v %v", cfg, err)
	}
	for key, value := range map[string]string{"TETRAL_LOG_LEVEL": "PRIVATE_SENTINEL", "TETRAL_LOG_MAX_RECORD_BYTES": "1023", "TETRAL_LOG_SUMMARY_INTERVAL_MS": "99", "TETRAL_LOG_BURST": "0"} {
		_, err := DiagnosticConfigFromEnv(func(k string) string {
			if k == key {
				return value
			}
			return ""
		})
		if err == nil || strings.Contains(err.Error(), value) {
			t.Fatalf("unsafe or accepted invalid control %s: %v", key, err)
		}
	}
}
func TestDiagnosticLimiterRecoveryAndContentBounds(t *testing.T) {
	var output bytes.Buffer
	cfg := DefaultDiagnosticConfig()
	state := newDiagnosticState(cfg)
	now := time.Now()
	state.clock = func() time.Time { return now }
	logger := newDiagnosticLogger(promptDiagnosticWriter{&output, &state.counts}, "test", "local", "unit", state)
	for n := 0; n < 1000; n++ {
		logger.Debug("queue.empty")
	}
	if output.Len() != 0 {
		t.Fatal("healthy poll visible at default level")
	}
	for n := 0; n < 1000; n++ {
		logger.Error("dependency.failed", slog.String("error.class", "dependency"), slog.String("error.code", "unavailable"), slog.String("error.message_safe", "dependency unavailable"), slog.Int("session.id", n), slog.String("phase", "connect"), slog.String("component", "connector"))
	}
	if bytes.Count(output.Bytes(), []byte("\n")) != 1 || state.stats().LimiterEntries != 1 {
		t.Fatal("failure storm was not bounded")
	}
	logger.Info("dependency.recovered", slog.String("recovery.event", "dependency.failed"))
	var summary map[string]any
	if err := json.Unmarshal(bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))[1], &summary); err != nil {
		t.Fatal(err)
	}
	if summary["error.class"] != "dependency" || summary["error.code"] != "unavailable" || summary["error.message_safe"] != "dependency unavailable" || summary["phase"] != "connect" || summary["component"] != "connector" {
		t.Fatalf("summary classification = %#v", summary)
	}
	if !strings.Contains(output.String(), `"suppressed.count":999`) || !strings.Contains(output.String(), `"event":"dependency.recovered"`) {
		t.Fatalf("missing recovery/summary: %s", output.String())
	}
	logger.Error("dependency.failed", slog.String("error.class", "dependency"), slog.String("error.code", "unavailable"), slog.String("error.message_safe", "dependency unavailable"))
	if bytes.Count(output.Bytes(), []byte("\n")) != 4 {
		t.Fatal("new failure after recovery was suppressed")
	}
	logger.Info("safe.fields", slog.String("tool_result", "PRIVATE_CONTENT_SENTINEL"), slog.String("message", "first\r\nsecond"), slog.String("note", strings.Repeat("x", 100000)))
	if strings.Contains(output.String(), "PRIVATE_CONTENT_SENTINEL") || strings.Contains(output.String(), "first\\r") {
		t.Fatal("unsafe content or CRLF survived")
	}
	before := output.Len()
	attrs := []any{}
	for _, key := range []string{"phase", "reason", "component", "kind", "message", "operation", "request.id", "workspace.id", "session.id", "thread.id", "job.id", "queue.kind", "binding.id", "sandbox.id", "cleanup.id", "provider.request.id", "trace.id", "span.id", "input.kind", "status"} {
		attrs = append(attrs, key, strings.Repeat("x", 1024))
	}
	logger.Info("oversize", attrs...)
	if output.Len() != before || state.stats().Dropped != 1 {
		t.Fatal("oversize record was emitted")
	}
	for n := 0; n < 1000; n++ {
		logger.Warn("event."+string(rune(n+1)), slog.String("reason", "failure"))
	}
	if state.stats().LimiterEntries > 256 {
		t.Fatal("unbounded limiter state")
	}
}

type panicDiagnosticWriter struct{}

func (panicDiagnosticWriter) Write([]byte) (int, error) { panic("sink panic") }

type errorDiagnosticWriter struct{}

func (errorDiagnosticWriter) Write([]byte) (int, error) { return 0, errors.New("sink failure") }
func TestDiagnosticSinkFailuresAndMetricsAreIndependent(t *testing.T) {
	for _, writer := range []io.Writer{panicDiagnosticWriter{}, errorDiagnosticWriter{}} {
		owner := NewProcessLogger(writer, "test", "local", "unit", DefaultDiagnosticConfig())
		owner.Logger.Error("operation.failed")
		owner.CloseWithBudget()
		s := owner.Stats()
		if s.SinkFailures != 1 || s.Dropped != 1 {
			t.Fatalf("lost sink accounting: %+v", s)
		}
		router := HealthRouter(NewReadiness(), WithMetricsCollector("diagnostics", DiagnosticMetrics(owner.Logger)))
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if !strings.Contains(response.Body.String(), "tetral_diagnostic_sink_failures_total 1") {
			t.Fatal("loss counters absent from actual metrics endpoint")
		}
		if !strings.Contains(response.Body.String(), "tetral_diagnostic_queue_records ") {
			t.Fatal("queue gauge absent from actual metrics endpoint")
		}
	}
}

type observedPipeWriter struct {
	writer  *os.File
	started chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (w *observedPipeWriter) Write(b []byte) (int, error) {
	w.calls.Add(1)
	w.once.Do(func() { close(w.started) })
	return w.writer.Write(b)
}
func TestProductionDiagnosticPipeBackpressureHasFixedResourcesAndBoundedClose(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close pipe reader: %v", err)
		}
	}()
	defer func() {
		if err := writer.Close(); err != nil {
			t.Errorf("close pipe writer: %v", err)
		}
	}()
	baselineWorkers := runtime.NumGoroutine()
	observed := &observedPipeWriter{writer: writer, started: make(chan struct{})}
	owner := NewProcessLogger(observed, "test", "local", "unit", DefaultDiagnosticConfig())
	// Enough bounded data to fill a real pipe while its reader is paused.
	attrs := []any{}
	for _, key := range []string{"phase", "reason", "component", "kind", "message", "operation", "request.id", "workspace.id", "session.id", "thread.id", "job.id", "queue.kind"} {
		attrs = append(attrs, key, strings.Repeat("x", 1024))
	}
	owner.Logger.Info("backpressure", attrs...)
	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("production worker never reached pipe")
	}
	for n := 0; n < 1000; n++ {
		owner.Logger.Info("business.observation", attrs...)
	}
	if owner.Stats().Queued > 64 || owner.Stats().Dropped == 0 {
		t.Fatalf("queue not bounded: %+v", owner.Stats())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	owner.Close(ctx)
	if time.Since(started) > 200*time.Millisecond {
		t.Fatal("close exceeded owned budget")
	}
	if observed.calls.Load() < 1 || runtime.NumGoroutine() > baselineWorkers+3 {
		t.Fatal("blocked sink did not retain a fixed worker bound")
	}
	// io.Writer has no cancellation contract: release the one blocked worker in cleanup.
	if err := reader.Close(); err != nil {
		t.Fatalf("release blocked pipe writer: %v", err)
	}
	select {
	case <-owner.done:
	case <-time.After(time.Second):
		t.Fatal("released writer worker did not join")
	}
}
func TestDiagnosticWithGroupKeepsExistingAttributesAtOriginalScope(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "local", "unit").With("request.id", "req").WithGroup("phase")
	logger.Info("phase.done", "outcome", "success")
	text := output.String()
	if !strings.Contains(text, `"request.id":"req"`) || !strings.Contains(text, `"phase.outcome":"success"`) || strings.Contains(text, `"phase.request.id"`) {
		t.Fatalf("wrong grouping: %s", text)
	}
}

func TestDiagnosticEscalationAndInvalidDynamicMessages(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "local", "unit")
	logger.Warn("dependency.failed", slog.String("error.code", "unavailable"), slog.String("error.class", "dependency"), slog.String("error.message_safe", "dependency unavailable"))
	logger.Warn("dependency.failed", slog.String("error.code", "unavailable"), slog.String("error.class", "dependency"), slog.String("error.message_safe", "dependency unavailable"))
	logger.Error("dependency.failed", slog.String("error.code", "unavailable"), slog.String("error.class", "dependency"), slog.String("error.message_safe", "dependency unavailable"))
	if bytes.Count(output.Bytes(), []byte("\n")) != 2 {
		t.Fatal("error escalation hidden by warn storm")
	}
	logger.Info("Bearer sk-private-secret")
	if strings.Contains(output.String(), "sk-private-secret") || !strings.Contains(output.String(), "diagnostic.invalid_event") {
		t.Fatal("dynamic message leaked")
	}
}

func TestDiagnosticFixedProseUsesExistingEventClassification(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "local", "unit")
	logger.Error("provider credential rejected", slog.String("event.kind", "provider_credential_rejected"), slog.String("error.class", "validation"), slog.String("error.code", "invalid_provider_oauth"), slog.String("error.message_safe", "provider credential rejected"))
	if !strings.Contains(output.String(), `"event":"provider_credential_rejected"`) || strings.Contains(output.String(), "diagnostic.invalid_event") {
		t.Fatal("existing fixed event classification lost")
	}
}

func TestDiagnosticFieldVocabularyProjection(t *testing.T) {
	data, err := os.ReadFile("../ts-observability/src/fields.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected []string
	if err := json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	actual := make([]string, 0, len(approvedDiagnosticFields))
	for name := range approvedDiagnosticFields {
		actual = append(actual, name)
	}
	sort.Strings(actual)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("Go/TS diagnostic vocabulary drift")
	}
	mutated := append([]string{}, actual[1:]...)
	if reflect.DeepEqual(mutated, expected) {
		t.Fatal("projection comparison did not reject missing field")
	}
}

func TestDiagnosticResourceMetadataIsBoundedBeforeEncoding(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "prod\r\nwest\x00", "Bearer sk-private-service-secret")
	logger.Info("safe.event")
	text := output.String()
	if strings.Contains(text, "sk-private-service-secret") || !strings.Contains(text, `"deployment.environment":"prod  west "`) || !strings.Contains(text, `"service.version":"[REDACTED]"`) {
		t.Fatalf("unsafe metadata: %s", text)
	}
	output.Reset()
	NewLogger(&output, "test", strings.Repeat("PRIVATE_METADATA_SENTINEL", 10000), "unit").Info("safe.event")
	if strings.Contains(output.String(), "PRIVATE_METADATA_SENTINEL") || !strings.Contains(output.String(), "[TRUNCATED]") {
		t.Fatal("oversize metadata retained")
	}
}

func TestDiagnosticStartupBuilderExcludesNonTokenPayload(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "local", "unit")
	failure := errors.New(strings.Repeat("PRIVATE_CONTENT_SENTINEL\r\n\x00", 10000))
	if returned := LogStartupFailure(logger, "test", failure); returned != failure {
		t.Fatal("startup diagnostics replaced the original failure")
	}
	if strings.Contains(output.String(), "PRIVATE_CONTENT_SENTINEL") || !strings.Contains(output.String(), `"error.message_safe":"startup failed"`) {
		t.Fatal("startup builder leaked dependency payload")
	}
}

func TestDiagnosticSensitiveCountExceptionRetainsOnlyApprovedNumbers(t *testing.T) {
	for _, value := range []slog.Value{slog.Int64Value(3), slog.Uint64Value(3), slog.Float64Value(3)} {
		actual, accepted := diagnosticValue("stream.open_reasoning_count", value)
		if !accepted || actual == "[REDACTED]" {
			t.Fatalf("approved numeric count was redacted: %v", value)
		}
	}
	for _, field := range []struct {
		key   string
		value slog.Value
	}{
		{"stream.open_reasoning_count", slog.StringValue("PRIVATE_CONTENT_SENTINEL")},
		{"unknown_reasoning_count", slog.Int64Value(3)},
		{"reasoning", slog.Int64Value(3)},
	} {
		actual, accepted := diagnosticValue(field.key, field.value)
		if !accepted || actual != "[REDACTED]" {
			t.Fatalf("sensitive field escaped count policy: %s = %v", field.key, actual)
		}
	}
}

func TestDiagnosticInheritedAttributesAreBoundedBeforeRetention(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "local", "unit").With("session.id", strings.Repeat("PRIVATE_CONTENT_SENTINEL", 100000))
	handler := logger.Handler().(*diagnosticHandler)
	if len(handler.attrs) != 1 || handler.attrs[0].Value.String() != "[TRUNCATED]" {
		t.Fatalf("retained attrs = %#v", handler.attrs)
	}
	logger.Warn("dependency.failed", "reason", "unavailable")
	logger.Warn("dependency.failed", "reason", "unavailable")
	handler.state.flush(handler.writer)
	if strings.Contains(output.String(), "PRIVATE_CONTENT_SENTINEL") || !strings.Contains(output.String(), `"session.id":"[TRUNCATED]"`) {
		t.Fatalf("unsafe output: %s", &output)
	}
}

func TestDiagnosticUnknownAndGroupedAttributeWorkIsBounded(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(&output, "test", "local", "unit")
	attrs := make([]slog.Attr, 1000)
	for n := range attrs {
		attrs[n] = slog.String("unknown", "ignored")
	}
	attrs[999] = slog.String("request.id", "late-request")
	logger.LogAttrs(context.Background(), slog.LevelInfo, "safe.event", slog.GroupAttrs("scope", attrs...))
	inherited := logger.With(slog.GroupAttrs("scope", attrs...))
	if strings.Contains(output.String(), "late-request") || len(inherited.Handler().(*diagnosticHandler).attrs) != 0 {
		t.Fatal("processed attributes beyond fixed inspection budget")
	}
}

func TestDiagnosticPartialSemanticTuplesCompleteAtLoweredLevels(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		for _, supplied := range []slog.Attr{slog.String("error.class", "storage_error"), slog.String("error.code", "unavailable"), slog.String("error.message_safe", "storage unavailable")} {
			var output bytes.Buffer
			logger := NewLogger(&output, "test", "local", "unit")
			logger.LogAttrs(context.Background(), level, "operation.failed", supplied)
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"error.class", "error.code", "error.message_safe"} {
				if value, ok := record[field].(string); !ok || value == "" {
					t.Fatalf("incomplete tuple at %s: %#v", level, record)
				}
			}
			if record[supplied.Key] != supplied.Value.String() {
				t.Fatalf("specific approved classification replaced: %#v", record)
			}
		}
	}
	var output bytes.Buffer
	NewLogger(&output, "test", "local", "unit").Warn("degradation.waiting", "reason", "retry_pending")
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"error.class", "error.code", "error.message_safe"} {
		if _, present := record[field]; present {
			t.Fatalf("warning fabricated a failure: %#v", record)
		}
	}
}

func TestDiagnosticSummaryKeepsFirstSemanticFailureAfterOrdinaryWarning(t *testing.T) {
	var output bytes.Buffer
	state := newDiagnosticState(DefaultDiagnosticConfig())
	writer := promptDiagnosticWriter{&output, &state.counts}
	logger := newDiagnosticLogger(writer, "test", "local", "unit", state)
	logger.Warn("dependency.failed", "reason", "unavailable", "phase", "waiting")
	for range 2 {
		logger.Error("dependency.failed", "error.class", "dependency_error", "error.code", "unavailable", "error.message_safe", "dependency unavailable", "phase", "final", "component", "connector")
	}
	state.flush(writer)
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("escalation/summary records = %s", output.String())
	}
	var summary map[string]any
	if err := json.Unmarshal(lines[2], &summary); err != nil {
		t.Fatal(err)
	}
	if summary["error.class"] != "dependency_error" || summary["error.code"] != "unavailable" || summary["error.message_safe"] != "dependency unavailable" || summary["phase"] != "final" || summary["component"] != "connector" || summary["suppressed.count"] != float64(1) {
		t.Fatalf("promoted safe sample = %#v", summary)
	}
}
