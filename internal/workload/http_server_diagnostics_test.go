package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

type serverDiagnosticHandler struct {
	records      []slog.Record
	panicOnWrite bool
}

func (*serverDiagnosticHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *serverDiagnosticHandler) Handle(_ context.Context, r slog.Record) error {
	if h.panicOnWrite {
		panic("fixture throwing sink")
	}
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *serverDiagnosticHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *serverDiagnosticHandler) WithGroup(string) slog.Handler      { return h }
func TestHTTPServerDiagnosticsClassifyBoundAndCarryTheServerText(t *testing.T) {
	var output bytes.Buffer
	writer := &httpServerDiagnostics{logger: NewLogger(&output, "diagnostic-test", "local", "unit")}
	rows := []struct{ input, phase, level, class, code, cause string }{
		{"http: TLS handshake error from 203.0.113.9:41234: remote error: tls: bad certificate", "tls_handshake", "WARN", "transport_error", "tls_handshake_failed", "http: TLS handshake error: remote error: tls: bad certificate"},
		{"http: panic serving 203.0.113.9:41234: runtime error: index out of range\ngoroutine 7 [running]:", "handler_panic", "ERROR", "panic", "internal_error", "http: panic serving: runtime error: index out of range"},
		{"http: Accept error: accept tcp: too many open files; retrying in 5ms", "http_server", "ERROR", "internal_error", "http_server_failed", "http: Accept error: accept tcp: too many open files; retrying in 5ms"},
	}
	for _, row := range rows {
		for range 100 {
			n, err := writer.Write([]byte(row.input))
			if err != nil || n != len(row.input) {
				t.Fatal("diagnostics changed server behavior")
			}
		}
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != len(rows) {
		t.Fatalf("unbounded diagnostics: %d records", len(lines))
	}
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		row := rows[i]
		if record["phase"] != row.phase || record["level"] != row.level || record["error.class"] != row.class || record["error.code"] != row.code || record["error.message"] != row.cause {
			t.Fatalf("record = %v; want %+v", record, row)
		}
	}
	inputs := []string{rows[0].input}
	failing := &httpServerDiagnostics{logger: slog.New(&serverDiagnosticHandler{panicOnWrite: true})}
	if _, err := failing.Write([]byte(inputs[0])); err != nil {
		t.Fatal("throwing diagnostic sink escaped")
	}
}
