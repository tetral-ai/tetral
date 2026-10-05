package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
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
func TestHTTPServerDiagnosticsClassifyAndBoundWithoutRawText(t *testing.T) {

	var output bytes.Buffer
	writer := &httpServerDiagnostics{logger: NewLogger(&output, "diagnostic-test", "local", "unit")}
	inputs := []string{"http: TLS handshake error from fixture-private-address: fixture-secret-cert", "http: panic serving fixture-private-address: fixture-secret-body", "fixture-unexpected-server-message: fixture-secret-query"}
	for _, input := range inputs {
		for range 100 {
			n, err := writer.Write([]byte(input))
			if err != nil || n != len(input) {
				t.Fatal("diagnostics changed server behavior")
			}
		}
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("unbounded diagnostics: %d records", len(lines))
	}
	expected := []string{"tls_handshake", "handler_panic", "http_server"}
	for i, line := range lines {
		if strings.Contains(string(line), "fixture") {
			t.Fatal("raw server diagnostic escaped")
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["phase"] != expected[i] {
			t.Fatalf("phase %v want %q", record["phase"], expected[i])
		}
		if i == 1 && (record["level"] != "ERROR" || record["error.class"] != "panic" || record["error.code"] != "internal_error") {
			t.Fatal("panic failure classification lost")
		}
	}
	failing := &httpServerDiagnostics{logger: slog.New(&serverDiagnosticHandler{panicOnWrite: true})}
	if _, err := failing.Write([]byte(inputs[0])); err != nil {
		t.Fatal("throwing diagnostic sink escaped")
	}
}
