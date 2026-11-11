// Package workloadtest gives tests the record writer a service runs with.
package workloadtest

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

// Logger writes service's records to writer as the service's process does,
// at DEBUG and with every repeat written. table is the path of the service's
// observability.json, registered first; a test in the service's own package,
// which registers it, passes "".
func Logger(t testing.TB, writer io.Writer, service, table string) *slog.Logger {
	t.Helper()
	if table != "" {
		data, err := os.ReadFile(table) //nolint:gosec // G304: a test names a table of this repository.
		if err != nil {
			t.Fatal(err)
		}
		workload.RegisterDeclarations(service, data)
	}
	cfg := workload.DefaultDiagnosticConfig()
	cfg.Level, cfg.Burst = slog.LevelDebug, 1<<20
	return workload.NewLoggerWithConfig(writer, service, "test", "unit", cfg)
}

// Records decodes the records written to a Logger's writer.
func Records(t testing.TB, output []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(output), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("record is not JSON: %q", line)
		}
		records = append(records, record)
	}
	return records
}

// Find returns the records of event.
func Find(records []map[string]any, event string) []map[string]any {
	var found []map[string]any
	for _, record := range records {
		if record["event"] == event {
			found = append(found, record)
		}
	}
	return found
}
