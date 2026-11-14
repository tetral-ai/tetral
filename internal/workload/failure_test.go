package workload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
)

const failureTestTable = `{"service": "failure-test",
  "events": {
    "dependency.failed": {"level": "ERROR", "log_type": "app", "audience": "operators",
      "fields": {"error.class": "string", "error.code": "string", "error.message_safe": "string", "error.message": "string",
        "session.id": "integer", "phase": "string", "component": "string", "reason": "string"},
      "errors": {
        "unavailable": {"class": "dependency", "message_safe": "dependency unavailable", "cause": true},
        "peer_refused": {"class": "peer", "message_safe": "the peer refused", "no_cause": "cli_report"}}},
    "dependency.slow": {"level": "WARN", "log_type": "app", "audience": "operators",
      "fields": {"error.class": "string", "error.code": "string", "error.message_safe": "string", "error.message": "string"},
      "errors": {"slow": {"class": "dependency", "message_safe": "dependency slow", "cause": true}}},
    "dependency.probed": {"level": "DEBUG", "log_type": "app", "audience": "operators",
      "fields": {"error.class": "string", "error.code": "string", "error.message_safe": "string", "error.message": "string"},
      "errors": {"unavailable": {"class": "dependency", "message_safe": "dependency unavailable", "cause": true}}}},
  "operations": {}, "metrics": {}, "spans": {}}`

// failureRecords runs write against a failure-test logger with the
// test-binary failure replaced by a list, and returns the problems the writer
// named and the records it then wrote, which are the production ones.
func failureRecords(t *testing.T, write func(*slog.Logger)) (problems []string, records []map[string]any) {
	t.Helper()
	RegisterDeclarations("failure-test", []byte(failureTestTable))
	fail := failRecordRule
	failRecordRule = func(problem string) { problems = append(problems, problem) }
	t.Cleanup(func() { failRecordRule = fail })
	var output bytes.Buffer
	write(NewLogger(&output, "failure-test", "local", "unit"))
	failRecordRule = fail
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("record is not JSON: %q", line)
		}
		records = append(records, record)
	}
	return problems, records
}

func TestFailureRecordAgreesWithItsCodeDeclaration(t *testing.T) {
	cause := CauseOf(errors.New("dial tcp 10.0.0.7:5432: connection refused"))
	for _, row := range []struct {
		name    string
		write   func(*slog.Logger)
		problem string // part of the problem that fails a test binary; "" passes
		record  map[string]any
	}{
		{name: "declared with a cause, written with one",
			write: func(l *slog.Logger) { WriteFailure(context.Background(), l, "dependency.failed", "unavailable", cause) },
			record: map[string]any{"event": "dependency.failed", "level": "ERROR", "error.class": "dependency", "error.code": "unavailable",
				"error.message_safe": "dependency unavailable", "error.message": "dial tcp 10.0.0.7:5432: connection refused"}},
		{name: "declared with a cause, written without one",
			write: func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.failed", "unavailable", NoCause)
			},
			problem: `event "dependency.failed", error code "unavailable": the code is declared with a cause and was written without one; written at `,
			record: map[string]any{"event": "dependency.failed", "level": "ERROR", "error.class": "dependency", "error.code": "unavailable",
				"error.message_safe": "dependency unavailable", "error.message": nil}},
		{name: "declared without a cause, written with one",
			write: func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.failed", "peer_refused", cause)
			},
			problem: `event "dependency.failed", error code "peer_refused": the code is declared without a cause and was written with one; written at `,
			record: map[string]any{"event": "dependency.failed", "level": "ERROR", "error.class": "peer", "error.code": "peer_refused",
				"error.message_safe": "the peer refused", "error.message": nil}},
		{name: "declared without a cause, written without one",
			write: func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.failed", "peer_refused", NoCause)
			},
			record: map[string]any{"event": "dependency.failed", "level": "ERROR", "error.class": "peer", "error.code": "peer_refused",
				"error.message_safe": "the peer refused", "error.message": nil}},
		{name: "a code the event does not declare",
			write:   func(l *slog.Logger) { WriteFailure(context.Background(), l, "dependency.failed", "disk_full", cause) },
			problem: `event "dependency.failed", error code "disk_full": the event does not declare the code in the observability.json of service failure-test; written at `,
			record: map[string]any{"event": "diagnostic.invalid_event", "level": "WARN", "event.original": "dependency.failed",
				"reason": "undeclared_error_code", "error.code": nil, "error.message": nil}},
		{name: "the entry's own fields passed by the site",
			write: func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.failed", "unavailable", cause, slog.String("error.message_safe", "site text"))
			},
			problem: `event "dependency.failed", error code "unavailable": the site passes error.message_safe as a field`,
			record:  map[string]any{"event": "dependency.failed", "error.message_safe": "dependency unavailable"}},
		{name: "a record at ERROR written without the entry",
			write:   func(l *slog.Logger) { l.Error("dependency.failed", slog.String("phase", "connect")) },
			problem: `event "dependency.failed": a failure record is written only through WriteFailure; written at `,
			record: map[string]any{"event": "diagnostic.invalid_event", "level": "WARN", "event.original": "dependency.failed",
				"reason": "failure_outside_entry", "phase": "connect"}},
		{name: "a record with an error field written without the entry",
			write: func(l *slog.Logger) {
				l.Warn("dependency.slow", slog.String("error.code", "slow"), slog.String("error.message", "took 9s"))
			},
			problem: `event "dependency.slow", error code "slow": a failure record is written only through WriteFailure; written at `,
			record: map[string]any{"event": "diagnostic.invalid_event", "level": "WARN", "event.original": "dependency.slow",
				"reason": "failure_outside_entry", "error.code": nil, "error.message": nil}},
		{name: "an event the service does not declare",
			write:   func(l *slog.Logger) { l.Warn("dependency.unheard_of", slog.String("phase", "connect")) },
			problem: `event "dependency.unheard_of": the event is not declared in the observability.json of service failure-test; written at `,
			record:  map[string]any{"event": "diagnostic.invalid_event", "level": "WARN", "event.original": "dependency.unheard_of", "phase": "connect"}},
		{name: "a failure through the entry on an event the service does not declare",
			write: func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.unheard_of", "unavailable", cause)
			},
			problem: `event "dependency.unheard_of", error code "unavailable": the event is not declared in the observability.json of service failure-test; written at `,
			record: map[string]any{"event": "diagnostic.invalid_event", "level": "ERROR", "event.original": "dependency.unheard_of",
				"reason": nil, "error.code": nil, "error.message": nil}},
		{name: "a record below the level set with an error field written without the entry",
			write: func(l *slog.Logger) {
				l.Debug("dependency.slow", slog.String("error.code", "slow"))
			},
			problem: `event "dependency.slow", error code "slow": a failure record is written only through WriteFailure; written at `},
		{name: "an ordinary WARN written without the entry",
			write: func(l *slog.Logger) { l.Warn("dependency.slow") },
			record: map[string]any{"event": "dependency.slow", "level": "WARN", "error.class": nil, "error.code": nil,
				"error.message_safe": nil, "error.message": nil}},
		{name: "a failure below ERROR written through the entry",
			write: func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.slow", "slow", CauseOf(errors.New("took 9s")))
			},
			record: map[string]any{"event": "dependency.slow", "level": "WARN", "error.code": "slow", "error.message": "took 9s"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			problems, records := failureRecords(t, row.write)
			if row.problem == "" && len(problems) != 0 {
				t.Fatalf("the writer failed the record: %v", problems)
			}
			// Every problem names its writing site, which is this file.
			if row.problem != "" && (len(problems) != 1 || !strings.Contains(problems[0], row.problem) || !strings.Contains(problems[0], "failure_test.go:")) {
				t.Fatalf("problems = %v; want one naming %q and this file as the site", problems, row.problem)
			}
			// A row without a record is written below the level set.
			if row.record == nil && len(records) == 0 {
				return
			}
			if row.record == nil || len(records) != 1 {
				t.Fatalf("records = %v; want %v", records, row.record)
			}
			for name, want := range row.record {
				if got, present := records[0][name]; got != want || (want == nil && present) {
					t.Fatalf("%s = %v; want %v in %v", name, got, want, records[0])
				}
			}
		})
	}
}

// The rows are the ones the TypeScript library's shapeCause is tested with.
func TestFailureRecordShapesItsCause(t *testing.T) {
	data, err := os.ReadFile("../ts-observability/test-support/cause-shaping.json")
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Rows []struct{ Name, Cause, Want string }
	}
	if err := json.Unmarshal(data, &table); err != nil || len(table.Rows) == 0 {
		t.Fatalf("cause-shaping.json: %v, %d rows", err, len(table.Rows))
	}
	for _, row := range table.Rows {
		t.Run(row.Name, func(t *testing.T) {
			problems, records := failureRecords(t, func(l *slog.Logger) {
				WriteFailure(context.Background(), l, "dependency.failed", "unavailable", CauseOf(errors.New(row.Cause)))
			})
			if len(problems) != 0 || len(records) != 1 {
				t.Fatalf("problems = %v, records = %v", problems, records)
			}
			if got := records[0]["error.message"]; got != row.Want {
				t.Fatalf("error.message = %q; want %q", got, row.Want)
			}
		})
	}
}
