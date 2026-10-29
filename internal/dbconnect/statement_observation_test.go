package dbconnect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
)

type statementSample struct {
	count float64
	sum   float64
}

func statementSamples(t *testing.T, telemetry *workload.Telemetry, operation string) statementSample {
	t.Helper()
	metrics, _ := telemetry.Collector()(context.Background())
	var sample statementSample
	for _, metric := range metrics {
		var matches bool
		for _, label := range metric.Labels {
			if label.Name == "operation" && label.Value == operation {
				matches = true
			}
		}
		switch {
		case matches && metric.Name == "db_client_operation_duration_seconds_count":
			sample.count += metric.Value
		case matches && metric.Name == "db_client_operation_duration_seconds_sum":
			sample.sum += metric.Value
		}
	}
	return sample
}

// installStatementTelemetry installs the process logger and telemetry a
// service builds from its environment, with TETRAL_DB_SLOW_QUERY_MS set to
// slowQueryMs. records ends the process logger and returns what it wrote.
func installStatementTelemetry(t *testing.T, slowQueryMs string) (records func() string, telemetry *workload.Telemetry) {
	t.Helper()
	t.Setenv("TETRAL_DB_SLOW_QUERY_MS", slowQueryMs)
	t.Setenv("TETRAL_LOG_LEVEL", "info")
	cfg, err := workload.DiagnosticConfigFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	written := &bytes.Buffer{}
	owner := workload.NewProcessLogger(written, "api", "test", "unit", cfg)
	t.Cleanup(workload.InstallDefaultLogger(owner.Logger))
	t.Cleanup(owner.CloseWithBudget)
	return func() string {
		owner.CloseWithBudget()
		return written.String()
	}, owner.Telemetry
}

func TestSlowStatementWritesOneSlowRecordWithoutItsText(t *testing.T) {
	records, telemetry := installStatementTelemetry(t, "250")
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	if _, err := client.Exec(context.Background(), "statement.slow", "SELECT pg_sleep(0.3)"); err != nil {
		t.Fatal(err)
	}
	written := records()
	lines := strings.Split(strings.TrimSpace(written), "\n")
	if len(lines) != 1 {
		t.Fatalf("records = %q; want one db.query.slow", written)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record["event"] != "db.query.slow" || record["level"] != "WARN" || record["operation"] != "statement.slow" {
		t.Fatalf("slow record = %v", record)
	}
	if ms, _ := record["duration_ms"].(float64); ms < 300 {
		t.Fatalf("duration_ms = %v; want at least 300", record["duration_ms"])
	}
	if strings.Contains(written, "pg_sleep") || strings.Contains(written, "SELECT") {
		t.Fatalf("slow record carries the statement text: %s", written)
	}
	if sample := statementSamples(t, telemetry, "statement.slow"); sample.count != 1 || sample.sum <= 0.25 {
		t.Fatalf("slow statement samples = %+v; want one above 0.25 seconds", sample)
	}
}

func TestStatementWithinTheSlowQueryThresholdWritesNoRecord(t *testing.T) {
	for _, tc := range []struct{ name, slowQueryMs, query string }{
		{name: "fast statement", slowQueryMs: "250", query: "SELECT 1"},
		{name: "statement under a raised threshold", slowQueryMs: "1000", query: "SELECT 1 FROM pg_sleep(0.3)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, telemetry := installStatementTelemetry(t, tc.slowQueryMs)
			client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
			var one int
			if err := client.QueryRow(context.Background(), "statement.fast", tc.query).Scan(&one); err != nil {
				t.Fatal(err)
			}
			if written := records(); written != "" {
				t.Fatalf("a statement within the threshold wrote a record: %s", written)
			}
			if sample := statementSamples(t, telemetry, "statement.fast"); sample.count != 1 {
				t.Fatalf("statement samples = %+v; want one", sample)
			}
		})
	}
}

// The lock's acquisition is a sample of the same operation, so the statement
// on the lock-holding connection is counted from inside the callback.
func TestSessionLockStatementIsObservedLikeAnyStatement(t *testing.T) {
	records, telemetry := installStatementTelemetry(t, "250")
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	var before, after statementSample
	locked, err := client.TryWithSessionLock(context.Background(), "statement.session_lock", "statement_observation", func(conn *SessionLockConn) error {
		before = statementSamples(t, telemetry, "statement.session_lock")
		var one int
		if err := conn.QueryRow(context.Background(), "SELECT 1 FROM pg_sleep(0.3)").Scan(&one); err != nil {
			return err
		}
		after = statementSamples(t, telemetry, "statement.session_lock")
		return nil
	})
	if !locked || err != nil {
		t.Fatalf("TryWithSessionLock = %v, %v; want the lock held and the statement run", locked, err)
	}
	if after.count-before.count != 1 || after.sum-before.sum <= 0.25 {
		t.Fatalf("samples went from %+v to %+v; want one statement above 0.25 seconds", before, after)
	}
	written := records()
	var slow []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(written), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event"] == "db.query.slow" {
			slow = append(slow, record)
		}
	}
	if len(slow) != 1 || slow[0]["operation"] != "statement.session_lock" {
		t.Fatalf("records = %q; want one db.query.slow of the statement", written)
	}
}

func TestListenObservesNoStatementSample(t *testing.T) {
	_, telemetry := installStatementTelemetry(t, "250")
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	ctx, cancel := context.WithCancel(context.Background())
	ready := false
	err := client.Listen(ctx, "statement.listen", "statement_observation", func() { ready = true; cancel() }, func(string) {})
	if !ready || (err != nil && !errors.Is(err, context.Canceled)) {
		t.Fatalf("Listen = %v, ready %v; want a LISTEN that became ready and ended by cancellation", err, ready)
	}
	if sample := statementSamples(t, telemetry, "statement.listen"); sample.count != 0 {
		t.Fatalf("listen samples = %+v; want none", sample)
	}
}

func TestHeldLockObservesOnlyItsAcquisitionWait(t *testing.T) {
	_, telemetry := installStatementTelemetry(t, "250")
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	if err := client.WithAdvisoryLock(context.Background(), "statement.lock", 11, 12, func() error {
		time.Sleep(time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if sample := statementSamples(t, telemetry, "statement.lock"); sample.count != 1 || sample.sum >= 0.25 {
		t.Fatalf("lock samples = %+v; want one acquisition below 0.25 seconds", sample)
	}
}
