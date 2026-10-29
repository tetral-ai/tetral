package dbconnect

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
)

func queryObservation(t *testing.T, operation string) (context.Context, func(string, int)) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: "api", SampleRatio: 1, SyncExporter: exporter})
	t.Cleanup(workload.InstallTelemetry(telemetry))
	ctx, caller := workload.StartSpan(context.Background(), "query.caller")
	t.Cleanup(caller.End)
	return ctx, func(outcome string, count int) {
		t.Helper()
		metrics, _ := telemetry.Collector()(ctx)
		counts := map[string]float64{}
		for _, metric := range metrics {
			if metric.Name != "db_client_operation_duration_seconds_count" {
				continue
			}
			op, result := "", ""
			for _, label := range metric.Labels {
				if label.Name == "operation" {
					op = label.Value
				}
				if label.Name == "outcome" {
					result = label.Value
				}
			}
			if op == operation {
				counts[result] += metric.Value
			}
		}
		if counts[outcome] != float64(count) || counts["ok"]+counts["error"] != float64(count) {
			t.Errorf("observations = %v; want exactly %d %s", counts, count, outcome)
		}
		spans := 0
		for _, span := range exporter.GetSpans() {
			if span.Name != operation {
				continue
			}
			spans++
			if span.SpanContext.TraceID() != trace.SpanContextFromContext(ctx).TraceID() {
				t.Error("statement did not retain caller trace")
			}
			failed := false
			for _, attr := range span.Attributes {
				if attr.Key == "error.code" && attr.Value.AsString() == "statement_failed" {
					failed = true
				}
			}
			if failed != (outcome == "error") {
				t.Errorf("span attributes = %v; outcome %s", span.Attributes, outcome)
			}
		}
		if spans != count {
			t.Errorf("ended statement spans = %d; want %d", spans, count)
		}
	}
}

func TestQueryObservationUsesFinalResult(t *testing.T) {
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	for _, tc := range []struct {
		name           string
		tx, row        bool
		query, outcome string
	}{
		{name: "client rows deferred error"},
		{name: "transaction rows deferred error", tx: true},
		{name: "client row deferred error", row: true},
		{name: "transaction row deferred error", tx: true, row: true},
		{name: "immediate query error", query: "SELECT 1/0"},
		{name: "immediate row error", row: true, query: "SELECT 1/0"},
		{name: "no rows", row: true, query: "SELECT 1 WHERE false", outcome: "ok"},
		{name: "successful exhaustion", query: "SELECT generate_series(1,3)", outcome: "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const operation = "statement.final_result"
			ctx, assertObservation := queryObservation(t, operation)
			query := tc.query
			if query == "" {
				query = "SELECT i / (8192-i) FROM generate_series(1,8192) AS i"
			}
			outcome := tc.outcome
			if outcome == "" {
				outcome = "error"
			}
			var tx *Tx
			if tc.tx {
				raw, err := client.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer raw.Rollback()
				tx = NewTxForTesting(raw, client, operation)
			}
			var finalErr error
			if tc.row {
				var row *Row
				if tx == nil {
					row = client.QueryRow(ctx, operation, query)
				} else {
					row = tx.QueryRow(ctx, query)
				}
				finalErr = row.Scan(new(int))
			} else {
				var rows *Rows
				if tx == nil {
					rows, finalErr = client.Query(ctx, operation, query)
				} else {
					rows, finalErr = tx.Query(ctx, query)
				}
				if tc.query == "" && finalErr != nil {
					t.Fatalf("expected prefix rows before deferred error: %v", finalErr)
				}
				if finalErr == nil {
					defer rows.Close()
					prefix := 0
					for rows.Next() {
						if err := rows.Scan(new(int)); err != nil {
							t.Fatal(err)
						}
						prefix++
					}
					finalErr = rows.Err()
					if prefix == 0 {
						t.Fatal("expected a nonempty result prefix")
					}
					_ = rows.Close()
					_ = rows.Err()
				}
			}
			if tc.name == "no rows" {
				if !errors.Is(finalErr, sql.ErrNoRows) {
					t.Fatalf("final error = %v; want sql.ErrNoRows", finalErr)
				}
			} else if outcome == "error" {
				var pgErr *pgconn.PgError
				if !errors.As(finalErr, &pgErr) || pgErr.Code != "22012" {
					t.Fatalf("final error = %v; want SQLSTATE22012", finalErr)
				}
			} else if finalErr != nil {
				t.Fatal(finalErr)
			}
			assertObservation(outcome, 1)
		})
	}
}

func TestRowsObservationWaitsForCloseAfterNonterminalScanError(t *testing.T) {
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	const operation = "statement.close_result"
	ctx, assertObservation := queryObservation(t, operation)
	rows, err := client.Query(ctx, operation, "SELECT 'not-an-integer' FROM generate_series(1,3)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("missing row: %v", rows.Err())
	}
	if err := rows.Scan(new(int)); err == nil {
		t.Fatal("conversion should fail")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	assertObservation("ok", 0)
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	assertObservation("ok", 1)
	_ = rows.Err()
	_ = rows.Close()
	assertObservation("ok", 1)
}

func TestRowObservationWaitsForScan(t *testing.T) {
	client := NewClientForTesting(storagetest.NewPostgreSQLDB(t))
	const operation = "statement.scan_result"
	ctx, assertObservation := queryObservation(t, operation)
	row := client.QueryRow(ctx, operation, "SELECT 1")
	assertObservation("ok", 0)
	if err := row.Scan(new(int)); err != nil {
		t.Fatal(err)
	}
	assertObservation("ok", 1)
	_ = row.Scan(new(int))
	assertObservation("ok", 1)
}
