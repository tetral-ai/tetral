package dbconnect

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestQueryRowScanClassifiesContextOperationalErrors(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := client.QueryRow(ctx, "dbconnect.queryrow_canceled", `SELECT 1`).Scan(new(int))
		diagnostic := assertDiagnostic(t, err, PhaseRuntimeQuery, KindCanceled)
		if diagnostic.Operation != "dbconnect.queryrow_canceled" {
			t.Fatalf("operation = %q; want dbconnect.queryrow_canceled", diagnostic.Operation)
		}
		if cause := context.Cause(ctx); !errors.Is(err, cause) {
			t.Fatalf("QueryRow Scan diagnostic must unwrap context cause %v, got %v", cause, diagnostic.Unwrap())
		}
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()

		err := client.QueryRow(ctx, "dbconnect.queryrow_timeout", `SELECT 1 FROM pg_sleep(1)`).Scan(new(int))
		diagnostic := assertDiagnostic(t, err, PhaseRuntimeQuery, KindTimeout)
		if diagnostic.Operation != "dbconnect.queryrow_timeout" {
			t.Fatalf("operation = %q; want dbconnect.queryrow_timeout", diagnostic.Operation)
		}
		if cause := context.Cause(ctx); !errors.Is(err, cause) {
			t.Fatalf("QueryRow Scan diagnostic must unwrap context cause %v, got %v", cause, diagnostic.Unwrap())
		}
	})
}

func TestQueryRowScanPreservesErrNoRows(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	err := client.QueryRow(context.Background(), "dbconnect.queryrow_no_rows", `SELECT 1 WHERE false`).Scan(new(int))
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("QueryRow Scan no rows err = %T %v; want sql.ErrNoRows", err, err)
	}
	var diagnostic *DiagnosticError
	if errors.As(err, &diagnostic) {
		t.Fatalf("sql.ErrNoRows must not be classified as DiagnosticError: %+v", diagnostic)
	}
}
