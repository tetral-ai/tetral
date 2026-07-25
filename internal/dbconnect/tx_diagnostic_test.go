package dbconnect

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestWithTxExecClassifiesRuntimeDiagnostics(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	for _, testCase := range []struct {
		name      string
		operation string
		kind      Kind
		run       func(testing.TB, *Tx) (context.Context, error)
	}{
		{
			name:      "canceled",
			operation: "dbconnect.tx_exec_canceled",
			kind:      KindCanceled,
			run: func(_ testing.TB, tx *Tx) (context.Context, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := tx.Exec(ctx, `SELECT 1`)
				return ctx, err
			},
		},
		{
			name:      "exec_context_canceled",
			operation: "dbconnect.tx_exec_context_canceled",
			kind:      KindCanceled,
			run: func(_ testing.TB, tx *Tx) (context.Context, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := tx.ExecContext(ctx, `SELECT 1`)
				return ctx, err
			},
		},
		{
			name:      "timeout",
			operation: "dbconnect.tx_exec_timeout",
			kind:      KindTimeout,
			run: func(t testing.TB, tx *Tx) (context.Context, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
				t.Cleanup(cancel)
				_, err := tx.Exec(ctx, `SELECT pg_sleep(1)`)
				return ctx, err
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var runtimeContext context.Context
			err := client.WithTx(context.Background(), testCase.operation, nil, func(tx *Tx) error {
				var execErr error
				runtimeContext, execErr = testCase.run(t, tx)
				return execErr
			})
			assertTransactionRuntimeDiagnostic(runtimeContext, t, err, testCase.operation, testCase.kind)
		})
	}
}

func TestWithTxQueryClassifiesRuntimeDiagnostics(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	t.Run("query canceled", func(t *testing.T) {
		operation := "dbconnect.tx_query_canceled"
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := client.WithTx(context.Background(), operation, nil, func(tx *Tx) error {
			_, queryErr := tx.Query(ctx, `SELECT 1`)
			return queryErr
		})
		assertTransactionRuntimeDiagnostic(ctx, t, err, operation, KindCanceled)
	})

	t.Run("query timeout", func(t *testing.T) {
		operation := "dbconnect.tx_query_timeout"
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		t.Cleanup(cancel)

		err := client.WithTx(context.Background(), operation, nil, func(tx *Tx) error {
			_, queryErr := tx.Query(ctx, `SELECT 1`)
			return queryErr
		})
		assertTransactionRuntimeDiagnostic(ctx, t, err, operation, KindTimeout)
	})

	t.Run("query rows canceled", func(t *testing.T) {
		operation := "dbconnect.tx_query_rows_canceled"
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := client.WithTx(context.Background(), operation, nil, func(tx *Tx) error {
			_, queryErr := tx.QueryRows(ctx, `SELECT 1`)
			return queryErr
		})
		assertTransactionRuntimeDiagnostic(ctx, t, err, operation, KindCanceled)
	})

	t.Run("rows err canceled", func(t *testing.T) {
		operation := "dbconnect.tx_rows_err_canceled"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := client.WithTx(context.Background(), operation, nil, func(tx *Tx) error {
			rows, queryErr := tx.Query(ctx, `SELECT generate_series(1, 100000)`)
			if queryErr != nil {
				return queryErr
			}
			defer func() { _ = rows.Close() }()
			if !rows.Next() {
				return errors.New("tx.Query returned no first row before cancellation")
			}
			cancel()
			for rows.Next() {
			}
			return rows.Err()
		})
		assertTransactionRuntimeDiagnostic(ctx, t, err, operation, KindCanceled)
	})
}

func TestWithTxQueryRowScanClassifiesRuntimeDiagnostics(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	for _, testCase := range []struct {
		name      string
		operation string
		kind      Kind
		run       func(testing.TB, *Tx) (context.Context, error)
	}{
		{
			name:      "canceled",
			operation: "dbconnect.tx_queryrow_canceled",
			kind:      KindCanceled,
			run: func(_ testing.TB, tx *Tx) (context.Context, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := tx.QueryRow(ctx, `SELECT 1`).Scan(new(int))
				return ctx, err
			},
		},
		{
			name:      "queryrow_scanner_canceled",
			operation: "dbconnect.tx_queryrow_scanner_canceled",
			kind:      KindCanceled,
			run: func(_ testing.TB, tx *Tx) (context.Context, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := tx.QueryRowScanner(ctx, `SELECT 1`).Scan(new(int))
				return ctx, err
			},
		},
		{
			name:      "timeout",
			operation: "dbconnect.tx_queryrow_timeout",
			kind:      KindTimeout,
			run: func(t testing.TB, tx *Tx) (context.Context, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
				t.Cleanup(cancel)
				err := tx.QueryRow(ctx, `SELECT 1 FROM pg_sleep(1)`).Scan(new(int))
				return ctx, err
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var runtimeContext context.Context
			err := client.WithTx(context.Background(), testCase.operation, nil, func(tx *Tx) error {
				var scanErr error
				runtimeContext, scanErr = testCase.run(t, tx)
				return scanErr
			})
			assertTransactionRuntimeDiagnostic(runtimeContext, t, err, testCase.operation, testCase.kind)
		})
	}
}

func TestWithWorkspaceTxClassifiesTransactionRuntimeDiagnostics(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)
	operation := "dbconnect.scoped_tx_queryrow_canceled"

	var runtimeContext context.Context
	err := client.WithWorkspaceTx(context.Background(), "default", operation, func(tx *Tx) error {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runtimeContext = ctx
		return tx.QueryRow(ctx, `SELECT 1`).Scan(new(int))
	})
	assertTransactionRuntimeDiagnostic(runtimeContext, t, err, operation, KindCanceled)
}

func assertTransactionRuntimeDiagnostic(runtimeContext context.Context, t *testing.T, err error, operation string, kind Kind) {
	t.Helper()
	diagnostic := assertDiagnostic(t, err, PhaseRuntimeQuery, kind)
	if diagnostic.Operation != operation {
		t.Fatalf("operation = %q; want %q", diagnostic.Operation, operation)
	}
	if runtimeContext == nil {
		t.Fatal("test did not execute a transaction-scoped runtime operation")
	}
	cause := context.Cause(runtimeContext)
	if cause == nil {
		t.Fatal("test did not capture a context cancellation cause")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("diagnostic must unwrap context cause %v, got %v", cause, diagnostic.Unwrap())
	}
}
