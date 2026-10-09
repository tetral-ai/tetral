package agentruntimebridge

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func lockPostgreSQLFinalizationFence(t *testing.T, db *sql.DB, query string, args ...any) (*sql.Tx, int) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin finalization fence transaction: %v", err)
	}
	var lockedID string
	if err := tx.QueryRowContext(context.Background(), query, args...).Scan(&lockedID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock finalization fence row: %v", err)
	}
	var backendPID int
	if err := tx.QueryRowContext(context.Background(), `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("read finalization fence backend: %v", err)
	}
	return tx, backendPID
}

func waitForPostgreSQLLockWaiters(t *testing.T, db *sql.DB, blockerPID int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		if err := db.QueryRowContext(context.Background(),
			`WITH RECURSIVE waiters(pid) AS (
				SELECT pid
				  FROM pg_stat_activity
				 WHERE $1 = ANY(pg_blocking_pids(pid))
				UNION
				SELECT activity.pid
				  FROM pg_stat_activity activity
				  JOIN waiters blocker ON blocker.pid = ANY(pg_blocking_pids(activity.pid))
			)
			SELECT count(*) FROM waiters`,
			blockerPID,
		).Scan(&waiters); err != nil {
			t.Fatalf("read PostgreSQL finalization waiters: %v", err)
		}
		if waiters >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PostgreSQL finalization waiters did not reach %d for blocker %d", want, blockerPID)
}
