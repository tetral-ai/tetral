package sessionevent

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// An admission without an Idempotency-Key header never reads or writes the
// receipt table, so two identical requests are two admissions. A supplied key
// on the same traced pool does reach the table, proving the trace observes it.
func TestAppendWithoutIdempotencyKeyReadsAndWritesNoReceipt(t *testing.T) {
	fixture := newIdempotencyRetentionFixture(t)
	tracer := &statementRecorder{}
	service := newSessionEventServiceForTest(fixture.workload.OpenWorkload(t, "api", tracer))
	sessionID := fixture.seedSession(t, "sesn_no_idempotency_key")

	for range 2 {
		if _, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, NoIdempotencyKey, messageAppendRequest("same body")); err != nil {
			t.Fatalf("AppendClientEvents without a key: %v", err)
		}
	}
	if touched := tracer.matching("session_event_idempotency_keys"); len(touched) != 0 {
		t.Fatalf("admission without a key touched receipts: %q", touched)
	}
	if got := len(readSessionEventLedgerRows(t, fixture.admin, sessionID)); got != 2 {
		t.Fatalf("ledger rows = %d; want two independent admissions", got)
	}
	if got := len(readSessionEventQueueJobs(t, fixture.admin, sessionID)); got != 2 {
		t.Fatalf("queue jobs = %d; want two", got)
	}
	assertSessionEventIdempotencyRowCount(t, fixture.admin, sessionID, 0)

	if _, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_supplied", messageAppendRequest("same body")); err != nil {
		t.Fatal(err)
	}
	if touched := tracer.matching("session_event_idempotency_keys"); len(touched) == 0 {
		t.Fatal("trace did not observe the supplied key's receipt statements")
	}
	assertSessionEventIdempotencyRowCount(t, fixture.admin, sessionID, 1)
}

// A receipt is live precisely while the admission time is before created_at
// plus 24 hours. A live replay returns the stored response without moving the
// receipt's timestamps; an expired receipt, still present because retention
// has not run, no longer deduplicates or conflicts and is replaced with
// created_at = updated_at = the database admission time, not the service clock.
func TestIdempotencyReceiptExpiresAtTheDatabaseAdmissionTime(t *testing.T) {
	fixture := newIdempotencyRetentionFixture(t)
	serviceClock := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	service := newSessionEventServiceForTest(fixture.workload.DB, WithClock(func() time.Time { return serviceClock }))
	sessionID := fixture.seedSession(t, "sesn_receipt_expiry")
	const key = "idem_receipt_expiry"

	first, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first"))
	if err != nil {
		t.Fatal(err)
	}
	live := fixture.ageReceipt(t, sessionID, "interval '23 hours 59 minutes'")
	replay, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first"))
	if err != nil || replay.Data[0].ID != first.Data[0].ID {
		t.Fatalf("live replay = %+v/%v; want the stored response", replay, err)
	}
	var conflict *ConflictError
	if _, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("different")); !errors.As(err, &conflict) {
		t.Fatalf("live different request = %v; want conflict", err)
	}
	if createdAt, updatedAt := fixture.receiptTimes(t, sessionID); !createdAt.Equal(live) || !updatedAt.Equal(live) {
		t.Fatalf("live replay moved receipt times to %s/%s; want %s", createdAt, updatedAt, live)
	}

	expired := fixture.ageReceipt(t, sessionID, "interval '24 hours 1 second'")
	before := dbNow(t, fixture.admin)
	renewed, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("different"))
	after := dbNow(t, fixture.admin)
	if err != nil || renewed.Data[0].ID == first.Data[0].ID {
		t.Fatalf("admission over an expired receipt = %+v/%v; want a new admission", renewed, err)
	}
	createdAt, updatedAt := fixture.receiptTimes(t, sessionID)
	if !createdAt.Equal(updatedAt) || createdAt.Before(before) || createdAt.After(after) || !createdAt.After(expired) {
		t.Fatalf("replacement receipt times %s/%s; want equal database admission time in [%s, %s]", createdAt, updatedAt, before, after)
	}
	if got := len(readSessionEventLedgerRows(t, fixture.admin, sessionID)); got != 2 {
		t.Fatalf("ledger rows = %d; want the original and the replacement admission", got)
	}
	assertSessionEventIdempotencyRowCount(t, fixture.admin, sessionID, 1)
	again, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("different"))
	if err != nil || again.Data[0].ID != renewed.Data[0].ID {
		t.Fatalf("replay of the replacement = %+v/%v", again, err)
	}
}

// The admission time is read after the receipt lock is granted. A receipt
// that the lock holder ages to expiry while the admission waits is expired for
// that admission.
func TestIdempotencyAdmissionTimeIsReadAfterTheReceiptLockWait(t *testing.T) {
	fixture := newIdempotencyRetentionFixture(t)
	service := newSessionEventServiceForTest(fixture.workload.DB)
	sessionID := fixture.seedSession(t, "sesn_receipt_lock_wait")
	const key = "idem_receipt_lock_wait"
	first, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first"))
	if err != nil {
		t.Fatal(err)
	}
	holder, err := fixture.admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec(`SELECT 1 FROM session_event_idempotency_keys WHERE session_id = $1 FOR UPDATE`, sessionID); err != nil {
		t.Fatal(err)
	}
	done := make(chan appendOutcomeForTest, 1)
	go func() {
		result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first"))
		done <- appendOutcomeForTest{result: result, err: err}
	}()
	waitForStatementLockWait(t, fixture.admin, "session_event_idempotency_keys")
	// While the admission waits, the holder ages the receipt to exactly 24
	// hours before the holder's own clock and commits. Any admission clock
	// read after the lock is later, so the receipt is expired; a clock read
	// before the lock wait would still see it live and replay.
	if _, err := holder.Exec(`UPDATE session_event_idempotency_keys k SET created_at = aged.at, updated_at = aged.at
		FROM (SELECT clock_timestamp() - interval '24 hours' AS at) aged WHERE k.session_id = $1`, sessionID); err != nil {
		t.Fatal(err)
	}
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	outcome := <-done
	if outcome.err != nil || outcome.result.Data[0].ID == first.Data[0].ID {
		t.Fatalf("admission after the lock wait = %+v/%v; want a new admission past expiry", outcome.result, outcome.err)
	}
}

// Replacing an expired receipt is part of the admission transaction: a failed
// admission restores the expired receipt and writes no event.
func TestExpiredReceiptReplacementRollsBackWithItsAdmission(t *testing.T) {
	fixture := newIdempotencyRetentionFixture(t)
	sessionID := fixture.seedSession(t, "sesn_receipt_rollback")
	const key = "idem_receipt_rollback"
	if _, err := newSessionEventServiceForTest(fixture.workload.DB).AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first")); err != nil {
		t.Fatal(err)
	}
	expired := fixture.ageReceipt(t, sessionID, "interval '25 hours'")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(fixture.workload.DB))
	store.beforeIdempotencyInsert = func() error { return errors.New("injected receipt failure") }
	if _, err := NewService(store).AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("second")); err == nil {
		t.Fatal("admission succeeded despite the injected receipt failure")
	}
	if createdAt, _ := fixture.receiptTimes(t, sessionID); !createdAt.Equal(expired) {
		t.Fatalf("receipt created_at = %s after rollback; want the expired %s", createdAt, expired)
	}
	if got := len(readSessionEventLedgerRows(t, fixture.admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows = %d; want only the original", got)
	}
}

// Receipt replacement and Cleanup's receipt retention serialize on the receipt
// row. When the pruner holds it first, admission waits, then finds no receipt
// and admits anew. When admission holds it first, the pruner skips it, and the
// replacement's new timestamp keeps it out of later pruning.
func TestIdempotencyReplacementAndRetentionInEitherOrder(t *testing.T) {
	t.Run("PrunerFirst", func(t *testing.T) {
		fixture := newIdempotencyRetentionFixture(t)
		service := newSessionEventServiceForTest(fixture.workload.DB)
		sessionID := fixture.seedSession(t, "sesn_receipt_pruner_first")
		const key = "idem_pruner_first"
		first, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first"))
		if err != nil {
			t.Fatal(err)
		}
		fixture.ageReceipt(t, sessionID, "interval '25 hours'")
		pruner, err := fixture.cleanup.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pruner.Rollback() }()
		var deleted int
		if err := pruner.QueryRow(`SELECT deleted_count FROM public.tetral_prune_event_idempotency(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`).Scan(&deleted); err != nil || deleted != 1 {
			t.Fatalf("pruner = %d/%v; want the expired receipt", deleted, err)
		}
		done := make(chan appendOutcomeForTest, 1)
		go func() {
			result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first"))
			done <- appendOutcomeForTest{result: result, err: err}
		}()
		waitForStatementLockWait(t, fixture.admin, "session_event_idempotency_keys")
		committedAt := dbNow(t, fixture.admin)
		if err := pruner.Commit(); err != nil {
			t.Fatal(err)
		}
		outcome := <-done
		if outcome.err != nil || outcome.result.Data[0].ID == first.Data[0].ID {
			t.Fatalf("admission after pruning = %+v/%v; want a new admission", outcome.result, outcome.err)
		}
		if createdAt, _ := fixture.receiptTimes(t, sessionID); createdAt.Before(committedAt) {
			t.Fatalf("new receipt created_at %s precedes the pruner commit %s", createdAt, committedAt)
		}
	})

	t.Run("ReplacementFirst", func(t *testing.T) {
		fixture := newIdempotencyRetentionFixture(t)
		sessionID := fixture.seedSession(t, "sesn_receipt_replacement_first")
		const key = "idem_replacement_first"
		if _, err := newSessionEventServiceForTest(fixture.workload.DB).AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("first")); err != nil {
			t.Fatal(err)
		}
		fixture.ageReceipt(t, sessionID, "interval '25 hours'")
		store := NewPostgreSQLStore(dbconnect.NewClientForTesting(fixture.workload.DB))
		entered, release := make(chan struct{}), make(chan struct{})
		store.beforeIdempotencyInsert = func() error {
			close(entered)
			<-release
			return nil
		}
		done := make(chan appendOutcomeForTest, 1)
		go func() {
			result, err := NewService(store).AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, key, messageAppendRequest("second"))
			done <- appendOutcomeForTest{result: result, err: err}
		}()
		<-entered
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var examined int
		if err := fixture.cleanup.QueryRowContext(ctx, `SELECT examined_count FROM public.tetral_prune_event_idempotency(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`).Scan(&examined); err != nil || examined != 0 {
			t.Fatalf("pruner during replacement = %d/%v; want the held receipt skipped without waiting", examined, err)
		}
		close(release)
		if outcome := <-done; outcome.err != nil {
			t.Fatal(outcome.err)
		}
		if err := fixture.cleanup.QueryRow(`SELECT examined_count FROM public.tetral_prune_event_idempotency(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`).Scan(&examined); err != nil || examined != 0 {
			t.Fatalf("pruner after replacement = %d/%v; want the renewed receipt kept", examined, err)
		}
		assertSessionEventIdempotencyRowCount(t, fixture.admin, sessionID, 1)
		if got := len(readSessionEventLedgerRows(t, fixture.admin, sessionID)); got != 2 {
			t.Fatalf("ledger rows = %d; want the original and the replacement", got)
		}
	})
}

type idempotencyRetentionFixture struct {
	admin    *sql.DB
	workload *storagetest.WorkloadDB
	cleanup  *sql.DB
}

func newIdempotencyRetentionFixture(t *testing.T) idempotencyRetentionFixture {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "api")
	return idempotencyRetentionFixture{admin: admin, workload: workload, cleanup: workload.OpenWorkload(t, "cleanup", nil)}
}

func (f idempotencyRetentionFixture) seedSession(t *testing.T, sessionID string) string {
	t.Helper()
	seedSessionEventSession(t, f.admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, f.admin, workspace.DefaultID, sessionID)
	return sessionID
}

// ageReceipt moves the Session's only receipt to the database clock minus age
// and returns the stored timestamp.
func (f idempotencyRetentionFixture) ageReceipt(t *testing.T, sessionID, age string) time.Time {
	t.Helper()
	var createdAt time.Time
	//nolint:gosec // G202: age is a fixed interval literal from the test.
	if err := f.admin.QueryRow(`UPDATE session_event_idempotency_keys k SET created_at = aged.at, updated_at = aged.at
		FROM (SELECT clock_timestamp() - `+age+` AS at) aged WHERE k.session_id = $1 RETURNING k.created_at`, sessionID).Scan(&createdAt); err != nil {
		t.Fatalf("age receipt: %v", err)
	}
	return createdAt
}

func (f idempotencyRetentionFixture) receiptTimes(t *testing.T, sessionID string) (time.Time, time.Time) {
	t.Helper()
	var createdAt, updatedAt time.Time
	if err := f.admin.QueryRow(`SELECT created_at, updated_at FROM session_event_idempotency_keys WHERE session_id = $1`, sessionID).Scan(&createdAt, &updatedAt); err != nil {
		t.Fatalf("read receipt times: %v", err)
	}
	return createdAt, updatedAt
}

type appendOutcomeForTest struct {
	result *AppendResult
	err    error
}

type statementRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *statementRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	r.statements = append(r.statements, data.SQL)
	r.mu.Unlock()
	return ctx
}

func (*statementRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *statementRecorder) matching(fragment string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var matched []string
	for _, statement := range r.statements {
		if strings.Contains(statement, fragment) {
			matched = append(matched, statement)
		}
	}
	return matched
}

func dbNow(t *testing.T, db *sql.DB) time.Time {
	t.Helper()
	return dbTimeExpression(t, db, "clock_timestamp()")
}

func dbTimeExpression(t *testing.T, db *sql.DB, expression string) time.Time {
	t.Helper()
	var value time.Time
	if err := db.QueryRow(`SELECT ` + expression).Scan(&value); err != nil {
		t.Fatalf("read database time: %v", err)
	}
	return value
}

// waitForStatementLockWait blocks until another backend whose current
// statement mentions fragment waits on a lock.
func waitForStatementLockWait(t *testing.T, admin *sql.DB, fragment string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%' || $1 || '%' AND pid <> pg_backend_pid())`, fragment).Scan(&waiting); err != nil {
			t.Fatalf("wait for a lock wait on %s: %v", fragment, err)
		}
		if waiting {
			return
		}
	}
}
