package tetralcleanup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// Receipt pruning pages in (created_at, workspace_id, session_id, digest)
// order across Workspaces, includes a receipt exactly at the cutoff, keeps one
// a microsecond younger, caps a future cutoff at the database clock minus 24
// hours, clamps the page limit and rejects invalid arguments.
func TestIdempotencyRetentionFunctionPagesAtTheExactCutoff(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedRetentionSession(t, admin, "default", "sesn_receipts")
	seedRetentionSession(t, admin, "ws_receipts_other", "sesn_receipts_other")
	cutoff := dbTime(t, admin, "date_trunc('second', clock_timestamp()) - interval '25 hours'")
	seedReceipt(t, admin, "default", "sesn_receipts", "a", cutoff.Add(-2*time.Second))
	seedReceipt(t, admin, "default", "sesn_receipts", "b", cutoff.Add(-time.Second))
	seedReceipt(t, admin, "ws_receipts_other", "sesn_receipts_other", "e", cutoff.Add(-time.Second))
	seedReceipt(t, admin, "default", "sesn_receipts", "c", cutoff)
	seedReceipt(t, admin, "default", "sesn_receipts", "d", cutoff.Add(time.Microsecond))
	seedReceipt(t, admin, "default", "sesn_receipts", "f", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"))

	first := pruneReceiptsAs(t, w.DB, cutoff, nil, 2)
	if first.deleted != 2 || first.examined != 2 || first.lastDigest != "b" || first.lastWorkspace != "default" || !first.more {
		t.Fatalf("first page = %+v; want a, b and more eligible rows", first)
	}
	if got := receiptKeys(t, admin); got != "c d e f" {
		t.Fatalf("receipts after first page = %q", got)
	}
	second := pruneReceiptsAs(t, w.DB, cutoff, &first, 256)
	if second.deleted != 2 || second.examined != 2 || second.lastDigest != "c" || second.more {
		t.Fatalf("continuation page = %+v; want e, c and nothing more at this cutoff", second)
	}
	if got := receiptKeys(t, admin); got != "d f" {
		t.Fatalf("receipts after the cutoff page = %q; want only the younger receipts", got)
	}

	future := dbTime(t, admin, "clock_timestamp() + interval '1 hour'")
	if capped := pruneReceiptsAs(t, w.DB, future, nil, 256); capped.examined != 1 || capped.lastDigest != "d" || capped.more {
		t.Fatalf("future cutoff page = %+v; want only d, capped at the database clock minus 24 hours", capped)
	}
	if got := receiptKeys(t, admin); got != "f" {
		t.Fatalf("future cutoff deleted a receipt younger than 24 hours: %q", got)
	}

	if _, err := admin.Exec(`INSERT INTO session_event_idempotency_keys (workspace_id, session_id, idempotency_key_digest, canonical_request_hash, response_events_json, created_at, updated_at)
		SELECT 'default', 'sesn_receipts', convert_to('bulk' || n, 'UTF8'), '\x00', '[]', $1, $1 FROM generate_series(1, 257) n`, cutoff); err != nil {
		t.Fatal(err)
	}
	if empty := pruneReceiptsAs(t, w.DB, cutoff, nil, 0); empty.examined != 0 || empty.deleted != 0 || !empty.more {
		t.Fatalf("limit 0 page = %+v; want nothing examined and eligible rows reported", empty)
	}
	if clamped := pruneReceiptsAs(t, w.DB, cutoff, nil, 1000); clamped.examined != 256 || clamped.deleted != 256 || !clamped.more {
		t.Fatalf("limit 1000 page = %+v; want 256 and one more", clamped)
	}
	if last := pruneReceiptsAs(t, w.DB, cutoff, nil, 1); last.examined != 1 || last.more {
		t.Fatalf("last eligible receipt = %+v; want nothing more", last)
	}

	for name, args := range map[string][]any{
		"null cutoff":       {nil, nil, nil, nil, nil, 1},
		"time only":         {cutoff, cutoff, nil, nil, nil, 1},
		"missing digest":    {cutoff, cutoff, "default", "sesn_receipts", nil, 1},
		"empty workspace":   {cutoff, cutoff, "", "sesn_receipts", []byte("a"), 1},
		"empty session":     {cutoff, cutoff, "default", "", []byte("a"), 1},
		"empty digest":      {cutoff, cutoff, "default", "sesn_receipts", []byte{}, 1},
		"digest only":       {cutoff, nil, nil, nil, []byte("a"), 1},
		"null limit":        {cutoff, nil, nil, nil, nil, nil},
		"missing timestamp": {cutoff, nil, "default", "sesn_receipts", []byte("a"), 1},
	} {
		_, err := w.DB.ExecContext(ctx, `SELECT * FROM public.tetral_prune_event_idempotency($1, $2, $3, $4, $5, $6)`, args...)
		if code := sqlState(err); code != "22023" {
			t.Fatalf("%s: SQLSTATE %q (%v); want invalid_parameter_value", name, code, err)
		}
	}
	for name, args := range map[string][]any{
		"null cutoff":     {nil, nil, nil, nil, nil, 1},
		"partial":         {cutoff, cutoff, "default", nil, nil, 1},
		"zero position":   {cutoff, cutoff, "default", "sesn_receipts", int64(0), 1},
		"empty session":   {cutoff, cutoff, "default", "", int64(1), 1},
		"position only":   {cutoff, nil, nil, nil, int64(1), 1},
		"null limit":      {cutoff, nil, nil, nil, nil, nil},
		"empty workspace": {cutoff, cutoff, "", "sesn_receipts", int64(1), 1},
	} {
		_, err := w.DB.ExecContext(ctx, `SELECT * FROM public.tetral_prune_event_changes($1, $2, $3, $4, $5, $6)`, args...)
		if code := sqlState(err); code != "22023" {
			t.Fatalf("changes %s: SQLSTATE %q (%v); want invalid_parameter_value", name, code, err)
		}
	}
}

// Only rows the pruning statement actually deleted advance a watermark, and
// only for a feed whose reader would have returned them. Watermarks never move
// backward, and a rolled-back prune leaves rows and watermarks untouched.
func TestChangeRetentionAdvancesOnlyEligibleFeedWatermarks(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	old := dbTime(t, admin, "clock_timestamp() - interval '2 days'")
	seedRetentionSession(t, admin, "default", "sesn_scope")
	seedRetentionThread(t, admin, "default", "sesn_scope", "thr_child", "subagent", "public")
	seedRetentionThread(t, admin, "default", "sesn_scope", "thr_private", "subagent", "internal")
	seedRetentionThread(t, admin, "default", "sesn_scope", "thr_reviewer", "approval_reviewer", "public")
	seedRetentionSession(t, admin, "default", "sesn_scope_other")

	sessionLevel := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", event: "evt_session", changedAt: old})
	mainRow := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", thread: "thr_sesn_scope", event: "evt_main", changedAt: old})
	hidden := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", thread: "thr_child", event: "evt_hidden", sessionHidden: true, changedAt: old})
	seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", thread: "thr_sesn_scope", event: "evt_internal_change", internalChange: true, changedAt: old})
	seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", thread: "thr_sesn_scope", event: "evt_internal_event", internalEvent: true, changedAt: old})
	seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", thread: "thr_private", event: "evt_private", changedAt: old})
	seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope", thread: "thr_reviewer", event: "evt_reviewer", changedAt: old})
	otherSession := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_scope_other", thread: "thr_sesn_scope_other", event: "evt_other", changedAt: old})
	if sessionLevel >= mainRow || mainRow >= hidden || hidden >= otherSession {
		t.Fatalf("fixture positions are not increasing: %d %d %d %d", sessionLevel, mainRow, hidden, otherSession)
	}
	// An existing higher watermark stays; a lower one is raised.
	if _, err := admin.Exec(`INSERT INTO session_event_feed_retention (workspace_id, session_id, feed_key, session_thread_id, pruned_through) VALUES
		('default', 'sesn_scope', 'thread:thr_child', 'thr_child', 1000000000),
		('default', 'sesn_scope', 'thread:thr_sesn_scope', 'thr_sesn_scope', 1)`); err != nil {
		t.Fatal(err)
	}
	cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")
	before := feedWatermarks(t, admin)

	tx, err := w.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SELECT * FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 256)`, cutoff); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if after := feedWatermarks(t, admin); after != before || changeCount(t, admin) != 8 {
		t.Fatalf("rolled-back prune changed state: watermarks %q -> %q, changes %d", before, after, changeCount(t, admin))
	}

	page := pruneChangesAs(t, w.DB, cutoff, 256)
	if page.deleted != 8 || page.examined != 8 || page.more {
		t.Fatalf("prune page = %+v; want all 8 rows", page)
	}
	want := fmt.Sprintf("default/sesn_scope/session=%d default/sesn_scope/thread:thr_child=1000000000 default/sesn_scope/thread:thr_sesn_scope=%d default/sesn_scope_other/session=%d default/sesn_scope_other/thread:thr_sesn_scope_other=%d",
		mainRow, mainRow, otherSession, otherSession)
	if got := feedWatermarks(t, admin); got != want {
		t.Fatalf("watermarks = %q\nwant %q", got, want)
	}
}

// Overlapping pruners never wait for each other's locked candidates, and a
// pruner that deletes older positions after another committed newer ones does
// not move the shared watermark backward.
func TestOverlappingChangePrunersSkipLockedPagesAndKeepTheGreatestWatermark(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	second := w.OpenWorkload(t, "cleanup", nil)
	seedRetentionSession(t, admin, "default", "sesn_overlap_a")
	seedRetentionSession(t, admin, "default", "sesn_overlap_b")
	oldest := dbTime(t, admin, "clock_timestamp() - interval '3 days'")
	cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")

	t.Run("DisjointFeedsDoNotWait", func(t *testing.T) {
		seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_overlap_a", event: "evt_a1", changedAt: oldest})
		bPosition := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_overlap_b", event: "evt_b1", changedAt: oldest.Add(time.Second)})
		holder, err := w.DB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback() }()
		var held int
		if err := holder.QueryRow(`SELECT examined_count FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 1)`, cutoff).Scan(&held); err != nil || held != 1 {
			t.Fatalf("holder page = %d/%v", held, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var examined int
		if err := second.QueryRowContext(ctx, `SELECT examined_count FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 256)`, cutoff).Scan(&examined); err != nil || examined != 1 {
			t.Fatalf("overlapping pruner = %d/%v; want it to skip the held row and finish", examined, err)
		}
		if got := watermark(t, admin, "sesn_overlap_b", "session"); got != bPosition {
			t.Fatalf("overlapping pruner watermark = %d; want %d", got, bPosition)
		}
		if err := holder.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("SameFeedKeepsGreatest", func(t *testing.T) {
		low := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_overlap_a", event: "evt_a_low", changedAt: oldest.Add(2 * time.Second)})
		high := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_overlap_a", event: "evt_a_high", changedAt: oldest.Add(time.Second)})
		holder, err := w.DB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback() }()
		if _, err := holder.Exec(`SELECT * FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 1)`, cutoff); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := second.Exec(`SELECT * FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 256)`, cutoff)
			done <- err
		}()
		waitForLockWait(t, admin, "tetral_prune_event_changes")
		if err := holder.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if got := watermark(t, admin, "sesn_overlap_a", "session"); got != high || low >= high {
			t.Fatalf("watermark = %d; want the higher committed position %d (low %d)", got, high, low)
		}
	})
}

// The first watermark INSERT for a feed takes FK KEY SHARE locks on its
// permanent Session parent. A transaction holding that Session FOR UPDATE makes
// the pruner wait; at its two-second batch deadline the whole batch rolls back,
// deletion included, and a later batch commits both together.
func TestChangeRetentionWaitsOnTheSessionParentAndRollsBackAtItsDeadline(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedRetentionSession(t, admin, "default", "sesn_parent_lock")
	position := seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_parent_lock", event: "evt_parent_lock", changedAt: dbTime(t, admin, "clock_timestamp() - interval '2 days'")})
	cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")
	locker, err := admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Rollback() }()
	if _, err := locker.Exec(`SELECT 1 FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_parent_lock' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	retention := NewRetention(dbconnect.NewClientForTesting(w.DB))
	result, err := retention.PruneStreamChanges(context.Background(), cutoff)
	if err == nil || result.Batches != 0 {
		t.Fatalf("prune behind a held Session = %+v/%v; want a failed first batch", result, err)
	}
	if changeCount(t, admin) != 1 || feedWatermarks(t, admin) != "" {
		t.Fatalf("timed-out batch left changes=%d watermarks=%q", changeCount(t, admin), feedWatermarks(t, admin))
	}
	if err := locker.Rollback(); err != nil {
		t.Fatal(err)
	}
	result, err = retention.PruneStreamChanges(context.Background(), cutoff)
	if err != nil || result.Deleted != 1 || result.BudgetExhausted {
		t.Fatalf("prune after release = %+v/%v", result, err)
	}
	if got := watermark(t, admin, "sesn_parent_lock", "session"); got != position || changeCount(t, admin) != 0 {
		t.Fatalf("watermark %d changes %d; want %d and 0", got, changeCount(t, admin), position)
	}
}

// A phase runs at most ten batches. Ten full batches that take exactly the
// last 2,560 eligible rows are not budget exhaustion: the last probe finds no
// further eligible row, a younger row included. With one more eligible row the last probe finds it, the phase
// counts exhaustion once, and the next invocation deletes that row without
// counting.
func TestRetentionPhasesCountBudgetExhaustionOnlyWithRowsLeft(t *testing.T) {
	for _, phase := range []string{RetentionPhaseIdempotency, RetentionPhaseStreamChanges} {
		t.Run(phase, func(t *testing.T) {
			admin := storagetest.NewPostgreSQLAdminDB(t)
			w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
			seedRetentionSession(t, admin, "default", "sesn_budget")
			base := dbTime(t, admin, "date_trunc('second', clock_timestamp()) - interval '3 days'")
			cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")
			const budgetRows = retentionBatchBudget * retentionBatchLimit
			retention := NewRetention(dbconnect.NewClientForTesting(w.DB))
			run := retention.PruneIdempotencyReceipts
			if phase == RetentionPhaseStreamChanges {
				run = retention.PruneStreamChanges
			}
			metrics := NewSchedulerMetrics()

			// One row younger than the cutoff stays and is not eligible.
			young := dbTime(t, admin, "clock_timestamp()")
			if phase == RetentionPhaseIdempotency {
				seedReceipt(t, admin, "default", "sesn_budget", "young", young)
			} else {
				seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_budget", event: "evt_budget_young", changedAt: young})
			}
			count := seedBulkRetentionRows(t, admin, phase, "sesn_budget", base, 0, budgetRows)
			result, err := run(context.Background(), cutoff)
			metrics.ObserveRetention(result)
			if err != nil || result.Batches != retentionBatchBudget || result.Deleted != budgetRows || result.BudgetExhausted || count() != 1 {
				t.Fatalf("exactly %d eligible rows = %+v/%v, %d left; want ten full batches without exhaustion", budgetRows, result, err, count())
			}
			seedBulkRetentionRows(t, admin, phase, "sesn_budget", base, budgetRows, budgetRows+1)
			result, err = run(context.Background(), cutoff)
			metrics.ObserveRetention(result)
			if err != nil || result.Batches != retentionBatchBudget || result.Deleted != budgetRows || !result.BudgetExhausted || count() != 2 {
				t.Fatalf("%d eligible rows = %+v/%v, %d left; want ten full batches and exhaustion", budgetRows+1, result, err, count())
			}
			result, err = run(context.Background(), cutoff)
			metrics.ObserveRetention(result)
			if err != nil || result.Batches != 1 || result.Deleted != 1 || result.BudgetExhausted || count() != 1 {
				t.Fatalf("remaining row = %+v/%v; want one short batch leaving the young row", result, err)
			}
			if got := budgetExhaustedCounter(t, metrics, phase); got != 1 {
				t.Fatalf("budget-exhausted counter = %v; want 1", got)
			}
		})
	}
}

// A candidate skipped while another transaction holds it is not revisited
// later in the same phase, even once released; the next invocation starts
// again at the oldest remaining row.
func TestRetentionPhaseDoesNotRevisitASkippedLockedRow(t *testing.T) {
	for _, phase := range []string{RetentionPhaseIdempotency, RetentionPhaseStreamChanges} {
		t.Run(phase, func(t *testing.T) {
			admin := storagetest.NewPostgreSQLAdminDB(t)
			w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
			seedRetentionSession(t, admin, "default", "sesn_skip")
			base := dbTime(t, admin, "date_trunc('second', clock_timestamp()) - interval '3 days'")
			cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")
			count := seedBulkRetentionRows(t, admin, phase, "sesn_skip", base, 0, 300)
			// The oldest row is held during the first batch and released when
			// the second batch starts.
			holder, err := admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = holder.Rollback() }()
			lockOldestRetentionRow(t, holder, phase)
			release := &releaseOnSecondPrune{holder: holder}
			retention := NewRetention(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", release)))
			run := retention.PruneIdempotencyReceipts
			if phase == RetentionPhaseStreamChanges {
				run = retention.PruneStreamChanges
			}
			result, err := run(context.Background(), cutoff)
			if err != nil || result.Batches != 2 || result.Deleted != 299 || result.BudgetExhausted {
				t.Fatalf("first invocation = %+v/%v; want 299 rows in two batches", result, err)
			}
			if count() != 1 || !oldestRetentionRowExists(t, admin, phase) {
				t.Fatalf("after the first invocation %d rows remain (oldest kept %t); want only the skipped oldest", count(), oldestRetentionRowExists(t, admin, phase))
			}
			result, err = run(context.Background(), cutoff)
			if err != nil || result.Deleted != 1 || count() != 0 {
				t.Fatalf("second invocation = %+v/%v; want the oldest row", result, err)
			}
		})
	}
}

// Every retention function reveals or changes nothing for anyone else: other
// workloads cannot execute it, a serving role that sets the purpose flag still
// sees only its own Workspace, a shadowed search path does not redirect it, and
// a definer that is not the table owner deletes nothing. Cleanup itself has no
// direct access to receipts, changes or feed metadata.
func TestRetentionFunctionsSecurityBoundary(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedRetentionSession(t, admin, "ws_boundary", "sesn_boundary")
	old := dbTime(t, admin, "clock_timestamp() - interval '2 days'")
	cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")
	seedReceipt(t, admin, "ws_boundary", "sesn_boundary", "boundary", old)
	seedFeedChange(t, admin, feedRow{ws: "ws_boundary", session: "sesn_boundary", event: "evt_boundary", changedAt: old})
	if _, err := admin.Exec(`INSERT INTO session_event_feed_retention (workspace_id, session_id, feed_key, pruned_through) VALUES ('ws_boundary', 'sesn_boundary', 'session', 1)`); err != nil {
		t.Fatal(err)
	}
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	roleName := func(db *sql.DB) string {
		var name string
		if err := db.QueryRow(`SELECT current_user`).Scan(&name); err != nil {
			t.Fatal(err)
		}
		return name
	}

	t.Run("CleanupHasNoDirectAccess", func(t *testing.T) {
		for _, statement := range []string{
			`SELECT count(*) FROM session_event_idempotency_keys`,
			`DELETE FROM session_event_idempotency_keys`,
			`SELECT count(*) FROM session_event_stream_changes`,
			`DELETE FROM session_event_stream_changes`,
			`SELECT count(*) FROM session_event_feed_retention`,
			`UPDATE session_event_feed_retention SET pruned_through = pruned_through + 1`,
		} {
			if _, err := w.DB.ExecContext(ctx, statement); sqlState(err) != "42501" {
				t.Fatalf("Cleanup %q: %v; want insufficient_privilege", statement, err)
			}
		}
	})

	t.Run("OtherWorkloadsCannotExecute", func(t *testing.T) {
		for _, workload := range contract.WorkloadNames() {
			if workload == "cleanup" {
				continue
			}
			db := w.OpenWorkload(t, workload, nil)
			for _, statement := range []string{
				`SELECT * FROM public.tetral_prune_event_idempotency($1, NULL, NULL, NULL, NULL, 256)`,
				`SELECT * FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 256)`,
			} {
				if _, err := db.ExecContext(ctx, statement, cutoff); sqlState(err) != "42501" {
					t.Fatalf("%s executed %q: %v", workload, statement, err)
				}
			}
		}
		if receiptCount(t, admin) != 1 || changeCount(t, admin) != 1 {
			t.Fatal("a denied execution deleted rows")
		}
	})

	t.Run("SpoofedPurposeFlag", func(t *testing.T) {
		for _, probe := range []struct {
			workload  string
			statement string
		}{
			{"api", `SELECT count(*) FROM session_event_idempotency_keys`},
			{"api", `SELECT count(*) FROM session_event_stream_changes`},
			{"event_stream", `SELECT count(*) FROM session_event_feed_retention`},
			{"event_stream", `SELECT count(*) FROM session_events`},
		} {
			tx, err := w.OpenWorkload(t, probe.workload, nil).BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `SELECT set_config('tetral.retention_maintenance', 'true', true)`); err != nil {
				t.Fatal(err)
			}
			var visible int
			if err := tx.QueryRowContext(ctx, probe.statement).Scan(&visible); err != nil || visible != 0 {
				t.Fatalf("%s with a spoofed flag: %q saw %d rows (%v)", probe.workload, probe.statement, visible, err)
			}
			_ = tx.Rollback()
		}
		api := w.OpenWorkload(t, "api", nil)
		tx, err := api.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `SELECT set_config('tetral.retention_maintenance', 'true', true)`); err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{`DELETE FROM session_event_idempotency_keys`, `DELETE FROM session_event_stream_changes`} {
			result, err := tx.ExecContext(ctx, statement)
			if err != nil {
				t.Fatal(err)
			}
			if affected, _ := result.RowsAffected(); affected != 0 {
				t.Fatalf("API with a spoofed flag: %q deleted %d rows", statement, affected)
			}
		}
	})

	t.Run("SearchPathShadowing", func(t *testing.T) {
		//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
		if _, err := admin.Exec(`CREATE SCHEMA retention_shadow;
			CREATE TABLE retention_shadow.session_event_idempotency_keys (LIKE public.session_event_idempotency_keys);
			CREATE FUNCTION retention_shadow.never(timestamptz, timestamptz) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
			CREATE OPERATOR retention_shadow.<= (LEFTARG = timestamptz, RIGHTARG = timestamptz, FUNCTION = retention_shadow.never);
			GRANT USAGE ON SCHEMA retention_shadow TO PUBLIC;
			GRANT ALL ON retention_shadow.session_event_idempotency_keys TO ` + pgx.Identifier{roleName(w.DB)}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		tx, err := w.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = retention_shadow, public, pg_catalog`); err != nil {
			t.Fatal(err)
		}
		var deleted int
		if err := tx.QueryRowContext(ctx, `SELECT deleted_count FROM public.tetral_prune_event_idempotency($1, NULL, NULL, NULL, NULL, 256)`, cutoff).Scan(&deleted); err != nil || deleted != 1 {
			t.Fatalf("shadowed search path changed pruning: deleted %d (%v)", deleted, err)
		}
	})

	t.Run("FunctionOwnershipAndCatalogPosture", func(t *testing.T) {
		for _, function := range []struct{ signature, table string }{
			{"public.tetral_prune_event_idempotency(timestamptz,timestamptz,text,text,bytea,integer)", "public.session_event_idempotency_keys"},
			{"public.tetral_prune_event_changes(timestamptz,timestamptz,text,text,bigint,integer)", "public.session_event_stream_changes"},
		} {
			var ownerMatches, definer, fixedPath, publicExecute bool
			if err := admin.QueryRow(`SELECT pg_get_userbyid(p.proowner) = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = $2::regclass),
				       p.prosecdef, COALESCE('search_path=pg_catalog' = ANY(p.proconfig), false),
				       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')
				  FROM pg_proc p WHERE p.oid = $1::regprocedure`, function.signature, function.table).Scan(&ownerMatches, &definer, &fixedPath, &publicExecute); err != nil {
				t.Fatal(err)
			}
			if !ownerMatches || !definer || !fixedPath || publicExecute {
				t.Fatalf("%s owner=table owner %t definer %t fixed path %t public %t", function.signature, ownerMatches, definer, fixedPath, publicExecute)
			}
		}
		// A definer that has the table privileges but is not the table owner
		// fails the owner predicate and sees no candidate.
		seedFeedChange(t, admin, feedRow{ws: "ws_boundary", session: "sesn_boundary", event: "evt_owner", changedAt: old})
		apiRole := roleName(w.OpenWorkload(t, "api", nil))
		for _, signature := range []string{
			"public.tetral_prune_event_idempotency(timestamptz,timestamptz,text,text,bytea,integer)",
			"public.tetral_prune_event_changes(timestamptz,timestamptz,text,text,bigint,integer)",
		} {
			//nolint:gosec // G202: fixed signature and quoted installer-owned role identifier.
			if _, err := admin.Exec(`ALTER FUNCTION ` + signature + ` OWNER TO ` + pgx.Identifier{apiRole}.Sanitize()); err != nil {
				t.Fatal(err)
			}
		}
		if page := pruneReceiptsAs(t, w.DB, cutoff, nil, 256); page.examined != 0 {
			t.Fatalf("non-owner definer examined %d receipts", page.examined)
		}
		// No serving role may write feed metadata, so the non-owner change
		// pruner fails before deleting anything.
		var examined int
		err := w.DB.QueryRow(`SELECT examined_count FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 256)`, cutoff).Scan(&examined)
		if (err == nil && examined != 0) || (err != nil && sqlState(err) != "42501") {
			t.Fatalf("non-owner change pruner = %d/%v", examined, err)
		}
		if receiptCount(t, admin) != 1 || changeCount(t, admin) != 2 {
			t.Fatalf("non-owner definers deleted rows: receipts %d changes %d", receiptCount(t, admin), changeCount(t, admin))
		}
	})
}

// Under the real Cleanup role, with sequential scans and sorts disabled and a
// generic plan, each pruning statement locks its page through the age index
// under its Limit with the continuation as an index condition, and the
// more_remaining probe is a LIMIT 1 seek of the same index after the page.
func TestRetentionPruningReadsTheAgeIndexes(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedRetentionSession(t, admin, "default", "sesn_plan")
	old := dbTime(t, admin, "clock_timestamp() - interval '2 days'")
	for index := 0; index < 4; index++ {
		seedReceipt(t, admin, "default", "sesn_plan", fmt.Sprintf("plan%d", index), old)
		seedFeedChange(t, admin, feedRow{ws: "default", session: "sesn_plan", event: fmt.Sprintf("evt_plan_%d", index), changedAt: old})
	}
	if _, err := admin.Exec(`ANALYZE session_event_idempotency_keys; ANALYZE session_event_stream_changes`); err != nil {
		t.Fatal(err)
	}
	cleanupRole := ""
	if err := w.DB.QueryRow(`SELECT current_user`).Scan(&cleanupRole); err != nil {
		t.Fatal(err)
	}
	cutoff := dbTime(t, admin, "clock_timestamp() - interval '1 day'")
	for _, probe := range []struct {
		name, statement, index, table string
		args                          []any
	}{
		{"receipts first", `SELECT * FROM public.tetral_prune_event_idempotency($1, NULL, NULL, NULL, NULL, 2)`, "idx_session_event_idempotency_keys_age", "session_event_idempotency_keys", []any{cutoff}},
		{"receipts continuation", `SELECT * FROM public.tetral_prune_event_idempotency($1, $2, 'default', 'sesn_plan', '\x00', 2)`, "idx_session_event_idempotency_keys_age", "session_event_idempotency_keys", []any{cutoff, old}},
		{"changes first", `SELECT * FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, 2)`, "idx_session_event_stream_changes_age", "session_event_stream_changes", []any{cutoff}},
		{"changes continuation", `SELECT * FROM public.tetral_prune_event_changes($1, $2, 'default', 'sesn_plan', 1, 2)`, "idx_session_event_stream_changes_age", "session_event_stream_changes", []any{cutoff, old}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			plans := explainNestedAs(t, admin, cleanupRole, probe.statement, probe.args...)
			page, probed := false, false
			for _, plan := range plans {
				if evidence := indexUnderLimit(plan, probe.index, probe.table); evidence.found {
					if evidence.sorts != 0 || evidence.seqScans != 0 {
						t.Fatalf("%s: Limit subtree has %d sorts and %d sequential scans", probe.name, evidence.sorts, evidence.seqScans)
					}
					if strings.Contains(probe.name, "continuation") && !strings.Contains(evidence.indexCond, "ROW(") {
						t.Fatalf("%s: continuation is not an index condition: %q", probe.name, evidence.indexCond)
					}
					page = true
				}
				probed = probed || probeSeeksIndex(plan, probe.index)
			}
			if !page || !probed {
				t.Fatalf("%s: %s serves the locked page %t and the more_remaining probe %t in %d nested plans", probe.name, probe.index, page, probed, len(plans))
			}
		})
	}
}

// probeSeeksIndex reports whether a plan is a top-level Limit directly over an
// unlocked scan of index whose condition starts after a key: the
// more_remaining probe.
func probeSeeksIndex(plan map[string]any, index string) bool {
	if nodeType, _ := plan["Node Type"].(string); nodeType != "Limit" {
		return false
	}
	// The only other child is the policy's owner lookup InitPlan.
	var child map[string]any
	children, _ := plan["Plans"].([]any)
	for _, candidate := range children {
		if node, ok := candidate.(map[string]any); ok && node["Parent Relationship"] == "Outer" {
			child = node
		}
	}
	childType, _ := child["Node Type"].(string)
	cond, _ := child["Index Cond"].(string)
	return (childType == "Index Scan" || childType == "Index Only Scan") && child["Index Name"] == index && strings.Contains(cond, "ROW(")
}

type receiptPage struct {
	deleted, examined          int
	lastCreatedAt              time.Time
	lastWorkspace, lastSession string
	lastDigest                 string
	more                       bool
}

func pruneReceiptsAs(t *testing.T, db *sql.DB, cutoff time.Time, after *receiptPage, limit int) receiptPage {
	t.Helper()
	args := []any{cutoff, nil, nil, nil, nil, limit}
	if after != nil {
		args = []any{cutoff, after.lastCreatedAt, after.lastWorkspace, after.lastSession, []byte(after.lastDigest), limit}
	}
	var page receiptPage
	var lastAt sql.NullTime
	var lastWorkspace, lastSession sql.NullString
	var lastDigest []byte
	if err := db.QueryRow(`SELECT deleted_count, examined_count, last_created_at, last_workspace_id, last_session_id, last_idempotency_key_digest, more_remaining
		FROM public.tetral_prune_event_idempotency($1, $2, $3, $4, $5, $6)`, args...).Scan(&page.deleted, &page.examined, &lastAt, &lastWorkspace, &lastSession, &lastDigest, &page.more); err != nil {
		t.Fatalf("prune receipts: %v", err)
	}
	page.lastCreatedAt, page.lastWorkspace, page.lastSession, page.lastDigest = lastAt.Time, lastWorkspace.String, lastSession.String, string(lastDigest)
	return page
}

type changePage struct {
	deleted, examined int
	more              bool
}

func pruneChangesAs(t *testing.T, db *sql.DB, cutoff time.Time, limit int) changePage {
	t.Helper()
	var page changePage
	if err := db.QueryRow(`SELECT deleted_count, examined_count, more_remaining FROM public.tetral_prune_event_changes($1, NULL, NULL, NULL, NULL, $2)`, cutoff, limit).Scan(&page.deleted, &page.examined, &page.more); err != nil {
		t.Fatalf("prune changes: %v", err)
	}
	return page
}

func seedRetentionSession(t *testing.T, admin *sql.DB, workspaceID, sessionID string) {
	t.Helper()
	const created = "2026-01-01T00:00:00Z"
	agentID, versionID, environmentID := "agent_ret_"+workspaceID, "agv_ret_"+workspaceID, "env_ret_"+workspaceID
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, $2) ON CONFLICT DO NOTHING`, []any{workspaceID, created}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, $2, $2, 1, $3, $3) ON CONFLICT DO NOTHING`, []any{workspaceID, agentID, created}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, $2, $3, 1, '{}', $2, $4) ON CONFLICT DO NOTHING`, []any{workspaceID, versionID, agentID, created}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $2, $2, '{}', $3, $3) ON CONFLICT DO NOTHING`, []any{workspaceID, environmentID, created}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at)
		  VALUES ($1, $2, 'thr_' || $2, 'session', 'idle', 'active', $3, 1, $4, $5, $5)`, []any{workspaceID, sessionID, agentID, environmentID, created}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at)
		  VALUES ($1, 'thr_' || $2, $2, 'main', 'public', 'idle', $3, $3, $3)`, []any{workspaceID, sessionID, created}},
	} {
		if _, err := admin.Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("seed retention session %s: %v", sessionID, err)
		}
	}
}

func seedRetentionThread(t *testing.T, admin *sql.DB, workspaceID, sessionID, threadID, role, visibility string) {
	t.Helper()
	if _, err := admin.Exec(`INSERT INTO session_threads (workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name, created_at, last_active_at, updated_at)
		VALUES ($1, $2, $3, 'thr_' || $3, $4, $5, 'idle', CASE WHEN $4 = 'subagent' THEN $2 END, now(), now(), now())`, workspaceID, threadID, sessionID, role, visibility); err != nil {
		t.Fatalf("seed thread %s: %v", threadID, err)
	}
}

func seedReceipt(t *testing.T, admin *sql.DB, workspaceID, sessionID, digest string, createdAt time.Time) {
	t.Helper()
	if _, err := admin.Exec(`INSERT INTO session_event_idempotency_keys (workspace_id, session_id, idempotency_key_digest, canonical_request_hash, response_events_json, created_at, updated_at)
		VALUES ($1, $2, $3, '\x00', '[]', $4, $4)`, workspaceID, sessionID, []byte(digest), createdAt); err != nil {
		t.Fatalf("seed receipt %s: %v", digest, err)
	}
}

type feedRow struct {
	ws, session, thread, event string
	sessionHidden              bool
	internalChange             bool
	internalEvent              bool
	changedAt                  time.Time
}

// seedFeedChange inserts one permanent event and its revision-1 change row and
// returns the change's stream position.
func seedFeedChange(t *testing.T, admin *sql.DB, row feedRow) int64 {
	t.Helper()
	var thread any
	if row.thread != "" {
		thread = row.thread
	}
	eventVisibility, changeVisibility := "public", "public"
	if row.internalEvent {
		eventVisibility = "internal"
	}
	if row.internalChange {
		changeVisibility = "internal"
	}
	var position int64
	if err := admin.QueryRow(`WITH event AS (
		  INSERT INTO session_events (workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json, visibility, session_visible, created_at, updated_at)
		  VALUES ($1, $2, $3, $4, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_events WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id IS NOT DISTINCT FROM $3),
		          'agent.message', '{}', $5, NOT $7, $8, $8)
		  RETURNING workspace_id, session_id, session_thread_id, event_id
		)
		INSERT INTO session_event_stream_changes (workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at)
		SELECT workspace_id, session_id, event_id, session_thread_id, 1, $6, NOT $7, $8 FROM event
		RETURNING stream_position`,
		row.ws, row.session, thread, row.event, eventVisibility, changeVisibility, row.sessionHidden, row.changedAt).Scan(&position); err != nil {
		t.Fatalf("seed feed change %s: %v", row.event, err)
	}
	if _, err := admin.Exec(`UPDATE session_events SET insert_stream_position = $2, latest_stream_position = $2 WHERE event_id = $1`, row.event, position); err != nil {
		t.Fatal(err)
	}
	return position
}

// seedBulkRetentionRows inserts n eligible rows for the phase numbered after
// offset, one second apart from base, and returns a counter of the phase's
// remaining rows.
func seedBulkRetentionRows(t *testing.T, admin *sql.DB, phase, sessionID string, base time.Time, offset, n int) func() int {
	t.Helper()
	if phase == RetentionPhaseIdempotency {
		if _, err := admin.Exec(`INSERT INTO session_event_idempotency_keys (workspace_id, session_id, idempotency_key_digest, canonical_request_hash, response_events_json, created_at, updated_at)
			SELECT 'default', $1, convert_to(lpad(n::text, 6, '0'), 'UTF8'), '\x00', '[]', $2::timestamptz + n * interval '1 second', $2::timestamptz + n * interval '1 second'
			  FROM generate_series($3::integer + 1, $3::integer + $4::integer) n`, sessionID, base, offset, n); err != nil {
			t.Fatal(err)
		}
		return func() int { return receiptCount(t, admin) }
	}
	if _, err := admin.Exec(`WITH events AS (
		  INSERT INTO session_events (workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json, created_at, updated_at)
		  SELECT 'default', $1, 'thr_' || $1, 'evt_bulk_' || n, n, 'agent.message', '{}', $2, $2 FROM generate_series($3::integer + 1, $3::integer + $4::integer) n
		  RETURNING event_id, sequence
		)
		INSERT INTO session_event_stream_changes (workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at)
		SELECT 'default', $1, event_id, 'thr_' || $1, 1, 'public', true, $2::timestamptz + sequence * interval '1 second' FROM events ORDER BY sequence`, sessionID, base, offset, n); err != nil {
		t.Fatal(err)
	}
	return func() int { return changeCount(t, admin) }
}

func lockOldestRetentionRow(t *testing.T, holder *sql.Tx, phase string) {
	t.Helper()
	statement := `SELECT 1 FROM session_event_idempotency_keys ORDER BY created_at LIMIT 1 FOR UPDATE`
	if phase == RetentionPhaseStreamChanges {
		statement = `SELECT 1 FROM session_event_stream_changes ORDER BY changed_at LIMIT 1 FOR UPDATE`
	}
	if _, err := holder.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func oldestRetentionRowExists(t *testing.T, admin *sql.DB, phase string) bool {
	t.Helper()
	statement := `SELECT EXISTS (SELECT 1 FROM session_event_idempotency_keys WHERE idempotency_key_digest = convert_to('000001', 'UTF8'))`
	if phase == RetentionPhaseStreamChanges {
		statement = `SELECT EXISTS (SELECT 1 FROM session_event_stream_changes WHERE event_id = 'evt_bulk_1')`
	}
	var exists bool
	if err := admin.QueryRow(statement).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

// releaseOnSecondPrune commits the holder transaction when the second pruning
// statement of the phase starts, so the row it held becomes available behind
// the phase's continuation.
type releaseOnSecondPrune struct {
	mu     sync.Mutex
	calls  int
	holder *sql.Tx
}

func (r *releaseOnSecondPrune) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "tetral_prune_event_") {
		r.mu.Lock()
		r.calls++
		if r.calls == 2 {
			_ = r.holder.Commit()
		}
		r.mu.Unlock()
	}
	return ctx
}

func (*releaseOnSecondPrune) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func receiptKeys(t *testing.T, admin *sql.DB) string {
	t.Helper()
	var keys sql.NullString
	if err := admin.QueryRow(`SELECT string_agg(convert_from(idempotency_key_digest, 'UTF8'), ' ' ORDER BY idempotency_key_digest) FROM session_event_idempotency_keys`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	return keys.String
}

func receiptCount(t *testing.T, admin *sql.DB) int {
	t.Helper()
	var count int
	if err := admin.QueryRow(`SELECT count(*) FROM session_event_idempotency_keys`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func changeCount(t *testing.T, admin *sql.DB) int {
	t.Helper()
	var count int
	if err := admin.QueryRow(`SELECT count(*) FROM session_event_stream_changes`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func feedWatermarks(t *testing.T, admin *sql.DB) string {
	t.Helper()
	var marks sql.NullString
	if err := admin.QueryRow(`SELECT string_agg(workspace_id || '/' || session_id || '/' || feed_key || '=' || pruned_through, ' ' ORDER BY workspace_id, session_id, feed_key COLLATE "C") FROM session_event_feed_retention`).Scan(&marks); err != nil {
		t.Fatal(err)
	}
	return marks.String
}

func watermark(t *testing.T, admin *sql.DB, sessionID, feedKey string) int64 {
	t.Helper()
	var value int64
	if err := admin.QueryRow(`SELECT pruned_through FROM session_event_feed_retention WHERE session_id = $1 AND feed_key = $2`, sessionID, feedKey).Scan(&value); err != nil {
		t.Fatalf("watermark %s/%s: %v", sessionID, feedKey, err)
	}
	return value
}

// waitForLockWait blocks until a backend running a statement containing
// fragment waits on a lock. It polls the catalog, not the clock.
func waitForLockWait(t *testing.T, admin *sql.DB, fragment string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%' || $1 || '%' AND pid <> pg_backend_pid())`, fragment).Scan(&waiting); err != nil {
			t.Fatalf("wait for lock wait: %v", err)
		}
		if waiting {
			return
		}
	}
}

func budgetExhaustedCounter(t *testing.T, metrics *SchedulerMetrics, phase string) float64 {
	t.Helper()
	samples, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range samples {
		if sample.Name == "tetral_cleanup_retention_budget_exhausted_total" && len(sample.Labels) == 1 && sample.Labels[0].Value == phase {
			return sample.Value
		}
	}
	t.Fatalf("no budget-exhausted sample for %s", phase)
	return 0
}

// explainNestedAs runs statement as role with auto_explain logging every
// nested statement's generic plan and returns the logged plans.
func explainNestedAs(t *testing.T, admin *sql.DB, role, statement string, args ...any) []map[string]any {
	t.Helper()
	ctx := context.Background()
	config, err := pgx.ParseConfig(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	var notices noticeLog
	config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) { notices.add(notice.Message) }
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `LOAD 'auto_explain'`); err != nil {
		t.Fatalf("load auto_explain: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, setting := range []string{
		`SET LOCAL auto_explain.log_min_duration = 0`,
		`SET LOCAL auto_explain.log_nested_statements = on`,
		`SET LOCAL auto_explain.log_format = 'json'`,
		`SET LOCAL auto_explain.log_level = 'notice'`,
		`SET LOCAL enable_seqscan = off`,
		`SET LOCAL enable_sort = off`,
		`SET LOCAL plan_cache_mode = force_generic_plan`,
		`SET LOCAL ROLE ` + pgx.Identifier{role}.Sanitize(),
	} {
		if _, err := tx.Exec(ctx, setting); err != nil {
			t.Fatalf("%s: %v", setting, err)
		}
	}
	if _, err := tx.Exec(ctx, statement, args...); err != nil {
		t.Fatalf("explain %q: %v", statement, err)
	}
	var plans []map[string]any
	for _, message := range strings.Split(notices.raw(), "\n---\n") {
		start := strings.Index(message, "{")
		if start < 0 {
			continue
		}
		var document struct {
			Plan map[string]any `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(message[start:]), &document); err == nil && document.Plan != nil {
			plans = append(plans, document.Plan)
		}
	}
	return plans
}

type limitIndexEvidence struct {
	found     bool
	locks     bool
	indexCond string
	sorts     int
	seqScans  int
}

// indexUnderLimit finds a Limit node whose subtree locks the rows it scans
// through index (the candidate page), and reports the Sort and table Seq Scan
// nodes inside that subtree.
func indexUnderLimit(node map[string]any, index, table string) limitIndexEvidence {
	if nodeType, _ := node["Node Type"].(string); nodeType == "Limit" {
		var evidence limitIndexEvidence
		walkLimitSubtree(node, index, table, &evidence)
		if evidence.found && evidence.locks {
			return evidence
		}
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			if evidence := indexUnderLimit(childNode, index, table); evidence.found {
				return evidence
			}
		}
	}
	return limitIndexEvidence{}
}

func walkLimitSubtree(node map[string]any, index, table string, evidence *limitIndexEvidence) {
	nodeType, _ := node["Node Type"].(string)
	relation, _ := node["Relation Name"].(string)
	switch {
	case nodeType == "LockRows":
		evidence.locks = true
	case nodeType == "Sort" || nodeType == "Incremental Sort":
		evidence.sorts++
	case nodeType == "Seq Scan" && relation == table:
		evidence.seqScans++
	case (nodeType == "Index Scan" || nodeType == "Index Only Scan") && node["Index Name"] == index:
		evidence.found = true
		evidence.indexCond, _ = node["Index Cond"].(string)
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			walkLimitSubtree(childNode, index, table, evidence)
		}
	}
}
