package tetralcleanup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// The scheduling phase runs under the installed Cleanup role. It discovers due,
// bound, idle and unclaimed Sessions across Workspaces in (cleanup_after,
// session_id) order, marks each one and enqueues one deduped job, and a cycle
// that reaches its end clears the cursor.
func TestSchedulingPhaseClaimsDueSessionsAcrossWorkspaces(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	past := dbTime(t, admin, "clock_timestamp() - interval '2 hours'")
	seedDueSessions(t, admin, "default", past.Add(time.Minute), "sesn_due_b")
	seedDueSessions(t, admin, "ws_cleanup_other", past, "sesn_due_a")
	seedDueSessions(t, admin, "ws_cleanup_other", past.Add(time.Minute), "sesn_due_c")
	seedDueSessions(t, admin, "default", past, "sesn_unbound", "sesn_running", "sesn_existing", "sesn_future")
	if _, err := admin.Exec(`UPDATE session_runtime_status SET binding_id = NULL, binding_generation = NULL WHERE session_id = 'sesn_unbound';
		UPDATE session_runtime_status SET status = 'running' WHERE session_id = 'sesn_running';
		UPDATE session_runtime_status SET cleanup_job_id = 'cleanup_existing' WHERE session_id = 'sesn_existing';
		UPDATE session_runtime_status SET cleanup_after = clock_timestamp() + interval '1 hour' WHERE session_id = 'sesn_future'`); err != nil {
		t.Fatalf("shape ineligible runtime rows: %v", err)
	}
	ids := 0
	scheduler := NewScheduler(dbconnect.NewClientForTesting(w.DB), 10)
	scheduler.IDStrategy = func(prefix string) string {
		ids++
		return prefix + strconv.Itoa(ids)
	}

	result, err := scheduler.RunSchedulingPhase(context.Background())
	if err != nil {
		t.Fatalf("RunSchedulingPhase: %v", err)
	}
	if !result.Elected || result.Attempted != 3 || result.Claimed != 3 || result.Failed != 0 || !result.CycleCompleted {
		t.Fatalf("result = %+v; want three claims and a completed cycle", result)
	}
	// IDs are minted in discovery order: (cleanup_after, session_id) across Workspaces.
	for sessionID, want := range map[string]string{"sesn_due_a": "cleanup_1", "sesn_due_b": "cleanup_3", "sesn_due_c": "cleanup_5"} {
		if got := markerJobID(t, admin, sessionID); got.String != want {
			t.Fatalf("%s cleanup_job_id = %v; want %s", sessionID, got, want)
		}
	}
	for _, sessionID := range []string{"sesn_unbound", "sesn_running", "sesn_future"} {
		if got := markerJobID(t, admin, sessionID); got.Valid {
			t.Fatalf("ineligible %s was claimed as %s", sessionID, got.String)
		}
	}
	var kind, partitionKey, dedupeKey, payloadJSON string
	var enqueuedAt, availableAt time.Time
	var claimedAt sql.NullTime
	if err := admin.QueryRow(`SELECT q.kind, q.partition_key, q.dedupe_key, q.payload_json, q.available_at, s.cleanup_enqueued_at, s.cleanup_claimed_at
		   FROM queue_jobs q JOIN session_runtime_status s ON s.workspace_id = q.workspace_id AND s.cleanup_job_id = 'cleanup_1'
		  WHERE q.workspace_id = 'ws_cleanup_other' AND q.id = 'qjob_2'`).Scan(&kind, &partitionKey, &dedupeKey, &payloadJSON, &availableAt, &enqueuedAt, &claimedAt); err != nil {
		t.Fatalf("read first claim: %v", err)
	}
	if kind != "cleanup_session" || partitionKey != "session:ws_cleanup_other:sesn_due_a" || dedupeKey != "cleanup_session:ws_cleanup_other:sesn_due_a:cleanup_1" || claimedAt.Valid || !availableAt.Equal(enqueuedAt) {
		t.Fatalf("first claim = %s/%s/%s claimed=%v available=%s enqueued=%s", kind, partitionKey, dedupeKey, claimedAt, availableAt, enqueuedAt)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload["workspace_id"] != "ws_cleanup_other" || payload["session_id"] != "sesn_due_a" || payload["cleanup_job_id"] != "cleanup_1" {
		t.Fatalf("payload = %s/%v", payloadJSON, err)
	}
	if cursor := readCursor(t, admin); cursor.cycleCutoff.Valid || cursor.afterSessionID.Valid || cursor.generation != 1 {
		t.Fatalf("cursor after completed cycle = %+v; want cleared cycle at generation 1", cursor)
	}

	replay, err := scheduler.RunSchedulingPhase(context.Background())
	if err != nil || replay.Attempted != 0 || !replay.CycleCompleted {
		t.Fatalf("replay = %+v/%v; want an empty completed cycle", replay, err)
	}
	if jobs := cleanupJobsPerSession(t, admin); len(jobs) != 3 || jobs["sesn_due_a"] != 1 || jobs["sesn_due_b"] != 1 || jobs["sesn_due_c"] != 1 {
		t.Fatalf("cleanup jobs = %v; want one per claimed Session", jobs)
	}
}

// Marker and Queue timestamps come from the database clock read after the
// Session arbitration lock, not from the transaction start or the cycle cutoff.
func TestSchedulingClaimStampsDatabaseTimeAfterSessionArbitration(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_arbitrated")
	resource, err := storage.SessionRuntimeMutationAdvisoryLockResource("default", "sesn_arbitrated")
	if err != nil {
		t.Fatal(err)
	}
	holder, err := admin.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if _, err := holder.ExecContext(context.Background(), `SELECT pg_advisory_lock($1, $2)`, storage.SessionRuntimeMutationAdvisoryLockCategory, resource); err != nil {
		t.Fatal(err)
	}
	// The tracer fires when the claim transaction, already begun, asks for the
	// arbitration lock, so the release time below follows the transaction start.
	arbitration := &arbitrationTracer{requested: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := NewScheduler(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", arbitration)), 10).RunSchedulingPhase(context.Background())
		done <- err
	}()
	<-arbitration.requested
	released := dbTime(t, admin, "clock_timestamp()")
	if _, err := holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1, $2)`, storage.SessionRuntimeMutationAdvisoryLockCategory, resource); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("RunSchedulingPhase: %v", err)
	}
	var enqueuedAt, availableAt time.Time
	if err := admin.QueryRow(`SELECT s.cleanup_enqueued_at, q.available_at
		   FROM session_runtime_status s JOIN queue_jobs q ON q.workspace_id = s.workspace_id AND q.dedupe_key = 'cleanup_session:default:sesn_arbitrated:' || s.cleanup_job_id
		  WHERE s.session_id = 'sesn_arbitrated'`).Scan(&enqueuedAt, &availableAt); err != nil {
		t.Fatalf("read claim timestamps: %v", err)
	}
	if !enqueuedAt.After(released) || !availableAt.Equal(enqueuedAt) {
		t.Fatalf("enqueued=%s available=%s; want both after the arbitration wait ended at %s", enqueuedAt, availableAt, released)
	}
}

// A replacement owner fences the old one: after ownerA's election connection is
// killed between its claim and checkpoint, ownerB takes a newer generation and
// checkpoints later work; ownerA's old-generation checkpoint then affects no
// row, so B's cursor never moves back and no Session gains a second job.
func TestSchedulingOwnershipLossFencesTheOldOwner(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	past := dbTime(t, admin, "clock_timestamp() - interval '1 hour'")
	seedDueSessions(t, admin, "default", past, "sesn_own_1", "sesn_own_2", "sesn_own_3", "sesn_own_4")

	ownerA := NewScheduler(dbconnect.NewClientForTesting(w.DB), 100)
	pauseA := pauseScheduler(ownerA, stageClaimed, "sesn_own_1")
	doneA := runSchedulerInBackground(ownerA)
	<-pauseA.paused
	terminateElectionBackend(t, admin)

	ownerB := NewScheduler(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", nil)), 100)
	pauseB := pauseScheduler(ownerB, stageClaimed, "sesn_own_3")
	doneB := runSchedulerInBackground(ownerB)
	<-pauseB.paused
	before := readCursor(t, admin)
	if before.generation != 2 || before.afterSessionID.String != "sesn_own_2" {
		t.Fatalf("ownerB cursor = %+v; want generation 2 after sesn_own_2", before)
	}

	close(pauseA.resume)
	if outcome := <-doneA; !errors.Is(outcome.err, ErrSchedulingOwnershipLost) {
		t.Fatalf("ownerA = %v; want ownership loss", outcome.err)
	}
	if after := readCursor(t, admin); !sameCursor(after, before) {
		t.Fatalf("ownerA changed ownerB's cursor: %+v -> %+v", before, after)
	}
	close(pauseB.resume)
	outcome := <-doneB
	if outcome.err != nil || outcome.result.Claimed != 3 || !outcome.result.CycleCompleted {
		t.Fatalf("ownerB = %+v/%v; want three claims and a completed cycle", outcome.result, outcome.err)
	}
	if jobs := cleanupJobsPerSession(t, admin); len(jobs) != 4 || jobs["sesn_own_1"] != 1 || jobs["sesn_own_2"] != 1 || jobs["sesn_own_3"] != 1 || jobs["sesn_own_4"] != 1 {
		t.Fatalf("cleanup jobs = %v; want exactly one per Session", jobs)
	}
}

// The generation check before each claim stops an owner that was replaced
// after its last checkpoint: ownerA, paused between checkpointing sesn_take_1
// and checking the generation for sesn_take_2, makes no further claim once
// ownerB holds the election, and ownerB claims the rest.
func TestSchedulingReplacedOwnerMakesNoFurtherClaim(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_take_1", "sesn_take_2", "sesn_take_3")

	ownerA := NewScheduler(dbconnect.NewClientForTesting(w.DB), 100)
	ownerA.IDStrategy = ownerIDs("a")
	pauseA := pauseScheduler(ownerA, stageGenerationCheck, "sesn_take_2")
	doneA := runSchedulerInBackground(ownerA)
	<-pauseA.paused
	if cursor := readCursor(t, admin); cursor.afterSessionID.String != "sesn_take_1" {
		t.Fatalf("ownerA paused at cursor %+v; want after its sesn_take_1 checkpoint", cursor)
	}
	terminateElectionBackend(t, admin)

	ownerB := NewScheduler(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", nil)), 100)
	ownerB.IDStrategy = ownerIDs("b")
	pauseB := pauseScheduler(ownerB, stageGenerationCheck, "sesn_take_2")
	doneB := runSchedulerInBackground(ownerB)
	<-pauseB.paused

	close(pauseA.resume)
	outcomeA := <-doneA
	if outcomeA.err == nil || outcomeA.result.Attempted != 1 || outcomeA.result.Claimed != 1 {
		t.Fatalf("ownerA = %+v/%v; want it stopped after its first claim", outcomeA.result, outcomeA.err)
	}
	if got := markerJobID(t, admin, "sesn_take_2"); got.Valid {
		t.Fatalf("replaced ownerA claimed sesn_take_2 as %s", got.String)
	}
	close(pauseB.resume)
	if outcomeB := <-doneB; outcomeB.err != nil || outcomeB.result.Claimed != 2 || !outcomeB.result.CycleCompleted {
		t.Fatalf("ownerB = %+v/%v; want the remaining two claims", outcomeB.result, outcomeB.err)
	}
	for sessionID, owner := range map[string]string{"sesn_take_1": "cleanup_a", "sesn_take_2": "cleanup_b", "sesn_take_3": "cleanup_b"} {
		if got := markerJobID(t, admin, sessionID); !strings.HasPrefix(got.String, owner) {
			t.Fatalf("%s claimed as %v; want %s", sessionID, got, owner)
		}
	}
}

// A replaced owner cannot start or end the cycle: its fenced cycle start and
// end-of-cycle reset affect no row, and the replacement's cursor is unchanged.
func TestSchedulingReplacedOwnerCannotStartOrResetTheCycle(t *testing.T) {
	for _, test := range []struct {
		name  string
		stage schedulingStage
	}{
		{"CycleStart", stageCycleStart},
		{"CycleReset", stageCycleReset},
	} {
		t.Run(test.name, func(t *testing.T) {
			admin := storagetest.NewPostgreSQLAdminDB(t)
			w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
			seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_cycle_1", "sesn_cycle_2")

			ownerA := NewScheduler(dbconnect.NewClientForTesting(w.DB), 100)
			pauseA := pauseScheduler(ownerA, test.stage, "")
			doneA := runSchedulerInBackground(ownerA)
			<-pauseA.paused
			terminateElectionBackend(t, admin)

			// ownerB holds generation 2 and stops before its own cycle step:
			// after starting its cycle when ownerA had none, or before
			// resetting the cycle ownerA had finished.
			ownerB := NewScheduler(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", nil)), 100)
			pauseB := pauseScheduler(ownerB, stageGenerationCheck, "sesn_cycle_1")
			if test.stage == stageCycleReset {
				pauseB = pauseScheduler(ownerB, stageCycleReset, "")
			}
			doneB := runSchedulerInBackground(ownerB)
			<-pauseB.paused
			before := readCursor(t, admin)
			if before.generation != 2 || !before.cycleCutoff.Valid {
				t.Fatalf("ownerB cursor = %+v; want its cycle at generation 2", before)
			}

			close(pauseA.resume)
			outcomeA := <-doneA
			if after := readCursor(t, admin); !sameCursor(after, before) {
				t.Fatalf("ownerA changed ownerB's cursor: %+v -> %+v", before, after)
			}
			if !errors.Is(outcomeA.err, ErrSchedulingOwnershipLost) {
				t.Fatalf("ownerA = %+v/%v; want ownership loss", outcomeA.result, outcomeA.err)
			}
			close(pauseB.resume)
			if outcome := <-doneB; outcome.err != nil || !outcome.result.CycleCompleted {
				t.Fatalf("ownerB = %+v/%v; want a completed cycle", outcome.result, outcome.err)
			}
		})
	}
}

// Only the phase's own deadline makes an unfinished phase successful. When the
// budget ends between candidates, the phase succeeds with its cursor at the
// last checkpoint; when a fenced write finds ownership lost and the budget ends
// just after, the loss is still reported.
func TestSchedulingPhaseDeadlineHidesNoOtherError(t *testing.T) {
	t.Run("BudgetExhaustionIsSuccess", func(t *testing.T) {
		admin := storagetest.NewPostgreSQLAdminDB(t)
		w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
		seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_budget_1", "sesn_budget_2")
		scheduler := NewScheduler(dbconnect.NewClientForTesting(w.DB), 100)
		budget := withExpiringPhase(scheduler)
		scheduler.pause = func(_ context.Context, stage schedulingStage, candidate dueSession) {
			if stage == stageGenerationCheck && candidate.sessionID == "sesn_budget_2" {
				budget.expire()
			}
		}
		result, err := scheduler.RunSchedulingPhase(context.Background())
		if err != nil || result.Claimed != 1 || result.CycleCompleted {
			t.Fatalf("exhausted phase = %+v/%v; want one claim and success", result, err)
		}
		if cursor := readCursor(t, admin); cursor.afterSessionID.String != "sesn_budget_1" {
			t.Fatalf("cursor = %+v; want it kept at the last checkpoint", cursor)
		}
	})
	t.Run("OwnershipLossAtTheDeadlineIsAnError", func(t *testing.T) {
		admin := storagetest.NewPostgreSQLAdminDB(t)
		w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
		seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_late_1")
		var budget *expiringContext
		// The phase budget ends as soon as the end-of-cycle reset returns.
		tracer := &queryEndHook{match: "SET cycle_cutoff = NULL", onEnd: func() { budget.expire() }}
		scheduler := NewScheduler(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", tracer)), 100)
		budget = withExpiringPhase(scheduler)
		scheduler.pause = func(_ context.Context, stage schedulingStage, _ dueSession) {
			if stage == stageCycleReset {
				// A replacement owner's generation acquisition.
				if _, err := admin.Exec(`UPDATE cleanup_schedule_cursor SET owner_generation = owner_generation + 1`); err != nil {
					t.Error(err)
				}
			}
		}
		_, err := scheduler.RunSchedulingPhase(context.Background())
		if !errors.Is(err, ErrSchedulingOwnershipLost) || budget.Err() == nil {
			t.Fatalf("phase = %v (budget %v); want ownership loss reported after the budget ended", err, budget.Err())
		}
	})
}

// With pages of one and equal due times, 1000 failing candidates are crossed
// across invocations: each failed key is checkpointed, the next invocation
// resumes after it within the same cycle, and the healthy candidate behind the
// prefix is claimed with a timestamp newer than the persisted cycle cutoff.
// The fault strikes the Queue insert after the marker UPDATE, so a failed
// claim also leaves no marker behind.
func TestSchedulingCrossesAPersistentFailingPrefix(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	sessionIDs := make([]string, 0, 1001)
	for index := 1; index <= 1000; index++ {
		sessionIDs = append(sessionIDs, fmt.Sprintf("sesn_fail_%04d", index))
	}
	sessionIDs = append(sessionIDs, "sesn_ok_1001")
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), sessionIDs...)
	// Test-only fault: enqueueing cleanup for a marked fixture Session raises a fixed SQLSTATE.
	if _, err := admin.Exec(`CREATE FUNCTION cleanup_fixture_fail_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture cleanup enqueue failure' USING ERRCODE = 'TC001'; END $$;
		CREATE TRIGGER cleanup_fixture_fail_enqueue BEFORE INSERT ON queue_jobs
		FOR EACH ROW WHEN (NEW.partition_key LIKE 'session:default:sesn_fail_%') EXECUTE FUNCTION cleanup_fixture_fail_enqueue()`); err != nil {
		t.Fatalf("install enqueue fault: %v", err)
	}
	scheduler := NewScheduler(dbconnect.NewClientForTesting(w.DB), 1)

	first, err := scheduler.RunSchedulingPhase(context.Background())
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "TC001" {
		t.Fatalf("first invocation error = %v; want the injected claim failure", err)
	}
	if first.Attempted != schedulingCandidateAttempts || first.Failed != first.Attempted || first.Claimed != 0 || first.CycleCompleted {
		t.Fatalf("first invocation = %+v; want 1000 failed attempts", first)
	}
	cursor := readCursor(t, admin)
	if !cursor.cycleCutoff.Valid || cursor.afterSessionID.String != "sesn_fail_1000" {
		t.Fatalf("persisted cursor = %+v; want the 1000th failed key", cursor)
	}
	if got := markerJobID(t, admin, "sesn_ok_1001"); got.Valid {
		t.Fatalf("healthy candidate claimed before its turn: %s", got.String)
	}
	var orphanMarkers int
	if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_status WHERE session_id LIKE 'sesn_fail_%' AND (cleanup_job_id IS NOT NULL OR cleanup_enqueued_at IS NOT NULL)`).Scan(&orphanMarkers); err != nil || orphanMarkers != 0 {
		t.Fatalf("failed claims left %d markers (%v); want marker and enqueue to roll back together", orphanMarkers, err)
	}

	second, err := scheduler.RunSchedulingPhase(context.Background())
	if err != nil || second.Attempted != 1 || second.Claimed != 1 || !second.CycleCompleted {
		t.Fatalf("second invocation = %+v/%v; want the healthy candidate and a completed cycle", second, err)
	}
	var enqueuedAt time.Time
	if err := admin.QueryRow(`SELECT cleanup_enqueued_at FROM session_runtime_status WHERE session_id = 'sesn_ok_1001'`).Scan(&enqueuedAt); err != nil {
		t.Fatal(err)
	}
	if !enqueuedAt.After(cursor.cycleCutoff.Time) {
		t.Fatalf("claim stamped %s; want a fresh time after the cycle cutoff %s", enqueuedAt, cursor.cycleCutoff.Time)
	}
}

// A process that stops after committing a claim but before its checkpoint
// leaves the cursor behind that candidate. The next invocation continues the
// cycle without a duplicate, and repeating the committed claim is stale.
func TestSchedulingStopBetweenClaimAndCheckpointRepeatsWithoutDuplicates(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_stop_1", "sesn_stop_2", "sesn_stop_3")
	client := dbconnect.NewClientForTesting(w.DB)
	ctx, stop := context.WithCancel(context.Background())
	crashed := NewScheduler(client, 100)
	var repeated dueSession
	crashed.pause = func(_ context.Context, stage schedulingStage, candidate dueSession) {
		if stage == stageClaimed {
			repeated = candidate
			stop()
		}
	}
	if _, err := crashed.RunSchedulingPhase(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped invocation = %v; want cancellation", err)
	}
	cursor := readCursor(t, admin)
	firstJob := markerJobID(t, admin, "sesn_stop_1")
	if !cursor.cycleCutoff.Valid || cursor.afterSessionID.Valid || !firstJob.Valid {
		t.Fatalf("after stop cursor=%+v first marker=%v; want an unpositioned cycle and a committed claim", cursor, firstJob)
	}

	claimed, err := crashed.claim(context.Background(), cursor.cycleCutoff.Time, repeated)
	if err != nil || claimed {
		t.Fatalf("repeated claim = %t/%v; want stale", claimed, err)
	}
	result, err := NewScheduler(client, 100).RunSchedulingPhase(context.Background())
	if err != nil || result.Claimed != 2 || !result.CycleCompleted {
		t.Fatalf("next invocation = %+v/%v; want the remaining two claims", result, err)
	}
	if got := markerJobID(t, admin, "sesn_stop_1"); got != firstJob {
		t.Fatalf("first marker = %v; want unchanged %v", got, firstJob)
	}
	if jobs := cleanupJobsPerSession(t, admin); len(jobs) != 3 || jobs["sesn_stop_1"] != 1 || jobs["sesn_stop_2"] != 1 || jobs["sesn_stop_3"] != 1 {
		t.Fatalf("cleanup jobs = %v; want exactly one per Session", jobs)
	}
}

// ClaimLimit narrows the global page but never raises it above 100.
func TestSchedulingClaimLimitAboveOneHundredYieldsPagesOfOneHundred(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	sessionIDs := make([]string, 0, 150)
	for index := 1; index <= 150; index++ {
		sessionIDs = append(sessionIDs, fmt.Sprintf("sesn_page_%03d", index))
	}
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), sessionIDs...)
	tracer := &discoveryLimitTracer{}
	result, err := NewScheduler(dbconnect.NewClientForTesting(w.OpenWorkload(t, "cleanup", tracer)), 250).RunSchedulingPhase(context.Background())
	if err != nil || result.Claimed != 150 || !result.CycleCompleted {
		t.Fatalf("result = %+v/%v; want all 150 claimed", result, err)
	}
	if limits := tracer.snapshot(); len(limits) != 2 || limits[0] != 100 || limits[1] != 100 {
		t.Fatalf("discovery page limits = %v; want [100 100]", limits)
	}
}

// Generation acquisition never wraps: at the maximum it fails before any
// cursor change or claim.
func TestSchedulingGenerationOverflowIsAnError(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_overflow")
	if _, err := admin.Exec(`UPDATE cleanup_schedule_cursor SET owner_generation = 9223372036854775807`); err != nil {
		t.Fatal(err)
	}
	result, err := NewScheduler(dbconnect.NewClientForTesting(w.DB), 100).RunSchedulingPhase(context.Background())
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22003" || result.Attempted != 0 {
		t.Fatalf("overflow = %+v/%v; want numeric_value_out_of_range before any attempt", result, err)
	}
	if cursor := readCursor(t, admin); cursor.generation != 9223372036854775807 || cursor.cycleCutoff.Valid {
		t.Fatalf("cursor after overflow = %+v; want unchanged", cursor)
	}
	if got := markerJobID(t, admin, "sesn_overflow"); got.Valid {
		t.Fatalf("overflowed generation claimed %s", got.String)
	}
}

// The singleton is fresh-install state. Its absence is an invariant error and
// Cleanup cannot recreate it.
func TestSchedulingMissingCursorIsAnInvariantError(t *testing.T) {
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	seedDueSessions(t, admin, "default", dbTime(t, admin, "clock_timestamp() - interval '1 hour'"), "sesn_no_cursor")
	if _, err := admin.Exec(`DELETE FROM cleanup_schedule_cursor`); err != nil {
		t.Fatal(err)
	}
	result, err := NewScheduler(dbconnect.NewClientForTesting(w.DB), 100).RunSchedulingPhase(context.Background())
	if !errors.Is(err, ErrScheduleCursorMissing) || !result.Elected || result.Attempted != 0 {
		t.Fatalf("missing cursor = %+v/%v; want the invariant error", result, err)
	}
	var rows int
	if err := admin.QueryRow(`SELECT count(*) FROM cleanup_schedule_cursor`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("cursor rows = %d/%v; want none recreated", rows, err)
	}
	if got := markerJobID(t, admin, "sesn_no_cursor"); got.Valid {
		t.Fatalf("missing cursor claimed %s", got.String)
	}
}

func TestCleanupWorkloadStaysWithinSchedulerBoundary(t *testing.T) {
	for _, path := range []string{
		"scheduler.go",
		filepath.Join("cmd", "tetral-cleanup", "main.go"),
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(body)
		for _, forbidden := range []string{"internal/" + "blob", "Blob" + "Store", "Delete" + "Prefix", "Collect" + "ResourcePrefixes"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s contains %q; cleanup workload must stay Postgres/Queue-only", path, forbidden)
			}
		}
	}
}

func TestSchedulerMetricsCollectorReportsSafeCounters(t *testing.T) {
	metrics := NewSchedulerMetrics()
	metrics.ObserveClaimDue(2, 25*time.Millisecond, nil)
	metrics.ObserveClaimDue(-1, -time.Second, nil)
	metrics.ObserveClaimDue(0, 5*time.Millisecond, fmt.Errorf("phase: %w", context.DeadlineExceeded))
	// A finished phase whose first failed claim hit its own claim deadline.
	metrics.ObserveClaimDue(1, 5*time.Millisecond, &claimFailuresError{failed: 1, first: fmt.Errorf("claim: %w", context.DeadlineExceeded)})

	collector := metrics.Collector()
	samples, err := collector(context.Background())
	if err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	got := map[string]float64{}
	for _, sample := range samples {
		got[sample.Name] = sample.Value
		if sample.Family == "tetral_operation_duration_seconds" {
			continue
		}
		if len(sample.Labels) != 0 {
			t.Fatalf("cleanup scheduler metric %s has labels %#v; want no user/session labels", sample.Name, sample.Labels)
		}
	}
	want := map[string]float64{
		"tetral_cleanup_claim_due_runs_total":        4,
		"tetral_cleanup_jobs_claimed_total":          3,
		"tetral_cleanup_claim_due_duration_ms_total": 35,
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("metric %s = %v; want %v in %#v", name, got[name], value, got)
		}
	}
	for _, want := range []string{
		`tetral_operation_duration_seconds_count{operation="claim_due",outcome="timeout",service="cleanup"} 1`,
		`tetral_operation_duration_seconds_count{operation="claim_due",outcome="error",service="cleanup"} 1`,
	} {
		if !strings.Contains(metrics.Operations.Text(), want) {
			t.Fatalf("failed scheduling phase is not observed as %s: %s", want, metrics.Operations.Text())
		}
	}
}

type schedulingOutcomeForTest struct {
	result SchedulingResult
	err    error
}

func runSchedulerInBackground(scheduler *Scheduler) <-chan schedulingOutcomeForTest {
	done := make(chan schedulingOutcomeForTest, 1)
	go func() {
		result, err := scheduler.RunSchedulingPhase(context.Background())
		done <- schedulingOutcomeForTest{result, err}
	}()
	return done
}

// stagePause blocks a scheduler once at one stage, for one candidate when
// named, until the test closes resume.
type stagePause struct {
	paused chan struct{}
	resume chan struct{}
	once   sync.Once
}

func pauseScheduler(scheduler *Scheduler, stage schedulingStage, sessionID string) *stagePause {
	pause := &stagePause{paused: make(chan struct{}), resume: make(chan struct{})}
	scheduler.pause = func(_ context.Context, at schedulingStage, candidate dueSession) {
		if at != stage || (sessionID != "" && candidate.sessionID != sessionID) {
			return
		}
		pause.once.Do(func() {
			close(pause.paused)
			<-pause.resume
		})
	}
	return pause
}

func ownerIDs(owner string) func(string) string {
	next := 0
	return func(prefix string) string {
		next++
		return prefix + owner + strconv.Itoa(next)
	}
}

// expiringContext is a phase context whose own deadline the test ends.
type expiringContext struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func withExpiringPhase(scheduler *Scheduler) *expiringContext {
	phase := &expiringContext{Context: context.Background(), done: make(chan struct{})}
	scheduler.phaseContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		phase.Context = parent
		return phase, func() {}
	}
	return phase
}

func (c *expiringContext) Done() <-chan struct{} { return c.done }

func (c *expiringContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *expiringContext) expire() { c.once.Do(func() { close(c.done) }) }

// queryEndHook runs onEnd after the first query whose SQL contains match.
type queryEndHook struct {
	match string
	onEnd func()
	once  sync.Once
}

func (q *queryEndHook) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, q.match) {
		return context.WithValue(ctx, queryEndHookKey{}, q)
	}
	return ctx
}

type queryEndHookKey struct{}

func (q *queryEndHook) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(queryEndHookKey{}) == q {
		q.once.Do(q.onEnd)
	}
}

type cursorRow struct {
	generation     int64
	cycleCutoff    sql.NullTime
	afterCleanupAt sql.NullTime
	afterSessionID sql.NullString
}

func readCursor(t *testing.T, admin *sql.DB) cursorRow {
	t.Helper()
	var cursor cursorRow
	if err := admin.QueryRow(`SELECT owner_generation, cycle_cutoff, after_cleanup_at, after_session_id FROM cleanup_schedule_cursor WHERE singleton`).Scan(
		&cursor.generation, &cursor.cycleCutoff, &cursor.afterCleanupAt, &cursor.afterSessionID,
	); err != nil {
		t.Fatalf("read cleanup cursor: %v", err)
	}
	return cursor
}

func sameCursor(left, right cursorRow) bool {
	return left.generation == right.generation &&
		left.cycleCutoff.Valid == right.cycleCutoff.Valid && left.cycleCutoff.Time.Equal(right.cycleCutoff.Time) &&
		left.afterCleanupAt.Valid == right.afterCleanupAt.Valid && left.afterCleanupAt.Time.Equal(right.afterCleanupAt.Time) &&
		left.afterSessionID == right.afterSessionID
}

func markerJobID(t *testing.T, admin *sql.DB, sessionID string) sql.NullString {
	t.Helper()
	var jobID sql.NullString
	if err := admin.QueryRow(`SELECT cleanup_job_id FROM session_runtime_status WHERE session_id = $1`, sessionID).Scan(&jobID); err != nil {
		t.Fatalf("read %s marker: %v", sessionID, err)
	}
	return jobID
}

func cleanupJobsPerSession(t *testing.T, admin *sql.DB) map[string]int {
	t.Helper()
	rows, err := admin.Query(`SELECT payload_json::jsonb ->> 'session_id', count(*) FROM queue_jobs WHERE kind = 'cleanup_session' GROUP BY 1`)
	if err != nil {
		t.Fatalf("count cleanup jobs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	jobs := map[string]int{}
	for rows.Next() {
		var sessionID string
		var count int
		if err := rows.Scan(&sessionID, &count); err != nil {
			t.Fatal(err)
		}
		jobs[sessionID] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return jobs
}

func dbTime(t *testing.T, admin *sql.DB, expression string) time.Time {
	t.Helper()
	var value time.Time
	if err := admin.QueryRow(`SELECT ` + expression).Scan(&value); err != nil {
		t.Fatalf("read database time: %v", err)
	}
	return value
}

// seedDueSessions creates idle, bound runtime rows due at cleanupAfter for new
// Sessions in workspaceID, creating the Workspace and its Agent and Environment
// on first use.
func seedDueSessions(t *testing.T, admin *sql.DB, workspaceID string, cleanupAfter time.Time, sessionIDs ...string) {
	t.Helper()
	const created = "2026-01-01T00:00:00Z"
	agentID, versionID, environmentID := "agent_cleanup_"+workspaceID, "agv_cleanup_"+workspaceID, "env_cleanup_"+workspaceID
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, $2) ON CONFLICT DO NOTHING`, []any{workspaceID, created}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, $2, $2, 1, $3, $3) ON CONFLICT DO NOTHING`, []any{workspaceID, agentID, created}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, $2, $3, 1, '{}', $2, $4) ON CONFLICT DO NOTHING`, []any{workspaceID, versionID, agentID, created}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $2, $2, '{}', $3, $3) ON CONFLICT DO NOTHING`, []any{workspaceID, environmentID, created}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at)
		  SELECT $1, sid, 'thr_' || sid, 'session', 'idle', 'active', $2, 1, $3, $4, $4 FROM unnest($5::text[]) sid`, []any{workspaceID, agentID, environmentID, created, sessionIDs}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at)
		  SELECT $1, 'thr_' || sid, sid, 'main', 'public', 'idle', $2, $2, $2 FROM unnest($3::text[]) sid`, []any{workspaceID, created, sessionIDs}},
		{`INSERT INTO session_runtime_status (workspace_id, session_id, status, cleanup_after, binding_id, binding_generation, created_at, updated_at)
		  SELECT $1, sid, 'idle', $2, 'bind_' || sid, 1, $3, $3 FROM unnest($4::text[]) sid`, []any{workspaceID, cleanupAfter, created, sessionIDs}},
	} {
		if _, err := admin.Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("seed cleanup sessions in %s: %v", workspaceID, err)
		}
	}
}

// terminateElectionBackend kills the backend holding the scheduling election
// and waits until it has exited, as a crashed owner's connection would.
func terminateElectionBackend(t *testing.T, admin *sql.DB) {
	t.Helper()
	var terminated bool
	if err := admin.QueryRow(`SELECT pg_terminate_backend(l.pid, 10000)
		   FROM pg_locks l
		  WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
		    AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
		    AND ((l.classid::bigint << 32) | l.objid::bigint) = hashtextextended('tetral.cleanup.scheduler', 0)`).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate election backend = %t/%v", terminated, err)
	}
}

// arbitrationTracer signals the first Session arbitration lock request.
type arbitrationTracer struct {
	requested chan struct{}
	once      sync.Once
}

func (a *arbitrationTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "SELECT pg_advisory_xact_lock") {
		a.once.Do(func() { close(a.requested) })
	}
	return ctx
}

func (*arbitrationTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// discoveryLimitTracer records the page limit of each discovery call.
type discoveryLimitTracer struct {
	mu     sync.Mutex
	limits []int64
}

func (d *discoveryLimitTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	// database/sql passes a leading result-format argument; the limit is last.
	if strings.Contains(data.SQL, "tetral_cleanup_due_sessions") && len(data.Args) >= 4 {
		limit := int64(-1)
		switch value := data.Args[len(data.Args)-1].(type) {
		case int64:
			limit = value
		case int:
			limit = int64(value)
		}
		d.mu.Lock()
		d.limits = append(d.limits, limit)
		d.mu.Unlock()
	}
	return ctx
}

func (*discoveryLimitTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (d *discoveryLimitTracer) snapshot() []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]int64(nil), d.limits...)
}
