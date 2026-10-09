package tetralcleanup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	ServiceName = "cleanup"

	defaultClaimLimit = 100

	// Scheduling-phase limits are internal constants. ClaimLimit only narrows
	// the global discovery page below maxSchedulingPageSize.
	maxSchedulingPageSize       = 100
	schedulingPhaseBudget       = 45 * time.Second
	schedulingCandidateAttempts = 1000
	schedulingQueryDeadline     = time.Second
	schedulingClaimDeadline     = 2 * time.Second

	// schedulingElectionName keys the session advisory lock
	// pg_catalog.hashtextextended('tetral.cleanup.scheduler', 0).
	schedulingElectionName = "tetral.cleanup.scheduler"
)

var (
	// ErrScheduleCursorMissing reports that the fresh-install singleton cursor
	// row is absent. Serving roles cannot recreate it.
	ErrScheduleCursorMissing = errors.New("cleanup schedule cursor row is missing")
	// ErrSchedulingOwnershipLost reports that a newer owner generation fenced
	// this scheduler: a cursor write affected no row or the generation moved.
	ErrSchedulingOwnershipLost = errors.New("cleanup scheduling ownership lost")
)

// Scheduler is the Cleanup scheduling phase. It discovers due idle Sessions
// across all Workspaces through the Cleanup-only discovery function and claims
// each one in its own Workspace transaction.
type Scheduler struct {
	Client     *dbconnect.Client
	ClaimLimit int
	IDStrategy func(string) string

	// pause and phaseContext are set only by in-package tests. pause runs at
	// each schedulingStage; phaseContext replaces the 45 s phase context.
	pause        func(context.Context, schedulingStage, dueSession)
	phaseContext func(context.Context) (context.Context, context.CancelFunc)
}

// schedulingStage names the points between fenced steps where in-package tests
// can pause a scheduler.
type schedulingStage int

const (
	// stageCycleStart precedes saving a new cycle's cutoff.
	stageCycleStart schedulingStage = iota
	// stageGenerationCheck precedes the generation check before a claim.
	stageGenerationCheck
	// stageClaimed follows a claim and precedes its checkpoint.
	stageClaimed
	// stageCycleReset precedes the end-of-cycle reset.
	stageCycleReset
)

// claimFailuresError reports candidate claims that failed after their keys
// were checkpointed; it unwraps to the first failure.
type claimFailuresError struct {
	failed int
	first  error
}

func (e *claimFailuresError) Error() string {
	return fmt.Sprintf("cleanup scheduling: %d candidate claims failed", e.failed)
}

func (e *claimFailuresError) Unwrap() error { return e.first }

// SchedulingResult summarizes one scheduling phase. Elected is false when
// another process held the scheduling election.
type SchedulingResult struct {
	Elected        bool
	Attempted      int
	Claimed        int
	Failed         int
	CycleCompleted bool
}

type dueSession struct {
	workspaceID  workspace.ID
	sessionID    string
	cleanupAfter time.Time
}

type scheduleCursor struct {
	cycleCutoff    sql.NullTime
	afterCleanupAt sql.NullTime
	afterSessionID sql.NullString
}

func NewScheduler(client *dbconnect.Client, claimLimit int) *Scheduler {
	return &Scheduler{
		Client:     client,
		ClaimLimit: claimLimit,
		IDStrategy: id.New,
	}
}

// RunSchedulingPhase runs one bounded scheduling phase under its own 45 s
// child of ctx and releases the scheduling election before it returns.
//
// The phase takes the session advisory election on a dedicated connection,
// increments the durable owner generation on that connection and fences every
// later cursor write with it. It then resumes the persisted cycle, or starts a
// cycle at the database clock, and pages through due Sessions in
// (cleanup_after, session_id) order. Each candidate is claimed in its own
// transaction and its key is checkpointed afterwards whether the claim
// succeeded, was stale or failed, so a failing prefix is crossed across
// invocations. The phase stops after 1000 candidate attempts or when its own
// budget ends, leaving the cursor where it is, and either limit alone is
// success; reaching the end of the cycle clears the cursor without starting
// another cycle. Candidate claim failures are reported together after the
// phase stops, and every other stop error is returned.
func (s *Scheduler) RunSchedulingPhase(ctx context.Context) (SchedulingResult, error) {
	var result SchedulingResult
	if s == nil || s.Client == nil {
		return result, &ValidationError{Message: "cleanup scheduler store is required"}
	}
	newPhaseContext := s.phaseContext
	if newPhaseContext == nil {
		newPhaseContext = func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, schedulingPhaseBudget)
		}
	}
	phaseCtx, cancel := newPhaseContext(ctx)
	defer cancel()
	run := schedulingRun{scheduler: s, result: &result}
	// electCtx bounds only connection acquisition and the try-lock; the
	// dedicated connection is not bound to it afterwards.
	electCtx, cancelElect := context.WithTimeout(phaseCtx, schedulingQueryDeadline)
	defer cancelElect()
	_, err := s.Client.TryWithSessionLock(electCtx, "tetralcleanup.schedule_election", schedulingElectionName, func(lock *dbconnect.SessionLockConn) error {
		result.Elected = true
		runErr := run.schedule(phaseCtx, lock)
		if errors.Is(runErr, context.DeadlineExceeded) && errors.Is(phaseCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			// The phase budget itself ended the phase. The cursor already holds
			// every completed checkpoint, so this is a partial, successful
			// phase. Any other error, including a lost fence, stays an error.
			runErr = nil
		}
		return runErr
	})
	if err == nil && result.Failed > 0 {
		err = &claimFailuresError{failed: result.Failed, first: run.firstFailure}
	}
	return result, err
}

type schedulingRun struct {
	scheduler    *Scheduler
	result       *SchedulingResult
	generation   int64
	firstFailure error
}

func (r *schedulingRun) schedule(ctx context.Context, lock *dbconnect.SessionLockConn) error {
	cursor, err := r.acquireGeneration(ctx, lock)
	if err != nil {
		return err
	}
	if !cursor.cycleCutoff.Valid {
		r.scheduler.pauseAt(ctx, stageCycleStart, dueSession{})
		if cursor.cycleCutoff, err = r.startCycle(ctx); err != nil {
			return err
		}
	}
	pageSize := r.scheduler.pageSize()
	for r.result.Attempted < schedulingCandidateAttempts {
		limit := min(pageSize, schedulingCandidateAttempts-r.result.Attempted)
		page, err := r.discover(ctx, cursor, limit)
		if err != nil {
			return err
		}
		for _, candidate := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			r.scheduler.pauseAt(ctx, stageGenerationCheck, candidate)
			if err := r.requireGeneration(ctx, lock); err != nil {
				return err
			}
			r.result.Attempted++
			claimed, claimErr := r.scheduler.claim(ctx, cursor.cycleCutoff.Time, candidate)
			if err := ctx.Err(); err != nil {
				// The phase ended during the claim. Its key stays unchecked, and
				// the idempotent claim repeats in a later invocation.
				return err
			}
			switch {
			case claimErr != nil:
				r.result.Failed++
				if r.firstFailure == nil {
					r.firstFailure = claimErr
				}
			case claimed:
				r.result.Claimed++
			}
			r.scheduler.pauseAt(ctx, stageClaimed, candidate)
			if err := r.checkpoint(ctx, candidate); err != nil {
				return err
			}
			cursor.afterCleanupAt = sql.NullTime{Time: candidate.cleanupAfter, Valid: true}
			cursor.afterSessionID = sql.NullString{String: candidate.sessionID, Valid: true}
		}
		if len(page) < limit {
			r.scheduler.pauseAt(ctx, stageCycleReset, dueSession{})
			if err := r.resetCycle(ctx); err != nil {
				return err
			}
			r.result.CycleCompleted = true
			return nil
		}
	}
	return nil
}

func (s *Scheduler) pauseAt(ctx context.Context, stage schedulingStage, candidate dueSession) {
	if s.pause != nil {
		s.pause(ctx, stage, candidate)
	}
}

func (s *Scheduler) pageSize() int {
	limit := s.ClaimLimit
	if limit <= 0 {
		limit = defaultClaimLimit
	}
	return min(limit, maxSchedulingPageSize)
}

// acquireGeneration increments the owner generation on the election connection
// and reads the cycle state in the same statement. Overflow is a database
// error, never a wrap.
func (r *schedulingRun) acquireGeneration(ctx context.Context, lock *dbconnect.SessionLockConn) (scheduleCursor, error) {
	queryCtx, cancel := context.WithTimeout(ctx, schedulingQueryDeadline)
	defer cancel()
	var cursor scheduleCursor
	err := lock.QueryRow(queryCtx,
		`UPDATE cleanup_schedule_cursor
		    SET owner_generation = owner_generation + 1
		  WHERE singleton
		RETURNING owner_generation, cycle_cutoff, after_cleanup_at, after_session_id`,
	).Scan(&r.generation, &cursor.cycleCutoff, &cursor.afterCleanupAt, &cursor.afterSessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return scheduleCursor{}, ErrScheduleCursorMissing
	}
	if err != nil {
		return scheduleCursor{}, fmt.Errorf("acquire cleanup scheduling generation: %w", err)
	}
	return cursor, nil
}

// requireGeneration rereads the durable generation on the election connection
// before a claim. A failed read means the election connection is unusable.
func (r *schedulingRun) requireGeneration(ctx context.Context, lock *dbconnect.SessionLockConn) error {
	queryCtx, cancel := context.WithTimeout(ctx, schedulingQueryDeadline)
	defer cancel()
	var current int64
	err := lock.QueryRow(queryCtx, `SELECT owner_generation FROM cleanup_schedule_cursor WHERE singleton`).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrScheduleCursorMissing
	}
	if err != nil {
		return fmt.Errorf("check cleanup scheduling generation: %w", err)
	}
	if current != r.generation {
		return ErrSchedulingOwnershipLost
	}
	return nil
}

// startCycle fixes the cycle cutoff at the database clock. Later due times wait
// for the next cycle, so expiring Sessions cannot extend this one.
func (r *schedulingRun) startCycle(ctx context.Context) (sql.NullTime, error) {
	queryCtx, cancel := context.WithTimeout(ctx, schedulingQueryDeadline)
	defer cancel()
	var cutoff sql.NullTime
	err := r.scheduler.Client.QueryRow(queryCtx, "tetralcleanup.start_cycle",
		`UPDATE cleanup_schedule_cursor
		    SET cycle_cutoff = clock_timestamp()
		  WHERE singleton AND owner_generation = $1
		RETURNING cycle_cutoff`, r.generation,
	).Scan(&cutoff)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.NullTime{}, ErrSchedulingOwnershipLost
	}
	if err != nil {
		return sql.NullTime{}, fmt.Errorf("start cleanup scheduling cycle: %w", err)
	}
	return cutoff, nil
}

// discover reads one page in a short read-only snapshot that ends before any
// claim begins.
func (r *schedulingRun) discover(ctx context.Context, cursor scheduleCursor, limit int) ([]dueSession, error) {
	queryCtx, cancel := context.WithTimeout(ctx, schedulingQueryDeadline)
	defer cancel()
	var page []dueSession
	err := r.scheduler.Client.WithTx(queryCtx, "tetralcleanup.discover_due", &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(queryCtx,
			`SELECT workspace_id, session_id, cleanup_after FROM public.tetral_cleanup_due_sessions($1, $2, $3, $4)`,
			cursor.cycleCutoff.Time, cursor.afterCleanupAt, cursor.afterSessionID, limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var candidate dueSession
			var workspaceID string
			if err := rows.Scan(&workspaceID, &candidate.sessionID, &candidate.cleanupAfter); err != nil {
				return err
			}
			candidate.workspaceID = workspace.ID(workspaceID)
			page = append(page, candidate)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("discover due cleanup sessions: %w", err)
	}
	return page, nil
}

// checkpoint records the candidate's discovery key in its own short statement,
// fenced by the owned generation.
func (r *schedulingRun) checkpoint(ctx context.Context, candidate dueSession) error {
	queryCtx, cancel := context.WithTimeout(ctx, schedulingQueryDeadline)
	defer cancel()
	result, err := r.scheduler.Client.Exec(queryCtx, "tetralcleanup.checkpoint",
		`UPDATE cleanup_schedule_cursor
		    SET after_cleanup_at = $2, after_session_id = $3
		  WHERE singleton AND owner_generation = $1`,
		r.generation, candidate.cleanupAfter, candidate.sessionID,
	)
	if err != nil {
		return fmt.Errorf("checkpoint cleanup scheduling cursor: %w", err)
	}
	if !rowsAffected(result) {
		return ErrSchedulingOwnershipLost
	}
	return nil
}

func (r *schedulingRun) resetCycle(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, schedulingQueryDeadline)
	defer cancel()
	result, err := r.scheduler.Client.Exec(queryCtx, "tetralcleanup.reset_cycle",
		`UPDATE cleanup_schedule_cursor
		    SET cycle_cutoff = NULL, after_cleanup_at = NULL, after_session_id = NULL
		  WHERE singleton AND owner_generation = $1`,
		r.generation,
	)
	if err != nil {
		return fmt.Errorf("reset cleanup scheduling cycle: %w", err)
	}
	if !rowsAffected(result) {
		return ErrSchedulingOwnershipLost
	}
	return nil
}

// claim marks one Session and enqueues its cleanup job in one Workspace
// transaction under the Session's runtime arbitration lock. The guarded UPDATE
// repeats the due predicate against the cycle cutoff; the marker and Queue
// timestamps come from the database clock read after the lock. It reports
// false for a stale candidate.
func (s *Scheduler) claim(ctx context.Context, cycleCutoff time.Time, candidate dueSession) (bool, error) {
	claimCtx, cancel := context.WithTimeout(ctx, schedulingClaimDeadline)
	defer cancel()
	claimed := false
	err := s.Client.WithWorkspaceTx(claimCtx, string(candidate.workspaceID), "tetralcleanup.claim_session", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireSessionRuntimeMutationLock(claimCtx, tx, string(candidate.workspaceID), candidate.sessionID); err != nil {
			return err
		}
		var now time.Time
		if err := tx.QueryRow(claimCtx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		now = now.UTC()
		cleanupJobID := s.newID("cleanup_")
		queueJobID := s.newID(queue.JobIDPrefix)
		updated, err := markCleanupEnqueuedTx(claimCtx, tx, candidate.workspaceID, candidate.sessionID, cleanupJobID, cycleCutoff, now)
		if err != nil || !updated {
			return err
		}
		payload, err := cleanupQueuePayload(candidate.workspaceID, candidate.sessionID, cleanupJobID)
		if err != nil {
			return err
		}
		if _, err := queue.EnqueueTx(claimCtx, tx, queue.EnqueueRequest{
			ID:             queueJobID,
			WorkspaceID:    candidate.workspaceID,
			Kind:           queue.KindCleanupSession,
			PartitionKey:   queue.FormatSessionPartitionKey(candidate.workspaceID, candidate.sessionID),
			DedupeKey:      queue.FormatCleanupSessionDedupeKey(candidate.workspaceID, candidate.sessionID, cleanupJobID),
			PayloadVersion: 1,
			PayloadJSON:    payload,
			AvailableAt:    now,
			Now:            now,
		}); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

func (s *Scheduler) newID(prefix string) string {
	if s != nil && s.IDStrategy != nil {
		return s.IDStrategy(prefix)
	}
	return id.New(prefix)
}

func markCleanupEnqueuedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, cleanupJobID string, cycleCutoff time.Time, now time.Time) (bool, error) {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_job_id = $4,
		        cleanup_enqueued_at = $5,
		        cleanup_claimed_at = NULL,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND status = 'idle'
		    AND cleanup_job_id IS NULL
		    AND binding_id IS NOT NULL
		    AND cleanup_after IS NOT NULL
		    AND cleanup_after <= $3`,
		string(workspaceID),
		sessionID,
		cycleCutoff,
		cleanupJobID,
		now,
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

func cleanupQueuePayload(workspaceID workspace.ID, sessionID string, cleanupJobID string) ([]byte, error) {
	return json.Marshal(map[string]string{
		"workspace_id":   string(workspaceID),
		"session_id":     sessionID,
		"cleanup_job_id": cleanupJobID,
	})
}
