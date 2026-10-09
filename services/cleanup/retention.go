package tetralcleanup

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// Retention phase names. They are also the fixed values of the phase label on
// tetral_cleanup_retention_budget_exhausted_total.
const (
	RetentionPhaseIdempotency   = "idempotency"
	RetentionPhaseStreamChanges = "stream_changes"
)

// Retention limits are internal constants. Each phase admits at most
// retentionBatchBudget transactions per invocation, each locking at most
// retentionBatchLimit candidates under its own two-second deadline, so one
// Cron invocation deletes at most 2,560 receipts and 2,560 change rows.
const (
	retentionBatchLimit     = 256
	retentionBatchBudget    = 10
	retentionBatchDeadline  = 2 * time.Second
	retentionCutoffDeadline = time.Second
)

// Retention prunes expired API idempotency receipts and expired event-stream
// change rows through the Cleanup-only security-definer functions
// tetral_prune_event_idempotency and tetral_prune_event_changes. Cleanup has
// no direct read or write grant on either table or on feed retention metadata.
type Retention struct {
	Client *dbconnect.Client
}

// RetentionResult summarizes one retention phase. BudgetExhausted is true only
// when the phase used every batch and the last batch's probe found another
// eligible row, which the next invocation will delete.
type RetentionResult struct {
	Phase           string
	Batches         int
	Deleted         int
	Examined        int
	BudgetExhausted bool
}

func NewRetention(client *dbconnect.Client) *Retention {
	return &Retention{Client: client}
}

// Cutoff reads the database clock once and returns it minus 24 hours. One
// invocation uses this single cutoff for both retention phases; the functions
// additionally cap any cutoff at their own database clock minus 24 hours.
func (r *Retention) Cutoff(ctx context.Context) (time.Time, error) {
	if r == nil || r.Client == nil {
		return time.Time{}, &ValidationError{Message: "cleanup retention store is required"}
	}
	queryCtx, cancel := context.WithTimeout(ctx, retentionCutoffDeadline)
	defer cancel()
	var cutoff time.Time
	if err := r.Client.QueryRow(queryCtx, "tetralcleanup.retention_cutoff", `SELECT clock_timestamp() - interval '24 hours'`).Scan(&cutoff); err != nil {
		return time.Time{}, fmt.Errorf("read cleanup retention cutoff: %w", err)
	}
	return cutoff, nil
}

// PruneIdempotencyReceipts deletes receipts created at or before cutoff in
// (created_at, workspace_id, session_id, idempotency_key_digest) order.
func (r *Retention) PruneIdempotencyReceipts(ctx context.Context, cutoff time.Time) (RetentionResult, error) {
	var afterCreatedAt sql.NullTime
	var afterWorkspace, afterSession sql.NullString
	var afterDigest []byte
	return r.run(ctx, RetentionPhaseIdempotency, func(batchCtx context.Context, tx *dbconnect.Tx) (retentionBatch, error) {
		var batch retentionBatch
		var lastCreatedAt sql.NullTime
		var lastWorkspace, lastSession sql.NullString
		var lastDigest []byte
		if err := tx.QueryRow(batchCtx,
			`SELECT deleted_count, examined_count, last_created_at, last_workspace_id, last_session_id, last_idempotency_key_digest, more_remaining
			   FROM public.tetral_prune_event_idempotency($1, $2, $3, $4, $5, $6)`,
			cutoff, afterCreatedAt, afterWorkspace, afterSession, afterDigest, retentionBatchLimit,
		).Scan(&batch.deleted, &batch.examined, &lastCreatedAt, &lastWorkspace, &lastSession, &lastDigest, &batch.moreRemaining); err != nil {
			return retentionBatch{}, err
		}
		if batch.examined > 0 {
			afterCreatedAt, afterWorkspace, afterSession, afterDigest = lastCreatedAt, lastWorkspace, lastSession, lastDigest
		}
		return batch, nil
	})
}

// PruneStreamChanges deletes change rows changed at or before cutoff in
// (changed_at, workspace_id, session_id, stream_position) order. The same
// transaction advances each affected feed's pruned_through watermark from the
// rows it actually deleted.
func (r *Retention) PruneStreamChanges(ctx context.Context, cutoff time.Time) (RetentionResult, error) {
	var afterChangedAt sql.NullTime
	var afterWorkspace, afterSession sql.NullString
	var afterPosition sql.NullInt64
	return r.run(ctx, RetentionPhaseStreamChanges, func(batchCtx context.Context, tx *dbconnect.Tx) (retentionBatch, error) {
		var batch retentionBatch
		var lastChangedAt sql.NullTime
		var lastWorkspace, lastSession sql.NullString
		var lastPosition sql.NullInt64
		if err := tx.QueryRow(batchCtx,
			`SELECT deleted_count, examined_count, last_changed_at, last_workspace_id, last_session_id, last_stream_position, more_remaining
			   FROM public.tetral_prune_event_changes($1, $2, $3, $4, $5, $6)`,
			cutoff, afterChangedAt, afterWorkspace, afterSession, afterPosition, retentionBatchLimit,
		).Scan(&batch.deleted, &batch.examined, &lastChangedAt, &lastWorkspace, &lastSession, &lastPosition, &batch.moreRemaining); err != nil {
			return retentionBatch{}, err
		}
		if batch.examined > 0 {
			afterChangedAt, afterWorkspace, afterSession, afterPosition = lastChangedAt, lastWorkspace, lastSession, lastPosition
		}
		return batch, nil
	})
}

// retentionBatch is one committed pruning transaction: rows deleted, rows
// locked and examined, and whether the probe after its page found another
// eligible row.
type retentionBatch struct {
	deleted, examined int
	moreRemaining     bool
}

// run executes at most retentionBatchBudget batch transactions, each under its
// own deadline derived from ctx. The phase continues only after a committed
// full batch whose probe found another eligible row; a short batch or an empty
// probe ends it, and a failed batch rolls back and ends it with its error, so
// no continuation is used after a failure. Spending the whole budget while the
// last probe still found an eligible row is successful partial maintenance.
func (r *Retention) run(ctx context.Context, phase string, prune func(context.Context, *dbconnect.Tx) (retentionBatch, error)) (RetentionResult, error) {
	result := RetentionResult{Phase: phase}
	if r == nil || r.Client == nil {
		return result, &ValidationError{Message: "cleanup retention store is required"}
	}
	for result.Batches < retentionBatchBudget {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		batch, err := r.batch(ctx, phase, prune)
		if err != nil {
			return result, fmt.Errorf("cleanup %s retention batch: %w", phase, err)
		}
		result.Batches++
		result.Deleted += batch.deleted
		result.Examined += batch.examined
		if batch.examined < retentionBatchLimit || !batch.moreRemaining {
			return result, nil
		}
	}
	result.BudgetExhausted = true
	return result, nil
}

func (r *Retention) batch(ctx context.Context, phase string, prune func(context.Context, *dbconnect.Tx) (retentionBatch, error)) (retentionBatch, error) {
	batchCtx, cancel := context.WithTimeout(ctx, retentionBatchDeadline)
	defer cancel()
	var batch retentionBatch
	err := r.Client.WithTx(batchCtx, "tetralcleanup.prune_"+phase, nil, func(tx *dbconnect.Tx) error {
		var err error
		batch, err = prune(batchCtx, tx)
		return err
	})
	return batch, err
}
