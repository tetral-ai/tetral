package runtimecontrol

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func EnsureSessionOutputCaptureCleanupTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, now time.Time) (bool, error) {
	transportOpen, err := HasOpenSessionSandboxQueueJobsTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return false, err
	}
	if transportOpen {
		return true, nil
	}
	type capture struct {
		writeID           string
		captureGeneration int64
		state             string
		cleanupGeneration int64
	}
	rows, err := tx.Query(ctx,
		`SELECT finish_idle_write_id, capture_generation, state, cleanup_generation
		   FROM sandbox_output_capture_operations
		  WHERE workspace_id=$1 AND session_id=$2
		  ORDER BY finish_idle_write_id, capture_generation
		  FOR UPDATE`,
		workspaceID, sessionID,
	)
	if err != nil {
		return false, err
	}
	var captures []capture
	for rows.Next() {
		var item capture
		if err := rows.Scan(&item.writeID, &item.captureGeneration, &item.state, &item.cleanupGeneration); err != nil {
			_ = rows.Close()
			return false, err
		}
		captures = append(captures, item)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	pending := false
	for _, item := range captures {
		switch item.state {
		case "adopted", "cleaned":
			continue
		case "pending", "running", "cleanup_pending":
			pending = true
		case "staged", "skipped_unavailable", "failed":
			nextGeneration := item.cleanupGeneration + 1
			result, err := tx.Exec(ctx,
				`UPDATE sandbox_output_capture_operations
				    SET state='cleanup_pending', cleanup_generation=$5, updated_at=$6
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
				    AND state IN ('staged','skipped_unavailable','failed')`,
				workspaceID, sessionID, item.writeID, item.captureGeneration, nextGeneration, now,
			)
			if err != nil {
				return false, err
			}
			if !RowsAffected(result) {
				return false, errors.New("output capture cleanup lost its state fence")
			}
			if err := queue.EnqueueSandboxOutputCaptureCleanupTx(ctx, tx, workspace.ID(workspaceID), sessionID, item.writeID, item.captureGeneration, nextGeneration, now); err != nil {
				return false, err
			}
			pending = true
		default:
			return false, errors.New("output capture cleanup found an invalid state")
		}
	}
	if pending {
		return true, nil
	}
	_, err = tx.Exec(ctx,
		`DELETE FROM sandbox_output_capture_operations WHERE workspace_id=$1 AND session_id=$2`,
		workspaceID, sessionID,
	)
	return false, err
}

func HasOpenSessionSandboxQueueJobsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (bool, error) {
	var open bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM queue_jobs
			 WHERE workspace_id=$1 AND payload_json::jsonb ->> 'session_id'=$2
			   AND kind IN (
			       'sandbox_tool_execute', 'sandbox_activate', 'sandbox_materialize',
			       'sandbox_release', 'sandbox_tool_cancel', 'sandbox_output_capture',
			       'sandbox_output_capture_cleanup', 'sandbox_memory_projection',
			       'sandbox_background_command', 'sandbox_background_reconcile'
			   )
			   AND status IN ('pending','leased')
		)`,
		workspaceID, sessionID,
	).Scan(&open)
	return open, err
}

// IdleCleanupDelay keeps closeout and delivery scheduling on the same durable deadline.
const IdleCleanupDelay = 30 * time.Minute
