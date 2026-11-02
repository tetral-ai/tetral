package runtimecontrol

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
)

// EnsureIdleCleanupScheduledTx restores the idle timer when input admission
// cleared it but terminal settlement started no run. The caller holds the
// Session mutation lock. Existing deadlines and cleanup claims retain ownership;
// the current binding and idle event remain the cleanup fence.
// Delivered agent mail may remain accepted after its committed_at records consumption.
func EnsureIdleCleanupScheduledTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE session_runtime_status srs
		SET cleanup_after=$3, updated_at=$4
		FROM sessions s, session_runtime_bindings binding, session_events idle
		WHERE srs.workspace_id=$1 AND srs.session_id=$2 AND srs.status='idle'
		AND srs.cleanup_after IS NULL AND srs.cleanup_job_id IS NULL
		AND srs.cleanup_enqueued_at IS NULL AND srs.cleanup_claimed_at IS NULL
		AND s.workspace_id=srs.workspace_id AND s.id=srs.session_id
		AND s.lifecycle_state='active' AND s.status<>'terminated'
		AND binding.workspace_id=srs.workspace_id AND binding.session_id=srs.session_id
		AND binding.binding_id=srs.binding_id AND binding.binding_generation=srs.binding_generation
		AND idle.workspace_id=srs.workspace_id AND idle.session_id=srs.session_id
		AND idle.event_id=srs.status_event_id AND idle.type='session.status_idle' AND idle.latest_stream_position>0
		AND NOT EXISTS (SELECT 1 FROM session_threads thread
		  WHERE thread.workspace_id=$1 AND thread.session_id=$2 AND thread.status IN ('running','rescheduling'))
		AND NOT EXISTS (SELECT 1 FROM session_runtime_inbox inbox
		  WHERE inbox.workspace_id=$1 AND inbox.session_id=$2
		  AND inbox.status IN ('queued','delivering','accepted','parked') AND inbox.input_kind<>ALL($5)
		  AND NOT (inbox.input_kind='agent_mail' AND inbox.status='accepted' AND inbox.committed_at IS NOT NULL))`,
		workspaceID, sessionID, now.Add(IdleCleanupDelay), now, queue.NonStartingRuntimeInputKinds())
	return err
}
