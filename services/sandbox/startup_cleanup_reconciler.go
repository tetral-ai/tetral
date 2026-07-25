package tetralsandbox

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const defaultStartupCleanupReconcileLimit = 25

type StartupCleanupReconcileHandler interface {
	ReconcileStartupCleanup(context.Context, workspace.ID, string) (*sandbox.Sandbox, error)
}

type StartupCleanupReconciler struct {
	Client  *dbconnect.Client
	Handler StartupCleanupReconcileHandler
	Config  StartupCleanupReconcilerConfig
}

type StartupCleanupReconcilerConfig struct {
	WorkspaceID string
	Limit       int
	Clock       func() time.Time
}

func (r *StartupCleanupReconciler) RunOnce(ctx context.Context) ([]string, error) {
	if r == nil || r.Client == nil {
		return nil, &ConfigError{Message: "startup cleanup reconciler database client is required"}
	}
	if r.Handler == nil {
		return nil, &ConfigError{Message: "startup cleanup reconciler handler is required"}
	}
	cfg := normalizedStartupCleanupReconcilerConfig(r.Config)
	now := cfg.Clock().UTC()
	due, err := r.dueSandboxIDs(ctx, cfg, now)
	if err != nil {
		return nil, err
	}
	for _, sandboxID := range due {
		if _, err := r.Handler.ReconcileStartupCleanup(ctx, workspace.ID(cfg.WorkspaceID), sandboxID); err != nil {
			return due, err
		}
	}
	return due, nil
}

func (r *StartupCleanupReconciler) dueSandboxIDs(ctx context.Context, cfg StartupCleanupReconcilerConfig, now time.Time) ([]string, error) {
	var sandboxIDs []string
	if err := r.Client.WithWorkspaceReadOnlyTx(ctx, cfg.WorkspaceID, "tetralsandbox.startup_cleanup_scan", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT sb.id
			   FROM sandboxes sb
			   JOIN sessions sess
			     ON sess.workspace_id = sb.workspace_id
			    AND sess.id = sb.session_id
			  WHERE sb.workspace_id = $1
			    AND sb.status = 'failed'
			    AND sb.startup_failure_reason IS NOT NULL
			    AND sess.lifecycle_state <> 'deleted'
			    AND (
			        sb.cleanup_status = 'pending'
			        OR (
			            sb.cleanup_status = 'retryable_failed'
			            AND (sb.cleanup_next_attempt_at IS NULL OR sb.cleanup_next_attempt_at <= $2)
			        )
			        OR (
			            sb.cleanup_status = 'in_progress'
			            AND sb.cleanup_lease_expires_at <= $2
			        )
			    )
			  ORDER BY COALESCE(sb.cleanup_next_attempt_at, sb.cleanup_lease_expires_at, sb.updated_at) ASC, sb.id ASC
			  LIMIT $3`,
			cfg.WorkspaceID,
			now,
			cfg.Limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var sandboxID string
			if err := rows.Scan(&sandboxID); err != nil {
				return err
			}
			sandboxIDs = append(sandboxIDs, sandboxID)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return sandboxIDs, nil
}

func normalizedStartupCleanupReconcilerConfig(cfg StartupCleanupReconcilerConfig) StartupCleanupReconcilerConfig {
	if cfg.Limit <= 0 {
		cfg.Limit = defaultStartupCleanupReconcileLimit
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return storage.Now() }
	}
	return cfg
}
