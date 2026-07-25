package tetralsandbox

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const defaultStaleCreatingReconcileLimit = 25

type StaleCreatingReconcileHandler interface {
	ReconcileStaleCreating(context.Context, workspace.ID, string) (*sandbox.Sandbox, error)
}

type StaleCreatingReconciler struct {
	Client  *dbconnect.Client
	Handler StaleCreatingReconcileHandler
	Config  StaleCreatingReconcilerConfig
}

type StaleCreatingReconcilerConfig struct {
	WorkspaceID string
	Limit       int
	StaleAfter  time.Duration
	Clock       func() time.Time
}

func (r *StaleCreatingReconciler) RunOnce(ctx context.Context) ([]string, error) {
	if r == nil || r.Client == nil {
		return nil, &ConfigError{Message: "stale creating reconciler database client is required"}
	}
	if r.Handler == nil {
		return nil, &ConfigError{Message: "stale creating reconciler handler is required"}
	}
	cfg := normalizedStaleCreatingReconcilerConfig(r.Config)
	now := cfg.Clock().UTC()
	due, err := r.dueSandboxIDs(ctx, cfg, now)
	if err != nil {
		return nil, err
	}
	for _, sandboxID := range due {
		if _, err := r.Handler.ReconcileStaleCreating(ctx, workspace.ID(cfg.WorkspaceID), sandboxID); err != nil {
			return due, err
		}
	}
	return due, nil
}

func (r *StaleCreatingReconciler) dueSandboxIDs(ctx context.Context, cfg StaleCreatingReconcilerConfig, now time.Time) ([]string, error) {
	staleBefore := now.Add(-cfg.StaleAfter).UTC().Format(time.RFC3339Nano)
	var sandboxIDs []string
	if err := r.Client.WithWorkspaceReadOnlyTx(ctx, cfg.WorkspaceID, "tetralsandbox.stale_creating_scan", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT sb.id
			   FROM sandboxes sb
			  WHERE sb.workspace_id = $1
			    AND sb.status IN ('creating', 'resuming')
			    AND sb.updated_at <= $2
			  ORDER BY sb.updated_at ASC, sb.id ASC
			  LIMIT $3`,
			cfg.WorkspaceID,
			staleBefore,
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

func normalizedStaleCreatingReconcilerConfig(cfg StaleCreatingReconcilerConfig) StaleCreatingReconcilerConfig {
	if cfg.Limit <= 0 {
		cfg.Limit = defaultStaleCreatingReconcileLimit
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = defaultSandboxStatusFreshnessWindow
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return storage.Now() }
	}
	return cfg
}
