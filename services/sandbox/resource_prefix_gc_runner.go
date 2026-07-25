package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

const (
	defaultResourcePrefixGCLimit      = 25
	defaultResourcePrefixGCClaimLease = 5 * time.Minute
	resourcePrefixGCErrorDeleteFailed = "delete_prefix_failed"
	resourcePrefixGCErrorInvalidFence = "invalid_prefix_fence"
)

type ResourcePrefixGCPreparer interface {
	DeleteSessionResourceCopiesForGC(context.Context, sandbox.SandboxSetup) error
}

type ResourcePrefixGCRunner struct {
	Client   *dbconnect.Client
	Preparer ResourcePrefixGCPreparer
	Config   ResourcePrefixGCRunnerConfig
}

type ResourcePrefixGCRunnerConfig struct {
	WorkspaceID string
	Limit       int
	RetryAfter  time.Duration
	ClaimLease  time.Duration
	Clock       func() time.Time
}

type ResourcePrefixGCJob struct {
	WorkspaceID workspace.ID
	SessionID   string
	Prefix      string
	ClaimedAt   time.Time
}

func (r *ResourcePrefixGCRunner) RunOnce(ctx context.Context) ([]ResourcePrefixGCJob, error) {
	if r == nil || r.Client == nil {
		return nil, &ConfigError{Message: "resource prefix gc database client is required"}
	}
	if r.Preparer == nil {
		return nil, &ConfigError{Message: "resource prefix gc preparer is required"}
	}
	cfg := normalizedResourcePrefixGCConfig(r.Config)
	if cfg.WorkspaceID == "" {
		return nil, &ConfigError{Message: "resource prefix gc workspace_id is required"}
	}
	now := cfg.Clock().UTC()
	claimed, err := r.claimDue(ctx, cfg, now)
	if err != nil {
		return nil, err
	}
	for _, job := range claimed {
		if err := validateResourcePrefixGCJob(job); err != nil {
			if markErr := r.markFailed(ctx, job, cfg, now, resourcePrefixGCErrorInvalidFence); markErr != nil {
				return claimed, markErr
			}
			continue
		}
		err := r.Preparer.DeleteSessionResourceCopiesForGC(ctx, sandbox.SandboxSetup{
			WorkspaceID: job.WorkspaceID,
			SessionID:   job.SessionID,
		})
		if err != nil {
			if markErr := r.markFailed(ctx, job, cfg, now, resourcePrefixGCErrorDeleteFailed); markErr != nil {
				return claimed, markErr
			}
			continue
		}
		if err := r.markDeleted(ctx, job, now); err != nil {
			return claimed, err
		}
	}
	return claimed, nil
}

func (r *ResourcePrefixGCRunner) claimDue(ctx context.Context, cfg ResourcePrefixGCRunnerConfig, now time.Time) ([]ResourcePrefixGCJob, error) {
	var jobs []ResourcePrefixGCJob
	leaseUntil := now.Add(cfg.ClaimLease)
	err := r.Client.WithWorkspaceTx(ctx, cfg.WorkspaceID, "tetralsandbox.resource_prefix_gc_claim", func(tx *dbconnect.Tx) error {
		nowText := now
		rows, err := tx.Query(ctx,
			`WITH due AS (
				SELECT g.workspace_id, g.session_id, g.prefix
				  FROM session_resource_prefix_gc g
				  JOIN sessions s
				    ON s.workspace_id = g.workspace_id
				   AND s.id = g.session_id
				   AND s.lifecycle_state = 'deleted'
				 WHERE g.workspace_id = $1
				   AND g.status IN ('pending', 'retryable_failed')
				   AND (g.next_attempt_at IS NULL OR g.next_attempt_at <= $2)
				   AND g.prefix = 'workspaces/' || g.workspace_id || '/sessions/' || g.session_id || '/'
				   AND NOT EXISTS (
					SELECT 1
					  FROM session_runtime_bindings rb
					 WHERE rb.workspace_id = g.workspace_id
					   AND rb.session_id = g.session_id
				   )
				   AND NOT EXISTS (
					SELECT 1
					  FROM session_runtime_status rs
					 WHERE rs.workspace_id = g.workspace_id
					   AND rs.session_id = g.session_id
					   AND rs.binding_id IS NOT NULL
				   )
				   AND NOT EXISTS (
					SELECT 1
					  FROM session_resources sr
					 WHERE sr.workspace_id = g.workspace_id
					   AND sr.session_id = g.session_id
					   AND sr.type = 'file'
					   AND sr.detached_at IS NULL
					   AND sr.delete_requested_at IS NULL
				   )
				   AND NOT EXISTS (
					SELECT 1
					  FROM sandboxes sb
					 WHERE sb.workspace_id = g.workspace_id
					   AND sb.session_id = g.session_id
					   AND sb.status <> 'released'
				   )
				 ORDER BY COALESCE(g.next_attempt_at, g.created_at) ASC,
				          g.created_at ASC,
				          g.session_id ASC
				 LIMIT $3
				 FOR UPDATE OF g SKIP LOCKED
			)
				UPDATE session_resource_prefix_gc g
				   SET status = 'retryable_failed',
				       attempt_count = g.attempt_count + 1,
				       last_attempt_at = $5,
				       next_attempt_at = $4,
				       completed_at = NULL,
				       last_error_kind = NULL,
				       updated_at = $5
			  FROM due
			 WHERE g.workspace_id = due.workspace_id
			   AND g.session_id = due.session_id
			RETURNING g.workspace_id, g.session_id, g.prefix`,
			cfg.WorkspaceID,
			nowText,
			cfg.Limit,
			leaseUntil,
			nowText,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var job ResourcePrefixGCJob
			var workspaceID string
			if err := rows.Scan(&workspaceID, &job.SessionID, &job.Prefix); err != nil {
				return err
			}
			job.WorkspaceID = workspace.ID(workspaceID)
			job.ClaimedAt = now
			jobs = append(jobs, job)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (r *ResourcePrefixGCRunner) markDeleted(ctx context.Context, job ResourcePrefixGCJob, now time.Time) error {
	return r.updateClaimed(ctx, job, "tetralsandbox.resource_prefix_gc_deleted", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE session_resource_prefix_gc
			    SET status = 'deleted',
			        next_attempt_at = NULL,
			        completed_at = $3,
			        last_error_kind = NULL,
			        updated_at = $3
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND prefix = $4
			    AND last_attempt_at = $5
			    AND status = 'retryable_failed'`,
			string(job.WorkspaceID),
			job.SessionID,
			now,
			job.Prefix,
			job.ClaimedAt,
		)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			return r.describeMissedClaim(ctx, tx, job, "deleted")
		}
		return nil
	})
}

func (r *ResourcePrefixGCRunner) markFailed(ctx context.Context, job ResourcePrefixGCJob, cfg ResourcePrefixGCRunnerConfig, now time.Time, errorKind string) error {
	nextAttemptAt := now.Add(cfg.RetryAfter)
	return r.updateClaimed(ctx, job, "tetralsandbox.resource_prefix_gc_failed", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE session_resource_prefix_gc
			    SET status = 'retryable_failed',
			        next_attempt_at = $3,
			        completed_at = NULL,
			        last_error_kind = $4,
			        updated_at = $5
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND prefix = $6
			    AND last_attempt_at = $7
			    AND status = 'retryable_failed'`,
			string(job.WorkspaceID),
			job.SessionID,
			nextAttemptAt,
			errorKind,
			now,
			job.Prefix,
			job.ClaimedAt,
		)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			return r.describeMissedClaim(ctx, tx, job, "failed")
		}
		return nil
	})
}

func (r *ResourcePrefixGCRunner) updateClaimed(ctx context.Context, job ResourcePrefixGCJob, operation string, fn func(*dbconnect.Tx) error) error {
	return r.Client.WithWorkspaceTx(ctx, string(job.WorkspaceID), operation, fn)
}

func (r *ResourcePrefixGCRunner) describeMissedClaim(ctx context.Context, tx *dbconnect.Tx, job ResourcePrefixGCJob, stage string) error {
	var status string
	var prefix string
	var lastAttemptAt sql.NullTime
	err := tx.QueryRow(ctx,
		`SELECT status, prefix, last_attempt_at
		   FROM session_resource_prefix_gc
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		string(job.WorkspaceID),
		job.SessionID,
	).Scan(&status, &prefix, &lastAttemptAt)
	if err != nil {
		return fmt.Errorf("resource prefix gc %s update was not applied and marker reload failed: %w", stage, err)
	}
	return fmt.Errorf("resource prefix gc %s update was not applied: marker status=%q prefix=%q last_attempt_at=%v claimed_at=%q", stage, status, prefix, lastAttemptAt.Time, job.ClaimedAt)
}

func normalizedResourcePrefixGCConfig(cfg ResourcePrefixGCRunnerConfig) ResourcePrefixGCRunnerConfig {
	if cfg.Limit <= 0 {
		cfg.Limit = defaultResourcePrefixGCLimit
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = time.Minute
	}
	if cfg.ClaimLease <= 0 {
		cfg.ClaimLease = defaultResourcePrefixGCClaimLease
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return storage.Now() }
	}
	return cfg
}

func validateResourcePrefixGCJob(job ResourcePrefixGCJob) error {
	if job.WorkspaceID == "" || job.SessionID == "" {
		return errors.New("resource prefix gc identity is incomplete")
	}
	if job.Prefix != resourceprojection.SessionPrefix(string(job.WorkspaceID), job.SessionID) {
		return errors.New("resource prefix gc prefix does not match deterministic session prefix")
	}
	return nil
}
