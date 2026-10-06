// This file owns cross-workspace cleanup claim orchestration; command startup
// remains responsible only for dependency wiring and process-level reporting.
package tetralcleanup

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type WorkspaceLister interface {
	ListIDs(context.Context) ([]workspace.ID, error)
}

type CleanupClaimer interface {
	ClaimDue(context.Context, ClaimDueRequest) ([]ClaimedCleanupJob, error)
}

func ClaimDueAcrossWorkspaces(ctx context.Context, lister WorkspaceLister, claimer CleanupClaimer, limit int, observe func(workspace.ID, int, time.Duration), metrics ...*SchedulerMetrics) (err error) {
	started := time.Now()
	defer func() {
		if len(metrics) == 0 || metrics[0] == nil {
			return
		}
		outcome := "success"
		if err != nil {
			outcome = "error"
			if errors.Is(err, context.Canceled) {
				outcome = "cancelled"
			} else if errors.Is(err, context.DeadlineExceeded) {
				outcome = "timeout"
			}
		}
		metrics[0].Operations.Observe("claim_due_across_workspaces", outcome, time.Since(started))
	}()
	workspaceIDs, err := lister.ListIDs(ctx)
	if err != nil {
		return err
	}
	for _, workspaceID := range workspaceIDs {
		if workspaceID == "" {
			return &ValidationError{Message: "workspace_id is required"}
		}
		started := time.Now()
		claimed, err := claimer.ClaimDue(ctx, ClaimDueRequest{WorkspaceID: workspaceID, Limit: limit})
		duration := time.Since(started)
		if err != nil {
			return err
		}
		if observe != nil {
			observe(workspaceID, len(claimed), duration)
		}
	}
	return nil
}
