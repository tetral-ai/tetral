// This file owns cross-workspace cleanup claim orchestration; command startup
// remains responsible only for dependency wiring and process-level reporting.
package tetralcleanup

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type WorkspaceLister interface {
	ListIDs(context.Context) ([]workspace.ID, error)
}

type CleanupClaimer interface {
	ClaimDue(context.Context, ClaimDueRequest) ([]ClaimedCleanupJob, error)
}

func ClaimDueAcrossWorkspaces(ctx context.Context, lister WorkspaceLister, claimer CleanupClaimer, limit int, observe func(workspace.ID, int, time.Duration)) error {
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
