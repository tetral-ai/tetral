package agentruntimebridge

import (
	"context"
	"errors"
	"fmt"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
)

// RuntimeInboxEventRefsExceededError identifies a pre-law inbox row that
// cannot be delivered within the queue-to-Runtime event-reference bound.
type RuntimeInboxEventRefsExceededError struct {
	WorkspaceID    string
	RuntimeInputID string
}

func (e *RuntimeInboxEventRefsExceededError) Error() string {
	return fmt.Sprintf(
		"bridge job runner startup: workspace %q runtime inbox row %q exceeds %d event references",
		e.WorkspaceID,
		e.RuntimeInputID,
		queue.MaxRuntimeInputEventRefsPerJob,
	)
}

// ValidateRuntimeInboxEventRefBounds fails startup when an active inbox row
// predates the queue construction bound. Workspace discovery is global, while
// each bounded row check remains inside the existing RLS-scoped transaction.
func ValidateRuntimeInboxEventRefBounds(ctx context.Context, client *dbconnect.Client, workspaces WorkspaceLister) error {
	if client == nil {
		return errors.New("bridge runtime inbox startup check database client is required")
	}
	if workspaces == nil {
		return errors.New("bridge runtime inbox startup check workspace lister is required")
	}
	workspaceIDs, err := workspaces.ListIDs(ctx)
	if err != nil {
		return err
	}
	for _, workspaceID := range workspaceIDs {
		if workspaceID == "" {
			return errors.New("bridge runtime inbox startup check discovered an empty workspace id")
		}
		var runtimeInputID string
		err := client.WithWorkspaceReadOnlyTx(ctx, workspaceID.String(), "agentruntimebridge.validate_runtime_inbox_event_refs", func(tx *dbconnect.Tx) error {
			err := tx.QueryRow(ctx,
				`SELECT runtime_input_id
				   FROM session_runtime_inbox
				  WHERE workspace_id = $1
				    AND status IN ('accepted', 'delivering')
				    AND jsonb_array_length(event_ids_json::jsonb) > $2
				  ORDER BY runtime_input_id
				  LIMIT 1`,
				workspaceID,
				queue.MaxRuntimeInputEventRefsPerJob,
			).Scan(&runtimeInputID)
			if dbconnect.IsNoRows(err) {
				return nil
			}
			return err
		})
		if err != nil {
			return err
		}
		if runtimeInputID != "" {
			return &RuntimeInboxEventRefsExceededError{
				WorkspaceID:    workspaceID.String(),
				RuntimeInputID: runtimeInputID,
			}
		}
	}
	return nil
}
