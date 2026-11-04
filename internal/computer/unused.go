package computer

import (
	"context"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// RefreshUnused evaluates the instance object's unused rule for instanceID
// over the computers and requests and writes it through instance.SetUnused:
// an active cloud instance is unused unless it is the current instance of an
// attached computer, the instance of a claimed request, or the current
// instance of a detached computer whose closure is still closing. A user or
// deregistered instance is never unused. Every writer that changes an input
// of the rule calls it in its transaction.
func RefreshUnused(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) error {
	var unused bool
	if err := tx.QueryRow(ctx, `SELECT i.kind='cloud' AND i.registration_state='active'
		AND NOT EXISTS (SELECT 1 FROM computers c WHERE c.workspace_id=i.workspace_id AND c.current_instance_id=i.id
			AND (c.association_state='attached' OR c.execution_state='closing'))
		AND NOT EXISTS (SELECT 1 FROM computer_requests r WHERE r.workspace_id=i.workspace_id AND r.instance_id=i.id AND r.state='claimed')
		FROM instances i WHERE i.workspace_id=$1 AND i.id=$2`, string(ws), instanceID).Scan(&unused); err != nil {
		return err
	}
	return instance.SetUnused(ctx, tx, ws, instanceID, unused)
}
