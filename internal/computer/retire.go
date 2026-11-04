package computer

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Ref names one computer of a Session.
type Ref struct {
	SessionID  string
	ComputerID string
}

// AttachedOn lists the attached computers whose current instance is
// instanceID, in Session then computer ID order.
func AttachedOn(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) ([]Ref, error) {
	rows, err := tx.Query(ctx, `SELECT session_id,id FROM computers WHERE workspace_id=$1 AND current_instance_id=$2
		AND association_state='attached' ORDER BY session_id,id`, string(ws), instanceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var refs []Ref
	for rows.Next() {
		var ref Ref
		if err := rows.Scan(&ref.SessionID, &ref.ComputerID); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// RetireCurrentInstance retires the current instance of computerID, an
// attached cloud computer whose instance is being deregistered, in the
// deregistration's transaction, which holds the Session's runtime mutation
// lock: the computer gets no current instance and becomes waiting with no
// connection or binding, so its next call records a new request; the
// Session gets the replaced machine message and one session.computer_replaced
// on its primary stream. It is the only writer that clears an attached
// computer's current instance.
func RetireCurrentInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string, now time.Time) error {
	if _, err := tx.Exec(ctx, `UPDATE computers SET current_instance_id=NULL,execution_state='waiting',connection_id=NULL,binding_id=NULL
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3`, string(ws), sessionID, computerID); err != nil {
		return err
	}
	if err := WriteMachineMessageTx(ctx, tx, ws, sessionID, computerID, ChangeReplaced, now); err != nil {
		return err
	}
	return writeSessionEvents(ctx, tx, ws, sessionID, now, []map[string]any{{"type": "session.computer_replaced", "computer_id": computerID}})
}
