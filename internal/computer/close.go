package computer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// issueClose closes the binding of a computer that Detach has detached and
// holds locked: when bindingID is preparing, prepared or failed on
// connectionID, the live connection of the computer's current instance, it
// issues session.close {session_id, binding_id} there, records it as the
// binding's close_message_id and sets the binding and the computer closing.
// It returns the close's message ID, or "" when it issued nothing: the
// computer has no binding, the binding is no longer active, or its
// connection is no longer live.
func issueClose(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string, currentInstanceID, connectionID, bindingID sql.NullString) (string, error) {
	if !currentInstanceID.Valid || !connectionID.Valid || !bindingID.Valid {
		return "", nil
	}
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM computer_bindings WHERE workspace_id=$1 AND binding_id=$2 FOR UPDATE`,
		string(ws), bindingID.String).Scan(&state); err != nil {
		return "", err
	}
	if state != BindingPreparing && state != BindingPrepared && state != BindingFailed {
		return "", nil
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return "", err
	}
	connection, live, err := instance.ReadLiveConnection(ctx, tx, ws, currentInstanceID.String, connectionID.String, now)
	if err != nil || !live {
		return "", err
	}
	messageID, err := instance.IssueInstruction(ctx, tx, ws, connection, protocol.SessionClosePayload{SessionID: sessionID, BindingID: bindingID.String}, "")
	var stale *instance.StaleConnectionError
	if errors.As(err, &stale) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_bindings SET state='closing',close_message_id=$3 WHERE workspace_id=$1 AND binding_id=$2`,
		string(ws), bindingID.String, messageID); err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `UPDATE computers SET execution_state='closing' WHERE workspace_id=$1 AND session_id=$2 AND id=$3`, string(ws), sessionID, computerID)
	return messageID, err
}

// LockClose matches a closed report to the close instruction recorded for its
// binding on this connection. Preparation IDs and other bindings cannot close it.
func LockClose(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, sessionID, bindingID, messageID string) (Binding, error) {
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM computer_bindings
 WHERE workspace_id=$1 AND binding_id=$2 AND connection_id=$3 AND session_id=$4 AND close_message_id=$5
 AND EXISTS (SELECT 1 FROM connection_instructions i WHERE i.workspace_id=$1 AND i.connection_id=$3 AND i.message_id=$5 AND i.type='session.close') FOR UPDATE`, string(ws), bindingID, connectionID, sessionID, messageID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, &UnknownPreparationError{Message: "request_message_id names no session.close of this session and binding on this connection"}
	}
	return binding, err
}

// StoreClosed stores a closed report of binding, in the report's
// transaction: the binding becomes closed, and so does the computer whose
// binding it is, an ordinary close's or a late close's, and the instance's
// unused is refreshed. A computer_cleanup_error for the report's cleanup
// failures and the report's lost_changes (RecordLostChanges) are written
// with the transition. Closed ends execution and access, not necessarily
// local file retention: residual paths require operator cleanup. A second
// report of this close instruction changes nothing, even with a new
// upstream message ID. It returns the lost_changes entries skipped, for the
// caller to log only after commit.
func StoreClosed(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, binding Binding, report protocol.SessionStatus, now time.Time) ([]SkippedLostChange, error) {
	if binding.State != BindingClosing {
		return nil, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_bindings SET state='closed',reported_at=$3 WHERE workspace_id=$1 AND binding_id=$2`, string(ws), binding.BindingID, now); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE computers SET execution_state='closed' WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND binding_id=$4`, string(ws), binding.SessionID, binding.ComputerID, binding.BindingID); err != nil {
		return nil, err
	}
	if err := RefreshUnused(ctx, tx, ws, binding.InstanceID); err != nil {
		return nil, err
	}
	if len(report.CleanupFailures.Value) > 0 {
		failures := make([]map[string]string, 0, len(report.CleanupFailures.Value))
		for _, failure := range report.CleanupFailures.Value {
			failures = append(failures, map[string]string{"path": failure.Path, "message": failure.Message})
		}
		if err := WriteSessionErrors(ctx, tx, ws, binding.SessionID, now, []SessionError{{Type: "computer_cleanup_error",
			Message: "Local data could not be removed after computer closure; operator cleanup is required.", Terminal: true,
			Fields: map[string]any{"computer_id": binding.ComputerID, "cleanup_failures": failures}}}); err != nil {
			return nil, err
		}
	}
	return RecordLostChanges(ctx, tx, ws, binding.InstanceID, report.RequestMessageID, report.LostChanges.Value, now)
}
