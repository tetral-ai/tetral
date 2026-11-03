package computer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// IssueClose closes the current binding on its live connection. The caller
// owns the removal decision; this helper only issues and tracks session.close.
// Repeated calls while closing or closed issue nothing. Callers take the
// Session runtime mutation lock before other lifecycle locks in their transaction.
func IssueClose(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (string, error) {
	if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(ws), sessionID); err != nil {
		return "", err
	}
	var instanceID, connectionID string
	err := tx.QueryRow(ctx, `SELECT current_instance_id,connection_id FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND binding_id IS NOT NULL`, string(ws), sessionID, computerID).Scan(&instanceID, &connectionID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return "", err
	}
	connection, err := instance.LockLiveConnection(ctx, tx, ws, instanceID, connectionID, now, true)
	if err != nil {
		return "", err
	}
	var bindingID string
	err = tx.QueryRow(ctx, `SELECT binding_id FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND connection_id=$4 FOR UPDATE`, string(ws), sessionID, computerID, connection.ID).Scan(&bindingID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM computer_bindings WHERE workspace_id=$1 AND binding_id=$2 FOR UPDATE`, string(ws), bindingID).Scan(&state); err != nil {
		return "", err
	}
	if state == BindingClosing || state == BindingClosed || state == BindingInvalid {
		return "", nil
	}
	messageID, err := instance.IssueInstruction(ctx, tx, ws, connection, protocol.SessionClosePayload{SessionID: sessionID, BindingID: bindingID}, "")
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_bindings SET state='closing',close_message_id=$3 WHERE workspace_id=$1 AND binding_id=$2`, string(ws), bindingID, messageID); err != nil {
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

// StoreClosed ends execution/access, not necessarily local file retention.
// Diagnostic events and the state transition share the report transaction.
// A second report of this close instruction changes nothing, even with a new
// upstream message ID. Residual paths require operator cleanup. It returns
// the count of mismatched lost-change instruction IDs skipped, for the caller
// to log only after commit.
func StoreClosed(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, binding Binding, report protocol.SessionStatus, now time.Time) (int, error) {
	if binding.State != BindingClosing {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_bindings SET state='closed',reported_at=$3 WHERE workspace_id=$1 AND binding_id=$2`, string(ws), binding.BindingID, now); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE computers SET execution_state='closed' WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND binding_id=$4`, string(ws), binding.SessionID, binding.ComputerID, binding.BindingID); err != nil {
		return 0, err
	}
	var events []SessionError
	if len(report.CleanupFailures.Value) > 0 {
		failures := make([]map[string]string, 0, len(report.CleanupFailures.Value))
		for _, failure := range report.CleanupFailures.Value {
			failures = append(failures, map[string]string{"path": failure.Path, "message": failure.Message})
		}
		events = append(events, SessionError{Type: "computer_cleanup_error", Message: "Local data could not be removed after computer closure; operator cleanup is required.", Terminal: true, Fields: map[string]any{"computer_id": binding.ComputerID, "cleanup_failures": failures}})
	}
	skipped := 0
	for _, lost := range report.LostChanges.Value {
		if lost.RequestMessageID != report.RequestMessageID {
			skipped++
			continue
		}
		events = append(events, SessionError{Type: "drive_changes_lost_error", Message: fmt.Sprintf("%d changes to drive %s could not be confirmed saved before computer %s was closed.", lost.PathsTotal, lost.DriveID, binding.ComputerID), Terminal: true, Fields: map[string]any{"computer_id": binding.ComputerID, "drive_id": lost.DriveID, "paths": lost.Paths, "paths_total": lost.PathsTotal}})
	}
	return skipped, WriteSessionErrors(ctx, tx, ws, binding.SessionID, now, events)
}
