package computer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// LastCloudComputerError answers the removal of a Session's last attached
// cloud computer.
type LastCloudComputerError struct{}

func (*LastCloudComputerError) Error() string {
	return "the Session's last cloud computer cannot be removed"
}

// Removal is a computer as the removal route finds it.
type Removal struct {
	Attached       bool
	ExecutionState string
}

// ReadRemoval reads computerID of sessionID for the removal route, or
// answers ErrNotFound.
func ReadRemoval(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (Removal, error) {
	var association, execution string
	err := tx.QueryRow(ctx, `SELECT association_state,execution_state FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		string(ws), sessionID, computerID).Scan(&association, &execution)
	if errors.Is(err, sql.ErrNoRows) {
		return Removal{}, ErrNotFound
	}
	return Removal{Attached: association == Attached, ExecutionState: execution}, err
}

// Remove serves the removal route on an attached computer of an idle
// Session, whose runtime mutation lock its caller holds: it refuses the
// Session's last attached cloud computer with LastCloudComputerError and
// otherwise detaches the computer with its removed machine message.
func Remove(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string, now time.Time) (string, error) {
	var last bool
	if err := tx.QueryRow(ctx, `SELECT c.kind='cloud' AND NOT EXISTS (SELECT 1 FROM computers o WHERE o.workspace_id=c.workspace_id
		AND o.session_id=c.session_id AND o.id<>c.id AND o.kind='cloud' AND o.association_state='attached')
		FROM computers c WHERE c.workspace_id=$1 AND c.session_id=$2 AND c.id=$3 AND c.association_state='attached'`,
		string(ws), sessionID, computerID).Scan(&last); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if last {
		return "", &LastCloudComputerError{}
	}
	return Detach(ctx, tx, ws, sessionID, computerID, true, now)
}

// DetachAll detaches every attached computer of sessionID, in the order
// they were added, for the Session's archive or deletion: no machine
// message and no last-cloud-computer rule.
func DetachAll(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT id FROM computers WHERE workspace_id=$1 AND session_id=$2 AND association_state='attached'
		ORDER BY storage_sequence`, string(ws), sessionID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var computerID string
		if err := rows.Scan(&computerID); err != nil {
			_ = rows.Close()
			return err
		}
		ids = append(ids, computerID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for _, computerID := range ids {
		if _, err := Detach(ctx, tx, ws, sessionID, computerID, false, now); err != nil {
			return err
		}
	}
	return nil
}

// Detach detaches computerID of sessionID in its caller's transaction,
// which holds the Session's runtime mutation lock. It locks the computer row
// FOR UPDATE first, which Bridge's admission, release and control routing
// also lock before they issue, so no instruction for the computer is issued
// after the commit. On an attached computer it
//  1. sets association_state detached and detached_at to now;
//  2. with machineMessage, writes the removed machine message;
//  3. closes it: a session.close for its binding when that binding is
//     preparing, prepared or failed on its current instance's live
//     connection, which makes the binding and the computer closing;
//     otherwise the computer becomes closed when it never had a binding,
//     and unknown when it had one;
//  4. refreshes its current instance's unused.
//
// It touches no other computer, binding or Session, and leaves the
// computer's open request as it is. A detached computer is left as it is.
// It returns the computer's execution state.
func Detach(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string, machineMessage bool, now time.Time) (string, error) {
	var association, execution string
	var currentInstanceID, connectionID, bindingID sql.NullString
	if err := tx.QueryRow(ctx, `SELECT association_state,execution_state,current_instance_id,connection_id,binding_id FROM computers
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 FOR UPDATE`, string(ws), sessionID, computerID).
		Scan(&association, &execution, &currentInstanceID, &connectionID, &bindingID); err != nil {
		return "", err
	}
	if association == Detached {
		return execution, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE computers SET association_state='detached',detached_at=$4 WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		string(ws), sessionID, computerID, now); err != nil {
		return "", err
	}
	if machineMessage {
		if err := WriteMachineMessageTx(ctx, tx, ws, sessionID, computerID, ChangeRemoved, now); err != nil {
			return "", err
		}
	}
	closeID, err := issueClose(ctx, tx, ws, sessionID, computerID, currentInstanceID, connectionID, bindingID)
	if err != nil {
		return "", err
	}
	state := Closing
	if closeID == "" {
		state = Unknown
		var bound bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM computer_bindings WHERE workspace_id=$1 AND computer_id=$2)`,
			string(ws), computerID).Scan(&bound); err != nil {
			return "", err
		}
		if !bound {
			state = Closed
		}
		if _, err := tx.Exec(ctx, `UPDATE computers SET execution_state=$4 WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
			string(ws), sessionID, computerID, state); err != nil {
			return "", err
		}
	}
	if currentInstanceID.Valid {
		if err := RefreshUnused(ctx, tx, ws, currentInstanceID.String); err != nil {
			return "", err
		}
	}
	return state, nil
}

// IssueLateCloses issues on connection, whose row its caller holds FOR
// UPDATE after accepting its ready, a late close for every detached computer
// whose execution_state is unknown and whose current instance is the
// connection's: a session.close with a new binding_id, recorded as a binding
// of kind late_close in state closing, which becomes the computer's binding
// on this connection. The computer stays unknown until that close's report
// is stored (StoreClosed).
func IssueLateCloses(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection) error {
	rows, err := tx.Query(ctx, `SELECT session_id,id FROM computers WHERE workspace_id=$1 AND current_instance_id=$2
		AND association_state='detached' AND execution_state='unknown' ORDER BY id FOR UPDATE`, string(ws), connection.InstanceID)
	if err != nil {
		return err
	}
	var computers [][2]string
	for rows.Next() {
		var computer [2]string
		if err := rows.Scan(&computer[0], &computer[1]); err != nil {
			_ = rows.Close()
			return err
		}
		computers = append(computers, computer)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for _, computer := range computers {
		bindingID := id.New("bind_")
		messageID, err := instance.IssueInstruction(ctx, tx, ws, connection, protocol.SessionClosePayload{SessionID: computer[0], BindingID: bindingID}, "")
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO computer_bindings (workspace_id,session_id,computer_id,binding_id,instance_id,connection_id,kind,state,close_message_id)
			VALUES ($1,$2,$3,$4,$5,$6,'late_close','closing',$7)`, string(ws), computer[0], computer[1], bindingID, connection.InstanceID, connection.ID, messageID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE computers SET connection_id=$4,binding_id=$5 WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
			string(ws), computer[0], computer[1], connection.ID, bindingID); err != nil {
			return err
		}
	}
	return nil
}
