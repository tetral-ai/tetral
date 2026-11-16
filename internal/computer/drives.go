package computer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// sessionDrives reads the drive bindings of sessionID (session_drive_bindings,
// owned by internal/session), in UTF-8 byte order of drive_id.
func sessionDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) ([]protocol.DriveBinding, error) {
	rows, err := tx.Query(ctx, `SELECT drive_id,access FROM session_drive_bindings WHERE workspace_id=$1 AND session_id=$2 ORDER BY drive_id`,
		string(ws), sessionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	drives := []protocol.DriveBinding{}
	for rows.Next() {
		var binding protocol.DriveBinding
		if err := rows.Scan(&binding.DriveID, &binding.Access); err != nil {
			return nil, err
		}
		drives = append(drives, binding)
	}
	return drives, rows.Err()
}

// preparationDrives is the drives a session.prepare of computerID on
// instanceID carries: the drive set of the computer's latest binding on that
// instance whose prepared report is stored, whatever its state now, and the
// Session's drive bindings when it has none.
func preparationDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID, instanceID string) ([]protocol.DriveBinding, error) {
	prepared, found, err := latestPreparedDrives(ctx, tx, ws, computerID, instanceID)
	if err != nil || found {
		return prepared, err
	}
	return sessionDrives(ctx, tx, ws, sessionID)
}

// latestPreparedDrives is the drive set of computerID's latest binding on
// instanceID whose prepared report is stored, and whether there is one. A
// stored report without an error is a prepared one.
func latestPreparedDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, computerID, instanceID string) ([]protocol.DriveBinding, bool, error) {
	var bindingID string
	err := tx.QueryRow(ctx, `SELECT binding_id FROM computer_bindings WHERE workspace_id=$1 AND computer_id=$2 AND instance_id=$3
		AND kind='prepare' AND reported_at IS NOT NULL AND error_code IS NULL ORDER BY created_at DESC,binding_id DESC LIMIT 1`,
		string(ws), computerID, instanceID).Scan(&bindingID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	drives, err := drive.BindingDriveSet(ctx, tx, ws, bindingID)
	return drives, err == nil, err
}

// preparingDrives is the drive set of each binding of computerID on
// instanceID whose session.prepare is unanswered.
func preparingDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, computerID, instanceID string) ([][]protocol.DriveBinding, error) {
	rows, err := tx.Query(ctx, `SELECT binding_id FROM computer_bindings WHERE workspace_id=$1 AND computer_id=$2 AND instance_id=$3
		AND kind='prepare' AND state='preparing' ORDER BY binding_id`, string(ws), computerID, instanceID)
	if err != nil {
		return nil, err
	}
	var bindingIDs []string
	for rows.Next() {
		var bindingID string
		if err := rows.Scan(&bindingID); err != nil {
			_ = rows.Close()
			return nil, err
		}
		bindingIDs = append(bindingIDs, bindingID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	var sets [][]protocol.DriveBinding
	for _, bindingID := range bindingIDs {
		set, err := drive.BindingDriveSet(ctx, tx, ws, bindingID)
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}
	return sets, nil
}

// SendDrives writes, in the transaction of a binding change of sessionID,
// which holds the Session's runtime mutation lock, one session.drives with
// drives, the Session's bindings after the change, to every binding of the
// Session whose prepared report is stored and whose close has not been sent
// (state prepared), in binding ID order, and records drives as each one's
// latest send. A binding whose connection is no longer live gets nothing:
// the connection's end invalidates it, and the computer's next preparation
// carries its drive set. It reads the bindings and connections without a
// lock: every writer that moves a binding out of prepared holds the
// Session's runtime mutation lock or ends the binding's connection, and
// IssueInstruction writes nothing on an ended connection.
func SendDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, drives []protocol.DriveBinding) error {
	rows, err := tx.Query(ctx, `SELECT binding_id,instance_id,connection_id FROM computer_bindings
		WHERE workspace_id=$1 AND session_id=$2 AND kind='prepare' AND state='prepared' ORDER BY binding_id`, string(ws), sessionID)
	if err != nil {
		return err
	}
	var bindings [][3]string
	for rows.Next() {
		var binding [3]string
		if err := rows.Scan(&binding[0], &binding[1], &binding[2]); err != nil {
			_ = rows.Close()
			return err
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		connection, live, err := instance.ReadLiveConnection(ctx, tx, ws, binding[1], binding[2], now)
		if err != nil {
			return err
		}
		if !live {
			continue
		}
		if err := issueDrives(ctx, tx, ws, connection, sessionID, binding[0], drives); err != nil {
			return err
		}
	}
	return nil
}

// issueDrives issues session.drives {session_id, binding_id, drives} on
// connection and records drives as the binding's latest send; a connection
// that ended meanwhile gets nothing.
func issueDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, sessionID, bindingID string, drives []protocol.DriveBinding) error {
	messageID, err := instance.IssueInstruction(ctx, tx, ws, connection,
		protocol.SessionDrivesPayload{SessionID: sessionID, BindingID: bindingID, Drives: append([]protocol.DriveBinding{}, drives...)}, "")
	var stale *instance.StaleConnectionError
	if errors.As(err, &stale) {
		return nil
	}
	if err != nil {
		return err
	}
	return drive.RecordBindingDrives(ctx, tx, ws, bindingID, messageID, drives)
}

// sendChangedDrives runs in the transaction that stores binding's prepared
// report on connection, which holds the Session's runtime mutation lock: when
// the drives its session.prepare carried differ from the Session's bindings,
// it issues one session.drives with the Session's bindings, before the
// report releases the computer's waiting calls, so it comes first in issue
// order.
func sendChangedDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, binding Binding) error {
	sent, err := drive.SentDrives(ctx, tx, ws, binding.BindingID, *binding.PrepareMessageID)
	if err != nil {
		return err
	}
	current, err := sessionDrives(ctx, tx, ws, binding.SessionID)
	if err != nil || drive.SameDrives(sent, current) {
		return err
	}
	return issueDrives(ctx, tx, ws, connection, binding.SessionID, binding.BindingID, current)
}

// CheckDrivesFit refuses, as invalid, drives as the new bindings of
// sessionID when an attached computer's session.prepare would exceed
// PrepareEnvelopeLimit with them, or with their union with the drive set of
// its latest prepared binding on its current instance, or of a binding there
// whose session.prepare is unanswered: once prepared, that binding is sent
// drives, and until it applies them the next preparation carries the union.
func CheckDrivesFit(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, drives []protocol.DriveBinding) error {
	objects, err := scanObjects(tx.Query(ctx, `SELECT `+objectColumns+` FROM computers
		WHERE workspace_id=$1 AND session_id=$2 AND association_state='attached' ORDER BY storage_sequence`, string(ws), sessionID))
	if err != nil {
		return err
	}
	if err := attachRepositories(ctx, tx, ws, objects); err != nil {
		return err
	}
	for _, object := range objects {
		sets := [][]protocol.DriveBinding{drives}
		if object.CurrentInstanceID != nil {
			prepared, found, err := latestPreparedDrives(ctx, tx, ws, object.ID, *object.CurrentInstanceID)
			if err != nil {
				return err
			}
			if found {
				sets = append(sets, drive.UnionDrives(drives, prepared))
			}
			preparing, err := preparingDrives(ctx, tx, ws, object.ID, *object.CurrentInstanceID)
			if err != nil {
				return err
			}
			for _, set := range preparing {
				sets = append(sets, drive.UnionDrives(drives, set))
			}
		}
		for _, set := range sets {
			if PrepareEnvelopeSize(sessionID, object.Kind, preparationCWD(object), object.DriveExclude, object.Repositories, set) > PrepareEnvelopeLimit {
				return &InvalidError{Message: prepareTooLargeMessage}
			}
		}
	}
	return nil
}

// LockDrivesReport reads, FOR UPDATE, the binding bindingID of sessionID on
// connectionID that an applied report answers, and whether a report of that
// session.drives is already stored: messageID must be a session.drives
// issued on connectionID to that session_id and binding_id. Any other
// answers UnknownPreparationError.
func LockDrivesReport(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, sessionID, bindingID, messageID string) (Binding, bool, error) {
	unknown := &UnknownPreparationError{Message: "request_message_id names no session.drives of this session and binding on this connection"}
	var sentSession, sentBinding string
	var reported bool
	err := tx.QueryRow(ctx, `SELECT convert_from(envelope,'UTF8')::jsonb#>>'{payload,session_id}',convert_from(envelope,'UTF8')::jsonb#>>'{payload,binding_id}',
		reported_at IS NOT NULL FROM connection_instructions WHERE workspace_id=$1 AND connection_id=$2 AND message_id=$3 AND type='session.drives'`,
		string(ws), connectionID, messageID).Scan(&sentSession, &sentBinding, &reported)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (sentSession != sessionID || sentBinding != bindingID) {
		return Binding{}, false, unknown
	}
	if err != nil {
		return Binding{}, false, err
	}
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM computer_bindings
		WHERE workspace_id=$1 AND binding_id=$2 AND kind='prepare' AND connection_id=$3 AND session_id=$4 FOR UPDATE`,
		string(ws), bindingID, connectionID, sessionID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, unknown
	}
	return binding, reported, err
}

// StoreApplied applies the first applied report of a session.drives to
// binding, locked by LockDrivesReport, in the report's transaction, which
// holds the Session's runtime mutation lock: the binding's drive set becomes that
// message's (drive.ApplyBindingDrivesReport), and it writes one
// drive_mount_error per mount failure and, through RecordLostChanges, one
// drive_changes_lost_error per new (session.drives, drive) of its
// lost_changes. It returns the lost_changes entries the rule skipped.
func StoreApplied(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, binding Binding, report protocol.SessionStatus, now time.Time) ([]SkippedLostChange, error) {
	if err := drive.ApplyBindingDrivesReport(ctx, tx, ws, binding.BindingID, report.RequestMessageID); err != nil {
		return nil, err
	}
	if err := WriteSessionErrors(ctx, tx, ws, binding.SessionID, now, DriveMountErrors(binding.ComputerID, report.DriveMountFailures.Value)); err != nil {
		return nil, err
	}
	return RecordLostChanges(ctx, tx, ws, binding.InstanceID, report.RequestMessageID, report.LostChanges.Value, now)
}

// IssuedBindingComputer reads the computer of bindingID when it is a binding
// issued on connectionID for sessionID, in any state.
func IssuedBindingComputer(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, sessionID, bindingID string) (string, bool, error) {
	var computerID string
	err := tx.QueryRow(ctx, `SELECT computer_id FROM computer_bindings WHERE workspace_id=$1 AND binding_id=$2 AND connection_id=$3 AND session_id=$4`,
		string(ws), bindingID, connectionID, sessionID).Scan(&computerID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return computerID, err == nil, err
}

// transferable holds for a binding of alias whose prepared report is stored
// and whose closed report is not: prepared, or closing once prepared.
func transferable(alias string) string {
	return alias + `.kind='prepare' AND ` + alias + `.state IN ('prepared','closing') AND ` + alias + `.reported_at IS NOT NULL AND ` + alias + `.error_code IS NULL`
}

// TransferBinding reads bindingID when it is a binding of connectionID whose
// prepared report is stored and whose closed report is not; its drive set
// authorizes the drive transfer of that connection's CLI.
func TransferBinding(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, bindingID string) (Binding, bool, error) {
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM computer_bindings b
		WHERE workspace_id=$1 AND binding_id=$2 AND connection_id=$3 AND `+transferable("b"), string(ws), bindingID, connectionID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	return binding, err == nil, err
}

// ConnectionDrives is the drives in the drive sets (drive_binding_sets, of
// internal/drive) of connectionID's bindings whose prepared report is stored
// and whose closed report is not, in UTF-8 byte order.
func ConnectionDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) ([]string, error) {
	return scanIDs(tx.Query(ctx, `SELECT DISTINCT s.drive_id FROM computer_bindings b
		JOIN drive_binding_sets s ON s.workspace_id=b.workspace_id AND s.binding_id=b.binding_id
		WHERE b.workspace_id=$1 AND b.connection_id=$2 AND `+transferable("b")+` ORDER BY s.drive_id`, string(ws), connectionID))
}

// DriveListeners is the connections among connectionIDs whose ready declared
// drives.mount and which have a binding whose prepared report is stored and
// whose closed report is not with driveID in its drive set: the streams a
// commit of driveID is announced on.
func DriveListeners(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID string, connectionIDs []string) ([]string, error) {
	return scanIDs(tx.Query(ctx, `SELECT DISTINCT b.connection_id FROM computer_bindings b
		JOIN drive_binding_sets s ON s.workspace_id=b.workspace_id AND s.binding_id=b.binding_id AND s.drive_id=$2
		JOIN instance_connections c ON c.workspace_id=b.workspace_id AND c.id=b.connection_id
		WHERE b.workspace_id=$1 AND b.connection_id=ANY($3) AND `+transferable("b")+` AND c.capabilities#>>'{drives,mount}'='true'
		ORDER BY b.connection_id`, string(ws), driveID, connectionIDs))
}

func scanIDs(rows *dbconnect.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
