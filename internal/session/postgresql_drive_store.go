package session

import (
	"context"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/protocol"
)

func (t *postgresqlTransaction) DriveBindings(ctx context.Context, sessionID string) ([]DriveBinding, error) {
	listed, err := t.listDriveBindings(ctx, []string{sessionID})
	return listed[sessionID], err
}

// CreateDriveBindings stores the drive bindings of a new Session, each drive
// locked FOR SHARE as a live drive of the workspace.
func (t *postgresqlTransaction) CreateDriveBindings(ctx context.Context, sessionID string, bindings []DriveBinding) error {
	for _, binding := range bindings {
		if err := t.insertDriveBinding(ctx, sessionID, binding); err != nil {
			return err
		}
	}
	return nil
}

// SetDriveBinding stores binding, a new binding of a live drive or a new
// access of a bound one, after the idle check of resource mutations.
func (t *postgresqlTransaction) SetDriveBinding(ctx context.Context, sessionID string, binding DriveBinding) error {
	if err := t.requireIdleRuntimeStatusForResourceMutation(ctx, sessionID); err != nil {
		return err
	}
	return t.insertDriveBinding(ctx, sessionID, binding)
}

func (t *postgresqlTransaction) insertDriveBinding(ctx context.Context, sessionID string, binding DriveBinding) error {
	tx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	if err := drive.ShareLiveDrive(ctx, tx, t.workspaceID, binding.DriveID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO session_drive_bindings (workspace_id,session_id,drive_id,access) VALUES ($1,$2,$3,$4)
		ON CONFLICT (workspace_id,session_id,drive_id) DO UPDATE SET access=EXCLUDED.access`,
		string(t.workspaceID), sessionID, binding.DriveID, binding.Access)
	return err
}

// DeleteDriveBinding removes the binding of driveID after the idle check of
// resource mutations.
func (t *postgresqlTransaction) DeleteDriveBinding(ctx context.Context, sessionID string, driveID string) error {
	if err := t.requireIdleRuntimeStatusForResourceMutation(ctx, sessionID); err != nil {
		return err
	}
	tx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM session_drive_bindings WHERE workspace_id=$1 AND session_id=$2 AND drive_id=$3`,
		string(t.workspaceID), sessionID, driveID)
	return err
}

func (t *postgresqlTransaction) CheckDriveBindingsFit(ctx context.Context, sessionID string, bindings []DriveBinding) error {
	tx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	return computer.CheckDrivesFit(ctx, tx, t.workspaceID, sessionID, protocolDrives(bindings))
}

func (t *postgresqlTransaction) SendDriveBindings(ctx context.Context, sessionID string, bindings []DriveBinding) error {
	tx, err := t.rawDBTx()
	if err != nil {
		return err
	}
	return computer.SendDrives(ctx, tx, t.workspaceID, sessionID, protocolDrives(bindings))
}

// attachDrives fills the drive bindings of sessions with one read.
func (t *postgresqlTransaction) attachDrives(ctx context.Context, sessions []*Session) error {
	ids := make([]string, len(sessions))
	for i, sess := range sessions {
		ids[i] = sess.ID
	}
	listed, err := t.listDriveBindings(ctx, ids)
	if err != nil {
		return err
	}
	for _, sess := range sessions {
		sess.Drives = listed[sess.ID]
	}
	return nil
}

// listDriveBindings reads the drive bindings of sessionIDs, each Session's
// in UTF-8 byte order of drive_id; a Session without any has an empty list.
func (t *postgresqlTransaction) listDriveBindings(ctx context.Context, sessionIDs []string) (map[string][]DriveBinding, error) {
	tx, err := t.rawDBTx()
	if err != nil {
		return nil, err
	}
	listed := make(map[string][]DriveBinding, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		listed[sessionID] = []DriveBinding{}
	}
	if len(sessionIDs) == 0 {
		return listed, nil
	}
	rows, err := tx.Query(ctx, `SELECT session_id,drive_id,access FROM session_drive_bindings WHERE workspace_id=$1 AND session_id=ANY($2)
		ORDER BY session_id,drive_id`, string(t.workspaceID), sessionIDs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sessionID string
		var binding DriveBinding
		if err := rows.Scan(&sessionID, &binding.DriveID, &binding.Access); err != nil {
			return nil, err
		}
		listed[sessionID] = append(listed[sessionID], binding)
	}
	return listed, rows.Err()
}

// protocolDrives is bindings as a session.prepare or session.drives carries
// them.
func protocolDrives(bindings []DriveBinding) []protocol.DriveBinding {
	drives := make([]protocol.DriveBinding, len(bindings))
	for i, binding := range bindings {
		drives[i] = protocol.DriveBinding{DriveID: binding.DriveID, Access: binding.Access}
	}
	return drives
}
