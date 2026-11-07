package drive

import (
	"context"
	"sort"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Drive accesses of a binding.
const (
	AccessReadOnly  = "read_only"
	AccessReadWrite = "read_write"
)

// RecordBindingDrives stores drives, the drives of the session.prepare or
// session.drives messageID sent to bindingID, in the transaction that issues
// it, under that instruction's issue_seq on the binding's connection.
func RecordBindingDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, bindingID, messageID string, drives []protocol.DriveBinding) error {
	for _, binding := range drives {
		if _, err := tx.Exec(ctx, `INSERT INTO drive_binding_sets (workspace_id,binding_id,send_seq,message_id,drive_id,access)
			SELECT $1,$2,issue_seq,$3,$4,$5 FROM connection_instructions WHERE workspace_id=$1 AND message_id=$3`,
			string(ws), bindingID, messageID, binding.DriveID, binding.Access); err != nil {
			return err
		}
	}
	return nil
}

// ApplyBindingDrivesReport retires, in the transaction that stores the report
// of the session.drives messageID sent to bindingID, the binding's drives of
// every earlier send, so the binding's drive set becomes that message's.
func ApplyBindingDrivesReport(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, bindingID, messageID string) error {
	_, err := tx.Exec(ctx, `DELETE FROM drive_binding_sets WHERE workspace_id=$1 AND binding_id=$2
		AND send_seq<(SELECT issue_seq FROM connection_instructions WHERE workspace_id=$1 AND message_id=$3)`, string(ws), bindingID, messageID)
	return err
}

// BindingDriveSet is bindingID's drive set: the union of the drives it was
// sent that no applied report retired, in UTF-8 byte order of drive_id, each
// read_write when any of those sends gave read_write.
func BindingDriveSet(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, bindingID string) ([]protocol.DriveBinding, error) {
	return scanDriveBindings(tx.Query(ctx, `SELECT drive_id,CASE WHEN bool_or(access='read_write') THEN 'read_write' ELSE 'read_only' END
		FROM drive_binding_sets WHERE workspace_id=$1 AND binding_id=$2 GROUP BY drive_id ORDER BY drive_id`, string(ws), bindingID))
}

// SentDrives is the drives of the one message messageID sent to bindingID, in
// UTF-8 byte order of drive_id.
func SentDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, bindingID, messageID string) ([]protocol.DriveBinding, error) {
	return scanDriveBindings(tx.Query(ctx, `SELECT drive_id,access FROM drive_binding_sets
		WHERE workspace_id=$1 AND binding_id=$2 AND message_id=$3 ORDER BY drive_id`, string(ws), bindingID, messageID))
}

// UnionDrives is the union of two drive sets in UTF-8 byte order of
// drive_id, each drive read_write when either set gives read_write.
func UnionDrives(a, b []protocol.DriveBinding) []protocol.DriveBinding {
	access := map[string]string{}
	for _, binding := range append(append([]protocol.DriveBinding{}, a...), b...) {
		if access[binding.DriveID] != AccessReadWrite {
			access[binding.DriveID] = binding.Access
		}
	}
	union := make([]protocol.DriveBinding, 0, len(access))
	for driveID, value := range access {
		union = append(union, protocol.DriveBinding{DriveID: driveID, Access: value})
	}
	sort.Slice(union, func(i, j int) bool { return union[i].DriveID < union[j].DriveID })
	return union
}

// SameDrives reports whether two drive sets hold the same (drive_id, access)
// pairs.
func SameDrives(a, b []protocol.DriveBinding) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[protocol.DriveBinding]bool, len(a))
	for _, binding := range a {
		seen[binding] = true
	}
	for _, binding := range b {
		if !seen[binding] {
			return false
		}
	}
	return true
}

func scanDriveBindings(rows *dbconnect.Rows, err error) ([]protocol.DriveBinding, error) {
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	bindings := []protocol.DriveBinding{}
	for rows.Next() {
		var binding protocol.DriveBinding
		if err := rows.Scan(&binding.DriveID, &binding.Access); err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, rows.Err()
}
