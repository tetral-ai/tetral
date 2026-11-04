package computer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Reasons a lost_changes entry is skipped, the reason of its
// client.lost_changes_ignored record.
const (
	LostChangeRequestMismatch    = "request_message_id_mismatch"
	LostChangeUnknownInstruction = "unknown_instruction"
)

// SkippedLostChange is a lost_changes entry the rule skipped: the
// instruction it names and why.
type SkippedLostChange struct {
	RequestMessageID string
	Reason           string
}

// RecordLostChanges applies the lost_changes rule, in the transaction that
// stores a close or session.drives report of instanceID or accepts its
// ready, to the report's or ready's entries. requestMessageID is the
// report's request_message_id, and "" for a ready. An entry that names no
// session.close or session.drives Engine issued to instanceID
// (LostChangeUnknownInstruction), or that names another one than
// requestMessageID (LostChangeRequestMismatch), is skipped and returned for
// the caller to log after commit. Any other entry's (request_message_id,
// drive_id) is inserted into computer_lost_changes, and only a new row
// writes a drive_changes_lost_error, retry_status terminal, on the Session
// of the computer that instruction was issued to. The Sessions' errors are
// written in Session ID order, each under its runtime mutation lock
// (WriteSessionErrors).
func RecordLostChanges(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, requestMessageID string, entries []protocol.LostChange, now time.Time) ([]SkippedLostChange, error) {
	var skipped []SkippedLostChange
	errs := map[string][]SessionError{}
	for _, entry := range entries {
		sessionID, computerID, drives, found, err := lostChangesInstruction(ctx, tx, ws, instanceID, entry.RequestMessageID)
		if err != nil {
			return nil, err
		}
		if !found {
			skipped = append(skipped, SkippedLostChange{RequestMessageID: entry.RequestMessageID, Reason: LostChangeUnknownInstruction})
			continue
		}
		if requestMessageID != "" && entry.RequestMessageID != requestMessageID {
			skipped = append(skipped, SkippedLostChange{RequestMessageID: entry.RequestMessageID, Reason: LostChangeRequestMismatch})
			continue
		}
		err = tx.QueryRow(ctx, `INSERT INTO computer_lost_changes (workspace_id,request_message_id,drive_id,session_id,computer_id)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING drive_id`, string(ws), entry.RequestMessageID, entry.DriveID, sessionID, computerID).Scan(new(string))
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		paths := entry.Paths
		if paths == nil {
			paths = []string{}
		}
		errs[sessionID] = append(errs[sessionID], SessionError{Terminal: true, Type: "drive_changes_lost_error",
			Message: lostChangesMessage(entry, computerID, drives),
			Fields:  map[string]any{"computer_id": computerID, "drive_id": entry.DriveID, "paths": paths, "paths_total": entry.PathsTotal}})
	}
	sessions := make([]string, 0, len(errs))
	for sessionID := range errs {
		sessions = append(sessions, sessionID)
	}
	sort.Strings(sessions)
	for _, sessionID := range sessions {
		if err := WriteSessionErrors(ctx, tx, ws, sessionID, now, errs[sessionID]); err != nil {
			return nil, err
		}
	}
	return skipped, nil
}

// lostChangesMessage is the message of the drive_changes_lost_error of
// entry, abandoned by a session.drives when drives is true and by a
// session.close otherwise.
func lostChangesMessage(entry protocol.LostChange, computerID string, drives bool) string {
	if drives {
		return fmt.Sprintf("%d changes to drive %s could not be confirmed saved on computer %s before the Session's drive bindings changed.", entry.PathsTotal, entry.DriveID, computerID)
	}
	return fmt.Sprintf("%d changes to drive %s could not be confirmed saved before computer %s was removed.", entry.PathsTotal, entry.DriveID, computerID)
}

// lostChangesInstruction reads the Session and computer of the session.close
// or session.drives messageID issued to instanceID, and whether it is a
// session.drives; found is false when Engine issued no such instruction
// there.
func lostChangesInstruction(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, messageID string) (string, string, bool, bool, error) {
	var sessionID, computerID string
	err := tx.QueryRow(ctx, `SELECT session_id,computer_id FROM computer_bindings WHERE workspace_id=$1 AND instance_id=$2 AND close_message_id=$3`,
		string(ws), instanceID, messageID).Scan(&sessionID, &computerID)
	if err == nil {
		return sessionID, computerID, false, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", "", false, false, err
	}
	err = tx.QueryRow(ctx, `SELECT b.session_id,b.computer_id FROM connection_instructions i
		JOIN instance_connections c ON c.workspace_id=i.workspace_id AND c.id=i.connection_id
		JOIN computer_bindings b ON b.workspace_id=i.workspace_id AND b.binding_id=convert_from(i.envelope,'UTF8')::jsonb#>>'{payload,binding_id}'
		WHERE i.workspace_id=$1 AND i.message_id=$3 AND i.type='session.drives' AND c.instance_id=$2`,
		string(ws), instanceID, messageID).Scan(&sessionID, &computerID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, false, nil
	}
	return sessionID, computerID, true, err == nil, err
}
