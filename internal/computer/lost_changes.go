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
// stores a close report of instanceID or accepts its ready, to the report's
// or ready's entries. requestMessageID is the close report's
// request_message_id, and "" for a ready. An entry that names no
// session.close Engine issued to instanceID (LostChangeUnknownInstruction),
// or that names another one than requestMessageID
// (LostChangeRequestMismatch), is skipped and returned for the caller to log
// after commit. Any other entry's
// (request_message_id, drive_id) is inserted into computer_lost_changes,
// and only a new row writes a drive_changes_lost_error, retry_status
// terminal, on the Session of the computer that close was issued to. The
// Sessions' errors are written in Session ID order, each under its runtime
// mutation lock (WriteSessionErrors).
func RecordLostChanges(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, requestMessageID string, entries []protocol.LostChange, now time.Time) ([]SkippedLostChange, error) {
	var skipped []SkippedLostChange
	errs := map[string][]SessionError{}
	for _, entry := range entries {
		var sessionID, computerID string
		err := tx.QueryRow(ctx, `SELECT session_id,computer_id FROM computer_bindings WHERE workspace_id=$1 AND instance_id=$2 AND close_message_id=$3`,
			string(ws), instanceID, entry.RequestMessageID).Scan(&sessionID, &computerID)
		if errors.Is(err, sql.ErrNoRows) {
			skipped = append(skipped, SkippedLostChange{RequestMessageID: entry.RequestMessageID, Reason: LostChangeUnknownInstruction})
			continue
		}
		if err != nil {
			return nil, err
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
			Message: fmt.Sprintf("%d changes to drive %s could not be confirmed saved before computer %s was removed.", entry.PathsTotal, entry.DriveID, computerID),
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
