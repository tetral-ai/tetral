package computer

import (
	"context"
	"maps"
	"slices"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The Session sets hubflow reads, without locks, before it takes the runtime
// mutation locks of the Sessions a connection's end, a deregistration or a
// ready changes, and reads again once it holds its rows. Each is in Session
// ID order, without repeats.

// ConnectionSessions returns the Sessions with a binding on connectionID in
// state preparing, prepared, failed or closing: those whose computers and
// bindings the connection's end changes (InvalidateConnection).
func ConnectionSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) ([]string, error) {
	return sessionIDs(tx.Query(ctx, `SELECT DISTINCT session_id FROM computer_bindings
		WHERE workspace_id=$1 AND connection_id=$2 AND state IN ('preparing','prepared','failed','closing') ORDER BY session_id`, string(ws), connectionID))
}

// InstanceSessions returns the Sessions with an attached computer whose
// current instance is instanceID, a detached one there whose closure is
// unknown, or a request for the instance that is claimed or, for a wake
// request, pending: those a ready of the instance changes
// (FulfilRequests, IssuePreparationsOn, IssueLateCloses) and its
// deregistration changes (AttachedOn, OpenWakeRequests).
func InstanceSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) ([]string, error) {
	return sessionIDs(tx.Query(ctx, `SELECT session_id FROM computers WHERE workspace_id=$1 AND current_instance_id=$2
		 AND (association_state='attached' OR execution_state='unknown')
		UNION SELECT session_id FROM computer_requests WHERE workspace_id=$1 AND instance_id=$2
		 AND (state='claimed' OR (kind='wake' AND state='pending'))
		ORDER BY session_id`, string(ws), instanceID))
}

// LostChangeSessions returns the Sessions of the session.close and
// session.drives instructions issued to instanceID that entries name: those
// RecordLostChanges may write an error to.
func LostChangeSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, entries []protocol.LostChange) ([]string, error) {
	seen := map[string]bool{}
	for _, entry := range entries {
		sessionID, _, _, found, err := lostChangesInstruction(ctx, tx, ws, instanceID, entry.RequestMessageID)
		if err != nil {
			return nil, err
		}
		if found {
			seen[sessionID] = true
		}
	}
	return slices.Sorted(maps.Keys(seen)), nil
}

func sessionIDs(rows *dbconnect.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sessions []string
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, err
		}
		sessions = append(sessions, sessionID)
	}
	return sessions, rows.Err()
}
