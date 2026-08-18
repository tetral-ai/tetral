// Package childcontrol owns the derived durable no-new-work predicate shared by
// public input admission and Bridge runtime writers. The predicate reads only
// existing Thread, internal control-event, Runtime Inbox, and lifecycle receipt
// facts; it introduces no second control state.
package childcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

type fenceCandidate struct {
	targetThreadID string
	rootThreadID   string
	action         string
	sourceID       string
	inboxStatus    string
}

// ThreadOrAncestorClosingTx reports whether the addressed Thread is fenced by
// an admitted child interrupt/close on itself or any ancestor. Callers must
// already hold the Session mutation lock so admission cannot race this read.
func ThreadOrAncestorClosingTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, threadID string) (bool, error) {
	return threadOrAncestorClosingTx(ctx, tx, workspaceID, sessionID, threadID, false)
}

// ThreadOrAncestorClosingOrClosedTx extends the admission fence for producers
// whose durable result must wait in parked custody after a child has closed.
// Reactivation continues to use ThreadOrAncestorClosingTx so it can reopen the
// exact closed subtree.
func ThreadOrAncestorClosingOrClosedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, threadID string) (bool, error) {
	return threadOrAncestorClosingTx(ctx, tx, workspaceID, sessionID, threadID, true)
}

func threadOrAncestorClosingTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, threadID string, includeClosed bool) (bool, error) {
	var targetStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3 FOR SHARE`, workspaceID, sessionID, threadID).Scan(&targetStatus); err != nil {
		return false, err
	}
	if targetStatus == "failed" || targetStatus == "terminated" {
		return false, nil
	}
	if includeClosed {
		var closed bool
		if err := tx.QueryRow(ctx, `WITH RECURSIVE ancestors(id,parent_thread_id,status) AS (
			SELECT id,parent_thread_id,status FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3
			UNION ALL
			SELECT parent.id,parent.parent_thread_id,parent.status
			  FROM session_threads parent JOIN ancestors child ON child.parent_thread_id=parent.id
			 WHERE parent.workspace_id=$1 AND parent.session_id=$2
		)
		SELECT EXISTS (SELECT 1 FROM ancestors WHERE status='closed_for_runtime')`, workspaceID, sessionID, threadID).Scan(&closed); err != nil {
			return false, err
		}
		if closed {
			return true, nil
		}
	}
	rows, err := tx.Query(ctx, `WITH RECURSIVE ancestors(id,parent_thread_id,status) AS (
		SELECT id,parent_thread_id,status FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		UNION ALL
		SELECT parent.id,parent.parent_thread_id,parent.status FROM session_threads parent JOIN ancestors child ON child.parent_thread_id=parent.id
		 WHERE parent.workspace_id=$1 AND parent.session_id=$2
	)
	SELECT event.session_thread_id,
	       event.payload_json::jsonb ->> 'root_child_thread_id',
	       event.payload_json::jsonb ->> 'action',
	       event.payload_json::jsonb ->> 'source_tool_use_event_id',
	       COALESCE(inbox.status,'')
	  FROM session_events event
	  JOIN ancestors ON ancestors.id=event.session_thread_id
	  LEFT JOIN session_runtime_inbox inbox
	    ON inbox.workspace_id=event.workspace_id
	   AND inbox.runtime_input_id=event.payload_json::jsonb ->> 'runtime_input_id'
	 WHERE event.workspace_id=$1 AND event.session_id=$2
	   AND event.type='agent.thread_interrupt_requested'
	   AND event.payload_json::jsonb ->> 'disposition'='pending_control'
	   AND ancestors.status NOT IN ('failed','terminated')
	 FOR SHARE OF event`, workspaceID, sessionID, threadID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	var candidates []fenceCandidate
	for rows.Next() {
		var candidate fenceCandidate
		if err := rows.Scan(&candidate.targetThreadID, &candidate.rootThreadID, &candidate.action, &candidate.sourceID, &candidate.inboxStatus); err != nil {
			return false, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	for _, candidate := range candidates {
		switch candidate.inboxStatus {
		case "queued", "delivering", "accepted", "parked":
			return true, nil
		case "committed":
			terminal, err := sourceToolTerminalTx(ctx, tx, workspaceID, sessionID, candidate.sourceID)
			if err != nil {
				return false, err
			}
			if terminal {
				continue
			}
			if candidate.action != "close" {
				continue
			}
			closed, err := matchingCloseReceiptExistsTx(ctx, tx, workspaceID, sessionID, candidate.targetThreadID, candidate.rootThreadID, candidate.sourceID)
			if err != nil {
				return false, err
			}
			if !closed {
				return true, nil
			}
		}
	}
	return false, nil
}

func sourceToolTerminalTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, sourceID string) (bool, error) {
	var terminal bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM session_events result
		WHERE result.workspace_id=$1 AND result.session_id=$2
		 AND result.type IN ('agent.tool_result','agent.mcp_tool_result')
		 AND COALESCE(result.payload_json::jsonb->>'tool_use_event_id',result.payload_json::jsonb->>'tool_use_id',result.payload_json::jsonb->>'mcp_tool_use_id')=$3)`,
		workspaceID, sessionID, sourceID).Scan(&terminal)
	return terminal, err
}

func matchingCloseReceiptExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, targetThreadID, rootThreadID, sourceID string) (bool, error) {
	operationID := stableID("child_tree_close", sourceID, rootThreadID)
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_bridge_operations WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND operation='close_child_control' AND source_kind='child_close_command' AND idempotency_key=$4)`, workspaceID, sessionID, targetThreadID, operationID).Scan(&exists)
	return exists, err
}

func stableID(parts ...string) string {
	hasher := sha256.New()
	var length [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(length[:], uint32(len([]byte(part))))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	return "stid_" + hex.EncodeToString(hasher.Sum(nil))
}
