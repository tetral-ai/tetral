package eventstream

import (
	"context"
	"database/sql"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// ReadPreviewRequest authenticates private preview identity against the existing
// Start and thread ledger. A broker frame supplies lookup keys, never authority.
func (r *PostgreSQLReader) ReadPreviewRequest(ctx context.Context, ws workspace.ID, sessionID, threadID, modelRequestID, startEventID string) (PreviewRequest, error) {
	if err := validateThreadReaderScope(ws, sessionID, threadID); err != nil {
		return PreviewRequest{}, err
	}
	if modelRequestID == "" || startEventID == "" {
		return PreviewRequest{}, &httpapi.ValidationError{Message: "model request identity is required"}
	}
	if r == nil || r.client == nil {
		return PreviewRequest{}, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	var result PreviewRequest
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.read_preview_request", func(tx *dbconnect.Tx) error {
		if err := ensureReadableThreadTx(ctx, tx, ws, sessionID, threadID); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `SELECT started.insert_stream_position,COALESCE(started.projection_json::jsonb ->> 'request_kind',''),
   t.role,t.visibility,t.role='main',EXISTS (SELECT 1 FROM session_events ended
    WHERE ended.workspace_id=started.workspace_id AND ended.session_id=started.session_id
     AND ended.session_thread_id=started.session_thread_id AND ended.model_request_id=started.model_request_id
     AND ended.type='span.model_request_end' AND ended.payload_json::jsonb ->> 'model_request_start_id'=started.event_id)
   FROM session_events started JOIN session_threads t ON t.workspace_id=started.workspace_id
    AND t.session_id=started.session_id AND t.id=started.session_thread_id
   WHERE started.workspace_id=$1 AND started.session_id=$2 AND started.session_thread_id=$3
    AND started.model_request_id=$4 AND started.event_id=$5 AND started.type='span.model_request_start'
    AND started.visibility='public' AND started.session_visible=TRUE`, string(ws), sessionID, threadID, modelRequestID, startEventID).
			Scan(&result.StartStreamPosition, &result.RequestKind, &result.ThreadRole, &result.ThreadVisibility, &result.IsPrimaryThread, &result.Ended)
		if dbconnect.IsNoRows(err) {
			return &httpapi.NotFoundError{Message: "model request start not found"}
		}
		return err
	})
	return result, err
}

// ListRequestFinalMessages expands only a database-proven End, paging the
// existing request/thread sequence index. It never materializes deferred bodies
// from the ordinary change feed or changes history-list ordering keys.
//
// Session scope is valid only for an already-open Session feed that has just
// read the End. Its deletion gate is keyed by the End's own insert position,
// as the change feed keys it by the cursor, so an End ordered before the
// session's permanent deletion event stays expandable until that deletion is
// delivered. Bodies come from permanent events, so change retention never
// interrupts a selected End group; each page is its own short snapshot.
// Thread scope uses the thread readability gate and closes on deletion.
func (r *PostgreSQLReader) ListRequestFinalMessages(ctx context.Context, scope ReadScope, endEventID string, afterSequence int64, limit int) ([]RequestFinalMessage, error) {
	if err := validateReaderScope(scope.WorkspaceID, scope.SessionID); err != nil {
		return nil, err
	}
	if endEventID == "" {
		return nil, &httpapi.ValidationError{Message: "model request end is required"}
	}
	if r == nil || r.client == nil {
		return nil, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	// One text body is the publication contract, not a tunable batching policy.
	if limit != 1 {
		return nil, &httpapi.ValidationError{Message: "request final page limit must be one"}
	}
	result := []RequestFinalMessage{}
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(scope.WorkspaceID), "eventstream.list_request_final_messages", func(tx *dbconnect.Tx) error {
		if scope.ThreadID == "" {
			var endPosition int64
			err := tx.QueryRow(ctx, `SELECT insert_stream_position FROM session_events
   WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3 AND type='span.model_request_end'`, string(scope.WorkspaceID), scope.SessionID, endEventID).Scan(&endPosition)
			if dbconnect.IsNoRows(err) {
				return &httpapi.NotFoundError{Message: "model request end not found"}
			}
			if err != nil {
				return err
			}
			if err := ensureReadableSessionFeedTx(ctx, tx, scope.WorkspaceID, scope.SessionID, endPosition); err != nil {
				return err
			}
		} else {
			if err := ensureReadableThreadTx(ctx, tx, scope.WorkspaceID, scope.SessionID, scope.ThreadID); err != nil {
				return err
			}
		}
		var threadID, modelRequestID string
		var startSequence, endSequence int64
		err := tx.QueryRow(ctx, `SELECT ended.session_thread_id,ended.model_request_id,started.sequence,ended.sequence
   FROM session_events ended JOIN session_events started ON started.workspace_id=ended.workspace_id
    AND started.session_id=ended.session_id AND started.session_thread_id=ended.session_thread_id
    AND started.model_request_id=ended.model_request_id AND started.type='span.model_request_start'
    AND started.event_id=ended.payload_json::jsonb ->> 'model_request_start_id'
   JOIN session_threads t ON t.workspace_id=ended.workspace_id AND t.session_id=ended.session_id AND t.id=ended.session_thread_id
   WHERE ended.workspace_id=$1 AND ended.session_id=$2 AND ended.event_id=$3 AND ended.type='span.model_request_end'
    AND ended.visibility='public' AND started.visibility='public' AND t.visibility='public' AND t.role<>'approval_reviewer'
    AND (($4='' AND ended.session_visible=TRUE) OR ended.session_thread_id=NULLIF($4,''))
    AND started.sequence<ended.sequence`, string(scope.WorkspaceID), scope.SessionID, endEventID, scope.ThreadID).
			Scan(&threadID, &modelRequestID, &startSequence, &endSequence)
		if dbconnect.IsNoRows(err) {
			return &httpapi.NotFoundError{Message: "model request end not found"}
		}
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT event_id,sequence,payload_json,processed_at FROM session_events
   WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND model_request_id=$4
    AND type='agent.message' AND visibility='public' AND ($5<>'' OR session_visible=TRUE)
    AND sequence>$6 AND sequence>$7 AND sequence<$8 ORDER BY sequence ASC LIMIT 1`,
			string(scope.WorkspaceID), scope.SessionID, threadID, modelRequestID, scope.ThreadID, afterSequence, startSequence, endSequence)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var message RequestFinalMessage
			var payload string
			var processed sql.NullTime
			if err := rows.Scan(&message.Event.ID, &message.Sequence, &payload, &processed); err != nil {
				return err
			}
			message.Event.SessionID = scope.SessionID
			message.Event.ThreadID = threadID
			message.Event.Type = "agent.message"
			message.Event.Payload = []byte(payload)
			if processed.Valid {
				formatted := processed.Time.UTC().Format(time.RFC3339Nano)
				message.Event.ProcessedAt = &formatted
			}
			result = append(result, message)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
