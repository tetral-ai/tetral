// Package sessioneventwrite writes the Session event feed. InsertInitialTx
// inserts an event's first revision together with its one feed change, and
// RecordProcessedRevisionTx records the later processed revision of an
// admitted input. Both run in the caller's open transaction after the caller
// has taken its Session mutation serialization and any Thread or process
// fence; neither authenticates, locks business rows, commits, nor opens
// another transaction. The package depends on the database client only.
package sessioneventwrite

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// reserveStreamPositionSQL reserves the next feed position from the change
// table's identity sequence. Positions are global and gap-tolerant: a rolled
// back writer leaves its reserved value unused, and readers are Session-scoped.
const reserveStreamPositionSQL = `SELECT nextval(pg_get_serial_sequence('public.session_event_stream_changes', 'stream_position'))`

// InitialEvent is one complete Session event as first written. Empty values
// of the nullable scalar columns (SessionThreadID, RuntimeWriteID,
// ModelRequestID, ModelToolCallID, ToolUseEventID) store NULL; an empty
// ProjectionJSON stores the column default '{}'. CreatedAt is also the
// event's updated_at and its change's changed_at. A nil ProcessedAt leaves an
// admitted input unprocessed.
type InitialEvent struct {
	WorkspaceID     string
	SessionID       string
	SessionThreadID string
	EventID         string
	Sequence        int64
	Type            string
	PayloadJSON     string
	ProjectionJSON  string
	Visibility      string
	SessionVisible  bool
	RuntimeWriteID  string
	ModelRequestID  string
	ModelToolCallID string
	ToolUseEventID  string
	CreatedAt       time.Time
	ProcessedAt     *time.Time
}

// InsertInitialTx reserves one feed position, inserts the complete event as
// revision 1 with both stream positions set to it, then inserts the matching
// revision-1 change at exactly that position. It returns the position. An
// error from either INSERT is returned unchanged so the caller can map
// constraint names; the caller must then roll back.
func InsertInitialTx(ctx context.Context, tx *dbconnect.Tx, event InitialEvent) (int64, error) {
	var position int64
	if err := tx.QueryRow(ctx, reserveStreamPositionSQL).Scan(&position); err != nil {
		return 0, err
	}
	projectionJSON := event.ProjectionJSON
	if projectionJSON == "" {
		projectionJSON = "{}"
	}
	var processedAt any
	if event.ProcessedAt != nil {
		processedAt = *event.ProcessedAt
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, revision, type, payload_json,
			visibility, session_visible, latest_stream_position, insert_stream_position,
			runtime_write_id, model_request_id, projection_json, model_tool_call_id, tool_use_event_id,
			created_at, updated_at, processed_at
		) VALUES (
			$1, $2, NULLIF($3, ''), $4, $5, 1, $6, $7,
			$8, $9, $10, $10,
			NULLIF($11, ''), NULLIF($12, ''), $13, NULLIF($14, ''), NULLIF($15, ''),
			$16, $16, $17
		)`,
		event.WorkspaceID, event.SessionID, event.SessionThreadID, event.EventID, event.Sequence, event.Type, event.PayloadJSON,
		event.Visibility, event.SessionVisible, position,
		event.RuntimeWriteID, event.ModelRequestID, projectionJSON, event.ModelToolCallID, event.ToolUseEventID,
		event.CreatedAt, processedAt,
	); err != nil {
		return 0, err
	}
	if err := insertChangeTx(ctx, tx, change{
		workspaceID: event.WorkspaceID, sessionID: event.SessionID, sessionThreadID: event.SessionThreadID,
		eventID: event.EventID, position: position, revision: 1,
		visibility: event.Visibility, sessionVisible: event.SessionVisible, changedAt: event.CreatedAt,
	}); err != nil {
		return 0, err
	}
	return position, nil
}

// ProcessedRevision identifies an admitted input event to mark processed. A
// nonempty SessionThreadID additionally requires the event to belong to that
// Thread.
type ProcessedRevision struct {
	WorkspaceID     string
	SessionID       string
	SessionThreadID string
	EventID         string
	ProcessedAt     time.Time
}

// Revision is the feed revision RecordProcessedRevisionTx wrote.
type Revision struct {
	SessionThreadID string
	Revision        int64
	StreamPosition  int64
}

// RecordProcessedRevisionTx reserves one feed position, stamps processed_at
// on the still-unprocessed event while incrementing its revision and setting
// its latest position, and inserts that revision's change. The event's insert
// position is never changed. It reports false, writing nothing but the
// reserved gap, when the event is absent from the scope or already processed.
func RecordProcessedRevisionTx(ctx context.Context, tx *dbconnect.Tx, request ProcessedRevision) (Revision, bool, error) {
	var position int64
	if err := tx.QueryRow(ctx, reserveStreamPositionSQL).Scan(&position); err != nil {
		return Revision{}, false, err
	}
	var revision Revision
	var visibility string
	var sessionVisible bool
	err := tx.QueryRow(ctx,
		`UPDATE session_events
		    SET processed_at = $5,
		        updated_at = $5,
		        revision = revision + 1,
		        latest_stream_position = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3
		    AND ($4 = '' OR session_thread_id = $4)
		    AND processed_at IS NULL
		  RETURNING revision, visibility, session_visible, COALESCE(session_thread_id, '')`,
		request.WorkspaceID, request.SessionID, request.EventID, request.SessionThreadID, request.ProcessedAt, position,
	).Scan(&revision.Revision, &visibility, &sessionVisible, &revision.SessionThreadID)
	if dbconnect.IsNoRows(err) {
		return Revision{}, false, nil
	}
	if err != nil {
		return Revision{}, false, err
	}
	if err := insertChangeTx(ctx, tx, change{
		workspaceID: request.WorkspaceID, sessionID: request.SessionID, sessionThreadID: revision.SessionThreadID,
		eventID: request.EventID, position: position, revision: revision.Revision,
		visibility: visibility, sessionVisible: sessionVisible, changedAt: request.ProcessedAt,
	}); err != nil {
		return Revision{}, false, err
	}
	revision.StreamPosition = position
	return revision, true, nil
}

type change struct {
	workspaceID     string
	sessionID       string
	sessionThreadID string
	eventID         string
	position        int64
	revision        int64
	visibility      string
	sessionVisible  bool
	changedAt       time.Time
}

// insertChangeTx writes one change at its reserved position. OVERRIDING SYSTEM
// VALUE keeps the identity column from drawing a second value; the immediate
// event foreign key requires the event row to exist first.
func insertChangeTx(ctx context.Context, tx *dbconnect.Tx, value change) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, stream_position, event_id, session_thread_id,
			revision, visibility, session_visible, changed_at
		) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7, $8, $9)`,
		value.workspaceID, value.sessionID, value.position, value.eventID, value.sessionThreadID,
		value.revision, value.visibility, value.sessionVisible, value.changedAt,
	)
	return err
}
