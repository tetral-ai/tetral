package eventstream

import (
	"context"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// MaxSessionSignalBatch bounds the Sessions one shared idle check reads in a
// single statement.
const MaxSessionSignalBatch = 128

// SessionSignal is the coarse idle signal of one Session: whether it exists,
// its lifecycle state and Position, the greater of its newest retained change
// of any visibility and the highest pruned_through among its feeds (zero when
// neither exists). It is shared by every Session and Thread viewer of that
// Session, so it is deliberately coarser than any feed: a different signal
// only tells viewers to run their own scoped read. It never advances a cursor
// or proves a feed complete. Including the retention watermark keeps a change
// that was inserted and pruned between two checks visible as a difference.
type SessionSignal struct {
	SessionID      string
	Exists         bool
	LifecycleState string
	Position       int64
}

// sessionSignalsQuery reads one workspace's signals for at most
// MaxSessionSignalBatch Session IDs in one statement: the Session header by
// primary key, then two top-one seeks per Session, over the change primary
// key (workspace_id, session_id, stream_position) and the feed metadata index
// (workspace_id, session_id, pruned_through DESC).
const sessionSignalsQuery = `SELECT ids.session_id, s.id IS NOT NULL, COALESCE(s.lifecycle_state, ''),
  GREATEST(COALESCE(newest.stream_position, 0), COALESCE(pruned.pruned_through, 0))
  FROM unnest($2::text[]) AS ids(session_id)
  LEFT JOIN sessions s ON s.workspace_id = $1 AND s.id = ids.session_id
  LEFT JOIN LATERAL (
    SELECT c.stream_position FROM session_event_stream_changes c
     WHERE c.workspace_id = $1 AND c.session_id = ids.session_id
     ORDER BY c.stream_position DESC LIMIT 1) newest ON TRUE
  LEFT JOIN LATERAL (
    SELECT r.pruned_through FROM session_event_feed_retention r
     WHERE r.workspace_id = $1 AND r.session_id = ids.session_id
     ORDER BY r.pruned_through DESC LIMIT 1) pruned ON TRUE`

// ReadSessionSignals returns one signal per requested Session of one
// workspace, in a single read-only statement. The caller passes unique IDs; a
// missing Session yields Exists = false rather than an error.
func (r *PostgreSQLReader) ReadSessionSignals(ctx context.Context, ws workspace.ID, sessionIDs []string) ([]SessionSignal, error) {
	if ws == "" {
		return nil, &httpapi.ValidationError{Message: "workspace_id is required"}
	}
	if len(sessionIDs) == 0 || len(sessionIDs) > MaxSessionSignalBatch {
		return nil, &httpapi.ValidationError{Message: "session signal batch size is invalid"}
	}
	if r == nil || r.client == nil {
		return nil, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	signals := make([]SessionSignal, 0, len(sessionIDs))
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.session_signals", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx, sessionSignalsQuery, string(ws), sessionIDs)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var signal SessionSignal
			if err := rows.Scan(&signal.SessionID, &signal.Exists, &signal.LifecycleState, &signal.Position); err != nil {
				return err
			}
			signals = append(signals, signal)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return signals, nil
}
