// Package eventstream serves the read-only public event list and Server-Sent
// Events change feed over the session event ledger. It never writes events,
// consumes queue jobs, executes tools, or calls Runtime Pod.
//
// OWNS:
//
//	Read RPCs over the ledger:
//	  ListSessionEvents / ListThreadEvents                 paged list APIs over session_events
//	  ListSessionEventChanges / ListThreadEventChanges     SSE change feed
//	  CurrentStreamPosition / CurrentThreadStreamPosition  SSE cursor head
//	Bounds: defaultStreamBatchSize (100) caps rows per change-feed fetch;
//	  defaultListLimit (20) and maxListLimit (100) bound the list page size.
//	Signed session_events page token (resource "session_events", version 3), in pagination.go.
//	Reads, never writes, four tables:
//	  session_event_stream_changes  SSE cursor movement (stream_position)
//	  session_events                event body (type, payload_json, processed_at) and list paging keys
//	  session_threads               thread visibility/role gate joined into every public read
//	  sessions                      lifecycle_state readability guard (a deleted session reads as not-found)
//
// STATE MACHINE (durable ledger keys this reader pages on; every writer named
// here lives OUTSIDE this package — the event append path in internal/sessionevent,
// internal/session, and services/bridge):
//
//	key                                          | states                  | writer (external) | reader (this pkg)                            | transitions
//	session_event_stream_changes.stream_position | absent -> N (IDENTITY)  | append path       | change feed + Current*StreamPosition (MAX)   | append-only, strictly increasing per (workspace,session); rows are never rewritten
//	session_events.insert_stream_position        | 0 (unset) -> N          | append path       | ListSessionEvents ordering + session cursor  | set once to the first stream_position the event appears at, immutable thereafter
//	session_events.sequence                      | assigned (thread-local) | append path       | ListThreadEvents ordering + thread cursor    | unique and stable per (workspace,session,thread); the thread-scoped paging key
//
// INVARIANTS:
//
//   - SSE cursor movement and the SSE head are read from
//     session_event_stream_changes (stream_position); the event body on each change
//     is JOINed in from session_events. The list APIs read session_events only and
//     never page over the change log.
//   - The session list orders and pages by the immutable insert_stream_position
//     (event_id tie-break), giving a stable session-global order across threads; the
//     thread list orders and pages by the thread-local sequence.
//   - Every read runs inside a workspace read-only transaction; the reader issues no
//     writes and touches no queue, tool, or Runtime Pod surface.
//   - Every public read excludes internal-visibility rows (visibility = 'public').
//     The session-hidden gate (session_visible = TRUE) is applied only by the
//     session-scoped reads — ListSessionEvents, CurrentStreamPosition,
//     ListSessionEventChanges; the thread-scoped reads (ListThreadEvents,
//     CurrentThreadStreamPosition, ListThreadEventChanges) intentionally omit it.
//     session_visible is independent of visibility, so a public but session-hidden
//     event (e.g. a non-whitelisted sub-thread agent.tool_use) is filtered from the
//     session views yet is still returned and counted by the thread views once the
//     thread passes the readability guard.
//   - Non-public and approval_reviewer threads are excluded two ways: the session
//     reads drop such events per row through the session_threads join, while the
//     thread reads gate the whole read once — a missing, non-public, or
//     approval_reviewer thread reads as not-found (ensureReadableThreadTx).
//
// UPDATE-WITH:
//
//   - internal/eventstream/postgresql_reader.go — the read queries themselves.
//   - internal/eventstream/pagination.go — page-token position/sequence keys.
//   - internal/eventstream/list.go — list limit bounds and query options.
//   - internal/storage/postgresql_schema.go — stream_position IDENTITY,
//     insert_stream_position default, and thread sequence uniqueness.
//   - internal/sessionevent/postgresql_store.go, internal/session/postgresql_store.go,
//     services/bridge/bridge_api_events.go — the append paths that
//     assign stream_position and freeze insert_stream_position.
package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const defaultStreamBatchSize = 100

type PostgreSQLReader struct {
	client          *dbconnect.Client
	pageTokenSecret []byte
}

type ReaderOption func(*PostgreSQLReader)

func WithPageTokenSecret(secret []byte) ReaderOption {
	return func(r *PostgreSQLReader) {
		r.pageTokenSecret = append([]byte(nil), secret...)
	}
}

func NewPostgreSQLReader(client *dbconnect.Client, opts ...ReaderOption) *PostgreSQLReader {
	reader := &PostgreSQLReader{client: client}
	for _, opt := range opts {
		opt(reader)
	}
	return reader
}

func (r *PostgreSQLReader) ListSessionEvents(ctx context.Context, ws workspace.ID, sessionID string, options ListOptions) (ListResult, error) {
	if err := validateReaderScope(ws, sessionID); err != nil {
		return ListResult{}, err
	}
	if r == nil || r.client == nil {
		return ListResult{}, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	options, err := normalizeReaderListOptions(options)
	if err != nil {
		return ListResult{}, err
	}
	var result ListResult
	err = r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.list_session_events", func(tx *dbconnect.Tx) error {
		if err := ensureReadableSessionTx(ctx, tx, ws, sessionID); err != nil {
			return err
		}

		var token sessionEventsPageToken
		var hasPage bool
		if options.Page != "" {
			decoded, err := decodeSessionEventsPageToken(r.pageTokenSecret, options.Page, ws, sessionID, "", options)
			if err != nil {
				return err
			}
			token = decoded
			hasPage = true
		}

		orderSQL := "ASC"
		pagePredicate := ""
		args := []any{string(ws), sessionID}
		if options.Order == "desc" {
			orderSQL = "DESC"
			if hasPage {
				pagePredicate = "AND (e.insert_stream_position, e.event_id) < ($3, $4)"
				args = append(args, token.Position, token.EventID)
			}
		} else if hasPage {
			pagePredicate = "AND (e.insert_stream_position, e.event_id) > ($3, $4)"
			args = append(args, token.Position, token.EventID)
		}
		filterPredicate, filterArgs := sessionListFilterPredicate(options, len(args)+1)
		args = append(args, filterArgs...)
		limitArg := len(args) + 1
		args = append(args, options.Limit+1)

		query := fmt.Sprintf(
			`SELECT e.event_id, COALESCE(e.session_thread_id, ''), e.sequence, e.insert_stream_position, e.type, e.payload_json, e.processed_at
			   FROM session_events e
			   LEFT JOIN session_threads t
			     ON t.workspace_id = e.workspace_id
			    AND t.session_id = e.session_id
			    AND t.id = e.session_thread_id
			  WHERE e.workspace_id = $1
			    AND e.session_id = $2
			    AND e.visibility = 'public'
			    AND e.session_visible = TRUE
			    AND (e.session_thread_id IS NULL OR (t.visibility = 'public' AND t.role <> 'approval_reviewer'))
			    %s
			    %s
			  ORDER BY e.insert_stream_position %s, e.event_id %s
			  LIMIT $%d`,
			pagePredicate,
			filterPredicate,
			orderSQL,
			orderSQL,
			limitArg,
		)
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		events, cursors, err := scanEventRows(rows, sessionID)
		if err != nil {
			return err
		}
		if len(events) > options.Limit {
			events = events[:options.Limit]
			cursors = cursors[:options.Limit]
			last := cursors[len(cursors)-1]
			encoded, err := encodeSessionEventsPageToken(r.pageTokenSecret, sessionEventsPageToken{
				Version:      sessionEventsPageTokenVersion,
				Resource:     sessionEventsPageTokenResource,
				WorkspaceID:  string(ws),
				SessionID:    sessionID,
				ThreadID:     "",
				Order:        options.Order,
				Position:     last.position,
				EventID:      last.eventID,
				Types:        options.Types,
				CreatedAtGT:  options.CreatedAtGT,
				CreatedAtGTE: options.CreatedAtGTE,
				CreatedAtLT:  options.CreatedAtLT,
				CreatedAtLTE: options.CreatedAtLTE,
			})
			if err != nil {
				return err
			}
			result.NextPage = &encoded
		}
		result.Data = events
		return nil
	})
	if err != nil {
		return ListResult{}, err
	}
	return result, nil
}

func (r *PostgreSQLReader) ListThreadEvents(ctx context.Context, ws workspace.ID, sessionID string, threadID string, options ListOptions) (ListResult, error) {
	if err := validateThreadReaderScope(ws, sessionID, threadID); err != nil {
		return ListResult{}, err
	}
	if r == nil || r.client == nil {
		return ListResult{}, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	options, err := normalizeReaderListOptions(options)
	if err != nil {
		return ListResult{}, err
	}
	var result ListResult
	err = r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.list_thread_events", func(tx *dbconnect.Tx) error {
		if err := ensureReadableThreadTx(ctx, tx, ws, sessionID, threadID); err != nil {
			return err
		}

		var token sessionEventsPageToken
		var hasPage bool
		if options.Page != "" {
			decoded, err := decodeSessionEventsPageToken(r.pageTokenSecret, options.Page, ws, sessionID, threadID, options)
			if err != nil {
				return err
			}
			token = decoded
			hasPage = true
		}

		orderSQL := "ASC"
		pagePredicate := ""
		args := []any{string(ws), sessionID, threadID}
		if options.Order == "desc" {
			orderSQL = "DESC"
			if hasPage {
				pagePredicate = "AND e.sequence < $4"
				args = append(args, token.Sequence)
			}
		} else if hasPage {
			pagePredicate = "AND e.sequence > $4"
			args = append(args, token.Sequence)
		}
		filterPredicate, filterArgs := sessionListFilterPredicate(options, len(args)+1)
		args = append(args, filterArgs...)
		limitArg := len(args) + 1
		args = append(args, options.Limit+1)

		query := fmt.Sprintf(
			`SELECT e.event_id, COALESCE(e.session_thread_id, ''), e.sequence, e.sequence, e.type, e.payload_json, e.processed_at
			   FROM session_events e
			  WHERE e.workspace_id = $1
			    AND e.session_id = $2
			    AND e.session_thread_id = $3
			    AND e.visibility = 'public'
			    %s
			    %s
			  ORDER BY e.sequence %s
			  LIMIT $%d`,
			pagePredicate,
			filterPredicate,
			orderSQL,
			limitArg,
		)
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		events, cursors, err := scanEventRows(rows, sessionID)
		if err != nil {
			return err
		}
		if len(events) > options.Limit {
			events = events[:options.Limit]
			cursors = cursors[:options.Limit]
			encoded, err := encodeSessionEventsPageToken(r.pageTokenSecret, sessionEventsPageToken{
				Version:      sessionEventsPageTokenVersion,
				Resource:     sessionEventsPageTokenResource,
				WorkspaceID:  string(ws),
				SessionID:    sessionID,
				ThreadID:     threadID,
				Order:        options.Order,
				Sequence:     cursors[len(cursors)-1].sequence,
				Types:        options.Types,
				CreatedAtGT:  options.CreatedAtGT,
				CreatedAtGTE: options.CreatedAtGTE,
				CreatedAtLT:  options.CreatedAtLT,
				CreatedAtLTE: options.CreatedAtLTE,
			})
			if err != nil {
				return err
			}
			result.NextPage = &encoded
		}
		result.Data = events
		return nil
	})
	if err != nil {
		return ListResult{}, err
	}
	return result, nil
}

func (r *PostgreSQLReader) CurrentStreamPosition(ctx context.Context, ws workspace.ID, sessionID string) (int64, error) {
	if err := validateReaderScope(ws, sessionID); err != nil {
		return 0, err
	}
	if r == nil || r.client == nil {
		return 0, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	var position int64
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.current_stream_position", func(tx *dbconnect.Tx) error {
		if err := ensureReadableSessionTx(ctx, tx, ws, sessionID); err != nil {
			return err
		}
		return tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(c.stream_position), 0)
			   FROM session_event_stream_changes c
			   JOIN session_events e
			     ON e.workspace_id = c.workspace_id
			    AND e.session_id = c.session_id
			    AND e.session_thread_id IS NOT DISTINCT FROM c.session_thread_id
			    AND e.event_id = c.event_id
			    AND e.visibility = 'public'
			   LEFT JOIN session_threads t
			     ON t.workspace_id = c.workspace_id
			    AND t.session_id = c.session_id
			    AND t.id = c.session_thread_id
			  WHERE c.workspace_id = $1
			    AND c.session_id = $2
			    AND c.visibility = 'public'
			    AND c.session_visible = TRUE
			    AND (c.session_thread_id IS NULL OR (t.visibility = 'public' AND t.role <> 'approval_reviewer'))`,
			string(ws),
			sessionID,
		).Scan(&position)
	})
	if err != nil {
		return 0, err
	}
	return position, nil
}

func (r *PostgreSQLReader) CurrentThreadStreamPosition(ctx context.Context, ws workspace.ID, sessionID string, threadID string) (int64, error) {
	if err := validateThreadReaderScope(ws, sessionID, threadID); err != nil {
		return 0, err
	}
	if r == nil || r.client == nil {
		return 0, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	var position int64
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.current_thread_stream_position", func(tx *dbconnect.Tx) error {
		if err := ensureReadableThreadTx(ctx, tx, ws, sessionID, threadID); err != nil {
			return err
		}
		return tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(c.stream_position), 0)
			   FROM session_event_stream_changes c
			   JOIN session_events e
			     ON e.workspace_id = c.workspace_id
			    AND e.session_id = c.session_id
			    AND e.session_thread_id = c.session_thread_id
			    AND e.event_id = c.event_id
			    AND e.visibility = 'public'
			  WHERE c.workspace_id = $1
			    AND c.session_id = $2
			    AND c.session_thread_id = $3
			    AND c.visibility = 'public'`,
			string(ws),
			sessionID,
			threadID,
		).Scan(&position)
	})
	if err != nil {
		return 0, err
	}
	return position, nil
}

func (r *PostgreSQLReader) ListSessionEventChanges(ctx context.Context, ws workspace.ID, sessionID string, after int64, limit int) ([]StreamChange, error) {
	if err := validateReaderScope(ws, sessionID); err != nil {
		return nil, err
	}
	if r == nil || r.client == nil {
		return nil, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	if limit <= 0 || limit > defaultStreamBatchSize {
		limit = defaultStreamBatchSize
	}
	var changes []StreamChange
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.list_session_event_changes", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT c.stream_position,
			        e.event_id,
			        COALESCE(e.session_thread_id, ''),
			        e.sequence,
			        e.type,
			        e.payload_json,
			        e.processed_at
			   FROM session_event_stream_changes c
			   JOIN session_events e
			     ON e.workspace_id = c.workspace_id
			    AND e.session_id = c.session_id
			    AND e.session_thread_id IS NOT DISTINCT FROM c.session_thread_id
			    AND e.event_id = c.event_id
			    AND e.visibility = 'public'
			   LEFT JOIN session_threads t
			     ON t.workspace_id = c.workspace_id
			    AND t.session_id = c.session_id
			    AND t.id = c.session_thread_id
			  WHERE c.workspace_id = $1
			    AND c.session_id = $2
			    AND c.visibility = 'public'
			    AND c.session_visible = TRUE
			    AND (c.session_thread_id IS NULL OR (t.visibility = 'public' AND t.role <> 'approval_reviewer'))
			    AND c.stream_position > $3
			  ORDER BY c.stream_position ASC
			  LIMIT $4`,
			string(ws),
			sessionID,
			after,
			limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		changes = []StreamChange{}
		for rows.Next() {
			var change StreamChange
			var sequence int64
			var payload string
			var processedAt sql.NullTime
			if err := rows.Scan(
				&change.StreamPosition,
				&change.Event.ID,
				&change.Event.ThreadID,
				&sequence,
				&change.Event.Type,
				&payload,
				&processedAt,
			); err != nil {
				return err
			}
			change.Event.SessionID = sessionID
			change.Event.Payload = []byte(payload)
			if processedAt.Valid {
				formatted := processedAt.Time.UTC().Format(time.RFC3339Nano)
				change.Event.ProcessedAt = &formatted
			}
			changes = append(changes, change)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

func (r *PostgreSQLReader) ListThreadEventChanges(ctx context.Context, ws workspace.ID, sessionID string, threadID string, after int64, limit int) ([]StreamChange, error) {
	if err := validateThreadReaderScope(ws, sessionID, threadID); err != nil {
		return nil, err
	}
	if r == nil || r.client == nil {
		return nil, &httpapi.ValidationError{Message: "event stream reader is required"}
	}
	if limit <= 0 || limit > defaultStreamBatchSize {
		limit = defaultStreamBatchSize
	}
	var changes []StreamChange
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "eventstream.list_thread_event_changes", func(tx *dbconnect.Tx) error {
		if err := ensureReadableThreadTx(ctx, tx, ws, sessionID, threadID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT c.stream_position,
			        e.event_id,
			        COALESCE(e.session_thread_id, ''),
			        e.sequence,
			        e.type,
			        e.payload_json,
			        e.processed_at
			   FROM session_event_stream_changes c
			   JOIN session_events e
			     ON e.workspace_id = c.workspace_id
			    AND e.session_id = c.session_id
			    AND e.session_thread_id = c.session_thread_id
			    AND e.event_id = c.event_id
			    AND e.visibility = 'public'
			  WHERE c.workspace_id = $1
			    AND c.session_id = $2
			    AND c.session_thread_id = $3
			    AND c.visibility = 'public'
			    AND c.stream_position > $4
			  ORDER BY c.stream_position ASC
			  LIMIT $5`,
			string(ws),
			sessionID,
			threadID,
			after,
			limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		changes = []StreamChange{}
		for rows.Next() {
			var change StreamChange
			var sequence int64
			var payload string
			var processedAt sql.NullTime
			if err := rows.Scan(
				&change.StreamPosition,
				&change.Event.ID,
				&change.Event.ThreadID,
				&sequence,
				&change.Event.Type,
				&payload,
				&processedAt,
			); err != nil {
				return err
			}
			change.Event.SessionID = sessionID
			change.Event.Payload = []byte(payload)
			if processedAt.Valid {
				formatted := processedAt.Time.UTC().Format(time.RFC3339Nano)
				change.Event.ProcessedAt = &formatted
			}
			changes = append(changes, change)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

func ensureReadableSessionTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) error {
	var lifecycleState string
	err := tx.QueryRow(ctx,
		`SELECT lifecycle_state
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2`,
		string(ws),
		sessionID,
	).Scan(&lifecycleState)
	if dbconnect.IsNoRows(err) {
		return &httpapi.NotFoundError{Message: "session not found"}
	}
	if err != nil {
		return err
	}
	if lifecycleState == "deleted" {
		return &httpapi.NotFoundError{Message: "session not found"}
	}
	return nil
}

func ensureReadableThreadTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, threadID string) error {
	var lifecycleState string
	err := tx.QueryRow(ctx,
		`SELECT s.lifecycle_state
		   FROM sessions s
		   JOIN session_threads t
		     ON t.workspace_id = s.workspace_id
		    AND t.session_id = s.id
		  WHERE s.workspace_id = $1
		    AND s.id = $2
		    AND t.id = $3
		    AND t.visibility = 'public'
		    AND t.role <> 'approval_reviewer'`,
		string(ws),
		sessionID,
		threadID,
	).Scan(&lifecycleState)
	if dbconnect.IsNoRows(err) {
		return &httpapi.NotFoundError{Message: "session thread not found"}
	}
	if err != nil {
		return err
	}
	if lifecycleState == "deleted" {
		return &httpapi.NotFoundError{Message: "session thread not found"}
	}
	return nil
}

type eventRowCursor struct {
	sequence int64
	position int64
	threadID string
	eventID  string
}

func scanEventRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}, sessionID string) ([]Event, []eventRowCursor, error) {
	events := []Event{}
	cursors := []eventRowCursor{}
	for rows.Next() {
		var event Event
		var sequence int64
		var position int64
		var payload string
		var processedAt sql.NullTime
		if err := rows.Scan(
			&event.ID,
			&event.ThreadID,
			&sequence,
			&position,
			&event.Type,
			&payload,
			&processedAt,
		); err != nil {
			return nil, nil, err
		}
		event.SessionID = sessionID
		event.Payload = []byte(payload)
		if processedAt.Valid {
			formatted := processedAt.Time.UTC().Format(time.RFC3339Nano)
			event.ProcessedAt = &formatted
		}
		events = append(events, event)
		cursors = append(cursors, eventRowCursor{sequence: sequence, position: position, threadID: event.ThreadID, eventID: event.ID})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return events, cursors, nil
}

func sessionListFilterPredicate(options ListOptions, firstPlaceholder int) (string, []any) {
	predicates := make([]string, 0)
	args := make([]any, 0)
	next := firstPlaceholder
	if len(options.Types) > 0 {
		placeholders := make([]string, 0, len(options.Types))
		for _, eventType := range options.Types {
			placeholders = append(placeholders, fmt.Sprintf("$%d", next))
			args = append(args, eventType)
			next++
		}
		predicates = append(predicates, fmt.Sprintf("AND e.type IN (%s)", joinSQLPlaceholders(placeholders)))
	}
	for _, filter := range []struct {
		value    string
		operator string
	}{
		{value: options.CreatedAtGT, operator: ">"},
		{value: options.CreatedAtGTE, operator: ">="},
		{value: options.CreatedAtLT, operator: "<"},
		{value: options.CreatedAtLTE, operator: "<="},
	} {
		if filter.value == "" {
			continue
		}
		predicates = append(predicates, fmt.Sprintf("AND e.created_at %s $%d", filter.operator, next))
		args = append(args, filter.value)
		next++
	}
	return joinSQLPredicates(predicates), args
}

func joinSQLPlaceholders(values []string) string {
	if len(values) == 0 {
		return ""
	}
	joined := values[0]
	for _, value := range values[1:] {
		joined += ", " + value
	}
	return joined
}

func joinSQLPredicates(values []string) string {
	if len(values) == 0 {
		return ""
	}
	joined := values[0]
	for _, value := range values[1:] {
		joined += "\n\t\t\t    " + value
	}
	return joined
}

func normalizeReaderListOptions(options ListOptions) (ListOptions, error) {
	if options.Limit <= 0 || options.Limit > maxListLimit {
		options.Limit = defaultListLimit
	}
	if options.Order == "" {
		options.Order = "asc"
	}
	if options.Order != "asc" && options.Order != "desc" {
		return ListOptions{}, &httpapi.ValidationError{Message: "order must be asc or desc"}
	}
	for _, eventType := range options.Types {
		if eventType == "" {
			return ListOptions{}, &httpapi.ValidationError{Message: "invalid query parameter"}
		}
	}
	for _, filter := range []struct {
		input  string
		output *string
	}{
		{input: options.CreatedAtGT, output: &options.CreatedAtGT},
		{input: options.CreatedAtGTE, output: &options.CreatedAtGTE},
		{input: options.CreatedAtLT, output: &options.CreatedAtLT},
		{input: options.CreatedAtLTE, output: &options.CreatedAtLTE},
	} {
		if filter.input == "" {
			continue
		}
		normalized, err := normalizeCreatedAtFilter(filter.input)
		if err != nil {
			return ListOptions{}, err
		}
		*filter.output = normalized
	}
	return options, nil
}

func validateReaderScope(ws workspace.ID, sessionID string) error {
	if ws == "" {
		return &httpapi.ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return &httpapi.ValidationError{Message: "session_id is required"}
	}
	return nil
}

func validateThreadReaderScope(ws workspace.ID, sessionID string, threadID string) error {
	if err := validateReaderScope(ws, sessionID); err != nil {
		return err
	}
	if threadID == "" {
		return &httpapi.ValidationError{Message: "thread_id is required"}
	}
	return nil
}
