package sessioneventwrite_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

type eventStatementTracer struct {
	mu         sync.Mutex
	statements []string
}

func (tracer *eventStatementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	tracer.statements = append(tracer.statements, strings.TrimSpace(data.SQL))
	tracer.mu.Unlock()
	return ctx
}

func (*eventStatementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *eventStatementTracer) take() []string {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	statements := tracer.statements
	tracer.statements = nil
	return statements
}

type feedState struct {
	revision       int64
	insertPosition int64
	latestPosition int64
	processed      bool
	changes        []feedChange
}

type feedChange struct {
	position   int64
	revision   int64
	visibility string
	visible    bool
	threadID   sql.NullString
}

func readFeedState(t *testing.T, admin *sql.DB, eventID string) feedState {
	t.Helper()
	var state feedState
	var processedAt sql.NullTime
	if err := admin.QueryRowContext(context.Background(), `SELECT revision, insert_stream_position, latest_stream_position, processed_at
		FROM session_events WHERE event_id=$1`, eventID).Scan(&state.revision, &state.insertPosition, &state.latestPosition, &processedAt); err != nil {
		t.Fatalf("read event %s: %v", eventID, err)
	}
	state.processed = processedAt.Valid
	rows, err := admin.QueryContext(context.Background(), `SELECT stream_position, revision, visibility, session_visible, session_thread_id
		FROM session_event_stream_changes WHERE event_id=$1 ORDER BY stream_position`, eventID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var change feedChange
		if err := rows.Scan(&change.position, &change.revision, &change.visibility, &change.visible, &change.threadID); err != nil {
			t.Fatal(err)
		}
		state.changes = append(state.changes, change)
	}
	return state
}

func requireSQLState(t *testing.T, name string, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("%s = %v; want SQLSTATE %s", name, err, code)
	}
}

// Each serving role that writes the feed inserts an event's first revision
// with one complete event INSERT, both stream positions set to one reserved
// identity value, and exactly one matching revision-1 change at that value;
// no follow-up UPDATE of the event is issued.
func TestPostgreSQLInitialEventInsertsOneEventAndChangeUnderServingRoles(t *testing.T) {
	for _, workload := range []string{"api", "bridge", "job_runner"} {
		t.Run(workload, func(t *testing.T) {
			ctx := context.Background()
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			tracer := &eventStatementTracer{}
			client := dbconnect.NewClientForTesting(storagetest.OpenWorkloadDB(t, admin, workload).OpenWorkload(t, workload, tracer))
			const sessionID, threadID = "sesn_initial_event", "thr_initial_event"
			sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
			now := time.Now().UTC()
			var position int64
			tracer.take()
			if err := client.WithWorkspaceTx(ctx, "default", "sessioneventwrite.test_initial", func(tx *dbconnect.Tx) error {
				var err error
				position, err = sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
					WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
					EventID: "evt_initial_" + workload, Sequence: 1, Type: "session.status_running",
					PayloadJSON: `{"type":"session.status_running"}`, ProjectionJSON: `{"type":"session.status_running"}`,
					Visibility: "internal", SessionVisible: false, RuntimeWriteID: "write_" + workload,
					CreatedAt: now, ProcessedAt: &now,
				})
				return err
			}); err != nil {
				t.Fatalf("%s initial event: %v", workload, err)
			}
			statements := tracer.take()
			inserts, updates := 0, 0
			for _, statement := range statements {
				if strings.HasPrefix(statement, "INSERT INTO session_events") {
					inserts++
				}
				if strings.HasPrefix(statement, "UPDATE session_events") {
					updates++
				}
			}
			if inserts != 1 || updates != 0 {
				t.Fatalf("event INSERT/UPDATE statements = %d/%d; want 1/0: %q", inserts, updates, statements)
			}
			state := readFeedState(t, admin, "evt_initial_"+workload)
			if state.revision != 1 || state.insertPosition != position || state.latestPosition != position || !state.processed ||
				len(state.changes) != 1 || state.changes[0].position != position || state.changes[0].revision != 1 ||
				state.changes[0].visibility != "internal" || state.changes[0].visible || state.changes[0].threadID.String != threadID {
				t.Fatalf("initial feed state = %+v at reserved %d; want revision 1 and one matching change at both positions", state, position)
			}
		})
	}
}

// The processed revision keeps the event's insert position, moves its latest
// position to a freshly reserved value and inserts that revision's change; a
// rolled-back revision leaves the first revision intact, and the change's
// event foreign key stays immediate.
func TestPostgreSQLProcessedRevisionKeepsInsertPosition(t *testing.T) {
	for _, workload := range []string{"bridge", "job_runner"} {
		t.Run(workload, func(t *testing.T) {
			ctx := context.Background()
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			client := dbconnect.NewClientForTesting(storagetest.OpenWorkloadDB(t, admin, workload).DB)
			const sessionID, threadID, eventID = "sesn_processed_revision", "thr_processed_revision", "evt_processed_revision"
			sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
			now := time.Now().UTC()
			var first int64
			if err := client.WithWorkspaceTx(ctx, "default", "sessioneventwrite.test_admitted", func(tx *dbconnect.Tx) error {
				var err error
				first, err = sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
					WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, EventID: eventID, Sequence: 1,
					Type: "user.message", PayloadJSON: `{"type":"user.message","content":[{"type":"text","text":"hi"}]}`,
					Visibility: "public", SessionVisible: true, CreatedAt: now,
				})
				return err
			}); err != nil {
				t.Fatalf("admit input: %v", err)
			}
			request := sessioneventwrite.ProcessedRevision{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, EventID: eventID, ProcessedAt: now}
			rollback := errors.New("roll back the revision")
			if err := client.WithWorkspaceTx(ctx, "default", "sessioneventwrite.test_rollback", func(tx *dbconnect.Tx) error {
				if _, revised, err := sessioneventwrite.RecordProcessedRevisionTx(ctx, tx, request); err != nil || !revised {
					t.Fatalf("revision before rollback = %v/%v", revised, err)
				}
				return rollback
			}); !errors.Is(err, rollback) {
				t.Fatalf("rolled-back revision = %v", err)
			}
			if state := readFeedState(t, admin, eventID); state.revision != 1 || state.processed || state.insertPosition != first ||
				state.latestPosition != first || len(state.changes) != 1 {
				t.Fatalf("rolled-back revision left %+v; want the first revision at %d", state, first)
			}
			var revision sessioneventwrite.Revision
			if err := client.WithWorkspaceTx(ctx, "default", "sessioneventwrite.test_revision", func(tx *dbconnect.Tx) error {
				var revised bool
				var err error
				revision, revised, err = sessioneventwrite.RecordProcessedRevisionTx(ctx, tx, request)
				if err == nil && !revised {
					t.Fatal("unprocessed input was not revised")
				}
				return err
			}); err != nil {
				t.Fatalf("processed revision: %v", err)
			}
			state := readFeedState(t, admin, eventID)
			if state.revision != 2 || !state.processed || state.insertPosition != first || state.latestPosition != revision.StreamPosition ||
				revision.StreamPosition <= first || len(state.changes) != 2 || state.changes[1].position != revision.StreamPosition ||
				state.changes[1].revision != 2 || state.changes[1].threadID.String != threadID {
				t.Fatalf("processed revision state = %+v revision %+v; want insert %d kept and one revision-2 change at a later position", state, revision, first)
			}
			var feed []int64
			rows, err := admin.QueryContext(ctx, `SELECT stream_position FROM session_event_stream_changes
				WHERE workspace_id='default' AND session_id=$1 AND stream_position > $2 ORDER BY stream_position`, sessionID, first)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var position int64
				if err := rows.Scan(&position); err != nil {
					t.Fatal(err)
				}
				feed = append(feed, position)
			}
			_ = rows.Close()
			if len(feed) != 1 || feed[0] != revision.StreamPosition {
				t.Fatalf("reader after %d sees %v; want the revision at %d", first, feed, revision.StreamPosition)
			}
			if err := client.WithWorkspaceTx(ctx, "default", "sessioneventwrite.test_replay", func(tx *dbconnect.Tx) error {
				if _, revised, err := sessioneventwrite.RecordProcessedRevisionTx(ctx, tx, request); err != nil || revised {
					t.Fatalf("second revision of a processed input = %v/%v; want not revised", revised, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			err = client.WithWorkspaceTx(ctx, "default", "sessioneventwrite.test_missing_event", func(tx *dbconnect.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO session_event_stream_changes (
					workspace_id, session_id, stream_position, event_id, session_thread_id, revision, visibility, session_visible, changed_at
				) OVERRIDING SYSTEM VALUE VALUES ('default', $1, nextval(pg_get_serial_sequence('public.session_event_stream_changes', 'stream_position')),
					'evt_missing_revision_event', $2, 1, 'public', true, now())`, sessionID, threadID)
				return err
			})
			requireSQLState(t, "change without its event", err, "23503")
		})
	}
}
