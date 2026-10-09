package eventstream_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Pruning every change of a feed keeps the feed's head: the head is the
// greater of the newest retained eligible change and the feed's watermark,
// for the Session feed and for each Thread feed. A viewer opening at that head
// sees no gap and receives later changes.
func TestFeedHeadKeepsThePrunedThroughWatermark(t *testing.T) {
	f := newRetentionReaderFixture(t)
	seedEventStreamSession(t, f.admin, "default", "sesn_head", "thr_head")
	mainPosition := f.seedChange(t, "sesn_head", "thr_head", "evt_head_main", true, "public")
	childPosition := f.seedChange(t, "sesn_head", "thr_child", "evt_head_child", false, "public")
	f.seedChange(t, "sesn_head", "thr_head", "evt_head_internal", true, "internal")
	f.pruneAll(t)
	if count := f.changeCount(t, "sesn_head"); count != 0 {
		t.Fatalf("%d changes survived pruning", count)
	}
	for _, probe := range []struct {
		thread string
		want   int64
	}{{"", mainPosition}, {"thr_head", mainPosition}, {"thr_child", childPosition}} {
		if got := f.head(t, "sesn_head", probe.thread); got != probe.want {
			t.Fatalf("head of %q after pruning = %d; want %d", probe.thread, got, probe.want)
		}
	}
	if changes, err := f.reader.ListSessionEventChanges(context.Background(), workspace.DefaultID, "sesn_head", mainPosition, 100); err != nil || len(changes) != 0 {
		t.Fatalf("read at the head = %d/%v; want no rows and no gap", len(changes), err)
	}
	later := f.seedFreshChange(t, "sesn_head", "thr_head", "evt_head_later", 1)
	changes, err := f.reader.ListSessionEventChanges(context.Background(), workspace.DefaultID, "sesn_head", mainPosition, 100)
	if err != nil || len(changes) != 1 || changes[0].StreamPosition != later {
		t.Fatalf("read after a new change = %+v/%v", changes, err)
	}
}

// The gap rule compares the watermark with the viewer's actual cursor only.
// A connection that keeps up survives several retention windows, sparse
// global positions shared with another Session are not gaps, equality is not a
// gap, and only a viewer behind an eligible pruned position fails. A pruned
// revision-1 change leaves its retained revision 2 readable from the
// watermark.
func TestChangeFeedGapRuleUsesTheActualCursor(t *testing.T) {
	f := newRetentionReaderFixture(t)
	ctx := context.Background()
	seedEventStreamSession(t, f.admin, "default", "sesn_gap", "thr_gap")
	seedEventStreamSession(t, f.admin, "ws_gap_other", "sesn_gap_other", "thr_gap_other")
	cursor := int64(0)
	read := func(after int64) ([]eventstream.StreamChange, error) {
		return f.reader.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_gap", after, 100)
	}
	var positions []int64
	for window := range 3 {
		positions = append(positions, f.seedChange(t, "sesn_gap", "thr_gap", "evt_gap_"+string(rune('a'+window)), true, "public"))
		f.seedChangeIn(t, "ws_gap_other", "sesn_gap_other", "thr_gap_other", "evt_other_"+string(rune('a'+window)), true, "public")
		positions = append(positions, f.seedChange(t, "sesn_gap", "thr_gap", "evt_gap_"+string(rune('k'+window)), true, "public"))
		changes, err := read(cursor)
		if err != nil || len(changes) != 2 {
			t.Fatalf("window %d read = %d/%v", window, len(changes), err)
		}
		cursor = changes[len(changes)-1].StreamPosition
		f.pruneAll(t)
		if got := f.watermark(t, "sesn_gap", "session"); got != cursor {
			t.Fatalf("window %d watermark = %d; want the consumed cursor %d", window, got, cursor)
		}
		if changes, err := read(cursor); err != nil || len(changes) != 0 {
			t.Fatalf("window %d read at the watermark = %d/%v; want no gap", window, len(changes), err)
		}
	}
	if positions[len(positions)-1]-positions[0] < int64(len(positions)) {
		t.Fatalf("fixture positions %v are not sparse", positions)
	}
	if _, err := read(positions[len(positions)-2]); !errors.Is(err, eventstream.ErrRetainedHistoryGap) {
		t.Fatalf("lagging cursor = %v; want a retained-history gap", err)
	}
	if _, err := f.reader.ListThreadEventChanges(ctx, workspace.DefaultID, "sesn_gap", "thr_gap", positions[0], 100); !errors.Is(err, eventstream.ErrRetainedHistoryGap) {
		t.Fatalf("lagging Thread cursor = %v; want a retained-history gap", err)
	}

	first := f.seedChange(t, "sesn_gap", "thr_gap", "evt_gap_revised", true, "public")
	second := f.seedFreshChange(t, "sesn_gap", "thr_gap", "evt_gap_revised", 2)
	f.pruneAll(t)
	if _, err := read(cursor); !errors.Is(err, eventstream.ErrRetainedHistoryGap) {
		t.Fatalf("cursor before the pruned revision 1 = %v; want a gap", err)
	}
	changes, err := read(first)
	if err != nil || len(changes) != 1 || changes[0].StreamPosition != second || changes[0].Event.ID != "evt_gap_revised" {
		t.Fatalf("read at the pruned revision 1 = %+v/%v; want revision 2", changes, err)
	}
}

// A change batch shares one repeatable-read snapshot with its watermark read:
// a prune committing after that snapshot hides no row of the batch and fakes
// no gap, and the next batch compares its consumed cursor with the new
// watermark. A prune committed before the snapshot is a real gap, and a
// rolled-back prune changes nothing.
func TestChangeFeedBatchSnapshotOrdersWithConcurrentPruning(t *testing.T) {
	f := newRetentionReaderFixture(t)
	ctx := context.Background()
	seedEventStreamSession(t, f.admin, "default", "sesn_snapshot", "thr_snapshot")
	var last int64
	for _, id := range []string{"evt_snapshot_a", "evt_snapshot_b", "evt_snapshot_c"} {
		last = f.seedChange(t, "sesn_snapshot", "thr_snapshot", id, true, "public")
	}

	pruner, err := f.cleanup.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pruner.Exec(`SELECT * FROM public.tetral_prune_event_changes(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`); err != nil {
		t.Fatal(err)
	}
	if err := pruner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if changes, err := f.reader.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_snapshot", 0, 100); err != nil || len(changes) != 3 {
		t.Fatalf("read after a rolled-back prune = %d/%v; want all rows", len(changes), err)
	}

	pause := &pauseAtStatement{fragment: "FROM session_event_feed_retention", paused: make(chan struct{}), resume: make(chan struct{})}
	paused := eventstream.NewPostgreSQLReader(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "event_stream", pause)))
	done := make(chan readOutcome, 1)
	go func() {
		changes, err := paused.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_snapshot", 0, 100)
		done <- readOutcome{changes: changes, err: err}
	}()
	<-pause.paused
	f.pruneAll(t)
	close(pause.resume)
	outcome := <-done
	if outcome.err != nil || len(outcome.changes) != 3 || outcome.changes[2].StreamPosition != last {
		t.Fatalf("batch whose snapshot preceded the prune = %d/%v; want all three rows", len(outcome.changes), outcome.err)
	}
	if changes, err := f.reader.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_snapshot", last, 100); err != nil || len(changes) != 0 {
		t.Fatalf("next batch from the consumed cursor = %d/%v; want no gap", len(changes), err)
	}
	if _, err := f.reader.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_snapshot", 0, 100); !errors.Is(err, eventstream.ErrRetainedHistoryGap) {
		t.Fatalf("snapshot after the prune from cursor 0 = %v; want a gap", err)
	}
}

// An open Session feed and a selected Session End group stay readable while
// the Session's permanent session.deleted event lies after their position,
// even after its change row was pruned: the End group still completes, and the
// feed then reports the real gap rather than a missing Session. Behind the
// deletion, or without a permanent deletion event, the Session is unreadable.
func TestDeletedSessionReadsUseThePermanentDeletionEvent(t *testing.T) {
	f := newRetentionReaderFixture(t)
	ctx := context.Background()
	seedEventStreamSession(t, f.admin, "default", "sesn_deleted_feed", "thr_deleted_feed")
	seedRequestEvent(t, f.admin, "sesn_deleted_feed", "thr_deleted_feed", "evt_del_start", 1, "span.model_request_start", "mreq_deleted", `{}`, "agent_provider_request", true)
	seedRequestEvent(t, f.admin, "sesn_deleted_feed", "thr_deleted_feed", "evt_del_message", 2, "agent.message", "mreq_deleted", `{"content":[{"type":"text","text":"kept body"}]}`, "", true)
	seedRequestEvent(t, f.admin, "sesn_deleted_feed", "thr_deleted_feed", "evt_del_end", 3, "span.model_request_end", "mreq_deleted", `{"model_request_start_id":"evt_del_start"}`, "", true)
	endPosition := f.position(t, "evt_del_end")
	seedEventStreamEvent(t, f.admin, "default", "sesn_deleted_feed", "", "evt_del_deleted", 1, "session.deleted", `{"id":"sesn_deleted_feed","type":"session.deleted"}`, "public", true, "2026-10-01T00:00:00Z")
	seedEventStreamChange(t, f.admin, "default", "sesn_deleted_feed", "", "evt_del_deleted", 1, "public", true)
	deletedPosition := f.position(t, "evt_del_deleted")
	if _, err := f.admin.Exec(`UPDATE sessions SET lifecycle_state = 'deleted' WHERE id = 'sesn_deleted_feed'`); err != nil {
		t.Fatal(err)
	}
	f.pruneAll(t)

	messages, err := f.reader.ListRequestFinalMessages(ctx, eventstream.ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_deleted_feed"}, "evt_del_end", 0, 1)
	if err != nil || len(messages) != 1 || messages[0].Event.ID != "evt_del_message" {
		t.Fatalf("End group before the pruned deletion = %+v/%v; want its permanent body", messages, err)
	}
	if _, err := f.reader.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_deleted_feed", endPosition, 100); !errors.Is(err, eventstream.ErrRetainedHistoryGap) {
		t.Fatalf("feed before the pruned deletion = %v; want a retained-history gap", err)
	}
	var notFound *httpapi.NotFoundError
	if _, err := f.reader.ListSessionEventChanges(ctx, workspace.DefaultID, "sesn_deleted_feed", deletedPosition, 100); !errors.As(err, &notFound) {
		t.Fatalf("feed behind the deletion = %v; want not found", err)
	}

	seedEventStreamSession(t, f.admin, "ws_deleted_bare", "sesn_deleted_bare", "thr_deleted_bare")
	f.seedChangeIn(t, "ws_deleted_bare", "sesn_deleted_bare", "thr_deleted_bare", "evt_bare", true, "public")
	if _, err := f.admin.Exec(`UPDATE sessions SET lifecycle_state = 'deleted' WHERE id = 'sesn_deleted_bare'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reader.ListSessionEventChanges(ctx, workspace.ID("ws_deleted_bare"), "sesn_deleted_bare", 0, 100); !errors.As(err, &notFound) {
		t.Fatalf("deleted Session without a deletion event = %v; want not found", err)
	}
}

// Under the real Event Stream role, with sequential scans and sorts disabled,
// each feed head reads its partial head index newest first under its Limit.
func TestFeedHeadsReadTheHeadIndexes(t *testing.T) {
	f := newRetentionReaderFixture(t)
	seedEventStreamSession(t, f.admin, "default", "sesn_head_plan", "thr_head_plan")
	for _, id := range []string{"evt_plan_a", "evt_plan_b", "evt_plan_c"} {
		f.seedChange(t, "sesn_head_plan", "thr_head_plan", id, true, "public")
	}
	if _, err := f.admin.Exec(`ANALYZE session_event_stream_changes; ANALYZE session_events; ANALYZE session_threads`); err != nil {
		t.Fatal(err)
	}
	var role string
	if err := f.workload.DB.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		name, index string
		args        []any
	}{
		{"session", "idx_session_event_stream_changes_session_head", []any{"default", "sesn_head_plan"}},
		{"thread", "idx_session_event_stream_changes_thread_head", []any{"default", "sesn_head_plan", "thr_head_plan"}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			query := eventstream.SessionFeedHeadQueryForTest
			if probe.name == "thread" {
				query = eventstream.ThreadFeedHeadQueryForTest
			}
			plan := explainAs(t, f.admin, role, query, probe.args...)
			found, sorts, seqScans := false, 0, 0
			walkHeadPlan(plan, false, probe.index, &found, &sorts, &seqScans)
			if !found || sorts != 0 || seqScans != 0 {
				t.Fatalf("%s head plan: index under Limit %t, sorts %d, change seq scans %d", probe.name, found, sorts, seqScans)
			}
		})
	}
}

type retentionReaderFixture struct {
	admin    *sql.DB
	workload *storagetest.WorkloadDB
	cleanup  *sql.DB
	reader   *eventstream.PostgreSQLReader
}

func newRetentionReaderFixture(t *testing.T) retentionReaderFixture {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "event_stream")
	return retentionReaderFixture{
		admin:    admin,
		workload: workload,
		cleanup:  workload.OpenWorkload(t, "cleanup", nil),
		reader:   eventstream.NewPostgreSQLReader(dbconnect.NewClientForTesting(workload.DB)),
	}
}

// seedChange inserts one event with an old revision-1 change, eligible for
// pruning, and returns its stream position.
func (f retentionReaderFixture) seedChange(t *testing.T, sessionID, threadID, eventID string, sessionVisible bool, visibility string) int64 {
	t.Helper()
	return f.seedChangeIn(t, "default", sessionID, threadID, eventID, sessionVisible, visibility)
}

func (f retentionReaderFixture) seedChangeIn(t *testing.T, workspaceID, sessionID, threadID, eventID string, sessionVisible bool, visibility string) int64 {
	t.Helper()
	var sequence int64
	if err := f.admin.QueryRow(`SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_events WHERE session_id = $1 AND session_thread_id = $2`, sessionID, threadID).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	seedEventStreamEvent(t, f.admin, workspaceID, sessionID, threadID, eventID, sequence, "agent.thinking", `{}`, visibility, sessionVisible, "2026-01-01T00:00:00Z")
	seedEventStreamChange(t, f.admin, workspaceID, sessionID, threadID, eventID, 1, visibility, sessionVisible)
	return f.position(t, eventID)
}

// seedFreshChange inserts a current change row, not yet eligible for pruning,
// for an existing or new event.
func (f retentionReaderFixture) seedFreshChange(t *testing.T, sessionID, threadID, eventID string, revision int64) int64 {
	t.Helper()
	if revision == 1 {
		var sequence int64
		if err := f.admin.QueryRow(`SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_events WHERE session_id = $1 AND session_thread_id = $2`, sessionID, threadID).Scan(&sequence); err != nil {
			t.Fatal(err)
		}
		seedEventStreamEvent(t, f.admin, "default", sessionID, threadID, eventID, sequence, "agent.thinking", `{}`, "public", true, "2026-01-01T00:00:00Z")
	}
	var position int64
	if err := f.admin.QueryRow(`INSERT INTO session_event_stream_changes (workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at)
		VALUES ('default', $1, $3, $2, $4, 'public', true, clock_timestamp()) RETURNING stream_position`, sessionID, threadID, eventID, revision).Scan(&position); err != nil {
		t.Fatal(err)
	}
	if _, err := f.admin.Exec(`UPDATE session_events SET latest_stream_position = $2, insert_stream_position = CASE WHEN insert_stream_position = 0 THEN $2 ELSE insert_stream_position END, revision = $3 WHERE event_id = $1`, eventID, position, revision); err != nil {
		t.Fatal(err)
	}
	return position
}

func (f retentionReaderFixture) position(t *testing.T, eventID string) int64 {
	t.Helper()
	var position int64
	if err := f.admin.QueryRow(`SELECT insert_stream_position FROM session_events WHERE event_id = $1`, eventID).Scan(&position); err != nil {
		t.Fatal(err)
	}
	return position
}

// pruneAll runs Cleanup's change retention as the real Cleanup role until no
// change older than 24 hours remains.
func (f retentionReaderFixture) pruneAll(t *testing.T) {
	t.Helper()
	for {
		var examined int
		if err := f.cleanup.QueryRow(`SELECT examined_count FROM public.tetral_prune_event_changes(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`).Scan(&examined); err != nil {
			t.Fatalf("prune changes: %v", err)
		}
		if examined < 256 {
			return
		}
	}
}

func (f retentionReaderFixture) head(t *testing.T, sessionID, threadID string) int64 {
	t.Helper()
	var head int64
	var err error
	if threadID == "" {
		head, err = f.reader.CurrentStreamPosition(context.Background(), workspace.DefaultID, sessionID)
	} else {
		head, err = f.reader.CurrentThreadStreamPosition(context.Background(), workspace.DefaultID, sessionID, threadID)
	}
	if err != nil {
		t.Fatalf("head of %s/%s: %v", sessionID, threadID, err)
	}
	return head
}

func (f retentionReaderFixture) watermark(t *testing.T, sessionID, feedKey string) int64 {
	t.Helper()
	var value int64
	if err := f.admin.QueryRow(`SELECT pruned_through FROM session_event_feed_retention WHERE session_id = $1 AND feed_key = $2`, sessionID, feedKey).Scan(&value); err != nil {
		t.Fatalf("watermark %s/%s: %v", sessionID, feedKey, err)
	}
	return value
}

func (f retentionReaderFixture) changeCount(t *testing.T, sessionID string) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(`SELECT count(*) FROM session_event_stream_changes WHERE session_id = $1`, sessionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type readOutcome struct {
	changes []eventstream.StreamChange
	err     error
}

// pauseAtStatement holds the first statement containing fragment until
// resume closes, after signalling paused.
type pauseAtStatement struct {
	fragment string
	paused   chan struct{}
	resume   chan struct{}
	done     bool
}

func (p *pauseAtStatement) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !p.done && strings.Contains(data.SQL, p.fragment) {
		p.done = true
		close(p.paused)
		<-p.resume
	}
	return ctx
}

func (*pauseAtStatement) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func explainAs(t *testing.T, admin *sql.DB, role, query string, args ...any) map[string]any {
	t.Helper()
	tx, err := admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, setting := range []string{
		`SET LOCAL enable_seqscan = off`,
		`SET LOCAL enable_sort = off`,
		`SELECT set_config('tetral.workspace_id', 'default', true)`,
		`SET LOCAL ROLE ` + pgx.Identifier{role}.Sanitize(),
	} {
		if _, err := tx.Exec(setting); err != nil {
			t.Fatalf("%s: %v", setting, err)
		}
	}
	var raw string
	if err := tx.QueryRow(`EXPLAIN (FORMAT JSON) `+query, args...).Scan(&raw); err != nil {
		t.Fatalf("explain: %v", err)
	}
	var documents []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &documents); err != nil || len(documents) != 1 {
		t.Fatalf("decode plan: %v", err)
	}
	return documents[0].Plan
}

func walkHeadPlan(node map[string]any, underLimit bool, index string, found *bool, sorts, seqScans *int) {
	nodeType, _ := node["Node Type"].(string)
	relation, _ := node["Relation Name"].(string)
	switch {
	case nodeType == "Limit":
		underLimit = true
	case nodeType == "Sort" || nodeType == "Incremental Sort":
		*sorts++
	case nodeType == "Seq Scan" && relation == "session_event_stream_changes":
		*seqScans++
	case (nodeType == "Index Scan" || nodeType == "Index Only Scan") && node["Index Name"] == index && underLimit:
		*found = true
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			walkHeadPlan(childNode, underLimit, index, found, sorts, seqScans)
		}
	}
}
