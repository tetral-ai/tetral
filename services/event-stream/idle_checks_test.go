package eventstream

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	readerpkg "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// unchangedSignals reports every Session as the same existing active Session.
type unchangedSignals struct{}

func (unchangedSignals) ReadSessionSignals(ctx context.Context, ws workspace.ID, sessionIDs []string) ([]SessionSignal, error) {
	return signalFunc(func(sessionID string) SessionSignal {
		return SessionSignal{SessionID: sessionID, Exists: true, LifecycleState: "active"}
	}).ReadSessionSignals(ctx, ws, sessionIDs)
}

// signalFunc reports each Session's signal from a function.
type signalFunc func(sessionID string) SessionSignal

func (f signalFunc) ReadSessionSignals(_ context.Context, _ workspace.ID, sessionIDs []string) ([]SessionSignal, error) {
	signals := make([]SessionSignal, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		signals = append(signals, f(sessionID))
	}
	return signals, nil
}

// idleChecksForTest returns shared idle checks over signals with a 1 ms poll
// interval, closed with the test. Unstarted checks never run, so only a forced
// read moves a stream.
func idleChecksForTest(t *testing.T, signals SessionSignalReader, start bool) *IdleCoalescer {
	t.Helper()
	idle := newIdleCoalescer(signals, time.Millisecond, nil)
	if start {
		idle.start()
	}
	t.Cleanup(idle.Close)
	return idle
}

const signalStatementMarker = "FROM unnest($2::text[]) AS ids(session_id)"

// signalTrace observes shared check statements on the real Event Stream role:
// started statements and Session batches per workspace, statements in flight,
// and an optional hold of a workspace's statements before they reach
// PostgreSQL.
type signalTrace struct {
	mu          sync.Mutex
	started     map[string]int
	batches     map[string][][]string
	inFlight    map[string]int
	maxInFlight map[string]int
	maxTotal    int
	total       int
	holds       map[string]chan struct{}
}

type signalTraceKey struct{}

func newSignalTrace() *signalTrace {
	return &signalTrace{started: map[string]int{}, batches: map[string][][]string{}, inFlight: map[string]int{}, maxInFlight: map[string]int{}, holds: map[string]chan struct{}{}}
}

func (s *signalTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !strings.Contains(data.SQL, signalStatementMarker) {
		return ctx
	}
	// database/sql arguments follow the driver's result-format argument.
	var ws string
	var ids []string
	for _, arg := range data.Args {
		switch value := arg.(type) {
		case string:
			if ws == "" {
				ws = value
			}
		case []string:
			ids = value
		}
	}
	s.mu.Lock()
	s.started[ws]++
	s.batches[ws] = append(s.batches[ws], append([]string(nil), ids...))
	s.inFlight[ws]++
	s.maxInFlight[ws] = max(s.maxInFlight[ws], s.inFlight[ws])
	s.total++
	s.maxTotal = max(s.maxTotal, s.total)
	hold := s.holds[ws]
	s.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
		}
	}
	return context.WithValue(ctx, signalTraceKey{}, ws)
}

func (s *signalTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	ws, ok := ctx.Value(signalTraceKey{}).(string)
	if !ok {
		return
	}
	s.mu.Lock()
	s.inFlight[ws]--
	s.total--
	s.mu.Unlock()
}

func (s *signalTrace) hold(ws workspace.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holds[string(ws)] = make(chan struct{})
}

func (s *signalTrace) release(ws workspace.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.holds[string(ws)])
	delete(s.holds, string(ws))
}

func (s *signalTrace) startedIn(ws workspace.ID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started[string(ws)]
}

func (s *signalTrace) batchesIn(ws workspace.ID) [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.batches[string(ws)]...)
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// idleFixture runs shared idle checks over the real Event Stream role with a
// manual scheduler clock: a workspace's first round starts at once, and every
// later round only when the test advances the clock by one poll interval.
type idleFixture struct {
	admin   *sql.DB
	role    *storagetest.WorkloadDB
	reader  *readerpkg.PostgreSQLReader
	trace   *signalTrace
	logs    *lockedBuffer
	idle    *IdleCoalescer
	handler *handler

	mu        sync.Mutex
	clock     time.Time
	published map[workspace.ID][]error
	chunks    map[workspace.ID][]int
	// quiet counts scheduler passes that dispatched nothing since the last
	// published chunk.
	quiet  int
	passes int
}

// newIdleFixture builds the fixture; wrap, when set, sits between the shared
// checks and the real reader. The checks start only on start().
func newIdleFixture(t *testing.T, wrap func(SessionSignalReader) SessionSignalReader) *idleFixture {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	role := storagetest.OpenWorkloadDB(t, admin, "event_stream")
	f := &idleFixture{admin: admin, role: role, trace: newSignalTrace(), logs: &lockedBuffer{}, clock: time.Unix(1_800_000_000, 0), published: map[workspace.ID][]error{}, chunks: map[workspace.ID][]int{}}
	f.reader = readerpkg.NewPostgreSQLReader(dbconnect.NewClientForTesting(role.OpenWorkload(t, "event_stream", f.trace)))
	var signals SessionSignalReader = f.reader
	if wrap != nil {
		signals = wrap(signals)
	}
	logger := workload.NewLogger(f.logs, "event-stream", "test", "unit")
	f.idle = newIdleCoalescer(signals, time.Second, logger)
	f.idle.now = f.now
	f.idle.published = func(chunk idleChunk, err error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.published[chunk.workspace.id] = append(f.published[chunk.workspace.id], err)
		f.chunks[chunk.workspace.id] = append(f.chunks[chunk.workspace.id], len(chunk.registrations))
		f.quiet = 0
	}
	f.idle.scheduled = func(dispatched int) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.passes++
		if dispatched == 0 {
			f.quiet++
		} else {
			f.quiet = 0
		}
	}
	t.Cleanup(f.idle.Close)
	// A one-minute heartbeat leaves the shared check as the only wake of an
	// idle viewer.
	config := DefaultStreamConfig()
	config.HeartbeatInterval = time.Minute
	f.handler = &handler{reader: f.reader, options: newOptions(WithStreamConfig(config), WithIdleCoalescer(f.idle), WithLogger(logger))}
	return f
}

func (f *idleFixture) start() { f.idle.start() }

func (f *idleFixture) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clock
}

// advance makes every completed round due again and wakes the scheduler.
func (f *idleFixture) advance() {
	f.mu.Lock()
	f.clock = f.clock.Add(f.idle.interval)
	f.mu.Unlock()
	f.idle.signal()
}

func (f *idleFixture) publishedIn(ws workspace.ID) []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.published[ws]...)
}

func (f *idleFixture) waitPublished(t *testing.T, ws workspace.ID, count int) {
	t.Helper()
	waitCondition(t, func() bool { return len(f.publishedIn(ws)) >= count })
}

// waitQuiet waits until no chunk is outstanding and a scheduler pass after
// the last published chunk dispatched nothing: no check runs again until the
// clock advances or a new workspace registers.
func (f *idleFixture) waitQuiet(t *testing.T) {
	t.Helper()
	waitCondition(t, func() bool {
		f.idle.mu.Lock()
		outstanding := f.idle.outstanding
		f.idle.mu.Unlock()
		f.mu.Lock()
		defer f.mu.Unlock()
		return outstanding == 0 && f.quiet > 0
	})
}

func idleSessionID(ws workspace.ID, session int) string {
	return fmt.Sprintf("sesn_%s_%d", ws, session)
}

func (f *idleFixture) scope(ws workspace.ID, session int, thread string) ReadScope {
	scope := ReadScope{WorkspaceID: ws, SessionID: idleSessionID(ws, session)}
	if thread != "" {
		scope.ThreadID = fmt.Sprintf("thr_%s_%d", thread, session)
	}
	return scope
}

func (f *idleFixture) head(scope ReadScope) func(context.Context) (int64, error) {
	return func(ctx context.Context) (int64, error) {
		if scope.ThreadID == "" {
			return f.reader.CurrentStreamPosition(ctx, scope.WorkspaceID, scope.SessionID)
		}
		return f.reader.CurrentThreadStreamPosition(ctx, scope.WorkspaceID, scope.SessionID, scope.ThreadID)
	}
}

func (f *idleFixture) read(scope ReadScope) func(context.Context, int64) ([]StreamChange, error) {
	return func(ctx context.Context, after int64) ([]StreamChange, error) {
		if scope.ThreadID == "" {
			return f.reader.ListSessionEventChanges(ctx, scope.WorkspaceID, scope.SessionID, after, defaultStreamBatchSize)
		}
		return f.reader.ListThreadEventChanges(ctx, scope.WorkspaceID, scope.SessionID, scope.ThreadID, after, defaultStreamBatchSize)
	}
}

type idleViewer struct {
	sink   *streamSink
	cancel context.CancelFunc
	done   chan struct{}
}

// open serves one viewer through the production stream loop until the test
// ends or the viewer is cancelled. read, when set, replaces its change read.
func (f *idleFixture) open(t *testing.T, scope ReadScope, read func(context.Context, int64) ([]StreamChange, error)) *idleViewer {
	t.Helper()
	if read == nil {
		read = f.read(scope)
	}
	ctx, cancel := context.WithCancel(t.Context())
	viewer := &idleViewer{sink: newStreamSink(), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(viewer.done)
		serveDeclaredStream(viewer.sink, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), scope, func(w http.ResponseWriter, r *http.Request) {
			f.handler.streamEvents(w, r, scope, nil, f.head(scope), read)
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-viewer.done
	})
	select {
	case <-viewer.sink.opened:
	case <-viewer.done:
		t.Fatal("viewer closed before opening")
	case <-time.After(5 * time.Second):
		t.Fatal("viewer did not open")
	}
	return viewer
}

func (v *idleViewer) waitIDs(t *testing.T, want string) {
	t.Helper()
	waitCondition(t, func() bool { return strings.Join(sseDataIDs(t, v.sink.String()), ",") == want })
}

func (v *idleViewer) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-v.done:
	case <-time.After(5 * time.Second):
		t.Fatal("viewer stayed open")
	}
}

// seedIdleSessions creates Sessions 1..count of one workspace (idleSessionID),
// each with a public main Thread thr_main_N and a public child Thread
// thr_child_N.
func seedIdleSessions(t *testing.T, admin *sql.DB, ws workspace.ID, count int) {
	t.Helper()
	for _, statement := range []string{
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, now()) ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, 'agent_' || $1, 'idle', 1, now(), now())`,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, 'agv_' || $1, 'agent_' || $1, 1, '{}', 'idle', now())`,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, 'env_' || $1, 'idle', '{}', now(), now())`,
		`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at)
		 SELECT $1, 'sesn_' || $1 || '_' || i, 'thr_main_' || i, 'session', 'idle', 'active', 'agent_' || $1, 1, 'env_' || $1, now(), now() FROM generate_series(1, $2::int) AS i`,
		`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at)
		 SELECT $1, 'thr_main_' || i, 'sesn_' || $1 || '_' || i, 'main', 'public', 'idle', now(), now(), now() FROM generate_series(1, $2::int) AS i`,
		`INSERT INTO session_threads (workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name, created_at, last_active_at, updated_at)
		 SELECT $1, 'thr_child_' || i, 'sesn_' || $1 || '_' || i, 'thr_main_' || i, 'subagent', 'public', 'idle', 'child', now(), now(), now() FROM generate_series(1, $2::int) AS i`,
	} {
		args := []any{string(ws)}
		if strings.Contains(statement, "generate_series") {
			args = append(args, count)
		}
		if _, err := admin.ExecContext(t.Context(), statement, args...); err != nil {
			t.Fatalf("seed idle sessions: %v", err)
		}
	}
}

// seedIdleChange commits one public event with one change on the scope's
// Thread (the main Thread for a Session scope). Only a main-Thread change is
// Session-visible. age backdates the change for retention.
func seedIdleChange(t *testing.T, admin *sql.DB, scope ReadScope, eventID string, age time.Duration) int64 {
	t.Helper()
	var position int64
	err := admin.QueryRowContext(t.Context(), `WITH thread AS (
	  SELECT t.id, t.role = 'main' AS session_visible FROM session_threads t
	   WHERE t.workspace_id = $1 AND t.session_id = $2
	     AND t.id = COALESCE(NULLIF($3, ''), (SELECT s.main_thread_id FROM sessions s WHERE s.workspace_id = $1 AND s.id = $2))),
	event AS (
	  INSERT INTO session_events (workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json, visibility, session_visible, created_at, updated_at, processed_at)
	  SELECT $1, $2, thread.id, $4,
	         (SELECT COALESCE(MAX(e.sequence), 0) + 1 FROM session_events e WHERE e.workspace_id = $1 AND e.session_id = $2 AND e.session_thread_id = thread.id),
	         'agent.thinking', '{}', 'public', thread.session_visible, now(), now(), now()
	    FROM thread
	  RETURNING workspace_id, session_id, session_thread_id, event_id, session_visible)
	INSERT INTO session_event_stream_changes (workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at)
	SELECT workspace_id, session_id, event_id, session_thread_id, 1, 'public', session_visible, clock_timestamp() - $5::interval FROM event
	RETURNING stream_position`, string(scope.WorkspaceID), scope.SessionID, scope.ThreadID, eventID, fmt.Sprintf("%d microseconds", age.Microseconds())).Scan(&position)
	if err != nil {
		t.Fatalf("seed change %s: %v", eventID, err)
	}
	if _, err := admin.ExecContext(t.Context(), `UPDATE session_events SET insert_stream_position = $2, latest_stream_position = $2 WHERE workspace_id = $3 AND event_id = $1`, eventID, position, string(scope.WorkspaceID)); err != nil {
		t.Fatal(err)
	}
	return position
}

// One statement checks up to 128 watched Sessions of a workspace however many
// viewers watch them: one for 128 Sessions, still one per round when each
// Session gains a second viewer, two for 129, and none once nothing is
// watched.
func TestSharedIdleCheckStatementsFollowUniqueSessions(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_count")
	seedIdleSessions(t, f.admin, ws, 129)
	var watches []*idleWatch
	join := func(session int) {
		watch, err := f.idle.join(ws, idleSessionID(ws, session), false)
		if err != nil {
			t.Fatal(err)
		}
		watches = append(watches, watch)
	}
	for session := 1; session <= 128; session++ {
		join(session)
	}
	f.start()
	f.waitPublished(t, ws, 1)
	f.waitQuiet(t)
	if started := f.trace.startedIn(ws); started != 1 {
		t.Fatalf("128 watched Sessions took %d statements; want 1", started)
	}
	for session := 1; session <= 128; session++ {
		join(session)
	}
	f.advance()
	f.waitPublished(t, ws, 2)
	f.waitQuiet(t)
	if started := f.trace.startedIn(ws); started != 2 {
		t.Fatalf("duplicated viewers took %d statements over two rounds; want 2", started)
	}
	join(129)
	f.advance()
	f.waitPublished(t, ws, 4)
	f.waitQuiet(t)
	batches := f.trace.batchesIn(ws)
	if len(batches) != 4 || len(batches[2]) != 128 || len(batches[3]) != 1 || batches[3][0] != idleSessionID(ws, 129) {
		t.Fatalf("129 watched Sessions: %d statements with last batches %v", len(batches), batchSizes(batches))
	}
	for _, watch := range watches {
		watch.close()
	}
	f.mu.Lock()
	passes := f.passes
	f.mu.Unlock()
	f.advance()
	waitCondition(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.passes > passes
	})
	f.advance()
	f.waitQuiet(t)
	if started := f.trace.startedIn(ws); started != 4 {
		t.Fatalf("an empty registry ran %d more statements", started-4)
	}
}

func batchSizes(batches [][]string) []int {
	sizes := make([]int, 0, len(batches))
	for _, batch := range batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

// Two Session viewers with different cursors and a Thread viewer of the same
// Session share one registration and one Session ID per statement. Each wake
// runs each viewer's own scoped read from its own cursor.
func TestSharedIdleCheckWakesEveryViewerOfASession(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_shared")
	seedIdleSessions(t, f.admin, ws, 1)
	f.start()
	first := f.open(t, f.scope(ws, 1, ""), nil)
	f.waitPublished(t, ws, 1)
	seedIdleChange(t, f.admin, f.scope(ws, 1, ""), "evt_shared_1", 0)
	f.advance()
	first.waitIDs(t, "evt_shared_1")
	second := f.open(t, f.scope(ws, 1, ""), nil)
	thread := f.open(t, f.scope(ws, 1, "child"), nil)
	f.idle.mu.Lock()
	refs := f.idle.workspaces[ws].sessions[idleSessionID(ws, 1)].refs
	f.idle.mu.Unlock()
	if refs != 3 {
		t.Fatalf("three viewers hold %d references", refs)
	}
	seedIdleChange(t, f.admin, f.scope(ws, 1, ""), "evt_shared_2", 0)
	seedIdleChange(t, f.admin, f.scope(ws, 1, "child"), "evt_shared_child", 0)
	f.advance()
	first.waitIDs(t, "evt_shared_1,evt_shared_2")
	second.waitIDs(t, "evt_shared_2")
	thread.waitIDs(t, "evt_shared_child")
	f.waitQuiet(t)
	for _, batch := range f.trace.batchesIn(ws) {
		if len(batch) != 1 || batch[0] != idleSessionID(ws, 1) {
			t.Fatalf("statement batches %v; want only the one Session once", f.trace.batchesIn(ws))
		}
	}
	if started := f.trace.startedIn(ws); started != 3 {
		t.Fatalf("three rounds ran %d statements", started)
	}
}

// viewerReads counts one viewer's formal change reads and records its Session
// registration's generation when the latest one started.
type viewerReads struct {
	mu      sync.Mutex
	count   int
	running bool
	from    uint64
}

// settled reports whether the viewer finished a read that started at
// generation: a viewer woken by that generation has already read.
func (r *viewerReads) settled(generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count > 0 && !r.running && r.from == generation
}

func (r *viewerReads) reads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

func (f *idleFixture) generation(ws workspace.ID, sessionID string) uint64 {
	f.idle.mu.Lock()
	defer f.idle.mu.Unlock()
	if entry := f.idle.workspaces[ws]; entry != nil && entry.sessions[sessionID] != nil {
		return entry.sessions[sessionID].generation
	}
	return 0
}

// countedRead is the scope's change read, counted in reads.
func (f *idleFixture) countedRead(scope ReadScope, reads *viewerReads) func(context.Context, int64) ([]StreamChange, error) {
	read := f.read(scope)
	return func(ctx context.Context, after int64) ([]StreamChange, error) {
		generation := f.generation(scope.WorkspaceID, scope.SessionID)
		reads.mu.Lock()
		reads.count++
		reads.running, reads.from = true, generation
		reads.mu.Unlock()
		defer func() {
			reads.mu.Lock()
			reads.running = false
			reads.mu.Unlock()
		}()
		return read(ctx, after)
	}
}

// Idle viewers cost no reads: after its opening read and the one re-read that
// the registration's first check forces, neither a Session viewer nor a
// Thread viewer of the Session reads again while further checks find it
// unchanged, and both still deliver the next change.
func TestUnchangedIdleChecksWakeNoViewer(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_unchanged")
	seedIdleSessions(t, f.admin, ws, 1)
	sessionScope, threadScope := f.scope(ws, 1, ""), f.scope(ws, 1, "child")
	sessionReads, threadReads := &viewerReads{}, &viewerReads{}
	session := f.open(t, sessionScope, f.countedRead(sessionScope, sessionReads))
	thread := f.open(t, threadScope, f.countedRead(threadScope, threadReads))
	// Both opening reads finish before any check runs.
	waitCondition(t, func() bool { return sessionReads.settled(0) && threadReads.settled(0) })
	f.start()
	for round := 1; round <= 4; round++ {
		if round > 1 {
			f.advance()
		}
		f.waitPublished(t, ws, round)
		// A viewer woken by this check has read once it settles at the
		// current generation.
		generation := f.generation(ws, sessionScope.SessionID)
		waitCondition(t, func() bool { return sessionReads.settled(generation) && threadReads.settled(generation) })
		if sessionCount, threadCount := sessionReads.reads(), threadReads.reads(); sessionCount != 2 || threadCount != 2 {
			t.Fatalf("after check %d the Session viewer read %d times and the Thread viewer %d; want 2 each", round, sessionCount, threadCount)
		}
	}
	seedIdleChange(t, f.admin, sessionScope, "evt_unchanged_main", 0)
	seedIdleChange(t, f.admin, threadScope, "evt_unchanged_child", 0)
	f.advance()
	session.waitIDs(t, "evt_unchanged_main")
	thread.waitIDs(t, "evt_unchanged_child")
}

// A change committed after a new viewer's empty read but inside the
// registration's first check is not lost: the first result always counts as a
// change.
func TestIdleViewerWakesOnTheFirstCheckAfterRegistration(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_first")
	seedIdleSessions(t, f.admin, ws, 1)
	scope := f.scope(ws, 1, "")
	f.trace.hold(ws)
	f.start()
	emptyRead := make(chan struct{})
	var once sync.Once
	viewer := f.open(t, scope, func(ctx context.Context, after int64) ([]StreamChange, error) {
		changes, err := f.read(scope)(ctx, after)
		if err == nil && len(changes) == 0 {
			once.Do(func() { close(emptyRead) })
		}
		return changes, err
	})
	waitCondition(t, func() bool { return f.trace.startedIn(ws) == 1 })
	select {
	case <-emptyRead:
	case <-time.After(5 * time.Second):
		t.Fatal("opening read did not finish")
	}
	seedIdleChange(t, f.admin, scope, "evt_first_check", 0)
	f.trace.release(ws)
	viewer.waitIDs(t, "evt_first_check")
	if published := len(f.publishedIn(ws)); published != 1 {
		t.Fatalf("delivery needed %d checks; want the first", published)
	}
}

// A check that changes the signal while the viewer's read is in flight, after
// that read's generation sample, makes an empty read run again instead of
// sleeping.
func TestIdleViewerRereadsWhenACheckChangesDuringItsEmptyRead(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_race")
	seedIdleSessions(t, f.admin, ws, 1)
	scope := f.scope(ws, 1, "")
	f.start()
	var once sync.Once
	held, resume := make(chan struct{}), make(chan struct{})
	viewer := f.open(t, scope, func(ctx context.Context, after int64) ([]StreamChange, error) {
		changes, err := f.read(scope)(ctx, after)
		// The feed is empty until evt_race_1, so the first empty read from a
		// positive cursor is the one that follows it. Hold it after its
		// statement returned, before the loop sees the result.
		hold := false
		if after > 0 && err == nil && len(changes) == 0 {
			once.Do(func() { hold = true })
		}
		if hold {
			close(held)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
		return changes, err
	})
	f.waitPublished(t, ws, 1)
	f.waitQuiet(t)
	seedIdleChange(t, f.admin, scope, "evt_race_1", 0)
	f.advance()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("empty read after the first change was not reached")
	}
	viewer.waitIDs(t, "evt_race_1")
	f.waitPublished(t, ws, 2)
	seedIdleChange(t, f.admin, scope, "evt_race_2", 0)
	f.advance()
	f.waitPublished(t, ws, 3)
	close(resume)
	viewer.waitIDs(t, "evt_race_1,evt_race_2")
	if published := len(f.publishedIn(ws)); published != 3 {
		t.Fatalf("delivery needed %d checks; want 3", published)
	}
}

// A change inserted and pruned while a round waits to run leaves the newest
// retained change unchanged; the Session's highest watermark still changes the
// signal, so the viewer wakes and its read reports the lost history.
func TestIdleViewerWakesForAChangePrunedBeforeTheCheckRan(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_prune")
	seedIdleSessions(t, f.admin, ws, 1)
	scope := f.scope(ws, 1, "")
	retained := seedIdleChange(t, f.admin, scope, "evt_prune_kept", 0)
	f.start()
	viewer := f.open(t, scope, nil)
	f.waitPublished(t, ws, 1)
	f.waitQuiet(t)
	f.trace.hold(ws)
	f.advance()
	waitCondition(t, func() bool { return f.trace.startedIn(ws) == 2 })
	pruned := seedIdleChange(t, f.admin, scope, "evt_prune_lost", 48*time.Hour)
	var deleted int
	cleanup := f.role.OpenWorkload(t, "cleanup", nil)
	if err := cleanup.QueryRowContext(t.Context(), `SELECT deleted_count FROM public.tetral_prune_event_changes(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`).Scan(&deleted); err != nil || deleted != 1 {
		t.Fatalf("prune = %d/%v", deleted, err)
	}
	var newest int64
	if err := f.admin.QueryRowContext(t.Context(), `SELECT max(stream_position) FROM session_event_stream_changes WHERE workspace_id = $1 AND session_id = $2`, string(ws), scope.SessionID).Scan(&newest); err != nil || newest != retained || pruned <= retained {
		t.Fatalf("newest retained change = %d/%v; want %d below the pruned %d", newest, err, retained, pruned)
	}
	f.trace.release(ws)
	viewer.waitClosed(t)
	if logs := f.logs.String(); !strings.Contains(logs, `"reason":"retained_history_gap"`) {
		t.Fatalf("viewer closed without the gap: %s", logs)
	}
}

// Deleting the parent Session changes only its lifecycle: no change row, no
// watermark. The Thread viewer still wakes and its read closes the feed.
func TestIdleThreadViewerWakesWhenOnlyItsSessionIsDeleted(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_parent")
	seedIdleSessions(t, f.admin, ws, 1)
	f.start()
	viewer := f.open(t, f.scope(ws, 1, "child"), nil)
	f.waitPublished(t, ws, 1)
	f.waitQuiet(t)
	if _, err := f.admin.ExecContext(t.Context(), `UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = $1 AND id = $2`, string(ws), idleSessionID(ws, 1)); err != nil {
		t.Fatal(err)
	}
	f.advance()
	viewer.waitClosed(t)
	if published := len(f.publishedIn(ws)); published != 2 {
		t.Fatalf("closing took %d checks; want 2", published)
	}
}

// failingSignals fails every check of the selected workspaces.
type failingSignals struct {
	SessionSignalReader
	mu   sync.Mutex
	fail map[workspace.ID]bool
}

func (s *failingSignals) set(ws workspace.ID, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail[ws] = fail
}

func (s *failingSignals) ReadSessionSignals(ctx context.Context, ws workspace.ID, sessionIDs []string) ([]SessionSignal, error) {
	s.mu.Lock()
	fail := s.fail[ws]
	s.mu.Unlock()
	if fail {
		return nil, errors.New("connection reset by peer while reading " + strings.Join(sessionIDs, ","))
	}
	return s.SessionSignalReader.ReadSessionSignals(ctx, ws, sessionIDs)
}

// A failed check detaches its chunk's registrations with a sticky error: their
// viewers close through the reader-failure path, a later Join gets a fresh
// registration that the old one can neither update nor remove, the failure is
// logged without identities, and other workspaces keep running.
func TestFailedIdleCheckClosesItsViewersAndReplacesTheRegistration(t *testing.T) {
	signals := &failingSignals{fail: map[workspace.ID]bool{}}
	f := newIdleFixture(t, func(real SessionSignalReader) SessionSignalReader {
		signals.SessionSignalReader = real
		return signals
	})
	failing, healthy := workspace.ID("ws_idle_fail"), workspace.ID("ws_idle_healthy")
	seedIdleSessions(t, f.admin, failing, 2)
	seedIdleSessions(t, f.admin, healthy, 1)
	f.start()
	first := f.open(t, f.scope(failing, 1, ""), nil)
	second := f.open(t, f.scope(failing, 2, "child"), nil)
	other := f.open(t, f.scope(healthy, 1, ""), nil)
	stale, err := f.idle.join(failing, idleSessionID(failing, 1), false)
	if err != nil {
		t.Fatal(err)
	}
	f.waitPublished(t, failing, 1)
	f.waitPublished(t, healthy, 1)
	f.waitQuiet(t)
	signals.set(failing, true)
	seedIdleChange(t, f.admin, f.scope(healthy, 1, ""), "evt_healthy_1", 0)
	f.advance()
	first.waitClosed(t)
	second.waitClosed(t)
	other.waitIDs(t, "evt_healthy_1")
	logs := f.logs.String()
	for _, want := range []string{`"msg":"event_stream.idle_check_failed"`, `"workspace.id":"ws_idle_fail"`, `"failed.count":2`, `"reason":"query_failed"`, `"outcome":"detached"`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("failure log missing %s: %s", want, logs)
		}
	}
	if strings.Contains(logs, idleSessionID(failing, 1)) || strings.Contains(logs, "connection reset") {
		t.Fatalf("failure log carried identities or the raw error: %s", logs)
	}
	signals.set(failing, false)
	replacement := f.open(t, f.scope(failing, 1, ""), nil)
	f.waitPublished(t, failing, 3)
	f.waitQuiet(t)
	if _, err := stale.sample(); !errors.Is(err, errIdleCheckFailed) {
		t.Fatalf("stale registration after a successful check = %v; want the sticky failure", err)
	}
	stale.close()
	f.idle.mu.Lock()
	current := f.idle.workspaces[failing].sessions[idleSessionID(failing, 1)]
	f.idle.mu.Unlock()
	if current == nil || current == stale.registration || !current.observed {
		t.Fatal("the stale registration replaced or removed its successor")
	}
	seedIdleChange(t, f.admin, f.scope(failing, 1, ""), "evt_replaced_1", 0)
	f.advance()
	replacement.waitIDs(t, "evt_replaced_1")
}

// A viewer that leaves while its Session's check is in flight unregisters at
// once. A viewer joining meanwhile gets a new registration, which the old
// in-flight result does not touch; the next round checks it.
func TestInFlightIdleCheckCannotUpdateAReplacedRegistration(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws := workspace.ID("ws_idle_cancel")
	seedIdleSessions(t, f.admin, ws, 1)
	scope := f.scope(ws, 1, "")
	f.start()
	leaving := f.open(t, scope, nil)
	f.waitPublished(t, ws, 1)
	f.waitQuiet(t)
	f.trace.hold(ws)
	f.advance()
	waitCondition(t, func() bool { return f.trace.startedIn(ws) == 2 })
	seedIdleChange(t, f.admin, scope, "evt_cancel_1", 0)
	f.idle.mu.Lock()
	old := f.idle.workspaces[ws].sessions[scope.SessionID]
	f.idle.mu.Unlock()
	leaving.cancel()
	leaving.waitClosed(t)
	joining := f.open(t, scope, nil)
	f.idle.mu.Lock()
	replacement := f.idle.workspaces[ws].sessions[scope.SessionID]
	f.idle.mu.Unlock()
	if replacement == nil || replacement == old || !old.detached {
		t.Fatal("a Join after the last viewer left reused the old registration")
	}
	f.trace.release(ws)
	f.waitPublished(t, ws, 2)
	f.waitQuiet(t)
	f.idle.mu.Lock()
	touched := replacement.observed || replacement.generation != 0
	current := f.idle.workspaces[ws].sessions[scope.SessionID]
	f.idle.mu.Unlock()
	if touched || current != replacement {
		t.Fatal("the old in-flight result updated or replaced the new registration")
	}
	seedIdleChange(t, f.admin, scope, "evt_cancel_2", 0)
	f.advance()
	joining.waitIDs(t, "evt_cancel_2")
}

// Shutdown with a check blocked inside PostgreSQL: the registry stays usable
// while it is blocked, and Close stops admission, fails every registration,
// cancels the statement and returns only after its worker finished, while the
// blocking lock is still held.
func TestIdleCheckShutdownCancelsAndJoinsABlockedCheck(t *testing.T) {
	f := newIdleFixture(t, nil)
	ws, other := workspace.ID("ws_idle_shutdown"), workspace.ID("ws_idle_other")
	seedIdleSessions(t, f.admin, ws, 1)
	seedIdleSessions(t, f.admin, other, 1)
	watch, err := f.idle.join(ws, idleSessionID(ws, 1), false)
	if err != nil {
		t.Fatal(err)
	}
	f.start()
	f.waitPublished(t, ws, 1)
	f.waitQuiet(t)
	lock, err := f.admin.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.ExecContext(t.Context(), `LOCK TABLE session_event_feed_retention IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	f.advance()
	blocked := func() bool {
		var waiting int
		if err := f.admin.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_locks WHERE NOT granted AND relation = 'session_event_feed_retention'::regclass`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting == 1
	}
	waitCondition(t, func() bool { return f.trace.startedIn(ws) == 2 && blocked() })
	registry := make(chan error, 1)
	go func() {
		extra, err := f.idle.join(other, idleSessionID(other, 1), false)
		if err == nil {
			_, err = extra.sample()
			extra.close()
		}
		if err == nil {
			_, err = watch.sample()
		}
		registry <- err
	}()
	select {
	case err := <-registry:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the registry was held while a check was blocked in PostgreSQL")
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		f.idle.Close()
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the blocked check")
	}
	if results := f.publishedIn(ws); len(results) != 2 || results[1] == nil {
		t.Fatalf("Close returned before the blocked worker finished: results %v", results)
	}
	if _, err := watch.sample(); !errors.Is(err, errIdleChecksClosed) {
		t.Fatalf("registration after Close = %v", err)
	}
	if _, err := f.idle.join(ws, idleSessionID(ws, 1), false); !errors.Is(err, errIdleChecksClosed) {
		t.Fatalf("Join after Close = %v", err)
	}
	// The lock is still held, so the waiting statement can only leave by
	// cancellation.
	waitCondition(t, func() bool { return !blocked() })
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// A slow workspace holds one worker with one statement; the other four
// workspaces keep completing rounds on the remaining workers. After it
// resumes, its chunks run one at a time, and its overdue next round starts at
// once.
func TestSlowWorkspaceDoesNotStallOtherIdleChecks(t *testing.T) {
	f := newIdleFixture(t, nil)
	slow := workspace.ID("ws_idle_slow")
	fast := []workspace.ID{"ws_idle_fast_1", "ws_idle_fast_2", "ws_idle_fast_3", "ws_idle_fast_4"}
	seedIdleSessions(t, f.admin, slow, 300)
	for session := 1; session <= 300; session++ {
		if _, err := f.idle.join(slow, idleSessionID(slow, session), false); err != nil {
			t.Fatal(err)
		}
	}
	for _, ws := range fast {
		seedIdleSessions(t, f.admin, ws, 1)
		if _, err := f.idle.join(ws, idleSessionID(ws, 1), false); err != nil {
			t.Fatal(err)
		}
	}
	f.trace.hold(slow)
	f.start()
	for round := 1; round <= 3; round++ {
		if round > 1 {
			f.advance()
		}
		for _, ws := range fast {
			f.waitPublished(t, ws, round)
		}
		if started, published := f.trace.startedIn(slow), len(f.publishedIn(slow)); started != 1 || published != 0 {
			t.Fatalf("round %d: slow workspace started %d statements, finished %d; want one held", round, started, published)
		}
	}
	f.trace.release(slow)
	f.waitPublished(t, slow, 6)
	f.waitQuiet(t)
	f.trace.mu.Lock()
	defer f.trace.mu.Unlock()
	if f.trace.started[string(slow)] != 6 || f.trace.maxInFlight[string(slow)] != 1 || f.trace.maxTotal > idleCheckWorkers {
		t.Fatalf("slow workspace statements=%d max in flight=%d; process max in flight=%d", f.trace.started[string(slow)], f.trace.maxInFlight[string(slow)], f.trace.maxTotal)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if sizes := fmt.Sprint(f.chunks[slow]); sizes != "[128 128 44 128 128 44]" {
		t.Fatalf("slow workspace chunks %s; want two rounds of 128, 128, 44", sizes)
	}
}
