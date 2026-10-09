package eventstream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	readerpkg "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type projectionReader struct {
	Reader
	mu               sync.Mutex
	pages            int
	descriptorBodies int
	batches          [][]StreamChange
}

func (r *projectionReader) ListRequestFinalMessages(ctx context.Context, scope ReadScope, end string, after int64, limit int) ([]RequestFinalMessage, error) {
	r.mu.Lock()
	r.pages++
	r.mu.Unlock()
	return r.Reader.ListRequestFinalMessages(ctx, scope, end, after, limit)
}
func (r *projectionReader) pageCount() int { r.mu.Lock(); defer r.mu.Unlock(); return r.pages }

// retainChanges keeps each returned batch, sharing the writer's backing array,
// so the test can observe from outside whether the writer dropped its payload
// references. It also sums any body selected for a deferred text descriptor.
func (r *projectionReader) retainChanges(changes []StreamChange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, change := range changes {
		if change.DeferredMessage {
			r.descriptorBodies += len(change.Event.Payload)
		}
	}
	r.batches = append(r.batches, changes)
}

// retainedRows counts every retained change row and the rows that still hold
// any field. Call it only after a happens-before barrier with the writer.
func (r *projectionReader) retainedRows() (rows, live int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, batch := range r.batches {
		for _, change := range batch {
			rows++
			if !reflect.ValueOf(change).IsZero() {
				live++
			}
		}
	}
	return rows, live
}

// fixtureResponse is the writer's owned test sink. When hold is set, the first
// body Write that carries the marker is held until release or cancellation;
// event headers and heartbeats use WriteString and pass through.
type fixtureResponse struct {
	*httptest.ResponseRecorder
	opened  chan struct{}
	once    sync.Once
	ctx     context.Context
	marker  []byte
	held    chan int
	release chan struct{}
	holding bool
}

func (w *fixtureResponse) Flush() { w.once.Do(func() { close(w.opened) }) }
func (w *fixtureResponse) Write(p []byte) (int, error) {
	if w.held != nil && !w.holding && bytes.Contains(p, w.marker) {
		w.holding = true
		w.held <- len(p)
		select {
		case <-w.release:
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		}
	}
	return w.ResponseRecorder.Write(p)
}
func seedProjectionSession(t *testing.T, db *sql.DB) {
	t.Helper()
	queries := []string{
		`INSERT INTO agents(workspace_id,id,name,version,created_at,updated_at) VALUES('default','agent_projection','projection',1,now(),now())`,
		`INSERT INTO agent_versions(workspace_id,id,agent_id,version,config_json,config_hash,created_at) VALUES('default','agv_projection','agent_projection',1,'{}','projection',now())`,
		`INSERT INTO environments(workspace_id,id,name,config_json,created_at,updated_at) VALUES('default','env_projection','projection','{}',now(),now())`,
		`INSERT INTO sessions(workspace_id,id,main_thread_id,type,status,lifecycle_state,agent_id,agent_version,environment_id,created_at,updated_at) VALUES('default','sesn_preview','thr_main','session','idle','active','agent_projection',1,'env_projection',now(),now())`,
		`INSERT INTO session_threads(workspace_id,id,session_id,role,visibility,status,created_at,last_active_at,updated_at) VALUES('default','thr_main','sesn_preview','main','public','idle',now(),now(),now())`,
	}
	for _, query := range queries {
		if _, err := db.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
}
func seedProjectionEvent(t *testing.T, db *sql.DB, id string, sequence int, eventType, model, payload string) {
	t.Helper()
	projection := `{}`
	if eventType == "span.model_request_start" {
		projection = `{"request_kind":"agent_provider_request"}`
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO session_events(workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,model_request_id,projection_json,created_at,updated_at,processed_at) VALUES('default','sesn_preview','thr_main',$1,$2,$3,$4,NULLIF($5,''),$6,now(),now(),now())`, id, sequence, eventType, payload, model, projection); err != nil {
		t.Fatal(err)
	}
	var position int64
	if err := db.QueryRowContext(t.Context(), `INSERT INTO session_event_stream_changes(workspace_id,session_id,event_id,session_thread_id,revision,visibility,session_visible,changed_at) VALUES('default','sesn_preview',$1,'thr_main',1,'public',TRUE,now()) RETURNING stream_position`, id).Scan(&position); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE session_events SET insert_stream_position=$2,latest_stream_position=$2 WHERE event_id=$1`, id, position); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLRequestEndProjectionResidency(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	for _, mode := range []string{"session_formal", "session_preview", "thread_formal"} {
		for _, cancelAtWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel_%v", mode, cancelAtWrite), func(t *testing.T) {
				_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
				seedProjectionSession(t, admin)
				role := storagetest.OpenWorkloadDB(t, admin, "event_stream")
				real := readerpkg.NewPostgreSQLReader(dbconnect.NewClientForTesting(role.DB))
				reader := &projectionReader{Reader: real}
				scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}
				if mode == "thread_formal" {
					scope.ThreadID = "thr_main"
				}
				config := DefaultStreamConfig()
				transport := &fixtureTransport{}
				hub, err := NewPreviewHub(transport, config, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer hub.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
				firstBody := strings.Repeat("A", 256*1024)
				response := &fixtureResponse{ResponseRecorder: httptest.NewRecorder(), opened: make(chan struct{}), ctx: ctx, marker: []byte(firstBody), held: make(chan int, 1), release: make(chan struct{})}
				changesRelease := make(chan struct{})
				handler := &handler{reader: reader, options: newOptions(WithStreamConfig(config), WithPreviewHub(hub), WithIdleCoalescer(idleChecksForTest(t, unchangedSignals{}, false)))}
				done := make(chan struct{})
				var types map[string]bool
				if mode == "session_preview" {
					types = map[string]bool{"agent.message": true}
				}
				firstPoll := true
				go func() {
					defer close(done)
					serveDeclaredStream(response, request, scope, func(w http.ResponseWriter, r *http.Request) {
						handler.streamEvents(w, r, scope, types, func(ctx context.Context) (int64, error) {
							if scope.ThreadID == "" {
								return real.CurrentStreamPosition(ctx, workspace.DefaultID, scope.SessionID)
							}
							return real.CurrentThreadStreamPosition(ctx, workspace.DefaultID, scope.SessionID, scope.ThreadID)
						}, func(ctx context.Context, after int64) ([]StreamChange, error) {
							if firstPoll {
								firstPoll = false
								select {
								case <-changesRelease:
								case <-ctx.Done():
									return nil, ctx.Err()
								}
							}
							var changes []StreamChange
							var err error
							if scope.ThreadID == "" {
								changes, err = real.ListSessionEventChanges(ctx, workspace.DefaultID, scope.SessionID, after, 100)
							} else {
								changes, err = real.ListThreadEventChanges(ctx, workspace.DefaultID, scope.SessionID, scope.ThreadID, after, 100)
							}
							reader.retainChanges(changes)
							return changes, err
						})
					})
				}()
				select {
				case <-response.opened:
				case <-time.After(5 * time.Second):
					t.Fatal("stream did not open")
				}
				seedProjectionEvent(t, admin, "evt_start", 1, "span.model_request_start", "mreq_preview", `{}`)
				expected := map[string]string{}
				for i, letter := range []string{"A", "B", "C"} {
					body := strings.Repeat(letter, 256*1024)
					payload, _ := json.Marshal(map[string]any{"content": []map[string]string{{"type": "text", "text": body}}})
					id := fmt.Sprintf("evt_message_%d", i)
					expected[id] = body
					seedProjectionEvent(t, admin, id, i+2, "agent.message", "mreq_preview", string(payload))
				}
				seedProjectionEvent(t, admin, "evt_end", 5, "span.model_request_end", "mreq_preview", `{"model_request_start_id":"evt_start","is_error":false}`)
				seedProjectionEvent(t, admin, "evt_suffix", 6, "session.status_idle", "", `{"stop_reason":"suffix_sentinel"}`)
				seedProjectionEvent(t, admin, "evt_deleted", 7, "session.deleted", "", `{}`)
				close(changesRelease)
				var heldBytes int
				select {
				case heldBytes = <-response.held:
				case <-time.After(5 * time.Second):
					t.Fatal("first complete-text response write not reached")
				}
				// The first End-group body is held at the sink. The single reader
				// batch (Start, three deferred texts, End and the two-row suffix)
				// no longer references any payload, exactly one End page was
				// requested, and the held write is the current full encoding.
				if rows, live := reader.retainedRows(); rows != 7 || live != 0 {
					t.Fatalf("reader-returned change rows=%d still referenced=%d at the first final write", rows, live)
				}
				if heldBytes < 256*1024 || reader.pageCount() != 1 {
					t.Fatalf("held write bytes=%d pages=%d", heldBytes, reader.pageCount())
				}
				reader.mu.Lock()
				bodied := reader.descriptorBodies
				reader.mu.Unlock()
				if bodied != 0 {
					t.Fatalf("SQL change reader materialized %d generated-text bytes", bodied)
				}
				history, err := real.ListThreadEvents(t.Context(), workspace.DefaultID, scope.SessionID, "thr_main", readerpkg.ListOptions{Limit: 20, Order: "asc"})
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range history.Data {
					if body, ok := expected[event.ID]; ok && !bytes.Contains(event.Payload, []byte(body)) {
						t.Fatalf("history body changed for %s", event.ID)
					}
				}
				if scope.ThreadID == "" {
					// Mark the lifecycle deleted, as DeleteSession does alongside its
					// session.deleted change, while the group is mid-publication. The
					// open Session feed still publishes the remaining End pages, End,
					// suffix and deletion.
					if _, err := admin.ExecContext(t.Context(), `UPDATE sessions SET lifecycle_state='deleted' WHERE workspace_id='default' AND id='sesn_preview'`); err != nil {
						t.Fatal(err)
					}
				}
				if cancelAtWrite {
					cancel()
				} else {
					close(response.release)
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("writer did not join")
				}
				wantLatencySamples := uint64(7)
				if cancelAtWrite {
					wantLatencySamples = 1
				}
				if latency := handler.options.previewMetrics.formalLatency.snapshot(); latency.count != wantLatencySamples || latency.buckets[5] != wantLatencySamples {
					t.Fatal("actual successful formal writes missing monotonic latency samples")
				}
				if handler.options.previewMetrics.activeStreams.Load() != 0 || handler.options.previewMetrics.activeRequests.Load() != 0 {
					t.Fatal("real writer cleanup retained active ownership")
				}
				if cancelAtWrite {
					if reader.pageCount() != 1 || strings.Contains(response.Body.String(), "evt_message_") {
						t.Fatal("canceled writer fetched/emitted a later page")
					}
				} else {
					events := []map[string]any{}
					for _, line := range strings.Split(response.Body.String(), "\n") {
						if strings.HasPrefix(line, "data: ") {
							var event map[string]any
							if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
								t.Fatal(err)
							}
							events = append(events, event)
						}
					}
					ids := []string{}
					for _, event := range events {
						ids = append(ids, event["id"].(string))
						if body, ok := expected[event["id"].(string)]; ok {
							content := event["content"].([]any)
							if content[0].(map[string]any)["text"] != body {
								t.Fatalf("wrong formal content for %v", event["id"])
							}
						}
					}
					if strings.Join(ids, ",") != "evt_start,evt_message_0,evt_message_1,evt_message_2,evt_end,evt_suffix,evt_deleted" {
						t.Fatalf("End group/suffix order=%v", ids)
					}
					if reader.pageCount() != 4 {
						t.Fatalf("page requests=%d want one per body plus EOF", reader.pageCount())
					}
				}
				if mode != "session_preview" && len(transport.subscriptions) != 0 {
					t.Fatal("formal-only viewer subscribed")
				}
			})
		}
	}
}

// Preview loss observed after the session became unreadable stops previews and
// releases the subscription; formal delivery continues to session.deleted.
func TestStreamLoopPreviewLossAfterSessionDeletionContinuesFormalDelivery(t *testing.T) {
	config := DefaultStreamConfig()
	metrics := NewPreviewMetrics()
	transport := &fixtureTransport{}
	hub, err := NewPreviewHub(transport, config, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	h := &handler{options: newOptions(WithStreamConfig(config), WithPreviewHub(hub), WithPreviewMetrics(metrics), WithIdleCoalescer(idleChecksForTest(t, unchangedSignals{}, false)))}
	scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}
	var mu sync.Mutex
	deleted, unreadableServed := false, false
	viewersAtDeletion, subscriptionsAtDeletion := int64(-1), int64(-1)
	currentPosition := func(context.Context) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		if deleted {
			unreadableServed = true
			return 0, &httpapi.NotFoundError{Message: "session not found"}
		}
		return 10, nil
	}
	// The deletion change becomes visible only after the loss re-read found the
	// session unreadable, so the stream must survive that re-read to emit it.
	listChanges := func(_ context.Context, after int64) ([]StreamChange, error) {
		mu.Lock()
		defer mu.Unlock()
		if !unreadableServed || after >= 11 {
			return nil, nil
		}
		viewersAtDeletion, subscriptionsAtDeletion = metrics.viewers.Load(), metrics.subscriptions.Load()
		return []StreamChange{{StreamPosition: 11, Event: Event{ID: "evt_deleted", Type: "session.deleted", SessionID: scope.SessionID, Payload: json.RawMessage(`{}`)}}}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	response := &fixtureResponse{ResponseRecorder: httptest.NewRecorder(), opened: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveDeclaredStream(response, request, scope, func(w http.ResponseWriter, r *http.Request) {
			h.streamEvents(w, r, scope, map[string]bool{"agent.message": true}, currentPosition, listChanges)
		})
	}()
	select {
	case <-response.opened:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not open")
	}
	transport.mu.Lock()
	subscription := transport.subscriptions[0]
	transport.mu.Unlock()
	mu.Lock()
	deleted = true
	mu.Unlock()
	subscription.loss("nats_disconnect")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not close after session.deleted")
	}
	if !strings.Contains(response.Body.String(), "event: session.deleted\n") {
		t.Fatalf("session.deleted not delivered after preview loss: %q", response.Body.String())
	}
	if viewersAtDeletion != 0 || subscriptionsAtDeletion != 0 {
		t.Fatalf("preview ownership before session.deleted viewers=%d subscriptions=%d", viewersAtDeletion, subscriptionsAtDeletion)
	}
	if metrics.stoppedRequests.Load() != 1 || metrics.activeStreams.Load() != 0 || metrics.viewers.Load() != 0 || metrics.subscriptions.Load() != 0 {
		t.Fatal("preview stop or stream ownership unbalanced")
	}
}

// streamSink is a concurrency-safe response sink for observing a live stream.
type streamSink struct {
	header http.Header
	mu     sync.Mutex
	body   bytes.Buffer
	opened chan struct{}
	once   sync.Once
}

func newStreamSink() *streamSink {
	return &streamSink{header: make(http.Header), opened: make(chan struct{})}
}
func (s *streamSink) Header() http.Header { return s.header }
func (*streamSink) WriteHeader(int)       {}
func (s *streamSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.Write(p)
}
func (s *streamSink) Flush() { s.once.Do(func() { close(s.opened) }) }
func (s *streamSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.String()
}
func (s *streamSink) count(frame string) int { return strings.Count(s.String(), frame) }

func sseDataIDs(t *testing.T, body string) []string {
	t.Helper()
	ids := []string{}
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, event.ID)
	}
	return ids
}

type noFinalMessagesReader struct{ Reader }

func (noFinalMessagesReader) ListRequestFinalMessages(context.Context, ReadScope, string, int64, int) ([]RequestFinalMessage, error) {
	return nil, nil
}

// A backlog larger than one batch, an End group and its re-read suffix are
// delivered back to back; the loop waits only after a read that returned
// nothing. No shared idle check ever runs and the heartbeat timer is one
// minute, so any intermediate wait would never end.
func TestStreamLoopDrainsBacklogAndEndSuffixWithoutPollWait(t *testing.T) {
	config := DefaultStreamConfig()
	config.HeartbeatInterval = time.Minute
	h := &handler{reader: noFinalMessagesReader{}, options: newOptions(WithStreamConfig(config), WithIdleCoalescer(idleChecksForTest(t, unchangedSignals{}, false)))}
	rows := []StreamChange{}
	want := []string{}
	for position := int64(1); position <= 150; position++ {
		id := fmt.Sprintf("evt_backlog_%03d", position)
		rows = append(rows, StreamChange{StreamPosition: position, Event: Event{ID: id, Type: "session.status_idle", Payload: json.RawMessage(`{"stop_reason":"end_turn"}`)}})
		want = append(want, id)
	}
	rows = append(rows,
		StreamChange{StreamPosition: 151, Event: Event{ID: "evt_end", ThreadID: "thr_main", Type: "span.model_request_end", Payload: json.RawMessage(`{"model_request_start_id":"evt_start"}`)}, ModelRequestID: "mreq_backlog", RequestStartEventID: "evt_start", ThreadRole: "main", RequestKind: "agent_provider_request"},
		StreamChange{StreamPosition: 152, Event: Event{ID: "evt_suffix", Type: "session.status_idle", Payload: json.RawMessage(`{"stop_reason":"end_turn"}`)}})
	want = append(want, "evt_end", "evt_suffix")
	var mu sync.Mutex
	polls := 0
	idle := make(chan struct{})
	var idleOnce sync.Once
	listChanges := func(_ context.Context, after int64) ([]StreamChange, error) {
		mu.Lock()
		defer mu.Unlock()
		polls++
		batch := []StreamChange{}
		for _, row := range rows {
			if row.StreamPosition > after && len(batch) < defaultStreamBatchSize {
				batch = append(batch, row)
			}
		}
		if len(batch) == 0 {
			idleOnce.Do(func() { close(idle) })
		}
		return batch, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sink := newStreamSink()
	done := make(chan struct{})
	go func() {
		defer close(done)
		scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_backlog"}
		serveDeclaredStream(sink, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), scope, func(w http.ResponseWriter, r *http.Request) {
			h.streamEvents(w, r, scope, nil, func(context.Context) (int64, error) { return 0, nil }, listChanges)
		})
	}()
	select {
	case <-idle:
	case <-time.After(5 * time.Second):
		t.Fatal("backlog, End group and suffix were not drained before a poll-interval wait")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not join")
	}
	if got := sseDataIDs(t, sink.String()); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("delivered order=%v", got)
	}
	// Two full or End-truncated batches, the re-read suffix, then one empty poll.
	mu.Lock()
	defer mu.Unlock()
	if polls != 4 {
		t.Fatalf("formal polls=%d want one per non-empty batch plus one empty poll", polls)
	}
}

// previewLoopFixture opens a preview Session stream whose Start is already
// visible after the opening mark, then waits until the loop is idle. idle
// supplies the shared checks that wake the stream for formal changes.
func previewLoopFixture(t *testing.T, idle *IdleCoalescer, formal func(after int64) []StreamChange) (*fixtureTransport, *streamSink, func() int) {
	t.Helper()
	config := DefaultStreamConfig()
	config.HeartbeatInterval = time.Minute
	metrics := NewPreviewMetrics()
	transport := &fixtureTransport{}
	hub, err := NewPreviewHub(transport, config, metrics)
	if err != nil {
		t.Fatal(err)
	}
	reader := &admissionReader{descriptor: PreviewRequest{StartStreamPosition: 11, RequestKind: "agent_provider_request", ThreadRole: "main", ThreadVisibility: "public", IsPrimaryThread: true}}
	h := &handler{reader: reader, options: newOptions(WithStreamConfig(config), WithPreviewHub(hub), WithPreviewMetrics(metrics), WithIdleCoalescer(idle))}
	start := StreamChange{StreamPosition: 11, Event: Event{ID: "evt_start", ThreadID: "thr_main", Type: "span.model_request_start", Payload: json.RawMessage(`{}`)}, ModelRequestID: "mreq_preview", RequestStartEventID: "evt_start", RequestStartStreamPosition: 11, RequestKind: "agent_provider_request", ThreadRole: "main"}
	var mu sync.Mutex
	polls := 0
	pollCount := func() int { mu.Lock(); defer mu.Unlock(); return polls }
	listChanges := func(_ context.Context, after int64) ([]StreamChange, error) {
		mu.Lock()
		polls++
		mu.Unlock()
		if after < start.StreamPosition {
			return []StreamChange{start}, nil
		}
		return formal(after), nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	sink := newStreamSink()
	done := make(chan struct{})
	go func() {
		defer close(done)
		scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}
		serveDeclaredStream(sink, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), scope, func(w http.ResponseWriter, r *http.Request) {
			h.streamEvents(w, r, scope, map[string]bool{"agent.message": true}, func(context.Context) (int64, error) { return 10, nil }, listChanges)
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("stream did not join")
		}
		hub.Close()
	})
	select {
	case <-sink.opened:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not open")
	}
	// The opening poll returns the Start; the next poll is empty.
	waitCondition(t, func() bool { return pollCount() >= 2 })
	transport.publish(t, fixtureFrame("request_open", "", 0, ""))
	transport.publish(t, fixtureFrame("event_start", "evt_message", 0, ""))
	waitCondition(t, func() bool { return sink.count("event: event_start\n") == 1 })
	return transport, sink, pollCount
}

// Each preview wake runs the preview slice without a formal database read.
// With no shared check running, fifty separately woken deltas leave only the
// opening read and the empty read that followed its progress.
func TestStreamLoopPreviewWakesDoNotPollPerDelta(t *testing.T) {
	transport, sink, pollCount := previewLoopFixture(t, idleChecksForTest(t, unchangedSignals{}, false), func(int64) []StreamChange { return nil })
	for sequence := int64(1); sequence <= 50; sequence++ {
		transport.publish(t, fixtureFrame("event_delta", "evt_message", sequence, "x"))
		waitCondition(t, func() bool { return sink.count("event: event_delta\n") == int(sequence) })
	}
	if polls := pollCount(); polls > 2 {
		t.Fatalf("preview wakes ran %d formal polls", polls)
	}
}

// A continuous delta flood cannot starve formal delivery: a row committed
// mid-flood changes the shared Session signal, and the woken stream writes it
// while deltas keep arriving.
func TestStreamLoopDeliversFormalRowDuringPreviewFlood(t *testing.T) {
	var mu sync.Mutex
	committed := false
	formalRow := StreamChange{StreamPosition: 12, Event: Event{ID: "evt_formal", Type: "session.status_idle", Payload: json.RawMessage(`{"stop_reason":"end_turn"}`)}}
	signals := signalFunc(func(sessionID string) SessionSignal {
		mu.Lock()
		defer mu.Unlock()
		signal := SessionSignal{SessionID: sessionID, Exists: true, LifecycleState: "active", Position: 11}
		if committed {
			signal.Position = formalRow.StreamPosition
		}
		return signal
	})
	transport, sink, _ := previewLoopFixture(t, idleChecksForTest(t, signals, true), func(after int64) []StreamChange {
		mu.Lock()
		defer mu.Unlock()
		if committed && after < formalRow.StreamPosition {
			return []StreamChange{formalRow}
		}
		return nil
	})
	deadline := time.Now().Add(5 * time.Second)
	for sequence := int64(1); !strings.Contains(sink.String(), `"id":"evt_formal"`); sequence++ {
		if time.Now().After(deadline) {
			t.Fatal("formal row starved by the preview flood")
		}
		if sequence == 10 {
			mu.Lock()
			committed = true
			mu.Unlock()
		}
		transport.publish(t, fixtureFrame("event_delta", "evt_message", sequence, "x"))
		waitCondition(t, func() bool { return sink.count("event: event_delta\n") == int(sequence) })
	}
}

// An admitted preview request whose Start is past the cursor forces a formal
// read by itself: no shared check ever runs here, yet the Start committed
// after the opening read is read and the request's preview starts.
func TestStreamLoopPreviewStartForcesAFormalRead(t *testing.T) {
	config := DefaultStreamConfig()
	config.HeartbeatInterval = time.Minute
	transport := &fixtureTransport{}
	hub, err := NewPreviewHub(transport, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	reader := &admissionReader{descriptor: PreviewRequest{StartStreamPosition: 11, RequestKind: "agent_provider_request", ThreadRole: "main", ThreadVisibility: "public", IsPrimaryThread: true}}
	h := &handler{reader: reader, options: newOptions(WithStreamConfig(config), WithPreviewHub(hub), WithIdleCoalescer(idleChecksForTest(t, unchangedSignals{}, false)))}
	start := StreamChange{StreamPosition: 11, Event: Event{ID: "evt_start", ThreadID: "thr_main", Type: "span.model_request_start", Payload: json.RawMessage(`{}`)}, ModelRequestID: "mreq_preview", RequestStartEventID: "evt_start", RequestStartStreamPosition: 11, RequestKind: "agent_provider_request", ThreadRole: "main"}
	var mu sync.Mutex
	committed, reads := false, 0
	listChanges := func(_ context.Context, after int64) ([]StreamChange, error) {
		mu.Lock()
		defer mu.Unlock()
		reads++
		if committed && after < start.StreamPosition {
			return []StreamChange{start}, nil
		}
		return nil, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sink := newStreamSink()
	done := make(chan struct{})
	go func() {
		defer close(done)
		scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}
		serveDeclaredStream(sink, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), scope, func(w http.ResponseWriter, r *http.Request) {
			h.streamEvents(w, r, scope, map[string]bool{"agent.message": true}, func(context.Context) (int64, error) { return 10, nil }, listChanges)
		})
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-sink.opened:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not open")
	}
	waitCondition(t, func() bool { mu.Lock(); defer mu.Unlock(); return reads == 1 })
	mu.Lock()
	committed = true
	mu.Unlock()
	transport.publish(t, fixtureFrame("request_open", "", 0, ""))
	transport.publish(t, fixtureFrame("event_start", "evt_message", 0, ""))
	waitCondition(t, func() bool { return sink.count("event: event_start\n") == 1 })
	if got := sseDataIDs(t, sink.String()); len(got) != 2 || got[0] != "evt_start" {
		t.Fatalf("delivered %v; want the formal Start before its preview", got)
	}
}

// serveDeclaredStream runs a stream handler as the router does: as the declared
// public stream operation for scope, on behalf of a principal of scope's
// Workspace, because streamEvents authorizes against both before reading.
func serveDeclaredStream(w http.ResponseWriter, r *http.Request, scope ReadScope, serve func(http.ResponseWriter, *http.Request)) {
	pattern := "/v1/sessions/{session_id}/events/stream"
	if scope.ThreadID != "" {
		pattern = "/v1/sessions/{session_id}/threads/{thread_id}/stream"
	}
	r = r.WithContext(auth.WithPrincipal(r.Context(), auth.IndependentKeyPrincipal(workspace.Workspace{ID: scope.WorkspaceID}, "ak_stream_fixture")))
	httpapi.DeclarePublicOperation(http.MethodGet, pattern, serve).ServeHTTP(w, r)
}
