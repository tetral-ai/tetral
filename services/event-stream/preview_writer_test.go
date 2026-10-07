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
				config.PollInterval = time.Millisecond
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
				handler := &handler{reader: reader, options: newOptions(WithStreamConfig(config), WithPreviewHub(hub))}
				done := make(chan struct{})
				var types map[string]bool
				if mode == "session_preview" {
					types = map[string]bool{"agent.message": true}
				}
				firstPoll := true
				go func() {
					defer close(done)
					handler.streamEvents(response, request, scope, types, func(ctx context.Context) (int64, error) {
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
				if handler.options.previewMetrics.formalLatency.count.Load() != wantLatencySamples || handler.options.previewMetrics.formalLatency.buckets[5].Load() != wantLatencySamples {
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
	config.PollInterval = time.Millisecond
	metrics := NewPreviewMetrics()
	transport := &fixtureTransport{}
	hub, err := NewPreviewHub(transport, config, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	h := &handler{options: newOptions(WithStreamConfig(config), WithPreviewHub(hub), WithPreviewMetrics(metrics))}
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
		h.streamEvents(response, request, scope, map[string]bool{"agent.message": true}, currentPosition, listChanges)
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
