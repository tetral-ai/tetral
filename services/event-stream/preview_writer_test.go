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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	readerpkg "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type projectionReader struct {
	Reader
	mu               sync.Mutex
	pages            int
	descriptorBodies int
}

func (r *projectionReader) ListRequestFinalMessages(ctx context.Context, scope ReadScope, end string, after int64, limit int) ([]RequestFinalMessage, error) {
	r.mu.Lock()
	r.pages++
	r.mu.Unlock()
	return r.Reader.ListRequestFinalMessages(ctx, scope, end, after, limit)
}
func (r *projectionReader) pageCount() int { r.mu.Lock(); defer r.mu.Unlock(); return r.pages }
func (r *projectionReader) checkChanges(changes []StreamChange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, change := range changes {
		if change.DeferredMessage {
			r.descriptorBodies += len(change.Event.Payload)
		}
	}
}

type fixtureResponse struct {
	*httptest.ResponseRecorder
	opened chan struct{}
	once   sync.Once
}

func (w *fixtureResponse) Flush() { w.once.Do(func() { close(w.opened) }) }
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
				response := &fixtureResponse{ResponseRecorder: httptest.NewRecorder(), opened: make(chan struct{})}
				changesRelease := make(chan struct{})
				atWrite := make(chan deliveryObservation, 1)
				writeRelease := make(chan struct{})
				observations := []deliveryObservation{}
				var observerMu sync.Mutex
				handler := &handler{reader: reader, options: newOptions(WithStreamConfig(config), WithPreviewHub(hub), withDeliveryObserver(func(observation deliveryObservation) {
					observerMu.Lock()
					observations = append(observations, observation)
					observerMu.Unlock()
					if observation.Phase == "final_write" {
						select {
						case atWrite <- observation:
						default:
						}
						select {
						case <-writeRelease:
						case <-ctx.Done():
						}
					}
				}))}
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
						reader.checkChanges(changes)
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
				var held deliveryObservation
				select {
				case held = <-atWrite:
				case <-time.After(5 * time.Second):
					t.Fatal("first complete-text writer barrier not reached")
				}
				if held.FinalBodyBytes < 256*1024 || held.EncodingBytes < 256*1024 || held.ChangeRows != 0 || held.ChangePayloadBytes != 0 || reader.pageCount() != 1 {
					t.Fatalf("held residency=%+v pages=%d", held, reader.pageCount())
				}
				observerMu.Lock()
				batchReleased := false
				for _, observation := range observations {
					if observation.Phase == "batch_released" {
						batchReleased = true
					}
				}
				observerMu.Unlock()
				if !batchReleased {
					t.Fatal("original batch/suffix retained across final materialization")
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
				if cancelAtWrite {
					cancel()
				} else {
					close(writeRelease)
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
				observerMu.Lock()
				last := observations[len(observations)-1]
				observerMu.Unlock()
				if last.Phase != "stream_released" || last.FinalBodyBytes != 0 || last.EncodingBytes != 0 {
					t.Fatalf("final residency=%+v", last)
				}
				if mode != "session_preview" && len(transport.subscriptions) != 0 {
					t.Fatal("formal-only viewer subscribed")
				}
			})
		}
	}
}
