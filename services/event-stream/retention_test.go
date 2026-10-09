package eventstream

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	readerpkg "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// A selected End group publishes its bodies from permanent events even when
// retention prunes the group's change rows while it is being written. The
// discarded suffix is then requeried from the End's position: if retention
// also pruned an unread suffix position, the stream closes through its
// reader-failure path with a fixed safe log line; if it pruned only through
// the End, the suffix is delivered.
func TestEndGroupCompletesBeforeAPrunedSuffixClosesTheStream(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	for _, testCase := range []struct {
		name       string
		prunedRows string
		wantIDs    string
		wantGapLog bool
	}{
		{"SuffixPruned", "'evt_start','evt_message','evt_end','evt_suffix_one'", "evt_start,evt_message,evt_end", true},
		{"PrunedThroughEnd", "'evt_start','evt_message','evt_end'", "evt_start,evt_message,evt_end,evt_suffix_one,evt_suffix_two", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedProjectionSession(t, admin)
			role := storagetest.OpenWorkloadDB(t, admin, "event_stream")
			cleanup := role.OpenWorkload(t, "cleanup", nil)
			real := readerpkg.NewPostgreSQLReader(dbconnect.NewClientForTesting(role.DB))
			scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}
			var logs bytes.Buffer
			handler := &handler{reader: real, options: newOptions(WithIdleCoalescer(idleChecksForTest(t, unchangedSignals{}, false)), WithLogger(workload.NewLogger(&logs, "event-stream", "test", "unit")))}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
			marker := []byte("held end group body")
			response := &fixtureResponse{ResponseRecorder: httptest.NewRecorder(), opened: make(chan struct{}), ctx: ctx, marker: marker, held: make(chan int, 1), release: make(chan struct{})}
			firstPoll, release := true, make(chan struct{})
			deliveredSuffix := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				serveDeclaredStream(response, request, scope, func(w http.ResponseWriter, r *http.Request) {
					handler.streamEvents(w, r, scope, nil, func(ctx context.Context) (int64, error) {
						return real.CurrentStreamPosition(ctx, workspace.DefaultID, scope.SessionID)
					}, func(ctx context.Context, after int64) ([]StreamChange, error) {
						requery := !firstPoll
						if firstPoll {
							firstPoll = false
							select {
							case <-release:
							case <-ctx.Done():
								return nil, ctx.Err()
							}
						}
						if deliveredSuffix {
							// The requeried suffix was delivered; end the connection.
							cancel()
							return nil, ctx.Err()
						}
						changes, err := real.ListSessionEventChanges(ctx, workspace.DefaultID, scope.SessionID, after, 100)
						for _, change := range changes {
							deliveredSuffix = deliveredSuffix || (requery && change.Event.ID == "evt_suffix_two")
						}
						return changes, err
					})
				})
			}()
			select {
			case <-response.opened:
			case <-time.After(5 * time.Second):
				t.Fatal("stream did not open")
			}
			seedProjectionEvent(t, admin, "evt_start", 1, "span.model_request_start", "mreq_retention", `{}`)
			seedProjectionEvent(t, admin, "evt_message", 2, "agent.message", "mreq_retention", `{"content":[{"type":"text","text":"`+string(marker)+`"}]}`)
			seedProjectionEvent(t, admin, "evt_end", 3, "span.model_request_end", "mreq_retention", `{"model_request_start_id":"evt_start","is_error":false}`)
			seedProjectionEvent(t, admin, "evt_suffix_one", 4, "session.status_running", "", `{}`)
			seedProjectionEvent(t, admin, "evt_suffix_two", 5, "session.status_idle", "", `{"stop_reason":"end_turn"}`)
			close(release)
			select {
			case <-response.held:
			case <-time.After(5 * time.Second):
				t.Fatal("End-group body write not reached")
			}
			// The End group is mid-publication. Age the selected rows and prune
			// them as the real Cleanup role.
			//nolint:gosec // G202: fixed event-ID list from the test table.
			if _, err := admin.Exec(`UPDATE session_event_stream_changes SET changed_at = clock_timestamp() - interval '2 days' WHERE event_id IN (` + testCase.prunedRows + `)`); err != nil {
				t.Fatal(err)
			}
			var deleted int
			if err := cleanup.QueryRow(`SELECT deleted_count FROM public.tetral_prune_event_changes(clock_timestamp() - interval '24 hours', NULL, NULL, NULL, NULL, 256)`).Scan(&deleted); err != nil || deleted != strings.Count(testCase.prunedRows, ",")+1 {
				t.Fatalf("prune during the End group = %d/%v", deleted, err)
			}
			close(response.release)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("stream did not close")
			}
			if got := strings.Join(streamedEventIDs(t, response.Body.String()), ","); got != testCase.wantIDs {
				t.Fatalf("streamed events = %s; want %s", got, testCase.wantIDs)
			}
			gapLogged := strings.Contains(logs.String(), `"reason":"retained_history_gap"`)
			if gapLogged != testCase.wantGapLog {
				t.Fatalf("gap log = %t; want %t: %s", gapLogged, testCase.wantGapLog, logs.String())
			}
			if testCase.wantGapLog {
				for _, want := range []string{`"msg":"event_stream.feed_closed"`, `"session.id":"sesn_preview"`, `"workspace.id":"default"`, `"outcome":"closed"`} {
					if !strings.Contains(logs.String(), want) {
						t.Fatalf("gap log missing %s: %s", want, logs.String())
					}
				}
				if strings.Contains(logs.String(), string(marker)) || strings.Contains(logs.String(), "stop_reason") || strings.Contains(logs.String(), `"thread.id"`) {
					t.Fatalf("Session-feed gap log carried event content or an empty Thread: %s", logs.String())
				}
			}
		})
	}
}

func streamedEventIDs(t *testing.T, body string) []string {
	t.Helper()
	var ids []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatalf("decode streamed event %q: %v", line, err)
		}
		ids = append(ids, event.ID)
	}
	return ids
}
