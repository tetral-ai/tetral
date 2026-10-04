package eventstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPreviewProcessShutdownCancelsAndJoinsLiveSSEReads(t *testing.T) {
	for _, mode := range []string{"session", "session_preview", "thread"} {
		t.Run(mode, func(t *testing.T) {
			lifetime, cancel := context.WithCancel(t.Context())
			defer cancel()
			metrics := NewPreviewMetrics()
			transport := &fixtureTransport{}
			hub, err := NewPreviewHub(transport, DefaultStreamConfig(), metrics)
			if err != nil {
				t.Fatal(err)
			}
			defer hub.Close()
			h := &handler{options: newOptions(WithStreamShutdownContext(lifetime), WithPreviewHub(hub), WithPreviewMetrics(metrics))}
			scope := ReadScope{WorkspaceID: workspace.DefaultID, SessionID: "sesn_preview"}
			var types map[string]bool
			if mode == "thread" {
				scope.ThreadID = "thr_public"
			}
			if mode == "session_preview" {
				types = map[string]bool{"agent.message": true}
			}
			entered, queryCanceled, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				h.streamEvents(w, r, scope, types, func(context.Context) (int64, error) { return 0, nil }, func(ctx context.Context, _ int64) ([]StreamChange, error) {
					close(entered)
					<-ctx.Done()
					close(queryCanceled)
					return nil, ctx.Err()
				})
			}))
			defer server.Close()
			response, err := server.Client().Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := response.Body.Close(); err != nil {
					t.Error(err)
				}
			}()
			if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
				t.Fatal("live SSE header barrier absent")
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("actual read did not enter context barrier")
			}
			if metrics.activeStreams.Load() != 1 {
				t.Fatal("live SSE connection not owned")
			}
			cancel()
			select {
			case <-queryCanceled:
			case <-time.After(5 * time.Second):
				t.Fatal("process shutdown did not cancel reader context")
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("process shutdown did not join writer/cancellation observer")
			}
			// Canceling a blocked writer expires its socket deadline; the final
			// HTTP chunk can be absent. Require closed body with no published data.
			if n, err := io.Copy(io.Discard, response.Body); n != 0 || (err != nil && !errors.Is(err, io.ErrUnexpectedEOF)) {
				t.Fatalf("canceled SSE bytes=%d error=%v", n, err)
			}
			samples, err := metrics.Collector()(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, sample := range samples {
				if sample.Type == "gauge" && sample.Value != 0 {
					t.Fatalf("shutdown ownership leaked %s=%g", sample.Name, sample.Value)
				}
			}
		})
	}
}
