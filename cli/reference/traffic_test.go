package reference

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The traffic log states only what the server can know of how a request
// ended: whether a client that went away is seen depends on whether its body
// was read to the end.
func TestTrafficRecordsHowEachRequestEndedAsTheServerSawIt(t *testing.T) {
	for _, row := range []struct {
		name   string
		method string
		body   string
		// leaves: the client goes while the handler holds the request.
		leaves bool
		handle func(http.ResponseWriter, *http.Request, <-chan struct{})
		want   outcome
		status int
	}{
		{
			name: "an answer to a client that stays", method: http.MethodGet,
			handle: func(w http.ResponseWriter, _ *http.Request, _ <-chan struct{}) { w.WriteHeader(http.StatusNoContent) },
			want:   endedAnswered, status: http.StatusNoContent,
		},
		{
			name: "a PUT whose body was read", method: http.MethodPut, body: "seventeen bytes!!",
			handle: func(w http.ResponseWriter, request *http.Request, _ <-chan struct{}) {
				_, _ = io.ReadAll(request.Body)
			},
			want: endedAnswered, status: http.StatusOK,
		},
		{
			name: "a held GET whose client goes", method: http.MethodGet, leaves: true,
			handle: func(w http.ResponseWriter, request *http.Request, _ <-chan struct{}) {
				<-request.Context().Done()
				w.WriteHeader(http.StatusBadRequest)
			},
			want: endedClientGone, status: http.StatusBadRequest,
		},
		{
			name: "a held PUT whose client goes before its body is read", method: http.MethodPut, body: "seventeen bytes!!", leaves: true,
			handle: func(w http.ResponseWriter, _ *http.Request, release <-chan struct{}) {
				<-release
				w.WriteHeader(http.StatusBadRequest)
			},
			want: endedClientUnknown, status: http.StatusBadRequest,
		},
		{
			name: "a cut", method: http.MethodGet,
			handle: func(w http.ResponseWriter, _ *http.Request, _ <-chan struct{}) { cut(w) },
			want:   endedCut,
		},
		{
			name: "a handler that aborts", method: http.MethodGet,
			handle: func(http.ResponseWriter, *http.Request, <-chan struct{}) { panic(http.ErrAbortHandler) },
			want:   endedCut,
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			log := &traffic{}
			release := make(chan struct{})
			server := httptest.NewServer(log.recorded(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				row.handle(w, request, release)
			})))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var body io.Reader
			if row.body != "" {
				body = strings.NewReader(row.body)
			}
			request, err := http.NewRequestWithContext(ctx, row.method, server.URL+"/held", body)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if response, err := server.Client().Do(request); err == nil {
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
				}
			}()
			if row.leaves {
				waitForTraffic(t, log, func(entry trafficEntry) bool { return true })
				cancel()
				<-done
			}
			close(release)
			entry := waitForTraffic(t, log, func(entry trafficEntry) bool { return entry.Outcome != "" })
			<-done
			if entry.Outcome != row.want || entry.Status != row.status {
				t.Fatalf("ended as %q with status %d, want %q with status %d", entry.Outcome, entry.Status, row.want, row.status)
			}
		})
	}
}

// waitForTraffic waits until the log's one entry meets condition.
func waitForTraffic(t *testing.T, log *traffic, condition func(trafficEntry) bool) trafficEntry {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if entries := log.snapshot(); len(entries) == 1 && condition(entries[0]) {
			return entries[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("the traffic log never got there: %+v", log.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}
