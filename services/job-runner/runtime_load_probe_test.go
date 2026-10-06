package jobrunner

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
)

func runtimeLoadFixture(active int) string {
	return fmt.Sprintf("runtimepod_active_sessions %d\nruntimepod_session_capacity 8\nruntimepod_container_memory_usage_bytes 20\nruntimepod_container_memory_limit_bytes 100\nruntimepod_ready 1\nruntimepod_accepting_commands 1\n", active)
}

func TestRuntimeLoadAdmissionValidation(t *testing.T) {
	valid := runtimeLoadFixture(0)
	for name, body := range map[string]string{
		"missing":          strings.ReplaceAll(valid, "runtimepod_container_memory_limit_bytes 100\n", ""),
		"duplicate":        valid + "runtimepod_active_sessions 0\n",
		"labelled":         strings.ReplaceAll(valid, "active_sessions 0", "active_sessions{pod=\"x\"} 0"),
		"NaN":              strings.ReplaceAll(valid, "usage_bytes 20", "usage_bytes NaN"),
		"infinite":         strings.ReplaceAll(valid, "limit_bytes 100", "limit_bytes +Inf"),
		"negative":         strings.ReplaceAll(valid, "active_sessions 0", "active_sessions -1"),
		"fractional count": strings.ReplaceAll(valid, "active_sessions 0", "active_sessions .5"),
		"no finite limit":  strings.ReplaceAll(valid, "limit_bytes 100", "limit_bytes 0"),
		"full":             runtimeLoadFixture(8),
		"memory cutoff":    strings.ReplaceAll(valid, "usage_bytes 20", "usage_bytes 80"),
		"draining":         strings.ReplaceAll(valid, "accepting_commands 1", "accepting_commands 0"),
		"not ready":        strings.ReplaceAll(valid, "ready 1", "ready 0"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRuntimeLoadReport(body, .8); err == nil {
				t.Fatal("excluded report admitted a new binding")
			}
		})
	}
	if got, err := ParseRuntimeLoadReport(valid, .8); err != nil || got.ActiveSessions != 0 {
		t.Fatalf("idle healthy Runtime excluded: %+v/%v", got, err)
	}
}

func runtimeLoadTestClient(t *testing.T, handler http.Handler) *http.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}
}

func TestRuntimeLoadProbeWireBounds(t *testing.T) {
	for _, scenario := range []string{"success", "redirect", "oversized", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			var calls int
			var lock sync.Mutex
			client := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				lock.Lock()
				calls++
				lock.Unlock()
				if r.Method != "GET" || r.URL.Path != "/metrics" || r.Host != "10.0.0.1:8080" {
					t.Errorf("wrong native load request: %s %s %s", r.Method, r.URL, r.Host)
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || len(body) != 0 {
					t.Errorf("load probe sent body: %q/%v", body, err)
				}
				switch scenario {
				case "redirect":
					http.Redirect(w, r, "/other", http.StatusFound)
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", 1025))
				case "deadline":
					<-r.Context().Done()
				default:
					_, _ = io.WriteString(w, runtimeLoadFixture(0))
				}
			}))
			policy := DefaultRuntimePlacementPolicy()
			policy.ProbeTimeout = 30 * time.Millisecond
			policy.MaxResponseBytes = 1024
			_, err := probeRuntimeLoad(context.Background(), client, kubernetes.BindingCandidate{PodIP: "10.0.0.1"}, policy)
			if (scenario == "success") != (err == nil) {
				t.Fatalf("probe %s: %v", scenario, err)
			}
			lock.Lock()
			count := calls
			lock.Unlock()
			if count != 1 {
				t.Fatalf("one probe replayed %d HTTP requests", count)
			}
		})
	}
}

// The transport ceiling is tested at its production boundary, independently of
// exposition validity. Closing the actual response body is an owned obligation.
type observedLoadTransport struct {
	delegate http.RoundTripper
	closed   chan struct{}
	reading  chan struct{}
}
type observedLoadBody struct {
	io.ReadCloser
	closed   chan struct{}
	reading  chan struct{}
	readOnce sync.Once
}

func (b *observedLoadBody) Read(p []byte) (int, error) {
	b.readOnce.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(p)
}
func (b *observedLoadBody) Close() error { err := b.ReadCloser.Close(); close(b.closed); return err }
func (tr observedLoadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := tr.delegate.RoundTrip(r)
	if err == nil {
		response.Body = &observedLoadBody{ReadCloser: response.Body, closed: tr.closed, reading: tr.reading}
	}
	return response, err
}
func TestRuntimeLoadProbeProductionBodyBoundary(t *testing.T) {
	for _, scenario := range []string{"exact limit", "one excess byte", "slow body", "caller cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			body := runtimeLoadFixture(0)
			for len(body) < 256*1024 {
				n := min(500, 256*1024-len(body))
				if n == 1 {
					body += "\n"
				} else {
					body += "#" + strings.Repeat(" ", n-2) + "\n"
				}
			}
			if scenario == "one excess byte" {
				body += "\n"
			}
			cancelled := make(chan struct{})
			client := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario == "slow body" || scenario == "caller cancellation" {
					_, _ = io.WriteString(w, "# partial\n")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					close(cancelled)
					return
				}
				_, _ = io.WriteString(w, body)
			}))
			closed := make(chan struct{})
			reading := make(chan struct{})
			client.Transport = observedLoadTransport{delegate: client.Transport, closed: closed, reading: reading}
			policy := DefaultRuntimePlacementPolicy()
			// Size acceptance and caller cancellation use the actual production
			// policy. Only the deliberately unfinished body accelerates the
			// timeout fault; a 100ms completion premise is not a byte-limit rule.
			if scenario == "slow body" {
				policy.ProbeTimeout = 100 * time.Millisecond
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := probeRuntimeLoad(ctx, client, kubernetes.BindingCandidate{PodIP: "10.0.0.1"}, policy)
				done <- err
			}()
			if scenario == "caller cancellation" {
				// A server flush does not prove client ownership: cancelling before
				// RoundTrip returns can prevent any response body reaching the probe.
				// Cancel only once the production reader owns and starts that body.
				select {
				case <-reading:
				case <-time.After(3 * time.Second):
					t.Fatal("body read did not begin")
				}
				cancel()
			}
			select {
			case err := <-done:
				if scenario == "exact limit" {
					if err != nil {
						t.Fatalf("semantically valid256KiB body rejected: %v", err)
					}
				} else if err == nil {
					t.Fatal("unbounded/oversized body admitted")
				}
				if scenario == "one excess byte" && runtimeLoadFailureReason(err) != "response_too_large" {
					t.Fatalf("size reason=%v", err)
				}
				if scenario == "slow body" && runtimeLoadFailureReason(err) != "timeout" {
					t.Fatalf("slow body reason=%v", err)
				}
				if scenario == "slow body" {
					select {
					case <-reading:
					default:
						t.Fatal("timeout occurred before the probe owned its body read")
					}
				}
				if scenario == "caller cancellation" && runtimeLoadFailureReason(err) != "cancelled" {
					t.Fatalf("parent cancellation=%v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("load body reader failed to join")
			}
			select {
			case <-closed:
			case <-time.After(3 * time.Second):
				t.Fatal("actual HTTP response reader not closed")
			}
			if scenario == "slow body" || scenario == "caller cancellation" {
				select {
				case <-cancelled:
				case <-time.After(3 * time.Second):
					t.Fatal("actual server request not cancelled")
				}
			}
		})
	}
}
