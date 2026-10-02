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
