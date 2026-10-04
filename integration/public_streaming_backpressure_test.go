package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
)

func TestPostgreSQLPublicStreamingBackpressure(t *testing.T) {
	publicIsolatedProcessCase(t, runPublicStreamingBackpressure)
}
func runPublicStreamingBackpressure(t *testing.T) {
	t.Run("preview-flood-formal-cancel-delete", runPublicFloodCancellation)
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprintf("socket-deadline/injected=%t", injected), func(t *testing.T) {
			env := map[string]string{}
			if injected {
				env["TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS"] = "250"
			}
			h, g := newPublicStreamingProcesses(t, "public-load", 1, env)
			h.open(t, "healthy", []string{"agent.message"}, "")
			slow := publicUnreadSocket(t, h, g)
			before := g.processes[0].metric(t, "event_stream_slow_writers_total")
			h.send(t)
			h.waitFragments(t)
			for i := 0; i < 32; i++ {
				h.releaseFragments(t, 1)
				h.waitEvent(t, "healthy", "event_delta", i+1)
			}
			publicWait(t, "healthy reader receives heartbeat during held request", func() bool { return h.snapshot(t, "healthy").Heartbeats > 0 })
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 1)
			h.finish(t)
			h.assertFormal(t, h.waitEvent(t, "healthy", "span.model_request_end", 1), []string{strings.Repeat("x", 32*96*1024)})
			// This is a real process-owned TCP write timeout, not a synthetic
			// ResponseWriter error. The peer has consumed only response headers.
			publicWait(t, "actual blocked socket write expires", func() bool { return g.processes[0].metric(t, "event_stream_slow_writers_total") > before })
			_ = slow.Close()
			h.client.control(t, "close_viewer", map[string]any{"viewer": "healthy"})
			publicAssertProcessBaseline(t, g.processes[0])
			publicLogAssertion(t, "real-unread-TCP-default-or-injected-write-deadline-and-healthy-SDK")
		})
	}
	t.Run("twenty-cycles-and-active-writer-SIGTERM", func(t *testing.T) {
		h, g := newPublicStreamingProcesses(t, "public-cycle", 1, nil)
		publicProcessRSS(t, g.processes[0], "before-cycles")
		for cycle := 0; cycle < 20; cycle++ {
			viewer := fmt.Sprintf("cycle-%d", cycle)
			h.open(t, viewer, []string{"agent.message"}, "")
			h.gateway.control(t, map[string]any{"kind": "configure_scenarios", "sessionId": h.session, "scenarios": []string{"public-cycle"}}, "configured_scenarios")
			h.send(t)
			h.waitFragments(t)
			for fragment := 0; fragment < 32; fragment++ {
				h.releaseFragments(t, 1)
				h.waitEvent(t, viewer, "event_delta", fragment+1)
			}
			h.finish(t)
			h.assertFormal(t, h.waitEvent(t, viewer, "span.model_request_end", 1), []string{strings.Repeat("cycle ", 32)})
			h.client.control(t, "close_viewer", map[string]any{"viewer": viewer})
			publicAssertProcessBaseline(t, g.processes[0])
			publicWait(t, "broker cycle unsubscribe", func() bool { return len(publicBrokerSubscriptions(t, h.broker, h.session)) == 0 })
			if (cycle+1)%5 == 0 {
				publicProcessRSS(t, g.processes[0], fmt.Sprintf("after-%d-cycles", cycle+1))
			}
		}
		h.open(t, "active-at-termination", []string{"agent.message"}, "")
		h.gateway.control(t, map[string]any{"kind": "configure_scenarios", "sessionId": h.session, "scenarios": []string{"public-load"}}, "configured_scenarios")
		slow := publicUnreadSocket(t, h, g)
		h.send(t)
		h.waitFragments(t)
		h.releaseFragments(t, 32)
		waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 21)
		h.finish(t)
		h.waitEvent(t, "active-at-termination", "span.model_request_end", 1)
		g.processes[0].stop(t)
		_ = slow.Close()
		publicWait(t, "SIGTERM removes broker subscriptions", func() bool { return len(publicBrokerSubscriptions(t, h.broker, h.session)) == 0 })
		publicLogAssertion(t, "twenty-request-32-fragment-cycles-and-actual-process-SIGTERM-join")
	})
}

func runPublicFloodCancellation(t *testing.T) {
	f := newPublicProjectionFixture(t, publicProjectionOptions{})
	f.open(t, "cancel-preview", []string{"agent.message"}, "")
	f.open(t, "ordinary", nil, "")
	f.open(t, "thread", nil, f.thread(t))
	r := f.start(t, "", "")
	final := strings.Repeat("x", 65536)
	message := f.text(t, r, final, false)
	f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", message, "agent.message", 0, ""))
	f.waitEvent(t, "cancel-preview", "event_start", 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var published atomic.Int64
	go func() {
		var sequence int64 = 1
		cadence := time.NewTicker(32 * time.Millisecond)
		defer cadence.Stop()
		for batch := 0; batch < 1024; batch++ {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-cadence.C:
			}
			for i := 0; i < 64; i++ {
				frame := f.frame(r, "event_delta", message, "agent.message", sequence, "x")
				sequence++
				body, err := json.Marshal(frame)
				if err == nil {
					err = f.publisher.Publish(eventwire.PreviewSubject(frame.WorkspaceID, frame.SessionID), body)
				}
				if err != nil {
					done <- err
					return
				}
			}
			if err := f.publisher.FlushTimeout(time.Second); err != nil {
				done <- err
				return
			}
			published.Add(64)
		}
		done <- nil
	}()
	joined := false
	join := func() {
		if joined {
			return
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("bounded native flood failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("native flood publisher failed to join")
		}
		joined = true
	}
	t.Cleanup(join)
	publicWait(t, "native flood has reached actual broker", func() bool { return published.Load() >= 64 })
	f.thinking(t, r)
	f.end(t, r)
	for _, viewer := range []string{"ordinary", "thread"} {
		f.assertFormal(t, f.waitEvent(t, viewer, "span.model_request_end", 1), []string{final})
	}
	f.client.control(t, "close_viewer", map[string]any{"viewer": "cancel-preview"})
	publicWait(t, "preview cancellation cleans native ownership while ingress continues", func() bool {
		return f.metric(t, "event_stream_preview_viewers") == 0 && f.metric(t, "event_stream_preview_subscriptions") == 0 && f.metric(t, "event_stream_preview_pending_bytes") == 0 && f.metric(t, "event_stream_preview_active_requests") == 0
	})
	publicWait(t, "ordinary heartbeat survives preview ingress", func() bool { return f.snapshot(t, "ordinary").Heartbeats > 0 })
	beforeDelete := published.Load()
	status, _ := f.raw(t, http.MethodDelete, "/v1/sessions/"+f.session+"?beta=true", "")
	if status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("actual Session delete status=%d", status)
	}
	publicWait(t, "deleted Session closes Session and Thread readers", func() bool { return f.snapshot(t, "ordinary").Ended && f.snapshot(t, "thread").Ended })
	publicWait(t, "all active readers released under flood", func() bool { return f.metric(t, "event_stream_active_sse_viewers") == 0 })
	publicWait(t, "publisher still progresses through reader deletion", func() bool { return published.Load() > beforeDelete })
	join()
	publicLogAssertion(t, "real-native-flood-preserves-formal-heartbeats-and-cancel-delete-cleanup")
}

func publicProcessRSS(t *testing.T, p *publicStreamProcess, label string) {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", p.command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("actual Event Stream memory phase=%s rss_bytes=%d", label, value*1024)
			return
		}
	}
	t.Fatal("actual process RSS observation absent")
}

func publicUnreadSocket(t *testing.T, h *publicStreamingHarness, g *publicStreamProcessGroup) *net.TCPConn {
	t.Helper()
	address := strings.TrimPrefix(g.processes[0].address, "http://")
	remote, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTCP("tcp", nil, remote)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err = connection.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, g.processes[0].address+"/v1/sessions/"+h.session+"/events/stream?beta=true&event_deltas%5B%5D=agent.message", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Reuse only a principal minted by the actual Auth edge for this exact
	// method/path. It stays in fixture memory and never enters evidence output.
	g.mu.Lock()
	request.Header = g.streamHeaders.Clone()
	g.mu.Unlock()
	if request.Header.Get("X-Tetral-Internal-Principal") == "" {
		t.Fatal("actual signed edge principal absent")
	}
	if err = connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = request.Write(connection); err != nil {
		t.Fatal("write authenticated slow request")
	}
	response, err := http.ReadResponse(bufio.NewReaderSize(connection, 1024), request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatal("slow socket failed authenticated stream setup")
	}
	if err = connection.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Intentionally never consume response.Body; connection close owns cleanup.
	return connection
}
