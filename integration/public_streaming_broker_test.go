package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

func TestPostgreSQLPublicStreamingBrokerFailure(t *testing.T) {
	t.Run("accepted-batch-disconnect-during-flush", runPublicAcceptedFlushFailure)
	for _, operation := range []string{"release_connect", "fail_connect"} {
		t.Run("pending-native-factory-shutdown/"+operation, func(t *testing.T) { runPublicPendingConnectShutdown(t, operation) })
	}
	for _, mode := range []string{"unavailable-at-start", "disconnect-after-first-delta"} {
		t.Run(mode, func(t *testing.T) {
			broker := publicFaultBroker(t)
			stop := func() { publicBrokerCommand(t, broker, "stop") }
			start := func() { publicBrokerCommand(t, broker, "start") }
			if mode == "unavailable-at-start" {
				stop()
			}
			h := newPublicStreamingHarness(t, "public-text", publicStreamingOptions{broker: &broker, gateway: map[string]any{"holdEveryRequest": true, "followupScenario": "public-text"}})
			opened := time.Now()
			h.open(t, "preview", []string{"agent.message"}, "")
			if mode == "unavailable-at-start" && time.Since(opened) > 3*time.Second {
				t.Fatal("unavailable preview setup blocked formal stream beyond bounded setup")
			}
			h.open(t, "formal", nil, "")
			h.send(t)
			h.waitFragments(t)
			if mode == "disconnect-after-first-delta" {
				h.releaseFragments(t, 1)
				h.waitEvent(t, "preview", "event_delta", 1)
				stop()
				publicWait(t, "actual publisher disconnect", func() bool { return publicGatewayMetric(t, h, "connected") == 0 })
				h.releaseFragments(t, 3)
			} else {
				h.releaseFragments(t, 4)
			}
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 2)
			h.finish(t)
			for _, viewer := range []string{"preview", "formal"} {
				snapshot := h.waitEvent(t, viewer, "span.model_request_end", 1)
				h.assertFormal(t, snapshot, []string{"alpha βeta omega\n", "second\n"})
				if mode == "unavailable-at-start" && countPublicEvents(snapshot, "event_delta") != 0 {
					t.Fatal("startup-unavailable publisher replayed stale fragments")
				}
			}
			assertContentE2EProvider(t, h.contentE2E, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"), 1, 0)
			before := countPublicEvents(h.snapshot(t, "preview"), "event_delta")
			start()
			publicWait(t, "fresh publisher and subscriber recovery", func() bool {
				return publicGatewayMetric(t, h, "connected") == 1 && len(publicBrokerSubscriptions(t, broker, h.session)) == 1
			})
			if publicGatewayMetric(t, h, "pending_bytes") != 0 || publicGatewayMetric(t, h, "pending_frames") != 0 {
				t.Fatal("old publisher queue survived reconnect")
			}
			if countPublicEvents(h.snapshot(t, "preview"), "event_delta") != before {
				t.Fatal("reconnect replayed old queued/inflight previews")
			}
			h.send(t)
			h.waitFragments(t)
			h.releaseFragments(t, 1)
			h.waitEvent(t, "preview", "event_delta", before+1)
			h.releaseFragments(t, 3)
			h.finish(t)
			final := h.waitEvent(t, "preview", "span.model_request_end", 2)
			h.assertFormal(t, final, []string{"alpha βeta omega\n", "second\n", "alpha βeta omega\n", "second\n"})
			assertContentE2EProvider(t, h.contentE2E, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"), 2, 0)
			publicLogAssertion(t, "broker-"+mode+"-formal-independent-new-request-recovery")
		})
	}
	for _, viewers := range []int{0, 1} {
		t.Run("publication-with-viewers-"+strconv.Itoa(viewers), func(t *testing.T) {
			h := newPublicStreamingHarness(t, "public-text", publicStreamingOptions{})
			beforeCPU := publicBrokerCPU(t, h.broker)
			h.open(t, "formal", nil, "")
			if viewers == 1 {
				h.open(t, "preview", []string{"agent.message"}, "")
			}
			h.send(t)
			h.waitFragments(t)
			h.releaseFragments(t, 1)
			if viewers == 1 {
				h.waitEvent(t, "preview", "event_delta", 1)
			}
			h.releaseFragments(t, 3)
			h.finish(t)
			h.assertFormal(t, h.waitEvent(t, "formal", "span.model_request_end", 1), []string{"alpha βeta omega\n", "second\n"})
			if h.metric(t, "event_stream_preview_subscriptions") != float64(viewers) || publicGatewayMetric(t, h, "encoded_frames_total") == 0 || publicGatewayMetric(t, h, "encoded_bytes_total") == 0 {
				t.Fatal("viewer-independent bounded publication contract changed")
			}
			if viewers == 0 && len(h.tap.snapshot()) != 0 {
				t.Fatal("subscriber received previews with no opted-in viewer")
			}
			observation := h.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
			t.Logf("preview workload viewers=%d encoded_frames=%.0f encoded_bytes=%.0f publisher_cpu_micros=%s broker_cpu_ticks=%d elapsed_ms=%s", viewers, publicGatewayMetric(t, h, "encoded_frames_total"), publicGatewayMetric(t, h, "encoded_bytes_total"), observation["cpuMicros"], publicBrokerCPU(t, h.broker)-beforeCPU, observation["measurementElapsedMs"])
			publicLogAssertion(t, "publication-independent-of-viewer-count")
		})
	}
}

type publicPublisherControlState struct {
	ConnectStarted, ConnectHeld, ConnectCompleted, ConnectFailed         int
	ConnectionsCreated, ConnectionsClosed, ConnectionsActive, CloseCalls int
	FlushStarted, FlushServerProcessed, FlushHeld                        int
	ConnectWaiting, FlushWaiting                                         bool
	ShutdownStarted                                                      bool
}

func publicPublisherState(t *testing.T, h *publicStreamingHarness) publicPublisherControlState {
	t.Helper()
	return publicDecodePublisherState(t, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"))
}
func publicDecodePublisherState(t *testing.T, raw map[string]json.RawMessage) publicPublisherControlState {
	t.Helper()
	var state publicPublisherControlState
	if json.Unmarshal(raw["publisherControls"], &state) != nil {
		t.Fatal("actual publisher control observation absent")
	}
	return state
}
func publicPublisherControl(t *testing.T, h *publicStreamingHarness, operation string) {
	t.Helper()
	h.gateway.control(t, map[string]any{"kind": "publisher_control", "operation": operation}, "publisher_controlled")
}
func runPublicAcceptedFlushFailure(t *testing.T) {
	broker := publicFaultBroker(t)
	h := newPublicStreamingHarness(t, "public-text", publicStreamingOptions{broker: &broker, gateway: map[string]any{"holdEveryRequest": true, "followupScenario": "public-text", "previewPublisherControls": map[string]any{}}})
	h.open(t, "preview", []string{"agent.message"}, "")
	h.open(t, "formal", nil, "")
	h.send(t)
	h.waitFragments(t)
	publicWait(t, "initial actual publisher batch flushed", func() bool { return publicGatewayMetric(t, h, "pending_frames") == 0 })
	before := publicPublisherState(t, h).FlushServerProcessed
	publicPublisherControl(t, h, "arm_flush")
	h.releaseFragments(t, 1)
	publicWait(t, "server positively accepted held flush batch", func() bool { s := publicPublisherState(t, h); return s.FlushWaiting && s.FlushServerProcessed > before })
	h.waitEvent(t, "preview", "event_delta", 1)
	publicBrokerCommand(t, h.broker, "stop")
	publicWait(t, "native publisher observes disconnect during held flush", func() bool { return publicGatewayMetric(t, h, "connected") == 0 })
	publicPublisherControl(t, h, "release_flush")
	h.releaseFragments(t, 3)
	h.finish(t)
	for _, viewer := range []string{"preview", "formal"} {
		h.assertFormal(t, h.waitEvent(t, viewer, "span.model_request_end", 1), []string{"alpha βeta omega\n", "second\n"})
	}
	assertContentE2EProvider(t, h.contentE2E, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"), 1, 0)
	old := countPublicEvents(h.snapshot(t, "preview"), "event_delta")
	publicBrokerCommand(t, h.broker, "start")
	publicWait(t, "native publisher and subscriber recover after accepted flush cut", func() bool {
		return publicGatewayMetric(t, h, "connected") == 1 && len(publicBrokerSubscriptions(t, h.broker, h.session)) == 1
	})
	if countPublicEvents(h.snapshot(t, "preview"), "event_delta") != old || publicGatewayMetric(t, h, "pending_frames") != 0 {
		t.Fatal("accepted prior batch replayed after reconnect")
	}
	h.send(t)
	h.waitFragments(t)
	h.releaseFragments(t, 1)
	h.waitEvent(t, "preview", "event_delta", old+1)
	h.releaseFragments(t, 3)
	h.finish(t)
	h.assertFormal(t, h.waitEvent(t, "preview", "span.model_request_end", 2), []string{"alpha βeta omega\n", "second\n", "alpha βeta omega\n", "second\n"})
	assertContentE2EProvider(t, h.contentE2E, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"), 2, 0)
	publicLogAssertion(t, "server-accepted-batch-flush-disconnect-does-not-replay-or-block-formal")
}
func runPublicPendingConnectShutdown(t *testing.T, operation string) {
	h := newPublicStreamingHarness(t, "public-text", publicStreamingOptions{gateway: map[string]any{"previewPublisherControls": map[string]any{"holdInitialConnect": true, "holdAfterNativeConnect": operation == "release_connect"}}})
	publicWait(t, "actual native publisher factory held", func() bool { return publicPublisherState(t, h).ConnectWaiting })
	h.open(t, "pending", []string{"agent.message"}, "")
	h.send(t)
	h.waitFragments(t)
	h.releaseFragments(t, 4)
	h.finish(t)
	result := h.waitEvent(t, "pending", "span.model_request_end", 1)
	h.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
	if countPublicEvents(result, "event_delta") != 0 {
		t.Fatal("held native factory still published preview")
	}
	assertContentE2EProvider(t, h.contentE2E, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"), 1, 0)
	c := h.gateway
	c.controlMu.Lock()
	_, err := c.input.Write([]byte("{\"kind\":\"stop\"}\n"))
	c.controlMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	publicWait(t, "production publisher close entered with pending factory", func() bool { return publicPublisherState(t, h).ShutdownStarted })
	publicPublisherControl(t, h, operation)
	var raw map[string]json.RawMessage
	select {
	case line, ok := <-c.lines:
		if !ok || json.Unmarshal(line, &raw) != nil {
			t.Fatal("Gateway native shutdown response missing")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Gateway supervisor did not join late native factory")
	}
	var kind string
	if json.Unmarshal(raw["kind"], &kind) != nil || kind != "stopped" {
		t.Fatal("Gateway did not complete actual shutdown")
	}
	state := publicDecodePublisherState(t, raw)
	if state.ConnectionsActive != 0 || state.ConnectionsCreated != state.ConnectionsClosed {
		t.Fatal("late native connection escaped Gateway shutdown")
	}
	if operation == "release_connect" && (state.ConnectionsCreated != 1 || state.CloseCalls != 1) {
		t.Fatal("late actual connection was not closed exactly once")
	}
	if operation == "fail_connect" && (state.ConnectFailed != 1 || state.ConnectionsCreated != 0) {
		t.Fatal("failed held factory unexpectedly created/retried transport")
	}
	select {
	case err := <-c.joined:
		c.stopped = true
		if err != nil {
			t.Fatal("Gateway native shutdown child failed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Gateway process did not exit after native shutdown")
	}
	_ = c.input.Close()
	publicLogAssertion(t, "actual-pending-native-factory-"+operation+"-shutdown-joins-and-formal-remains-independent")
}
func publicBrokerCPU(t *testing.T, broker testinfra.NATSFixture) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "docker", "exec", broker.Container, "cat", "/proc/1/stat").Output()
	if err != nil {
		t.Fatal("read actual broker CPU counters")
	}
	_, tail, found := strings.Cut(string(data), ") ")
	fields := strings.Fields(tail)
	if !found || len(fields) < 13 {
		t.Fatal("broker CPU counters malformed")
	}
	var total uint64
	for _, raw := range fields[11:13] {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatal(fmt.Errorf("broker CPU counter: %w", err))
		}
		total += value
	}
	return total
}

// Fault owners must not stop the runner's shared broker: other Full workers
// may be executing independent role or service contracts against it.
func publicFaultBroker(t *testing.T) testinfra.NATSFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	broker, closeBroker, err := testinfra.NewNATSFixture(ctx, transporttest.RepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the harness so LIFO joins clients before broker removal.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := closeBroker(ctx); err != nil {
			t.Errorf("isolated fault broker cleanup: %v", err)
		}
	})
	t.Logf("isolated fault broker container=%s image=%s servers=%v", broker.Container, broker.Image, broker.Servers)
	return broker
}

func publicBrokerCommand(t *testing.T, broker testinfra.NATSFixture, operation string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := []string{operation}
	if operation == "stop" {
		args = append(args, "--time", "1")
	}
	args = append(args, broker.Container)
	command := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // Fixed start/stop operation on the runner-owned fixture.
	if err := command.Run(); err != nil {
		t.Fatalf("broker %s failed", operation)
	}
}
func publicGatewayMetric(t *testing.T, h *publicStreamingHarness, name string) float64 {
	t.Helper()
	observation := h.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	var rendered string
	if json.Unmarshal(observation["previewMetrics"], &rendered) != nil {
		t.Fatal("actual publisher metric observation missing")
	}
	for _, line := range strings.Split(rendered, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "providergateway_preview_"+name {
			value, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("publisher metric missing: %s", name)
	return 0
}
