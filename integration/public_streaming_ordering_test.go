package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestPostgreSQLPublicStreamingOrdering(t *testing.T) {
	for _, bound := range []string{"bytes", "count"} {
		t.Run("one-viewer-queue-"+bound+"-overflow", func(t *testing.T) { runPublicViewerOverflow(t, bound) })
	}
	t.Run("event-identity-bound", runPublicIdentityBound)
	for _, mode := range []string{"drop-open", "drop-start", "drop-middle", "drop-tail", "duplicate-start", "duplicate-delta", "reorder", "unknown-event", "scope-mismatch", "oversized-frame"} {
		t.Run(mode, func(t *testing.T) {
			first := ""
			var held []byte
			transform := func(frame eventwire.PreviewFrame, _ string, data []byte) [][]byte {
				if mode == "drop-open" && frame.Kind == "request_open" {
					return nil
				}
				if frame.Kind == "event_start" && frame.EventType == "agent.message" && first == "" {
					first = frame.EventID
				}
				if frame.EventID != first || first == "" {
					return [][]byte{data}
				}
				if frame.Kind == "event_start" {
					switch mode {
					case "drop-start":
						return nil
					case "duplicate-start":
						return [][]byte{data, data}
					}
				}
				if frame.Kind != "event_delta" || frame.PreviewSequence == nil {
					return [][]byte{data}
				}
				sequence := *frame.PreviewSequence
				switch mode {
				case "drop-middle":
					if sequence == 2 {
						return nil
					}
				case "drop-tail":
					if sequence == 3 {
						return nil
					}
				case "duplicate-delta":
					if sequence == 1 {
						return [][]byte{data, data}
					}
				case "reorder":
					if sequence == 1 {
						held = append([]byte(nil), data...)
						return nil
					}
					if sequence == 2 {
						return [][]byte{data, held}
					}
				case "unknown-event":
					if sequence == 1 {
						frame.EventID = "evt_99999999999999999999999999999999"
						body, _ := json.Marshal(frame)
						return [][]byte{body}
					}
				case "scope-mismatch":
					if sequence == 1 {
						frame.WorkspaceID = "other-workspace"
						body, _ := json.Marshal(frame)
						return [][]byte{body}
					}
				case "oversized-frame":
					if sequence == 1 {
						text := strings.Repeat("x", eventwire.MaxPreviewFrameBytes)
						frame.Text = &text
						body, _ := json.Marshal(frame)
						return [][]byte{body}
					}
				}
				return [][]byte{data}
			}
			h := newPublicStreamingHarness(t, "public-text", publicStreamingOptions{transform: transform})
			h.open(t, "fault", []string{"agent.message"}, "")
			h.send(t)
			h.waitFragments(t)
			h.releaseFragments(t, 1)
			publicWait(t, "first real broker delta", func() bool { return publicTappedDelta(h, "alpha ") })
			if mode == "drop-middle" || mode == "drop-tail" || mode == "duplicate-delta" {
				h.waitEvent(t, "fault", "event_delta", 1)
			}
			h.releaseFragments(t, 3)
			publicWait(t, "drained real broker batch through final text fragment", func() bool { return publicTappedDelta(h, "second\n") })
			publicWait(t, "controlled frame batch drained by the viewer", func() bool {
				return h.metric(t, "event_stream_preview_pending_bytes") == 0
			})
			h.finish(t)
			snapshot := h.waitEvent(t, "fault", "span.model_request_end", 1)
			h.assertFormal(t, snapshot, []string{"alpha βeta omega\n", "second\n"})
			assertPublicPreviewShapes(t, snapshot, []string{"agent.message"}, nil)
			var firstID string
			for _, frame := range h.tap.snapshot() {
				if frame.Kind == "event_start" && frame.EventType == "agent.message" {
					firstID = frame.EventID
					break
				}
			}
			got := publicPreviewText(snapshot, firstID)
			want := ""
			switch mode {
			case "drop-middle", "duplicate-delta":
				want = "alpha "
			case "drop-tail":
				want = "alpha βeta "
			}
			if got != want {
				t.Fatalf("controlled %s preview=%q; want contiguous prefix %q", mode, got, want)
			}
			if mode == "drop-open" && countPublicEvents(snapshot, "event_start") != 0 {
				t.Fatal("missing admission invented a public start")
			}
			if mode == "oversized-frame" && h.metric(t, "event_stream_preview_hub_drops_total") == 0 {
				t.Fatal("oversized actual ingress did not hit declared bound")
			}
			publicLogAssertion(t, "loss-ordering-"+mode+"-formal-reconciliation")
		})
	}
	t.Run("split-surrogate-single-event", func(t *testing.T) {
		h := newPublicStreamingHarness(t, "public-unicode-single", publicStreamingOptions{})
		h.open(t, "single", []string{"agent.message"}, "")
		h.send(t)
		h.waitFragments(t)
		h.releaseFragments(t, 1)
		h.waitFragments(t)
		// The high surrogate has crossed the provider boundary. Reading process
		// observations yields to the actual publisher before the low half arrives.
		h.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
		h.releaseFragments(t, 1)
		waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 1)
		h.finish(t)
		snapshot := h.waitEvent(t, "single", "span.model_request_end", 1)
		h.assertFormal(t, snapshot, []string{"😀"})
		assertPublicPreviewShapes(t, snapshot, []string{"agent.message"}, nil)
		publicLogAssertion(t, "split-surrogate-single-event-unicode-prefix-or-stop")
	})
}

func runPublicViewerOverflow(t *testing.T, bound string) {
	barrier := newPublicTextWriteBarrier(t)
	barrier.eventName = "event_delta"
	f := newPublicProjectionFixture(t, publicProjectionOptions{wrapWriter: barrier.wrap, config: func(c *eventstream.StreamConfig) {
		if bound == "bytes" {
			c.ViewerMaxBytes = 2048
		} else {
			c.ViewerMaxFrames = 3
		}
	}})
	t.Cleanup(barrier.unblock)
	f.open(t, "overflow", []string{"agent.message"}, "")
	f.open(t, "healthy", []string{"agent.message"}, "")
	r := f.start(t, "", "")
	fragment := "fragment "
	if bound == "bytes" {
		fragment = strings.Repeat("b", 400)
	}
	final := strings.Repeat(fragment, 8)
	message := f.text(t, r, final, false)
	f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", message, "agent.message", 0, ""))
	f.waitEvent(t, "overflow", "event_start", 1)
	f.waitEvent(t, "healthy", "event_start", 1)
	barrier.armed.Store(true)
	f.publish(t, f.frame(r, "event_delta", message, "agent.message", 1, fragment))
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("slow viewer's actual preview write not held")
	}
	f.waitEvent(t, "healthy", "event_delta", 1)
	for sequence := int64(2); sequence <= 8; sequence++ {
		f.publish(t, f.frame(r, "event_delta", message, "agent.message", sequence, fragment))
		f.waitEvent(t, "healthy", "event_delta", int(sequence))
	}
	if f.metric(t, "event_stream_preview_viewer_drops_total") == 0 {
		t.Fatal("known viewer overflow did not reach configured bound")
	}
	f.end(t, r)
	healthy := f.waitEvent(t, "healthy", "span.model_request_end", 1)
	f.assertFormal(t, healthy, []string{final})
	if publicPreviewText(healthy, message) != final {
		t.Fatal("one slow viewer stopped healthy preview")
	}
	barrier.unblock()
	stopped := f.waitEvent(t, "overflow", "span.model_request_end", 1)
	f.assertFormal(t, stopped, []string{final})
	if publicPreviewText(stopped, message) != fragment {
		t.Fatal("known viewer overflow emitted a suffix after its held prefix")
	}
	publicLogAssertion(t, "independent-viewer-"+bound+"-overflow-retains-prefix-and-healthy-full-preview")
}

func runPublicIdentityBound(t *testing.T) {
	f := newPublicProjectionFixture(t, publicProjectionOptions{})
	f.open(t, "identities", []string{"agent.message"}, "")
	r := f.start(t, "", "")
	message := f.text(t, r, "alpha βeta omega\n", false)
	f.publish(t, f.frame(r, "request_open", "", "", 0, ""), f.frame(r, "event_start", message, "agent.message", 0, ""))
	f.waitEvent(t, "identities", "event_start", 1)
	for i := 1; i < 4096; i++ {
		event := fmt.Sprintf("evt_%032x", i)
		f.publish(t, f.frame(r, "event_start", event, "agent.message", 0, ""))
		f.client.control(t, "wait_count", map[string]any{"viewer": "identities", "eventType": "event_start", "count": i + 1})
	}
	f.publish(t, f.frame(r, "event_start", fmt.Sprintf("evt_%032x", 4096), "agent.message", 0, ""), f.frame(r, "event_delta", message, "agent.message", 1, "alpha "))
	f.fence(t, "identities", r)
	f.end(t, r)
	result := f.waitEvent(t, "identities", "span.model_request_end", 1)
	f.assertFormal(t, result, []string{"alpha βeta omega\n"})
	if countPublicEvents(result, "event_start") != 4096 || countPublicEvents(result, "event_delta") != 0 {
		t.Fatal("event identity capacity failed exact-bound/one-over prefix stop")
	}
	publicLogAssertion(t, "actual-native-SDK-4096-event-identities-and-one-over-stop")
}
func publicTappedDelta(h *publicStreamingHarness, text string) bool {
	for _, frame := range h.tap.snapshot() {
		if frame.Kind == "event_delta" && frame.Text != nil && *frame.Text == text {
			return true
		}
	}
	return false
}
func publicPreviewText(snapshot publicSDKSnapshot, id string) string {
	var result strings.Builder
	for _, event := range snapshot.Events {
		if publicEventType(event) != "event_delta" || event["event_id"] != id {
			continue
		}
		delta, _ := event["delta"].(map[string]any)
		content, _ := delta["content"].(map[string]any)
		text, _ := content["text"].(string)
		result.WriteString(text)
	}
	return result.String()
}
