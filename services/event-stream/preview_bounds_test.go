package eventstream

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The producer fixture pads literal ASCII JSON to independently chosen encoded
// lengths. It deliberately leaves '<' unescaped, as a JavaScript producer can;
// the independently marshaled public wrapper therefore includes Go HTML escape
// expansion as well as the complete SDK envelope.
func paddedPreview(t *testing.T, session string, size int, html bool) ([]byte, int) {
	t.Helper()
	text := ""
	if html {
		text = strings.Repeat("<", 300)
	}
	prefix := `{"version":1,"workspace_id":"default","session_id":"` + session + `","thread_id":"thr_main","model_request_id":"mreq_preview","model_request_start_event_id":"evt_start","request_kind":"agent_provider_request","kind":"event_delta","event_type":"agent.message","event_id":"evt_message","preview_sequence":1,"text":"`
	if padding := size - len(prefix) - len(text) - 2; padding >= 0 {
		text += strings.Repeat("x", padding)
	} else {
		t.Fatalf("fixture frame too small: %d", size)
	}
	raw := []byte(prefix + text + `"}`)
	public, err := json.Marshal(map[string]any{"type": "event_delta", "event_id": "evt_message", "delta": map[string]any{"type": "content_delta", "index": 0, "content": map[string]any{"type": "text", "text": text}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != size {
		t.Fatal("independent producer padding failed")
	}
	if _, err := eventwire.DecodePreviewFrame(eventwire.PreviewSubject("default", session), raw); err != nil {
		t.Fatal(err)
	}
	return raw, len(public)
}

func publishRaw(t *testing.T, transport *fixtureTransport, session string, raw []byte) {
	t.Helper()
	subject := eventwire.PreviewSubject("default", session)
	transport.mu.Lock()
	var subscriptions []*fixtureSubscription
	for _, sub := range transport.subscriptions {
		if !sub.closed && sub.subject == subject {
			subscriptions = append(subscriptions, sub)
		}
	}
	transport.mu.Unlock()
	if len(subscriptions) != 1 {
		t.Fatalf("fixture subscription count=%d", len(subscriptions))
	}
	subscriptions[0].frame(subject, raw)
}

func boundsHub(t *testing.T, config StreamConfig) (*PreviewHub, *fixtureTransport) {
	t.Helper()
	transport := &fixtureTransport{}
	hub, err := NewPreviewHub(transport, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		hub.Close()
		hub.mu.Lock()
		defer hub.mu.Unlock()
		if hub.pendingBytes != 0 || hub.viewerBytes != 0 || hub.pendingFrames != 0 || hub.viewerFrames != 0 || hub.metrics.hubPendingBytes.Load() != 0 || hub.metrics.viewers.Load() != 0 || hub.metrics.subscriptions.Load() != 0 {
			t.Error("joined hub retained byte/count/subscription ownership")
		}
	})
	return hub, transport
}
func boundsViewer(t *testing.T, hub *PreviewHub, session string) *PreviewViewer {
	t.Helper()
	v, err := hub.Join(t.Context(), workspace.DefaultID, session)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func settledViewer(t *testing.T, v *PreviewViewer) {
	t.Helper()
	waitCondition(t, func() bool {
		v.hub.mu.Lock()
		defer v.hub.mu.Unlock()
		return v.hub.pendingBytes == 0 && (len(v.queue) > 0 || v.epoch > 0)
	})
}
func assertViewer(t *testing.T, v *PreviewViewer, bytes, frames int, lost bool) {
	t.Helper()
	v.hub.mu.Lock()
	defer v.hub.mu.Unlock()
	if v.pendingBytes != bytes || len(v.queue) != frames || (v.epoch > 0) != lost {
		t.Fatalf("viewer bytes=%d frames=%d epoch=%d; want %d/%d/lost=%v", v.pendingBytes, len(v.queue), v.epoch, bytes, frames, lost)
	}
}

func TestPreviewHubExactIngressAndViewerQueuedByteBounds(t *testing.T) {
	for _, boundary := range []string{"ingress", "viewer"} {
		for _, offset := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/%+d", boundary, offset), func(t *testing.T) {
				config := DefaultStreamConfig()
				config.ViewerMaxBytes = 4096
				if boundary == "ingress" {
					config.HubMaxBytes = 4096
				}
				hub, tr := boundsHub(t, config)
				v := boundsViewer(t, hub, "sesn_preview")
				if boundary == "ingress" {
					hub.cancel()
					<-hub.done
				}
				raw, _ := paddedPreview(t, "sesn_preview", 4096+offset, false)
				publishRaw(t, tr, "sesn_preview", raw)
				if boundary == "ingress" {
					hub.mu.Lock()
					bytes, frames := hub.pendingBytes, hub.pendingFrames
					hub.mu.Unlock()
					if offset <= 0 {
						if bytes != len(raw) || frames != 1 {
							t.Fatalf("accepted ingress ownership=%d/%d", bytes, frames)
						}
					} else {
						if bytes != 0 || frames != 0 {
							t.Fatal("rejected ingress remains charged")
						}
						assertViewer(t, v, 0, 0, true)
					}
				} else {
					settledViewer(t, v)
					if offset <= 0 {
						assertViewer(t, v, len(raw), 1, false)
					} else {
						assertViewer(t, v, 0, 0, true)
					}
				}
			})
		}
	}
}

func TestPreviewHubExactFanoutByteBounds(t *testing.T) {
	for _, offset := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("%+d", offset), func(t *testing.T) {
			config := DefaultStreamConfig()
			config.HubMaxBytes = 4096
			config.ViewerMaxBytes = 4096
			hub, tr := boundsHub(t, config)
			base := boundsViewer(t, hub, "sesn_base")
			target := boundsViewer(t, hub, "sesn_preview")
			sum := 0
			for _, size := range []int{1024, 1024 + offset} {
				sum += size
				raw, _ := paddedPreview(t, "sesn_base", size, false)
				publishRaw(t, tr, "sesn_base", raw)
				waitCondition(t, func() bool {
					hub.mu.Lock()
					defer hub.mu.Unlock()
					return hub.pendingBytes == 0 && base.pendingBytes == sum
				})
			}
			assertViewer(t, base, 2048+offset, 2, false)
			raw, _ := paddedPreview(t, "sesn_preview", 1024, false)
			publishRaw(t, tr, "sesn_preview", raw)
			settledViewer(t, target)
			if offset <= 0 {
				assertViewer(t, target, 1024, 1, false)
			} else {
				assertViewer(t, target, 0, 0, true)
			}
			assertViewer(t, base, 2048+offset, 2, false)
			takeFrame(t, base)
			takeFrame(t, base)
		})
	}
}

func TestPreviewHubIngressCountRemainsChargedDuringFanout(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			config := DefaultStreamConfig()
			config.HubMaxFrames = limit
			config.ViewerMaxFrames = 1
			hub, tr := boundsHub(t, config)
			a := boundsViewer(t, hub, "sesn_preview")
			var b *PreviewViewer
			if limit > 1 {
				b = boundsViewer(t, hub, "sesn_preview")
			}
			tr.publish(t, fixtureFrame("request_open", "", 0, ""))
			settledViewer(t, a)
			if limit < 3 {
				assertViewer(t, a, 0, 0, true)
				if b != nil {
					assertViewer(t, b, 0, 0, true)
				}
			} else {
				hub.mu.Lock()
				if hub.viewerFrames != 2 || hub.pendingFrames != 0 {
					t.Error("healthy fanout count wrong")
				}
				hub.mu.Unlock()
				takeFrame(t, a)
				takeFrame(t, b)
			}
		})
	}
}

func TestPreviewViewerExactQueuedInflightEncodingByteBounds(t *testing.T) {
	for _, offset := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("%+d", offset), func(t *testing.T) {
			config := DefaultStreamConfig()
			config.ViewerMaxBytes = 4096
			hub, tr := boundsHub(t, config)
			slow := boundsViewer(t, hub, "sesn_preview")
			healthy := boundsViewer(t, hub, "sesn_preview")
			raw, publicSize := paddedPreview(t, "sesn_preview", 1024, true)
			publishRaw(t, tr, "sesn_preview", raw)
			settledViewer(t, slow)
			if _, _, _, ok := slow.Take(); !ok {
				t.Fatal("current write not held")
			}
			takeFrame(t, healthy)
			queuedSize := 4096 - 1024 - publicSize + offset
			queued, _ := paddedPreview(t, "sesn_preview", queuedSize, false)
			publishRaw(t, tr, "sesn_preview", queued)
			settledViewer(t, slow)
			takeFrame(t, healthy)
			if accepted := slow.reserveEncoding(publicSize); accepted != (offset <= 0) {
				t.Fatalf("total %d encoding accepted=%v", 4096+offset, accepted)
			}
			if offset <= 0 {
				assertViewer(t, slow, queuedSize, 1, false)
			} else {
				assertViewer(t, slow, 0, 0, true)
			}
			assertViewer(t, healthy, 0, 0, false)
			hub.mu.Lock()
			held := slow.inFlightBytes
			charged := hub.viewerBytes
			hub.mu.Unlock()
			want := 1024
			if offset <= 0 {
				want = 4096 + offset
			}
			if held != 1024 || charged != want {
				t.Fatalf("held=%d actual charged=%d want=%d", held, charged, want)
			}
			slow.Release()
			slow.Close()
			publishRaw(t, tr, "sesn_preview", raw)
			if got := takeFrame(t, healthy); got.EventID != "evt_message" {
				t.Fatal("healthy viewer cannot continue after local overflow")
			}
		})
	}
}

func TestPreviewHubExactAggregateEncodingByteBounds(t *testing.T) {
	for _, offset := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("%+d", offset), func(t *testing.T) {
			config := DefaultStreamConfig()
			config.HubMaxBytes = 4096
			config.ViewerMaxBytes = 4096
			hub, tr := boundsHub(t, config)
			base := boundsViewer(t, hub, "sesn_base")
			target := boundsViewer(t, hub, "sesn_preview")
			raw, publicSize := paddedPreview(t, "sesn_preview", 1024, true)
			baseSize := 4096 - 1024 - publicSize + offset
			other, _ := paddedPreview(t, "sesn_base", baseSize, false)
			publishRaw(t, tr, "sesn_base", other)
			settledViewer(t, base)
			publishRaw(t, tr, "sesn_preview", raw)
			settledViewer(t, target)
			if _, _, _, ok := target.Take(); !ok {
				t.Fatal("target not held")
			}
			if accepted := target.reserveEncoding(publicSize); accepted != (offset <= 0) {
				t.Fatalf("aggregate total=%d encoding accepted=%v", 4096+offset, accepted)
			}
			assertViewer(t, base, baseSize, 1, false)
			want := baseSize + 1024
			if offset <= 0 {
				want = 4096 + offset
			}
			if got := hub.metrics.hubPendingBytes.Load(); got != int64(want) {
				t.Fatalf("aggregate charged=%d want=%d", got, want)
			}
			target.Release()
			target.Close()
			takeFrame(t, base)
			publishRaw(t, tr, "sesn_base", other)
			takeFrame(t, base)
		})
	}
}

func TestPreviewViewerCountIncludesCurrentWrite(t *testing.T) {
	config := DefaultStreamConfig()
	config.ViewerMaxFrames = 2
	hub, tr := boundsHub(t, config)
	slow := boundsViewer(t, hub, "sesn_preview")
	healthy := boundsViewer(t, hub, "sesn_preview")
	tr.publish(t, fixtureFrame("event_start", "evt_current", 0, ""))
	settledViewer(t, slow)
	if _, _, _, ok := slow.Take(); !ok {
		t.Fatal("current write missing")
	}
	takeFrame(t, healthy)
	tr.publish(t, fixtureFrame("event_start", "evt_queued", 0, ""))
	settledViewer(t, slow)
	takeFrame(t, healthy)
	hub.mu.Lock()
	at := len(slow.queue) + boolCount(slow.inFlightBytes > 0)
	hub.mu.Unlock()
	if at != 2 {
		t.Fatal("at-bound queued/current count wrong")
	}
	tr.publish(t, fixtureFrame("event_start", "evt_over", 0, ""))
	takeFrame(t, healthy)
	assertViewer(t, slow, 0, 0, true)
	assertViewer(t, healthy, 0, 0, false)
	hub.mu.Lock()
	remaining := hub.viewerFrames
	hub.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("held current count released early=%d", remaining)
	}
	slow.Release()
}
