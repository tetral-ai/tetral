package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type fixtureTransport struct {
	mu                         sync.Mutex
	subscriptions              []*fixtureSubscription
	setupErr                   error
	closeEntered, closeRelease chan struct{}
}
type fixtureSubscription struct {
	owner   *fixtureTransport
	subject string
	frame   func(string, []byte)
	loss    func(string)
	closed  bool
}

func (f *fixtureTransport) Subscribe(_ context.Context, subject string, frame func(string, []byte), loss func(string)) (PreviewSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &fixtureSubscription{owner: f, subject: subject, frame: frame, loss: loss}
	f.subscriptions = append(f.subscriptions, s)
	return s, f.setupErr
}
func (s *fixtureSubscription) Close() error {
	f := s.owner
	f.mu.Lock()
	s.closed = true
	entered, release := f.closeEntered, f.closeRelease
	f.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	return nil
}
func (f *fixtureTransport) publish(t *testing.T, frame eventwire.PreviewFrame) {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	subscriptions := append([]*fixtureSubscription(nil), f.subscriptions...)
	f.mu.Unlock()
	for _, s := range subscriptions {
		f.mu.Lock()
		closed := s.closed
		f.mu.Unlock()
		if !closed {
			s.frame(s.subject, data)
		}
	}
}
func fixtureFrame(kind, eventID string, sequence int64, text string) eventwire.PreviewFrame {
	frame := eventwire.PreviewFrame{Version: 1, WorkspaceID: "default", SessionID: "sesn_preview", ThreadID: "thr_main", ModelRequestID: "mreq_preview", ModelRequestStartEventID: "evt_start", RequestKind: "agent_provider_request", Kind: kind}
	if kind != "request_open" {
		frame.EventType = "agent.message"
		frame.EventID = eventID
		frame.PreviewSequence = &sequence
		if kind == "event_delta" {
			frame.Text = &text
		}
	}
	return frame
}
func waitCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("fixture observation timed out")
		case <-ticker.C:
		}
	}
}
func takeFrame(t *testing.T, v *PreviewViewer) eventwire.PreviewFrame {
	t.Helper()
	var frame eventwire.PreviewFrame
	waitCondition(t, func() bool {
		candidate, _, _, ok := v.Take()
		if ok {
			frame = candidate
			return true
		}
		return false
	})
	v.Release()
	return frame
}

func TestPreviewHubFanoutAndLastUnsubscribeJoinRace(t *testing.T) {
	transport := &fixtureTransport{}
	metrics := NewPreviewMetrics()
	hub, err := NewPreviewHub(transport, DefaultStreamConfig(), metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	first, err := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.subscriptions.Load() != 1 || metrics.viewers.Load() != 2 {
		t.Fatal("local viewers did not share subscription")
	}
	transport.publish(t, fixtureFrame("request_open", "", 0, ""))
	if takeFrame(t, first).Kind != "request_open" || takeFrame(t, second).Kind != "request_open" {
		t.Fatal("ordinary fanout failed")
	}
	first.Close()
	first.Close()
	if metrics.viewers.Load() != 1 || metrics.subscriptions.Load() != 1 {
		t.Fatal("idempotent departure removed surviving subscription")
	}
	transport.publish(t, fixtureFrame("event_start", "evt_message", 0, ""))
	if takeFrame(t, second).EventID != "evt_message" {
		t.Fatal("surviving viewer missed frame")
	}
	transport.mu.Lock()
	transport.closeEntered = make(chan struct{}, 1)
	transport.closeRelease = make(chan struct{})
	entered, release := transport.closeEntered, transport.closeRelease
	transport.mu.Unlock()
	closed := make(chan struct{})
	go func() { second.Close(); close(closed) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("teardown barrier not reached")
	}
	joined := make(chan *PreviewViewer, 1)
	go func() { viewer, _ := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview"); joined <- viewer }()
	select {
	case <-joined:
		t.Fatal("join reused a closing subscription")
	default:
	}
	close(release)
	<-closed
	var third *PreviewViewer
	select {
	case third = <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("new join failed after teardown")
	}
	defer third.Close()
	if metrics.subscriptions.Load() != 1 || metrics.viewers.Load() != 1 {
		t.Fatal("subscription replacement unbalanced")
	}
	transport.mu.Lock()
	transport.closeEntered = nil
	transport.closeRelease = nil
	count := len(transport.subscriptions)
	transport.mu.Unlock()
	if count != 2 {
		t.Fatalf("subscriptions installed=%d", count)
	}
	transport.publish(t, fixtureFrame("request_open", "", 0, ""))
	takeFrame(t, third)
}

func TestPreviewHubViewerLossDoesNotStopHealthyViewer(t *testing.T) {
	config := DefaultStreamConfig()
	config.ViewerMaxFrames = 2
	transport := &fixtureTransport{}
	metrics := NewPreviewMetrics()
	hub, err := NewPreviewHub(transport, config, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	slow, _ := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
	healthy, _ := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
	defer slow.Close()
	defer healthy.Close()
	for _, id := range []string{"evt_one", "evt_two", "evt_three"} {
		transport.publish(t, fixtureFrame("event_start", id, 0, ""))
		if takeFrame(t, healthy).EventID != id {
			t.Fatal("healthy viewer missed a frame")
		}
	}
	epoch, reason := slow.Epoch()
	if epoch == 0 || reason != "viewer_overflow" {
		t.Fatalf("slow viewer loss=%d %s", epoch, reason)
	}
	healthyEpoch, _ := healthy.Epoch()
	if healthyEpoch != 0 {
		t.Fatal("healthy viewer incorrectly stopped")
	}
	hub.mu.Lock()
	subscription := transport.subscriptions[0]
	hub.mu.Unlock()
	subscription.loss("nats_disconnect")
	after, _ := healthy.Epoch()
	if after == 0 {
		t.Fatal("subscription-wide loss was not propagated")
	}
	transport.publish(t, fixtureFrame("request_open", "", 0, ""))
	if takeFrame(t, healthy).Kind != "request_open" {
		t.Fatal("future admission cannot recover")
	}
}

func TestPreviewHubCountsQueuedInflightEncodingAndCleanup(t *testing.T) {
	transport := &fixtureTransport{}
	metrics := NewPreviewMetrics()
	config := DefaultStreamConfig()
	hub, err := NewPreviewHub(transport, config, metrics)
	if err != nil {
		t.Fatal(err)
	}
	viewer, _ := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
	frame := fixtureFrame("event_delta", "evt_message", 1, "β")
	encoded, _ := json.Marshal(frame)
	transport.publish(t, frame)
	waitCondition(t, func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return viewer.pendingBytes == len(encoded) && hub.pendingBytes == 0
	})
	if metrics.hubPendingBytes.Load() != int64(len(encoded)) {
		t.Fatal("viewer queue bytes were uncharged")
	}
	_, _, _, ok := viewer.Take()
	if !ok {
		t.Fatal("frame not available")
	}
	if metrics.hubPendingBytes.Load() != int64(len(encoded)) {
		t.Fatal("dequeued write bytes were released early")
	}
	if !viewer.reserveEncoding(137) || metrics.hubPendingBytes.Load() != int64(len(encoded)+137) {
		t.Fatal("encoding storage was uncharged")
	}
	viewer.Release()
	if metrics.hubPendingBytes.Load() != 0 {
		t.Fatal("released encoding/body remains charged")
	}
	transport.publish(t, fixtureFrame("event_start", "evt_later", 0, ""))
	waitCondition(t, func() bool { return metrics.hubPendingBytes.Load() > 0 })
	viewer.Close()
	hub.Close()
	hub.Close()
	if metrics.viewers.Load() != 0 || metrics.subscriptions.Load() != 0 || metrics.hubPendingBytes.Load() != 0 {
		t.Fatal("hub resources were not joined and released")
	}
}

func TestPreviewHubUnavailableSetupKeepsFutureSubscription(t *testing.T) {
	transport := &fixtureTransport{setupErr: errors.New("unavailable")}
	hub, err := NewPreviewHub(transport, DefaultStreamConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	viewer, err := hub.Join(context.Background(), workspace.DefaultID, "sesn_preview")
	if err == nil || viewer == nil {
		t.Fatal("unavailable setup lost desired viewer")
	}
	defer viewer.Close()
	epoch, _ := viewer.Epoch()
	if epoch == 0 {
		t.Fatal("setup degradation not classified")
	}
	transport.publish(t, fixtureFrame("request_open", "", 0, ""))
	if takeFrame(t, viewer).Kind != "request_open" {
		t.Fatal("new requests cannot preview after recovery")
	}
}

// Stop the package-owned dispatcher before injecting transport callbacks. This
// fixture leaves an explicitly paused/drained consumer so queue-removal credit
// accounting is exercised deterministically without sleeps or a production hook.
func TestPreviewHubLossAndLastCloseReturnQueuedFrameCredits(t *testing.T) {
	transport := &fixtureTransport{}
	config := DefaultStreamConfig()
	config.HubMaxFrames = 2
	config.ViewerMaxFrames = 2
	hub, err := NewPreviewHub(transport, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	hub.cancel()
	<-hub.done
	defer hub.Close()
	for iteration := 0; iteration < 4; iteration++ {
		viewer, err := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
		if err != nil {
			t.Fatal(err)
		}
		transport.publish(t, fixtureFrame("event_start", "evt_message", 0, ""))
		transport.publish(t, fixtureFrame("event_delta", "evt_message", 1, "queued"))
		hub.mu.Lock()
		queued := hub.pendingFrames
		hub.mu.Unlock()
		if queued != 2 {
			t.Fatalf("at-count-bound transport frames=%d want2", queued)
		}
		if iteration%2 == 0 {
			hub.invalidate(viewer.entry, "controlled_loss")
		} else {
			viewer.Close()
		}
		hub.mu.Lock()
		frames, bytes, rows := hub.pendingFrames, hub.pendingBytes, len(hub.queue)
		hub.mu.Unlock()
		if frames != 0 || bytes != 0 || rows != 0 {
			t.Fatalf("removed queue credits remain: frames=%d bytes=%d rows=%d", frames, bytes, rows)
		}
		if hub.metrics.hubDrops.Load() != 0 {
			t.Fatal("returned credits exhausted the next healthy count-bound queue")
		}
		viewer.Close()
	}
}
