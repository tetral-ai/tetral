package eventstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type PreviewSubscription interface{ Close() error }

// A subscription is returned even when its initial setup cannot complete; its
// process-owned transport restores the desired subscription after recovery.
type PreviewTransport interface {
	Subscribe(context.Context, string, func(string, []byte), func(string)) (PreviewSubscription, error)
}

type previewEnvelope struct {
	subject    string
	data       []byte
	entry      *previewEntry
	generation uint64
	receivedAt time.Time
}
type queuedPreview struct {
	frame      eventwire.PreviewFrame
	bytes      int
	receivedAt time.Time
}
type previewEntry struct {
	subject      string
	viewers      map[*PreviewViewer]struct{}
	subscription PreviewSubscription
	closed       bool
	generation   uint64
}

type PreviewHub struct {
	transport PreviewTransport
	config    StreamConfig
	metrics   *PreviewMetrics
	// subscriptionsMu serializes viewer joins and departures with transport
	// subscription setup and teardown (Join, PreviewViewer.Close and Close).
	// An entry's subscription is assigned before any path can close it, and a
	// join cannot reuse a subscription that is still closing. Lock order is
	// subscriptionsMu, then mu. The lock is hub-wide, so a slow Subscribe delays
	// other joins and departures for up to its context bound, and a transport
	// Close delays them until it returns.
	subscriptionsMu sync.Mutex
	// mu guards entries, queues and byte/frame accounting. It is never held
	// across a transport call, and transport callbacks (offer, invalidate) take
	// only mu, so a callback delivered during Subscribe or Close cannot deadlock.
	mu            sync.Mutex
	entries       map[string]*previewEntry
	queue         []previewEnvelope
	pendingBytes  int
	pendingFrames int
	viewerBytes   int
	viewerFrames  int
	wake          chan struct{}
	done          chan struct{}
	cancel        context.CancelFunc
	closed        bool
}

type PreviewViewer struct {
	hub                *PreviewHub
	entry              *previewEntry
	queue              []queuedPreview
	pendingBytes       int
	inFlightReceivedAt time.Time
	inFlightBytes      int
	encodingBytes      int
	epoch              uint64
	lastReason         string
	wake               chan struct{}
	closed             bool
}

func NewPreviewHub(transport PreviewTransport, config StreamConfig, metrics *PreviewMetrics) (*PreviewHub, error) {
	if transport == nil {
		return nil, errors.New("preview transport is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if metrics == nil {
		metrics = NewPreviewMetrics()
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &PreviewHub{transport: transport, config: config, metrics: metrics, entries: map[string]*previewEntry{}, wake: make(chan struct{}, 1), done: make(chan struct{}), cancel: cancel}
	go h.dispatch(ctx)
	return h, nil
}

// Join registers a viewer for the authorized workspace/Session subject. The
// first local viewer installs the transport subscription while holding
// subscriptionsMu but not mu; ctx bounds that setup. On setup failure the
// viewer is still returned, with its previews already invalidated.
func (h *PreviewHub) Join(ctx context.Context, ws workspace.ID, sessionID string) (*PreviewViewer, error) {
	subject := eventwire.PreviewSubject(string(ws), sessionID)
	h.subscriptionsMu.Lock()
	defer h.subscriptionsMu.Unlock()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, errors.New("preview hub closed")
	}
	entry := h.entries[subject]
	first := entry == nil
	if first {
		entry = &previewEntry{subject: subject, viewers: map[*PreviewViewer]struct{}{}}
		h.entries[subject] = entry
	}
	viewer := &PreviewViewer{hub: h, entry: entry, wake: make(chan struct{}, 1)}
	entry.viewers[viewer] = struct{}{}
	h.metrics.viewers.Add(1)
	h.mu.Unlock()
	if !first {
		return viewer, nil
	}
	subscription, err := h.transport.Subscribe(ctx, subject, func(subject string, data []byte) { h.offer(entry, subject, data) }, func(reason string) { h.invalidate(entry, reason) })
	h.mu.Lock()
	entry.subscription = subscription
	if subscription != nil {
		h.metrics.subscriptions.Add(1)
	}
	h.mu.Unlock()
	if err != nil {
		h.invalidate(entry, "setup_unavailable")
	}
	return viewer, err
}

func (h *PreviewHub) offer(entry *previewEntry, subject string, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || entry.closed {
		return
	}
	if len(data) > eventwire.MaxPreviewFrameBytes || h.pendingBytes+h.viewerBytes+len(data) > h.config.HubMaxBytes || h.pendingFrames+h.viewerFrames+1 > h.config.HubMaxFrames {
		h.metrics.hubDrops.Add(1)
		h.invalidateLocked(entry, "hub_overflow")
		return
	}
	h.queue = append(h.queue, previewEnvelope{subject: subject, data: append([]byte(nil), data...), entry: entry, generation: entry.generation, receivedAt: time.Now()})
	h.pendingBytes += len(data)
	h.pendingFrames++
	h.updatePendingLocked()
	signal(h.wake)
}

func (h *PreviewHub) invalidate(entry *previewEntry, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.invalidateLocked(entry, reason)
}
func (h *PreviewHub) invalidateLocked(entry *previewEntry, reason string) {
	if entry.closed {
		return
	}
	entry.generation++
	// Discard every queued frame for the affected subscription. Keeping old
	// request_open frames would re-admit an active request after reconnect.
	kept := h.queue[:0]
	for _, item := range h.queue {
		if item.entry == entry {
			h.pendingBytes -= len(item.data)
			h.pendingFrames--
		} else {
			kept = append(kept, item)
		}
	}
	clear(h.queue[len(kept):])
	h.queue = kept
	h.updatePendingLocked()
	for viewer := range entry.viewers {
		h.stopViewerLocked(viewer, reason)
	}
}
func (h *PreviewHub) stopViewerLocked(viewer *PreviewViewer, reason string) {
	h.viewerBytes -= viewer.pendingBytes
	h.viewerFrames -= len(viewer.queue)
	clear(viewer.queue)
	viewer.queue = nil
	viewer.pendingBytes = 0
	viewer.epoch++
	viewer.lastReason = reason
	h.metrics.viewerDrops.Add(1)
	h.updatePendingLocked()
	signal(viewer.wake)
}

func (h *PreviewHub) dispatch(ctx context.Context) {
	defer close(h.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.wake:
		}
		for {
			h.mu.Lock()
			if len(h.queue) == 0 {
				h.mu.Unlock()
				break
			}
			item := h.queue[0]
			h.queue[0] = previewEnvelope{}
			h.queue = h.queue[1:]
			h.mu.Unlock()
			frame, err := eventwire.DecodePreviewFrame(item.subject, item.data)
			if item.subject != item.entry.subject {
				err = errors.New("preview subscription scope mismatch")
			}
			h.mu.Lock()
			if err == nil && !item.entry.closed && item.generation == item.entry.generation {
				for viewer := range item.entry.viewers {
					if viewer.closed {
						continue
					}
					if viewer.pendingBytes+viewer.inFlightBytes+viewer.encodingBytes+len(item.data) > h.config.ViewerMaxBytes || len(viewer.queue)+boolCount(viewer.inFlightBytes > 0)+1 > h.config.ViewerMaxFrames {
						h.stopViewerLocked(viewer, "viewer_overflow")
						continue
					}
					if h.pendingBytes+h.viewerBytes+len(item.data) > h.config.HubMaxBytes || h.pendingFrames+h.viewerFrames+1 > h.config.HubMaxFrames {
						h.metrics.hubDrops.Add(1)
						h.invalidateLocked(item.entry, "hub_overflow")
						break
					}
					h.viewerBytes += len(item.data)
					h.viewerFrames++
					viewer.queue = append(viewer.queue, queuedPreview{frame: frame, bytes: len(item.data), receivedAt: item.receivedAt})
					viewer.pendingBytes += len(item.data)
					signal(viewer.wake)
				}
			} else if err != nil {
				h.metrics.invalidFrames.Add(1)
			}
			// The current ingress envelope remains owned through fanout. Return
			// its frame credit at the same point as its encoded byte credit.
			h.pendingBytes -= len(item.data)
			h.pendingFrames--
			h.updatePendingLocked()
			h.mu.Unlock()
		}
	}
}
func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
func signal(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (v *PreviewViewer) Wake() <-chan struct{} { return v.wake }

// Take keeps the dequeued frame charged until Release, including blocked writes.
func (v *PreviewViewer) Take() (eventwire.PreviewFrame, uint64, string, bool) {
	h := v.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if v.closed || len(v.queue) == 0 || v.inFlightBytes > 0 {
		return eventwire.PreviewFrame{}, v.epoch, v.lastReason, false
	}
	item := v.queue[0]
	v.queue[0] = queuedPreview{}
	v.queue = v.queue[1:]
	v.pendingBytes -= item.bytes
	v.inFlightBytes += item.bytes
	v.inFlightReceivedAt = item.receivedAt
	return item.frame, v.epoch, v.lastReason, true
}
func (v *PreviewViewer) receivedAt() time.Time {
	v.hub.mu.Lock()
	defer v.hub.mu.Unlock()
	return v.inFlightReceivedAt
}
func (v *PreviewViewer) Release() {
	h := v.hub
	h.mu.Lock()
	if v.inFlightBytes > 0 {
		h.viewerBytes -= v.inFlightBytes + v.encodingBytes
		h.viewerFrames--
	}
	v.inFlightBytes = 0
	v.inFlightReceivedAt = time.Time{}
	v.encodingBytes = 0
	h.updatePendingLocked()
	h.mu.Unlock()
}
func (v *PreviewViewer) reserveEncoding(size int) bool {
	h := v.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if v.closed || v.pendingBytes+v.inFlightBytes+size > h.config.ViewerMaxBytes || h.pendingBytes+h.viewerBytes+size > h.config.HubMaxBytes {
		h.stopViewerLocked(v, "viewer_overflow")
		return false
	}
	v.encodingBytes = size
	h.viewerBytes += size
	h.updatePendingLocked()
	return true
}
func (h *PreviewHub) updatePendingLocked() {
	h.metrics.hubPendingBytes.Store(int64(h.pendingBytes + h.viewerBytes))
}
func (v *PreviewViewer) Epoch() (uint64, string) {
	v.hub.mu.Lock()
	defer v.hub.mu.Unlock()
	return v.epoch, v.lastReason
}
func (v *PreviewViewer) Close() {
	h := v.hub
	h.subscriptionsMu.Lock()
	defer h.subscriptionsMu.Unlock()
	h.mu.Lock()
	if v.closed {
		h.mu.Unlock()
		return
	}
	v.closed = true
	h.viewerBytes -= v.pendingBytes + v.inFlightBytes + v.encodingBytes
	h.viewerFrames -= len(v.queue) + boolCount(v.inFlightBytes > 0)
	clear(v.queue)
	v.queue = nil
	v.pendingBytes = 0
	v.inFlightBytes = 0
	v.inFlightReceivedAt = time.Time{}
	v.encodingBytes = 0
	h.updatePendingLocked()
	delete(v.entry.viewers, v)
	h.metrics.viewers.Add(-1)
	if len(v.entry.viewers) != 0 {
		h.mu.Unlock()
		return
	}
	entry := v.entry
	entry.closed = true
	delete(h.entries, entry.subject)
	h.invalidateQueueLocked(entry)
	subscription := entry.subscription
	h.mu.Unlock()
	if subscription != nil {
		_ = subscription.Close()
		h.metrics.subscriptions.Add(-1)
	}
}
func (h *PreviewHub) invalidateQueueLocked(entry *previewEntry) {
	kept := h.queue[:0]
	for _, item := range h.queue {
		if item.entry == entry {
			h.pendingBytes -= len(item.data)
			h.pendingFrames--
		} else {
			kept = append(kept, item)
		}
	}
	clear(h.queue[len(kept):])
	h.queue = kept
	h.updatePendingLocked()
}
func (h *PreviewHub) Close() {
	h.subscriptionsMu.Lock()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.subscriptionsMu.Unlock()
		<-h.done
		return
	}
	h.closed = true
	h.cancel()
	entries := h.entries
	h.entries = map[string]*previewEntry{}
	for _, entry := range entries {
		entry.closed = true
		for viewer := range entry.viewers {
			viewer.closed = true
			h.viewerBytes -= viewer.pendingBytes + viewer.inFlightBytes + viewer.encodingBytes
			h.viewerFrames -= len(viewer.queue) + boolCount(viewer.inFlightBytes > 0)
			clear(viewer.queue)
			viewer.queue = nil
			viewer.pendingBytes = 0
			viewer.inFlightBytes = 0
			viewer.encodingBytes = 0
			signal(viewer.wake)
			h.metrics.viewers.Add(-1)
		}
	}
	clear(h.queue)
	h.queue = nil
	h.mu.Unlock()
	for _, entry := range entries {
		if entry.subscription != nil {
			_ = entry.subscription.Close()
			h.metrics.subscriptions.Add(-1)
		}
	}
	h.subscriptionsMu.Unlock()
	<-h.done
	h.mu.Lock()
	h.pendingBytes = 0
	h.pendingFrames = 0
	h.metrics.hubPendingBytes.Store(0)
	h.mu.Unlock()
}
