package eventstream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

type nativePreviewMetrics struct {
	queueDepth                    func() int
	reservedBytes, reservedFrames int64
	currentBytes, currentFrames   atomic.Int64
}

// Local elapsed observations use time.Since's monotonic clock. Each latency is
// one fixed-bucket histogram family through the shared exposition, with only
// constant le labels and no identity labels.
type deliveryLatency struct {
	count, nanos atomic.Uint64
	buckets      [6]atomic.Uint64
}

func (l *deliveryLatency) observe(elapsed time.Duration) {
	if elapsed < 0 {
		elapsed = 0
	}
	l.count.Add(1)
	l.nanos.Add(uint64(elapsed))
	for i, upper := range [...]time.Duration{time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second, time.Duration(1<<63 - 1)} {
		if elapsed <= upper {
			l.buckets[i].Add(1)
		}
	}
}
func (l *deliveryLatency) samples(prefix, help string) []workload.Metric {
	family := prefix + "_seconds"
	help += " latency in local elapsed seconds."
	var result []workload.Metric
	for i, upper := range [...]string{"0.001", "0.01", "0.1", "1", "10", "+Inf"} {
		result = append(result, workload.Metric{Name: family + "_bucket", Family: family, Help: help, Type: "histogram", Labels: []workload.MetricLabel{{Name: "le", Value: upper}}, Value: float64(l.buckets[i].Load())})
	}
	return append(result,
		workload.Metric{Name: family + "_count", Family: family, Help: help, Type: "histogram", Value: float64(l.count.Load())},
		workload.Metric{Name: family + "_sum", Family: family, Help: help, Type: "histogram", Value: float64(l.nanos.Load()) / float64(time.Second)},
	)
}

type PreviewMetrics struct {
	activeStreams                 atomic.Int64
	activeRequests                atomic.Int64
	formalLatency, previewLatency deliveryLatency
	native                        atomic.Pointer[nativePreviewMetrics]
	viewers                       atomic.Int64
	subscriptions                 atomic.Int64
	hubPendingBytes               atomic.Int64
	hubDrops                      atomic.Uint64
	viewerDrops                   atomic.Uint64
	invalidFrames                 atomic.Uint64
	sequenceStops                 atomic.Uint64
	stoppedRequests               atomic.Uint64
	slowWriters                   atomic.Uint64
	previewEvents                 atomic.Uint64
	formalEvents                  atomic.Uint64
	sentBytes                     atomic.Uint64
	disconnects                   atomic.Uint64
	reconnects                    atomic.Uint64
}

func NewPreviewMetrics() *PreviewMetrics { return &PreviewMetrics{} }
func (m *PreviewMetrics) Collector() workload.MetricsCollector {
	return func(context.Context) ([]workload.Metric, error) {
		if m == nil {
			return nil, nil
		}
		var nativeBytes, nativeFrames, nativePendingFrames, nativeDispatchBytes int64
		if native := m.native.Load(); native != nil {
			nativeBytes, nativeFrames = native.reservedBytes, native.reservedFrames
			nativePendingFrames = int64(native.queueDepth()) + native.currentFrames.Load()
			nativeDispatchBytes = native.currentBytes.Load()
		}
		values := []struct {
			name, help, kind string
			value            float64
		}{
			{"event_stream_active_sse_viewers", "Active Session and Thread SSE connections.", "gauge", float64(m.activeStreams.Load())},
			{"event_stream_preview_active_requests", "Retained per-connection preview request sequencer entries, including stopped entries until End or cleanup.", "gauge", float64(m.activeRequests.Load())},
			{"event_stream_preview_viewers", "Local opted-in Session viewers.", "gauge", float64(m.viewers.Load())},
			{"event_stream_preview_subscriptions", "Local watched Session subscriptions.", "gauge", float64(m.subscriptions.Load())},
			{"event_stream_preview_pending_bytes", "Actual encoded hub/viewer queued, dispatching and encoding bytes.", "gauge", float64(m.hubPendingBytes.Load())},
			{"event_stream_nats_reserved_bytes", "Conservative process native payload reservation including parser, admission and dispatcher.", "gauge", float64(nativeBytes)},
			{"event_stream_nats_reserved_frames", "Conservative process native message ownership slots including parser and admission.", "gauge", float64(nativeFrames)},
			{"event_stream_nats_pending_frames", "Native shared-channel queued and currently dispatching messages.", "gauge", float64(nativePendingFrames)},
			{"event_stream_nats_dispatching_bytes", "Actual encoded payload bytes held by the current native dispatcher.", "gauge", float64(nativeDispatchBytes)},
			{"event_stream_preview_hub_drops_total", "Hub capacity losses.", "counter", float64(m.hubDrops.Load())},
			{"event_stream_preview_viewer_drops_total", "Viewer preview invalidations.", "counter", float64(m.viewerDrops.Load())},
			{"event_stream_preview_invalid_frames_total", "Rejected private frames.", "counter", float64(m.invalidFrames.Load())},
			{"event_stream_preview_sequence_stops_total", "Detected preview sequence failures.", "counter", float64(m.sequenceStops.Load())},
			{"event_stream_preview_stopped_requests_total", "Requests stopped for preview loss, capacity or unavailable admission.", "counter", float64(m.stoppedRequests.Load())},
			{"event_stream_slow_writers_total", "Response writes that exceeded their deadline.", "counter", float64(m.slowWriters.Load())},
			{"event_stream_preview_events_total", "Successfully written preview events.", "counter", float64(m.previewEvents.Load())},
			{"event_stream_formal_events_total", "Successfully written formal events.", "counter", float64(m.formalEvents.Load())},
			{"event_stream_sent_bytes_total", "Successfully written SSE data and heartbeat bytes.", "counter", float64(m.sentBytes.Load())},
			{"event_stream_nats_disconnects_total", "Observed subscriber connection losses.", "counter", float64(m.disconnects.Load())},
			{"event_stream_nats_reconnects_total", "Observed subscriber connection restorations.", "counter", float64(m.reconnects.Load())},
		}
		result := make([]workload.Metric, 0, len(values))
		for _, v := range values {
			result = append(result, workload.Metric{Name: v.name, Help: v.help, Type: v.kind, Value: v.value})
		}
		result = append(result, m.formalLatency.samples("event_stream_formal_delivery_latency", "Formal selection/read through successful flush")...)
		result = append(result, m.previewLatency.samples("event_stream_preview_delivery_latency", "Local hub ingress through successful preview flush")...)
		return result, nil
	}
}
