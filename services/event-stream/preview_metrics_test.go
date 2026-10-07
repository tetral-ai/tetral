package eventstream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPreviewMetricsReportBalancedOwnershipAndBoundedLabels(t *testing.T) {
	metrics := NewPreviewMetrics()
	transport := &fixtureTransport{}
	hub, err := NewPreviewHub(transport, DefaultStreamConfig(), metrics)
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := hub.Join(t.Context(), workspace.DefaultID, "sesn_preview")
	if err != nil {
		t.Fatal(err)
	}
	samples, err := metrics.Collector()(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, sample := range samples {
		if len(sample.Labels) != 0 {
			allowed := (sample.Name == "event_stream_formal_delivery_latency_seconds_bucket" || sample.Name == "event_stream_preview_delivery_latency_seconds_bucket") && len(sample.Labels) == 1 && sample.Labels[0].Name == "le"
			if allowed {
				allowed = false
				for _, value := range []string{"0.001", "0.01", "0.1", "1", "10", "+Inf"} {
					if sample.Labels[0].Value == value {
						allowed = true
					}
				}
			}
			if !allowed {
				t.Fatalf("identity/content or unbounded metric labels=%s %v", sample.Name, sample.Labels)
			}
		}
		values[sample.Name] = sample.Value
	}
	if values["event_stream_preview_viewers"] != 1 || values["event_stream_preview_subscriptions"] != 1 {
		t.Fatalf("active ownership gauges=%v", values)
	}
	transport.publish(t, fixtureFrame("event_start", "evt_message", 0, ""))
	takeFrame(t, viewer)
	viewer.Close()
	hub.Close()
	samples, err = metrics.Collector()(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range samples {
		if sample.Type == "gauge" && sample.Value != 0 {
			t.Fatalf("cleanup gauge %s=%g", sample.Name, sample.Value)
		}
	}
}

func TestPreviewMetricsMonotonicLatencyBucketsAndRequestOwnership(t *testing.T) {
	metrics := NewPreviewMetrics()
	metrics.formalLatency.observe(3 * time.Millisecond)
	metrics.formalLatency.observe(250 * time.Millisecond)
	metrics.previewLatency.observe(7 * time.Millisecond)
	samples, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, sample := range samples {
		key := sample.Name
		if len(sample.Labels) == 1 {
			key += "/" + sample.Labels[0].Value
		}
		values[key] = sample.Value
	}
	for key, want := range map[string]float64{
		"event_stream_formal_delivery_latency_seconds_count":        2,
		"event_stream_formal_delivery_latency_seconds_sum":          0.253,
		"event_stream_formal_delivery_latency_seconds_bucket/0.001": 0,
		"event_stream_formal_delivery_latency_seconds_bucket/0.01":  1,
		"event_stream_formal_delivery_latency_seconds_bucket/0.1":   1,
		"event_stream_formal_delivery_latency_seconds_bucket/1":     2,
		"event_stream_formal_delivery_latency_seconds_bucket/+Inf":  2,
		"event_stream_preview_delivery_latency_seconds_count":       1,
		"event_stream_preview_delivery_latency_seconds_sum":         0.007,
	} {
		if values[key] != want {
			t.Fatalf("latency instrument %s=%g want %g", key, values[key], want)
		}
	}
	h := &handler{options: newOptions(WithPreviewMetrics(metrics))}
	state := streamPreviewState{requests: map[string]*previewRequestState{}, types: map[string]bool{"agent.message": true}}
	h.trackPreviewRequest(&state, "request-1", &previewRequestState{})
	h.trackPreviewRequest(&state, "request-2", &previewRequestState{stopped: true})
	if metrics.activeRequests.Load() != 2 {
		t.Fatal("retained stopped request uncharged")
	}
	h.closeFormalPreview(&state, StreamChange{Event: Event{Type: "span.model_request_end"}, ModelRequestID: "request-1", ThreadRole: "main", RequestKind: "agent_provider_request"})
	if metrics.activeRequests.Load() != 1 {
		t.Fatal("durable End did not release sequencer ownership")
	}
	h.releasePreviewRequests(&state)
	h.releasePreviewRequests(&state)
	if metrics.activeRequests.Load() != 0 || len(state.requests) != 0 {
		t.Fatal("cancel/reset cleanup leaked request ownership")
	}
	for _, sample := range samples {
		if !strings.Contains(sample.Name, "latency") {
			continue
		}
		family := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(sample.Name, "_bucket"), "_count"), "_sum")
		if sample.Type != "histogram" || sample.Family != family || (family != "event_stream_formal_delivery_latency_seconds" && family != "event_stream_preview_delivery_latency_seconds") {
			t.Fatalf("latency sample %s is not part of its histogram family: type=%s family=%s", sample.Name, sample.Type, sample.Family)
		}
	}
	text := workload.RuntimeMetricsTextWith(samples)
	for _, family := range []string{"event_stream_formal_delivery_latency_seconds", "event_stream_preview_delivery_latency_seconds"} {
		if strings.Count(text, "# TYPE "+family+" histogram\n") != 1 || strings.Contains(text, "# TYPE "+family+"_") {
			t.Fatalf("latency family %s lacks exactly one histogram header:\n%s", family, text)
		}
	}
}
