package jobrunner

import (
	"context"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

// Labels are finite operation outcomes. Pod, Session and process identities
// belong in the single owning attempt log, never unbounded metric labels.
type RuntimePlacementMetrics struct {
	mutex                        sync.Mutex
	probes, attempts             map[string]float64
	probeSeconds, attemptSeconds float64
}

func (m *RuntimePlacementMetrics) observeProbe(outcome string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.probes == nil {
		m.probes = map[string]float64{}
	}
	m.probes[outcome]++
	m.probeSeconds += elapsed.Seconds()
}
func (m *RuntimePlacementMetrics) observeAttempt(outcome string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.attempts == nil {
		m.attempts = map[string]float64{}
	}
	m.attempts[outcome]++
	m.attemptSeconds += elapsed.Seconds()
}
func (m *RuntimePlacementMetrics) Collector() workload.MetricsCollector {
	return func(context.Context) ([]workload.Metric, error) {
		if m == nil {
			return nil, nil
		}
		m.mutex.Lock()
		defer m.mutex.Unlock()
		metrics := []workload.Metric{{Name: "runtime_placement_probe_seconds_total", Help: "Total Runtime placement probe duration.", Type: "counter", Value: m.probeSeconds}, {Name: "runtime_placement_seconds_total", Help: "Total Runtime placement attempt duration.", Type: "counter", Value: m.attemptSeconds}}
		for _, outcome := range []string{"eligible", "registry_unavailable", "load_unavailable"} {
			metrics = append(metrics, workload.Metric{Name: "runtime_placement_probe_total", Help: "Runtime placement probe outcomes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "outcome", Value: outcome}}, Value: m.probes[outcome]})
		}
		for _, outcome := range []string{"selected", "unavailable"} {
			metrics = append(metrics, workload.Metric{Name: "runtime_placement_total", Help: "Bounded Runtime placement sample outcomes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "outcome", Value: outcome}}, Value: m.attempts[outcome]})
		}
		return metrics, nil
	}
}
