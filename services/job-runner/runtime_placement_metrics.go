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
	mutex            sync.Mutex
	probes, attempts map[string]float64
	operations       *workload.OperationMetrics
}

// The zero value is used by owning resolvers and tests. Every access, including
// collector initialization, is protected by mutex; the public collector reads
// the very registry populated by the existing joined probe/attempt hooks.
func (m *RuntimePlacementMetrics) operationMetricsLocked() *workload.OperationMetrics {
	if m.operations == nil {
		m.operations = workload.NewOperationMetrics("job-runner", "runtime_placement_probe", "runtime_placement")
	}
	return m.operations
}

func runtimePlacementMetricOutcome(reason string) string {
	switch reason {
	case "eligible", "selected":
		return "success"
	case "timeout":
		return "timeout"
	case "cancelled":
		return "cancelled"
	case "not_accepting", "capacity_excluded", "no_candidates", "exhausted":
		return "rejected"
	default:
		return "error"
	}
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
	m.operationMetricsLocked().Observe("runtime_placement_probe", runtimePlacementMetricOutcome(outcome), elapsed)
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
	m.operationMetricsLocked().Observe("runtime_placement", runtimePlacementMetricOutcome(outcome), elapsed)
}
func (m *RuntimePlacementMetrics) Collector() workload.MetricsCollector {
	return func(ctx context.Context) ([]workload.Metric, error) {
		if m == nil {
			return nil, nil
		}
		m.mutex.Lock()
		defer m.mutex.Unlock()
		// Durations are exported only through the operation histogram; these
		// counters keep the fine-grained reasons its outcome label folds together.
		var metrics []workload.Metric
		for _, outcome := range []string{"eligible", "registry_unavailable", "timeout", "cancelled", "transport_error", "http_error", "invalid_metrics", "not_accepting", "capacity_excluded", "response_too_large"} {
			metrics = append(metrics, workload.Metric{Name: "runtime_placement_probe_total", Help: "Runtime placement probe outcomes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "outcome", Value: outcome}}, Value: m.probes[outcome]})
		}
		for _, outcome := range []string{"selected", "exhausted", "no_candidates", "visibility_not_ready", "timeout", "cancelled", "invalid_policy", "random_unavailable"} {
			metrics = append(metrics, workload.Metric{Name: "runtime_placement_total", Help: "Bounded Runtime placement sample outcomes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "outcome", Value: outcome}}, Value: m.attempts[outcome]})
		}
		operations, err := m.operationMetricsLocked().Collector()(ctx)
		return append(metrics, operations...), err
	}
}
