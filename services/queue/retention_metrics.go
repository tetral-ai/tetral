package tetralqueue

import (
	"context"
	"sync/atomic"

	"github.com/tetral-ai/tetral/internal/workload"
)

// jobRunnerRetentionPhase is the fixed phase label of the Queue retention
// budget counter.
const jobRunnerRetentionPhase = "job_runner_terminal"

// RetentionMetrics counts Job Runner terminal-retention passes in which at
// least one terminal state spent its 256-row page while the probe after it
// still found an eligible row. A pass whose candidates ran out never counts.
type RetentionMetrics struct {
	budgetExhausted atomic.Int64
}

func NewRetentionMetrics() *RetentionMetrics { return &RetentionMetrics{} }

func (m *RetentionMetrics) observeBudgetExhausted() {
	if m != nil {
		m.budgetExhausted.Add(1)
	}
}

func (m *RetentionMetrics) Collector() workload.MetricsCollector {
	return func(context.Context) ([]workload.Metric, error) {
		if m == nil {
			return nil, nil
		}
		return []workload.Metric{{
			Name:   "queue_retention_budget_exhausted_total",
			Help:   "Job Runner terminal-retention passes that stopped on their batch budget while eligible rows remained.",
			Type:   "counter",
			Labels: []workload.MetricLabel{{Name: "phase", Value: jobRunnerRetentionPhase}},
			Value:  float64(m.budgetExhausted.Load()),
		}}, nil
	}
}
