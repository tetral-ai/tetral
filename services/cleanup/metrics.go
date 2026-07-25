package tetralcleanup

import (
	"context"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

type SchedulerMetrics struct {
	mu               sync.Mutex
	claimDueRuns     int64
	claimDueJobs     int64
	claimDueDuration time.Duration
}

func NewSchedulerMetrics() *SchedulerMetrics {
	return &SchedulerMetrics{}
}

func (m *SchedulerMetrics) ObserveClaimDue(claimedJobs int, duration time.Duration) {
	if m == nil {
		return
	}
	if claimedJobs < 0 {
		claimedJobs = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimDueRuns++
	m.claimDueJobs += int64(claimedJobs)
	if duration > 0 {
		m.claimDueDuration += duration
	}
}

func (m *SchedulerMetrics) Collector() workload.MetricsCollector {
	return func(context.Context) ([]workload.Metric, error) {
		if m == nil {
			return nil, nil
		}
		m.mu.Lock()
		runs := m.claimDueRuns
		jobs := m.claimDueJobs
		durationMS := m.claimDueDuration.Milliseconds()
		m.mu.Unlock()
		return []workload.Metric{
			{
				Name:  "tetral_cleanup_claim_due_runs_total",
				Help:  "Cleanup scheduler claim_due runs.",
				Type:  "counter",
				Value: float64(runs),
			},
			{
				Name:  "tetral_cleanup_jobs_claimed_total",
				Help:  "Cleanup session jobs claimed by the cleanup scheduler.",
				Type:  "counter",
				Value: float64(jobs),
			},
			{
				Name:  "tetral_cleanup_claim_due_duration_ms_total",
				Help:  "Total cleanup scheduler claim_due duration in milliseconds.",
				Type:  "counter",
				Value: float64(durationMS),
			},
		}, nil
	}
}
