package tetralcleanup

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

type SchedulerMetrics struct {
	Operations       *workload.OperationMetrics
	mu               sync.Mutex
	claimDueRuns     int64
	claimDueJobs     int64
	claimDueDuration time.Duration
}

func NewSchedulerMetrics() *SchedulerMetrics {
	return &SchedulerMetrics{Operations: workload.NewOperationMetrics("cleanup", "claim_due")}
}

// ObserveClaimDue records one scheduling phase: its outcome and duration in the
// operation histogram, and its committed claims and duration in the counters.
func (m *SchedulerMetrics) ObserveClaimDue(claimedJobs int, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.Operations.Observe("claim_due", schedulingOutcome(err), duration)
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
		samples := []workload.Metric{
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
		}
		observations, _ := m.Operations.Collector()(context.Background())
		return append(samples, observations...), nil
	}
}

// schedulingOutcome classifies the phase itself. A phase that finished with
// failed candidate claims is an error, even when a claim failed on its own
// deadline.
func schedulingOutcome(err error) string {
	var claimFailures *claimFailuresError
	switch {
	case err == nil:
		return "success"
	case errors.As(err, &claimFailures):
		return "error"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "error"
	}
}
