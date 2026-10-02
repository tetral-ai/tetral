package transporttest

import (
	"encoding/json"
	"math"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc/status"
)

// MeasureCompletions records a fixed local correctness cohort. It reports raw
// monotonic operation boundaries, not whole-test latency or a capacity gate.
// One warmup batch precedes three measured batches of sixteen serial calls.
func MeasureCompletions(t *testing.T, cohort, receiver, method string, operation func() error) {
	t.Helper()
	epoch := time.Now()
	durations := []int64{}
	failures, warmupFailures := 0, 0
	for batch := 0; batch < 4; batch++ {
		for sample := 0; sample < 16; sample++ {
			start := time.Now()
			err := operation()
			end := time.Now()
			outcome := "success"
			if err != nil {
				outcome = status.Code(err).String()
				if batch > 0 {
					failures++
				} else {
					warmupFailures++
				}
			} else if batch > 0 {
				durations = append(durations, end.Sub(start).Nanoseconds())
			}
			record := map[string]any{"cohort": cohort, "receiver": receiver, "method": method, "batch": batch, "sample": sample, "warmup": batch == 0, "startOffsetNs": start.Sub(epoch).Nanoseconds(), "endOffsetNs": end.Sub(epoch).Nanoseconds(), "durationNs": end.Sub(start).Nanoseconds(), "outcome": outcome}
			data, _ := json.Marshal(record)
			t.Logf("completion_sample %s", data)
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	percentile := func(p float64) int64 {
		if len(durations) == 0 {
			return 0
		}
		return durations[int(math.Ceil(float64(len(durations))*p))-1]
	}
	summary, _ := json.Marshal(map[string]any{"cohort": cohort, "attempted": 48, "succeeded": len(durations), "failed": failures, "warmupAttempted": 16, "warmupFailed": warmupFailures, "totalAttempted": 64, "totalFailed": failures + warmupFailures, "percentileConvention": "nearest-rank", "p50Ns": percentile(.5), "p95Ns": percentile(.95), "p99Ns": percentile(.99), "concurrency": 1, "deadlineMs": 2000, "measuredBatches": 3, "samplesPerBatch": 16})
	t.Logf("completion_summary %s", summary)
	if failures+warmupFailures > 0 {
		t.Fatalf("correctness cohort%s had %d failed operations; durations retained", cohort, failures+warmupFailures)
	}
}

// RecordBoundaryCompletion preserves failed-operation durations separately from
// the successful latency cohorts. These controls prove finite completion, not
// a percentile or capacity estimate.
func RecordBoundaryCompletion(t *testing.T, receiver, method, boundary string, start time.Time, err error) {
	t.Helper()
	end := time.Now()
	outcome := "success"
	if err != nil {
		outcome = status.Code(err).String()
	}
	data, _ := json.Marshal(map[string]any{"receiver": receiver, "method": method, "boundary": boundary, "startOffsetNs": int64(0), "endOffsetNs": end.Sub(start).Nanoseconds(), "durationNs": end.Sub(start).Nanoseconds(), "outcome": outcome, "attempted": 1, "failed": err != nil})
	t.Logf("boundary_completion %s", data)
}
