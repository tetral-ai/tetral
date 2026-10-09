package tetralqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

func TestMaintenanceTickReclaimsThenSweepsSandboxJobsAndCounters(t *testing.T) {
	store := &recordingMaintenanceStore{}
	now := time.Date(2026, 7, 31, 15, 0, 0, 0, time.UTC)
	runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 37}, now)

	wantCalls := []string{"reclaim:37", "sandbox-terminal:100", "empty-counters:100", "job-runner-terminal"}
	if len(store.calls) != len(wantCalls) {
		t.Fatalf("maintenance calls = %v; want %v", store.calls, wantCalls)
	}
	for index := range wantCalls {
		if store.calls[index] != wantCalls[index] {
			t.Fatalf("maintenance calls = %v; want %v", store.calls, wantCalls)
		}
	}
	if !store.sandboxSweepNow.Equal(now) {
		t.Fatalf("sandbox sweep time = %v; want %v", store.sandboxSweepNow, now)
	}
}

func TestMaintenanceTickStopsAfterReclaimFailure(t *testing.T) {
	store := &recordingMaintenanceStore{reclaimErr: errors.New("reclaim failed")}
	runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 10}, time.Now())
	if len(store.calls) != 1 || store.calls[0] != "reclaim:10" {
		t.Fatalf("maintenance calls after reclaim failure = %v; want reclaim only", store.calls)
	}
}

func TestMaintenanceTickContinuesCounterSweepAfterTerminalRowIntegritySignal(t *testing.T) {
	store := &recordingMaintenanceStore{sandboxSweepErr: &queue.IntegrityError{Message: "terminal timestamp is missing"}}
	runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 10}, time.Now())
	wantCalls := []string{"reclaim:10", "sandbox-terminal:100", "empty-counters:100", "job-runner-terminal"}
	if !reflect.DeepEqual(store.calls, wantCalls) {
		t.Fatalf("maintenance calls after terminal integrity signal = %v; want %v", store.calls, wantCalls)
	}
}

// Job Runner terminal retention runs last in a tick, under its own two-second
// deadline and with the tick's time. The budget counter grows by one per pass
// in which the store reports any exhausted state (full page and another
// eligible row), however many, and never for a full page alone; the number of
// exhausted states is logged; retained rows with a NULL terminal timestamp are
// reported as an integrity count; a failure logs no driver text.
func TestMaintenanceTickRunsJobRunnerRetentionLastWithItsOwnBudget(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	metrics := NewRetentionMetrics()
	counter := func() float64 {
		samples, err := metrics.Collector()(context.Background())
		if err != nil || len(samples) != 1 || samples[0].Name != "queue_retention_budget_exhausted_total" ||
			len(samples[0].Labels) != 1 || samples[0].Labels[0].Name != "phase" || samples[0].Labels[0].Value != "job_runner_terminal" {
			t.Fatalf("retention samples = %#v/%v", samples, err)
		}
		return samples[0].Value
	}
	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil))
	for _, pass := range []struct {
		result      queue.JobRunnerTerminalRetentionResult
		err         error
		wantCounter float64
		wantLog     string
	}{
		{queue.JobRunnerTerminalRetentionResult{Deleted: 256}, nil, 0, `"budget.exhausted":false`},
		{queue.JobRunnerTerminalRetentionResult{Deleted: 512, ExhaustedStates: 2}, nil, 1, `"target.count":2`},
		{queue.JobRunnerTerminalRetentionResult{Malformed: 2}, nil, 1, `"malformed.count":2`},
		{queue.JobRunnerTerminalRetentionResult{Deleted: 1}, errors.New("raw driver detail"), 1, `"error.code":"queue_retention_failed"`},
	} {
		buffer.Reset()
		store := &recordingMaintenanceStore{runnerRetention: pass.result, runnerRetentionErr: pass.err}
		started := time.Now()
		runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 10, Logger: logger, Metrics: metrics}, now)
		finished := time.Now()
		if got := store.calls[len(store.calls)-1]; got != "job-runner-terminal" || !store.runnerRetentionNow.Equal(now) {
			t.Fatalf("last call %s at %s; want Runner retention at the tick time", got, store.runnerRetentionNow)
		}
		if deadline := store.runnerRetentionDeadline; deadline.Before(started.Add(jobRunnerRetentionDeadline)) || deadline.After(finished.Add(jobRunnerRetentionDeadline)) {
			t.Fatalf("Runner retention deadline = %s; want %s after the phase started", deadline, jobRunnerRetentionDeadline)
		}
		if got := counter(); got != pass.wantCounter {
			t.Fatalf("budget counter = %v; want %v", got, pass.wantCounter)
		}
		if !strings.Contains(buffer.String(), pass.wantLog) || strings.Contains(buffer.String(), "raw driver detail") {
			t.Fatalf("retention log = %s; want %s without driver text", buffer.String(), pass.wantLog)
		}
	}
	store := &recordingMaintenanceStore{reclaimErr: errors.New("reclaim failed")}
	runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 10, Metrics: metrics}, now)
	if len(store.calls) != 1 {
		t.Fatalf("calls after reclaim failure = %v; want the existing early stop", store.calls)
	}
}

func TestLeaseReclaimMaintenanceLogsSharedOperationAndErrorFields(t *testing.T) {
	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil)).With(
		slog.String("service.name", "queue"),
		slog.String("deployment.environment", "test"),
		slog.String("service.version", "unit"),
	)

	logLeaseReclaimResult(logger, 2, nil, 25*time.Millisecond)
	logLeaseReclaimResult(logger, 0, errors.New("raw database details must not be logged"), 10*time.Millisecond)

	records := decodeJSONLogRecords(t, buffer.Bytes())
	if len(records) != 2 {
		t.Fatalf("records = %d; want 2", len(records))
	}
	if records[0]["msg"] != "queue.lease_reclaim.completed" ||
		records[0]["operation"] != "queue.lease_reclaim" ||
		records[0]["event.kind"] != "queue.lease_reclaim.completed" ||
		records[0]["component"] != "queue" ||
		records[0]["service.name"] != "queue" ||
		records[0]["deployment.environment"] != "test" ||
		records[0]["service.version"] != "unit" ||
		records[0]["duration.ms"] != float64(25) ||
		records[0]["queue.jobs.reclaimed"] != float64(2) {
		t.Fatalf("success record = %#v; want shared operation fields", records[0])
	}
	if records[1]["msg"] != "queue.lease_reclaim.failed" ||
		records[1]["operation"] != "queue.lease_reclaim" ||
		records[1]["event.kind"] != "queue.lease_reclaim.failed" ||
		records[1]["component"] != "queue" ||
		records[1]["duration.ms"] != float64(10) ||
		records[1]["retryable"] != true ||
		records[1]["terminal"] != false ||
		records[1]["error.class"] != "queue_maintenance_error" ||
		records[1]["error.code"] != "lease_reclaim_failed" ||
		records[1]["error.message_safe"] != "queue lease reclaim failed" {
		t.Fatalf("failure record = %#v; want shared operation/error fields", records[1])
	}
	if bytes.Contains(buffer.Bytes(), []byte("raw database details")) {
		t.Fatalf("failure log leaked raw error details: %s", buffer.String())
	}
}

func decodeJSONLogRecords(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(body), []byte("\n"))
	records := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode log record %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

type recordingMaintenanceStore struct {
	calls                   []string
	sandboxSweepNow         time.Time
	reclaimErr              error
	sandboxSweepErr         error
	runnerRetention         queue.JobRunnerTerminalRetentionResult
	runnerRetentionErr      error
	runnerRetentionNow      time.Time
	runnerRetentionDeadline time.Time
}

func (s *recordingMaintenanceStore) ReclaimExpiredLeases(_ context.Context, request queue.ReclaimExpiredLeasesRequest) (int, error) {
	s.calls = append(s.calls, "reclaim:"+strconv.Itoa(request.Limit))
	return 0, s.reclaimErr
}

func (s *recordingMaintenanceStore) SweepSandboxTerminalJobs(_ context.Context, request queue.SandboxTerminalSweepRequest) (int, error) {
	s.calls = append(s.calls, "sandbox-terminal:"+strconv.Itoa(request.Limit))
	s.sandboxSweepNow = request.Now
	return 0, s.sandboxSweepErr
}

func (s *recordingMaintenanceStore) SweepEmptyPartitionCounters(_ context.Context, request queue.EmptyPartitionCounterSweepRequest) (int, error) {
	s.calls = append(s.calls, "empty-counters:"+strconv.Itoa(request.Limit))
	return 0, nil
}

func (s *recordingMaintenanceStore) PruneJobRunnerTerminalJobs(ctx context.Context, request queue.JobRunnerTerminalRetentionRequest) (queue.JobRunnerTerminalRetentionResult, error) {
	s.calls = append(s.calls, "job-runner-terminal")
	s.runnerRetentionNow = request.Now
	s.runnerRetentionDeadline, _ = ctx.Deadline()
	return s.runnerRetention, s.runnerRetentionErr
}
