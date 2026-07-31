package tetralqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
)

func TestMaintenanceTickReclaimsThenSweepsSandboxJobsAndCounters(t *testing.T) {
	store := &recordingMaintenanceStore{}
	now := time.Date(2026, 7, 31, 15, 0, 0, 0, time.UTC)
	runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 37}, now)

	wantCalls := []string{"reclaim:37", "sandbox-terminal:100", "empty-counters:100"}
	if len(store.calls) != len(wantCalls) {
		t.Fatalf("maintenance calls = %v; want %v", store.calls, wantCalls)
	}
	for index := range wantCalls {
		if store.calls[index] != wantCalls[index] {
			t.Fatalf("maintenance calls = %v; want %v", store.calls, wantCalls)
		}
	}
	if !store.reclaimNow.Equal(now) || !store.sandboxSweepNow.Equal(now) {
		t.Fatalf("maintenance times = reclaim %v sweep %v; want %v", store.reclaimNow, store.sandboxSweepNow, now)
	}
}

func TestMaintenanceTickStopsAfterReclaimFailure(t *testing.T) {
	store := &recordingMaintenanceStore{reclaimErr: errors.New("reclaim failed")}
	runMaintenanceTick(context.Background(), store, MaintenanceConfig{Limit: 10}, time.Now())
	if len(store.calls) != 1 || store.calls[0] != "reclaim:10" {
		t.Fatalf("maintenance calls after reclaim failure = %v; want reclaim only", store.calls)
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
	calls           []string
	reclaimNow      time.Time
	sandboxSweepNow time.Time
	reclaimErr      error
}

func (s *recordingMaintenanceStore) ReclaimExpiredLeases(_ context.Context, request queue.ReclaimExpiredLeasesRequest) (int, error) {
	s.calls = append(s.calls, "reclaim:"+strconv.Itoa(request.Limit))
	s.reclaimNow = request.Now
	return 0, s.reclaimErr
}

func (s *recordingMaintenanceStore) SweepSandboxTerminalJobs(_ context.Context, request queue.SandboxTerminalSweepRequest) (int, error) {
	s.calls = append(s.calls, "sandbox-terminal:"+strconv.Itoa(request.Limit))
	s.sandboxSweepNow = request.Now
	return 0, nil
}

func (s *recordingMaintenanceStore) SweepEmptyPartitionCounters(_ context.Context, request queue.EmptyPartitionCounterSweepRequest) (int, error) {
	s.calls = append(s.calls, "empty-counters:"+strconv.Itoa(request.Limit))
	return 0, nil
}
