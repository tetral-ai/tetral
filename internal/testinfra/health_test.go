package testinfra

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCollectGoTestDurationsCountsCompletedTopLevelTests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-test.jsonl")
	body := []byte("{\"Action\":\"pass\",\"Package\":\"example/pkg\",\"Test\":\"TestOne\",\"Elapsed\":1.25}\n" +
		"{\"Action\":\"pass\",\"Package\":\"example/pkg\",\"Test\":\"TestOne/sub\",\"Elapsed\":0.25}\n" +
		"{\"Action\":\"fail\",\"Package\":\"example/pkg\",\"Test\":\"TestTwo\",\"Elapsed\":2}\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	durations := map[string]float64{}
	if err := collectGoTestDurations(path, durations); err != nil {
		t.Fatal(err)
	}
	if len(durations) != 1 || durations["example/pkg/TestOne"] != 1.25 {
		t.Fatalf("durations = %#v", durations)
	}
}

func TestCollectCompletedJobHealthIgnoresCurrentAndMalformedJobs(t *testing.T) {
	started := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	report := CIHealthReport{JobConclusions: map[string]int{}, GoShardMinutes: map[string]float64{}}
	collectCompletedJobHealth(&report, []ShadowJob{
		{Name: "Go Race (shard 0)", Status: "completed", Conclusion: "success", StartedAt: started, CompletedAt: started.Add(2 * time.Minute)},
		{Name: "CI Health and Calibration", Status: "in_progress", StartedAt: started},
		{Name: "broken", Status: "completed", Conclusion: "failure", StartedAt: started},
	})
	if report.RunnerMinutes != 2 || report.JobConclusions["success"] != 1 || len(report.JobConclusions) != 1 || report.GoShardMinutes["Go Race (shard 0)"] != 2 {
		t.Fatalf("health summary = %+v", report)
	}
}
