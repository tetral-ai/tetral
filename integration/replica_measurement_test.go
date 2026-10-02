package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// Samples use monotonic operation boundaries. Batch orchestration and percentile
// reporting belong to the evidence runner; whole-test elapsed times are never
// substituted for these call/stream completion measurements.
type replicaCompletionSample struct {
	Cohort        string `json:"cohort"`
	Method        string `json:"method"`
	Receiver      string `json:"receiver"`
	Outcome       string `json:"outcome"`
	StartBoundary string `json:"start_boundary"`
	EndBoundary   string `json:"end_boundary"`
	ClockID       string `json:"clock_id"`
	StartNS       int64  `json:"start_ns"`
	EndNS         int64  `json:"end_ns"`
	DurationNS    int64  `json:"duration_ns"`
	BindingID     string `json:"binding_id,omitempty"`
	PodUID        string `json:"pod_uid,omitempty"`
	ProcessID     string `json:"process_id,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
}

var replicaMeasurementOrigin = time.Now()

func replicaRecordCompletion(t *testing.T, cohort, method, receiver, outcome string, start time.Time) {
	t.Helper()
	end := time.Now()
	replicaLogCompletion(t, replicaCompletionSample{Cohort: cohort, Method: method, Receiver: receiver, Outcome: outcome, StartBoundary: "call_started", EndBoundary: "call_completed", ClockID: fmt.Sprintf("go:%d", os.Getpid()), StartNS: start.Sub(replicaMeasurementOrigin).Nanoseconds(), EndNS: end.Sub(replicaMeasurementOrigin).Nanoseconds(), DurationNS: end.Sub(start).Nanoseconds()})
}
func replicaLogCompletion(t *testing.T, sample replicaCompletionSample) {
	t.Helper()
	if sample.Cohort == "" || sample.Method == "" || sample.Receiver == "" || sample.Outcome == "" || sample.StartBoundary == "" || sample.EndBoundary == "" || sample.ClockID == "" || sample.StartNS < 0 || sample.EndNS-sample.StartNS != sample.DurationNS || sample.DurationNS < 0 {
		t.Fatalf("invalid completion sample: %+v", sample)
	}
	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("replica_completion %s", encoded)
}
