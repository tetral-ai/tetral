package jobrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
)

func placementVectorBody(active, usage int) string {
	return fmt.Sprintf("runtimepod_active_sessions %d\nruntimepod_session_capacity 16\nruntimepod_container_memory_usage_bytes %d\nruntimepod_container_memory_limit_bytes 1000\nruntimepod_ready 1\nruntimepod_accepting_commands 1\n", active, usage)
}
func placementEvidenceCandidates(t *testing.T, client *dbconnect.Client) []kubernetes.BindingCandidate {
	t.Helper()
	var candidates []kubernetes.BindingCandidate
	for i := 1; i <= 4; i++ {
		candidate := kubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: fmt.Sprintf("pod-%d", i), PodUID: fmt.Sprintf("uid-%d", i), PodIP: fmt.Sprintf("10.0.0.%d", i)}
		identity := runtimecontrol.ProcessIdentity{Namespace: candidate.Namespace, PodUID: candidate.PodUID, ID: "process_" + candidate.PodUID}
		registered, err := runtimecontrol.RegisterProcess(context.Background(), client, identity)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := runtimecontrol.ReportProcess(context.Background(), client, identity, registered.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

// All24 finite Fisher-Yates draw sequences are enumerated. Each ordered first
// pair appears twice, and both tie draws select every Pod equally. No statistic
// or wall-clock arrival order is used as a correctness oracle.
func TestPostgreSQLRuntimePlacementVectorsAndFiniteDraws(t *testing.T) {
	runtime, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	candidates := placementEvidenceCandidates(t, client)
	bodies := map[string]string{"10.0.0.1:8080": placementVectorBody(0, 100), "10.0.0.2:8080": placementVectorBody(4, 200), "10.0.0.3:8080": placementVectorBody(2, 800), "10.0.0.4:8080": placementVectorBody(16, 100)}
	var mu sync.Mutex
	calls := map[string]int{}
	httpClient := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.Host]++
		body := bodies[r.Host]
		mu.Unlock()
		_, _ = io.WriteString(w, body)
	}))
	resolver := KubernetesRuntimeTargetResolver{LoadClient: httpClient, PlacementPolicy: DefaultRuntimePlacementPolicy(), Snapshot: func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.NewBindingVisibilitySnapshotForTest(true, append(candidates, candidates[0]))
	}}
	resolver.PlacementPolicy.Rounds = 1
	type draws struct {
		values []int
		pair   string
	}
	var sequences []draws
	for a := 0; a < 4; a++ {
		for b := 0; b < 3; b++ {
			for c := 0; c < 2; c++ {
				order := []int{0, 1, 2, 3}
				values := []int{a, b, c}
				for index, i := range []int{3, 2, 1} {
					order[i], order[values[index]] = order[values[index]], order[i]
				}
				sequences = append(sequences, draws{values, fmt.Sprintf("%d/%d", order[0], order[1])})
			}
		}
	}
	for _, vector := range []struct {
		pair   string
		winner string
	}{{"0/1", "uid-1"}, {"1/2", "uid-2"}, {"2/3", ""}} {
		t.Run(vector.pair, func(t *testing.T) {
			var values []int
			for _, sequence := range sequences {
				if sequence.pair == vector.pair {
					values = sequence.values
					break
				}
			}
			index := 0
			resolver.RandomIndex = func(int) int { v := values[index]; index++; return v }
			choice, err := resolver.sampleRuntimePlacement(context.Background(), client, RuntimeJob{WorkspaceID: "default", SessionID: "vectors"})
			if vector.winner == "" {
				if err == nil || len(choice.Observations) != 2 || choice.Observations[0].Reason != "capacity_excluded" || choice.Observations[1].Reason != "capacity_excluded" {
					t.Fatalf("C/D exclusion=%+v/%v", choice, err)
				}
			} else if err != nil || choice.Candidate.PodUID != vector.winner {
				t.Fatalf("actual vector selected=%+v/%v", choice, err)
			}
			for _, observation := range choice.Observations {
				if observation.Report.Capacity != 16 || observation.Report.MemoryLimit != 1000 {
					t.Fatalf("validated vector observations lost: %+v", observation)
				}
			}
		})
	}
	mu.Lock()
	for host := range bodies {
		bodies[host] = placementVectorBody(0, 100)
	}
	mu.Unlock()
	pairs := map[string]int{}
	selected := map[string]int{}
	for _, sequence := range sequences {
		for tie := 0; tie < 2; tie++ {
			values := append(append([]int{}, sequence.values...), tie)
			index := 0
			resolver.RandomIndex = func(bound int) int {
				v := values[index]
				index++
				if v < 0 || v >= bound {
					t.Fatal("invalid deterministic draw")
				}
				return v
			}
			mu.Lock()
			calls = map[string]int{}
			mu.Unlock()
			choice, err := resolver.sampleRuntimePlacement(context.Background(), client, RuntimeJob{WorkspaceID: "default", SessionID: "draws"})
			if err != nil {
				t.Fatal(err)
			}
			a, b := choice.Observations[0].Candidate.PodUID, choice.Observations[1].Candidate.PodUID
			pairs[a+"/"+b]++
			selected[choice.Candidate.PodUID]++
			want := a
			if tie == 1 {
				want = b
			}
			if choice.Candidate.PodUID != want {
				t.Fatalf("tie=%d chose %s want%s", tie, choice.Candidate.PodUID, want)
			}
			mu.Lock()
			if len(calls) != 2 {
				t.Fatalf("distinct finite draw probes=%v", calls)
			}
			for _, count := range calls {
				if count != 1 {
					t.Fatal("sampled with replacement")
				}
			}
			mu.Unlock()
		}
	}
	if len(pairs) != 12 || len(selected) != 4 {
		t.Fatalf("fixed-first bias pairs=%v selected=%v", pairs, selected)
	}
	for pair, count := range pairs {
		if count != 4 {
			t.Fatalf("orderedpair%s count=%d", pair, count)
		}
	}
	for pod, count := range selected {
		if count != 12 {
			t.Fatalf("finite uniform choice%s count=%d", pod, count)
		}
	}
}

// Actual ActivateRuntimeRecovery uses a real Queue capability and production
// placement assembly to feed the process sink and metric collector. Parse the process sink rather than raw slog
// records so stripped fields, unsafe strings and exhausted summaries are caught.
func TestPostgreSQLRuntimePlacementDiagnosticReasons(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	candidates := placementEvidenceCandidates(t, client)
	for _, scenario := range []string{"selected", "no_candidates", "invalid_metrics", "capacity_excluded", "timeout", "http_error"} {
		t.Run(scenario, func(t *testing.T) {
			httpClient := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch scenario {
				case "invalid_metrics":
					_, _ = io.WriteString(w, "provider payload PRIVATE_SENTINEL")
				case "capacity_excluded":
					_, _ = io.WriteString(w, placementVectorBody(16, 100))
				case "timeout":
					<-r.Context().Done()
				case "http_error":
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					_, _ = io.WriteString(w, placementVectorBody(0, 100))
				}
			}))
			metrics := &RuntimePlacementMetrics{}
			policy := DefaultRuntimePlacementPolicy()
			policy.ProbeTimeout = 30 * time.Millisecond
			policy.ProbeBudget = 100 * time.Millisecond
			resolver := KubernetesRuntimeTargetResolver{PlacementMetrics: metrics, PlacementPolicy: policy, LoadClient: httpClient, RandomIndex: func(n int) int { return n - 1 }, Snapshot: func() kubernetes.BindingVisibilitySnapshot {
				inventory := candidates
				if scenario == "no_candidates" {
					inventory = nil
				}
				return kubernetes.NewBindingVisibilitySnapshotForTest(true, inventory)
			}}
			sessionID := "diagnostics-" + scenario
			threadID := "thread-" + scenario
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "source-"+scenario, 1, "session.status_rescheduled", "{}")
			if _, e := admin.Exec(`INSERT INTO session_runtime_status(workspace_id,session_id,status,created_at,updated_at) VALUES('default',$1,'idle',clock_timestamp(),clock_timestamp())`, sessionID); e != nil {
				t.Fatal(e)
			}
			enqueue, e := queue.NewRuntimeRecoveryEnqueueRequest("default", sessionID, threadID, "source-"+scenario, time.Now())
			if e != nil {
				t.Fatal(e)
			}
			q := queue.NewPostgreSQLStore(client)
			if _, e := q.Enqueue(context.Background(), enqueue); e != nil {
				t.Fatal(e)
			}
			leased, e := q.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: "default", Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "diagnostic-runner", MaxJobs: 1, LeaseDuration: time.Minute})
			if e != nil || len(leased) != 1 {
				t.Fatalf("actual Queue capability=%v/%v", leased, e)
			}
			job, e := DecodeRuntimeJob(queueJobProto(leased[0]))
			if e != nil {
				t.Fatal(e)
			}
			var sink bytes.Buffer
			owner := workload.NewProcessLogger(&sink, "job-runner", "test", "unit", workload.DefaultDiagnosticConfig())
			defer owner.CloseWithBudget()
			store := NewPostgreSQLRuntimeDeliveryStore(client, 19090)
			store.Logger = owner.Logger
			store.TargetResolver = resolver
			plan, err := store.ActivateRuntimeRecovery(context.Background(), job)
			if plan.placement == nil {
				t.Fatalf("actual placement attempt observations missing:%+v/%v", plan, err)
			}
			choice := *plan.placement
			owner.CloseWithBudget()
			var record map[string]any
			decoder := json.NewDecoder(&sink)
			if decodeErr := decoder.Decode(&record); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if decoder.More() {
				t.Fatal("placement emitted duplicate attempt records")
			}
			if record["event"] != "runtime_placement" || strings.Contains(sink.String(), "PRIVATE_SENTINEL") {
				t.Fatalf("unsafe placement sink=%v", record)
			}
			wantCount := 4
			if scenario == "no_candidates" {
				wantCount = 0
			}
			if scenario == "selected" {
				wantCount = 2
			}
			if len(choice.Observations) != wantCount {
				t.Fatalf("observation bounds=%+v", choice)
			}
			for index, observation := range choice.Observations {
				prefix := fmt.Sprintf("runtime.placement.candidate.%d.", index+1)
				reason := scenario
				if scenario == "selected" {
					reason = "eligible"
				}
				if record[prefix+"reason"] != reason || record[prefix+"kubernetes.uid"] != observation.Candidate.PodUID || record[prefix+"runtime.process.id"] != observation.ProcessID {
					t.Fatalf("safe rejected/winning observation lost:%v", record)
				}
				if scenario == "selected" || scenario == "capacity_excluded" {
					for key, value := range map[string]float64{"runtime.load.active_sessions": observation.Report.ActiveSessions, "runtime.load.session_capacity": 16, "runtime.load.memory_ratio": .1} {
						if record[prefix+key] != value {
							t.Fatalf("valid excluded/ranked load lost%s=%v", prefix+key, record[prefix+key])
						}
					}
				}
			}
			if scenario == "selected" && (record["outcome"] != "committed" || record["kubernetes.uid"] != record["runtime.placement.sampled_pod_uid"]) {
				t.Fatal("actual chosen/binding decision lost")
			}
			if scenario == "no_candidates" && record["error.code"] != "runtime_placement_no_candidates" {
				t.Fatalf("no inventory reason lost:%v", record)
			}
			samples, e := metrics.Collector()(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			var reasons float64
			for _, sample := range samples {
				for _, label := range sample.Labels {
					if label.Name != "outcome" || strings.Contains(label.Value, "uid-") {
						t.Fatalf("unbounded metric label=%+v", sample)
					}
				}
				if sample.Name == "runtime_placement_probe_total" && len(sample.Labels) == 1 {
					reason := scenario
					if scenario == "selected" {
						reason = "eligible"
					}
					if sample.Labels[0].Value == reason {
						reasons += sample.Value
					}
				}
			}
			if reasons != float64(wantCount) {
				t.Fatalf("distinguishing finite metric reason=%v want%d", reasons, wantCount)
			}
		})
	}
}
