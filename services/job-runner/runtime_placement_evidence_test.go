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
	"sync/atomic"
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

// Actual recovery activation and mail/task preparation use real Queue
// capabilities and production placement assembly. Parse the process sink rather
// than raw slog records so stripped identities, unsafe strings and incorrectly
// classified committed/reused outcomes are caught.
func TestPostgreSQLRuntimePlacementDiagnosticReasons(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	candidates := placementEvidenceCandidates(t, client)
	for _, scenario := range []string{"selected", "no_candidates", "invalid_metrics", "capacity_excluded", "timeout", "http_error"} {
		t.Run(scenario, func(t *testing.T) {
			wantCount := 4
			if scenario == "no_candidates" {
				wantCount = 0
			}
			if scenario == "selected" {
				wantCount = 2
			}
			entered, exited := make(chan string, 4), make(chan string, 4)
			gates := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var releaseOnce [2]sync.Once
			release := func(round int) { releaseOnce[round].Do(func() { close(gates[round]) }) }
			var arrivals atomic.Int32
			httpClient := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				index := int(arrivals.Add(1)) - 1
				entered <- r.Host
				defer func() { exited <- r.Host }()
				if scenario == "timeout" {
					// The failure is an admitted native HTTP probe, never an
					// incidental expiry while establishing its registry proof.
					<-r.Context().Done()
					return
				}
				if index >= 4 {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				select {
				case <-gates[index/2]:
				case <-r.Context().Done():
					return
				}
				switch scenario {
				case "invalid_metrics":
					_, _ = io.WriteString(w, "provider payload PRIVATE_SENTINEL")
				case "capacity_excluded":
					_, _ = io.WriteString(w, placementVectorBody(16, 100))
				case "http_error":
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					_, _ = io.WriteString(w, placementVectorBody(0, 100))
				}
			}))
			t.Cleanup(func() { release(0); release(1) })
			metrics := &RuntimePlacementMetrics{}
			// The production policy bounds both registry lookup and HTTP.
			// Response gates model each reason after actual probe entry;
			// scheduler latency is not a synthetic 30ms eligibility oracle.
			policy := DefaultRuntimePlacementPolicy()
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
			store := NewPostgreSQLRuntimeDeliveryStore(client, 19090, resolver)
			store.Logger = owner.Logger
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var plan RuntimeCommandPlan
			var err error
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				plan, err = store.ActivateRuntimeRecovery(ctx, job)
			}()
			defer func() {
				cancel()
				release(0)
				release(1)
				select {
				case <-joined:
				case <-time.After(3 * time.Second):
					t.Error("actual placement did not join")
				}
			}()
			hosts := map[string]bool{}
			for index := 0; index < wantCount; index++ {
				select {
				case host := <-entered:
					if hosts[host] {
						t.Fatalf("actual HTTP probe repeated candidate %s", host)
					}
					hosts[host] = true
					if index%2 == 1 && scenario != "timeout" {
						release(index / 2)
					}
				case <-ctx.Done():
					t.Fatalf("native HTTP entry %d/%d: %v", index, wantCount, ctx.Err())
				}
			}
			select {
			case <-joined:
			case <-ctx.Done():
				t.Fatalf("actual placement did not finish: %v", ctx.Err())
			}
			for index := 0; index < wantCount; index++ {
				select {
				case <-exited:
				case <-ctx.Done():
					t.Fatalf("native HTTP handler %d/%d did not join", index, wantCount)
				}
			}
			if got := int(arrivals.Load()); got != wantCount {
				t.Fatalf("actual HTTP requests=%d; want %d", got, wantCount)
			}
			if plan.placement == nil {
				t.Fatalf("actual placement attempt observations missing:%+v/%v", plan, err)
			}
			choice := *plan.placement
			owner.CloseWithBudget()
			var record map[string]any
			output := sink.String()
			decoder := json.NewDecoder(strings.NewReader(output))
			if decodeErr := decoder.Decode(&record); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if decoder.More() {
				t.Fatal("placement emitted duplicate attempt records")
			}
			if record["event"] != "runtime_placement" || strings.Contains(output, "PRIVATE_SENTINEL") {
				t.Fatalf("unsafe placement sink=%v", record)
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
				if record[prefix+"reason"] != reason || record[prefix+"kubernetes.uid"] != observation.Candidate.PodUID || record[prefix+"runtime.process.id"] != observation.ProcessID || observation.ProcessID != "process_"+observation.Candidate.PodUID {
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
	for _, inputKind := range []string{"agent_mail", "task_notification"} {
		for _, bindingState := range []string{"fresh_sample", "existing_reuse"} {
			t.Run(inputKind+"/"+bindingState, func(t *testing.T) {
				ctx := context.Background()
				kindLabel, stateLabel := "mail", "fresh"
				if inputKind == "task_notification" {
					kindLabel = "task"
				}
				if bindingState == "existing_reuse" {
					stateLabel = "reuse"
				}
				sessionID := "sesn_diag_" + kindLabel + "_" + stateLabel
				threadID := "thread-" + sessionID
				seedBridgeAPISession(t, admin, "default", sessionID, threadID)
				candidate := candidates[0]
				if bindingState == "existing_reuse" {
					seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "binding-"+sessionID, 1, candidate.PodUID)
					candidate.PodName, candidate.PodIP = "runtime-pod-0", "10.0.0.10"
				}
				q := queue.NewPostgreSQLStore(client)
				if inputKind == "agent_mail" {
					childID := "child-" + sessionID
					seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, childID)
					deliveryID := "delivery-" + sessionID
					messageJSON := bridgePublicMessageJSONForTest(t, completionMailEnvelope("main", "task_"+childID, "PRIVATE_SENTINEL"))
					seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "sent-"+sessionID, 1, "agent.thread_message_sent",
						bridgeInterAgentSentEventJSON(t, deliveryID, childID, threadID, "", "source-"+sessionID, messageJSON))
					seedAgentMailCustody(t, admin, sessionID, threadID, deliveryID, time.Now())
				} else {
					taskID := "task-" + sessionID
					seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, "", taskID, "source-"+sessionID)
					settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", `{"status":"completed","stdout":{"text":"PRIVATE_SENTINEL","truncated":false},"stderr":{"text":"","truncated":false}}`)
					if _, err := admin.ExecContext(ctx, `INSERT INTO session_runtime_inbox (
						workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at
					) VALUES ('default',$1,$2,$3,'task_notification','[]','queued',clock_timestamp(),clock_timestamp())`, sessionID, threadID, "task_notification:"+taskID); err != nil {
						t.Fatal(err)
					}
					enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest("default", sessionID, threadID, taskID, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					if _, err := q.Enqueue(ctx, enqueue); err != nil {
						t.Fatal(err)
					}
				}
				leased, err := q.Lease(ctx, queue.LeaseRequest{WorkspaceID: "default", Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "input-diagnostic-runner", MaxJobs: 1, LeaseDuration: time.Minute})
				if err != nil || len(leased) != 1 {
					t.Fatalf("actual input Queue capability=%v/%v", leased, err)
				}
				job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
				if err != nil || job.InputKind != inputKind || job.SessionID != sessionID {
					t.Fatalf("actual leased input=%+v/%v", job, err)
				}
				var probes atomic.Int32
				httpClient := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					probes.Add(1)
					_, _ = io.WriteString(w, placementVectorBody(0, 100))
				}))
				var sink bytes.Buffer
				owner := workload.NewProcessLogger(&sink, "job-runner", "test", "unit", workload.DefaultDiagnosticConfig())
				defer owner.CloseWithBudget()
				store := NewPostgreSQLRuntimeDeliveryStore(client, 19090, KubernetesRuntimeTargetResolver{LoadClient: httpClient, PlacementPolicy: DefaultRuntimePlacementPolicy(), Snapshot: func() kubernetes.BindingVisibilitySnapshot {
					return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{candidate})
				}})
				store.Logger = owner.Logger
				plan, err := store.PrepareRuntimeCommand(ctx, job)
				if err != nil || !plan.hasCommand() {
					t.Fatalf("actual %s preparation=%+v/%v", inputKind, plan, err)
				}
				var persisted runtimecontrol.Binding
				if err := admin.QueryRowContext(ctx, `SELECT binding_id,binding_generation,agent_runtime_namespace,
					agent_runtime_pod_name,agent_runtime_pod_uid,agent_runtime_pod_ip,runtime_process_id
					FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(
					&persisted.BindingID, &persisted.BindingGeneration, &persisted.Namespace, &persisted.PodName, &persisted.PodUID, &persisted.PodIP, &persisted.RuntimeProcessID); err != nil {
					t.Fatal(err)
				}
				wantProcessID := "process_" + candidate.PodUID
				if persisted.RuntimeProcessID != wantProcessID || persisted.PodUID != candidate.PodUID {
					t.Fatalf("persisted binding=%+v; want actual accepting candidate", persisted)
				}
				wantTarget := RuntimePodTarget{Namespace: persisted.Namespace, PodName: persisted.PodName, PodUID: persisted.PodUID, PodIP: persisted.PodIP, RuntimeProcessID: persisted.RuntimeProcessID, Port: 19090}
				wantAttempt := RuntimeAttemptedBinding{BindingID: persisted.BindingID, Generation: persisted.BindingGeneration, TargetPodUID: persisted.PodUID, RuntimeProcessID: persisted.RuntimeProcessID}
				if plan.Target != wantTarget || plan.AttemptedBinding != wantAttempt {
					t.Errorf("prepared target/attempt=%+v/%+v; want persisted tuple %+v/%+v", plan.Target, plan.AttemptedBinding, wantTarget, wantAttempt)
				}
				var rpcBindingID, rpcPodUID, rpcProcessID string
				var rpcGeneration int64
				if inputKind == "agent_mail" {
					if plan.AcceptAgentMail == nil || plan.AcceptTask != nil {
						t.Fatalf("wrong actual mail command=%+v", plan)
					}
					rpcBindingID, rpcGeneration, rpcPodUID, rpcProcessID = plan.AcceptAgentMail.GetBindingId(), plan.AcceptAgentMail.GetBindingGeneration(), plan.AcceptAgentMail.GetTargetPodUid(), plan.AcceptAgentMail.GetRuntimeProcessId()
				} else {
					if plan.AcceptTask == nil || plan.AcceptAgentMail != nil {
						t.Fatalf("wrong actual task command=%+v", plan)
					}
					rpcBindingID, rpcGeneration, rpcPodUID, rpcProcessID = plan.AcceptTask.GetBindingId(), plan.AcceptTask.GetBindingGeneration(), plan.AcceptTask.GetTargetPodUid(), plan.AcceptTask.GetRuntimeProcessId()
				}
				if rpcBindingID != persisted.BindingID || rpcGeneration != persisted.BindingGeneration || rpcPodUID != persisted.PodUID || rpcProcessID != persisted.RuntimeProcessID {
					t.Errorf("actual RPC identity=%s/%d/%s/%s; want persisted %+v", rpcBindingID, rpcGeneration, rpcPodUID, rpcProcessID, persisted)
				}
				owner.CloseWithBudget()
				output := sink.String()
				decoder := json.NewDecoder(strings.NewReader(output))
				var record map[string]any
				if err := decoder.Decode(&record); err != nil {
					t.Fatal(err)
				}
				var extra map[string]any
				if err := decoder.Decode(&extra); err != io.EOF {
					t.Fatalf("expected one actual placement record: extra=%v/%v", extra, err)
				}
				for key, want := range map[string]any{"event": "runtime_placement", "workspace.id": "default", "session.id": sessionID, "job.id": job.JobID, "kubernetes.uid": persisted.PodUID, "runtime.process.id": persisted.RuntimeProcessID} {
					if record[key] != want {
						t.Errorf("actual safe sink %s=%v; want %v: %v", key, record[key], want, record)
					}
				}
				if strings.Contains(output, "PRIVATE_SENTINEL") {
					t.Fatal("placement sink leaked input payload")
				}
				if bindingState == "existing_reuse" {
					if probes.Load() != 0 || plan.placement != nil || record["outcome"] != "reused" || record["runtime.placement.sampled_pod_uid"] != nil || persisted.BindingID != "binding-"+sessionID || persisted.BindingGeneration != 1 {
						t.Errorf("bound input must reuse original identity without sampling: probes=%d plan=%+v record=%v", probes.Load(), plan, record)
					}
				} else {
					if probes.Load() != 1 || plan.placement == nil {
						t.Fatalf("fresh input must sample actual candidate once: probes=%d plan=%+v", probes.Load(), plan)
					}
					if plan.placement.ProcessID != persisted.RuntimeProcessID || plan.placement.Candidate.PodUID != persisted.PodUID || record["outcome"] != "committed" || record["runtime.placement.sampled_pod_uid"] != persisted.PodUID || record["runtime.placement.candidate.1.runtime.process.id"] != persisted.RuntimeProcessID || record["runtime.placement.candidate.1.reason"] != "eligible" {
						t.Errorf("fresh input must report sampled=committed winner: choice=%+v record=%v", plan.placement, record)
					}
				}
			})
		}
	}
}
