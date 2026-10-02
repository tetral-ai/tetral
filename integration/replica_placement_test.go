package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestPostgreSQLReplicaPlacementBinding(t *testing.T) {
	for _, scenario := range []string{"existing binding has zero probes", "temporary unready has no replacement", "concurrent binders converge", "sample changes before commit", "caller cancels before commit", "proven loss before replacement", "different binding wins during sample"} {
		t.Run(scenario, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			seedBridgeAPISession(t, admin, "default", "placement-session", "placement-thread")
			seedBridgeAPIEvent(t, admin, "default", "placement-session", "placement-thread", "placement-source", 1, "session.status_rescheduled", "{}")
			if _, err := admin.ExecContext(ctx, `INSERT INTO session_runtime_status(workspace_id,session_id,status,created_at,updated_at) VALUES('default','placement-session','idle',clock_timestamp(),clock_timestamp())`); err != nil {
				t.Fatal(err)
			}
			candidate := kubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "placement-pod", PodIP: "10.0.0.10"}
			seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), candidate.Namespace, candidate.PodUID)
			hasBinding := scenario == "existing binding has zero probes" || scenario == "temporary unready has no replacement" || scenario == "proven loss before replacement"
			if hasBinding {
				pod := candidate.PodUID
				if scenario == "proven loss before replacement" {
					pod = "old-placement-pod"
				}
				seedBridgeAPIRuntimeBinding(t, admin, "default", "placement-session", "original-placement-binding", 1, pod)
				if _, err := admin.ExecContext(ctx, `UPDATE session_runtime_status SET binding_id='original-placement-binding',binding_generation=1 WHERE session_id='placement-session'`); err != nil {
					t.Fatal(err)
				}
			}
			enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest("default", "placement-session", "placement-thread", "placement-source", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
			if _, err := queueStore.Enqueue(ctx, enqueue); err != nil {
				t.Fatal(err)
			}
			leased, err := queueStore.Lease(ctx, queue.LeaseRequest{WorkspaceID: "default", Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "placement-fixture", MaxJobs: 1, LeaseDuration: time.Minute})
			if err != nil || len(leased) != 1 {
				t.Fatalf("actual Queue lease=%v/%v", leased, err)
			}
			job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
			if err != nil {
				t.Fatal(err)
			}
			// Two independent pools authenticate as the production Runner role. The
			// sampler can read/lock process facts but cannot grant itself mutation rights.
			role := storagetest.OpenWorkloadDB(t, admin, "job_runner")
			var stores [2]*jobrunner.PostgreSQLRuntimeDeliveryStore
			var sinks [2]bytes.Buffer
			var logOwners [2]*workload.ProcessLogger
			winner := kubernetes.BindingCandidate{Namespace: candidate.Namespace, PodName: "runtime-winner", PodUID: "placement-winner", PodIP: "10.0.0.20"}
			if scenario == "different binding wins during sample" {
				seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), winner.Namespace, winner.PodUID)
			}
			var mu sync.Mutex
			inventory := []kubernetes.BindingCandidate{candidate}
			var probes atomic.Int32
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			held := scenario == "concurrent binders converge" || scenario == "sample changes before commit" || scenario == "caller cancels before commit" || scenario == "different binding wins during sample"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				probes.Add(1)
				if r.Method != "GET" || r.URL.Path != "/metrics" || (r.Host != "10.0.0.10:8080" && r.Host != "10.0.0.20:8080") {
					t.Errorf("wrong native metrics route: %s %s", r.Host, r.URL)
				}
				if held && r.Host != "10.0.0.20:8080" {
					entered <- struct{}{}
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				_, _ = io.WriteString(w, "runtimepod_active_sessions 0\nruntimepod_session_capacity 16\nruntimepod_container_memory_usage_bytes 100\nruntimepod_container_memory_limit_bytes 1000\nruntimepod_ready 1\nruntimepod_accepting_commands 1\n")
			}))
			t.Cleanup(server.Close)
			transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}}
			t.Cleanup(transport.CloseIdleConnections)
			for i := range stores {
				pool := storagetest.OpenRuntimeRoleDBWithTracer(t, role.DB, nil)
				stores[i] = jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(pool), 19090)
				logOwners[i] = workload.NewProcessLogger(&sinks[i], "job-runner", "test", "unit", workload.DefaultDiagnosticConfig())
				t.Cleanup(logOwners[i].CloseWithBudget)
				stores[i].Logger = logOwners[i].Logger
				index := i
				policy := jobrunner.DefaultRuntimePlacementPolicy()
				policy.ProbeTimeout = 2 * time.Second
				policy.ProbeBudget = 4 * time.Second
				stores[i].TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{PlacementPolicy: policy, LoadClient: &http.Client{Transport: transport}, Snapshot: func() kubernetes.BindingVisibilitySnapshot {
					mu.Lock()
					defer mu.Unlock()
					if scenario == "temporary unready has no replacement" {
						return kubernetes.NewBindingVisibilitySnapshotStateForTest(true, kubernetes.BoundRuntimePod(candidate), kubernetes.BindingVisibilityNotReady)
					}
					if scenario == "different binding wins during sample" && index == 1 {
						return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{winner})
					}
					return kubernetes.NewBindingVisibilitySnapshotForTest(true, inventory)
				}, GetPod: func(_ context.Context, namespace, name string) (*kubernetes.PodObservation, error) {
					if scenario == "proven loss before replacement" {
						return &kubernetes.PodObservation{Absent: true}, nil
					}
					return &kubernetes.PodObservation{Namespace: namespace, Name: name, UID: candidate.PodUID, Running: true, IP: candidate.PodIP}, nil
				}}
			}
			type result struct {
				plan jobrunner.RuntimeCommandPlan
				err  error
			}
			done := make(chan result, 2)
			activateMeasured := func(index int) (jobrunner.RuntimeCommandPlan, error) {
				// Recovery preparation only validates the Queue authority. Placement
				// completes here, after sampling and binding arbitration have returned.
				started := time.Now()
				plan, err := stores[index].ActivateRuntimeRecovery(ctx, job)
				ended := time.Now()
				outcome, errorCode := "success", ""
				if err != nil {
					outcome = "error"
					if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
						outcome = "cancelled"
					} else if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
						outcome = "deadline_exceeded"
					}
					var preparation runtimecontrol.PreparationError
					if errors.As(err, &preparation) {
						errorCode = preparation.Kind
					}
				} else if plan.StaleAccepted {
					outcome = "stale"
				} else if plan.DeliveryAuthorityLost {
					outcome = "authority_lost"
				} else if plan.RecoverThread == nil {
					outcome = "no_binding"
				}
				replicaLogCompletion(t, replicaCompletionSample{
					Cohort: "placement", Method: "ActivateRuntimeRecovery", Receiver: fmt.Sprintf("runner-%d", index+1), Outcome: outcome,
					StartBoundary: "activation_started", EndBoundary: "binding_arbitration_returned", ClockID: fmt.Sprintf("go:%d", os.Getpid()),
					StartNS: started.Sub(replicaMeasurementOrigin).Nanoseconds(), EndNS: ended.Sub(replicaMeasurementOrigin).Nanoseconds(), DurationNS: ended.Sub(started).Nanoseconds(),
					BindingID: plan.AttemptedBinding.BindingID, PodUID: plan.Target.PodUID, ProcessID: plan.Target.RuntimeProcessID, ErrorCode: errorCode,
				})
				return plan, err
			}
			activate := func(index int) {
				started := time.Now()
				prepared, err := stores[index].PrepareRuntimeCommand(ctx, job)
				outcome := "prepared"
				if err != nil {
					outcome = "error"
				}
				replicaRecordCompletion(t, "recovery_preparation", "PrepareRuntimeCommand", fmt.Sprintf("runner-%d", index+1), outcome, started)
				if err != nil || !prepared.RecoveryPrepared {
					done <- result{err: fmt.Errorf("actual recovery preparation: %+v/%v", prepared, err)}
					return
				}
				plan, err := activateMeasured(index)
				done <- result{plan, err}
			}
			go activate(0)
			attempts := 1
			if scenario == "concurrent binders converge" {
				attempts = 2
				go activate(1)
			}
			if held {
				for i := 0; i < attempts; i++ {
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("actual HTTP sampling barrier not reached")
					}
				}
				if err := dbconnect.NewClientForTesting(role.DB).WithWorkspaceTx(ctx, "default", "replica_placement.session_lock", func(tx *dbconnect.Tx) error {
					return runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, "default", "placement-session")
				}); err != nil {
					t.Fatalf("HTTP held Session transaction: %v", err)
				}
				if scenario == "different binding wins during sample" {
					started := time.Now()
					prepared, prepareErr := stores[1].PrepareRuntimeCommand(ctx, job)
					outcome := "prepared"
					if prepareErr != nil {
						outcome = "error"
					}
					replicaRecordCompletion(t, "recovery_preparation", "PrepareRuntimeCommand", "runner-2", outcome, started)
					if prepareErr != nil || !prepared.RecoveryPrepared {
						t.Fatalf("independent winner preparation=%+v/%v", prepared, prepareErr)
					}
					winnerPlan, activateErr := activateMeasured(1)
					if activateErr != nil || winnerPlan.Target.PodUID != winner.PodUID {
						t.Fatalf("independent winner activation=%+v/%v", winnerPlan, activateErr)
					}
					mu.Lock()
					inventory = append(inventory, winner)
					mu.Unlock()
				}
				if scenario == "sample changes before commit" {
					mu.Lock()
					inventory[0].PodUID = "replacement-uid"
					mu.Unlock()
				}
				if scenario == "caller cancels before commit" {
					cancel()
				}
				close(release)
			}
			var outcomes []result
			for i := 0; i < attempts; i++ {
				select {
				case got := <-done:
					outcomes = append(outcomes, got)
				case <-time.After(5 * time.Second):
					t.Fatal("placement owner did not join")
				}
			}
			failed := scenario == "temporary unready has no replacement" || scenario == "sample changes before commit" || scenario == "caller cancels before commit"
			for _, got := range outcomes {
				if failed {
					if got.err == nil {
						t.Fatalf("unsafe placement committed: %+v", got.plan)
					}
				} else if got.err != nil || got.plan.RecoverThread == nil {
					t.Fatalf("actual placement failed: %+v/%v", got.plan, got.err)
				}
			}
			var count int
			if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_bindings WHERE session_id='placement-session'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 1
			if scenario == "sample changes before commit" || scenario == "caller cancels before commit" {
				want = 0
			}
			if count != want {
				t.Fatalf("durable binding census=%d want=%d", count, want)
			}
			if hasBinding && scenario != "proven loss before replacement" {
				if probes.Load() != 0 {
					t.Fatalf("existing binding performed %d load probes", probes.Load())
				}
				var identity string
				if err := admin.QueryRow(`SELECT binding_id FROM session_runtime_bindings WHERE session_id='placement-session'`).Scan(&identity); err != nil || identity != "original-placement-binding" {
					t.Fatalf("unavailable/reused owner replaced: %s/%v", identity, err)
				}
			}
			if scenario == "concurrent binders converge" {
				a, b := outcomes[0].plan, outcomes[1].plan
				if a.AttemptedBinding != b.AttemptedBinding || a.Target != b.Target {
					t.Fatalf("concurrent binders diverged: %+v/%+v", a.AttemptedBinding, b.AttemptedBinding)
				}
			}
			if scenario == "proven loss before replacement" {
				if outcomes[0].plan.Target.PodUID != candidate.PodUID || outcomes[0].plan.AttemptedBinding.BindingID == "original-placement-binding" {
					t.Fatal("proven loss did not precede a fresh binding")
				}
			}
			for i, owner := range logOwners {
				owner.CloseWithBudget()
				decoder := json.NewDecoder(&sinks[i])
				var records []map[string]any
				for decoder.More() {
					var record map[string]any
					if err := decoder.Decode(&record); err != nil {
						t.Fatal(err)
					}
					if record["event.kind"] == "runtime_placement" {
						records = append(records, record)
					}
				}
				if i == 0 && !failed {
					if len(records) != 1 {
						t.Fatalf("safe sink placement decisions=%d", len(records))
					}
					record := records[0]
					for key, want := range map[string]any{"workspace.id": "default", "session.id": "placement-session", "job.id": job.JobID, "kubernetes.uid": outcomes[0].plan.Target.PodUID, "runtime.process.id": outcomes[0].plan.Target.RuntimeProcessID} {
						if record[key] != want {
							t.Errorf("safe placement sink %s=%v want=%v", key, record[key], want)
						}
					}
					if scenario != "existing binding has zero probes" {
						for key, want := range map[string]any{"runtime.placement.rounds": float64(1), "runtime.placement.probes": float64(1), "runtime.placement.sampled_pod_uid": candidate.PodUID, "runtime.load.active_sessions": float64(0), "runtime.load.session_capacity": float64(16), "runtime.load.memory_ratio": float64(.1)} {
							if record[key] != want {
								t.Errorf("safe placement sink lost actual sample %s=%v want=%v", key, record[key], want)
							}
						}
					}
					if scenario == "different binding wins during sample" && (record["outcome"] != "concurrent_binding_reused" || record["kubernetes.uid"] == record["runtime.placement.sampled_pod_uid"]) {
						t.Fatalf("safe sink conflated sample/winner=%v", record)
					}
				}
			}

		})
	}
}
