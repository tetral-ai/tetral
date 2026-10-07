package jobrunner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

func TestPostgreSQLRuntimePlacementP2C(t *testing.T) {
	for _, scenario := range []string{"lower load", "one invalid", "second round", "all invalid", "caller cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionfixture.SeedBridgeAPISession(t, admin, "default", "session_placement", "thread_placement")
			client := dbconnect.NewClientForTesting(runtime)
			var candidates []kubernetes.BindingCandidate
			for i := 1; i <= 5; i++ {
				candidate := kubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: fmt.Sprintf("pod-%d", i), PodUID: fmt.Sprintf("uid-%d", i), PodIP: fmt.Sprintf("10.0.0.%d", i)}
				identity := runtimecontrol.ProcessIdentity{Namespace: candidate.Namespace, PodUID: candidate.PodUID, ID: "process_" + candidate.PodUID}
				registration, err := runtimecontrol.RegisterProcess(context.Background(), client, identity)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = runtimecontrol.ReportProcess(context.Background(), client, identity, registration.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
					t.Fatal(err)
				}
				candidates = append(candidates, candidate)
			}
			var mu sync.Mutex
			calls := map[string]int{}
			started := make(chan struct{}, 4)
			httpClient := runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				calls[r.Host]++
				mu.Unlock()
				started <- struct{}{}
				if scenario == "caller cancellation" {
					<-r.Context().Done()
					return
				}
				if scenario == "all invalid" || (scenario == "second round" && (r.Host == "10.0.0.1:8080" || r.Host == "10.0.0.2:8080")) || (scenario == "one invalid" && r.Host == "10.0.0.1:8080") {
					w.WriteHeader(503)
					return
				}
				load := 5
				if r.Host == "10.0.0.2:8080" {
					load = 0
				}
				_, _ = io.WriteString(w, runtimeLoadFixture(load))
			}))
			metrics := &RuntimePlacementMetrics{}
			resolver := KubernetesRuntimeTargetResolver{PlacementMetrics: metrics, Snapshot: func() kubernetes.BindingVisibilitySnapshot {
				return kubernetes.NewBindingVisibilitySnapshotForTest(true, append(candidates, candidates[0]))
			}, LoadClient: httpClient, RandomIndex: func(n int) int { return n - 1 }}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			job := RuntimeJob{WorkspaceID: "default", SessionID: "session_placement"}
			if scenario == "caller cancellation" {
				done := make(chan error, 1)
				go func() { _, err := resolver.sampleRuntimePlacement(ctx, client, job); done <- err }()
				for i := 0; i < 2; i++ {
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal("parallel probes did not start")
					}
				}
				cancel()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("cancelled placement succeeded")
					}
				case <-time.After(time.Second):
					t.Fatal("placement failed to join cancelled probes")
				}
			} else {
				choice, err := resolver.sampleRuntimePlacement(ctx, client, job)
				if scenario == "all invalid" {
					if err == nil {
						t.Fatal("all invalid candidates admitted")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if scenario != "second round" && choice.Candidate.PodIP != "10.0.0.2" {
						t.Fatalf("selected higher/invalid load: %+v", choice)
					}
					wantRounds := 1
					if scenario == "second round" {
						wantRounds = 2
					}
					if choice.Rounds != wantRounds || choice.Probes != wantRounds*2 {
						t.Fatalf("sample bounds=%+v", choice)
					}
				}
			}
			mu.Lock()
			defer mu.Unlock()
			samples, metricsErr := metrics.Collector()(context.Background())
			if metricsErr != nil {
				t.Fatal(metricsErr)
			}
			var measuredProbes, measuredAttempts float64
			for _, sample := range samples {
				if sample.Name == "runtime_placement_probe_total" {
					measuredProbes += sample.Value
				}
				if sample.Name == "runtime_placement_total" {
					measuredAttempts += sample.Value
				}
			}
			if measuredProbes != float64(len(calls)) || measuredAttempts != 1 {
				t.Fatalf("actual joined metric census probes=%v attempts=%v calls=%v", measuredProbes, measuredAttempts, calls)
			}
			if len(calls) > 4 {
				t.Fatalf("bounded sampling probed %d Pods", len(calls))
			}
			for pod, count := range calls {
				if count != 1 {
					t.Errorf("Pod %s probed %d times", pod, count)
				}
			}
			if scenario == "all invalid" && len(calls) != 4 {
				t.Fatalf("all invalid sample count=%d", len(calls))
			}
		})
	}
}
