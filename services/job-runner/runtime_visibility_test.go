package jobrunner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestRuntimeProcessVisibility(t *testing.T) {
	now := time.Now()
	fresh := runtimecontrol.Process{Current: true, Phase: runtimecontrol.ProcessAccepting, ReportedAt: sql.NullTime{Valid: true, Time: now}}
	expired := fresh
	expired.ReportedAt.Time = now.Add(-11 * time.Second)
	draining := fresh
	draining.Phase = runtimecontrol.ProcessDraining
	retired := expired
	retired.Current = false
	retired.RetiredAt = sql.NullTime{Valid: true, Time: now}
	for _, tc := range []struct {
		name                      string
		ready, found, replacement bool
		visibility                kubernetes.BindingVisibilityState
		process                   runtimecontrol.Process
		observation               *runtimePodObservation
		want                      runtimeProcessDecision
	}{
		{"startup absent", true, false, false, kubernetes.BindingVisibilityAbsent, fresh, nil, runtimeProcessUnavailable},
		{"watcher unsynchronized", false, true, false, kubernetes.BindingVisibilityAbsent, expired, &runtimePodObservation{Absent: true}, runtimeProcessUnavailable},
		{"accepting ready ignores heartbeat expiry", true, true, false, kubernetes.BindingVisibilityReusable, expired, nil, runtimeProcessReuse},
		{"fresh draining retains settlement", true, true, false, kubernetes.BindingVisibilityTerminating, draining, &runtimePodObservation{}, runtimeProcessUnavailable},
		{"fresh unready unavailable", true, true, false, kubernetes.BindingVisibilityNotReady, fresh, &runtimePodObservation{}, runtimeProcessUnavailable},
		{"stale unready needs GET", true, true, false, kubernetes.BindingVisibilityNotReady, expired, nil, runtimeProcessConfirm},
		{"stale deleting needs GET", true, true, false, kubernetes.BindingVisibilityDeleted, expired, nil, runtimeProcessConfirm},
		{"cached absent needs GET", true, true, false, kubernetes.BindingVisibilityAbsent, fresh, nil, runtimeProcessConfirm},
		{"fresh GET absent defeats old heartbeat", true, true, false, kubernetes.BindingVisibilityAbsent, fresh, &runtimePodObservation{Absent: true}, runtimeProcessLoss},
		{"fresh GET replacement", true, true, false, kubernetes.BindingVisibilityUIDChanged, fresh, &runtimePodObservation{Replaced: true}, runtimeProcessLoss},
		{"stale confirmed unready", true, true, false, kubernetes.BindingVisibilityNotReady, expired, &runtimePodObservation{}, runtimeProcessLoss},
		{"fresh GET reusable cancels expiry repair", true, true, false, kubernetes.BindingVisibilityNotReady, expired, &runtimePodObservation{Reusable: true}, runtimeProcessUnavailable},
		{"same Pod promoted replacement", true, true, true, kubernetes.BindingVisibilityReusable, retired, nil, runtimeProcessLoss},
		{"no confirmed replacement", true, true, false, kubernetes.BindingVisibilityReusable, retired, nil, runtimeProcessUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRuntimeProcess(tc.ready, tc.visibility, tc.process, tc.found, tc.replacement, now, 10*time.Second, tc.observation); got != tc.want {
				t.Fatalf("decision=%s want=%s", got, tc.want)
			}
		})
	}
	for _, scenario := range []string{"absent", "changed UID", "same ready", "same deleting", "failure", "deadline"} {
		t.Run("GET "+scenario, func(t *testing.T) {
			binding := runtimecontrol.Binding{BindingID: "b", BindingGeneration: 1, Namespace: "ns", PodName: "pod", PodUID: "uid", PodIP: "10.0.0.1", RuntimeProcessID: "process"}
			resolver := KubernetesRuntimeTargetResolver{GetPod: func(ctx context.Context, namespace, name string) (*kubernetes.PodObservation, error) {
				if namespace != "ns" || name != "pod" {
					t.Fatal("wrong GET scope")
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
					t.Fatal("GET lacks two-second bound")
				}
				if scenario == "absent" {
					return &kubernetes.PodObservation{Absent: true}, nil
				}
				if scenario == "failure" {
					return nil, fmt.Errorf("fresh Pod access denied")
				}
				if scenario == "deadline" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				pod := &kubernetes.PodObservation{Namespace: namespace, Name: name, UID: "uid", Running: true, Ready: true, IP: "10.0.0.1"}
				if scenario == "changed UID" {
					pod.UID = "replacement"
				}
				if scenario == "same deleting" {
					pod.Deleting = true
				}
				return pod, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			fact, err := resolver.confirmRuntimePod(ctx, binding)
			if scenario == "failure" || scenario == "deadline" {
				if err == nil {
					t.Fatal("failed GET inferred death")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if fact.Absent != (scenario == "absent") || fact.Replaced != (scenario == "changed UID") || fact.Reusable != (scenario == "same ready") {
				t.Fatalf("GET fact=%+v", fact)
			}
		})
	}
}

func TestPostgreSQLRuntimeProcessVisibilityRechecksReport(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "visibility-race", "visibility-thread")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "visibility-race", "visibility-binding", 1, "visibility-pod")
	seedRuntimePodLostStatusFence(t, admin, "visibility-race", "visibility-binding", 1)
	if _, err := admin.Exec(`UPDATE runtime_processes SET reported_at=clock_timestamp()-interval '11 seconds' WHERE runtime_process_id='process_visibility-pod'`); err != nil {
		t.Fatal(err)
	}
	var receipt string
	if err := admin.QueryRow(`SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id='process_visibility-pod'`).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	resume := make(chan struct{})
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 19090)
	binding := runtimecontrol.Binding{BindingID: "visibility-binding", BindingGeneration: 1, Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "visibility-pod", PodIP: "10.0.0.10", RuntimeProcessID: "process_visibility-pod"}
	store.TargetResolver = KubernetesRuntimeTargetResolver{
		Snapshot: func() kubernetes.BindingVisibilitySnapshot {
			return kubernetes.NewBindingVisibilitySnapshotStateForTest(true, kubernetes.BoundRuntimePod{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, PodIP: binding.PodIP}, kubernetes.BindingVisibilityNotReady)
		},
		GetPod: func(ctx context.Context, namespace, name string) (*kubernetes.PodObservation, error) {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &kubernetes.PodObservation{Namespace: namespace, Name: name, UID: binding.PodUID, Running: true, IP: binding.PodIP}, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type outcome struct {
		result runtimePodLossMutationResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.mutateLostRuntimeBinding(ctx, "default", "visibility-race", binding, time.Now(), true)
		done <- outcome{result, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("confirming GET did not start")
	}
	// This real independent mutation also proves the confirming GET has released
	// the previous Session/process transaction instead of holding either lock.
	if _, _, err := runtimecontrol.ReportProcess(ctx, dbconnect.NewClientForTesting(admin), runtimecontrol.ProcessIdentity{Namespace: binding.Namespace, PodUID: binding.PodUID, ID: binding.RuntimeProcessID}, receipt, runtimecontrol.ProcessAccepting); err != nil {
		t.Fatal(err)
	}
	if err := dbconnect.NewClientForTesting(runtime).WithWorkspaceTx(ctx, "default", "visibility_race.independent_session", func(tx *dbconnect.Tx) error {
		return runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, "default", "visibility-race")
	}); err != nil {
		t.Fatalf("Session arbitration: %v, cause=%v", err, errors.Unwrap(err))
	}
	close(resume)
	select {
	case got := <-done:
		if got.err != nil || got.result.status != runtimePodLossMutationStale {
			t.Fatalf("fresh report lost race: %+v/%v", got.result, got.err)
		}
	case <-ctx.Done():
		t.Fatal("loss classifier did not join")
	}
	var bindings, ends, errors int
	if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_bindings WHERE session_id='visibility-race'),(SELECT count(*) FROM session_events WHERE session_id='visibility-race' AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id='visibility-race' AND type='session.error')`).Scan(&bindings, &ends, &errors); err != nil {
		t.Fatal(err)
	}
	if bindings != 1 || ends != 0 || errors != 0 {
		t.Fatalf("late heartbeat changed custody: bindings=%d ends=%d errors=%d", bindings, ends, errors)
	}
}
