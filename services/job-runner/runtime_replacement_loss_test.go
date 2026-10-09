package jobrunner

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

func TestPostgreSQLRuntimePromotedReplacementRetainsLossAuthority(t *testing.T) {
	for _, scenario := range []struct {
		name                                                                            string
		promote, drainReplacement, absent, laterCandidate, drainOwner, abandonCandidate bool
	}{
		{name: "promoted accepting", promote: true},
		{name: "promoted then draining", promote: true, drainReplacement: true},
		{name: "promoted then draining and Pod absent", promote: true, drainReplacement: true, absent: true},
		{name: "promoted then draining with later candidate", promote: true, drainReplacement: true, laterCandidate: true},
		{name: "unpromoted candidate retains old owner"},
		{name: "abandoned candidate retains old owner", abandonCandidate: true},
		{name: "current draining owner retains custody", drainOwner: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionfixture.SeedBridgeAPISession(t, admin, "default", "replacement-session", "replacement-thread")
			seedBridgeAPIRuntimeBinding(t, admin, "default", "replacement-session", "replacement-binding", 1, "replacement-pod")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, "replacement-session", "replacement-binding", 1)
			seedBridgeAPIEvent(t, admin, "default", "replacement-session", "replacement-thread", "replacement-event", 1, "user.message", `{"type":"user.message"}`)
			sessionfixture.SeedBridgeAPIRuntimeInbox(t, admin, "default", "replacement-session", "replacement-thread", "replacement-input", "messages", `["replacement-event"]`, "accepted", "replacement-binding", "replacement-pod", 1, 1)
			request, err := runtimecontrol.RuntimeInputEnqueueRequest("default", "replacement-session", runtimecontrol.AcceptedRuntimeInput{
				SessionThreadID: "replacement-thread", RuntimeInputID: "replacement-input", InputKind: "messages", EventIDsJSON: `["replacement-event"]`, SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true},
			}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			request.ID = "replacement-job"
			if _, err := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin)).Enqueue(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			installed := storagetest.OpenWorkloadDB(t, admin, "job_runner")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			registry := dbconnect.NewClientForTesting(admin)
			replacement := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: "replacement-pod", ID: "replacement-p2"}
			candidate, err := runtimecontrol.RegisterProcess(ctx, registry, replacement)
			if err != nil {
				t.Fatal(err)
			}
			report := func(identity runtimecontrol.ProcessIdentity, receipt, phase string) {
				t.Helper()
				if _, _, err := runtimecontrol.ReportProcess(ctx, registry, identity, receipt, phase); err != nil {
					t.Fatal(err)
				}
			}
			if scenario.promote {
				report(replacement, candidate.RegistrationReceipt, runtimecontrol.ProcessAccepting)
			}
			if scenario.drainReplacement || scenario.abandonCandidate {
				report(replacement, candidate.RegistrationReceipt, runtimecontrol.ProcessDraining)
			}
			if scenario.laterCandidate {
				later := replacement
				later.ID = "replacement-p3"
				if _, err := runtimecontrol.RegisterProcess(ctx, registry, later); err != nil {
					t.Fatal(err)
				}
			}
			binding := runtimecontrol.Binding{BindingID: "replacement-binding", BindingGeneration: 1, Namespace: replacement.Namespace, PodName: "runtime-pod-0", PodUID: replacement.PodUID, PodIP: "10.0.0.10", RuntimeProcessID: "process_replacement-pod"}
			if scenario.drainOwner {
				var receipt string
				if err := admin.QueryRowContext(ctx, `SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id=$1`, binding.RuntimeProcessID).Scan(&receipt); err != nil {
					t.Fatal(err)
				}
				report(runtimecontrol.ProcessIdentity{Namespace: binding.Namespace, PodUID: binding.PodUID, ID: binding.RuntimeProcessID}, receipt, runtimecontrol.ProcessDraining)
			}
			visibility := kubernetes.BindingVisibilityReusable
			if scenario.absent {
				visibility = kubernetes.BindingVisibilityAbsent
			}
			if scenario.drainOwner {
				visibility = kubernetes.BindingVisibilityNotReady
			}
			getCalls := 0
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(installed.DB), 19090, KubernetesRuntimeTargetResolver{
				Snapshot: func() kubernetes.BindingVisibilitySnapshot {
					return kubernetes.NewBindingVisibilitySnapshotStateForTest(true, kubernetes.BoundRuntimePod{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, PodIP: binding.PodIP}, visibility)
				},
				GetPod: func(context.Context, string, string) (*kubernetes.PodObservation, error) {
					getCalls++
					return &kubernetes.PodObservation{Namespace: binding.Namespace, Name: binding.PodName, UID: binding.PodUID, IP: binding.PodIP, Running: true, Absent: scenario.absent}, nil
				},
			})
			first, err := store.mutateLostRuntimeBinding(ctx, "default", "replacement-session", binding, time.Now(), true)
			if err != nil {
				t.Fatal(err)
			}
			want := runtimePodLossMutationStale
			if scenario.promote {
				want = runtimePodLossMutationRepaired
			}
			if first.status != want {
				t.Fatalf("recovery=%s want=%s", first.status, want)
			}
			type effects struct {
				bindings, statusBindings, events, jobs int
				inbox, job                             string
			}
			read := func() effects {
				t.Helper()
				var state effects
				if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_runtime_bindings WHERE session_id='replacement-session'),(SELECT count(*) FROM session_runtime_status WHERE session_id='replacement-session' AND binding_id IS NOT NULL),(SELECT count(*) FROM session_events WHERE session_id='replacement-session'),(SELECT count(*) FROM queue_jobs)`).Scan(&state.bindings, &state.statusBindings, &state.events, &state.jobs); err != nil {
					t.Fatal(err)
				}
				if err := admin.QueryRowContext(ctx, `SELECT (SELECT status FROM session_runtime_inbox WHERE runtime_input_id='replacement-input'),(SELECT status FROM queue_jobs WHERE id='replacement-job')`).Scan(&state.inbox, &state.job); err != nil {
					t.Fatal(err)
				}
				return state
			}
			after := read()
			wantInbox := "accepted"
			if scenario.promote {
				wantInbox = "queued"
			}
			if after.inbox != wantInbox || after.job != queue.StatusPending || after.jobs != 1 {
				t.Fatalf("input/Queue custody=%+v want %s/pending with original one job", after, wantInbox)
			}
			wantBindings := 1
			if scenario.promote {
				wantBindings = 0
			}
			if after.bindings != wantBindings || after.statusBindings != wantBindings {
				t.Fatalf("durable custody=%+v want bindings=%d", after, wantBindings)
			}
			if scenario.promote && getCalls != 0 {
				t.Fatalf("durable promotion required %d external observations", getCalls)
			}
			again, err := store.mutateLostRuntimeBinding(ctx, "default", "replacement-session", binding, time.Now(), true)
			if err != nil || again.status != runtimePodLossMutationStale {
				t.Fatalf("replay=%s/%v", again.status, err)
			}
			if got := read(); got != after {
				t.Fatalf("replay duplicated effects: before=%+v after=%+v", after, got)
			}
			t.Logf("installed Runner role recovery=%s bindings=%d effects stable on replay; fresh GETs=%d", first.status, after.bindings, getCalls)
		})
	}
}
