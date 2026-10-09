package jobrunner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

func TestRuntimeProcessVisibility(t *testing.T) {
	now := time.Now()
	accepting := runtimecontrol.Process{Current: true, Phase: runtimecontrol.ProcessAccepting}
	draining := accepting
	draining.Phase = runtimecontrol.ProcessDraining
	retired := accepting
	retired.Current = false
	retired.RetiredAt = sql.NullTime{Valid: true, Time: now}
	fresh := sql.NullTime{Valid: true, Time: now}
	expired := sql.NullTime{Valid: true, Time: now.Add(-11 * time.Second)}
	unreported := sql.NullTime{}
	for _, tc := range []struct {
		name                      string
		ready, found, replacement bool
		visibility                kubernetes.BindingVisibilityState
		process                   runtimecontrol.Process
		reportedAt                sql.NullTime
		observation               *runtimePodObservation
		want                      runtimeProcessDecision
	}{
		{"startup absent", true, false, false, kubernetes.BindingVisibilityAbsent, accepting, fresh, nil, runtimeProcessUnavailable},
		{"watcher unsynchronized", false, true, false, kubernetes.BindingVisibilityAbsent, accepting, expired, &runtimePodObservation{Absent: true}, runtimeProcessUnavailable},
		{"accepting ready ignores heartbeat expiry", true, true, false, kubernetes.BindingVisibilityReusable, accepting, expired, nil, runtimeProcessReuse},
		{"fresh draining retains settlement", true, true, false, kubernetes.BindingVisibilityTerminating, draining, fresh, &runtimePodObservation{}, runtimeProcessUnavailable},
		{"fresh unready unavailable", true, true, false, kubernetes.BindingVisibilityNotReady, accepting, fresh, &runtimePodObservation{}, runtimeProcessUnavailable},
		{"stale unready needs GET", true, true, false, kubernetes.BindingVisibilityNotReady, accepting, expired, nil, runtimeProcessConfirm},
		{"stale deleting needs GET", true, true, false, kubernetes.BindingVisibilityDeleted, accepting, expired, nil, runtimeProcessConfirm},
		{"cached absent needs GET", true, true, false, kubernetes.BindingVisibilityAbsent, accepting, fresh, nil, runtimeProcessConfirm},
		{"fresh GET absent defeats old heartbeat", true, true, false, kubernetes.BindingVisibilityAbsent, accepting, fresh, &runtimePodObservation{Absent: true}, runtimeProcessLoss},
		{"fresh GET replacement", true, true, false, kubernetes.BindingVisibilityUIDChanged, accepting, fresh, &runtimePodObservation{Replaced: true}, runtimeProcessLoss},
		{"stale confirmed unready", true, true, false, kubernetes.BindingVisibilityNotReady, accepting, expired, &runtimePodObservation{}, runtimeProcessLoss},
		{"fresh GET reusable cancels expiry repair", true, true, false, kubernetes.BindingVisibilityNotReady, accepting, expired, &runtimePodObservation{Reusable: true}, runtimeProcessUnavailable},
		{"current without report is never loss", true, true, false, kubernetes.BindingVisibilityNotReady, accepting, unreported, &runtimePodObservation{Absent: true}, runtimeProcessUnavailable},
		{"current without report is never reused", true, true, false, kubernetes.BindingVisibilityReusable, accepting, unreported, nil, runtimeProcessUnavailable},
		{"same Pod promoted replacement ignores report", true, true, true, kubernetes.BindingVisibilityReusable, retired, unreported, nil, runtimeProcessLoss},
		{"no confirmed replacement", true, true, false, kubernetes.BindingVisibilityReusable, retired, expired, nil, runtimeProcessUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRuntimeProcess(tc.ready, tc.visibility, tc.process, tc.found, tc.replacement, tc.reportedAt, now, 10*time.Second, tc.observation); got != tc.want {
				t.Fatalf("decision=%s want=%s", got, tc.want)
			}
		})
	}
	for _, scenario := range []string{"absent", "changed UID", "same ready", "same deleting", "failure", "deadline"} {
		t.Run("GET "+scenario, func(t *testing.T) {
			binding := runtimecontrol.Binding{BindingID: "b", BindingGeneration: 1, Namespace: "ns", PodName: "pod", PodUID: "uid", PodIP: "10.0.0.1", RuntimeProcessID: "process"}
			// Without a caller deadline the GET carries exactly its own two-second bound;
			// the separate deadline scenario keeps a shorter caller deadline in force.
			ctx := context.Background()
			var callerDeadline time.Time
			if scenario == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
				callerDeadline, _ = ctx.Deadline()
			}
			var getDeadline time.Time
			resolver := KubernetesRuntimeTargetResolver{GetPod: func(ctx context.Context, namespace, name string) (*kubernetes.PodObservation, error) {
				if namespace != "ns" || name != "pod" {
					t.Fatal("wrong GET scope")
				}
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatal("GET lacks a deadline")
				}
				getDeadline = deadline
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
			before := time.Now()
			fact, err := resolver.confirmRuntimePod(ctx, binding)
			after := time.Now()
			if scenario == "deadline" {
				if getDeadline.After(callerDeadline) {
					t.Fatalf("GET deadline %s outlived caller deadline %s", getDeadline, callerDeadline)
				}
			} else if getDeadline.Before(before.Add(2*time.Second)) || getDeadline.After(after.Add(2*time.Second)) {
				t.Fatalf("GET deadline %s outside two-second bound [%s, %s]", getDeadline, before.Add(2*time.Second), after.Add(2*time.Second))
			}
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

// sqlBarrierTracer parks the given occurrence of a completed statement whose
// text contains pattern until release is closed.
type sqlBarrierTracer struct {
	pattern    string
	occurrence int
	mu         sync.Mutex
	seen       int
	reached    chan struct{}
	release    chan struct{}
}

type sqlBarrierTextKey struct{}

func newSQLBarrierTracer(pattern string, occurrence int) *sqlBarrierTracer {
	return &sqlBarrierTracer{pattern: pattern, occurrence: occurrence, reached: make(chan struct{}), release: make(chan struct{})}
}

func (tr *sqlBarrierTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, sqlBarrierTextKey{}, data.SQL)
}

func (tr *sqlBarrierTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	text, _ := ctx.Value(sqlBarrierTextKey{}).(string)
	if tr.occurrence == 0 || data.Err != nil || !strings.Contains(text, tr.pattern) {
		return
	}
	tr.mu.Lock()
	tr.seen++
	fire := tr.seen == tr.occurrence
	tr.mu.Unlock()
	if fire {
		close(tr.reached)
		<-tr.release
	}
}

// awaitLockWait returns once an independent backend running a statement that
// contains pattern is waiting for a PostgreSQL lock.
func awaitLockWait(ctx context.Context, t *testing.T, admin *sql.DB, pattern string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)`, "%"+pattern+"%").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("no backend waited on a lock in %q", pattern)
		}
	}
}

// The final loss classification holds the liveness row FOR SHARE after the
// process fence and reads database time after that wait. Real report
// transactions therefore either finish before the decision or wait for it: a
// held report is never classified until it commits (fresh, no loss) or rolls
// back (the stale value proves loss), and a report arriving after the
// classifier's lock waits for the old-before-new decision. A current process
// without a recorded report is unavailable, never lost, even when the
// confirming GET proves the Pod absent.
func TestPostgreSQLRuntimeFinalLossClassificationOrdersWithLivenessReports(t *testing.T) {
	const (
		reportUpdate = "UPDATE public.runtime_process_liveness AS live"
		livenessLock = "tetral_lock_runtime_process_liveness"
	)
	for _, tc := range []struct {
		name     string
		liveness string // stale, missing or NULL before the decision
		order    string // when the real report runs, if at all
		absent   bool   // confirming GET proves the Pod absent
		loss     bool
	}{
		{name: "report committed before classification", liveness: "stale", order: "committed"},
		{name: "held report then commit", liveness: "stale", order: "held commit"},
		{name: "held report then rollback", liveness: "stale", order: "held rollback", loss: true},
		{name: "classifier first", liveness: "stale", order: "classifier first", loss: true},
		{name: "missing liveness", liveness: "missing", absent: true},
		{name: "NULL liveness", liveness: "NULL", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionfixture.SeedBridgeAPISession(t, admin, "default", "visibility-race", "visibility-thread")
			seedBridgeAPIRuntimeBinding(t, admin, "default", "visibility-race", "visibility-binding", 1, "visibility-pod")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, "visibility-race", "visibility-binding", 1)
			fixture := map[string]string{
				"stale":   `UPDATE runtime_process_liveness SET reported_at=clock_timestamp()-interval '11 seconds' WHERE runtime_process_id='process_visibility-pod'`,
				"missing": `DELETE FROM runtime_process_liveness WHERE runtime_process_id='process_visibility-pod'`,
				"NULL":    `UPDATE runtime_process_liveness SET reported_at=NULL WHERE runtime_process_id='process_visibility-pod'`,
			}[tc.liveness]
			if _, err := admin.Exec(fixture); err != nil {
				t.Fatal(err)
			}
			var receipt string
			var before sql.NullTime
			if err := admin.QueryRow(`SELECT process.registration_receipt,(SELECT reported_at FROM runtime_process_liveness WHERE runtime_process_id=process.runtime_process_id) FROM runtime_processes process WHERE process.runtime_process_id='process_visibility-pod'`).Scan(&receipt, &before); err != nil {
				t.Fatal(err)
			}
			installed := storagetest.OpenWorkloadDB(t, admin, "job_runner")
			runnerBarrier := &sqlBarrierTracer{}
			if tc.order == "classifier first" {
				// The first decision asks for confirmation; the second is final.
				runnerBarrier = newSQLBarrierTracer(livenessLock, 2)
			}
			reportBarrier := &sqlBarrierTracer{}
			if strings.HasPrefix(tc.order, "held") {
				reportBarrier = newSQLBarrierTracer(reportUpdate, 1)
			}
			bridge := dbconnect.NewClientForTesting(installed.OpenWorkload(t, "bridge", reportBarrier))
			entered := make(chan struct{})
			resume := make(chan struct{})
			binding := runtimecontrol.Binding{BindingID: "visibility-binding", BindingGeneration: 1, Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "visibility-pod", PodIP: "10.0.0.10", RuntimeProcessID: "process_visibility-pod"}
			identity := runtimecontrol.ProcessIdentity{Namespace: binding.Namespace, PodUID: binding.PodUID, ID: binding.RuntimeProcessID}
			var enteredOnce sync.Once
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(installed.OpenWorkload(t, "job_runner", runnerBarrier)), 19090, KubernetesRuntimeTargetResolver{
				Snapshot: func() kubernetes.BindingVisibilitySnapshot {
					return kubernetes.NewBindingVisibilitySnapshotStateForTest(true, kubernetes.BoundRuntimePod{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, PodIP: binding.PodIP}, kubernetes.BindingVisibilityNotReady)
				},
				GetPod: func(ctx context.Context, namespace, name string) (*kubernetes.PodObservation, error) {
					enteredOnce.Do(func() { close(entered) })
					select {
					case <-resume:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if tc.absent {
						return &kubernetes.PodObservation{Absent: true}, nil
					}
					// Present, same UID and not ready: only freshness decides.
					return &kubernetes.PodObservation{Namespace: namespace, Name: name, UID: binding.PodUID, Running: true, IP: binding.PodIP}, nil
				},
			})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if tc.liveness != "stale" {
				// Should a regression ask for confirmation, its GET proves absence.
				close(resume)
			}
			type outcome struct {
				result runtimePodLossMutationResult
				err    error
			}
			classified := make(chan outcome, 1)
			go func() {
				result, err := store.mutateLostRuntimeBinding(ctx, "default", "visibility-race", binding, time.Now(), true)
				classified <- outcome{result, err}
			}()
			reportCtx, cancelReport := context.WithCancel(ctx)
			defer cancelReport()
			reported := make(chan error, 1)
			report := func() {
				go func() {
					_, _, err := runtimecontrol.ReportProcess(reportCtx, bridge, identity, receipt, runtimecontrol.ProcessAccepting)
					reported <- err
				}()
			}
			awaitClassifier := func() outcome {
				t.Helper()
				select {
				case got := <-classified:
					return got
				case <-ctx.Done():
					t.Fatal("loss classifier did not finish")
				}
				return outcome{}
			}
			var got outcome
			if tc.liveness != "stale" {
				// A current process without a report never asks for confirmation.
				got = awaitClassifier()
			} else {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("confirming GET did not start")
				}
				switch tc.order {
				case "committed":
					// The GET runs outside any transaction, so a real report and
					// independent Session arbitration both proceed.
					report()
					if err := <-reported; err != nil {
						t.Fatal(err)
					}
					if err := dbconnect.NewClientForTesting(admin).WithWorkspaceTx(ctx, "default", "visibility_race.independent_session", func(tx *dbconnect.Tx) error {
						return runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, "default", "visibility-race")
					}); err != nil {
						t.Fatalf("Session arbitration: %v, cause=%v", err, errors.Unwrap(err))
					}
					close(resume)
					got = awaitClassifier()
				case "held commit", "held rollback":
					report()
					select {
					case <-reportBarrier.reached:
					case <-ctx.Done():
						t.Fatal("report did not hold its liveness UPDATE")
					}
					close(resume)
					awaitLockWait(ctx, t, admin, livenessLock)
					select {
					case got := <-classified:
						t.Fatalf("classification passed an uncommitted report: %+v", got)
					default:
					}
					if tc.order == "held rollback" {
						cancelReport()
					}
					close(reportBarrier.release)
					err := <-reported
					if (err == nil) != (tc.order == "held commit") {
						t.Fatalf("held report outcome: %v", err)
					}
					got = awaitClassifier()
				case "classifier first":
					close(resume)
					select {
					case <-runnerBarrier.reached:
					case <-ctx.Done():
						t.Fatal("final classification did not lock liveness")
					}
					report()
					awaitLockWait(ctx, t, admin, reportUpdate)
					select {
					case err := <-reported:
						t.Fatalf("report passed the classifier's liveness lock: %v", err)
					default:
					}
					close(runnerBarrier.release)
					got = awaitClassifier()
					if err := <-reported; err != nil {
						t.Fatalf("report after the classifier's decision: %v", err)
					}
				}
			}
			want := runtimePodLossMutationStale
			if tc.loss {
				want = runtimePodLossMutationRepaired
			}
			if got.err != nil || got.result.status != want {
				t.Fatalf("classification = %+v/%v; want %s", got.result, got.err, want)
			}
			var bindings, ends, failures int
			var after sql.NullTime
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_bindings WHERE session_id='visibility-race'),(SELECT count(*) FROM session_events WHERE session_id='visibility-race' AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id='visibility-race' AND type='session.error'),(SELECT reported_at FROM runtime_process_liveness WHERE runtime_process_id='process_visibility-pod')`).Scan(&bindings, &ends, &failures, &after); err != nil {
				t.Fatal(err)
			}
			if tc.loss != (bindings == 0) || (!tc.loss && (ends != 0 || failures != 0)) {
				t.Fatalf("custody bindings=%d ends=%d errors=%d; want loss=%t", bindings, ends, failures, tc.loss)
			}
			switch tc.order {
			case "held rollback":
				if after.Valid != before.Valid || !after.Time.Equal(before.Time) {
					t.Fatalf("rolled-back report changed liveness %v -> %v", before, after)
				}
			case "committed", "held commit", "classifier first":
				if !after.Valid || !after.Time.After(before.Time) {
					t.Fatalf("committed report liveness %v -> %v", before, after)
				}
			}
		})
	}
}
