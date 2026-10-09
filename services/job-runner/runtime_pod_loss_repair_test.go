package jobrunner

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLRuntimePodLossSweepPreservesActiveToolOwnerAndIsIdempotent(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 80, "Write", "idle", false, false, false, false, true)
	var logs bytes.Buffer
	store := runtimePodLossSweepStore(t, runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})

	repaired, err := runRuntimePodLossRepair(context.Background(), store)
	if err != nil || repaired != 1 {
		t.Fatalf("first pod-loss sweep = %d/%v; want 1/nil", repaired, err)
	}
	if repaired, err = runRuntimePodLossRepair(context.Background(), store); err != nil || repaired != 0 {
		t.Fatalf("second pod-loss sweep = %d/%v; want 0/nil", repaired, err)
	}

	var requestEnds, toolResults, sessionErrors, bindingRows int
	var sessionStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'
		  AND payload_json::jsonb ->> 'error_kind'='runtime_pod_lost'`, fixture.sessionID).Scan(&requestEnds); err != nil {
		t.Fatalf("count repaired request end: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
		  AND tool_use_event_id=$2
		  AND payload_json::jsonb ->> 'reason'='runtime_pod_lost'`, fixture.sessionID, fixture.toolUseEventID).Scan(&toolResults); err != nil {
		t.Fatalf("count repaired tool result: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, fixture.sessionID).Scan(&sessionErrors); err != nil {
		t.Fatalf("count repaired session error: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM sessions WHERE workspace_id='default' AND id=$1`, fixture.sessionID).Scan(&sessionStatus); err != nil {
		t.Fatalf("read repaired session: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, fixture.sessionID).Scan(&bindingRows); err != nil {
		t.Fatalf("count repaired binding: %v", err)
	}
	if requestEnds != 1 || toolResults != 0 || sessionErrors != 0 || sessionStatus != "idle" || bindingRows != 0 {
		t.Fatalf("recovery facts end=%d result=%d errors=%d status=%s bindings=%d; want 1/0/0/idle/0", requestEnds, toolResults, sessionErrors, sessionStatus, bindingRows)
	}
	for _, event := range []string{"runtime_pod_loss_detected", "runtime_pod_loss_repaired", "runtime_pod_loss_repair_run_completed"} {
		if !strings.Contains(logs.String(), `"event":"`+event+`"`) {
			t.Fatalf("pod-loss logs missing %s: %s", event, logs.String())
		}
	}
	for _, forbidden := range []string{fixture.binding.PodName, fixture.binding.PodUID, fixture.binding.PodIP, fixture.toolUseEventID} {
		if strings.Contains(logs.String(), forbidden) {
			t.Fatalf("pod-loss logs leaked %q: %s", forbidden, logs.String())
		}
	}
	records := decodeRuntimePodLossLogRecords(t, &logs)
	for _, event := range []string{"runtime_pod_loss_detected", "runtime_pod_loss_repaired"} {
		var matched bool
		for _, record := range records {
			if record["event"] == event && record["session.id"] == fixture.sessionID && record["binding.id"] == fixture.binding.BindingID && record["binding.generation"] == float64(fixture.binding.BindingGeneration) {
				matched = true
			}
		}
		if !matched {
			t.Fatalf("pod-loss logs lack correlated %s identity: %s", event, logs.String())
		}
	}
	var summaries []map[string]any
	for _, record := range records {
		if record["event"] == "runtime_pod_loss_repair_run_completed" {
			summaries = append(summaries, record)
		}
	}
	// The second run finds no binding at all and stays quiet.
	if len(summaries) != 1 || summaries[0]["level"] != "INFO" || summaries[0]["outcome"] != "completed" || summaries[0]["candidate.count"] != float64(1) || summaries[0]["repaired.count"] != float64(1) {
		t.Fatalf("idempotent pod-loss summaries = %#v; want one repair then no work", summaries)
	}
}

func TestPostgreSQLRuntimePodLossPreservesRequestForExactInterruptOwner(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 81, "Write", "idle", false, false, false, false, true)
	const interruptID = "rin_pod_loss_interrupt_barrier"
	seedBridgeAPIEvent(t, admin, "default", fixture.sessionID, fixture.parentThreadID, "evt_pod_loss_interrupt_barrier", 3, "user.interrupt", `{}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,
		sequence_from,sequence_to,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'interrupt_control','["evt_pod_loss_interrupt_barrier"]',2,2,'queued',clock_timestamp(),clock_timestamp())`,
		fixture.sessionID, fixture.parentThreadID, interruptID); err != nil {
		t.Fatalf("seed interrupt barrier Inbox: %v", err)
	}
	seedActiveInterruptQueueCustody(t, runtime, fixture.sessionID, fixture.parentThreadID, interruptID, "evt_pod_loss_interrupt_barrier", 2)
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})

	repaired, err := runRuntimePodLossRepair(context.Background(), store)
	if err != nil || repaired != 1 {
		t.Fatalf("pod-loss repair under interrupt = %d/%v; want 1/nil", repaired, err)
	}
	var requestEnds, bindingRows int
	var interruptStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'
		  AND payload_json::jsonb ->> 'error_kind'='runtime_pod_lost'`, fixture.sessionID).Scan(&requestEnds); err != nil {
		t.Fatalf("count interrupt-fenced request end: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings
		WHERE workspace_id='default' AND session_id=$1`, fixture.sessionID).Scan(&bindingRows); err != nil {
		t.Fatalf("count interrupt-fenced old binding: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, interruptID).Scan(&interruptStatus); err != nil {
		t.Fatalf("read continuing interrupt custody: %v", err)
	}
	if requestEnds != 0 || bindingRows != 0 || interruptStatus != "queued" {
		t.Fatalf("interrupt-owned pod-loss handoff = ends:%d bindings:%d interrupt:%s; want 0/0/queued", requestEnds, bindingRows, interruptStatus)
	}
	if err := store.repairLostRuntimeBinding(context.Background(), "default", fixture.sessionID, fixture.binding, time.Now().UTC()); err == nil {
		t.Fatal("stale old-binding repair unexpectedly retained authority")
	} else if runtimeJobPreparationErrorKind(err) != "runtime_pod_lost_claim_stale" {
		t.Fatalf("stale old-binding repair error = %v; want pod-loss stale fence", err)
	}
}

// The census owns no loss decision by itself: a cached non-reusable state only
// selects a candidate, and repair requires fresh loss evidence from the confirming
// GET or an expired heartbeat under the same process classifier as delivery.
func TestPostgreSQLRuntimePodLossSweepRequiresFreshLossEvidence(t *testing.T) {
	type getFact string
	const (
		getAbsent          getFact = "absent"
		getPresentDeleting getFact = "present-deleting"
		getPresentNotReady getFact = "present-not-ready"
		getPresentReusable getFact = "present-reusable"
		getReplaced        getFact = "replaced"
		getError           getFact = "error"
	)
	tests := []struct {
		name         string
		state        enginekubernetes.BindingVisibilityState
		ready        bool
		phase        string
		get          getFact
		expired      bool
		wantRepaired int
		wantGETs     int
		wantErr      bool
	}{
		{"cached deleted, Pod still deleting, fresh heartbeat", enginekubernetes.BindingVisibilityDeleted, true, runtimecontrol.ProcessDraining, getPresentDeleting, false, 0, 1, false},
		{"cached deleted, Pod still deleting, expired heartbeat", enginekubernetes.BindingVisibilityDeleted, true, runtimecontrol.ProcessDraining, getPresentDeleting, true, 1, 1, false},
		{"cached not ready, Pod absent, fresh heartbeat", enginekubernetes.BindingVisibilityNotReady, true, runtimecontrol.ProcessAccepting, getAbsent, false, 1, 1, false},
		{"cached not ready, Pod reusable, expired heartbeat", enginekubernetes.BindingVisibilityNotReady, true, runtimecontrol.ProcessAccepting, getPresentReusable, true, 0, 1, false},
		{"cached not serving, Pod not ready, fresh heartbeat", enginekubernetes.BindingVisibilityNotServing, true, runtimecontrol.ProcessAccepting, getPresentNotReady, false, 0, 1, false},
		{"cached absent, Pod absent, fresh heartbeat", enginekubernetes.BindingVisibilityAbsent, true, runtimecontrol.ProcessAccepting, getAbsent, false, 1, 1, false},
		{"cached UID changed, Pod replaced", enginekubernetes.BindingVisibilityUIDChanged, true, runtimecontrol.ProcessAccepting, getReplaced, false, 1, 1, false},
		{"cached IP changed, Pod absent, fresh heartbeat", enginekubernetes.BindingVisibilityIPChanged, true, runtimecontrol.ProcessAccepting, getAbsent, false, 1, 1, false},
		{"cached terminating, GET failed", enginekubernetes.BindingVisibilityTerminating, true, runtimecontrol.ProcessDraining, getError, true, 0, 1, true},
		{"cached reusable and accepting", enginekubernetes.BindingVisibilityReusable, true, runtimecontrol.ProcessAccepting, getAbsent, true, 0, 0, false},
		{"visibility snapshot not ready", enginekubernetes.BindingVisibilitySnapshotNotReady, false, runtimecontrol.ProcessAccepting, getAbsent, true, 0, 0, false},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			candidate := seedRuntimePodLossSweepSession(t, admin, index, "running")
			age := "0 seconds"
			if tc.expired {
				age = "11 seconds"
			}
			if _, err := admin.ExecContext(context.Background(), `UPDATE runtime_processes SET phase=$2 WHERE runtime_process_id=$1`, candidate.binding.RuntimeProcessID, tc.phase); err != nil {
				t.Fatalf("seed process phase: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(), `UPDATE runtime_process_liveness SET reported_at=clock_timestamp()-$2::interval WHERE runtime_process_id=$1`, candidate.binding.RuntimeProcessID, age); err != nil {
				t.Fatalf("seed process heartbeat: %v", err)
			}
			bound := boundRuntimePod(candidate.binding)
			var logs bytes.Buffer
			store := runtimePodLossSweepStore(t, runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotStateForTest(tc.ready, bound, tc.state)
			})
			resolver := store.TargetResolver.(KubernetesRuntimeTargetResolver)
			gets := 0
			resolver.GetPod = func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
				gets++
				pod := &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: candidate.binding.PodUID, Running: true, IP: candidate.binding.PodIP}
				switch tc.get {
				case getAbsent:
					return &enginekubernetes.PodObservation{Absent: true}, nil
				case getPresentDeleting:
					pod.Deleting = true
				case getPresentReusable:
					pod.Ready = true
				case getReplaced:
					pod.UID = "pod-uid-replacement"
					pod.Ready = true
				case getError:
					return nil, fmt.Errorf("fresh Pod observation unavailable")
				}
				return pod, nil
			}
			store.TargetResolver = resolver
			repaired, err := runRuntimePodLossRepair(context.Background(), store)
			if (err != nil) != tc.wantErr || repaired != tc.wantRepaired {
				t.Fatalf("sweep = %d/%v; want %d repaired, error %t", repaired, err, tc.wantRepaired, tc.wantErr)
			}
			if gets != tc.wantGETs {
				t.Fatalf("confirming GETs = %d; want %d", gets, tc.wantGETs)
			}
			var bindingRows int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, candidate.sessionID).Scan(&bindingRows); err != nil {
				t.Fatalf("count visibility binding: %v", err)
			}
			if bindingRows != 1-tc.wantRepaired {
				t.Fatalf("binding rows = %d; want %d", bindingRows, 1-tc.wantRepaired)
			}
			var sessionStatus string
			var errorEvents int
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM sessions WHERE workspace_id='default' AND id=$1`, candidate.sessionID).Scan(&sessionStatus); err != nil {
				t.Fatalf("read visibility session status: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, candidate.sessionID).Scan(&errorEvents); err != nil {
				t.Fatalf("count visibility errors: %v", err)
			}
			if tc.wantRepaired == 0 && (sessionStatus != "idle" || errorEvents != 0) {
				t.Fatalf("unrepaired Session changed turn status=%s errors=%d; want idle/0", sessionStatus, errorEvents)
			}
			if !tc.ready {
				records := decodeRuntimePodLossLogRecords(t, &logs)
				if len(records) != 1 || records[0]["event"] != "runtime_pod_loss_repair_run_completed" || records[0]["level"] != "INFO" || records[0]["outcome"] != "snapshot_not_ready" || records[0]["candidate.count"] != float64(0) {
					t.Fatalf("snapshot-not-ready logs = %#v; want one finite no-work summary", records)
				}
			}
		})
	}
}

func TestPostgreSQLRuntimePodLossSweepLeavesIdleBindingForInputRecovery(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 20, "idle")
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := runRuntimePodLossRepair(context.Background(), store); err != nil || repaired != 0 {
		t.Fatalf("idle proactive sweep = %d/%v; want 0/nil", repaired, err)
	}

	seedBridgeAPIEvent(t, admin, "default", candidate.sessionID, candidate.threadID, "evt_idle_rebind_input", 1, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
	replacement := enginekubernetes.BindingCandidate{
		Namespace: candidate.binding.Namespace,
		PodName:   "runtime-replacement-idle",
		PodUID:    "pod-replacement-idle",
		PodIP:     "10.33.0.250",
	}
	registerPlacementCandidateForTest(t, admin, replacement)
	store.TargetResolver = KubernetesRuntimeTargetResolver{GetPod: fixtureAbsentRuntimePod, LoadClient: runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, runtimeLoadFixture(0)) })), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
			true,
			boundRuntimePod(candidate.binding),
			enginekubernetes.BindingVisibilityDeleted,
			[]enginekubernetes.BindingCandidate{replacement},
		)
	}}
	job := RuntimeJob{
		JobID:           "job_idle_rebind",
		LeaseToken:      "lease_idle_rebind",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       candidate.sessionID,
		SessionThreadID: candidate.threadID,
		RuntimeInputID:  "rin_idle_rebind",
		EventIDs:        []string{"evt_idle_rebind_input"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default","session_id":"` + candidate.sessionID + `","session_thread_id":"` + candidate.threadID + `","runtime_input_id":"rin_idle_rebind","event_ids":["evt_idle_rebind_input"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("idle input-triggered rebind: %v", err)
	}
	if plan.AcceptInput == nil || plan.AcceptInput.GetTargetPodUid() != replacement.PodUID || plan.AttemptedBinding.BindingID == candidate.binding.BindingID || plan.AttemptedBinding.Generation <= candidate.binding.BindingGeneration {
		t.Fatalf("idle rebind request = %#v; want a new replacement binding after generation %d", plan.AcceptInput, candidate.binding.BindingGeneration)
	}
	var errorEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, candidate.sessionID).Scan(&errorEvents); err != nil {
		t.Fatalf("count idle rebind errors: %v", err)
	}
	if errorEvents != 0 {
		t.Fatalf("idle rebind session errors = %d; want 0", errorEvents)
	}
}

func TestPostgreSQLRuntimePodLossSweepIncludesReschedulingSession(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 21, "idle")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET status='rescheduling' WHERE workspace_id='default' AND id=$1`, candidate.sessionID); err != nil {
		t.Fatalf("mark session rescheduling: %v", err)
	}
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := runRuntimePodLossRepair(context.Background(), store); err != nil || repaired != 1 {
		t.Fatalf("rescheduling-session sweep = %d/%v; want 1/nil", repaired, err)
	}
}

func TestPostgreSQLRuntimePodLossSweepIncludesAcceptedInputBeforeRunningStatus(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 23, "idle")
	const runtimeInputID = "rin_pod_loss_pre_running_accept"
	sessionfixture.SeedBridgeAPIRuntimeInbox(
		t,
		admin,
		"default",
		candidate.sessionID,
		candidate.threadID,
		runtimeInputID,
		"messages",
		`["evt_pod_loss_pre_running_accept"]`,
		"accepted",
		candidate.binding.BindingID,
		candidate.binding.PodUID,
		1,
		1,
	)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET binding_generation = $2
		WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID, candidate.binding.BindingGeneration); err != nil {
		t.Fatalf("align accepted input binding generation: %v", err)
	}
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := runRuntimePodLossRepair(context.Background(), store); err != nil || repaired != 1 {
		t.Fatalf("pre-running accepted-input sweep = %d/%v; want 1/nil", repaired, err)
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status, job.status
		FROM session_runtime_inbox inbox
		JOIN queue_jobs job
		  ON job.workspace_id = inbox.workspace_id
		 AND job.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE inbox.workspace_id = 'default' AND inbox.runtime_input_id = $1`, runtimeInputID).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read pre-running accepted-input handoff: %v", err)
	}
	if inboxStatus != "queued" || queueStatus != queue.StatusPending {
		t.Fatalf("pre-running accepted-input handoff = inbox %q / Queue %q; want queued / pending", inboxStatus, queueStatus)
	}
	if repaired, err := runRuntimePodLossRepair(context.Background(), store); err != nil || repaired != 0 {
		t.Fatalf("repeated pre-running accepted-input sweep = %d/%v; want 0/nil", repaired, err)
	}
}

func TestPostgreSQLRuntimePodLossSweepRequiresMatchingRuntimeStatusBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 22, "running")
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET binding_generation=binding_generation+1 WHERE workspace_id='default' AND session_id=$1`, candidate.sessionID); err != nil {
		t.Fatalf("make runtime status binding stale: %v", err)
	}
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := runRuntimePodLossRepair(context.Background(), store); err != nil || repaired != 0 {
		t.Fatalf("mismatched-status sweep = %d/%v; want 0/nil", repaired, err)
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, candidate.sessionID).Scan(&bindingRows); err != nil || bindingRows != 1 {
		t.Fatalf("mismatched-status binding rows = %d/%v; want 1/nil", bindingRows, err)
	}
}

// Trace: one large Workspace's first raw page nominates 12 lost bindings. A
// watcher that is not ready leaves the cycle's cursor where it was. A run then
// performs 8 mutations and keeps the other 4; the next run drains them before
// reading on, crosses an all-inactive page and ends the cycle. A binding
// replaced after nomination is stale at its locked recheck and is not mutated.
func TestPostgreSQLRuntimePodLossRepairRunsBoundedPagesAcrossRuns(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	active := map[int]bool{260: true, 261: true}
	for index := 0; index < 12; index++ {
		active[3+index*10] = true
	}
	seeds := map[int]runtimePodLossSweepSeed{}
	for index := 0; index < 262; index++ {
		status := "idle"
		if active[index] {
			status = "running"
		}
		seeds[index] = seedRuntimePodLossSweepSession(t, admin, index, status)
	}
	ready := false
	rebound := seeds[3]
	var rebindOnce sync.Once
	var logs bytes.Buffer
	store := runtimePodLossSweepStore(t, runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		if ready {
			// The page query already ran: the nominated identity is frozen.
			rebindOnce.Do(func() {
				for _, statement := range []string{
					`UPDATE session_runtime_bindings SET binding_generation=binding_generation+1000 WHERE workspace_id='default' AND session_id=$1`,
					`UPDATE session_runtime_status SET binding_generation=binding_generation+1000 WHERE workspace_id='default' AND session_id=$1`,
				} {
					if _, err := admin.ExecContext(context.Background(), statement, rebound.sessionID); err != nil {
						t.Errorf("replace nominated binding: %v", err)
					}
				}
			})
		}
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(ready, nil)
	})
	owner := NewRuntimePodLossRepair(store, nil, store.Logger)
	runningBindings := func() int {
		var count int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings binding
			JOIN session_runtime_status runtime ON runtime.workspace_id=binding.workspace_id AND runtime.session_id=binding.session_id
			WHERE runtime.status='running'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	notReady := owner.RepairRun(context.Background())
	if notReady.Outcome != "snapshot_not_ready" || notReady.Pages != 0 || !owner.inCycle || owner.hasAfter {
		t.Fatalf("unready run = %+v inCycle=%t hasAfter=%t; want no page consumed", notReady, owner.inCycle, owner.hasAfter)
	}
	ready = true
	first := owner.RepairRun(context.Background())
	if first.Pages != 1 || first.Candidates != 12 || first.Repaired != 7 || first.Stale != 1 || len(owner.pending) != 4 {
		t.Fatalf("first run = %+v pending=%d; want one page, 12 candidates, 8 mutations (one stale), 4 retained", first, len(owner.pending))
	}
	if !owner.hasAfter || owner.cursor.sessionID != seeds[127].sessionID {
		t.Fatalf("cursor after the first page = %+v; want the last raw key of page one", owner.cursor)
	}
	if got := runningBindings(); got != 7 {
		t.Fatalf("running bindings after the first run = %d; want 7", got)
	}
	second := owner.RepairRun(context.Background())
	if second.Pages != 2 || second.Candidates != 2 || second.Repaired != 6 || owner.inCycle || len(owner.pending) != 0 {
		t.Fatalf("second run = %+v inCycle=%t; want 4 retained repairs, then the inactive page and the last page", second, owner.inCycle)
	}
	if got := runningBindings(); got != 1 {
		t.Fatalf("running bindings after the cycle = %d; want only the replaced binding", got)
	}
	var staleReason any
	var unreadyLogged bool
	for _, record := range decodeRuntimePodLossLogRecords(t, &logs) {
		if record["event"] == "runtime_pod_loss_stale" && record["session.id"] == rebound.sessionID {
			staleReason = record["stale.reason"]
		}
		unreadyLogged = unreadyLogged || (record["event"] == "runtime_pod_loss_repair_run_completed" && record["outcome"] == "snapshot_not_ready")
	}
	if staleReason != "binding_changed" || !unreadyLogged {
		t.Fatalf("stale reason = %v, unready run logged = %t; want binding_changed and a logged unready run", staleReason, unreadyLogged)
	}
}

func TestPostgreSQLRuntimePodLossSweepConvergesConcurrentReplicasAndActiveFences(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 210, "running")
	var entered atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	snapshot := func() enginekubernetes.BindingVisibilitySnapshot {
		if entered.Add(1) == 2 {
			releaseOnce.Do(func() { close(release) })
		}
		<-release
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	}
	stores := []*PostgreSQLRuntimeDeliveryStore{
		runtimePodLossSweepStore(t, runtime, nil, snapshot),
		runtimePodLossSweepStore(t, runtime, nil, snapshot),
	}
	type result struct {
		repaired int
		err      error
	}
	results := make(chan result, 2)
	for _, store := range stores {
		go func(store *PostgreSQLRuntimeDeliveryStore) {
			repaired, err := runRuntimePodLossRepair(context.Background(), store)
			results <- result{repaired: repaired, err: err}
		}(store)
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.repaired+second.repaired != 1 {
		t.Fatalf("concurrent sweeps = %+v %+v; want one repair and one stale no-op", first, second)
	}

	inactive := seedRuntimePodLossSweepSession(t, admin, 211, "running")
	inactiveStore := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET status='idle' WHERE workspace_id='default' AND session_id=$1`, inactive.sessionID); err != nil {
			t.Fatalf("make frozen candidate inactive: %v", err)
		}
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := runRuntimePodLossRepair(context.Background(), inactiveStore); err != nil || repaired != 0 {
		t.Fatalf("inactive-fenced sweep = %d/%v; want 0/nil", repaired, err)
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, inactive.sessionID).Scan(&bindingRows); err != nil || bindingRows != 1 {
		t.Fatalf("inactive-fenced binding rows = %d/%v; want 1/nil", bindingRows, err)
	}
	_ = candidate
}

func TestPostgreSQLRuntimePodLossSweepTreatsDeletedFrozenCandidateAsInactiveAndContinues(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	deleted := seedRuntimePodLossSweepSession(t, admin, 311, "running")
	later := seedRuntimePodLossSweepSession(t, admin, 312, "running")
	client := dbconnect.NewClientForTesting(runtime)
	deleteStore := session.NewPostgreSQLSessionStore(client, session.WithSessionDeleteSandboxRelease(
		func(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, now time.Time) error {
			_, _, err := sandboxrelease.EnsureTx(ctx, tx, string(ws), sessionID, sandboxrelease.SessionDelete, "", now)
			return err
		},
	))
	var deletedAfterCensus atomic.Bool
	var logs bytes.Buffer
	store := runtimePodLossSweepStore(t, runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		if deletedAfterCensus.CompareAndSwap(false, true) {
			if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET status='idle'
				WHERE workspace_id='default' AND session_id=$1`, deleted.sessionID); err != nil {
				t.Fatalf("finish frozen candidate before production deletion: %v", err)
			}
			if err := deleteStore.WithWorkspaceTx(context.Background(), workspace.DefaultID, func(tx session.Transaction) error {
				return tx.DeleteSession(context.Background(), deleted.sessionID)
			}); err != nil {
				t.Fatalf("delete frozen pod-loss candidate through Session store: %v", err)
			}
		}
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	repaired, err := runRuntimePodLossRepair(context.Background(), store)
	if err != nil || repaired != 1 {
		t.Fatalf("pod-loss sweep after frozen candidate deletion = %d/%v; want 1/nil", repaired, err)
	}
	if !deletedAfterCensus.Load() {
		t.Fatal("pod-loss census did not run deletion race hook")
	}
	if err := store.repairLostRuntimeBinding(context.Background(), "default", deleted.sessionID, deleted.binding, time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)); err == nil {
		t.Fatal("input-triggered pod-loss repair accepted a deleted Session")
	} else if code, ok := runtimecontrol.SentinelCode(err); !ok || code != runtimecontrol.ScopeSupersededCode || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("input-triggered deleted-Session repair error = %v; want typed scope-superseded rejection", err)
	}
	var lifecycleState, bindingID string
	var runtimeState string
	var bindingGeneration int64
	var closeoutEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT lifecycle_state FROM sessions
		WHERE workspace_id='default' AND id=$1`, deleted.sessionID).Scan(&lifecycleState); err != nil {
		t.Fatalf("read deleted frozen candidate: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT binding_id, binding_generation
		FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, deleted.sessionID).Scan(&bindingID, &bindingGeneration); err != nil {
		t.Fatalf("read deleted frozen binding: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_status
		WHERE workspace_id='default' AND session_id=$1`, deleted.sessionID).Scan(&runtimeState); err != nil {
		t.Fatalf("read deleted frozen Runtime status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1
		  AND type IN ('session.error','span.model_request_end','agent.tool_result')`, deleted.sessionID).Scan(&closeoutEvents); err != nil {
		t.Fatalf("count deleted frozen closeout events: %v", err)
	}
	if lifecycleState != "deleted" || runtimeState != "idle" || bindingID != deleted.binding.BindingID || bindingGeneration != deleted.binding.BindingGeneration || closeoutEvents != 0 {
		t.Fatalf("deleted frozen candidate = lifecycle %q Runtime %q binding %q/%d closeout %d; want deleted idle scope, original binding, and no repair",
			lifecycleState, runtimeState, bindingID, bindingGeneration, closeoutEvents)
	}
	var laterBindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings
		WHERE workspace_id='default' AND session_id=$1`, later.sessionID).Scan(&laterBindings); err != nil || laterBindings != 0 {
		t.Fatalf("later candidate binding rows = %d/%v; want repaired", laterBindings, err)
	}
	records := decodeRuntimePodLossLogRecords(t, &logs)
	var staleRecord, summary map[string]any
	for _, record := range records {
		if record["event"] == "runtime_pod_loss_stale" && record["session.id"] == deleted.sessionID {
			staleRecord = record
		}
		if record["event"] == "runtime_pod_loss_repair_run_completed" {
			summary = record
		}
	}
	if staleRecord == nil || staleRecord["stale.reason"] != "inactive" {
		t.Fatalf("deleted frozen stale record = %#v; want inactive", staleRecord)
	}
	if summary == nil || summary["repaired.count"] != float64(1) || summary["stale.count"] != float64(1) || summary["failed.count"] != float64(0) {
		t.Fatalf("deleted frozen sweep summary = %#v; want repaired/stale/failed 1/1/0", summary)
	}
}

func TestPostgreSQLRuntimePodLossSweepRacingInputWritesOneCloseout(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 212, "Write", "idle", false, false, false, false, true)
	inputEventID := "evt_pod_loss_racing_input"
	seedBridgeAPIEvent(t, admin, "default", fixture.sessionID, fixture.parentThreadID, inputEventID, 3, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
	replacement := enginekubernetes.BindingCandidate{
		Namespace: fixture.binding.Namespace,
		PodName:   "runtime-race-replacement",
		PodUID:    "pod-race-replacement",
		PodIP:     "10.55.0.1",
	}
	registerPlacementCandidateForTest(t, admin, replacement)
	var entered atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		if call := entered.Add(1); call <= 2 {
			if call == 2 {
				releaseOnce.Do(func() { close(release) })
			}
			<-release
		}
		return enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
			true,
			boundRuntimePod(fixture.binding),
			enginekubernetes.BindingVisibilityDeleted,
			[]enginekubernetes.BindingCandidate{replacement},
		)
	})
	type sweepResult struct {
		repaired int
		err      error
	}
	type inputResult struct {
		plan RuntimeCommandPlan
		err  error
	}
	sweepResults := make(chan sweepResult, 1)
	inputResults := make(chan inputResult, 1)
	job := RuntimeJob{
		JobID:           "job_pod_loss_racing_input",
		LeaseToken:      "lease_pod_loss_racing_input",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       fixture.sessionID,
		SessionThreadID: fixture.parentThreadID,
		RuntimeInputID:  "rin_pod_loss_racing_input",
		EventIDs:        []string{inputEventID},
		SequenceFrom:    3,
		SequenceTo:      3,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	go func() {
		repaired, err := runRuntimePodLossRepair(context.Background(), store)
		sweepResults <- sweepResult{repaired: repaired, err: err}
	}()
	go func() {
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		inputResults <- inputResult{plan: plan, err: err}
	}()
	sweep := <-sweepResults
	input := <-inputResults
	if sweep.err != nil || (sweep.repaired != 0 && sweep.repaired != 1) {
		t.Fatalf("racing sweep = %+v; want converged no-op or one repair", sweep)
	}
	if input.err != nil {
		var stale runtimecontrol.PreparationError
		if !errors.As(input.err, &stale) || stale.Kind != "runtime_pod_lost_claim_stale" {
			t.Fatalf("racing input error = %v; want successful replacement or stale fence", input.err)
		}
	} else if input.plan.AcceptInput == nil || input.plan.AcceptInput.GetTargetPodUid() != replacement.PodUID {
		t.Fatalf("racing input plan = %#v; want replacement target", input.plan)
	}
	var requestEnds, toolResults int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'
		  AND payload_json::jsonb ->> 'error_kind'='runtime_pod_lost'`, fixture.sessionID).Scan(&requestEnds); err != nil {
		t.Fatalf("count racing request ends: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
		  AND tool_use_event_id=$2
		  AND payload_json::jsonb ->> 'reason'='runtime_pod_lost'`, fixture.sessionID, fixture.toolUseEventID).Scan(&toolResults); err != nil {
		t.Fatalf("count racing tool results: %v", err)
	}
	if requestEnds != 1 || toolResults != 0 {
		t.Fatalf("racing recovery facts end=%d result=%d; want exactly 1/0", requestEnds, toolResults)
	}
}

func TestPostgreSQLRuntimePodLossSweepIsolatesEarlyPageFailureWithListenerConnectionOccupied(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const candidateCount = 34
	candidates := make([]runtimePodLossSweepSeed, 0, candidateCount)
	for index := 0; index < candidateCount; index++ {
		candidates = append(candidates, seedRuntimePodLossSweepSession(t, admin, 220+index, "running"))
	}
	broken := candidates[0]
	later := candidates[len(candidates)-1]
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='invalid' WHERE workspace_id='default' AND session_id=$1`, broken.sessionID); err != nil {
		t.Fatalf("corrupt early candidate binding: %v", err)
	}
	runtime.SetMaxOpenConns(2)
	client := dbconnect.NewClientForTesting(runtime)
	wake := queue.NewWakeSignal()
	listenerCtx, cancelListener := context.WithCancel(context.Background())
	listenerDone := make(chan error, 1)
	readySnapshot := wake.Snapshot()
	go func() {
		listenerDone <- queue.RunNotificationListener(
			listenerCtx, queue.PostgreSQLNotificationListener{Client: client}, queue.ConsumerClassJobRunner, wake, nil,
		)
	}()
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 2*time.Second)
	if err := wake.Wait(readyCtx, time.Hour, readySnapshot); err != nil {
		cancelReady()
		cancelListener()
		t.Fatalf("wait for PostgreSQL notification listener: %v", err)
	}
	cancelReady()
	defer func() {
		cancelListener()
		select {
		case err := <-listenerDone:
			if err != nil {
				t.Errorf("notification listener shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("notification listener did not stop")
		}
	}()
	var logs bytes.Buffer
	store := runtimePodLossSweepStore(t, runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := NewRuntimePodLossRepair(store, nil, store.Logger)
	var repaired, failed int
	for run := 0; run < 5; run++ {
		summary := owner.RepairRun(ctx)
		repaired += summary.Repaired
		failed += summary.Failed
	}
	// A later cycle legitimately retries the broken candidate.
	if repaired != candidateCount-1 || failed < 1 {
		t.Fatalf("isolated candidate runs repaired=%d failed=%d; want %d later repairs despite the early failure", repaired, failed, candidateCount-1)
	}
	var laterBindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, later.sessionID).Scan(&laterBindings); err != nil || laterBindings != 0 {
		t.Fatalf("later candidate binding rows = %d/%v; want 0/nil", laterBindings, err)
	}
	if strings.Contains(logs.String(), "invalid") {
		t.Fatalf("candidate failure logs leaked payload: %s", logs.String())
	}
	records := decodeRuntimePodLossLogRecords(t, &logs)
	var failure, summary map[string]any
	for _, record := range records {
		switch record["event"] {
		case "runtime_pod_loss_repair_failed":
			failure = record
		case "runtime_pod_loss_repair_run_completed":
			if summary == nil {
				summary = record
			}
		}
	}
	if failure == nil || failure["level"] != "ERROR" || failure["error.code"] != "candidate_mutation_failed" || failure["error.message_safe"] != "runtime pod-loss candidate repair failed" {
		t.Fatalf("candidate failure record = %#v; want normalized ERROR tuple", failure)
	}
	if summary == nil || summary["level"] != "ERROR" || summary["page.count"] != float64(1) || summary["candidate.count"] != float64(candidateCount) || summary["failed.count"] != float64(1) {
		t.Fatalf("first run summary = %#v; want one page of every candidate with the early failure", summary)
	}
}

type runtimePodLossSweepSeed struct {
	sessionID string
	threadID  string
	binding   runtimecontrol.Binding
}

func seedRuntimePodLossSweepSession(t *testing.T, db *sql.DB, index int, runtimeStatus string) runtimePodLossSweepSeed {
	t.Helper()
	sessionID := fmt.Sprintf("sesn_pod_loss_sweep_%03d", index)
	threadID := fmt.Sprintf("thr_pod_loss_sweep_%03d", index)
	bindingID := fmt.Sprintf("bind_pod_loss_sweep_%03d", index)
	podName := fmt.Sprintf("runtime-pod-sweep-%03d", index)
	podUID := fmt.Sprintf("pod-uid-sweep-%03d", index)
	podIP := fmt.Sprintf("10.44.%d.%d", (index/250)%250, index%250+1)
	sessionfixture.SeedBridgeAPISession(t, db, "default", sessionID, threadID)
	var bindingGeneration int64
	if err := db.QueryRowContext(context.Background(), `SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&bindingGeneration); err != nil {
		t.Fatalf("allocate sweep binding generation: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, db, "default", sessionID, bindingID, bindingGeneration, podUID)
	sessionfixture.SeedRuntimePodLostStatusFence(t, db, sessionID, bindingID, bindingGeneration)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_name=$2, agent_runtime_pod_uid=$3, agent_runtime_pod_ip=$4
		WHERE workspace_id='default' AND session_id=$1`, sessionID, podName, podUID, podIP); err != nil {
		t.Fatalf("specialize sweep binding: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_status SET status=$2 WHERE workspace_id='default' AND session_id=$1`, sessionID, runtimeStatus); err != nil {
		t.Fatalf("set sweep runtime status: %v", err)
	}
	return runtimePodLossSweepSeed{
		sessionID: sessionID,
		threadID:  threadID,
		binding: runtimecontrol.Binding{
			BindingID: bindingID, BindingGeneration: bindingGeneration,
			Namespace: "tetral-agent-runtime", PodName: podName, PodUID: podUID, PodIP: podIP, RuntimeProcessID: "process_" + podUID,
		},
	}
}

func runtimePodLossSweepStore(t *testing.T, runtime *sql.DB, logs *bytes.Buffer, snapshot func() enginekubernetes.BindingVisibilitySnapshot) *PostgreSQLRuntimeDeliveryStore {
	clock := func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, KubernetesRuntimeTargetResolver{Snapshot: snapshot, Clock: clock,
		GetPod:     fixtureAbsentRuntimePod,
		LoadClient: runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, runtimeLoadFixture(0)) })),
	})
	store.Clock = clock
	if logs != nil {
		store.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return store
}

func boundRuntimePod(binding runtimecontrol.Binding) enginekubernetes.BoundRuntimePod {
	return enginekubernetes.BoundRuntimePod{
		Namespace: binding.Namespace,
		PodName:   binding.PodName,
		PodUID:    binding.PodUID,
		PodIP:     binding.PodIP,
	}
}

func decodeRuntimePodLossLogRecords(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	records := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode pod-loss log: %v", err)
		}
		records = append(records, record)
	}
	return records
}

// Cached deletion/IP/UID states do not prove loss. These historical closeout
// fixtures now supply explicit fresh NotFound evidence while preserving their
// custody, projection, pagination, race and idempotency assertions.
func fixtureAbsentRuntimePod(_ context.Context, _, name string) (*enginekubernetes.PodObservation, error) {
	return &enginekubernetes.PodObservation{Absent: true}, nil
}
func registerPlacementCandidateForTest(t *testing.T, admin *sql.DB, candidate enginekubernetes.BindingCandidate) {
	t.Helper()
	identity := runtimecontrol.ProcessIdentity{Namespace: candidate.Namespace, PodUID: candidate.PodUID, ID: "process_" + candidate.PodUID}
	client := dbconnect.NewClientForTesting(admin)
	registered, err := runtimecontrol.RegisterProcess(context.Background(), client, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtimecontrol.ReportProcess(context.Background(), client, identity, registered.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
		t.Fatal(err)
	}
}

// runRuntimePodLossRepair performs one production repair run from a new
// discovery cycle and reports its repairs; any failed candidate or page is an
// error.
func runRuntimePodLossRepair(ctx context.Context, store *PostgreSQLRuntimeDeliveryStore) (int, error) {
	run := NewRuntimePodLossRepair(store, nil, store.Logger).RepairRun(ctx)
	return run.Repaired, run.Err()
}

// repairRunRecorder forwards the repaired count of each completed repair run.
type repairRunRecorder struct{ repaired chan<- int64 }

func (repairRunRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h repairRunRecorder) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "runtime_pod_loss_repair_run_completed" {
		return nil
	}
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "repaired.count" {
			h.repaired <- attr.Value.Int64()
			return false
		}
		return true
	})
	return nil
}
func (h repairRunRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h repairRunRecorder) WithGroup(string) slog.Handler      { return h }

type repairOwnerTimer struct {
	duration time.Duration
	fire     chan time.Time
}

func TestRuntimePodLossRepairRunsAtStartupOnPodDeletionAndAtItsDeadline(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	// An idle retained binding gives every run one raw page, so each run is
	// reported; it is never nominated.
	seedRuntimePodLossSweepSession(t, admin, 0, "idle")
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	repaired := make(chan int64, 8)
	signal := NewRuntimePodLossRepairSignal()
	owner := NewRuntimePodLossRepair(store, signal, slog.New(repairRunRecorder{repaired: repaired}))
	timers := make(chan repairOwnerTimer, 8)
	// Unbuffered timer channels: a fire completes only while Run waits on it.
	owner.after = func(duration time.Duration) <-chan time.Time {
		timer := repairOwnerTimer{duration: duration, fire: make(chan time.Time)}
		timers <- timer
		return timer.fire
	}
	// The production Pod watcher cache is the Pod-deletion signal source.
	pods := enginekubernetes.NewWatcherCache("tetral-agent-runtime", enginekubernetes.WithPodDeleted(signal.Mark))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan struct{})
	go func() { defer close(stopped); owner.Run(ctx) }()

	nextRun := func(step string) (spacing, deadline repairOwnerTimer) {
		t.Helper()
		for index, want := range []time.Duration{runtimePodLossRepairMinSpacing, runtimePodLossRepairInterval} {
			select {
			case timer := <-timers:
				if timer.duration != want {
					t.Fatalf("%s timer %d = %s; want %s", step, index, timer.duration, want)
				}
				if index == 0 {
					spacing = timer
				} else {
					deadline = timer
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s did not start a repair run", step)
			}
		}
		return spacing, deadline
	}
	runRepaired := func(step string, want int64) {
		t.Helper()
		select {
		case got := <-repaired:
			if got != want {
				t.Fatalf("%s repaired %d bindings; want %d", step, got, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s run did not complete", step)
		}
	}

	fire := func(timer repairOwnerTimer, name string) {
		t.Helper()
		select {
		case timer.fire <- time.Now():
		case <-time.After(10 * time.Second):
			t.Fatalf("repair owner did not wait on its %s timer", name)
		}
	}

	// Startup runs once without waiting for any timer or signal.
	spacing, _ := nextRun("startup")
	runRepaired("startup", 0)

	// A Pod deletion before the 1 s spacing elapses is held: Run must still be
	// waiting on the spacing timer, not starting a run. Once the spacing
	// elapses, the deletion starts the next run before the periodic deadline,
	// and that run repairs a binding lost since startup.
	seedRuntimePodLossSweepSession(t, admin, 1, "running")
	pods.DeletePod("runtime-pod-sweep-001")
	select {
	case spacing.fire <- time.Now():
	case timer := <-timers:
		t.Fatalf("a Pod deletion started a run (timer %s) before the minimum spacing elapsed", timer.duration)
	case <-time.After(10 * time.Second):
		t.Fatal("repair owner did not wait on its spacing timer after startup")
	}
	spacing, deadline := nextRun("pod deletion")
	runRepaired("pod deletion", 1)

	// With no further signal, the periodic deadline starts the next run.
	seedRuntimePodLossSweepSession(t, admin, 2, "running")
	fire(spacing, "spacing")
	fire(deadline, "deadline")
	nextRun("deadline")
	runRepaired("deadline", 1)

	cancel()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("repair owner did not stop after cancellation")
	}
}
