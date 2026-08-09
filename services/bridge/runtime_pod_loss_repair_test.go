package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

func TestPostgreSQLRuntimePodLossSweepClosesActiveTurnWithoutInputAndIsIdempotent(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 80, "Write", "idle", false, false, false, false, true)
	var logs bytes.Buffer
	store := runtimePodLossSweepStore(runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})

	repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default")
	if err != nil || repaired != 1 {
		t.Fatalf("first pod-loss sweep = %d/%v; want 1/nil", repaired, err)
	}
	if repaired, err = store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 0 {
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
		  AND payload_json::jsonb ->> 'tool_use_event_id'=$2
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
	if requestEnds != 1 || toolResults != 1 || sessionErrors != 1 || sessionStatus != "idle" || bindingRows != 0 {
		t.Fatalf("closeout facts end=%d result=%d errors=%d status=%s bindings=%d; want 1/1/1/idle/0", requestEnds, toolResults, sessionErrors, sessionStatus, bindingRows)
	}
	for _, event := range []string{"runtime_pod_loss_detected", "runtime_pod_loss_repaired", "runtime_pod_loss_sweep_completed"} {
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
		if record["event"] == "runtime_pod_loss_sweep_completed" {
			summaries = append(summaries, record)
		}
	}
	if len(summaries) != 2 || summaries[0]["level"] != "INFO" || summaries[0]["outcome"] != "completed" || summaries[0]["candidate.count"] != float64(1) || summaries[0]["repaired.count"] != float64(1) || summaries[1]["candidate.count"] != float64(0) || summaries[1]["repaired.count"] != float64(0) {
		t.Fatalf("idempotent pod-loss summaries = %#v; want one repair then no work", summaries)
	}
}

func TestPostgreSQLRuntimePodLossSweepUsesClosedVisibilityPartition(t *testing.T) {
	tests := []struct {
		state enginekubernetes.BindingVisibilityState
		ready bool
		want  int
	}{
		{enginekubernetes.BindingVisibilityAbsent, true, 1},
		{enginekubernetes.BindingVisibilityDeleted, true, 1},
		{enginekubernetes.BindingVisibilityUIDChanged, true, 1},
		{enginekubernetes.BindingVisibilityIPChanged, true, 1},
		{enginekubernetes.BindingVisibilityReusable, true, 0},
		{enginekubernetes.BindingVisibilityNotReady, true, 0},
		{enginekubernetes.BindingVisibilityNotServing, true, 0},
		{enginekubernetes.BindingVisibilityTerminating, true, 0},
		{enginekubernetes.BindingVisibilitySnapshotNotReady, false, 0},
	}
	for index, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			candidate := seedRuntimePodLossSweepSession(t, admin, index, "running")
			bound := boundRuntimePod(candidate.binding)
			var logs bytes.Buffer
			store := runtimePodLossSweepStore(runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotStateForTest(tc.ready, bound, tc.state)
			})
			repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default")
			if err != nil || repaired != tc.want {
				t.Fatalf("visibility %s sweep = %d/%v; want %d/nil", tc.state, repaired, err, tc.want)
			}
			var bindingRows int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, candidate.sessionID).Scan(&bindingRows); err != nil {
				t.Fatalf("count visibility binding: %v", err)
			}
			if bindingRows != 1-tc.want {
				t.Fatalf("visibility %s binding rows = %d; want %d", tc.state, bindingRows, 1-tc.want)
			}
			var sessionStatus string
			var errorEvents int
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM sessions WHERE workspace_id='default' AND id=$1`, candidate.sessionID).Scan(&sessionStatus); err != nil {
				t.Fatalf("read visibility session status: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, candidate.sessionID).Scan(&errorEvents); err != nil {
				t.Fatalf("count visibility errors: %v", err)
			}
			if tc.want == 0 && (sessionStatus != "idle" || errorEvents != 0) {
				t.Fatalf("visibility %s changed turn status=%s errors=%d; want idle/0", tc.state, sessionStatus, errorEvents)
			}
			if !tc.ready {
				records := decodeRuntimePodLossLogRecords(t, &logs)
				if len(records) != 1 || records[0]["event"] != "runtime_pod_loss_sweep_completed" || records[0]["level"] != "INFO" || records[0]["outcome"] != "snapshot_not_ready" || records[0]["candidate.count"] != float64(0) {
					t.Fatalf("snapshot-not-ready logs = %#v; want one finite no-work summary", records)
				}
			}
		})
	}
}

func TestPostgreSQLRuntimePodLossSweepLeavesIdleBindingForInputRecovery(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 20, "idle")
	store := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 0 {
		t.Fatalf("idle proactive sweep = %d/%v; want 0/nil", repaired, err)
	}

	seedBridgeAPIEvent(t, admin, "default", candidate.sessionID, candidate.threadID, "evt_idle_rebind_input", 1, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
	replacement := enginekubernetes.BindingCandidate{
		Namespace: candidate.binding.Namespace,
		PodName:   "runtime-replacement-idle",
		PodUID:    "pod-replacement-idle",
		PodIP:     "10.33.0.250",
	}
	store.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
			true,
			boundRuntimePod(candidate.binding),
			enginekubernetes.BindingVisibilityDeleted,
			[]enginekubernetes.BindingCandidate{replacement},
		)
	}}
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
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
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"` + candidate.sessionID + `","session_thread_id":"` + candidate.threadID + `","runtime_input_id":"rin_idle_rebind","event_ids":["evt_idle_rebind_input"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	})
	if err != nil {
		t.Fatalf("idle input-triggered rebind: %v", err)
	}
	if plan.Request == nil || plan.Request.GetTargetPodUid() != replacement.PodUID || plan.Request.GetBindingId() == candidate.binding.BindingID || plan.Request.GetBindingGeneration() <= candidate.binding.BindingGeneration {
		t.Fatalf("idle rebind request = %#v; want a new replacement binding after generation %d", plan.Request, candidate.binding.BindingGeneration)
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
	store := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 1 {
		t.Fatalf("rescheduling-session sweep = %d/%v; want 1/nil", repaired, err)
	}
}

func TestPostgreSQLRuntimePodLossSweepRequiresMatchingRuntimeStatusBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	candidate := seedRuntimePodLossSweepSession(t, admin, 22, "running")
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET binding_generation=binding_generation+1 WHERE workspace_id='default' AND session_id=$1`, candidate.sessionID); err != nil {
		t.Fatalf("make runtime status binding stale: %v", err)
	}
	store := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 0 {
		t.Fatalf("mismatched-status sweep = %d/%v; want 0/nil", repaired, err)
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, candidate.sessionID).Scan(&bindingRows); err != nil || bindingRows != 1 {
		t.Fatalf("mismatched-status binding rows = %d/%v; want 1/nil", bindingRows, err)
	}
}

func TestPostgreSQLRuntimePodLossSweepFreezesCensusBeforeWatcherAndContinuesPages(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const healthyCount = 33
	const lostCount = 33
	healthy := make([]enginekubernetes.BindingCandidate, 0, healthyCount)
	for index := 0; index < healthyCount+lostCount; index++ {
		candidate := seedRuntimePodLossSweepSession(t, admin, 100+index, "running")
		if index < healthyCount {
			healthy = append(healthy, enginekubernetes.BindingCandidate{
				Namespace: candidate.binding.Namespace,
				PodName:   candidate.binding.PodName,
				PodUID:    candidate.binding.PodUID,
				PodIP:     candidate.binding.PodIP,
			})
		}
	}
	rebound := seedRuntimePodLossSweepSession(t, admin, 180, "running")
	watcherCalls := 0
	inserted := runtimePodLossSweepSeed{}
	reboundBindingID := "bind_pod_loss_rebound"
	var logs bytes.Buffer
	store := runtimePodLossSweepStore(runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		watcherCalls++
		inserted = seedRuntimePodLossSweepSession(t, admin, 190, "running")
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
			SET binding_id=$2, binding_generation=binding_generation+100
			WHERE workspace_id='default' AND session_id=$1`, rebound.sessionID, reboundBindingID); err != nil {
			t.Fatalf("rebind after census snapshot: %v", err)
		}
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status
			SET binding_id=$2, binding_generation=binding_generation+100
			WHERE workspace_id='default' AND session_id=$1`, rebound.sessionID, reboundBindingID); err != nil {
			t.Fatalf("rebind status after census snapshot: %v", err)
		}
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, healthy)
	})

	repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default")
	if err != nil {
		t.Fatalf("frozen pod-loss sweep: %v", err)
	}
	if watcherCalls != 1 || runtimePodLossCensusPageSize != 32 || repaired != lostCount {
		t.Fatalf("frozen sweep calls=%d page_size=%d repaired=%d; want 1/32/%d", watcherCalls, runtimePodLossCensusPageSize, repaired, lostCount)
	}
	var insertedRows, reboundRows, remainingInitiallyLost int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, inserted.sessionID).Scan(&insertedRows); err != nil {
		t.Fatalf("count post-snapshot insert: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, rebound.sessionID, reboundBindingID).Scan(&reboundRows); err != nil {
		t.Fatalf("count post-snapshot rebind: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings
		WHERE workspace_id='default' AND session_id >= 'sesn_pod_loss_sweep_133' AND session_id <= 'sesn_pod_loss_sweep_165'`).Scan(&remainingInitiallyLost); err != nil {
		t.Fatalf("count initially visible lost bindings: %v", err)
	}
	if insertedRows != 1 || reboundRows != 1 || remainingInitiallyLost != 0 {
		t.Fatalf("frozen sweep survivors inserted=%d rebound=%d initially_lost=%d; want 1/1/0", insertedRows, reboundRows, remainingInitiallyLost)
	}
	records := decodeRuntimePodLossLogRecords(t, &logs)
	var summary map[string]any
	for _, record := range records {
		if record["event"] == "runtime_pod_loss_sweep_completed" {
			summary = record
		}
		if record["event"] == "runtime_pod_loss_stale" && record["level"] != "INFO" {
			t.Fatalf("stale record level = %v; want INFO", record["level"])
		}
	}
	if summary == nil || summary["page.count"] != float64(3) || summary["candidate.count"] != float64(lostCount+1) || summary["repaired.count"] != float64(lostCount) || summary["stale.count"] != float64(1) {
		t.Fatalf("frozen sweep summary = %#v; want pages/candidates/repaired/stale 3/%d/%d/1", summary, lostCount+1, lostCount)
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
		runtimePodLossSweepStore(runtime, nil, snapshot),
		runtimePodLossSweepStore(runtime, nil, snapshot),
	}
	type result struct {
		repaired int
		err      error
	}
	results := make(chan result, 2)
	for _, store := range stores {
		go func(store *PostgreSQLRuntimeDeliveryStore) {
			repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default")
			results <- result{repaired: repaired, err: err}
		}(store)
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.repaired+second.repaired != 1 {
		t.Fatalf("concurrent sweeps = %+v %+v; want one repair and one stale no-op", first, second)
	}

	inactive := seedRuntimePodLossSweepSession(t, admin, 211, "running")
	inactiveStore := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET status='idle' WHERE workspace_id='default' AND session_id=$1`, inactive.sessionID); err != nil {
			t.Fatalf("make frozen candidate inactive: %v", err)
		}
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := inactiveStore.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 0 {
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
	store := runtimePodLossSweepStore(runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
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
	repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default")
	if err != nil || repaired != 1 {
		t.Fatalf("pod-loss sweep after frozen candidate deletion = %d/%v; want 1/nil", repaired, err)
	}
	if !deletedAfterCensus.Load() {
		t.Fatal("pod-loss census did not run deletion race hook")
	}
	if err := store.repairLostRuntimeBinding(context.Background(), "default", deleted.sessionID, deleted.binding, time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)); err == nil {
		t.Fatal("input-triggered pod-loss repair accepted a deleted Session")
	} else if code, ok := closeoutSentinelCode(err); !ok || code != closeoutScopeSupersededCode || status.Code(err) != codes.FailedPrecondition {
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
		if record["event"] == "runtime_pod_loss_sweep_completed" {
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
	var entered atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	store := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
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
	go func() {
		repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default")
		sweepResults <- sweepResult{repaired: repaired, err: err}
	}()
	go func() {
		plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
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
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default"}`,
		})
		inputResults <- inputResult{plan: plan, err: err}
	}()
	sweep := <-sweepResults
	input := <-inputResults
	if sweep.err != nil || (sweep.repaired != 0 && sweep.repaired != 1) {
		t.Fatalf("racing sweep = %+v; want converged no-op or one repair", sweep)
	}
	if input.err != nil {
		var stale runtimeDeliveryPrepareError
		if !errors.As(input.err, &stale) || stale.kind != "runtime_pod_lost_claim_stale" {
			t.Fatalf("racing input error = %v; want successful replacement or stale fence", input.err)
		}
	} else if input.plan.Request == nil || input.plan.Request.GetTargetPodUid() != replacement.PodUID {
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
		  AND payload_json::jsonb ->> 'tool_use_event_id'=$2
		  AND payload_json::jsonb ->> 'reason'='runtime_pod_lost'`, fixture.sessionID, fixture.toolUseEventID).Scan(&toolResults); err != nil {
		t.Fatalf("count racing tool results: %v", err)
	}
	if requestEnds != 1 || toolResults != 1 {
		t.Fatalf("racing closeout facts end=%d result=%d; want exactly 1/1", requestEnds, toolResults)
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
			listenerCtx, queue.PostgreSQLNotificationListener{Client: client}, queue.ConsumerClassBridge, wake, nil,
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
	store := runtimePodLossSweepStore(runtime, &logs, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repaired, sweepErr := store.RepairLostRuntimeBindings(ctx, "default")
	if repaired != candidateCount-1 || sweepErr == nil || !strings.Contains(sweepErr.Error(), broken.sessionID) {
		t.Fatalf("isolated candidate sweep = %d/%v; want %d later repairs plus early error", repaired, sweepErr, candidateCount-1)
	}
	if strings.Contains(sweepErr.Error(), "invalid") {
		t.Fatalf("candidate error was not normalized: %v", sweepErr)
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
		case "runtime_pod_loss_sweep_completed":
			summary = record
		}
	}
	if failure == nil || failure["level"] != "ERROR" || failure["error.code"] != "candidate_mutation_failed" || failure["error.message_safe"] != "runtime pod-loss candidate repair failed" {
		t.Fatalf("candidate failure record = %#v; want normalized ERROR tuple", failure)
	}
	if summary == nil || summary["level"] != "ERROR" || summary["page.count"] != float64(2) || summary["candidate.count"] != float64(candidateCount) || summary["repaired.count"] != float64(candidateCount-1) || summary["failed.count"] != float64(1) {
		t.Fatalf("candidate failure summary = %#v; want two-page aggregate", summary)
	}
}

type runtimePodLossSweepSeed struct {
	sessionID string
	threadID  string
	binding   runtimeBindingForDelivery
}

func seedRuntimePodLossSweepSession(t *testing.T, db *sql.DB, index int, runtimeStatus string) runtimePodLossSweepSeed {
	t.Helper()
	sessionID := fmt.Sprintf("sesn_pod_loss_sweep_%03d", index)
	threadID := fmt.Sprintf("thr_pod_loss_sweep_%03d", index)
	bindingID := fmt.Sprintf("bind_pod_loss_sweep_%03d", index)
	podName := fmt.Sprintf("runtime-pod-sweep-%03d", index)
	podUID := fmt.Sprintf("pod-uid-sweep-%03d", index)
	podIP := fmt.Sprintf("10.44.%d.%d", (index/250)%250, index%250+1)
	seedBridgeAPISession(t, db, "default", sessionID, threadID)
	var bindingGeneration int64
	if err := db.QueryRowContext(context.Background(), `SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&bindingGeneration); err != nil {
		t.Fatalf("allocate sweep binding generation: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, db, "default", sessionID, bindingID, bindingGeneration, podUID)
	seedRuntimePodLostStatusFence(t, db, sessionID, bindingID, bindingGeneration)
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
		binding: runtimeBindingForDelivery{
			BindingID: bindingID, BindingGeneration: bindingGeneration,
			Namespace: "tetral-agent-runtime", PodName: podName, PodUID: podUID, PodIP: podIP,
		},
	}
}

func runtimePodLossSweepStore(runtime *sql.DB, logs *bytes.Buffer, snapshot func() enginekubernetes.BindingVisibilitySnapshot) *PostgreSQLRuntimeDeliveryStore {
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	store.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: snapshot, Clock: store.Clock}
	if logs != nil {
		store.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return store
}

func boundRuntimePod(binding runtimeBindingForDelivery) enginekubernetes.BoundRuntimePod {
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
