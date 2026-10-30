package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/id"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimeconfig"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPrepareRuntimeRecoveryRequiresExactLiveQueueLeaseBeforeMutation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_recovery_lease_fence"
		threadID  = "thr_recovery_lease_fence"
		sourceID  = "evt_recovery_lease_fence"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "session.status_rescheduled", `{}`)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, threadID, sourceID, time.Now().UTC())
	if err != nil {
		t.Fatalf("build recovery Queue job: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue recovery Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "recovery-lease-fence",
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease recovery Queue job = %#v/%v", leased, err)
	}
	job := RuntimeJob{
		JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Kind: leased[0].Kind,
		PartitionKey: leased[0].PartitionKey, DedupeKey: leased[0].DedupeKey,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RecoverySourceEventID: sourceID, PayloadJSON: string(leased[0].PayloadJSON),
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, job.JobID); err != nil {
		t.Fatalf("expire recovery Queue lease: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(client, 9090, KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-lease-fence", PodUID: "pod-recovery-lease-fence", PodIP: "127.0.0.1",
		}})
	}})
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || !plan.DeliveryAuthorityLost || plan.hasCommand() {
		t.Fatalf("expired recovery preparation = %#v/%v; want authority-loss no-op", plan, err)
	}
	var bindingCount int
	var runtimeStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT status || CASE WHEN updated_at = '2026-01-01T00:00:00Z' THEN '' ELSE ' changed' END FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&bindingCount, &runtimeStatus); err != nil {
		t.Fatalf("read recovery mutation census: %v", err)
	}
	if bindingCount != 0 || runtimeStatus != "idle" {
		t.Fatalf("expired recovery mutated binding/status = %d/%s; want 0/the seeded idle row unchanged", bindingCount, runtimeStatus)
	}
}

type blockingRecoveryActivationStore struct {
	*PostgreSQLRuntimeDeliveryStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockingRecoveryActivationStore) ActivateRuntimeRecovery(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	close(s.entered)
	select {
	case <-ctx.Done():
		return RuntimeCommandPlan{}, ctx.Err()
	case <-s.release:
	}
	return s.PostgreSQLRuntimeDeliveryStore.ActivateRuntimeRecovery(ctx, job)
}

func TestRuntimeRecoveryRevalidatesReclaimedLeaseBeforeBindingAndRuntime(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_recovery_reclaimed"
		threadID  = "thr_recovery_reclaimed"
		sourceID  = "evt_recovery_reclaimed"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "session.status_rescheduled", `{}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status='rescheduling' WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("seed rescheduled thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (workspace_id, session_id, status, idle_since, created_at, updated_at)
		 VALUES ('default',$1,'idle',now(),now(),now())
		 ON CONFLICT (workspace_id, session_id) DO UPDATE SET idle_since = EXCLUDED.idle_since, updated_at = EXCLUDED.updated_at`, sessionID); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, threadID, sourceID, time.Now().UTC())
	if err != nil {
		t.Fatalf("build recovery Queue job: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue recovery Queue job: %v", err)
	}
	lease := func(owner string) RuntimeJob {
		t.Helper()
		leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
			WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: owner,
			MaxJobs: 1, LeaseDuration: time.Minute,
		})
		if err != nil || len(leased) != 1 {
			t.Fatalf("lease recovery as %s = %#v/%v", owner, leased, err)
		}
		return RuntimeJob{
			JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Kind: leased[0].Kind,
			PartitionKey: leased[0].PartitionKey, DedupeKey: leased[0].DedupeKey,
			WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
			RecoverySourceEventID: sourceID, PayloadJSON: string(leased[0].PayloadJSON),
			AttemptCount: int32(leased[0].AttemptCount), MaxAttempts: int32(leased[0].MaxAttempts),
		}
	}
	jobA := lease("worker-a")
	recoveryCandidate := enginekubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: "runtime-recovery", PodUID: "pod-recovery", PodIP: "127.0.0.1"}
	registerPlacementCandidateForTest(t, admin, recoveryCandidate)
	baseStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090, KubernetesRuntimeTargetResolver{LoadClient: runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, runtimeLoadFixture(0)) })), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-recovery", PodUID: "pod-recovery", PodIP: "127.0.0.1",
		}})
	}})
	blockedStore := &blockingRecoveryActivationStore{
		PostgreSQLRuntimeDeliveryStore: baseStore,
		entered:                        make(chan struct{}), release: make(chan struct{}),
	}
	senderA := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	resultA := make(chan RuntimeDeliveryResult, 1)
	go func() {
		result, _ := (RuntimePodDirectDeliverer{Store: blockedStore, Sender: senderA}).DeliverRuntimeJob(context.Background(), jobA)
		resultA <- result
	}()
	<-blockedStore.entered
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		 WHERE workspace_id='default' AND id=$1`, jobA.JobID); err != nil {
		t.Fatalf("expire worker A lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeRecovery, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim worker A lease = %d/%v", reclaimed, err)
	}
	jobB := lease("worker-b")
	senderB := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	resultB, err := (RuntimePodDirectDeliverer{Store: baseStore, Sender: senderB}).DeliverRuntimeJob(context.Background(), jobB)
	if err != nil || resultB.Status != RuntimeDeliveryAccepted || len(senderB.requests) != 1 {
		t.Fatalf("worker B recovery = %#v/%v calls=%d", resultB, err, len(senderB.requests))
	}
	recoveryRequest, ok := senderB.requests[0].(*agentruntimev1.RecoverThreadRequest)
	if !ok || recoveryRequest.GetRecoveryLeaseRef().GetLeaseToken() != jobB.LeaseToken {
		t.Fatalf("worker B recovery request = %#v; want exact live lease", senderB.requests[0])
	}
	type recoveryDurableSnapshot struct {
		bindingCount    int
		runtimeStatus   string
		statusBindingID string
		queueStatus     string
		leaseToken      string
		eventCount      int
		operationCount  int
	}
	readSnapshot := func() recoveryDurableSnapshot {
		t.Helper()
		var snapshot recoveryDurableSnapshot
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
			(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
			(SELECT binding_id FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
			(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
			(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$2),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
			(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`,
			sessionID, jobB.JobID,
		).Scan(&snapshot.bindingCount, &snapshot.runtimeStatus, &snapshot.statusBindingID,
			&snapshot.queueStatus, &snapshot.leaseToken, &snapshot.eventCount, &snapshot.operationCount); err != nil {
			t.Fatalf("read recovery durable snapshot: %v", err)
		}
		return snapshot
	}
	beforeStaleWorker := readSnapshot()
	close(blockedStore.release)
	if stale := <-resultA; stale.Status != RuntimeDeliveryAuthorityLost || len(senderA.requests) != 0 {
		t.Fatalf("worker A resumed = %#v calls=%d; want authority loss before Runtime", stale, len(senderA.requests))
	}
	afterStaleWorker := readSnapshot()
	if afterStaleWorker != beforeStaleWorker {
		t.Fatalf("stale recovery worker changed durable state: before=%+v after=%+v", beforeStaleWorker, afterStaleWorker)
	}
	var bindingCount int
	var runtimeStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1)`, sessionID,
	).Scan(&bindingCount, &runtimeStatus); err != nil {
		t.Fatalf("read recovery winner: %v", err)
	}
	if bindingCount != 1 || runtimeStatus != "running" {
		t.Fatalf("recovery winner binding/status = %d/%s; want 1/running", bindingCount, runtimeStatus)
	}
}

func TestRuntimeRecoveryFinalExhaustionTerminatesSessionAndPendingRecovery(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_recovery_exhausted"
		threadID  = "thr_recovery_exhausted"
		sourceA   = "evt_recovery_exhausted_a"
		sourceB   = "evt_recovery_exhausted_b"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceA, 1, "session.status_rescheduled", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceB, 2, "session.status_rescheduled", `{}`)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (workspace_id, session_id, status, idle_since, created_at, updated_at)
		 VALUES ('default',$1,'idle',now(),now(),now())
		 ON CONFLICT (workspace_id, session_id) DO UPDATE SET idle_since = EXCLUDED.idle_since, updated_at = EXCLUDED.updated_at`, sessionID); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	for _, sourceID := range []string{sourceA, sourceB} {
		enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, threadID, sourceID, time.Now().UTC())
		if err != nil {
			t.Fatalf("build recovery Queue job: %v", err)
		}
		if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
			t.Fatalf("enqueue recovery Queue job: %v", err)
		}
	}
	// The Runner's direct lease: termination must also clear its provenance.
	directLease, err := leaseJobRunnerJobsUntilLeased(context.Background(), queueStore, queue.LeaseJobRunnerJobsRequest{
		LeaseOwner: "final-owner", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	leased := directLease.Jobs
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease final recovery = %#v/%v", leased, err)
	}
	var leasedPayload struct {
		SourceEventID string `json:"source_event_id"`
	}
	if err := json.Unmarshal(leased[0].PayloadJSON, &leasedPayload); err != nil || leasedPayload.SourceEventID == "" {
		t.Fatalf("decode leased recovery payload: %#v/%v", leasedPayload, err)
	}
	job := RuntimeJob{
		JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Kind: leased[0].Kind,
		PartitionKey: leased[0].PartitionKey, DedupeKey: leased[0].DedupeKey,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RecoverySourceEventID: leasedPayload.SourceEventID, PayloadJSON: string(leased[0].PayloadJSON),
		AttemptCount: int32(leased[0].MaxAttempts), MaxAttempts: int32(leased[0].MaxAttempts),
	}
	store := fixtureRuntimeDeliveryStore(client, admin, 9090)
	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{
		Status: RuntimeDeliveryRejected, Retryable: true,
		ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime recovery failed",
	})
	if err != nil || result.Status != RuntimeDeliveryRejected || !result.QueueLeaseSettled {
		t.Fatalf("finalize recovery exhaustion = %#v/%v", result, err)
	}
	var sessionStatus, threadStatus, runtimeStatus string
	var liveRecoveryJobs, bindingCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key=$3 AND kind=$4 AND status IN ('pending','leased')),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`,
		sessionID, threadID, queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID), queue.KindRuntimeRecovery,
	).Scan(&sessionStatus, &threadStatus, &runtimeStatus, &liveRecoveryJobs, &bindingCount); err != nil {
		t.Fatalf("read recovery exhaustion state: %v", err)
	}
	if sessionStatus != "terminated" || threadStatus != "failed" || runtimeStatus != "idle" || liveRecoveryJobs != 0 || bindingCount != 0 {
		t.Fatalf("recovery exhaustion = %s/%s/%s jobs=%d bindings=%d", sessionStatus, threadStatus, runtimeStatus, liveRecoveryJobs, bindingCount)
	}
}

// A final recovery attempt whose Session terminated while it held the
// Runner's direct lease cancels that lease, clearing its provenance with it.
func TestRuntimeRecoveryFinalAttemptAfterSessionTerminationCancelsItsDirectLease(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_recovery_terminated"
		threadID  = "thr_recovery_terminated"
		sourceID  = "evt_recovery_terminated"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "session.status_rescheduled", `{}`)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, threadID, sourceID, time.Now().UTC())
	if err != nil {
		t.Fatalf("build recovery Queue job: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue recovery Queue job: %v", err)
	}
	directLease, err := leaseJobRunnerJobsUntilLeased(context.Background(), queueStore, queue.LeaseJobRunnerJobsRequest{
		LeaseOwner: "terminated-recovery-owner", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	leased := directLease.Jobs
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease final recovery = %#v/%v", leased, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET status='terminated' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("terminate Session: %v", err)
	}
	job := RuntimeJob{
		JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Kind: leased[0].Kind,
		PartitionKey: leased[0].PartitionKey, DedupeKey: leased[0].DedupeKey,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RecoverySourceEventID: sourceID, PayloadJSON: string(leased[0].PayloadJSON),
		AttemptCount: int32(leased[0].MaxAttempts), MaxAttempts: int32(leased[0].MaxAttempts),
	}
	store := fixtureRuntimeDeliveryStore(client, admin, 9090)
	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{
		Status: RuntimeDeliveryRejected, Retryable: true,
		ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime recovery failed",
	})
	if err != nil || result.Status != RuntimeDeliveryDuplicate || !result.QueueLeaseSettled {
		t.Fatalf("finalize recovery for terminated Session = %#v/%v; want a settled duplicate", result, err)
	}
	var status string
	var leaseToken sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT status, lease_token FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, job.JobID).Scan(&status, &leaseToken); err != nil {
		t.Fatalf("read recovery Queue job: %v", err)
	}
	if status != queue.StatusCancelled || leaseToken.Valid {
		t.Fatalf("recovery Queue job = %s (lease token %t); want cancelled without a lease", status, leaseToken.Valid)
	}
}

func TestRuntimeRecoveryChildFinalExhaustionSettlesLeaseAndRecomputesResidency(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		activeSibling     bool
		mainWaits         bool
		noBinding         bool
		wantRuntimeStatus string
		wantCleanup       bool
	}{
		{name: "active sibling keeps residency", activeSibling: true, wantRuntimeStatus: "running"},
		{name: "no remaining work arms cleanup", wantRuntimeStatus: "idle", wantCleanup: true},
		{name: "a waiting main thread keeps its wait", mainWaits: true, wantRuntimeStatus: "idle", wantCleanup: true},
		{name: "no binding", noBinding: true, wantRuntimeStatus: "idle", wantCleanup: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(testCase.name, " ", "_")
			sessionID := "sesn_child_recovery_exhausted_" + suffix
			mainThreadID := "thr_child_recovery_main_" + suffix
			childThreadID := "thr_child_recovery_target_" + suffix
			siblingThreadID := "thr_child_recovery_sibling_" + suffix
			sourceID := "evt_child_recovery_source_" + suffix
			bindingID := "bind_child_recovery_" + suffix
			podUID := "pod_child_recovery_" + suffix
			sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
			sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, "evt_child_recovery_created_"+suffix, 1, "session.thread_created",
				`{"type":"session.thread_created","parent_thread_id":"`+mainThreadID+`","source_tool_use_event_id":"evt_child_recovery_spawn_`+suffix+`"}`)
			if testCase.activeSibling {
				sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, siblingThreadID)
			} else if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
				SET role='approval_reviewer', visibility='internal', task_name=NULL
				WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childThreadID); err != nil {
				t.Fatalf("seed non-mail child role: %v", err)
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, sourceID, 2, "session.thread_status_rescheduled", `{}`)
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
			if testCase.noBinding {
				// A pod loss removed the binding and no recovery attempt placed the Session again.
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET binding_id=NULL, binding_generation=NULL
					WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
					t.Fatalf("seed unbound runtime status: %v", err)
				}
			} else {
				seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			}
			if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET status='running' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
				t.Fatalf("seed child recovery Session state: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='rescheduling'
				WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childThreadID); err != nil {
				t.Fatalf("seed child recovery Thread state: %v", err)
			}
			if testCase.activeSibling {
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='running'
					WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, siblingThreadID); err != nil {
					t.Fatalf("seed active sibling: %v", err)
				}
			}
			if testCase.mainWaits {
				seedBridgeAPIEvent(t, admin, "default", sessionID, mainThreadID, "evt_child_recovery_main_waits_"+suffix, 3, "session.status_idle",
					`{"type":"session.status_idle","stop_reason":{"type":"requires_action","event_ids":["evt_child_recovery_call_`+suffix+`"]}}`)
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='requires_action'
					WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, mainThreadID); err != nil {
					t.Fatalf("seed waiting main thread: %v", err)
				}
			}
			client := dbconnect.NewClientForTesting(runtimeDB)
			queueStore := queue.NewPostgreSQLStore(client)
			enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, childThreadID, sourceID, time.Now().UTC())
			if err != nil {
				t.Fatalf("build child recovery Queue job: %v", err)
			}
			queued, err := queueStore.Enqueue(context.Background(), enqueue)
			if err != nil {
				t.Fatalf("enqueue child recovery Queue job: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET max_attempts=1
				WHERE workspace_id='default' AND id=$1`, queued.ID); err != nil {
				t.Fatalf("set child recovery attempt ceiling: %v", err)
			}
			store := fixtureRuntimeDeliveryStore(client, admin, 9090)
			store.Clock = func() time.Time { return time.Date(2026, 8, 21, 12, 30, 0, 0, time.UTC) }
			deliverer := &postgresFinalizingDeliverer{store: store, result: RuntimeDeliveryResult{
				Status: RuntimeDeliveryRejected, Retryable: true,
				ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime recovery failed",
			}}
			runner := &JobRunner{
				Queue: tetralqueue.NewServer(queueStore, nil), Deliverer: deliverer,
				Config: JobRunnerConfig{LeaseOwner: "child-recovery-finalizer", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
			}
			if err := acquireAndJoinUntilDispatched(context.Background(), runner); err != nil {
				t.Fatalf("run child recovery final attempt: %v", err)
			}
			var queueStatus, sessionStatus, childStatus, runtimeStatus string
			var cleanupAfter sql.NullTime
			var statusEventID, runtimeBindingID sql.NullString
			var bindingCount, failureEvents, closeoutEvents, mainIdleEvents int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
				(SELECT status FROM sessions WHERE workspace_id='default' AND id=$2),
				(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$2 AND id=$3),
				(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$2),
				(SELECT cleanup_after FROM session_runtime_status WHERE workspace_id='default' AND session_id=$2),
				(SELECT status_event_id FROM session_runtime_status WHERE workspace_id='default' AND session_id=$2),
				(SELECT binding_id FROM session_runtime_status WHERE workspace_id='default' AND session_id=$2),
				(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$2),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND session_thread_id=$3 AND type='session.error'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND session_thread_id=$3 AND type='session.thread_status_terminated'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND session_thread_id=$4 AND type='session.status_idle')`,
				queued.ID, sessionID, childThreadID, mainThreadID,
			).Scan(&queueStatus, &sessionStatus, &childStatus, &runtimeStatus, &cleanupAfter, &statusEventID, &runtimeBindingID, &bindingCount, &failureEvents, &closeoutEvents, &mainIdleEvents); err != nil {
				t.Fatalf("read child recovery finalization: %v", err)
			}
			if queueStatus != queue.StatusDeadLettered || sessionStatus == "terminated" || childStatus != "failed" ||
				runtimeStatus != testCase.wantRuntimeStatus || cleanupAfter.Valid != testCase.wantCleanup ||
				(testCase.noBinding != (!runtimeBindingID.Valid && bindingCount == 0)) || (!testCase.noBinding && (runtimeBindingID.String != bindingID || bindingCount != 1)) ||
				failureEvents != 1 || closeoutEvents != 1 || (mainIdleEvents == 1) != testCase.mainWaits {
				t.Fatalf("child finalization = Queue %s Session %s child %s Runtime %s cleanup %v statusEvent %v binding %v/%d events %d/%d main idle %d",
					queueStatus, sessionStatus, childStatus, runtimeStatus, cleanupAfter, statusEventID, runtimeBindingID, bindingCount, failureEvents, closeoutEvents, mainIdleEvents)
			}
			if testCase.wantCleanup && !statusEventID.Valid {
				t.Fatal("idle child finalization omitted cleanup idle fence")
			}
			if testCase.mainWaits {
				var stopReason string
				if err := admin.QueryRowContext(context.Background(), `SELECT payload_json::jsonb->>'stop_reason' FROM session_events
					WHERE workspace_id='default' AND session_id=$1 AND session_thread_id IS NULL AND type='session.status_idle'`, sessionID).Scan(&stopReason); err != nil {
					t.Fatalf("read Session idle: %v", err)
				}
				if want := `{"type": "requires_action", "event_ids": ["evt_child_recovery_call_` + suffix + `"]}`; stopReason != want {
					t.Fatalf("Session idle stop reason = %s; want %s", stopReason, want)
				}
			}
			if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
				WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeRecovery, Limit: 1,
			}); err != nil || reclaimed != 0 {
				t.Fatalf("reclaim settled child recovery = %d/%v; want zero", reclaimed, err)
			}
			if err := acquireAndJoin(context.Background(), runner); err != nil {
				t.Fatalf("replay settled child recovery: %v", err)
			}
			var replayFailureEvents, replayCloseoutEvents int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				count(*) FILTER (WHERE type='session.error'),
				count(*) FILTER (WHERE type='session.thread_status_terminated')
				FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, childThreadID,
			).Scan(&replayFailureEvents, &replayCloseoutEvents); err != nil {
				t.Fatalf("read replay child events: %v", err)
			}
			if replayFailureEvents != 1 || replayCloseoutEvents != 1 {
				t.Fatalf("replayed child events = %d/%d; want one durable pair", replayFailureEvents, replayCloseoutEvents)
			}
		})
	}
}

type recordingMCPConnectorServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer

	mu       sync.Mutex
	requests []*providergatewayv1.ListMcpToolsRequest
}

func (s *recordingMCPConnectorServer) ListMcpTools(_ context.Context, request *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	s.mu.Unlock()
	return &providergatewayv1.ListMcpToolsResponse{
		ManifestEtag: "etag_production_assembly",
		Tools: []*providergatewayv1.McpToolDefinition{{
			Name:            "github_search",
			Description:     "Search GitHub",
			InputSchemaJson: `{"type":"object"}`,
		}},
	}, nil
}

func (s *recordingMCPConnectorServer) recordedRequests() []*providergatewayv1.ListMcpToolsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*providergatewayv1.ListMcpToolsRequest(nil), s.requests...)
}

type expiringMCPManifestLister struct {
	contexts []context.Context
}

func (l *expiringMCPManifestLister) ListMCPTools(ctx context.Context, request mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
	if len(l.contexts) > 0 {
		select {
		case <-l.contexts[len(l.contexts)-1].Done():
		default:
			return mcpmanifest.ListResult{}, errors.New("previous list context was not canceled before the next iteration")
		}
	}
	l.contexts = append(l.contexts, ctx)
	<-ctx.Done()
	return mcpmanifest.ListResult{
		ManifestETag: "etag_" + request.MCPServerName,
		Tools: []mcpmanifest.Tool{{
			Name:            request.MCPServerName + "_search",
			Description:     "Search " + request.MCPServerName,
			InputSchemaJSON: `{"type":"object"}`,
		}},
	}, nil
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestCaptureAdvancesInputAndRedrivesGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_initial_mcp", "thr_bridge_initial_mcp")
	sessionfixture.SeedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_initial_mcp", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = 'default' AND id = 'sesn_bridge_initial_mcp'`); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_initial_mcp", "bind_bridge_initial_mcp", 1, "pod_uid_initial_mcp")

	lister := &recordingMCPManifestLister{results: []mcpmanifest.ListResult{{
		ManifestETag: "etag_initial",
		Tools:        []mcpmanifest.Tool{{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`}},
	}}}
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MCPManifestLister = lister
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	job := RuntimeJob{
		JobID:           "qjob_bridge_initial_mcp",
		LeaseToken:      "lease_bridge_initial_mcp",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_initial_mcp",
		SessionThreadID: "thr_bridge_initial_mcp",
		RuntimeInputID:  "rin_bridge_initial_mcp",
		EventIDs:        []string{"evt_bridge_initial_mcp"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedBridgeAPIEvent(t, admin, "default", job.SessionID, job.SessionThreadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob initial MCP manifest: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("initial MCP manifest delivery result = %#v; want accepted", result)
	}
	if len(lister.requests) != 1 ||
		lister.requests[0].WorkspaceID != "default" ||
		lister.requests[0].SessionID != "sesn_bridge_initial_mcp" ||
		lister.requests[0].MCPServerName != "github" ||
		lister.requests[0].ManifestETag != "" {
		t.Fatalf("MCP manifest lister requests = %#v; want initial github list", lister.requests)
	}
	if len(sender.requests) != 2 || sender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest).GetMcpManifest().GetGeneration() != 1 || sender.requests[1].(*agentruntimev1.AcceptInputRequest).GetRuntimeInputId() != job.RuntimeInputID {
		t.Fatalf("runtime commands = %#v; want manifest installation followed by the original input", sender.requests)
	}
	sessionfixture.AssertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_initial_mcp", "github", 1)
	var operationRuntimeInputID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT runtime_input_id
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_initial_mcp'
		    AND operation = $1
		    AND idempotency_key = 'github:1'`,
		mcpmanifest.OperationChanged,
	).Scan(&operationRuntimeInputID); err != nil {
		t.Fatalf("read initial MCP manifest bridge operation: %v", err)
	}
	if operationRuntimeInputID != "runtime_config_update:mcp_manifest:sesn_bridge_initial_mcp:github:1" {
		t.Fatalf("bridge operation runtime_input_id = %q; want manifest runtime config update id", operationRuntimeInputID)
	}

	// The independent hot-projection carrier remains durable even though the
	// original input no longer waits for that Queue job.
	var queuedJobID string
	var queuedPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT id, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND payload_json::jsonb ->> 'session_id' = 'sesn_bridge_initial_mcp'
		    AND kind = 'runtime_config_update'
		    AND payload_json::jsonb ->> 'mcp_server_name' = 'github'
		    AND payload_json::jsonb ->> 'manifest_generation' = '1'`,
	).Scan(&queuedJobID, &queuedPayload); err != nil {
		t.Fatalf("read committed MCP manifest delivery intent: %v", err)
	}
	redrivenJob, err := DecodeRuntimeJob(&queuev1.QueueJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Id:          queuedJobID,
		WorkspaceId: "default",
		Kind:        queue.KindRuntimeConfigUpdate,
		LeaseToken:  "lease_redriven_manifest",
		PayloadJson: queuedPayload,
	})
	if err != nil {
		t.Fatalf("decode committed MCP manifest delivery intent: %v", err)
	}
	redriveSender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	redriveStore := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	redriveResult, err := (RuntimePodDirectDeliverer{Store: redriveStore, Sender: redriveSender}).DeliverRuntimeJob(context.Background(), redrivenJob)
	if err != nil {
		t.Fatalf("redrive committed MCP manifest generation: %v", err)
	}
	if redriveResult.Status != RuntimeDeliveryAccepted || len(redriveSender.requests) != 1 ||
		redriveSender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest).GetMcpManifest().GetGeneration() != 1 {
		t.Fatalf("redriven manifest delivery = %#v requests %#v; want one accepted generation-1 command", redriveResult, redriveSender.requests)
	}
	var delivered struct {
		MCPManifest map[string]json.RawMessage `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(redriveSender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest).GetMcpManifest().GetContentJson()), &delivered); err != nil {
		t.Fatalf("decode rebuilt MCP command: %v", err)
	}
	var manifestETag string
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(delivered.MCPManifest["manifest_etag"], &manifestETag); err != nil {
		t.Fatalf("decode rebuilt manifest etag: %v", err)
	}
	if err := json.Unmarshal(delivered.MCPManifest["tools"], &tools); err != nil {
		t.Fatalf("decode rebuilt manifest tools: %v", err)
	}
	if manifestETag != "etag_initial" || len(tools) != 1 || tools[0].Name != "github_search" {
		t.Fatalf("rebuilt MCP command = %#v; want durable manifest row content", delivered)
	}
	for _, forbidden := range []string{"default_config", "configs"} {
		if _, exists := delivered.MCPManifest[forbidden]; exists {
			t.Fatalf("rebuilt MCP command carries forbidden policy key %q: %#v", forbidden, delivered)
		}
	}
	if len(lister.requests) != 1 {
		t.Fatalf("connector list calls after durable redrive = %d; want original capture call only", len(lister.requests))
	}
}

func TestPostgreSQLRuntimeDeliveryPreparationBoundsStateDrivenReentry(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_runtime_prepare_reentry_bound"
		threadID  = "thr_runtime_prepare_reentry_bound"
	)
	seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runtime_prepare_reentry_bound", 1, "pod_runtime_prepare_reentry_bound")
	job := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_runtime_prepare_reentry_bound", EventIDs: []string{"evt_runtime_prepare_reentry_bound"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)
	lister := &recordingMCPManifestLister{results: []mcpmanifest.ListResult{mcpManifestResult("etag_reentry_bound", "github_search")}}
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.MCPManifestLister = lister

	_, err := store.prepareRuntimeCommand(context.Background(), job, maxRuntimePreparationReentries)
	var prepareErr runtimecontrol.PreparationError
	if !errors.As(err, &prepareErr) || prepareErr.Kind != "runtime_reconcile_invariant" || prepareErr.Retryable {
		t.Fatalf("bounded preparation error = %#v; want terminal runtime_reconcile_invariant", err)
	}
	if len(lister.requests) != 0 {
		t.Fatalf("manifest calls after reentry bound = %d; want zero", len(lister.requests))
	}
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPTransitionRollbackRetainsInputCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_initial_mcp_rollback"
		threadID  = "thr_initial_mcp_rollback"
	)
	seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_rollback", 1, "pod_initial_mcp_rollback")
	job := RuntimeJob{
		JobID: "qjob_initial_mcp_rollback", LeaseToken: "lease_initial_mcp_rollback", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_initial_mcp_rollback", EventIDs: []string{"evt_initial_mcp_rollback"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"retain"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)
	if _, err := admin.ExecContext(context.Background(), `DROP TABLE queue_jobs`); err != nil {
		t.Fatalf("remove manifest Queue persistence: %v", err)
	}
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.MCPManifestLister = &recordingMCPManifestLister{err: mcpmanifest.DiscoveryError{Diagnostic: mcpmanifest.DiagnosticDiscoveryUnavailable}}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || !result.Retryable || len(sender.requests) != 0 {
		t.Fatalf("delivery after manifest transaction rollback = %#v/%v requests %d; want retryable rejection/nil/0", result, err, len(sender.requests))
	}
	var manifests int
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_mcp_manifests
		WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&manifests); err != nil {
		t.Fatalf("count rolled-back manifests: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read retained Inbox custody: %v", err)
	}
	if manifests != 0 || inboxStatus != "queued" {
		t.Fatalf("rolled-back manifests/Inbox = %d/%s; want 0/queued", manifests, inboxStatus)
	}
}

func TestJobRunnerRuntimeDeliveryStoreDiscoversInitialMCPManifestThroughProductionAssembly(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_job_runner_initial_mcp"
		threadID    = "thr_job_runner_initial_mcp"
		eventID     = "evt_job_runner_initial_mcp"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = $1 AND id = $2`, workspaceID, sessionID); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for MCP connector: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connector := &recordingMCPConnectorServer{}
	server := grpc.NewServer()
	providergatewayv1.RegisterMcpConnectorServiceServer(server, connector)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	tokenPath := t.TempDir() + "/gateway-token"
	if err := os.WriteFile(tokenPath, []byte("test-gateway-token"), 0o600); err != nil {
		t.Fatalf("write gateway token: %v", err)
	}
	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-initial-mcp",
		PodUID:    "pod-uid-initial-mcp",
		PodIP:     "10.0.0.42",
	}
	store := NewJobRunnerRuntimeDeliveryStore(
		dbconnect.NewClientForTesting(runtime),
		nil,
		JobRunnerConfig{
			AgentRuntimeGRPCPort:    9090,
			MCPConnectorGRPCAddress: listener.Addr().String(),
			GatewayTokenPath:        tokenPath,
		},
		func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	)
	registerPlacementCandidateForTest(t, admin, candidate)
	resolver := store.TargetResolver.(KubernetesRuntimeTargetResolver)
	resolver.LoadClient = runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, runtimeLoadFixture(0)) }))
	store.TargetResolver = resolver

	t.Cleanup(func() {
		if owner, ok := store.MCPManifestLister.(interface{ Close() error }); ok {
			_ = owner.Close()
		}
	})
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_job_runner_initial_mcp",
		LeaseToken:      "lease_job_runner_initial_mcp",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		SessionThreadID: threadID,
		RuntimeInputID:  "rin_job_runner_initial_mcp",
		EventIDs:        []string{eventID},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	requests := connector.recordedRequests()
	if len(requests) != 1 ||
		requests[0].GetWorkspaceId() != workspaceID ||
		requests[0].GetSessionId() != sessionID ||
		requests[0].GetMcpServerName() != "github" {
		t.Errorf("connector requests = %#v; want one request for %s/%s/github", requests, workspaceID, sessionID)
	}
	var readiness string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT readiness FROM session_mcp_manifests WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = 'github'`,
		workspaceID, sessionID,
	).Scan(&readiness); err != nil {
		t.Fatalf("read captured MCP manifest: %v", err)
	}
	if readiness != "ready" {
		t.Fatalf("captured MCP manifest readiness = %q; want ready", readiness)
	}
	if plan.AcceptInput == nil || plan.AcceptInput.GetRuntimeInputId() != job.RuntimeInputID {
		t.Fatalf("PrepareRuntimeCommand plan = %#v; want same-attempt runtime input delivery", plan)
	}
}

func TestInitialMCPManifestListUsesFreshRPCOnlyDeadline(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_mcp_list_deadline"
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, "thr_mcp_list_deadline")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}]}' WHERE workspace_id = 'default' AND id = $1`, sessionID); err != nil {
		t.Fatalf("seed deadline MCP toolsets: %v", err)
	}
	lister := &expiringMCPManifestLister{}
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.MCPManifestLister = lister
	err := store.captureInitialMCPManifestsWithListTimeout(
		context.Background(),
		RuntimeJob{WorkspaceID: "default", SessionID: sessionID},
		[]mcpmanifest.ToolsetConfig{
			{MCPServerName: "github", BuiltinFamily: "claude"},
			{MCPServerName: "github", BuiltinFamily: "claude"},
		},
		time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC),
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("captureInitialMCPManifestsWithListTimeout: %v", err)
	}
	if len(lister.contexts) != 2 || lister.contexts[0] == lister.contexts[1] {
		t.Fatalf("MCP list contexts = %#v; want one fresh context per toolset", lister.contexts)
	}
	for index, listCtx := range lister.contexts {
		if listCtx.Err() != context.DeadlineExceeded {
			t.Fatalf("MCP list context %d error = %v; want deadline exceeded before persistence", index, listCtx.Err())
		}
	}
	var readyRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_mcp_manifests WHERE workspace_id = 'default' AND session_id = $1 AND readiness = 'ready'`,
		sessionID,
	).Scan(&readyRows); err != nil {
		t.Fatalf("count persisted MCP manifests: %v", err)
	}
	if readyRows != 1 {
		t.Fatalf("persisted ready MCP manifests = %d; want 1 duplicate-safe row after both list contexts expired", readyRows)
	}
}

func TestRuntimeConfigDeliveryRebuildsTheColdBootstrapAgentSettings(t *testing.T) {
	for _, test := range []struct {
		name          string
		sessionID     string
		agentConfig   string
		installedJSON string
		wantSystem    string
	}{
		{
			name:          "configured MCP policy",
			sessionID:     "sesn_runtime_config_policy",
			agentConfig:   `{"name":"agent","model":"anthropic/claude-opus-4-8","system":"Operate as the session specialist.","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`,
			installedJSON: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`,
			wantSystem:    "Operate as the session specialist.",
		},
		{
			name:          "empty MCP policy",
			sessionID:     "sesn_runtime_config_policy_empty",
			agentConfig:   `{"name":"agent","model":"anthropic/claude-opus-4-8","system":null,"tools":[],"mcp_servers":[],"skills":[],"metadata":{}}`,
			installedJSON: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"}],"mcp_servers":[]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionfixture.SeedBridgeAPISession(t, admin, "default", test.sessionID, "thr_"+test.sessionID)
			sessionfixture.SeedBridgeAPIAgentConfig(t, admin, "default", test.sessionID, test.agentConfig)
			sessionfixture.SeedBridgeAPIWritableMemoryStore(t, admin, "default", test.sessionID, "memstore_runtime_config")
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_memory_store_resources
				    SET name = 'Project notes', instructions = 'Preserve this guidance.'
				  WHERE workspace_id = 'default' AND session_id = $1`, test.sessionID); err != nil {
				t.Fatalf("seed runtime memory guidance: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sessions SET approval_mode = 'approve_for_me', config_generation = 7, installed_tools_json = $1
				  WHERE workspace_id = 'default' AND id = $2`, test.installedJSON, test.sessionID); err != nil {
				t.Fatalf("seed runtime config: %v", err)
			}

			var payloadJSON string
			var runtimeInputID string
			client := dbconnect.NewClientForTesting(runtime)
			if err := client.WithWorkspaceTx(context.Background(), "default", "test.rebuild_runtime_config", func(tx *dbconnect.Tx) error {
				var err error
				payloadJSON, runtimeInputID, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
					Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: test.sessionID, ConfigGeneration: "1",
				})
				return err
			}); err != nil {
				t.Fatalf("rebuild runtime config: %v", err)
			}
			if runtimeInputID != runtimeConfigUpdateInputID(test.sessionID, "7") {
				t.Fatalf("rebuilt runtime input id = %q; want current generation 7", runtimeInputID)
			}
			var payload struct {
				ToolPolicy   any                         `json:"tool_policy"`
				System       *string                     `json:"system"`
				MemoryStores []runtimeconfig.MemoryStore `json:"memory_stores"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				t.Fatalf("decode rebuilt runtime config: %v", err)
			}
			var payloadFields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(payloadJSON), &payloadFields); err != nil {
				t.Fatalf("decode rebuilt runtime config fields: %v", err)
			}
			if _, ok := payloadFields["system"]; !ok {
				t.Fatalf("rebuilt runtime config omits nullable system: %s", payloadJSON)
			}
			settings, err := runtimeconfig.InterpretSessionSettings("approve_for_me", test.agentConfig, test.installedJSON, payload.MemoryStores)
			if err != nil {
				t.Fatalf("resolve cold-bootstrap agent settings: %v", err)
			}
			wantJSON, err := json.Marshal(settings.ToolPolicy)
			if err != nil {
				t.Fatalf("marshal cold-bootstrap policy: %v", err)
			}
			var want any
			if err := json.Unmarshal(wantJSON, &want); err != nil {
				t.Fatalf("decode cold-bootstrap policy: %v", err)
			}
			if !reflect.DeepEqual(payload.ToolPolicy, want) {
				t.Fatalf("rebuilt policy = %#v; cold-bootstrap policy = %#v", payload.ToolPolicy, want)
			}
			if !reflect.DeepEqual(payload.MemoryStores, settings.MemoryStores) || len(payload.MemoryStores) != 1 ||
				payload.MemoryStores[0].MemoryStoreID != "memstore_runtime_config" ||
				payload.MemoryStores[0].Name != "Project notes" ||
				payload.MemoryStores[0].Access != "read_write" ||
				payload.MemoryStores[0].Instructions == nil || *payload.MemoryStores[0].Instructions != "Preserve this guidance." {
				t.Fatalf("rebuilt memory stores = %#v; cold-bootstrap memory stores = %#v", payload.MemoryStores, settings.MemoryStores)
			}
			if test.wantSystem == "" {
				if payload.System != nil || settings.System != nil {
					t.Fatalf("rebuilt system = %#v; cold-bootstrap system = %#v; want nil", payload.System, settings.System)
				}
			} else if payload.System == nil || settings.System == nil || *payload.System != test.wantSystem || *settings.System != test.wantSystem {
				t.Fatalf("rebuilt system = %#v; cold-bootstrap system = %#v; want %q", payload.System, settings.System, test.wantSystem)
			}
		})
	}
}

func TestRuntimeConfigDeliveryRebuildsManifestWithoutConsultingToolPolicy(t *testing.T) {
	const sessionID = "sesn_manifest_single_row"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, "thr_manifest_single_row")
	sessionfixture.SeedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{
		"name":"agent",
		"model":"anthropic/claude-opus-4-8",
		"tools":[],
		"mcp_servers":[],
		"skills":[],
		"metadata":{}
	}`)
	insertAcceptedMCPManifest(t, admin, sessionID, "etag_single_row", 4, "github_single_row")

	var payloadJSON string
	var runtimeInputID string
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_single_row", func(tx *dbconnect.Tx) error {
		var err error
		payloadJSON, runtimeInputID, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
			Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID,
			MCPServerName: "github", MCPManifestGeneration: "1",
		})
		return err
	}); err != nil {
		t.Fatalf("rebuild MCP manifest without held policy: %v", err)
	}
	if runtimeInputID != mcpmanifest.InputID(sessionID, "github", 4) {
		t.Fatalf("rebuilt runtime input id = %q; want durable generation 4", runtimeInputID)
	}
	var payload struct {
		Manifest struct {
			ServerName string `json:"mcp_server_name"`
			Tools      []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode rebuilt MCP manifest: %v", err)
	}
	if payload.Manifest.ServerName != "github" || len(payload.Manifest.Tools) != 1 || payload.Manifest.Tools[0].Name != "github_single_row" {
		t.Fatalf("rebuilt MCP manifest = %+v; want durable row content", payload.Manifest)
	}
}

func TestRuntimeConfigDeliveryRebuildsSupersededManifestAtTheCurrentGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_runtime_manifest_superseded"
	seedMCPFamilySession(t, admin, sessionID, "thr_"+sessionID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runtime_manifest_superseded", 1, "pod_runtime_manifest_superseded")
	insertAcceptedMCPManifest(t, admin, sessionID, "etag_old", 1, "github_old")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_mcp_manifests
		    SET tools_json = '[{"name":"github_current","description":"current","input_schema":{"type":"object"}}]',
		        manifest_etag = 'etag_current', manifest_generation = 2, updated_at = '2026-01-01T00:00:20Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND mcp_server_name = 'github'`, sessionID); err != nil {
		t.Fatalf("advance durable manifest: %v", err)
	}

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), RuntimeJob{
		Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID,
		RuntimeInputID: mcpmanifest.InputID(sessionID, "github", 1), MCPServerName: "github", MCPManifestGeneration: "1",
	})
	if err != nil {
		t.Fatalf("deliver superseded manifest intent: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
		t.Fatalf("superseded manifest result = %#v requests=%d; want one accepted fresh apply", result, len(sender.requests))
	}
	request := sender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest)
	contentJSON := request.GetMcpManifest().GetContentJson()
	if request.GetMcpManifest().GetGeneration() != 2 ||
		!strings.Contains(contentJSON, `"manifest_generation":2`) ||
		!strings.Contains(contentJSON, `"name":"github_current"`) ||
		strings.Contains(contentJSON, `github_old`) {
		t.Fatalf("rebuilt superseded config generation/payload = %d/%s; want current generation and content", request.GetMcpManifest().GetGeneration(), contentJSON)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationTerminalDuplicateIsStale(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "thr_bridge_task_delivery_terminal_dup")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "bind_bridge_task_delivery_terminal_dup", 1, "pod_uid_task_delivery_terminal_dup")
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "thr_bridge_task_delivery_terminal_dup", "bind_bridge_task_delivery_terminal_dup", "task_bridge_delivery_terminal_dup", "sevt_tool_delivery_terminal_dup")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_background_tasks
		    SET status = 'completed', terminal_event_id = 'sevt_terminal_dup', updated_at = '2026-01-01T00:20:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_delivery_terminal_dup'
		    AND task_id = 'task_bridge_delivery_terminal_dup'`); err != nil {
		t.Fatalf("mark terminal task: %v", err)
	}

	resolver := &recordingRuntimeTargetResolver{err: errors.New("resolver must not run for terminal duplicate task notification")}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, resolver)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 40, 1, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:           "qjob_bridge_task_delivery_terminal_dup",
		LeaseToken:      "lease_bridge_task_delivery_terminal_dup",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_task_delivery_terminal_dup",
		SessionThreadID: "thr_bridge_task_delivery_terminal_dup",
		RuntimeInputID:  "task_notification:task_bridge_delivery_terminal_dup",
		InputKind:       "task_notification",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_task_delivery_terminal_dup","session_thread_id":"thr_bridge_task_delivery_terminal_dup","runtime_input_id":"task_notification:task_bridge_delivery_terminal_dup","event_ids":[],"input_kind":"task_notification"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand terminal duplicate task notification: %v", err)
	}
	if !plan.StaleAccepted || plan.hasCommand() || plan.TaskNotification != nil {
		t.Fatalf("plan = %#v; want terminal duplicate stale accepted without runtime command", plan)
	}
	assertNoRuntimeInboxRow(t, admin, "task_notification:task_bridge_delivery_terminal_dup")
	if len(resolver.jobs) != 0 {
		t.Fatalf("target resolver jobs = %+v; want none for terminal duplicate task notification", resolver.jobs)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreMarksInboxAcceptedAfterRuntimeAccepts(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept", "bind_bridge_inbox_accept", 1, "pod_uid_inbox_accept")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept", "sevt_inbox_accept", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	job := RuntimeJob{
		JobID:           "qjob_inbox_accept",
		LeaseToken:      "lease_inbox_accept",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_accept",
		SessionThreadID: "thr_bridge_inbox_accept",
		RuntimeInputID:  "rin_inbox_accept",
		EventIDs:        []string{"sevt_inbox_accept"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept","session_thread_id":"thr_bridge_inbox_accept","runtime_input_id":"rin_inbox_accept","event_ids":["sevt_inbox_accept"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery result = %#v; want accepted", result)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read accepted inbox status: %v", err)
	}
	if inboxStatus != "accepted" {
		t.Fatalf("inbox status = %q; want accepted after Runtime ACK", inboxStatus)
	}
}

// A store without a target resolver has no process-unaware fallback: it never
// reads a binding directly to deliver to it or to decide cleanup.
func TestPostgreSQLRuntimeDeliveryStoreWithoutResolverFailsClosed(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_resolver_required"
		threadID  = "thr_resolver_required"
		bindingID = "bind_resolver_required"
		podUID    = "pod_uid_resolver_required"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_resolver_required", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, nil)
	job := RuntimeJob{
		JobID: "qjob_resolver_required", LeaseToken: "lease_resolver_required", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: "rin_resolver_required",
		EventIDs: []string{"sevt_resolver_required"}, SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		PayloadJSON: `{"workspace_id":"default","session_id":"sesn_resolver_required","session_thread_id":"thr_resolver_required","runtime_input_id":"rin_resolver_required","event_ids":["sevt_resolver_required"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	assertVisibilityUnavailable := func(operation string, err error) {
		t.Helper()
		var prepareErr runtimecontrol.PreparationError
		if !errors.As(err, &prepareErr) || prepareErr.Kind != "runtime_visibility_unavailable" || !prepareErr.Retryable {
			t.Fatalf("%s without resolver = %v; want retryable runtime_visibility_unavailable", operation, err)
		}
	}

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	assertVisibilityUnavailable("delivery preparation", err)
	if plan.hasCommand() {
		t.Fatalf("delivery preparation without resolver planned %#v", plan)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id='rin_resolver_required'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read inbox after refused preparation: %v", err)
	}
	if inboxStatus != "queued" {
		t.Fatalf("inbox after refused preparation = %q; want queued", inboxStatus)
	}

	binding := sessionfixture.RuntimePodLostBinding(sessionID, bindingID, 1)
	binding.PodUID, binding.RuntimeProcessID = podUID, "process_"+podUID
	err = store.Client.WithWorkspaceTx(context.Background(), "default", "jobrunner.test_cleanup_target", func(tx *dbconnect.Tx) error {
		_, err := store.cleanupTargetProvenGone(context.Background(), tx, RuntimeJob{Kind: queue.KindCleanupSession, WorkspaceID: "default", SessionID: sessionID, CleanupJobID: "cleanup_resolver_required"}, cleanupSessionClaim{
			WorkspaceID: "default", SessionID: sessionID, BindingID: binding.BindingID, BindingGeneration: binding.BindingGeneration,
			Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, PodIP: binding.PodIP, RuntimeProcessID: binding.RuntimeProcessID,
		})
		return err
	})
	assertVisibilityUnavailable("cleanup target decision", err)
}

func TestPostgreSQLRuntimeDeliveryStorePersistsDistinctInboxesForChunkedBacklog(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_chunked_backlog"
		threadID  = "thr_bridge_chunked_backlog"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_chunked_backlog", 1, "pod_uid_chunked_backlog")
	eventIDs := make([]string, queue.MaxRuntimeInputEventRefsPerJob+1)
	for index := range eventIDs {
		eventIDs[index] = fmt.Sprintf("sevt_bridge_chunk_%04d", index+1)
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventIDs[index], int64(index+1), "user.message", `{"content":[{"type":"text","text":"queued"}]}`)
	}

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	chunks := [][]string{eventIDs[:queue.MaxRuntimeInputEventRefsPerJob], eventIDs[queue.MaxRuntimeInputEventRefsPerJob:]}
	for index, chunk := range chunks {
		runtimeInputID := fmt.Sprintf("rin_bridge_chunk_%d", index+1)
		payload, err := json.Marshal(map[string]any{
			"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
			"runtime_input_id": runtimeInputID, "event_ids": chunk,
			"sequence_from": index*queue.MaxRuntimeInputEventRefsPerJob + 1,
			"sequence_to":   index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk),
			"input_kind":    "messages",
		})
		if err != nil {
			t.Fatalf("marshal chunk %d: %v", index+1, err)
		}
		job := RuntimeJob{
			JobID: fmt.Sprintf("qjob_bridge_chunk_%d", index+1), LeaseToken: fmt.Sprintf("lease_bridge_chunk_%d", index+1),
			Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID,
			SessionThreadID: threadID,
			RuntimeInputID:  runtimeInputID, EventIDs: chunk,
			SequenceFrom: int64(index*queue.MaxRuntimeInputEventRefsPerJob + 1),
			SequenceTo:   int64(index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk)),
			InputKind:    "messages",
			PayloadJSON:  string(payload),
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand chunk %d: %v", index+1, err)
		}
		if plan.AcceptInput == nil || plan.AcceptInput.GetRuntimeInputId() != runtimeInputID {
			t.Fatalf("chunk %d plan = %#v; want runtime input %s", index+1, plan, runtimeInputID)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_runtime_inbox SET status = 'committed', updated_at = '2026-01-01T00:04:05Z'
			  WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID); err != nil {
			t.Fatalf("commit chunk %d inbox: %v", index+1, err)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events SET processed_at = '2026-01-01T00:04:05Z'
			  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
			    AND sequence BETWEEN $3 AND $4`, sessionID, threadID,
			index*queue.MaxRuntimeInputEventRefsPerJob+1,
			index*queue.MaxRuntimeInputEventRefsPerJob+len(chunk)); err != nil {
			t.Fatalf("settle chunk %d events: %v", index+1, err)
		}
	}

	var inboxCount int
	var distinctInputs int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), count(DISTINCT runtime_input_id)
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&inboxCount, &distinctInputs); err != nil {
		t.Fatalf("count chunked inbox rows: %v", err)
	}
	if inboxCount != 2 || distinctInputs != 2 {
		t.Fatalf("chunked inbox rows/distinct inputs = %d/%d; want 2/2", inboxCount, distinctInputs)
	}
}

func TestAcceptedMessageCommandPayloadAvoidsHTMLExpansionAtTheAdmissionLimit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_escape_fuse"
		threadID  = "thr_bridge_escape_fuse"
		eventID   = "sevt_bridge_escape_fuse"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)

	const bodyCap = 1 << 20
	prefix := `{"content":[{"type":"text","text":"`
	suffix := `"}]}`
	repeated := strings.Repeat(`&<\"`, (bodyCap-len(prefix)-len(suffix))/4)
	payloadJSON := prefix + repeated + suffix
	if len(payloadJSON) > bodyCap {
		t.Fatalf("fixture payload bytes = %d; want at most %d", len(payloadJSON), bodyCap)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", payloadJSON)

	commandPayload := bridgeAcceptedMessageDeliveryPayload(
		t,
		runtime,
		"default",
		sessionID,
		threadID,
		"rin_bridge_escape_fuse",
		[]string{eventID},
		1,
		1,
	)

	if len(commandPayload) > 2*1024*1024 {
		t.Fatalf("command payload bytes = %d; want within 2 MiB payload fuse", len(commandPayload))
	}
	if len(commandPayload) >= 4*1024*1024 {
		t.Fatalf("command payload bytes = %d; want headroom within 4 MiB channel fuse", len(commandPayload))
	}
	for _, escaped := range []string{`\u0026`, `\u003c`} {
		if strings.Contains(commandPayload, escaped) {
			t.Fatalf("command payload contains HTML escape %q", escaped)
		}
	}
}

func TestPostgreSQLRuntimeDeliveryStoreMarkAcceptedFencesRuntimeInboxBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept_fence", "bind_bridge_inbox_accept_fence", 1, "pod_uid_inbox_accept_fence")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence", "sevt_inbox_accept_fence", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_inbox_accept_fence",
		LeaseToken:      "lease_inbox_accept_fence",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_accept_fence",
		SessionThreadID: "thr_bridge_inbox_accept_fence",
		RuntimeInputID:  "rin_inbox_accept_fence",
		EventIDs:        []string{"sevt_inbox_accept_fence"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept_fence","session_thread_id":"thr_bridge_inbox_accept_fence","runtime_input_id":"rin_inbox_accept_fence","event_ids":["sevt_inbox_accept_fence"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	replayedPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand replay: %v", err)
	}
	if replayedPlan.AcceptInput.GetMessagesJson() != plan.AcceptInput.GetMessagesJson() {
		t.Fatalf("replayed message payload = %q; want byte-identical %q", replayedPlan.AcceptInput.GetMessagesJson(), plan.AcceptInput.GetMessagesJson())
	}
	var payload struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal([]byte(plan.AcceptInput.GetMessagesJson()), &payload); err != nil {
		t.Fatalf("decode accepted message payload: %v", err)
	}
	if len(payload.Messages) != 1 {
		t.Fatalf("accepted message payload = %#v; want one context draft", payload.Messages)
	}
	message, err := runtimecontrol.DecodeRuntimeDeclarationObject(string(payload.Messages[0]))
	if err != nil || len(message) != 1 {
		t.Fatalf("accepted message payload = %#v, %v; want parts-only context draft", message, err)
	}
	parts, ok := message["parts"].([]any)
	if !ok || len(parts) != 1 {
		t.Fatalf("accepted message parts = %#v; want one text part", message["parts"])
	}
	part, ok := parts[0].(map[string]any)
	if !ok || len(part) != 3 || part["type"] != "text" || part["text"] != "hello" || part["truncated"] != false {
		t.Fatalf("accepted message part = %#v; want exact narrow text", parts[0])
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET target_pod_uid = 'pod_uid_inbox_accept_fence_other'
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept_fence'`); err != nil {
		t.Fatalf("mutate inbox fence: %v", err)
	}
	_, err = store.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding)
	var prepareErr runtimecontrol.PreparationError
	if !errors.As(err, &prepareErr) || prepareErr.Kind != "runtime_inbox_accept_missing" || !prepareErr.Retryable {
		t.Fatalf("MarkRuntimeInputAccepted fenced err = %v; want retryable runtime_inbox_accept_missing", err)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept_fence'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read fenced accepted inbox status: %v", err)
	}
	if inboxStatus != "delivering" {
		t.Fatalf("fenced accepted inbox status = %q; want delivering", inboxStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreAgentMailAcceptanceDoesNotRegressCommittedMessageInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_message_committed_control"
		threadID  = "thr_message_committed_control"
		bindingID = "bind_message_committed_control"
		podUID    = "pod_message_committed_control"
		inputID   = "rin_message_committed_control"
		eventID   = "evt_message_committed_control"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"control"}]}`)
	job := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID,
		SessionThreadID: threadID, RuntimeInputID: inputID, EventIDs: []string{eventID},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"` + inputID + `","event_ids":["` + eventID + `"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("prepare message input: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='committed'
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, inputID); err != nil {
		t.Fatalf("mark message input committed: %v", err)
	}
	if settled, err := store.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding); err != nil || settled {
		t.Fatalf("mark committed message input accepted = settled:%t err:%v", settled, err)
	}
	var statusValue string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, inputID).Scan(&statusValue); err != nil {
		t.Fatalf("read committed message control: %v", err)
	}
	if statusValue != "committed" {
		t.Fatalf("committed non-agent-mail status = %q; want committed", statusValue)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRejectsRuntimeInboxPayloadConflict(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_conflict", "bind_bridge_inbox_conflict", 1, "pod_uid_inbox_conflict")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_one", 1, "user.message", `{"content":[{"type":"text","text":"one"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_two", 2, "user.message", `{"content":[{"type":"text","text":"two"}]}`)

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 5, 0, time.UTC) }
	first := RuntimeJob{
		JobID:           "qjob_inbox_conflict_one",
		LeaseToken:      "lease_inbox_conflict_one",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_conflict",
		SessionThreadID: "thr_bridge_inbox_conflict",
		RuntimeInputID:  "rin_inbox_conflict",
		EventIDs:        []string{"sevt_inbox_conflict_one"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_one"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, first)
	if _, err := store.PrepareRuntimeCommand(context.Background(), first); err != nil {
		t.Fatalf("PrepareRuntimeCommand first: %v", err)
	}
	second := first
	second.JobID = "qjob_inbox_conflict_two"
	second.LeaseToken = "lease_inbox_conflict_two"
	second.EventIDs = []string{"sevt_inbox_conflict_two"}
	second.SequenceFrom = 2
	second.SequenceTo = 2
	second.PayloadJSON = `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_two"],"sequence_from":2,"sequence_to":2,"input_kind":"messages"}`

	_, err := store.PrepareRuntimeCommand(context.Background(), second)
	var prepareErr runtimecontrol.PreparationError
	if !errors.As(err, &prepareErr) || prepareErr.Kind != "runtime_inbox_payload_conflict" || prepareErr.Retryable {
		t.Fatalf("PrepareRuntimeCommand conflicting replay err = %v; want terminal runtime_inbox_payload_conflict", err)
	}
	var eventIDsJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_ids_json
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_conflict'`).Scan(&eventIDsJSON); err != nil {
		t.Fatalf("read conflict inbox event ids: %v", err)
	}
	if eventIDsJSON != `["sevt_inbox_conflict_one"]` {
		t.Fatalf("event_ids_json = %s; want original payload preserved", eventIDsJSON)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreBuildsControlPayloadsFromSourceEvents(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_control_delivery", "bind_bridge_control_delivery", 1, "pod_uid_control_delivery")
	seedBridgeAPIOpenDurableTurn(t, admin, sessionfixture.BridgeAPIScope("sesn_bridge_control_delivery", "thr_bridge_control_delivery", "bind_bridge_control_delivery", 1, "pod_uid_control_delivery"), "sevt_control_delivery_run")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_interrupt_control", 2, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_confirmation_control", 3, "user.tool_confirmation", `{"tool_use_id":"sevt_tool_control","result":"deny","deny_message":"not now"}`)

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC) }

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "rin_bridge_interrupt", "interrupt_control", "sevt_interrupt_control", 2, 3, time.Now().UTC().Add(-time.Minute))
	leasedInterrupt := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "control-payload-test",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	interruptJob, err := DecodeRuntimeJob(queueJobProto(leasedInterrupt))
	if err != nil {
		t.Fatalf("decode interrupt control lease: %v", err)
	}
	seedRuntimeInboxBirthForJob(t, admin, interruptJob)
	interrupt, err := store.PrepareRuntimeCommand(context.Background(), interruptJob)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand interrupt: %v", err)
	}
	if interrupt.Interrupt == nil || interrupt.Interrupt.GetRuntimeInputId() != interruptJob.RuntimeInputID ||
		interrupt.Interrupt.GetInterruptLeaseRef().GetJobId() != interruptJob.JobID || interrupt.Interrupt.GetOrigin() != agentruntimev1.InterruptOrigin_INTERRUPT_ORIGIN_USER {
		t.Fatalf("interrupt runtime request = %#v; want typed user interrupt", interrupt.Interrupt)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_control_delivery'
		    AND event_id = 'sevt_interrupt_control'`); err != nil {
		t.Fatalf("mark interrupt event processed: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET status = 'committed',
		        committed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_control_delivery'
		    AND runtime_input_id = 'rin_bridge_interrupt'`); err != nil {
		t.Fatalf("mark interrupt inbox committed: %v", err)
	}

	confirmationJob := RuntimeJob{
		JobID:           "qjob_bridge_confirmation",
		LeaseToken:      "lease_bridge_confirmation",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_control_delivery",
		SessionThreadID: "thr_bridge_control_delivery",
		RuntimeInputID:  "rin_bridge_confirmation",
		EventIDs:        []string{"sevt_confirmation_control"},
		SequenceFrom:    3,
		SequenceTo:      3,
		InputKind:       "tool_confirmation",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_confirmation","event_ids":["sevt_confirmation_control"],"sequence_from":3,"sequence_to":3,"input_kind":"tool_confirmation"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, confirmationJob)
	confirmation, err := store.PrepareRuntimeCommand(context.Background(), confirmationJob)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand tool confirmation: %v", err)
	}
	if confirmation.ToolConfirmation == nil ||
		confirmation.ToolConfirmation.GetRuntimeInputId() != confirmationJob.RuntimeInputID ||
		confirmation.ToolConfirmation.GetToolUseEventId() != "sevt_tool_control" ||
		confirmation.ToolConfirmation.GetDecision() != agentruntimev1.ToolConfirmationDecision_TOOL_CONFIRMATION_DECISION_DENY ||
		confirmation.ToolConfirmation.GetDenyMessage() != "not now" {
		t.Fatalf("tool confirmation runtime request = %#v; want typed deny decision", confirmation.ToolConfirmation)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreClaimsBindingFromKubernetesVisibility(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_resolve", "thr_bridge_resolve")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_resolve", "thr_bridge_resolve", "evt_bridge_resolve", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-a",
		PodUID:    "pod-uid-a",
		PodIP:     "10.0.0.25",
	}
	registerPlacementCandidateForTest(t, admin, candidate)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, KubernetesRuntimeTargetResolver{LoadClient: runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, runtimeLoadFixture(0)) })),
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	})
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_bridge_resolve",
		LeaseToken:      "lease_bridge_resolve",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_resolve",
		SessionThreadID: "thr_bridge_resolve",
		RuntimeInputID:  "rin_bridge_resolve",
		EventIDs:        []string{"evt_bridge_resolve"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	if plan.Target.PodName != candidate.PodName || plan.Target.PodUID != candidate.PodUID || plan.Target.PodIP != candidate.PodIP {
		t.Fatalf("target = %+v; want candidate %+v", plan.Target, candidate)
	}
	if plan.AttemptedBinding.BindingID == "" || plan.AttemptedBinding.Generation == 0 || plan.AttemptedBinding.TargetPodUID != candidate.PodUID {
		t.Fatalf("attempted binding = %+v; want claimed binding and target identity", plan.AttemptedBinding)
	}

	var podName string
	var podUID string
	var podIP string
	var generation int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, binding_generation
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_resolve'`).Scan(&podName, &podUID, &podIP, &generation); err != nil {
		t.Fatalf("read claimed binding: %v", err)
	}
	if podName != candidate.PodName || podUID != candidate.PodUID || podIP != candidate.PodIP || generation != plan.AttemptedBinding.Generation {
		t.Fatalf("claimed binding = %s/%s/%s gen %d; want %s/%s/%s gen %d", podName, podUID, podIP, generation, candidate.PodName, candidate.PodUID, candidate.PodIP, plan.AttemptedBinding.Generation)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreConvertsOversizedInputToBoundedLoopRejection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_runtime_rejected", "bind_bridge_runtime_rejected", 1, "pod_uid_runtime_rejected")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected", "evt_bridge_runtime_rejected", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"hello"}]}`)
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_bridge_runtime_rejected",
		LeaseToken:      "lease_bridge_runtime_rejected",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_runtime_rejected",
		SessionThreadID: "thr_bridge_runtime_rejected",
		RuntimeInputID:  "rin_bridge_runtime_rejected",
		EventIDs:        []string{"evt_bridge_runtime_rejected"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_runtime_rejected"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand runtime rejection: %v", err)
	}
	if got := plan.AcceptInput.GetRuntimeInputId(); got != "rin_bridge_runtime_rejected" {
		t.Fatalf("prepared runtime input id = %q; want rin_bridge_runtime_rejected", got)
	}
	result := RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    false,
		ErrorKind:    "runtime_command_payload_too_large",
		ErrorMessage: "runtime command exceeds the transport fuse",
	}
	converted, err := store.PrepareRuntimeInputRejection(context.Background(), job, result)
	if err != nil {
		t.Fatalf("PrepareRuntimeInputRejection: %T %v", err, err)
	}
	if !converted {
		t.Fatal("PrepareRuntimeInputRejection converted = false; want true")
	}

	var inboxStatus string
	var inboxKind string
	var rejectionReason string
	var inboxUpdatedAt string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, input_kind, rejection_reason_code, updated_at
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_bridge_runtime_rejected'`).Scan(&inboxStatus, &inboxKind, &rejectionReason, &inboxUpdatedAt); err != nil {
		t.Fatalf("read rejected runtime inbox: %v", err)
	}
	if inboxStatus != "delivering" || inboxKind != "rejection" ||
		rejectionReason != "runtime_command_payload_too_large" ||
		inboxUpdatedAt != "2026-01-01T00:02:00Z" {
		t.Fatalf("rejected inbox status/kind/reason/updatedAt = %q/%q/%q/%q; want delivering bounded rejection",
			inboxStatus, inboxKind, rejectionReason, inboxUpdatedAt)
	}
	var inputProcessedAt sql.NullString
	var inputRevision int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at, revision
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_runtime_rejected'`).Scan(&inputProcessedAt, &inputRevision); err != nil {
		t.Fatalf("read rejected input event: %v", err)
	}
	if inputProcessedAt.Valid || inputRevision != 1 {
		t.Fatalf("rejected input event processed=%v revision=%d; want untouched source until loop commit", inputProcessedAt.Valid, inputRevision)
	}

	var errorEventCount int
	var messageProjectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_runtime_rejected'
		    AND type = 'session.error'`).Scan(&errorEventCount); err != nil {
		t.Fatalf("count runtime rejection session.error rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_runtime_rejected'`).Scan(&messageProjectionCount); err != nil {
		t.Fatalf("count runtime rejection message projections: %v", err)
	}
	if errorEventCount != 0 || messageProjectionCount != 0 {
		t.Fatalf("Bridge-authored rejection content = events %d messages %d; want none", errorEventCount, messageProjectionCount)
	}

	retryPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand bounded rejection: %v", err)
	}
	if retryPlan.AcceptInput == nil || retryPlan.AcceptInput.GetRejection().GetReason() != agentruntimev1.AcceptInputRejectionReason_ACCEPT_INPUT_REJECTION_REASON_PAYLOAD_TOO_LARGE {
		t.Fatalf("bounded rejection request = %+v; want typed payload-too-large fact", retryPlan.AcceptInput)
	}
}

// machineMessageFixture holds one idle Session with a cloud computer, serves
// its api computer routes as the api role, and runs Job Runner's processing,
// as the job_runner role, of runtime_input jobs leased from the real Queue.
// Its resolver can place the Session on one accepting Runtime, so a message
// that wrongly started a run would create a binding; a recording sender
// stands in for that Runtime.
type machineMessageFixture struct {
	t          *testing.T
	ctx        context.Context
	admin      *sql.DB
	workloads  *storagetest.WorkloadDB
	api        *dbconnect.Client
	queue      *queue.PostgreSQLQueueStore
	store      *PostgreSQLRuntimeDeliveryStore
	sender     *recordingRuntimeCommandSender
	runner     *JobRunner
	router     http.Handler
	candidate  enginekubernetes.BindingCandidate
	podLost    bool
	sessionID  string
	mainID     string
	computerID string
	clock      time.Time
}

func newMachineMessageFixture(t *testing.T, sessionID string) *machineMessageFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	f := &machineMessageFixture{t: t, ctx: ctx, admin: admin, sessionID: sessionID, mainID: "sthr_" + sessionID, computerID: "comp_" + sessionID,
		candidate: enginekubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_" + sessionID, PodIP: "10.0.0.10"},
		clock:     time.Now().UTC().Add(-time.Hour)}
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, f.mainID)
	if _, err := admin.ExecContext(ctx, `INSERT INTO computers
		(workspace_id,session_id,id,kind,environment_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ('default',$1,$2,'cloud',$3,'/workspace','{"type":"unrestricted"}','[{"drive_id":"drv_assets","paths":["/cache"]}]','attached','waiting')`,
		sessionID, f.computerID, "env_"+sessionID); err != nil {
		t.Fatalf("seed cloud computer: %v", err)
	}
	registerPlacementCandidateForTest(t, admin, f.candidate)
	f.workloads = storagetest.OpenWorkloadDB(t, admin, "job_runner")
	f.api = dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "api", nil))
	runner := dbconnect.NewClientForTesting(f.workloads.DB)
	f.queue = queue.NewPostgreSQLStore(runner)
	f.store = NewPostgreSQLRuntimeDeliveryStore(runner, 9090, KubernetesRuntimeTargetResolver{
		LoadClient: runtimeLoadTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, runtimeLoadFixture(0)) })),
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			if f.podLost {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
			}
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{f.candidate})
		},
		GetPod: fixtureAbsentRuntimePod,
	})
	f.sender = &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	f.runner = &JobRunner{
		Queue:     tetralqueue.NewServer(f.queue, nil),
		Deliverer: RuntimePodDirectDeliverer{Store: f.store, Sender: f.sender},
		Config:    JobRunnerConfig{LeaseOwner: "machine-message-test", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	service := session.NewService(nil, nil, nil, nil,
		session.NewPostgreSQLSessionStore(f.api, session.WithPageTokenSecret([]byte("machine-message-secret-12345678"))), nil)
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID}, "ak_machine_messages"), nil //nolint:gosec // G101: synthetic test API key id
	})
	f.router = httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
	return f
}

func (f *machineMessageFixture) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f *machineMessageFixture) setStatus(status string) {
	f.t.Helper()
	f.exec(`UPDATE session_runtime_status SET status=$2 WHERE workspace_id='default' AND session_id=$1`, f.sessionID, status)
}

// bind binds the Session to the fixture's Runtime as a placement would.
func (f *machineMessageFixture) bind(bindingID string) runtimecontrol.Binding {
	f.t.Helper()
	f.exec(`INSERT INTO session_runtime_bindings (workspace_id,session_id,binding_id,binding_generation,agent_runtime_namespace,
		agent_runtime_pod_name,agent_runtime_pod_uid,agent_runtime_pod_ip,runtime_process_id,bound_at,updated_at)
		VALUES ('default',$1,$2,1,$3,$4,$5,$6,$7,clock_timestamp(),clock_timestamp())`,
		f.sessionID, bindingID, f.candidate.Namespace, f.candidate.PodName, f.candidate.PodUID, f.candidate.PodIP, "process_"+f.candidate.PodUID)
	f.exec(`UPDATE session_runtime_status SET binding_id=$2,binding_generation=1 WHERE workspace_id='default' AND session_id=$1`, f.sessionID, bindingID)
	return runtimecontrol.Binding{BindingID: bindingID, BindingGeneration: 1, Namespace: f.candidate.Namespace, PodName: f.candidate.PodName,
		PodUID: f.candidate.PodUID, PodIP: f.candidate.PodIP, RuntimeProcessID: "process_" + f.candidate.PodUID}
}

// add sends POST /v1/sessions/{session_id}/computers and returns the status
// and the new computer's ID.
func (f *machineMessageFixture) add(key string) (int, string) {
	f.t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+f.sessionID+"/computers?beta=true", strings.NewReader(`{"environment_id":"env_`+f.sessionID+`"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-api-key", "sk_machine_messages")
	request.Header.Set("Idempotency-Key", key)
	f.router.ServeHTTP(recorder, request)
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &created)
	return recorder.Code, created.ID
}

// write calls WriteMachineMessageTx for the seeded computer in an api
// transaction that commits or rolls back.
func (f *machineMessageFixture) write(change string, commit bool) {
	f.t.Helper()
	f.clock = f.clock.Add(time.Second)
	rollback := errors.New("roll back the computer change")
	err := f.api.WithWorkspaceTx(f.ctx, "default", "test.machine_message", func(tx *dbconnect.Tx) error {
		if err := computer.WriteMachineMessageTx(f.ctx, tx, workspace.DefaultID, f.sessionID, f.computerID, change, f.clock); err != nil {
			return err
		}
		if !commit {
			return rollback
		}
		return nil
	})
	if (commit && err != nil) || (!commit && !errors.Is(err, rollback)) {
		f.t.Fatalf("write %s message (commit %v): %v", change, commit, err)
	}
}

// deliverNext leases the next runtime_input job and runs Job Runner's
// processing of it; it reports false when the Queue has none to lease.
func (f *machineMessageFixture) deliverNext() bool {
	f.t.Helper()
	leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput},
		LeaseOwner: "machine-message-test", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC()})
	if err != nil {
		f.t.Fatalf("lease runtime_input: %v", err)
	}
	if len(leased) == 0 {
		return false
	}
	if err := f.runner.processRuntimeJob(f.ctx, queueJobProto(leased[0]), f.runner.Config); err != nil {
		f.t.Fatalf("process runtime_input %s: %v", leased[0].ID, err)
	}
	return true
}

// cleanUpIdleWithPodGone makes the Session idle with its cleanup due, then
// runs the idle cleanup job as Job Runner does after the Runtime pod is gone,
// and requires the binding deleted.
func (f *machineMessageFixture) cleanUpIdleWithPodGone() {
	f.t.Helper()
	seedBridgeAPIEvent(f.t, f.admin, "default", f.sessionID, f.mainID, "sevt_idle_"+f.sessionID, 1, "session.status_idle", `{"type":"session.status_idle"}`)
	f.exec(`UPDATE session_events SET latest_stream_position=1 WHERE workspace_id='default' AND event_id=$1`, "sevt_idle_"+f.sessionID)
	f.exec(`UPDATE session_runtime_status SET status='idle', status_event_id=$2, cleanup_job_id='cleanup_mmsg', cleanup_enqueued_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, f.sessionID, "sevt_idle_"+f.sessionID)
	f.podLost = true
	if _, err := f.queue.Enqueue(f.ctx, queue.EnqueueRequest{
		ID: id.New(queue.JobIDPrefix), WorkspaceID: workspace.DefaultID, Kind: queue.KindCleanupSession,
		PartitionKey: queue.FormatSessionPartitionKey(workspace.DefaultID, f.sessionID),
		DedupeKey:    queue.FormatCleanupSessionDedupeKey(workspace.DefaultID, f.sessionID, "cleanup_mmsg"), PayloadVersion: 1,
		PayloadJSON: []byte(`{"workspace_id":"default","session_id":"` + f.sessionID + `","cleanup_job_id":"cleanup_mmsg"}`), MaxAttempts: 2,
	}); err != nil {
		f.t.Fatalf("enqueue the idle cleanup: %v", err)
	}
	leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindCleanupSession},
		LeaseOwner: "machine-message-test", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC()})
	if err != nil || len(leased) != 1 {
		f.t.Fatalf("lease the idle cleanup = %v, %v", leased, err)
	}
	if err := f.runner.processRuntimeJob(f.ctx, queueJobProto(leased[0]), f.runner.Config); err != nil {
		f.t.Fatalf("process the idle cleanup: %v", err)
	}
	if _, bindings, _ := f.sessionFacts(); bindings != 0 {
		f.t.Fatalf("bindings after the idle cleanup of a gone pod = %d; want 0", bindings)
	}
}

// placeUserMessage prepares a user message of the main thread as Job Runner
// does, which places the Session on the fixture's candidate Runtime, and
// returns the new binding's ID.
func (f *machineMessageFixture) placeUserMessage(name string) string {
	f.t.Helper()
	var sequence int64
	if err := f.admin.QueryRowContext(f.ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`,
		f.sessionID, f.mainID).Scan(&sequence); err != nil {
		f.t.Fatal(err)
	}
	seedBridgeAPIEvent(f.t, f.admin, "default", f.sessionID, f.mainID, "sevt_"+name, sequence, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	input := RuntimeJob{
		JobID: "qjob_" + name, LeaseToken: "lease_" + name, Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: f.sessionID, SessionThreadID: f.mainID, RuntimeInputID: "rin_" + name,
		EventIDs: []string{"sevt_" + name}, SequenceFrom: sequence, SequenceTo: sequence, InputKind: "messages", PayloadJSON: `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(f.t, f.admin, input)
	plan, err := f.store.PrepareRuntimeCommand(f.ctx, input)
	if err != nil || plan.AttemptedBinding.BindingID == "" {
		f.t.Fatalf("messages placement = %+v/%v; want a new binding", plan.AttemptedBinding, err)
	}
	return plan.AttemptedBinding.BindingID
}

func (f *machineMessageFixture) deliverAll() int {
	f.t.Helper()
	processed := 0
	for f.deliverNext() {
		processed++
	}
	return processed
}

type machineMessageState struct {
	ID, Change, Payload, Status string
	Bound                       bool
	ActiveJobs, AckedJobs       int
}

// messages returns the Session's machine messages in writing order with their
// inbox row and jobs; no observation may find one parked.
func (f *machineMessageFixture) messages() []machineMessageState {
	f.t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT m.runtime_input_id, m.change, m.payload_json, i.status, i.binding_id IS NOT NULL,
		(SELECT count(*) FROM queue_jobs j WHERE j.workspace_id=m.workspace_id AND j.kind='runtime_input'
		   AND j.dedupe_key='runtime_input:'||m.workspace_id||':'||m.session_id||':'||m.runtime_input_id AND j.status IN ('pending','leased')),
		(SELECT count(*) FROM queue_jobs j WHERE j.workspace_id=m.workspace_id AND j.kind='runtime_input'
		   AND j.dedupe_key='runtime_input:'||m.workspace_id||':'||m.session_id||':'||m.runtime_input_id AND j.status='acknowledged')
		FROM session_machine_messages m
		JOIN session_runtime_inbox i ON i.workspace_id=m.workspace_id AND i.runtime_input_id=m.runtime_input_id
		WHERE m.workspace_id='default' AND m.session_id=$1 ORDER BY m.created_at, m.runtime_input_id`, f.sessionID)
	if err != nil {
		f.t.Fatalf("read machine messages: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var states []machineMessageState
	for rows.Next() {
		var state machineMessageState
		if err := rows.Scan(&state.ID, &state.Change, &state.Payload, &state.Status, &state.Bound, &state.ActiveJobs, &state.AckedJobs); err != nil {
			f.t.Fatal(err)
		}
		if state.Status == "parked" {
			f.t.Fatalf("machine message %s is parked", state.ID)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	var orphans int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='machine_message'`,
		f.sessionID).Scan(&orphans); err != nil || orphans != len(states) {
		f.t.Fatalf("machine_message inbox rows = %d/%v; want one per message (%d)", orphans, err, len(states))
	}
	return states
}

// sessionFacts reads the Session's status, its bindings and its
// session.status_running events.
func (f *machineMessageFixture) sessionFacts() (string, int, int) {
	f.t.Helper()
	var status string
	var bindings, running int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_running')`,
		f.sessionID).Scan(&status, &bindings, &running); err != nil {
		f.t.Fatal(err)
	}
	return status, bindings, running
}

func (f *machineMessageFixture) nonStartingRequests() []*agentruntimev1.AcceptNonStartingInputRequest {
	var requests []*agentruntimev1.AcceptNonStartingInputRequest
	for _, request := range f.sender.requests {
		accept, ok := request.(*agentruntimev1.AcceptNonStartingInputRequest)
		if !ok {
			f.t.Fatalf("Runtime received %T; want only AcceptNonStartingInput", request)
		}
		requests = append(requests, accept)
	}
	return requests
}

// An addition while the Session runs is refused and
// writes no message; one to an idle Session writes one added message, which
// Job Runner accepts with no binding, so no Runtime is placed and no run
// starts. A message still queued or accepted never makes the next addition or
// the archive of the main thread look busy.
func TestComputerAdditionMessageNeverStartsARun(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_mmsg_add")
	f.setStatus("running")
	if code, _ := f.add("add-while-running"); code != http.StatusConflict {
		t.Fatalf("addition while running = %d; want 409", code)
	}
	if got := f.messages(); len(got) != 0 {
		t.Fatalf("refused addition wrote %+v; want no message", got)
	}
	f.setStatus("idle")
	code, firstID := f.add("add-first")
	if code != http.StatusCreated {
		t.Fatalf("addition while idle = %d; want 201", code)
	}
	added := f.messages()
	var payload struct {
		Change string              `json:"change"`
		Row    computer.RuntimeRow `json:"row"`
	}
	if len(added) != 1 || added[0].Change != computer.ChangeAdded || added[0].Status != "queued" || added[0].ActiveJobs != 1 ||
		json.Unmarshal([]byte(added[0].Payload), &payload) != nil || payload.Change != computer.ChangeAdded || payload.Row.ComputerID != firstID ||
		payload.Row.Kind != computer.KindCloud || payload.Row.WorkingDirectory != computer.DefaultCloudWorkingDirectory {
		t.Fatalf("addition messages = %+v; want one queued added message with one job carrying the Runtime row of %s", added, firstID)
	}
	if code, _ := f.add("add-second-while-first-queued"); code != http.StatusCreated {
		t.Fatalf("addition with a queued message = %d; want 201", code)
	}

	if processed := f.deliverAll(); processed != 2 {
		t.Fatalf("processed %d jobs; want the two messages'", processed)
	}
	for _, message := range f.messages() {
		if message.Status != "accepted" || message.Bound || message.ActiveJobs != 0 || message.AckedJobs != 1 {
			t.Fatalf("message after Job Runner = %+v; want accepted with no binding and its job acknowledged", message)
		}
	}
	if status, bindings, running := f.sessionFacts(); status != "idle" || bindings != 0 || running != 0 || len(f.sender.requests) != 0 {
		t.Fatalf("Session after its messages: status %s, %d bindings, %d running events, %d Runtime calls; want idle and none",
			status, bindings, running, len(f.sender.requests))
	}
	if code, _ := f.add("add-third-while-accepted"); code != http.StatusCreated {
		t.Fatalf("addition with accepted messages = %d; want 201", code)
	}
	store := session.NewPostgreSQLSessionStore(f.api, session.WithPageTokenSecret([]byte("machine-message-secret-12345678")))
	if thread, err := store.ArchiveThread(f.ctx, workspace.DefaultID, f.sessionID, f.mainID, time.Now().UTC()); err != nil || thread.ArchivedAt == nil {
		t.Fatalf("main thread archive with a queued and two accepted messages = %+v, %v; want archived", thread, err)
	}
}

// A message for a Session that a Runtime holds reaches that Runtime once with
// the stored payload, and its row ends accepted under the binding.
func TestMachineMessageReachesTheRuntimeHoldingTheSession(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_mmsg_bound")
	binding := f.bind("bind_mmsg_bound")
	f.write(computer.ChangeAdded, true)
	if processed := f.deliverAll(); processed != 1 {
		t.Fatalf("processed %d jobs; want one", processed)
	}
	message := f.messages()[0]
	requests := f.nonStartingRequests()
	if len(requests) != 1 || requests[0].GetPayloadJson() != message.Payload || requests[0].GetInputKind() != queue.RuntimeInputKindMachineMessage ||
		requests[0].GetRuntimeInputId() != message.ID || requests[0].GetSessionThreadId() != f.mainID ||
		requests[0].GetBindingId() != binding.BindingID || requests[0].GetTargetPodUid() != binding.PodUID || requests[0].GetRuntimeProcessId() != binding.RuntimeProcessID {
		t.Fatalf("Runtime requests = %v; want one AcceptNonStartingInput to %s with the stored payload of %s", requests, binding.BindingID, message.ID)
	}
	if message.Status != "accepted" || !message.Bound || message.ActiveJobs != 0 {
		t.Fatalf("message = %+v; want accepted under the binding with no active job", message)
	}
}

// A replaced message with no Runtime stays queued with no job; the binding
// transaction of the next placement enqueues it, and it reaches that Runtime
// once.
func TestReplacedMessageWaitsForTheNextRuntime(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_mmsg_replaced")
	f.write(computer.ChangeReplaced, true)
	if processed := f.deliverAll(); processed != 1 {
		t.Fatalf("processed %d jobs; want one", processed)
	}
	if message := f.messages()[0]; message.Status != "queued" || message.Bound || message.ActiveJobs != 0 || message.AckedJobs != 1 {
		t.Fatalf("replaced message with no Runtime = %+v; want queued with no job", message)
	}
	if _, bindings, _ := f.sessionFacts(); bindings != 0 || len(f.sender.requests) != 0 {
		t.Fatalf("replaced message made %d bindings and %d Runtime calls; want none", bindings, len(f.sender.requests))
	}

	placed := f.placeUserMessage("mmsg_replaced_user")
	if message := f.messages()[0]; message.Status != "queued" || message.ActiveJobs != 1 {
		t.Fatalf("replaced message after the binding = %+v; want queued with one job", message)
	}
	if processed := f.deliverAll(); processed != 1 {
		t.Fatalf("processed %d jobs; want the replaced message's", processed)
	}
	message := f.messages()[0]
	if requests := f.nonStartingRequests(); len(requests) != 1 || requests[0].GetRuntimeInputId() != message.ID || requests[0].GetBindingId() != placed {
		t.Fatalf("Runtime requests = %v; want the replaced message once, to %s", requests, placed)
	}
	if message.Status != "accepted" || !message.Bound || message.ActiveJobs != 0 {
		t.Fatalf("delivered replaced message = %+v; want accepted under the new binding", message)
	}
}

// Pod loss returns accepted messages to queued with a job each; with no new
// binding the added one is accepted with no binding and the replaced one
// waits queued.
func TestPodLossHandsBackAcceptedMessages(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_mmsg_pod_loss")
	f.bind("bind_mmsg_pod_loss")
	f.write(computer.ChangeAdded, true)
	f.write(computer.ChangeReplaced, true)
	if processed := f.deliverAll(); processed != 2 {
		t.Fatalf("processed %d jobs; want two", processed)
	}
	for _, message := range f.messages() {
		if message.Status != "accepted" || !message.Bound {
			t.Fatalf("message before the pod loss = %+v; want accepted under the binding", message)
		}
	}
	repair := runtimePodLossSweepStore(t, f.workloads.DB, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := runRuntimePodLossRepair(f.ctx, repair); err != nil || repaired != 1 {
		t.Fatalf("pod-loss repair = %d/%v; want 1/nil", repaired, err)
	}
	for _, message := range f.messages() {
		if message.Status != "queued" || message.Bound || message.ActiveJobs != 1 {
			t.Fatalf("message after the pod loss = %+v; want queued with one job", message)
		}
	}
	if processed := f.deliverAll(); processed != 2 {
		t.Fatalf("processed %d jobs; want two", processed)
	}
	got := f.messages()
	if got[0].Change != computer.ChangeAdded || got[0].Status != "accepted" || got[0].Bound || got[0].ActiveJobs != 0 ||
		got[1].Change != computer.ChangeReplaced || got[1].Status != "queued" || got[1].ActiveJobs != 0 {
		t.Fatalf("messages after the pod loss with no Runtime = %+v; want added accepted with no binding and replaced queued", got)
	}
	if _, bindings, _ := f.sessionFacts(); bindings != 0 || len(f.sender.requests) != 2 {
		t.Fatalf("after the pod loss: %d bindings, %d Runtime calls; want no binding and only the two first deliveries", bindings, len(f.sender.requests))
	}
}

// A pod loss while a machine message is delivering hands the message back
// with its job. Prepared again with no binding, the job leaves an added
// message accepted and a replaced one queued, both with no binding. When
// another input's preparation repairs the loss and places a new Runtime
// first, the job delivers the message once, to that Runtime.
func TestPodLossDuringDeliveryHandsBackTheDeliveringMessage(t *testing.T) {
	for _, tc := range []struct {
		name, session, change, want string
		newRuntime                  bool
	}{
		{name: "added", session: "sesn_mmsg_lost_added", change: computer.ChangeAdded, want: "accepted"},
		{name: "replaced", session: "sesn_mmsg_lost_replaced", change: computer.ChangeReplaced, want: "queued"},
		{name: "added with a new Runtime placed first", session: "sesn_mmsg_lost_placed", change: computer.ChangeAdded, want: "accepted", newRuntime: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMachineMessageFixture(t, tc.session)
			f.bind("bind_" + tc.session)
			f.sender.result = RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true,
				ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime transport unavailable"}
			f.write(tc.change, true)
			if processed := f.deliverAll(); processed != 1 {
				t.Fatalf("processed %d jobs; want one", processed)
			}
			if got := f.messages(); got[0].Status != "delivering" || !got[0].Bound || got[0].ActiveJobs != 1 {
				t.Fatalf("message before the pod loss = %+v; want delivering under the binding with its job", got[0])
			}
			placed, calls, bindings := "", 1, 0
			if tc.newRuntime {
				f.candidate = enginekubernetes.BindingCandidate{Namespace: f.candidate.Namespace, PodName: "runtime-pod-1", PodUID: "pod_new_" + tc.session, PodIP: "10.0.0.11"}
				registerPlacementCandidateForTest(t, f.admin, f.candidate)
				f.sender.result = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
				placed, calls, bindings = f.placeUserMessage("mmsg_lost_user"), 2, 1
			} else {
				f.podLost = true
			}
			f.exec(`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND kind='runtime_input' AND status='pending'`)
			if processed := f.deliverAll(); processed != 1 {
				t.Fatalf("processed %d jobs after the pod loss; want one", processed)
			}
			if got := f.messages(); got[0].Status != tc.want || got[0].Bound != tc.newRuntime || got[0].ActiveJobs != 0 {
				t.Fatalf("message after the pod loss = %+v; want %s, bound %v, with no job", got[0], tc.want, tc.newRuntime)
			}
			requests := f.nonStartingRequests()
			if len(requests) != calls || (tc.newRuntime && requests[1].GetBindingId() != placed) {
				t.Fatalf("Runtime requests = %v; want %d, the last to %q", requests, calls, placed)
			}
			var deadLettered int
			if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND status=$1`, queue.StatusDeadLettered).Scan(&deadLettered); err != nil {
				t.Fatal(err)
			}
			if _, got, _ := f.sessionFacts(); got != bindings || deadLettered != 0 {
				t.Fatalf("after the pod loss: %d bindings, %d dead-lettered jobs; want %d and 0", got, deadLettered, bindings)
			}
		})
	}
}

// Idle cleanup that finds the Runtime pod gone hands the binding's messages
// back before it deletes the binding: an accepted one returns to queued with
// a new job, and a delivering one keeps its job, which delivers it once to the
// Runtime that holds the Session next.
func TestIdleCleanupOfAGonePodHandsBackMachineMessages(t *testing.T) {
	for _, tc := range []struct {
		name, session string
		delivering    bool
	}{
		{name: "accepted", session: "sesn_mmsg_gone_accepted"},
		{name: "delivering", session: "sesn_mmsg_gone_delivering", delivering: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMachineMessageFixture(t, tc.session)
			f.bind("bind_" + tc.session)
			if tc.delivering {
				f.sender.result = RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true,
					ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime transport unavailable"}
			}
			f.write(computer.ChangeAdded, true)
			if processed := f.deliverAll(); processed != 1 {
				t.Fatalf("processed %d jobs; want one", processed)
			}
			want := map[bool]string{false: "accepted", true: "delivering"}[tc.delivering]
			if got := f.messages()[0]; got.Status != want || !got.Bound {
				t.Fatalf("message before the cleanup = %+v; want %s under the binding", got, want)
			}
			f.cleanUpIdleWithPodGone()
			if got := f.messages()[0]; got.Status != "queued" || got.Bound || got.ActiveJobs != 1 {
				t.Fatalf("message after the idle cleanup = %+v; want queued with no binding and one job", got)
			}
			if !tc.delivering {
				return
			}
			f.candidate = enginekubernetes.BindingCandidate{Namespace: f.candidate.Namespace, PodName: "runtime-pod-1", PodUID: "pod_new_" + tc.session, PodIP: "10.0.0.11"}
			registerPlacementCandidateForTest(t, f.admin, f.candidate)
			f.podLost = false
			f.sender.result = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
			placed := f.placeUserMessage("mmsg_gone_user")
			f.exec(`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND kind='runtime_input' AND status='pending'`)
			if processed := f.deliverAll(); processed != 1 {
				t.Fatalf("processed %d jobs after the new placement; want one", processed)
			}
			requests := f.nonStartingRequests()
			if got := f.messages()[0]; got.Status != "accepted" || !got.Bound || got.ActiveJobs != 0 || len(requests) != 2 || requests[1].GetBindingId() != placed {
				t.Fatalf("message after the new placement = %+v with requests %v; want accepted, delivered once more, to %s", got, requests, placed)
			}
		})
	}
}

// WriteMachineMessageTx writes the message and its input in its caller's
// transaction: one row and one job with the computer's Runtime row after a
// commit, nothing after a rollback.
func TestMachineMessageFollowsItsTransaction(t *testing.T) {
	for _, change := range []string{computer.ChangeRemoved, computer.ChangeReplaced} {
		t.Run(change, func(t *testing.T) {
			f := newMachineMessageFixture(t, "sesn_mmsg_tx_"+change)
			f.write(change, false)
			if got := f.messages(); len(got) != 0 {
				t.Fatalf("rolled-back %s wrote %+v", change, got)
			}
			f.write(change, true)
			got := f.messages()
			want := `{"change":"` + change + `","row":{"computerId":"` + f.computerID + `","kind":"cloud","workingDirectory":"/workspace",` +
				`"networking":{"type":"unrestricted"},"repositories":[],"driveExclude":[{"driveId":"drv_assets","paths":["/cache"]}]}}`
			if len(got) != 1 || got[0].Change != change || got[0].Payload != want || got[0].Status != "queued" || got[0].Bound || got[0].ActiveJobs != 1 ||
				!strings.HasPrefix(got[0].ID, "machine_message:mmsg_") {
				t.Fatalf("committed %s = %+v; want one queued message %s with one job", change, got, want)
			}
			var eventIDs, payload string
			var sequenceFrom, sequenceTo sql.NullInt64
			var priority int
			if err := f.admin.QueryRowContext(f.ctx, `SELECT i.event_ids_json, i.sequence_from, i.sequence_to, j.payload_json, j.priority
				FROM session_runtime_inbox i JOIN queue_jobs j ON j.workspace_id=i.workspace_id AND j.kind='runtime_input'
				 AND j.dedupe_key='runtime_input:'||i.workspace_id||':'||i.session_id||':'||i.runtime_input_id
				WHERE i.workspace_id='default' AND i.runtime_input_id=$1`, got[0].ID).Scan(&eventIDs, &sequenceFrom, &sequenceTo, &payload, &priority); err != nil {
				t.Fatal(err)
			}
			wantJob := `{"workspace_id":"default","session_id":"` + f.sessionID + `","session_thread_id":"` + f.mainID + `","runtime_input_id":"` + got[0].ID +
				`","event_ids":[],"sequence_from":0,"sequence_to":0,"input_kind":"machine_message"}`
			if eventIDs != "[]" || sequenceFrom.Valid || sequenceTo.Valid || payload != wantJob || priority != 0 {
				t.Fatalf("input = events %s sequences %v/%v, job payload %s priority %d; want no events or sequences and job %s at priority 0",
					eventIDs, sequenceFrom, sequenceTo, payload, priority, wantJob)
			}
		})
	}
}

// Releasing a binding leaves a notice the Runtime accepted, and a notice the
// Runtime handed back with its job, as they are, and hands back one still
// being delivered with its job: the Runtime's own hand-back owns these inputs.
func TestBindingReleaseLeavesAcceptedMessages(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_mmsg_release")
	binding := f.bind("bind_mmsg_release")
	f.write(computer.ChangeAdded, true)
	if processed := f.deliverAll(); processed != 1 {
		t.Fatalf("processed %d jobs; want one", processed)
	}
	f.write(computer.ChangeReplaced, true)
	leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput},
		LeaseOwner: "machine-message-test", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC()})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease the delivering message = %v/%v", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PrepareRuntimeCommand(f.ctx, job); err != nil {
		t.Fatalf("prepare the delivering message: %v", err)
	}
	f.write(computer.ChangeReplaced, true)
	before := f.messages()
	if before[0].Status != "accepted" || before[1].Status != "delivering" || before[2].Status != "queued" || before[2].ActiveJobs != 1 {
		t.Fatalf("messages before the release = %+v; want accepted, delivering, queued with its job", before)
	}

	identity := runtimecontrol.ProcessIdentity{Namespace: binding.Namespace, PodUID: binding.PodUID, ID: binding.RuntimeProcessID}
	var receipt string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id=$1`, identity.ID).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	bridge := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "bridge", nil))
	if _, _, err := runtimecontrol.ReportProcess(f.ctx, bridge, identity, receipt, runtimecontrol.ProcessDraining); err != nil {
		t.Fatal(err)
	}
	if err := bridge.WithWorkspaceTx(f.ctx, "default", "test.release_binding", func(tx *dbconnect.Tx) error {
		_, _, err := runtimecontrol.ReleaseBindingTx(f.ctx, tx, identity, &bridgev1.ReleaseRuntimeBindingRequest{WorkspaceId: "default", SessionId: f.sessionID,
			BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, RuntimeProcessId: identity.ID, OperationId: "release-mmsg"}, time.Now().UTC())
		return err
	}); err != nil {
		t.Fatalf("release the binding: %v", err)
	}
	after := f.messages()
	if after[0].Status != "accepted" || after[0].ActiveJobs != 0 {
		t.Fatalf("accepted message after the release = %+v; want accepted with no job", after[0])
	}
	if after[1].Status != "queued" || after[1].Bound || after[1].ActiveJobs != 1 {
		t.Fatalf("delivering message after the release = %+v; want handed back queued with its job", after[1])
	}
	if after[2].Status != "queued" || after[2].ActiveJobs != 1 {
		t.Fatalf("queued message after the release = %+v; want queued with exactly its one job", after[2])
	}
}
