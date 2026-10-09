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
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimeconfig"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
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
		COALESCE((SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1), 'absent')`, sessionID).Scan(&bindingCount, &runtimeStatus); err != nil {
		t.Fatalf("read recovery mutation census: %v", err)
	}
	if bindingCount != 0 || runtimeStatus != "absent" {
		t.Fatalf("expired recovery mutated binding/status = %d/%s; want 0/absent", bindingCount, runtimeStatus)
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
		`INSERT INTO session_runtime_status (workspace_id, session_id, status, idle_since, created_at, updated_at)
		 VALUES ('default',$1,'idle',now(),now(),now())`, sessionID); err != nil {
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
		 VALUES ('default',$1,'idle',now(),now(),now())`, sessionID); err != nil {
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
	directLease, err := queueStore.LeaseJobRunnerJobs(context.Background(), queue.LeaseJobRunnerJobsRequest{
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
	directLease, err := queueStore.LeaseJobRunnerJobs(context.Background(), queue.LeaseJobRunnerJobsRequest{
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
		wantRuntimeStatus string
		wantCleanup       bool
	}{
		{name: "active sibling keeps residency", activeSibling: true, wantRuntimeStatus: "running"},
		{name: "no remaining work arms cleanup", wantRuntimeStatus: "idle", wantCleanup: true},
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
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
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
			if err := acquireAndJoin(context.Background(), runner); err != nil {
				t.Fatalf("run child recovery final attempt: %v", err)
			}
			var queueStatus, sessionStatus, childStatus, runtimeStatus string
			var cleanupAfter sql.NullTime
			var statusEventID, runtimeBindingID sql.NullString
			var bindingCount, failureEvents, closeoutEvents int
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
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND session_thread_id=$3 AND type='session.thread_status_terminated')`,
				queued.ID, sessionID, childThreadID,
			).Scan(&queueStatus, &sessionStatus, &childStatus, &runtimeStatus, &cleanupAfter, &statusEventID, &runtimeBindingID, &bindingCount, &failureEvents, &closeoutEvents); err != nil {
				t.Fatalf("read child recovery finalization: %v", err)
			}
			if queueStatus != queue.StatusDeadLettered || sessionStatus == "terminated" || childStatus != "failed" ||
				runtimeStatus != testCase.wantRuntimeStatus || cleanupAfter.Valid != testCase.wantCleanup ||
				!runtimeBindingID.Valid || runtimeBindingID.String != bindingID || bindingCount != 1 ||
				failureEvents != 1 || closeoutEvents != 1 {
				t.Fatalf("child finalization = Queue %s Session %s child %s Runtime %s cleanup %v statusEvent %v binding %v/%d events %d/%d",
					queueStatus, sessionStatus, childStatus, runtimeStatus, cleanupAfter, statusEventID, runtimeBindingID, bindingCount, failureEvents, closeoutEvents)
			}
			if testCase.wantCleanup && !statusEventID.Valid {
				t.Fatal("idle child finalization omitted cleanup idle fence")
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
