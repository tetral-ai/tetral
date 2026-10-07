package jobrunner

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLRuntimeHandoffRecoveryExactSourceAndLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	seedBridgeAPISession(t, admin, "default", "session_handoff", "thread_handoff")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "session_handoff", "binding_old", 1, "pod_old")
	seedRuntimePodLostStatusFence(t, admin, "session_handoff", "binding_old", 1)
	seedBridgeAPIEvent(t, admin, "default", "session_handoff", "thread_handoff", "turn-open", 1, "session.status_running", "{}")
	identity := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: "pod_old", ID: "process_pod_old"}
	var receipt string
	if err := admin.QueryRowContext(ctx, `SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id=$1`, identity.ID).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	if _, _, err := runtimecontrol.ReportProcess(ctx, client, identity, receipt, runtimecontrol.ProcessDraining); err != nil {
		t.Fatal(err)
	}
	var released *bridgev1.ReleaseRuntimeBindingResponse
	if err := client.WithWorkspaceTx(ctx, "default", "runtime_handoff_test.release", func(tx *dbconnect.Tx) error {
		var err error
		released, _, err = runtimecontrol.ReleaseBindingTx(ctx, tx, identity, &bridgev1.ReleaseRuntimeBindingRequest{WorkspaceId: "default", SessionId: "session_handoff", BindingId: "binding_old", BindingGeneration: 1, RuntimeProcessId: identity.ID, OperationId: "release-original"}, time.Now())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(released.Threads) != 1 || released.Threads[0].QueueJobId == "" {
		t.Fatalf("release=%v", released)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	leased, err := queueStore.Lease(ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "replacement-runner", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease=%v error=%v", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatal(err)
	}
	if job.RecoveryHandoffID != released.HandoffId || job.RecoverySourceEventID != "" {
		t.Fatalf("decoded recovery aliases event: %+v", job)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", "session_handoff", "binding_new", 2, "pod_new")
	store := NewPostgreSQLRuntimeDeliveryStore(client, 19090, KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_new", PodIP: "10.0.0.10"}})
	}})
	plan, err := store.ActivateRuntimeRecovery(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if plan.RecoverThread == nil || plan.RecoverThread.HandoffId != released.HandoffId || plan.RecoverThread.SourceEventId != "" || plan.RecoverThread.RuntimeProcessId != "process_pod_new" || plan.AttemptedBinding.RuntimeProcessID != "process_pod_new" {
		t.Fatalf("handoff recovery plan=%+v", plan)
	}
	changed := job
	changed.RecoveryHandoffID = "uncommitted-handoff"
	changed.DedupeKey = queue.FormatRuntimeHandoffDedupeKey(workspace.DefaultID, job.SessionID, job.SessionThreadID, changed.RecoveryHandoffID)
	rejected, err := store.ActivateRuntimeRecovery(ctx, changed)
	if err != nil || !rejected.DeliveryAuthorityLost || rejected.RecoverThread != nil {
		t.Fatalf("forged source plan=%+v error=%v", rejected, err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE id=$1`, job.JobID); err != nil {
		t.Fatal(err)
	}
	expired, err := store.ActivateRuntimeRecovery(ctx, job)
	if err != nil || !expired.DeliveryAuthorityLost || expired.RecoverThread != nil {
		t.Fatalf("expired handoff lease=%+v error=%v", expired, err)
	}
}
