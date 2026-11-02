package integration

import (
	"context"
	"database/sql"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	"testing"
	"time"

	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeliveringInputsReachReplacementRuntimeAfterPodLoss(t *testing.T) {
	for _, kind := range []string{"messages", "task_notification"} {
		t.Run(kind, func(t *testing.T) {
			f := newInputRedeliveryFixture(t, "sesn_redelivery_"+kind)
			binding := f.bind("bind_original")
			runtimeInputID := "rin_message"
			var enqueue queue.EnqueueRequest
			var err error
			if kind == "messages" {
				sequence := f.nextSequence()
				seedBridgeAPIEvent(t, f.admin, "default", f.sessionID, f.mainID, "sevt_message", sequence, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
				seedRuntimeInboxBirthForJob(t, f.admin, jobrunner.RuntimeJob{
					WorkspaceID: "default", SessionID: f.sessionID, SessionThreadID: f.mainID,
					RuntimeInputID: runtimeInputID, InputKind: kind, EventIDs: []string{"sevt_message"}, SequenceFrom: sequence, SequenceTo: sequence,
				})
				enqueue, err = runtimecontrol.RuntimeInputEnqueueRequest("default", f.sessionID, runtimecontrol.AcceptedRuntimeInput{
					SessionThreadID: f.mainID, RuntimeInputID: runtimeInputID, InputKind: kind, EventIDsJSON: `["sevt_message"]`,
					SequenceFrom: sql.NullInt64{Int64: sequence, Valid: true}, SequenceTo: sql.NullInt64{Int64: sequence, Valid: true},
				}, time.Now())
			} else {
				const taskID = "task_completed"
				runtimeInputID = "task_notification:" + taskID
				seedBridgeAPINotifiableBackgroundTask(t, f.admin, "default", f.sessionID, f.mainID, binding.BindingID, taskID, "sevt_task_source")
				settleBridgeAPIBackgroundTask(t, f.admin, f.sessionID, taskID, "completed",
					`{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
				f.exec(`INSERT INTO session_runtime_inbox (workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at)
					VALUES ('default',$1,$2,$3,'task_notification','[]','queued',clock_timestamp(),clock_timestamp())`, f.sessionID, f.mainID, runtimeInputID)
				enqueue, err = queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, f.sessionID, f.mainID, taskID, time.Now())
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.queue.Enqueue(f.ctx, enqueue); err != nil {
				t.Fatal(err)
			}
			f.sender.err = status.Error(codes.Unavailable, "runtime pod unreachable")
			if got := f.deliverAll(); got != 1 {
				t.Fatalf("initial delivery attempts = %d; want 1", got)
			}
			var inboxStatus string
			if err := f.admin.QueryRowContext(f.ctx, `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).Scan(&inboxStatus); err != nil {
				t.Fatal(err)
			}
			if inboxStatus != "delivering" {
				t.Fatalf("input after transport failure = %s; want delivering", inboxStatus)
			}
			f.sender.err = nil
			f.candidate = enginekubernetes.BindingCandidate{Namespace: f.candidate.Namespace, PodName: "runtime-pod-1", PodUID: "pod_replacement", PodIP: "10.0.0.11"}
			seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(f.admin), f.candidate.Namespace, f.candidate.PodUID)
			f.exec(`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND dedupe_key=$1 AND status='pending'`, enqueue.DedupeKey)
			f.deliverAll()

			var targetPodUID, jobStatus string
			var jobCount int
			if err := f.admin.QueryRowContext(f.ctx, `SELECT i.status, COALESCE(i.target_pod_uid,''), j.status,
				(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2)
				FROM session_runtime_inbox i JOIN queue_jobs j ON j.workspace_id=i.workspace_id AND j.dedupe_key=$2
				WHERE i.workspace_id='default' AND i.runtime_input_id=$1`, runtimeInputID, enqueue.DedupeKey).
				Scan(&inboxStatus, &targetPodUID, &jobStatus, &jobCount); err != nil {
				t.Fatal(err)
			}
			if inboxStatus != "accepted" || targetPodUID != f.candidate.PodUID || jobStatus != queue.StatusAcknowledged || jobCount != 1 {
				t.Fatalf("replacement delivery = inbox %s, pod %s, job %s, jobs %d; want accepted on replacement, original job acknowledged", inboxStatus, targetPodUID, jobStatus, jobCount)
			}
			calls := 0
			for _, request := range f.sender.requests {
				switch request.(type) {
				case *agentruntimev1.AcceptInputRequest, *agentruntimev1.AcceptTaskNotificationRequest:
					calls++
				}
			}
			if calls != 2 {
				t.Fatalf("Runtime input calls = %d; want failed original and successful replacement", calls)
			}
			if kind == "messages" {
				var delivered *agentruntimev1.AcceptInputRequest
				for _, request := range f.sender.requests {
					if input, ok := request.(*agentruntimev1.AcceptInputRequest); ok {
						delivered = input
					}
				}
				bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "bridge", nil)))
				bridgeStore.RuntimeBindingTokenHMACKey = []byte("redelivery-commit-test-signing-key")
				scope := sessionfixture.BridgeAPIScope(f.sessionID, f.mainID, delivered.GetBindingId(), delivered.GetBindingGeneration(), delivered.GetTargetPodUid())
				committed, err := bridgeStore.CommitInputs(f.ctx, &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: runtimeInputID})
				if err != nil || committed.GetCommitted() == nil {
					t.Fatalf("commit replacement input = %v/%v", committed, err)
				}
				var processed bool
				if err := f.admin.QueryRowContext(f.ctx, `SELECT processed_at IS NOT NULL FROM session_events WHERE workspace_id='default' AND event_id='sevt_message'`).Scan(&processed); err != nil {
					t.Fatal(err)
				}
				if !processed {
					t.Fatal("accepted message was not marked processed")
				}
			}
		})
	}
}

// inputRedeliveryFixture composes Queue, placement and Bridge against installed
// database roles; the Runtime transport is controlled to reproduce Pod loss.
type inputRedeliveryFixture struct {
	t                 *testing.T
	ctx               context.Context
	admin             *sql.DB
	workloads         *storagetest.WorkloadDB
	queue             *queue.PostgreSQLQueueStore
	runner            *jobrunner.JobRunner
	sender            *recordingRuntimeCommandSender
	candidate         enginekubernetes.BindingCandidate
	sessionID, mainID string
}

func newInputRedeliveryFixture(t *testing.T, sessionID string) *inputRedeliveryFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	f := &inputRedeliveryFixture{t: t, ctx: ctx, admin: admin, sessionID: sessionID, mainID: "sthr_" + sessionID,
		candidate: enginekubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_original", PodIP: "10.0.0.10"}}
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, f.mainID)
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), f.candidate.Namespace, f.candidate.PodUID)
	f.workloads = storagetest.OpenWorkloadDB(t, admin, "job_runner")
	client := dbconnect.NewClientForTesting(f.workloads.DB)
	f.queue = queue.NewPostgreSQLStoreWithRetryPolicy(client, queue.RetryPolicy{BaseDelay: time.Minute, MaxDelay: time.Minute, RandomInt64: func(bound int64) int64 { return bound - 1 }})
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, 9090, jobrunner.KubernetesRuntimeTargetResolver{
		LoadClient: fixtureRuntimeLoadClient(t), GetPod: fixtureConfirmedMissingRuntimePod,
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{f.candidate})
		},
	})
	f.sender = &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
	f.runner = &jobrunner.JobRunner{Queue: tetralqueue.NewServer(f.queue, nil), Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: f.sender}, Config: jobrunner.JobRunnerConfig{LeaseOwner: "input-redelivery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}}
	return f
}
func (f *inputRedeliveryFixture) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		f.t.Fatal(err)
	}
}
func (f *inputRedeliveryFixture) bind(bindingID string) runtimecontrol.Binding {
	f.t.Helper()
	seedBridgeAPIRuntimeBinding(f.t, f.admin, "default", f.sessionID, bindingID, 1, f.candidate.PodUID)
	sessionfixture.SeedRuntimePodLostStatusFence(f.t, f.admin, f.sessionID, bindingID, 1)
	return runtimecontrol.Binding{BindingID: bindingID, BindingGeneration: 1, PodUID: f.candidate.PodUID}
}
func (f *inputRedeliveryFixture) nextSequence() int64 {
	f.t.Helper()
	var seq int64
	if err := f.admin.QueryRowContext(f.ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, f.sessionID, f.mainID).Scan(&seq); err != nil {
		f.t.Fatal(err)
	}
	return seq
}
func (f *inputRedeliveryFixture) deliverAll() int {
	f.t.Helper()
	n, err := acquireAndJoinJobRunnerJobs(f.ctx, f.runner, 1)
	if err != nil {
		f.t.Fatal(err)
	}
	return n
}
