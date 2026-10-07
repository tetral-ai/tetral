package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	runtimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type drainRuntimeSender struct {
	*jobrunner.RuntimePodCommandClient
	admitted, release chan struct{}
	calls             atomic.Int32
}

func (s *drainRuntimeSender) AcceptInput(ctx context.Context, target jobrunner.RuntimePodTarget, request *runtimev1.AcceptInputRequest) (*runtimev1.AcceptInputResponse, error) {
	s.calls.Add(1)
	response, err := s.RuntimePodCommandClient.AcceptInput(ctx, target, request)
	if err != nil {
		return nil, err
	}
	close(s.admitted)
	select {
	case <-s.release:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Runtime/Gateway are actual Bun children. Runner shutdown only changes Queue
// acquisition: already-admitted Runtime work must continue after Runner exits.
func TestPostgreSQLReplicaWorkerDrain(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "completion before cutoff"
		if force {
			name = "forced sender join and custody recovery"
		}
		t.Run(name, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			client := dbconnect.NewClientForTesting(runtimeDB)
			store := bridge.NewPostgreSQLBridgeAPIStore(client)
			store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
			sessions := []string{"sesn_worker_drain_a2", "sesn_worker_successor_a2"}
			for _, session := range sessions {
				sessionfixture.SeedBridgeAPISession(t, admin, "default", session, "thr_"+session)
				seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_"+session, 1, "pod_old")
				sessionfixture.SeedRuntimePodLostStatusFence(t, admin, session, "bind_"+session, 1)
			}
			if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'`); err != nil {
				t.Fatal(err)
			}
			server := serveReplicaBridge(t, store, map[string]string{"old": "pod_old"}, nil)
			child := startHandoffRuntimeChild(t, server.Address, "pod_old", "process_pod_old", "old", false, nil)
			ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			appendHandoffMessage(t, client, sessions[0], "worker-original")
			qStore := queue.NewPostgreSQLStore(client)
			rpc := serveQueueReplica(t, qStore, &queueReplicaResponseFault{})
			sender := &drainRuntimeSender{RuntimePodCommandClient: fixtureRuntimeCommandClient(t, attachmentRuntimeTokenSource{}), admitted: make(chan struct{}), release: make(chan struct{})}
			delivery := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, child.port, jobrunner.KubernetesRuntimeTargetResolver{Snapshot: func() kubernetes.BindingVisibilitySnapshot {
				return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_old", PodIP: "127.0.0.1"}})
			}})
			acquire, quiesce := context.WithCancel(ctx)
			defer quiesce()
			cfg := jobrunner.JobRunnerConfig{LeaseOwner: "draining-runner", MaxJobs: 1, LeaseDuration: 3 * time.Second, HeartbeatInterval: 150 * time.Millisecond, PollInterval: 10 * time.Millisecond, DrainTimeout: 600 * time.Millisecond, CancelJoinTimeout: time.Second}
			runner := &jobrunner.JobRunner{Queue: jobrunner.QueueClientFromGRPC(rpc), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: sender}, Config: cfg}
			done := make(chan error, 1)
			joined := make(chan struct{})
			go func() { defer close(joined); done <- jobrunner.RunJobRunnerLoop(acquire, runner, nil, nil) }()
			t.Cleanup(func() {
				quiesce()
				select {
				case <-joined:
				case <-time.After(3 * time.Second):
					t.Error("Runner did not join")
				}
			})
			select {
			case <-sender.admitted:
			case <-ctx.Done():
				t.Fatal("actual Runtime command did not admit")
			}
			waitHandoffCondition(t, "actual Runtime provider frame", func() bool { return child.calls(sessions[0]) == 1 })
			var jobID, token string
			var leaseUntil time.Time
			if err := admin.QueryRowContext(ctx, `SELECT id,lease_token,leased_until FROM queue_jobs WHERE kind='runtime_input' AND payload_json::jsonb->>'session_id'=$1 AND status='leased'`, sessions[0]).Scan(&jobID, &token, &leaseUntil); err != nil {
				t.Fatal(err)
			}
			quiesce()
			appendHandoffMessage(t, client, sessions[1], "worker-successor")
			waitHandoffCondition(t, "heartbeat continues during Runner drain", func() bool {
				var renewed time.Time
				if err := admin.QueryRowContext(ctx, `SELECT leased_until FROM queue_jobs WHERE id=$1`, jobID).Scan(&renewed); err != nil {
					t.Fatal(err)
				}
				return renewed.After(leaseUntil)
			})
			if !force {
				close(sender.release)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Runner did not finish bounded shutdown")
			}
			<-joined
			if sender.calls.Load() != 1 || child.calls(sessions[0]) != 1 || child.calls(sessions[1]) != 0 {
				t.Fatalf("Runner quiesce crossed admission or cancelled Runtime: %d/%d/%d", sender.calls.Load(), child.calls(sessions[0]), child.calls(sessions[1]))
			}
			// The replacement Runner owns an independent pool and TCP Queue channel.
			successorPool := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil)
			successorClient := dbconnect.NewClientForTesting(successorPool)
			successorQueue := queue.NewPostgreSQLStore(successorClient)
			if force {
				awaitReplicaQueueExpiry(ctx, t, admin, jobID)
				if n, err := successorQueue.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{Limit: 10}); err != nil || n != 1 {
					t.Fatalf("forced Runner custody reclamation=%d/%v", n, err)
				}
			}
			nextDelivery := jobrunner.NewPostgreSQLRuntimeDeliveryStore(successorClient, child.port, delivery.TargetResolver)
			successor := &jobrunner.JobRunner{Queue: jobrunner.QueueClientFromGRPC(serveQueueReplica(t, successorQueue, &queueReplicaResponseFault{})), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: nextDelivery, Sender: fixtureRuntimeCommandClient(t, attachmentRuntimeTokenSource{})}, Config: jobrunner.JobRunnerConfig{LeaseOwner: "replacement-runner", MaxJobs: 2, LeaseDuration: 3 * time.Second, HeartbeatInterval: 150 * time.Millisecond}}
			for range 2 {
				if _, err := successor.RunOnceWithActivity(ctx); err != nil {
					t.Fatal(err)
				}
			}
			waitHandoffCondition(t, "replacement delivers pending input", func() bool { return child.calls(sessions[1]) == 1 })
			child.signal(t, sessions[0]+"-1.release")
			child.signal(t, sessions[1]+"-1.release")
			waitHandoffCondition(t, "Runtime continues and settles both requests", func() bool {
				var count int
				if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM session_events WHERE session_id IN ($1,$2) AND type='span.model_request_end'`, sessions[0], sessions[1]).Scan(&count); err != nil {
					t.Fatal(err)
				}
				return count == 2
			})
			var errors, bindings int
			if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_events WHERE type='session.error'),(SELECT count(*) FROM session_runtime_bindings)`).Scan(&errors, &bindings); err != nil || errors != 0 || bindings != 2 {
				t.Fatalf("Runner shutdown changed Runtime lifecycle=%d/%d/%v", errors, bindings, err)
			}
			if child.calls(sessions[0]) != 1 {
				t.Fatal("replacement replay duplicated original provider request")
			}
			assertReplicaQueueState(ctx, t, admin, "default", jobID, queue.StatusAcknowledged)
			if ack, err := rpc.Ack(ctx, &queuev1.AckRequest{WorkspaceId: "default", JobId: jobID, LeaseToken: token}); err != nil || ack.GetUpdated() {
				t.Fatalf("old Runner token crossed replacement custody=%v/%v", ack, err)
			}
			child.signal(t, "quiesce")
			child.join(t)
			t.Logf("Runner shutdown %s preserved actual Runtime requests and Queue custody", fmt.Sprint(force))
		})
	}
}
