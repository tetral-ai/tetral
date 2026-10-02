package jobrunner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type drainingRunnerQueue struct {
	*recordingQueueClient
	leaseEntered, releaseLease chan struct{}
	once                       sync.Once
}

func (q *drainingRunnerQueue) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	q.once.Do(func() { close(q.leaseEntered) })
	if q.releaseLease != nil {
		select {
		case <-q.releaseLease:
		case <-ctx.Done():
			// Simulate an actual already-committed lease response racing cancellation.
			<-q.releaseLease
		}
	}
	return q.recordingQueueClient.Lease(ctx, request)
}

type drainingRunnerDeliverer struct {
	*recordingDeliverer
	entered, release, canceled, joined chan struct{}
}

func (d *drainingRunnerDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	close(d.entered)
	defer close(d.joined)
	select {
	case <-d.release:
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
	case <-ctx.Done():
		close(d.canceled)
		return RuntimeDeliveryResult{}, ctx.Err()
	}
}

func TestJobRunnerAcquisitionAndJoinedDrain(t *testing.T) {
	for _, scenario := range []string{"completes with heartbeat", "cancel and join", "late lease response"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			q := &drainingRunnerQueue{recordingQueueClient: &recordingQueueClient{leased: []*queuev1.QueueJob{runtimeInputQueueJob()}, heartbeatNotify: make(chan struct{}, 16)}, leaseEntered: make(chan struct{})}
			d := &drainingRunnerDeliverer{recordingDeliverer: &recordingDeliverer{}, entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{}), joined: make(chan struct{})}
			if scenario == "late lease response" {
				q.releaseLease = make(chan struct{})
			}
			runner := &JobRunner{Queue: q, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: d, Config: JobRunnerConfig{LeaseDuration: time.Second, HeartbeatInterval: 20 * time.Millisecond, PollInterval: time.Millisecond, DrainTimeout: 200 * time.Millisecond, CancelJoinTimeout: time.Second}}
			done := make(chan error, 1)
			go func() { done <- RunJobRunnerLoop(ctx, runner, nil, queue.NewWakeSignal()) }()
			await := func(ch <-chan struct{}, name string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s not reached", name)
				}
			}
			if scenario == "late lease response" {
				await(q.leaseEntered, "lease")
				stop()
				close(q.releaseLease)
			} else {
				await(d.entered, "active command")
				await(q.heartbeatNotify, "first heartbeat")
				stop()
				if scenario == "completes with heartbeat" {
					await(q.heartbeatNotify, "draining heartbeat")
					close(d.release)
				} else {
					await(d.canceled, "bounded command cancellation")
				}
				await(d.joined, "command join")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("consumer did not join")
			}
			q.mu.Lock()
			defer q.mu.Unlock()
			if len(q.leaseWorkspaceIDs) != 1 {
				t.Fatalf("new acquisition after shutdown: %v", q.leaseWorkspaceIDs)
			}
			switch scenario {
			case "late lease response":
				select {
				case <-d.entered:
					t.Fatal("late lease dispatched new command")
				default:
				}
				if len(q.transitions) != 1 || q.transitions[0] != "defer:qjob_1" {
					t.Fatalf("late acquired capability not returned: %v", q.transitions)
				}
			case "completes with heartbeat":
				if q.heartbeats < 2 || len(q.transitions) != 1 || q.transitions[0] != "ack:qjob_1" {
					t.Fatalf("drain lost heartbeat/final disposition: heartbeats=%d transitions=%v", q.heartbeats, q.transitions)
				}
			}
		})
	}
}
