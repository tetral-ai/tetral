package tetralsandbox

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type lateAcquisitionQueue struct {
	recordingSandboxQueue
	entered, reply chan struct{}
}

func (q *lateAcquisitionQueue) Lease(ctx context.Context, _ *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	close(q.entered)
	<-q.reply
	return &queuev1.LeaseResponse{Jobs: []*queuev1.QueueJob{{Id: "late", WorkspaceId: "ws", LeaseToken: "exact"}}}, nil
}
func TestSandboxConsumerAcquisitionAndJoinedDrain(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "complete"
		if force {
			name = "forced"
		}
		t.Run(name, func(t *testing.T) {
			acquire, quiesce := context.WithCancel(context.Background())
			defer quiesce()
			work, cancel := context.WithCancel(context.Background())
			defer cancel()
			work = WithAcquisitionContext(work, acquire)
			pool, _ := NewWorkspaceConsumerPool(1)
			entered := make(chan struct{})
			released := make(chan struct{})
			finished := make(chan struct{})
			var calls atomic.Int32
			go func() {
				defer close(finished)
				_ = RunWorkspaceConsumerGroup(work, 3, pool, sandboxStaticWorkspaceLister{workspace.ID("ws")}, time.Millisecond, func(ctx context.Context, _ workspace.ID) (bool, error) {
					calls.Add(1)
					close(entered)
					select {
					case <-released:
					case <-ctx.Done():
					}
					if force {
						time.Sleep(20 * time.Millisecond)
					}
					return true, nil
				}, nil, nil)
			}()
			<-entered
			quiesce()
			if work.Err() != nil {
				t.Fatal("quiesce cancelled admitted worker")
			}
			metrics := NewOperationMetrics()
			joined := make(chan error, 1)
			go func() { joined <- JoinSandboxWorkers(finished, cancel, 60*time.Millisecond, time.Second, metrics) }()
			if !force {
				close(released)
			}
			select {
			case err := <-joined:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("consumer owner did not join")
			}
			outcome := "success"
			if force {
				outcome = "timeout"
			}
			if !strings.Contains(metrics.Text(), `operation="shutdown_workers_drain",outcome="`+outcome+`",service="sandbox"} 1`) {
				t.Fatalf("missing joined owner metrics: %s", metrics.Text())
			}
			if calls.Load() != 1 {
				t.Fatalf("consumer acquired after quiesce: %d", calls.Load())
			}
		})
	}
	t.Run("late lease returns exact capability", func(t *testing.T) {
		acquire, quiesce := context.WithCancel(context.Background())
		defer quiesce()
		ctx := WithAcquisitionContext(context.Background(), acquire)
		q := &lateAcquisitionQueue{entered: make(chan struct{}), reply: make(chan struct{})}
		client := WithQueueAcquisition(q)
		done := make(chan error, 1)
		go func() {
			response, err := client.Lease(ctx, &queuev1.LeaseRequest{})
			if len(response.GetJobs()) != 0 {
				done <- errors.New("late jobs were admitted")
				return
			}
			if !errors.Is(err, context.Canceled) {
				done <- errors.New("quiesce error missing")
				return
			}
			done <- nil
		}()
		<-q.entered
		quiesce()
		close(q.reply)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if len(q.transitions) != 1 || q.transitions[0] != "defer:late" {
			t.Fatalf("late capability disposition=%v", q.transitions)
		}
	})
}
