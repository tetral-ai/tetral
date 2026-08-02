package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestRunWorkspaceConsumerCycleVisitsEveryDiscoveredWorkspace(t *testing.T) {
	var consumed []workspace.ID
	hadWork, err := RunWorkspaceConsumerCycle(context.Background(), sandboxStaticWorkspaceLister{"ws_alpha", "ws_beta"}, func(_ context.Context, workspaceID workspace.ID) (bool, error) {
		consumed = append(consumed, workspaceID)
		return workspaceID == "ws_beta", nil
	})
	if err != nil {
		t.Fatalf("RunWorkspaceConsumerCycle: %v", err)
	}
	if !reflect.DeepEqual(consumed, []workspace.ID{"ws_alpha", "ws_beta"}) {
		t.Fatalf("consumed = %v; want every discovered workspace", consumed)
	}
	if !hadWork {
		t.Fatal("cycle hadWork=false; want true when one workspace consumed work")
	}
}

func TestRunWorkspaceConsumerLoopBacksOffAcrossConsecutiveEmptyPolls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var delays []time.Duration
	err := runWorkspaceConsumerLoop(ctx, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Second, func(context.Context, workspace.ID) (bool, error) {
		return false, nil
	}, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 4 {
			cancel()
			return context.Canceled
		}
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("runWorkspaceConsumerLoop = %v; want context.Canceled", err)
	}
	if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("empty-poll delays = %v; want %v", delays, want)
	}
}

func TestRunWorkspaceConsumerLoopResetsBackoffAfterActiveFailedPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	polls := 0
	var delays []time.Duration
	err := runWorkspaceConsumerLoop(ctx, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Second, func(context.Context, workspace.ID) (bool, error) {
		polls++
		if polls == 3 {
			return true, errors.New("leased work failed")
		}
		return false, nil
	}, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 3 {
			cancel()
			return context.Canceled
		}
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("runWorkspaceConsumerLoop = %v; want context.Canceled", err)
	}
	if want := []time.Duration{time.Second, 2 * time.Second, time.Second}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("poll delays = %v; want active failure to reset backoff to %v", delays, want)
	}
}

func TestRunWorkspaceConsumerGroupExposesSharedBoundedConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := NewWorkspaceConsumerPool(3)
	if err != nil {
		t.Fatalf("NewWorkspaceConsumerPool: %v", err)
	}
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	var once sync.Once
	consumer := func(ctx context.Context, _ workspace.ID) (bool, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
			once.Do(cancel)
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- RunWorkspaceConsumerGroup(ctx, 6, pool, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Millisecond, consumer)
	}()
	for range 3 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("consumer group did not expose the configured worker capacity")
		}
	}
	select {
	case <-entered:
		t.Fatal("consumer group exceeded the shared worker capacity")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWorkspaceConsumerGroup error = %v; want context cancellation", err)
	}
	if maximum.Load() != 3 {
		t.Fatalf("maximum active consumers = %d; want 3", maximum.Load())
	}
}

type sandboxStaticWorkspaceLister []workspace.ID

func (l sandboxStaticWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return append([]workspace.ID(nil), l...), nil
}
