package tetralsandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestRunWorkspaceConsumerLoopWakeInterruptsMaximumIdleSleep(t *testing.T) {
	wake := queue.NewWakeSignal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cycles := make(chan struct{}, 2)
	done := make(chan error, 1)
	go func() {
		done <- RunWorkspaceConsumerLoop(ctx, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Hour, func(context.Context, workspace.ID) (bool, error) {
			cycles <- struct{}{}
			return false, nil
		}, wake, nil)
	}()
	select {
	case <-cycles:
	case <-time.After(time.Second):
		t.Fatal("initial consumer cycle did not run")
	}
	started := time.Now()
	wake.Broadcast()
	select {
	case <-cycles:
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("wake-to-poll elapsed = %s; want less than one second", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("wake did not interrupt the idle sleep")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunWorkspaceConsumerLoop = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer loop did not stop")
	}
}

func TestRunWorkspaceConsumerLoopPollingContinuesWithoutNotificationListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cycles atomic.Int32
	workObserved := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- RunWorkspaceConsumerLoop(ctx, sandboxStaticWorkspaceLister{"ws_alpha"}, 5*time.Millisecond, func(context.Context, workspace.ID) (bool, error) {
			if cycles.Add(1) == 2 {
				close(workObserved)
				return true, nil
			}
			return false, nil
		}, nil, nil)
	}()
	select {
	case <-workObserved:
	case <-time.After(time.Second):
		t.Fatal("timer fallback did not poll after the listener was absent")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer loop did not stop")
	}
}

func TestRunWorkspaceConsumerLoopWakesFromCommittedPostgreSQLNotificationAndLeases(t *testing.T) {
	runtimeDB, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := queue.NewPostgreSQLStore(client)
	wake := queue.NewWakeSignal()

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	listenerDone := make(chan error, 1)
	readySnapshot := wake.Snapshot()
	go func() {
		listenerDone <- queue.RunNotificationListener(listenerCtx, queue.PostgreSQLNotificationListener{Client: client}, queue.ConsumerClassSandbox, wake, nil)
	}()
	readyCtx, cancelReady := context.WithTimeout(context.Background(), time.Second)
	defer cancelReady()
	if err := wake.Wait(readyCtx, time.Hour, readySnapshot); err != nil {
		t.Fatalf("wait for PostgreSQL listener: %v", err)
	}

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	initialEmptyPoll := make(chan struct{})
	leasedAfter := make(chan time.Duration, 1)
	consumerDone := make(chan error, 1)
	var initialEmptyPollOnce sync.Once
	var enqueueStarted atomic.Int64
	workspaceID := workspace.ID("ws_queue_wake_e2e")
	go func() {
		consumerDone <- RunWorkspaceConsumerLoop(consumerCtx, sandboxStaticWorkspaceLister{workspaceID}, time.Hour, func(ctx context.Context, currentWorkspaceID workspace.ID) (bool, error) {
			jobs, err := store.Lease(ctx, queue.LeaseRequest{
				WorkspaceID:   currentWorkspaceID,
				Kinds:         []string{queue.KindEnvironmentBuild},
				LeaseOwner:    "sandbox-wake-e2e",
				MaxJobs:       1,
				LeaseDuration: time.Minute,
			})
			if err != nil {
				return false, err
			}
			if len(jobs) == 0 {
				initialEmptyPollOnce.Do(func() { close(initialEmptyPoll) })
				return false, nil
			}
			leasedAfter <- time.Since(time.Unix(0, enqueueStarted.Load()))
			return true, nil
		}, wake, nil)
	}()
	select {
	case <-initialEmptyPoll:
	case <-time.After(time.Second):
		t.Fatal("consumer did not complete its initial empty poll")
	}

	payload, err := json.Marshal(map[string]any{
		"workspace_id":   string(workspaceID),
		"environment_id": "env_queue_wake_e2e",
		"generation":     "1",
	})
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	enqueueStarted.Store(time.Now().UnixNano())
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: "qjob_queue_wake_e2e", WorkspaceID: workspaceID, Kind: queue.KindEnvironmentBuild,
		PartitionKey: queue.FormatEnvironmentPartitionKey(workspaceID, "env_queue_wake_e2e"),
		DedupeKey:    queue.FormatEnvironmentBuildDedupeKey(workspaceID, "env_queue_wake_e2e", "1"),
		PayloadJSON:  payload,
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("enqueue committed work: %v", err)
	}
	select {
	case elapsed := <-leasedAfter:
		if elapsed >= time.Second {
			t.Fatalf("committed enqueue to Lease elapsed = %s; want less than one second", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("committed enqueue did not wake the consumer before its one-hour poll")
	}

	cancelConsumer()
	cancelListener()
	select {
	case err := <-consumerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer shutdown = %v; want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
	select {
	case err := <-listenerDone:
		if err != nil {
			t.Fatalf("listener shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
}

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
	}, nil, nil, func(_ context.Context, delay time.Duration, _ queue.WakeSnapshot) error {
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
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	polls := 0
	var delays []time.Duration
	err := runWorkspaceConsumerLoop(ctx, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Second, func(context.Context, workspace.ID) (bool, error) {
		polls++
		if polls == 3 {
			return true, fmt.Errorf("unsafe-token: %w", &pgconn.PgError{Code: "42501", Message: "unsafe-query", Detail: "unsafe-password"})
		}
		return false, nil
	}, nil, logger, func(_ context.Context, delay time.Duration, _ queue.WakeSnapshot) error {
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
	if want := []time.Duration{time.Second, 2 * time.Second, time.Second, time.Second}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("poll delays = %v; want active failure to reset backoff to %v", delays, want)
	}
	logText := output.String()
	if strings.Count(logText, "\n") != 1 || !strings.Contains(logText, `"event.kind":"queue_consume_failed"`) ||
		!strings.Contains(logText, `"operation":"sandbox.queue.consume"`) || !strings.Contains(logText, `"error.class":"database_permission_denied"`) ||
		!strings.Contains(logText, `"retry.delay.ms":1000`) || strings.Contains(logText, "unsafe-") {
		t.Fatalf("consumer failure log = %q", logText)
	}
}

func TestRunWorkspaceConsumerLoopBoundsIdleFailureLogsAndSuppressesShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	var delays []time.Duration
	polls := 0
	err := runWorkspaceConsumerLoop(ctx, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Second, func(context.Context, workspace.ID) (bool, error) {
		polls++
		if polls == 8 {
			cancel()
			return false, ctx.Err()
		}
		return false, errors.New("private-provider-payload")
	}, nil, logger, func(_ context.Context, delay time.Duration, _ queue.WakeSnapshot) error {
		delays = append(delays, delay)
		return ctx.Err()
	})
	if err != context.Canceled {
		t.Fatalf("loop shutdown: %v", err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second}
	if !reflect.DeepEqual(delays, want) {
		t.Fatalf("failed-poll backoff = %v; want %v", delays, want)
	}
	if logText := output.String(); strings.Count(logText, "\n") != 7 || strings.Count(logText, `"error.class":"consumer_error"`) != 7 || strings.Contains(logText, "private-provider-payload") {
		t.Fatalf("failure/shutdown diagnostics: %q", logText)
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
		done <- RunWorkspaceConsumerGroup(ctx, 6, pool, sandboxStaticWorkspaceLister{"ws_alpha"}, time.Millisecond, consumer, nil, nil)
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
