package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestConsumerClassForKindCoversEveryKnownKind(t *testing.T) {
	kinds := []string{
		KindRuntimeInput, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup,
		KindEnvironmentBuild, KindEnvironmentReadyFanout, KindSandboxToolExecute,
		KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease,
		KindSandboxToolCancel, KindSandboxOutputCapture, KindSandboxOutputCaptureCleanup,
		KindSandboxMemoryProjection, KindSandboxBackgroundCommand, KindSandboxBackgroundReconcile,
	}
	for _, kind := range kinds {
		consumerClass, ok := ConsumerClassForKind(kind)
		if !ok || (consumerClass != ConsumerClassBridge && consumerClass != ConsumerClassSandbox) {
			t.Fatalf("ConsumerClassForKind(%q) = %q,%t; want a known class", kind, consumerClass, ok)
		}
	}
	if consumerClass, ok := ConsumerClassForKind("unknown"); ok || consumerClass != "" {
		t.Fatalf("ConsumerClassForKind(unknown) = %q,%t; want empty,false", consumerClass, ok)
	}
}

func TestWakeSignalDoesNotLoseBroadcastBetweenPollAndWait(t *testing.T) {
	wake := NewWakeSignal()
	snapshot := wake.Snapshot()
	wake.Broadcast()
	started := time.Now()
	if err := wake.Wait(context.Background(), time.Hour, snapshot); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("wake elapsed = %s; want less than one second", elapsed)
	}
}

func TestRunNotificationListenerBroadcastsCatchupAndRelevantPayloadAfterReconnect(t *testing.T) {
	listener := &scriptedNotificationListener{calls: make(chan int, 2), hold: make(chan struct{})}
	wake := NewWakeSignal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunNotificationListener(ctx, listener, ConsumerClassBridge, wake, nil)
	}()

	waitForGeneration := func(after WakeSnapshot) {
		t.Helper()
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
		defer waitCancel()
		if err := wake.Wait(waitCtx, time.Hour, after); err != nil {
			t.Fatalf("wait for notification: %v", err)
		}
	}

	initial := wake.Snapshot()
	if call := <-listener.calls; call != 1 {
		t.Fatalf("first listen call = %d", call)
	}
	waitForGeneration(initial)

	reconnect := wake.Snapshot()
	select {
	case call := <-listener.calls:
		if call != 2 {
			t.Fatalf("second listen call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not reconnect")
	}
	waitForGeneration(reconnect)

	payload := wake.Snapshot()
	listener.notify(ConsumerClassSandbox)
	unchangedCtx, unchangedCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	if err := wake.Wait(unchangedCtx, time.Hour, payload); !errors.Is(err, context.DeadlineExceeded) {
		unchangedCancel()
		t.Fatalf("unrelated payload wait = %v; want deadline", err)
	}
	unchangedCancel()
	listener.notify(ConsumerClassBridge)
	waitForGeneration(payload)

	close(listener.hold)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunNotificationListener: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
}

type scriptedNotificationListener struct {
	mu       sync.Mutex
	count    int
	calls    chan int
	hold     chan struct{}
	onNotify func(string)
}

func (l *scriptedNotificationListener) Listen(ctx context.Context, _ string, onReady func(), onNotification func(string)) error {
	l.mu.Lock()
	l.count++
	call := l.count
	l.onNotify = onNotification
	l.mu.Unlock()
	l.calls <- call
	onReady()
	if call == 1 {
		return errors.New("connection lost")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.hold:
		return errors.New("connection lost")
	}
}

func (l *scriptedNotificationListener) notify(payload string) {
	l.mu.Lock()
	onNotify := l.onNotify
	l.mu.Unlock()
	if onNotify != nil {
		onNotify(payload)
	}
}
