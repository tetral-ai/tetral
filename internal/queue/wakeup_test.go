package queue

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
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

func TestNotificationListenerFailureLogsSafePermanentAndTransientCategories(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      string
		retryable bool
	}{
		{
			name: "authentication",
			err: &dbconnect.DiagnosticError{
				Kind:  dbconnect.KindAuthenticationFailed,
				Cause: errors.New("password=listener-secret"),
			},
			code: "notification_listener_authentication", retryable: false,
		},
		{
			name: "endpoint transport",
			err: &pgconn.PgError{
				Code: "08006", Message: "connection to postgresql://user:listener-secret@example.invalid failed",
			},
			code: "notification_listener_endpoint_transport", retryable: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			logNotificationListenerFailure(slog.New(slog.NewJSONHandler(&logs, nil)), ConsumerClassBridge, test.err)
			for _, want := range []string{
				`"error.code":"` + test.code + `"`,
				`"retryable":` + map[bool]string{true: "true", false: "false"}[test.retryable],
				`"error.message_safe":"queue notification listener disconnected"`,
			} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("listener log missing %s: %s", want, logs.String())
				}
			}
			if strings.Contains(logs.String(), "listener-secret") || strings.Contains(logs.String(), "postgresql://") {
				t.Fatalf("listener log exposed raw cause: %s", logs.String())
			}
		})
	}
}

func TestWakeSignalDoesNotLoseBroadcastBetweenPollAndWait(t *testing.T) {
	for _, mode := range []string{"timed", "notification-only"} {
		t.Run(mode, func(t *testing.T) {
			wake := NewWakeSignal()
			snapshot := wake.Snapshot()
			wake.Broadcast()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var err error
			if mode == "timed" {
				err = wake.Wait(ctx, time.Hour, snapshot)
			} else {
				err = wake.WaitForWake(ctx, snapshot)
			}
			if err != nil {
				t.Fatalf("wait lost the preceding broadcast: %v", err)
			}
		})
	}
}

func TestRunNotificationListenerBroadcastsCatchupAndRelevantPayloadAfterReconnect(t *testing.T) {
	listener := &scriptedNotificationListener{calls: make(chan int, 2), allowDisconnect: make(chan struct{})}
	wake := NewWakeSignal()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("RunNotificationListener: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("listener did not stop")
		}
	})
	initial := wake.Snapshot()
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

	select {
	case call := <-listener.calls:
		if call != 1 {
			t.Fatalf("first listen call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not connect")
	}
	waitForGeneration(initial)

	// Capture the baseline before allowing a disconnect. Otherwise the
	// reconnect broadcast can precede this snapshot, leaving us waiting for
	// an additional notification that the test has not sent.
	reconnect := wake.Snapshot()
	close(listener.allowDisconnect)
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
}

type scriptedNotificationListener struct {
	mu              sync.Mutex
	count           int
	calls           chan int
	allowDisconnect chan struct{}
	onNotify        func(string)
}

func (l *scriptedNotificationListener) Listen(ctx context.Context, _ string, onReady func(), onNotification func(string)) error {
	l.mu.Lock()
	l.count++
	call := l.count
	l.onNotify = onNotification
	l.mu.Unlock()
	select {
	case l.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
	onReady()
	if call == 1 {
		select {
		case <-l.allowDisconnect:
			return errors.New("connection lost")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (l *scriptedNotificationListener) notify(payload string) {
	l.mu.Lock()
	onNotify := l.onNotify
	l.mu.Unlock()
	if onNotify != nil {
		onNotify(payload)
	}
}

func TestRunListenerReconnectsWithReadinessAndRawPayloads(t *testing.T) {
	listener := &scriptedNotificationListener{calls: make(chan int, 2), allowDisconnect: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("RunListener: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("listener did not stop")
		}
	})
	var mu sync.Mutex
	var readyCalls, disconnects int
	var payloads []string
	go func() {
		done <- RunListener(ctx, listener, "queue_listener_test",
			func() {
				mu.Lock()
				readyCalls++
				mu.Unlock()
			},
			func(payload string) {
				mu.Lock()
				payloads = append(payloads, payload)
				mu.Unlock()
			},
			func(error) {
				mu.Lock()
				disconnects++
				mu.Unlock()
			},
		)
	}()

	select {
	case call := <-listener.calls:
		if call != 1 {
			t.Fatalf("first listen call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not connect")
	}
	listener.notify(`{"workspace_id":"ws_1"}`)
	close(listener.allowDisconnect)
	select {
	case call := <-listener.calls:
		if call != 2 {
			t.Fatalf("second listen call = %d", call)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not reconnect")
	}
	mu.Lock()
	defer mu.Unlock()
	if readyCalls != 2 {
		t.Fatalf("readiness calls = %d; want one per (re)connection", readyCalls)
	}
	if disconnects != 1 {
		t.Fatalf("disconnect callbacks = %d; want 1", disconnects)
	}
	if len(payloads) != 1 || payloads[0] != `{"workspace_id":"ws_1"}` {
		t.Fatalf("payloads = %v; want the raw payload passed through unfiltered", payloads)
	}
}
