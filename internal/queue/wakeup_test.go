package queue

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workload/workloadtest"
)

func TestConsumerClassForKindCoversEveryKnownKind(t *testing.T) {
	if ConsumerClassJobRunner != "bridge" {
		t.Fatal("Job Runner notification payload must preserve established bridge vocabulary")
	}
	kinds := []string{
		KindRuntimeInput, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup,
		KindEnvironmentBuild, KindEnvironmentReadyFanout, KindSandboxToolExecute,
		KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease,
		KindSandboxToolCancel, KindSandboxOutputCapture, KindSandboxOutputCaptureCleanup,
		KindSandboxMemoryProjection, KindSandboxBackgroundCommand, KindSandboxBackgroundReconcile,
	}
	for _, kind := range kinds {
		consumerClass, ok := ConsumerClassForKind(kind)
		if !ok || (consumerClass != ConsumerClassJobRunner && consumerClass != ConsumerClassSandbox) {
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
		cause     string
	}{
		{
			name: "authentication",
			err: &dbconnect.DiagnosticError{
				Kind:  dbconnect.KindAuthenticationFailed,
				Cause: errors.New(`password authentication failed for user "listener"`),
			},
			code: "notification_listener_authentication", retryable: false,
			cause: `postgresql runtime client: provider= phase= kind=authentication_failed: password authentication failed for user "listener"`,
		},
		{
			name: "endpoint transport",
			err: &pgconn.PgError{
				Code: "08006", Message: "connection to postgresql://user:listener-secret@example.invalid/app?sslmode=require failed",
			},
			code: "notification_listener_endpoint_transport", retryable: true,
			cause: ": connection to postgresql://example.invalid/app failed (SQLSTATE 08006)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			logNotificationListenerFailure(workloadtest.Logger(t, &logs, "queue", "../../services/queue/observability.json"), ConsumerClassJobRunner, test.err)
			records := workloadtest.Records(t, logs.Bytes())
			if len(records) != 1 || records[0]["error.code"] != test.code || records[0]["retryable"] != test.retryable ||
				records[0]["error.message_safe"] != "queue notification listener disconnected" || records[0]["error.message"] != test.cause {
				t.Fatalf("listener record = %v; want code %s with cause %q", records, test.code, test.cause)
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
		done <- RunNotificationListener(ctx, listener, ConsumerClassJobRunner, wake, nil)
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
	listener.notify(ConsumerClassJobRunner)
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
	// A published fixture call means its readiness callback has completed.
	onReady()
	select {
	case l.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
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

func TestNotificationListenerExistingReconnectEmitsFailureThenRecovery(t *testing.T) {
	var logs bytes.Buffer
	listener := &scriptedNotificationListener{calls: make(chan int, 2), allowDisconnect: make(chan struct{})}
	wake := NewWakeSignal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunNotificationListener(ctx, listener, ConsumerClassJobRunner, wake, workloadtest.Logger(t, &logs, "queue", "../../services/queue/observability.json"))
	}()
	waitCall := func(want int) {
		t.Helper()
		select {
		case got := <-listener.calls:
			if got != want {
				t.Fatalf("listen call = %d", got)
			}
		case <-time.After(time.Second):
			t.Fatal("listener did not connect")
		}
	}
	waitCall(1)
	close(listener.allowDisconnect)
	waitCall(2)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener failed to stop")
	}
	body := logs.String()
	if strings.Count(body, `"event":"queue.notification_listener.disconnected"`) != 1 || strings.Count(body, `"event":"queue.notification_listener.recovered"`) != 1 || !strings.Contains(body, `"outcome":"recovered"`) || strings.Count(body, `"error.message":"connection lost"`) != 1 {
		t.Fatalf("reconnect records = %s", body)
	}
}

type notificationDiagnosticFaultWriter struct{}

func (notificationDiagnosticFaultWriter) Write([]byte) (int, error) { panic("diagnostic sink failed") }
func TestNotificationReconnectWakeSurvivesDiagnosticSinkFault(t *testing.T) {
	owner := workload.NewProcessLogger(notificationDiagnosticFaultWriter{}, "queue", "local", "unit", workload.DefaultDiagnosticConfig())
	defer owner.CloseWithBudget()
	listener := &scriptedNotificationListener{calls: make(chan int, 2), allowDisconnect: make(chan struct{})}
	wake := NewWakeSignal()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- RunNotificationListener(ctx, listener, ConsumerClassJobRunner, wake, owner.Logger)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("listener did not stop")
		}
	})
	for call := 1; call <= 2; call++ {
		select {
		case got := <-listener.calls:
			if got != call {
				t.Fatalf("listen call = %d", got)
			}
		case <-time.After(time.Second):
			t.Fatal("listener failed to connect")
		}
		if call == 1 {
			close(listener.allowDisconnect)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
	owner.CloseWithBudget()
	if wake.Snapshot().generation != 2 {
		t.Fatal("sink fault changed initial/reconnect catch-up wakes")
	}
	if owner.Stats().SinkFailures != 2 {
		t.Fatalf("sink failures = %+v", owner.Stats())
	}
}
