package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/pollbackoff"
)

const (
	NotificationChannel      = "tetral_queue_wakeup"
	ConsumerClassBridge      = "bridge"
	ConsumerClassSandbox     = "sandbox"
	listenerReconnectBase    = 100 * time.Millisecond
	listenerReconnectMaximum = 10 * time.Second
)

// ConsumerClassForKind maps every admitted Queue kind to the service that can
// lease it. Notification payloads carry only this class, never work content.
func ConsumerClassForKind(kind string) (string, bool) {
	switch kind {
	case KindRuntimeInput, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup:
		return ConsumerClassBridge, true
	case KindEnvironmentBuild, KindEnvironmentReadyFanout,
		KindSandboxToolExecute, KindSandboxActivate, KindSandboxMaterialize,
		KindSandboxRelease, KindSandboxToolCancel, KindSandboxOutputCapture,
		KindSandboxOutputCaptureCleanup, KindSandboxMemoryProjection,
		KindSandboxBackgroundCommand, KindSandboxBackgroundReconcile:
		return ConsumerClassSandbox, true
	default:
		return "", false
	}
}

type WakeSnapshot struct {
	generation uint64
	ready      <-chan struct{}
}

// WakeSignal is a broadcast hint. Its generation closes the race where a
// notification arrives after a poll but before the consumer starts waiting.
type WakeSignal struct {
	mu         sync.Mutex
	generation uint64
	ready      chan struct{}
}

func NewWakeSignal() *WakeSignal {
	return &WakeSignal{ready: make(chan struct{})}
}

func (s *WakeSignal) Snapshot() WakeSnapshot {
	if s == nil {
		return WakeSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return WakeSnapshot{generation: s.generation, ready: s.ready}
}

func (s *WakeSignal) Broadcast() {
	if s == nil {
		return
	}
	s.mu.Lock()
	close(s.ready)
	s.generation++
	s.ready = make(chan struct{})
	s.mu.Unlock()
}

func (s *WakeSignal) Wait(ctx context.Context, delay time.Duration, snapshot WakeSnapshot) error {
	if s == nil {
		return waitForWakeTimer(ctx, delay)
	}
	s.mu.Lock()
	if snapshot.generation != s.generation {
		s.mu.Unlock()
		return nil
	}
	ready := snapshot.ready
	s.mu.Unlock()
	if ready == nil {
		ready = s.Snapshot().ready
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		return nil
	case <-timer.C:
		return nil
	}
}

func waitForWakeTimer(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type NotificationListener interface {
	Listen(context.Context, string, func(), func(string)) error
}

type PostgreSQLNotificationListener struct {
	Client *dbconnect.Client
}

func (l PostgreSQLNotificationListener) Listen(ctx context.Context, channel string, onReady func(), onNotification func(string)) error {
	if l.Client == nil {
		return errors.New("queue notification database client is required")
	}
	return l.Client.Listen(ctx, "queue.listen", channel, onReady, onNotification)
}

// RunNotificationListener reconnects one service-owned LISTEN connection.
// Initial connection and every reconnect broadcast a catch-up poll.
func RunNotificationListener(ctx context.Context, listener NotificationListener, consumerClass string, wake *WakeSignal, logger *slog.Logger) error {
	if listener == nil || wake == nil {
		return errors.New("queue notification listener and wake signal are required")
	}
	if consumerClass != ConsumerClassBridge && consumerClass != ConsumerClassSandbox {
		return errors.New("queue notification consumer class is invalid")
	}
	backoff := pollbackoff.New(listenerReconnectBase, listenerReconnectMaximum)
	for {
		err := listener.Listen(ctx, NotificationChannel, wake.Broadcast, func(payload string) {
			if payload == consumerClass {
				wake.Broadcast()
			}
		})
		if ctx.Err() != nil {
			return nil
		}
		if logger != nil {
			logger.Warn("queue.notification_listener.disconnected",
				slog.String("operation", "queue.notification_listener"),
				slog.String("event.kind", "listener_disconnected"),
				slog.String("consumer.class", consumerClass),
				slog.String("error.class", "queue_notification_listener_error"),
				slog.String("error.code", "listener_disconnected"),
				slog.String("error.message_safe", "queue notification listener disconnected"),
				slog.Bool("retryable", true),
				slog.Bool("terminal", false),
			)
		}
		delay := backoff.Next(false)
		if err == nil {
			delay = backoff.Next(true)
		}
		if waitErr := waitForWakeTimer(ctx, delay); waitErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return waitErr
		}
	}
}
