package queue

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

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
	case KindRuntimeInput, KindRuntimeRecovery, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup:
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
	timer := time.NewTimer(delay)
	defer timer.Stop()
	return s.wait(ctx, timer.C, snapshot)
}

// WaitForWake waits only for a broadcast or context cancellation/deadline.
// Like Wait, it preserves broadcasts occurring after the caller's snapshot.
func (s *WakeSignal) WaitForWake(ctx context.Context, snapshot WakeSnapshot) error {
	return s.wait(ctx, nil, snapshot)
}

func (s *WakeSignal) wait(ctx context.Context, timer <-chan time.Time, snapshot WakeSnapshot) error {
	var ready <-chan struct{}
	if s != nil {
		s.mu.Lock()
		if snapshot.generation != s.generation {
			s.mu.Unlock()
			return nil
		}
		ready = snapshot.ready
		s.mu.Unlock()
		if ready == nil {
			ready = s.Snapshot().ready
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		return nil
	case <-timer:
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
	// Operation labels the LISTEN for diagnostics; empty means "queue.listen".
	Operation string
}

func (l PostgreSQLNotificationListener) Listen(ctx context.Context, channel string, onReady func(), onNotification func(string)) error {
	if l.Client == nil {
		return errors.New("queue notification database client is required")
	}
	operation := l.Operation
	if operation == "" {
		operation = "queue.listen"
	}
	return l.Client.Listen(ctx, operation, channel, onReady, onNotification)
}

// RunListener reconnects one service-owned LISTEN connection on channel until
// ctx is cancelled. onReady fires after the initial LISTEN and every reconnect
// so the consumer can catch up on notifications missed while disconnected;
// onPayload receives raw payloads. onDisconnect observes each dropped
// connection for diagnostics. This is the shared mechanism behind the Queue
// wakeup protocol; other PostgreSQL notification protocols (for example the
// Sandbox execution-result hints consumed by Bridge) reuse it with their own
// channel, payload handling, and disconnect logging.
func RunListener(ctx context.Context, listener NotificationListener, channel string, onReady func(), onPayload func(string), onDisconnect func(error)) error {
	if listener == nil {
		return errors.New("queue notification listener is required")
	}
	if onPayload == nil {
		onPayload = func(string) {}
	}
	backoff := pollbackoff.New(listenerReconnectBase, listenerReconnectMaximum)
	for {
		err := listener.Listen(ctx, channel, onReady, onPayload)
		if ctx.Err() != nil {
			return nil
		}
		if onDisconnect != nil {
			onDisconnect(err)
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

// RunNotificationListener reconnects one service-owned LISTEN connection.
// Initial connection and every reconnect broadcast a catch-up poll.
func RunNotificationListener(ctx context.Context, listener NotificationListener, consumerClass string, wake *WakeSignal, logger *slog.Logger) error {
	if listener == nil || wake == nil {
		return errors.New("queue notification listener and wake signal are required")
	}
	if consumerClass != ConsumerClassBridge && consumerClass != ConsumerClassSandbox {
		return errors.New("queue notification consumer class is invalid")
	}
	var onDisconnect func(error)
	if logger != nil {
		onDisconnect = func(err error) { logNotificationListenerFailure(logger, consumerClass, err) }
	}
	return RunListener(ctx, listener, NotificationChannel, wake.Broadcast, func(payload string) {
		if payload == consumerClass {
			wake.Broadcast()
		}
	}, onDisconnect)
}

type notificationListenerFailure struct {
	category  string
	retryable bool
}

func logNotificationListenerFailure(logger *slog.Logger, consumerClass string, err error) {
	failure := classifyNotificationListenerFailure(err)
	logger.Warn("queue.notification_listener.disconnected",
		slog.String("operation", "queue.notification_listener"),
		slog.String("event.kind", "listener_disconnected"),
		slog.String("consumer.class", consumerClass),
		slog.String("error.class", "queue_notification_listener_error"),
		slog.String("error.code", "notification_listener_"+failure.category),
		slog.String("error.message_safe", "queue notification listener disconnected"),
		slog.Bool("retryable", failure.retryable),
		slog.Bool("terminal", false),
	)
}

// NotificationListenerFailure is the safe disconnect classification shared by
// every service running a PostgreSQL notification listener.
type NotificationListenerFailure struct {
	Category  string
	Retryable bool
}

// ClassifyNotificationListenerFailure normalizes a dropped LISTEN connection
// into a log-safe category; raw causes never cross this boundary.
func ClassifyNotificationListenerFailure(err error) NotificationListenerFailure {
	failure := classifyNotificationListenerFailure(err)
	return NotificationListenerFailure{Category: failure.category, Retryable: failure.retryable}
}

func classifyNotificationListenerFailure(err error) notificationListenerFailure {
	var diagnostic *dbconnect.DiagnosticError
	if errors.As(err, &diagnostic) {
		switch diagnostic.Kind {
		case dbconnect.KindAuthenticationFailed:
			return notificationListenerFailure{category: "authentication", retryable: false}
		case dbconnect.KindPermissionDenied, dbconnect.KindRuntimeRoleInvalid:
			return notificationListenerFailure{category: "permission", retryable: false}
		case dbconnect.KindEndpointUnreachable, dbconnect.KindTLSFailed:
			return notificationListenerFailure{category: "endpoint_transport", retryable: true}
		case dbconnect.KindTimeout:
			return notificationListenerFailure{category: "timeout", retryable: true}
		}
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch {
		case len(pgError.Code) >= 2 && pgError.Code[:2] == "28":
			return notificationListenerFailure{category: "authentication", retryable: false}
		case pgError.Code == "42501":
			return notificationListenerFailure{category: "permission", retryable: false}
		case len(pgError.Code) >= 2 && pgError.Code[:2] == "08", pgError.Code == "57P01":
			return notificationListenerFailure{category: "endpoint_transport", retryable: true}
		case pgError.Code == "57014":
			return notificationListenerFailure{category: "timeout", retryable: true}
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return notificationListenerFailure{category: "timeout", retryable: true}
	}
	return notificationListenerFailure{category: "unknown", retryable: true}
}
