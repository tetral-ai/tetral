package agentruntimebridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/tetral-ai/tetral/internal/queue"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

var errExecutionResultListenerUnavailable = errors.New("bridge execution result listener requires a database client")

// This file owns the Bridge Sandbox execution-result wake-up path.
//
// AwaitSandboxExecution waiters register on a process-local hub keyed by the
// execution's workspace-qualified durable identity. One LISTEN connection per
// Bridge API process receives refs-only hints from the Sandbox execution
// terminal writers and broadcasts each hint to exactly the local waiters it
// names; LISTEN readiness and every reconnect broadcast a catch-up to all
// local waiters. A hint is never a result: every wake leads through the
// durable verification read, and the one-second fallback bounds the recheck
// interval when a hint is coalesced, missed, or the listener is down.

// sandboxExecutionResultWakeHub routes execution-result hints to the local
// AwaitSandboxExecution waiters for one Bridge API store.
type sandboxExecutionResultWakeHub struct {
	mu      sync.Mutex
	waiters map[sandboxmodel.ExecutionResultHint]*sandboxExecutionResultWaitSet
}

type sandboxExecutionResultWaitSet struct {
	signal *queue.WakeSignal
	count  int
}

func newSandboxExecutionResultWakeHub() *sandboxExecutionResultWakeHub {
	return &sandboxExecutionResultWakeHub{waiters: map[sandboxmodel.ExecutionResultHint]*sandboxExecutionResultWaitSet{}}
}

// register adds one waiter for key. Callers must unregister exactly once.
func (h *sandboxExecutionResultWakeHub) register(key sandboxmodel.ExecutionResultHint) *queue.WakeSignal {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.waiters[key]
	if !ok {
		set = &sandboxExecutionResultWaitSet{signal: queue.NewWakeSignal()}
		h.waiters[key] = set
	}
	set.count++
	return set.signal
}

func (h *sandboxExecutionResultWakeHub) unregister(key sandboxmodel.ExecutionResultHint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.waiters[key]
	if !ok {
		return
	}
	set.count--
	if set.count <= 0 {
		delete(h.waiters, key)
	}
}

// notify broadcasts one received hint to the local waiters it names. Hints
// for executions with no local waiter are dropped; unmatched and malformed
// payloads never reach a waiter's wait set.
func (h *sandboxExecutionResultWakeHub) notify(hint sandboxmodel.ExecutionResultHint) {
	h.mu.Lock()
	set, ok := h.waiters[hint]
	h.mu.Unlock()
	if ok {
		set.signal.Broadcast()
	}
}

// broadcastAll wakes every local waiter. LISTEN readiness and reconnect use
// it as the catch-up for notifications missed while no connection was live.
func (h *sandboxExecutionResultWakeHub) broadcastAll() {
	h.mu.Lock()
	signals := make([]*queue.WakeSignal, 0, len(h.waiters))
	for _, set := range h.waiters {
		signals = append(signals, set.signal)
	}
	h.mu.Unlock()
	for _, signal := range signals {
		signal.Broadcast()
	}
}

// waiterCount reports the live registration count for lifecycle assertions.
func (h *sandboxExecutionResultWakeHub) waiterCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, set := range h.waiters {
		count += set.count
	}
	return count
}

func sandboxExecutionResultKey(scope *bridgev1.RuntimeScope, toolUseEventID string) sandboxmodel.ExecutionResultHint {
	return sandboxmodel.ExecutionResultHint{
		WorkspaceID:     scope.GetWorkspaceId(),
		SessionID:       scope.GetSessionId(),
		SessionThreadID: scope.GetSessionThreadId(),
		ToolUseEventID:  toolUseEventID,
	}
}

func (s *PostgreSQLBridgeAPIStore) executionResultWake() *sandboxExecutionResultWakeHub {
	s.executionResultHubOnce.Do(func() {
		s.executionResultHub = newSandboxExecutionResultWakeHub()
	})
	return s.executionResultHub
}

// RunExecutionResultListener owns this process's single LISTEN connection for
// Sandbox execution-result hints. It reconnects with the shared listener
// machinery until ctx is cancelled; readiness and each reconnect trigger a
// catch-up wake for all current waiters.
func (s *PostgreSQLBridgeAPIStore) RunExecutionResultListener(ctx context.Context) error {
	if s == nil || s.Client == nil {
		return errExecutionResultListenerUnavailable
	}
	return s.runExecutionResultListener(ctx, queue.PostgreSQLNotificationListener{
		Client: s.Client, Operation: "agentruntimebridge.listen_sandbox_execution_result",
	})
}

func (s *PostgreSQLBridgeAPIStore) runExecutionResultListener(ctx context.Context, listener queue.NotificationListener) error {
	hub := s.executionResultWake()
	var onDisconnect func(error)
	if s.Logger != nil {
		onDisconnect = func(err error) {
			failure := queue.ClassifyNotificationListenerFailure(err)
			s.Logger.Warn("bridge.execution_result_listener.disconnected",
				slog.String("operation", "agentruntimebridge.listen_sandbox_execution_result"),
				slog.String("event.kind", "listener_disconnected"),
				slog.String("error.class", "bridge_execution_result_listener_error"),
				slog.String("error.code", "notification_listener_"+failure.Category),
				slog.String("error.message_safe", "sandbox execution result listener disconnected"),
				slog.Bool("retryable", failure.Retryable),
				slog.Bool("terminal", false),
			)
		}
	}
	return queue.RunListener(ctx, listener, sandboxmodel.ExecutionResultNotificationChannel, hub.broadcastAll, func(payload string) {
		if hint, ok := sandboxmodel.ParseExecutionResultHint(payload); ok {
			hub.notify(hint)
		}
	}, onDisconnect)
}
