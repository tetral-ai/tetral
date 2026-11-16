package agentruntimebridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol/resultnotify"
	"github.com/tetral-ai/tetral/internal/workload"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

var errExecutionResultListenerUnavailable = errors.New("bridge execution result listener requires a database client")

// This file owns Bridge's tool-result wake-up path.
//
// AwaitSandboxExecution and AwaitToolCall waiters register on a process-local
// hub keyed by the call's workspace-qualified durable identity. One LISTEN
// connection per Bridge API process receives the refs-only notices
// (internal/runtimecontrol/resultnotify) of every writer that settles a call
// and wakes exactly the local waiters a notice names; LISTEN readiness and
// every reconnect broadcast a catch-up to all local waiters. A hint is never a
// result: every wake leads through the durable verification read. There is no
// periodic result query within a wait; listener recovery broadcasts catch-up,
// and the Runtime rejoins after AwaitSandboxExecution's deadline, or after an
// AwaitToolCall RPC breaks, if a result remains unobserved while the listener
// is unavailable.

// executionResultWakeHub routes result notices to the local waiters of one
// Bridge API store.
type executionResultWakeHub struct {
	mu      sync.Mutex
	waiters map[resultnotify.ExecutionResultHint]*executionResultWaitSet
}

type executionResultWaitSet struct {
	signal *queue.WakeSignal
	count  int
}

func newExecutionResultWakeHub() *executionResultWakeHub {
	return &executionResultWakeHub{waiters: map[resultnotify.ExecutionResultHint]*executionResultWaitSet{}}
}

// register adds one waiter for key. Callers must unregister exactly once.
func (h *executionResultWakeHub) register(key resultnotify.ExecutionResultHint) *queue.WakeSignal {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.waiters[key]
	if !ok {
		set = &executionResultWaitSet{signal: queue.NewWakeSignal()}
		h.waiters[key] = set
	}
	set.count++
	return set.signal
}

func (h *executionResultWakeHub) unregister(key resultnotify.ExecutionResultHint) {
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
func (h *executionResultWakeHub) notify(hint resultnotify.ExecutionResultHint) {
	h.mu.Lock()
	set, ok := h.waiters[hint]
	h.mu.Unlock()
	if ok {
		set.signal.Broadcast()
	}
}

// broadcastAll wakes every local waiter. LISTEN readiness and reconnect use
// it as the catch-up for notifications missed while no connection was live.
func (h *executionResultWakeHub) broadcastAll() {
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

func executionResultKey(scope *bridgev1.RuntimeScope, toolUseEventID string) resultnotify.ExecutionResultHint {
	return resultnotify.ExecutionResultHint{
		WorkspaceID:     scope.GetWorkspaceId(),
		SessionID:       scope.GetSessionId(),
		SessionThreadID: scope.GetSessionThreadId(),
		ToolUseEventID:  toolUseEventID,
	}
}

func (s *PostgreSQLBridgeAPIStore) executionResultWake() *executionResultWakeHub {
	s.executionResultHubOnce.Do(func() {
		s.executionResultHub = newExecutionResultWakeHub()
	})
	return s.executionResultHub
}

// RunExecutionResultListener owns this process's single LISTEN connection for
// tool result notices. It reconnects with the shared listener
// machinery until ctx is cancelled; readiness and each reconnect trigger a
// catch-up wake for all current waiters.
func (s *PostgreSQLBridgeAPIStore) RunExecutionResultListener(ctx context.Context) error {
	if s == nil || s.Client == nil {
		return errExecutionResultListenerUnavailable
	}
	return s.runExecutionResultListener(ctx, queue.PostgreSQLNotificationListener{
		Client: s.Client, Operation: "agentruntimebridge.listen_tool_execution_result",
	})
}

func (s *PostgreSQLBridgeAPIStore) runExecutionResultListener(ctx context.Context, listener queue.NotificationListener) error {
	hub := s.executionResultWake()
	var onDisconnect func(error)
	if s.Logger != nil {
		onDisconnect = func(err error) {
			failure := queue.ClassifyNotificationListenerFailure(err)
			workload.WriteFailure(ctx, s.Logger, "bridge.execution_result_listener.disconnected", "notification_listener_"+failure.Category, workload.CauseOf(err),
				slog.String("operation", "agentruntimebridge.listen_tool_execution_result"),
				slog.String("event.kind", "listener_disconnected"),
				slog.Bool("retryable", failure.Retryable),
				slog.Bool("terminal", false),
			)
		}
	}
	return queue.RunListener(ctx, listener, resultnotify.ExecutionResultNotificationChannel, hub.broadcastAll, func(payload string) {
		if hint, ok := resultnotify.ParseExecutionResultHint(payload); ok {
			hub.notify(hint)
		}
	}, onDisconnect)
}
