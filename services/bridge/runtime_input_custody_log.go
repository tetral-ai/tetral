package agentruntimebridge

import (
	"log/slog"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// logRuntimeInputCustodyTransition emits bounded post-commit evidence. Durable
// Inbox and Queue state remain authoritative; logging is never consulted by a
// lifecycle decision.
func logRuntimeInputCustodyTransition(logger *slog.Logger, scope *bridgev1.RuntimeScope, transition string, count int) {
	if logger == nil || scope == nil || count <= 0 {
		return
	}
	attributes := []any{
		slog.String("event.kind", "runtime_input_custody_transition"),
		slog.String("operation", "runtime_input_custody_transition"),
		slog.String("component", ServiceNameBridgeAPI),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("outcome", transition),
		slog.Int("input.count", count),
	}
	if scope.GetSessionThreadId() != "" {
		attributes = append(attributes, slog.String("thread.id", scope.GetSessionThreadId()))
	}
	logger.Info("runtime_input_custody_transition", attributes...)
}
