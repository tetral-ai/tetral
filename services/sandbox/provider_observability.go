package tetralsandbox

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

const maxProviderLogMessageBytes = 512

type providerOperationIdentity struct {
	workspaceID          string
	sessionID            string
	threadID             string
	environmentID        string
	lifecycleOperationID string
	providerResourceID   string
	toolUseEventID       string
}

func observeProviderLookup[T any](ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, call func() (T, bool, error)) (result T, found bool, err error) {
	started := time.Now()
	defer func() {
		logProviderCompletion(ctx, logger, operation, identity, time.Since(started), err)
	}()
	return call()
}

func observeProviderCall[T any](ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, call func() (T, error)) (result T, err error) {
	started := time.Now()
	defer func() {
		logProviderCompletion(ctx, logger, operation, identity, time.Since(started), err)
	}()
	return call()
}

func observeProviderAction(ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, call func() error) (err error) {
	started := time.Now()
	defer func() {
		logProviderCompletion(ctx, logger, operation, identity, time.Since(started), err)
	}()
	return call()
}

func observeProviderOutcome[T any](ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, call func() ProviderOutcome[T]) (outcome ProviderOutcome[T]) {
	started := time.Now()
	defer func() {
		logProviderOutcomeCompletion(ctx, logger, operation, identity, time.Since(started), outcome)
	}()
	return call()
}

func logProviderOutcomeCompletion[T any](ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, duration time.Duration, outcome ProviderOutcome[T]) {
	if logger == nil {
		return
	}
	level := slog.LevelInfo
	result := "success"
	if outcome.Failed() {
		level = slog.LevelError
		result = "error"
	}
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("event.kind", "sandbox_provider_operation_completed"),
		slog.String("provider.name", "daytona"),
		slog.String("outcome", result),
		slog.Int64("duration.ms", duration.Milliseconds()),
	}
	attrs = appendProviderIdentityAttrs(attrs, identity)
	if outcome.Failed() {
		message := outcome.ProviderSafeMessage
		if message == "" {
			message = outcome.SafeMessage
		}
		if sandbox.ValidateProviderSafeMessage(message) != nil {
			message = "sandbox provider failure detail redacted"
		}
		attrs = append(attrs,
			slog.String("error.kind", outcome.ErrorKind),
			slog.Int("provider.status_code", outcome.ProviderStatusCode),
			slog.String("error.message_safe", boundedProviderLogMessage(message)),
		)
	}
	logger.LogAttrs(ctx, level, "sandbox.provider.operation_completed", attrs...)
}

func logProviderCompletion(ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, duration time.Duration, err error) {
	if logger == nil {
		return
	}
	level := slog.LevelInfo
	outcome := "success"
	errorKind := ""
	statusCode := 0
	safeMessage := ""
	if err != nil {
		level = slog.LevelError
		outcome = "error"
		errorKind = "provider_unknown"
		safeMessage = "sandbox provider operation failed"
		var providerErr *sandbox.ProviderError
		if errors.As(err, &providerErr) {
			errorKind = string(providerErr.Kind)
			statusCode = providerErr.StatusCode
			if sandbox.ValidateProviderSafeMessage(providerErr.SafeMessage) == nil {
				safeMessage = providerErr.SafeMessage
			} else {
				safeMessage = "sandbox provider failure detail redacted"
			}
		}
	}
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("event.kind", "sandbox_provider_operation_completed"),
		slog.String("provider.name", "daytona"),
		slog.String("outcome", outcome),
		slog.Int64("duration.ms", duration.Milliseconds()),
	}
	attrs = appendProviderIdentityAttrs(attrs, identity)
	if err != nil {
		attrs = append(attrs,
			slog.String("error.kind", errorKind),
			slog.Int("provider.status_code", statusCode),
			slog.String("error.message_safe", boundedProviderLogMessage(safeMessage)),
		)
	}
	logger.LogAttrs(ctx, level, "sandbox.provider.operation_completed", attrs...)
}

func appendProviderIdentityAttrs(attrs []slog.Attr, identity providerOperationIdentity) []slog.Attr {
	for _, item := range []struct {
		key   string
		value string
	}{
		{key: "workspace.id", value: identity.workspaceID},
		{key: "session.id", value: identity.sessionID},
		{key: "session.thread.id", value: identity.threadID},
		{key: "environment.id", value: identity.environmentID},
		{key: "sandbox.lifecycle_operation.id", value: identity.lifecycleOperationID},
		{key: "provider.resource.id", value: identity.providerResourceID},
		{key: "tool_use.event.id", value: identity.toolUseEventID},
	} {
		if item.value != "" {
			attrs = append(attrs, slog.String(item.key, item.value))
		}
	}
	return attrs
}

func providerIdentityForSetup(setup sandbox.SandboxSetup, providerResourceID string) providerOperationIdentity {
	return providerOperationIdentity{
		workspaceID: string(setup.WorkspaceID), sessionID: setup.SessionID,
		environmentID: setup.EnvironmentID, lifecycleOperationID: setup.LifecycleOperationID,
		providerResourceID: providerResourceID,
	}
}

func providerIdentityForTool(target string, workspaceID string, sessionID string, threadID string, toolUseEventID string) providerOperationIdentity {
	return providerOperationIdentity{
		workspaceID: workspaceID, sessionID: sessionID, threadID: threadID,
		providerResourceID: target, toolUseEventID: toolUseEventID,
	}
}

func boundedProviderLogMessage(value string) string {
	value = strings.ToValidUTF8(strings.TrimSpace(value), "")
	if len(value) <= maxProviderLogMessageBytes {
		return value
	}
	value = value[:maxProviderLogMessageBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
