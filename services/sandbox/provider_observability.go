package tetralsandbox

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workload"
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
	metricOutcome := "success"
	if outcome.Failed() {
		metricOutcome = "error"
		if ctx.Err() == context.Canceled {
			metricOutcome = "cancelled"
		} else if ctx.Err() == context.DeadlineExceeded {
			metricOutcome = "timeout"
		}
	}
	if build, ok := any(outcome.Value).(sandbox.BuildArtifactResult); ok && build.State == sandbox.ArtifactBuildFailed {
		metricOutcome = "error"
	}
	operationMetrics(ctx).Observe(operation, metricOutcome, duration)
	if logger == nil {
		return
	}
	level := slog.LevelInfo
	result := "success"
	if outcome.Failed() {
		level = slog.LevelError
		result = "error"
	}
	buildFailed := false
	if build, ok := any(outcome.Value).(sandbox.BuildArtifactResult); ok && !outcome.Failed() {
		result = string(build.State)
		if build.State == sandbox.ArtifactBuildFailed {
			level = slog.LevelError
			buildFailed = true
		}
	}
	event := providerOperationEvent(level)
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("event", strings.ReplaceAll(event, ".", "_")),
		slog.String("event.kind", strings.ReplaceAll(event, ".", "_")),
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
			slog.Int("provider.status_code", outcome.ProviderStatusCode),
			slog.String("provider.failure_detail", boundedProviderLogMessage(message)),
		)
		attrs = appendProviderDiagnosticAttrs(attrs, outcome.Diagnostic)
		writeProviderFailure(ctx, logger, event, outcome.ErrorKind, message, attrs)
		return
	} else if buildFailed {
		writeProviderFailure(ctx, logger, event, "artifact_build_failed", "the provider reported the environment artifact build as failed", attrs)
		return
	}
	logger.LogAttrs(ctx, level, event, attrs...)
}

// writeProviderFailure writes a provider operation's failure record. Its
// cause is this service's own safe text for the failure, never the
// provider's response.
func writeProviderFailure(ctx context.Context, logger *slog.Logger, event, code, safeMessage string, attrs []slog.Attr) {
	fields := make([]any, len(attrs))
	for i, attr := range attrs {
		fields[i] = attr
	}
	workload.WriteFailure(ctx, logger, event, code, providerFailureCause(code, safeMessage), fields...)
}

// providerFailureCause is the cause of a provider failure: this service's
// safe text for it. The message of an output capture scan failure is written
// by the capture helper inside the Session's sandbox, so its cause names only
// the kind the helper reported.
func providerFailureCause(code, safeMessage string) workload.Cause {
	if reported, ok := strings.CutPrefix(code, "output_capture_scan_"); ok {
		safeMessage = "the sandbox output capture helper reported " + reported
	}
	return workload.CauseOf(errors.New(safeMessage))
}

// providerOperationEvent names a provider operation's record: a failure has an
// event of its own, so each event keeps one level.
func providerOperationEvent(level slog.Level) string {
	if level >= slog.LevelError {
		return "sandbox.provider.operation_failed"
	}
	return "sandbox.provider.operation_completed"
}

func logProviderCompletion(ctx context.Context, logger *slog.Logger, operation string, identity providerOperationIdentity, duration time.Duration, err error) {
	metricOutcome := "success"
	if err != nil {
		metricOutcome = "error"
		if errors.Is(err, context.Canceled) {
			metricOutcome = "cancelled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			metricOutcome = "timeout"
		}
	}
	operationMetrics(ctx).Observe(operation, metricOutcome, duration)
	if logger == nil {
		return
	}
	level := slog.LevelInfo
	outcome := "success"
	errorKind := ""
	statusCode := 0
	var diagnostic sandbox.ProviderDiagnostic
	safeMessage := ""
	if err != nil {
		level = slog.LevelError
		outcome = "error"
		errorKind = "provider_unknown"
		safeMessage = "sandbox provider operation failed"
		var providerErr *sandbox.ProviderError
		if errors.As(err, &providerErr) {
			diagnostic = providerErr.Diagnostic
			errorKind = string(providerErr.Kind)
			statusCode = providerErr.StatusCode
			if sandbox.ValidateProviderSafeMessage(providerErr.SafeMessage) == nil {
				safeMessage = providerErr.SafeMessage
			} else {
				safeMessage = "sandbox provider failure detail redacted"
			}
		}
	}
	event := providerOperationEvent(level)
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("event", strings.ReplaceAll(event, ".", "_")),
		slog.String("event.kind", strings.ReplaceAll(event, ".", "_")),
		slog.String("provider.name", "daytona"),
		slog.String("outcome", outcome),
		slog.Int64("duration.ms", duration.Milliseconds()),
	}
	attrs = appendProviderIdentityAttrs(attrs, identity)
	if err != nil {
		attrs = append(attrs,
			slog.Int("provider.status_code", statusCode),
			slog.String("provider.failure_detail", boundedProviderLogMessage(safeMessage)),
		)
		attrs = appendProviderDiagnosticAttrs(attrs, diagnostic)
		writeProviderFailure(ctx, logger, event, errorKind, safeMessage, attrs)
		return
	}
	logger.LogAttrs(ctx, level, event, attrs...)
}

func appendProviderDiagnosticAttrs(attrs []slog.Attr, diagnostic sandbox.ProviderDiagnostic) []slog.Attr {
	if diagnostic.Operation != "" && sandbox.ValidateProviderSafeMessage(diagnostic.Operation) == nil {
		attrs = append(attrs, slog.String("provider.operation", boundedProviderLogMessage(diagnostic.Operation)))
	}
	if diagnostic.Message != "" {
		message := diagnostic.Message
		if sandbox.ValidateProviderSafeMessage(message) != nil {
			message = "Provider diagnostic detail redacted."
		}
		attrs = append(attrs, slog.String("provider.error_detail", boundedProviderLogMessage(message)))
	}
	return attrs
}

func appendProviderIdentityAttrs(attrs []slog.Attr, identity providerOperationIdentity) []slog.Attr {
	for _, item := range []struct {
		key   string
		value string
	}{
		{key: "workspace.id", value: identity.workspaceID},
		{key: "session.id", value: identity.sessionID},
		{key: "thread.id", value: identity.threadID},
		{key: "environment.id", value: identity.environmentID},
		{key: "operation.id", value: identity.lifecycleOperationID},
		{key: "provider.resource.id", value: identity.providerResourceID},
		{key: "tool.use.event.id", value: identity.toolUseEventID},
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
