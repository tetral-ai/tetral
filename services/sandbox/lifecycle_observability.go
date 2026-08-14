package tetralsandbox

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type sandboxQueueAuthorityLossError struct {
	writer string
	err    error
}

func (e *sandboxQueueAuthorityLossError) Error() string { return e.err.Error() }
func (e *sandboxQueueAuthorityLossError) Unwrap() error { return e.err }

func queueAuthorityLostBy(writer string, err error) error {
	if err == nil {
		err = errQueueLeaseLost
	}
	return &sandboxQueueAuthorityLossError{writer: writer, err: err}
}

func queueAuthorityLossWriter(err error) string {
	var loss *sandboxQueueAuthorityLossError
	if errors.As(err, &loss) {
		return loss.writer
	}
	return ""
}

type activationAttemptStartedContextKey struct{}

func withActivationAttemptStarted(ctx context.Context, started time.Time) context.Context {
	return context.WithValue(ctx, activationAttemptStartedContextKey{}, started)
}

func activationAttemptDuration(ctx context.Context) time.Duration {
	started, _ := ctx.Value(activationAttemptStartedContextKey{}).(time.Time)
	if started.IsZero() {
		return 0
	}
	return time.Since(started)
}

func activationIdentityAttrs(job SandboxLifecycleJob) []slog.Attr {
	attrs := make([]slog.Attr, 0, 6)
	for _, field := range []struct {
		key   string
		value string
	}{
		{key: "workspace.id", value: job.WorkspaceID},
		{key: "session.id", value: job.SessionID},
		{key: "sandbox.id", value: job.LogicalSandboxID},
		{key: "operation.id", value: job.OperationID},
		{key: "job.id", value: job.JobID},
	} {
		if field.value != "" {
			attrs = append(attrs, slog.String(field.key, field.value))
		}
	}
	return attrs
}

func logSandboxActivationResolved(logger *slog.Logger, job SandboxLifecycleJob, resolution string, providerState string) {
	if logger == nil {
		return
	}
	attrs := activationIdentityAttrs(job)
	attrs = append(attrs,
		slog.String("event", "sandbox_activation_resolved"),
		slog.String("event.kind", "sandbox_activation_resolved"),
		slog.String("provider.name", "daytona"),
		slog.String("resolution", resolution),
	)
	if providerState != "" {
		attrs = append(attrs, slog.String("provider.state", providerState))
	}
	logger.LogAttrs(context.Background(), slog.LevelInfo, "sandbox.activation.resolved", attrs...)
}

func logSandboxQueueAuthorityLost(logger *slog.Logger, job SandboxLifecycleJob, queueKind string, writer string) {
	if logger == nil || writer == "" {
		return
	}
	attrs := activationIdentityAttrs(job)
	attrs = append(attrs,
		slog.String("event", "sandbox_queue_authority_lost"),
		slog.String("event.kind", "sandbox_queue_authority_lost"),
		slog.String("writer", writer),
		slog.String("queue.kind", queueKind),
	)
	logger.LogAttrs(context.Background(), slog.LevelInfo, "sandbox.queue.authority_lost", attrs...)
}

func logSandboxActivationAttemptCompleted(ctx context.Context, logger *slog.Logger, job SandboxLifecycleJob, outcome string, errorCode string, safeMessage string) {
	if logger == nil {
		return
	}
	attrs := activationIdentityAttrs(job)
	attrs = append(attrs,
		slog.String("event", "sandbox_activation_attempt_completed"),
		slog.String("event.kind", "sandbox_activation_attempt_completed"),
		slog.String("provider.name", "daytona"),
		slog.String("outcome", outcome),
		slog.Int("queue.attempt.count", job.AttemptCount),
		slog.Int("queue.attempt.max", job.MaxAttempts),
		slog.Int64("duration.ms", activationAttemptDuration(ctx).Milliseconds()),
	)
	level := slog.LevelInfo
	if errorCode != "" {
		attrs = append(attrs,
			slog.String("error.class", "sandbox_activation_error"),
			slog.String("error.code", errorCode),
			slog.String("error.message_safe", boundedProviderLogMessage(safeMessage)),
		)
	}
	if outcome == "terminal_failure" {
		level = slog.LevelError
	}
	logger.LogAttrs(context.Background(), level, "sandbox.activation.attempt_completed", attrs...)
}
