package workload

import (
	"context"
	"errors"
	"log/slog"
)

// A startup failure's error.code: config_error for a ConfigError, whose own
// message also goes to startup.config_problem, and startup_error for every
// other failure.
const (
	startupCodeConfig = "config_error"
	startupCodeOther  = "startup_error"
)

// StartupFailureCause identifies the startup boundary that failed.
type StartupFailureCause string

const (
	StartupFailureCauseConfiguration       StartupFailureCause = "configuration"
	StartupFailureCauseSchema              StartupFailureCause = "schema"
	StartupFailureCauseListener            StartupFailureCause = "listener"
	StartupFailureCauseDependencyReadiness StartupFailureCause = "dependency_readiness"
	StartupFailureCauseUnknown             StartupFailureCause = "unknown"
)

type startupFailureCauseError struct {
	cause StartupFailureCause
	err   error
}

func (e *startupFailureCauseError) Error() string { return e.err.Error() }
func (e *startupFailureCauseError) Unwrap() error { return e.err }

// WithStartupFailureCause marks the safe startup boundary classification that
// the caller knows at the point of failure.
func WithStartupFailureCause(cause StartupFailureCause, err error) error {
	if err == nil {
		return nil
	}
	return &startupFailureCauseError{cause: cause, err: err}
}

// ConfigError marks a startup failure that came from parsing or validating an
// env-derived configuration value. Its message is operator-safe.
//
// Safety contract: the message MAY name configuration KEYS, expected formats,
// and safe shape-validated derivations of a value (counts, file modes). It MUST
// NEVER embed the operator-supplied env VALUE itself — not the raw filesystem
// path, listen address, secret value, API key, vault key, database DSN, bearer
// token, or any raw request/response payload — because any such value can carry
// a secret-shaped segment. An operator-supplied value that must appear is
// dropped or shape-redacted first (see the safeListenAddress shape-redaction
// standard, which masks a value by its shape rather than by keyword matching).
// Producers are responsible for keeping the message static. Dependency and
// bootstrap failures (database connect, schema init, TokenReview/Kubernetes
// client construction) are NOT ConfigError; their record carries the
// dependency's error as its cause, shaped by WriteFailure.
type ConfigError struct {
	// Message is the static, operator-safe description of the misconfiguration.
	Message string
}

// NewConfigError builds a ConfigError carrying the given safe static message.
func NewConfigError(message string) *ConfigError {
	return &ConfigError{Message: message}
}

// Error returns the safe static message.
func (e *ConfigError) Error() string {
	return e.Message
}

// AsConfigError reports whether err is, or wraps (via fmt.Errorf("%w", ...)), a
// ConfigError and returns it when so. It is the single detection point every
// consumer uses to decide between config_error and startup_error classification.
func AsConfigError(err error) (*ConfigError, bool) {
	var configErr *ConfigError
	if errors.As(err, &configErr) {
		return configErr, true
	}
	return nil, false
}

// StartupFailureCode is the error.code of a startup failure: config_error
// for a ConfigError and startup_error for any other error.
func StartupFailureCode(err error) string {
	if _, ok := AsConfigError(err); ok {
		return startupCodeConfig
	}
	return startupCodeOther
}

// WriteStartupFailure writes event, a startup-failure record, for err: its
// code is StartupFailureCode, its cause err's text, and it carries the
// boundary that failed in startup.cause. base holds the record's other
// fields.
func WriteStartupFailure(logger *slog.Logger, event string, err error, base ...any) {
	code := StartupFailureCode(err)
	fields := append([]any{
		slog.String("operation", "workload.startup"),
		slog.String("event.kind", "startup_failed"),
	}, base...)
	fields = append(fields, slog.String("startup.cause", string(startupFailureCause(err))))
	if configErr, ok := AsConfigError(err); ok {
		fields = append(fields, slog.String("startup.config_problem", configErr.Message))
	}
	WriteFailure(context.Background(), logger, event, code, CauseOf(err), fields...)
}

func startupFailureCause(err error) StartupFailureCause {
	if _, ok := AsConfigError(err); ok {
		return StartupFailureCauseConfiguration
	}
	var caused *startupFailureCauseError
	if errors.As(err, &caused) {
		return caused.cause
	}
	return StartupFailureCauseUnknown
}

// LogStartupFailure emits the shared structured startup-failure record and
// returns err so command bootstraps can fail through a single expression.
func LogStartupFailure(logger *slog.Logger, component string, err error, base ...any) error {
	if logger == nil {
		return err
	}
	attrs := []any{
		slog.String("component", component),
		slog.String("readiness.state", "not ready"),
		slog.String("listener.state", "not started"),
	}
	attrs = append(attrs, base...)
	WriteStartupFailure(logger, "startup.failed", err, attrs...)
	failProcessColdStart(StartupFailureCode(err))
	return err
}
