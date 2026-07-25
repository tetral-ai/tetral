package workload

import (
	"errors"
	"log/slog"
)

// startupErrorClassConfig classifies a configuration-validation failure on a
// workload startup path. Its safe static message is emitted to operators.
const startupErrorClassConfig = "config_error"

// startupErrorClassConstant classifies every other startup failure (dependency
// and bootstrap failures). It is emitted with a constant safe message because
// such errors may embed DSNs, tokens, or provider payloads.
const startupErrorClassConstant = "startup_error"
const startupErrorMessageSafe = "startup failed"

// ConfigError marks a startup failure that came from parsing or validating an
// env-derived configuration value. Its message is operator-safe and is the only
// startup error text any workload emits to logs or stderr.
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
// client construction) are NOT ConfigError and stay class-only.
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

// StartupFailureFields maps a startup error to the safe classification every
// workload startup-failure line emits. A ConfigError yields class config_error
// plus its safe message; any other error yields the constant startup_error with
// a safe constant message so dependency error text never reaches logs.
func StartupFailureFields(err error) (class string, message string, hasMessage bool) {
	if configErr, ok := AsConfigError(err); ok {
		return startupErrorClassConfig, configErr.Message, true
	}
	return startupErrorClassConstant, startupErrorMessageSafe, true
}

// StartupFailureAttrs appends the contract error fields to a startup-failure log
// line: error.class/error.code are config_error for a ConfigError and
// startup_error otherwise; error.message_safe is always populated with either the
// config-safe message or a constant dependency-safe message. Callers pass the
// line's other attributes as base and spread the result into Logger.Error, so
// every workload startup-failure line classifies identically instead of carrying
// a divergent copy.
func StartupFailureAttrs(err error, base ...any) []any {
	class, message, hasMessage := StartupFailureFields(err)
	attrs := append([]any{
		slog.String("operation", "workload.startup"),
		slog.String("event.kind", "startup_failed"),
	}, base...)
	attrs = append(attrs,
		slog.String("error.class", class),
		slog.String("error.code", class),
	)
	if hasMessage {
		attrs = append(attrs, slog.String("error.message_safe", message))
	}
	return attrs
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
	logger.Error("startup.failed", StartupFailureAttrs(err, attrs...)...)
	return err
}
