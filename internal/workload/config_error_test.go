package workload_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

// TestConfigErrorReportsSafeMessage proves a ConfigError surfaces its static
// operator-safe message verbatim through Error().
func TestConfigErrorReportsSafeMessage(t *testing.T) {
	err := workload.NewConfigError("ENGINE_VAULT_KEY must be 64 hex characters")
	if err.Error() != "ENGINE_VAULT_KEY must be 64 hex characters" {
		t.Fatalf("Error() = %q; want the safe message verbatim", err.Error())
	}
}

// TestAsConfigErrorDetectsDirectAndWrapped proves detection works on a bare
// ConfigError and through fmt.Errorf("%w") wrapping chains, so producers that
// add context with %w still classify as config errors.
func TestAsConfigErrorDetectsDirectAndWrapped(t *testing.T) {
	direct := workload.NewConfigError("TETRAL_KUBERNETES_NAMESPACE is required")
	if got, ok := workload.AsConfigError(direct); !ok || got.Error() != direct.Error() {
		t.Fatalf("AsConfigError(direct) = (%v, %v); want the same ConfigError", got, ok)
	}

	wrapped := fmt.Errorf("runtime control configuration: %w", direct)
	got, ok := workload.AsConfigError(wrapped)
	if !ok {
		t.Fatalf("AsConfigError did not detect a %%w-wrapped ConfigError: %v", wrapped)
	}
	if got.Error() != direct.Error() {
		t.Fatalf("AsConfigError(wrapped).Error() = %q; want the inner safe message %q", got.Error(), direct.Error())
	}
}

// TestAsConfigErrorRejectsNonConfigErrors proves a plain dependency error is not
// classified as a config error, so a dependency failure is a startup_error.
func TestAsConfigErrorRejectsNonConfigErrors(t *testing.T) {
	if _, ok := workload.AsConfigError(errors.New("dial tcp: connection refused")); ok {
		t.Fatal("AsConfigError classified a plain dependency error as a ConfigError")
	}
	if _, ok := workload.AsConfigError(nil); ok {
		t.Fatal("AsConfigError classified nil as a ConfigError")
	}
}

// A startup failure's record carries config_error or startup_error with the
// fixed class and safe text of that code, the boundary that failed, the
// error's own text as its cause, and that the command is neither ready nor
// listening.
func TestStartupFailureRecordCarriesItsCodeBoundaryAndCause(t *testing.T) {
	for _, row := range []struct {
		name          string
		err           error
		code, message string
		boundary      string
		configProblem any
	}{
		{"a configuration error", workload.NewConfigError("ENGINE_API_KEY is required"),
			"config_error", "configuration invalid", "configuration", "ENGINE_API_KEY is required"},
		{"a wrapped configuration error", fmt.Errorf("bootstrap api key: %w", workload.NewConfigError("ENGINE_API_KEY is too weak")),
			"config_error", "configuration invalid", "configuration", "ENGINE_API_KEY is too weak"},
		{"a schema failure", workload.WithStartupFailureCause(workload.StartupFailureCauseSchema, errors.New("schema is behind")),
			"startup_error", "startup failed", "schema", nil},
		{"a listener failure", workload.WithStartupFailureCause(workload.StartupFailureCauseListener, errors.New("listen tcp :8080: bind: address already in use")),
			"startup_error", "startup failed", "listener", nil},
		{"a dependency failure", workload.WithStartupFailureCause(workload.StartupFailureCauseDependencyReadiness, errors.New("dial tcp 10.0.0.5:5432: connect: connection refused")),
			"startup_error", "startup failed", "dependency_readiness", nil},
		{"a failure of no named boundary", errors.New("unexpected"),
			"startup_error", "startup failed", "unknown", nil},
	} {
		var buffer bytes.Buffer
		_ = workload.LogStartupFailure(workload.NewLogger(&buffer, "startup-test", "test", "unit"), "startup-test", row.err)
		var fields map[string]any
		if err := json.Unmarshal(buffer.Bytes(), &fields); err != nil {
			t.Fatalf("%s: decode startup record: %v; line=%s", row.name, err, buffer.String())
		}
		want := map[string]any{
			"level": "ERROR", "error.class": row.code, "error.code": row.code, "error.message_safe": row.message,
			"error.message": row.err.Error(), "startup.cause": row.boundary, "startup.config_problem": row.configProblem,
			"operation": "workload.startup", "event.kind": "startup_failed",
			"component": "startup-test", "readiness.state": "not ready", "listener.state": "not started",
		}
		for name, value := range want {
			if fields[name] != value {
				t.Errorf("%s: %s = %v; want %v", row.name, name, fields[name], value)
			}
		}
		if got := workload.StartupFailureCode(row.err); got != row.code {
			t.Errorf("%s: StartupFailureCode = %q; want %q", row.name, got, row.code)
		}
	}
}
