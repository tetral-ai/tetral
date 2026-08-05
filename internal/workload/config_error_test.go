package workload_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
// classified as a config error, keeping dependency failures class-only.
func TestAsConfigErrorRejectsNonConfigErrors(t *testing.T) {
	if _, ok := workload.AsConfigError(errors.New("dial tcp: connection refused")); ok {
		t.Fatal("AsConfigError classified a plain dependency error as a ConfigError")
	}
	if _, ok := workload.AsConfigError(nil); ok {
		t.Fatal("AsConfigError classified nil as a ConfigError")
	}
}

// TestStartupFailureFieldsClassifiesConfigAndDependency proves the shared
// consumer helper emits config_error + the safe message for ConfigError, and
// the constant startup_error + a constant safe message for dependency errors.
func TestStartupFailureFieldsClassifiesConfigAndDependency(t *testing.T) {
	configClass, configMessage, hasConfigMessage := workload.StartupFailureFields(
		fmt.Errorf("bootstrap api key: %w", workload.NewConfigError("ENGINE_API_KEY is too weak")),
	)
	if configClass != "config_error" {
		t.Fatalf("config class = %q; want config_error", configClass)
	}
	if !hasConfigMessage || configMessage != "ENGINE_API_KEY is too weak" {
		t.Fatalf("config message = (%q, %v); want the safe inner message", configMessage, hasConfigMessage)
	}

	depClass, depMessage, hasDepMessage := workload.StartupFailureFields(errors.New("dial tcp: connection refused"))
	if depClass != "startup_error" {
		t.Fatalf("dependency class = %q; want startup_error", depClass)
	}
	if !hasDepMessage || depMessage != "startup failed" {
		t.Fatalf("dependency message = (%q, %v); want the constant safe message", depMessage, hasDepMessage)
	}
}

// TestStartupFailureAttrsUsesSharedLogFieldNames proves startup failures use
// the shared error.code/error.message_safe names instead of legacy error.message
// while still suppressing dependency text.
func TestStartupFailureAttrsUsesSharedLogFieldNames(t *testing.T) {
	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil))
	logger.Error("startup.failed", workload.StartupFailureAttrs(workload.NewConfigError("ENGINE_API_KEY is required"))...)

	var fields map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &fields); err != nil {
		t.Fatalf("decode config startup log: %v; line=%s", err, buffer.String())
	}
	if fields["error.class"] != "config_error" ||
		fields["error.code"] != "config_error" ||
		fields["startup.cause"] != "configuration" ||
		fields["error.message_safe"] != "ENGINE_API_KEY is required" ||
		fields["operation"] != "workload.startup" ||
		fields["event.kind"] != "startup_failed" {
		t.Fatalf("config startup attrs = %#v", fields)
	}
	if _, ok := fields["error.message"]; ok {
		t.Fatalf("config startup log still emitted legacy error.message: %#v", fields)
	}

	buffer.Reset()
	logger.Error("startup.failed", workload.StartupFailureAttrs(workload.WithStartupFailureCause(
		workload.StartupFailureCauseDependencyReadiness,
		errors.New("dial tcp: secret database dsn"),
	))...)
	fields = map[string]any{}
	if err := json.Unmarshal(buffer.Bytes(), &fields); err != nil {
		t.Fatalf("decode dependency startup log: %v; line=%s", err, buffer.String())
	}
	if fields["error.class"] != "startup_error" ||
		fields["error.code"] != "startup_error" ||
		fields["startup.cause"] != "dependency_readiness" ||
		fields["error.message_safe"] != "startup failed" ||
		fields["operation"] != "workload.startup" ||
		fields["event.kind"] != "startup_failed" {
		t.Fatalf("dependency startup attrs = %#v", fields)
	}
	if strings.Contains(buffer.String(), "secret database dsn") {
		t.Fatalf("dependency startup log leaked dependency text: %s", buffer.String())
	}
}

func TestStartupFailureAttrsPreservesEverySafeCauseCategory(t *testing.T) {
	for _, cause := range []workload.StartupFailureCause{
		workload.StartupFailureCauseSchema,
		workload.StartupFailureCauseListener,
		workload.StartupFailureCauseDependencyReadiness,
		workload.StartupFailureCauseUnknown,
	} {
		var buffer bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buffer, nil))
		logger.Error("startup.failed", workload.StartupFailureAttrs(
			workload.WithStartupFailureCause(cause, errors.New("private lower-layer detail")),
		)...)

		var fields map[string]any
		if err := json.Unmarshal(buffer.Bytes(), &fields); err != nil {
			t.Fatalf("decode %s startup log: %v", cause, err)
		}
		if fields["startup.cause"] != string(cause) {
			t.Fatalf("startup cause = %v; want %s", fields["startup.cause"], cause)
		}
		if strings.Contains(buffer.String(), "private lower-layer detail") {
			t.Fatalf("startup log leaked lower-layer detail: %s", buffer.String())
		}
	}
}
