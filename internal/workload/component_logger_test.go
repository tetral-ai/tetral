package workload

import (
	"bytes"
	"strings"
	"testing"
)

// NewLogger applies the documented resource defaults when deployment
// environment and version are unset.
func TestNewLoggerDefaultsUnsetResourceFields(t *testing.T) {
	var buffer bytes.Buffer
	NewLogger(&buffer, "svc", "", "").Info("probe")
	line := buffer.String()
	for _, required := range []string{
		`"service.name":"svc"`,
		`"service.version":"unknown"`,
		`"deployment.environment":"local"`,
	} {
		if !strings.Contains(line, required) {
			t.Fatalf("logger record missing %s in %s", required, line)
		}
	}
}

// Library default loggers use the installed process logger, so their records
// reach the process sink with the process identity. This test replaces the
// process-wide slog default and must not run in parallel.
func TestComponentLoggerUsesInstalledProcessLogger(t *testing.T) {
	var buffer bytes.Buffer
	owner := NewProcessLogger(&buffer, "proc", "env", "ver", DefaultDiagnosticConfig())
	restore := InstallDefaultLogger(owner.Logger)
	t.Cleanup(restore)

	ComponentLogger("api").Info("component.probe")
	owner.CloseWithBudget()

	line := buffer.String()
	for _, required := range []string{
		`"event":"component.probe"`,
		`"service.name":"proc"`,
		`"deployment.environment":"env"`,
		`"service.version":"ver"`,
	} {
		if !strings.Contains(line, required) {
			t.Fatalf("component record missing %s in %q", required, line)
		}
	}
}
