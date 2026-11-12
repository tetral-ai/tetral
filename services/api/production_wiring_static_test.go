package tetralapi

import (
	"os"
	"strings"
	"testing"
)

// The release of a Session's Sandbox at deletion goes with the Sandbox
// service; this check of its wiring goes with it.
func TestTetralAPIProductionConstructionRecordsSessionDeleteReleaseWithoutProviderWork(t *testing.T) {
	body, err := os.ReadFile("tetralapi.go")
	if err != nil {
		t.Fatalf("read tetralapi.go: %v", err)
	}
	source := string(body)
	for _, required := range []string{
		"session.WithSessionDeleteSandboxRelease(",
		"sandboxrelease.EnsureTx(",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("tetralapi.go missing Session-delete Sandbox release wiring %q", required)
		}
	}
	for _, forbidden := range []string{
		`"github.com/tetral-ai/tetral/services/sandbox"`,
		"session.WithSandboxLifecycle(",
		"sandbox.NewService(",
		"sandbox.NewUnavailableLifecycleProvider(",
		"sandbox.NewPostgreSQLStore(",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("tetralapi.go must not wire public Session API directly to sandbox lifecycle fragment %q", forbidden)
		}
	}
}
