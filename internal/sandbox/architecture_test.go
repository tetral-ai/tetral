package sandbox

import (
	"os"
	"strings"
	"testing"
)

func TestSandboxLifecycleBoundaryRejectsObsoleteGenericProviderSurface(t *testing.T) {
	for _, name := range []string{"sandbox.go", "service.go"} {
		body, err := os.ReadFile(name) //nolint:gosec // repository-local architecture fixture.
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		source := string(body)
		for _, forbidden := range []string{
			"type " + "Provider interface",
			"provider " + "Provider",
			"NewUnavailable" + "Provider",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s retains obsolete sandbox provider surface %q", name, forbidden)
			}
		}
	}
}
