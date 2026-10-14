package static

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type sdkCompatibilityRegistryCase struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Surface     string `json:"surface"`
	Status      string `json:"status"`
	Proof       string `json:"proof"`
	Expectation string `json:"expectation"`
	Test        string `json:"test"`
	Locator     struct {
		Suite     string `json:"suite"`
		Scenario  string `json:"scenario"`
		Assertion string `json:"assertion"`
		Handler   string `json:"handler"`
	} `json:"locator"`
}

type sdkCompatibilityProofSuite struct {
	Suite  string `json:"suite"`
	Source string `json:"source"`
	Export string `json:"export"`
}

// TestSDKCompatibilityRowsEqualRegistry validates the public fork-SDK registry
// and executable proof locators. Its name is frozen because the CI traceability
// script selects it exactly with go test -run.
func TestSDKCompatibilityRowsEqualRegistry(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	sdkRoot := forkSDKRootForStaticTest(t, engineRoot)
	registryPath := filepath.Join(sdkRoot, "tests", "compatibility", "compat-cases.json")
	registryCases := readSDKCompatibilityRegistry(t, registryPath)

	registryIDs := make(map[string]struct{}, len(registryCases))
	for _, registryCase := range registryCases {
		if _, exists := registryIDs[registryCase.ID]; exists {
			t.Fatalf("duplicate SDK compatibility registry ID %q", registryCase.ID)
		}
		registryIDs[registryCase.ID] = struct{}{}
	}

	assertSDKCompatibilityExecutableLocators(t, sdkRoot, registryCases)
	amendedEngineProofIDs := map[string]struct{}{
		"T-COMPAT-AGENT-1":  {},
		"T-COMPAT-AGENT-3":  {},
		"T-COMPAT-AGENT-17": {},
		"T-COMPAT-SESS-3":   {},
		"T-COMPAT-SESS-15":  {},
		"T-COMPAT-SESS-16":  {},
		"T-COMPAT-SESS-17":  {},
		"T-COMPAT-SESS-18":  {},
	}
	assertAmendedSDKCompatibilityEngineTests(t, engineRoot, amendedEngineProofIDs)
}

func assertAmendedSDKCompatibilityEngineTests(t *testing.T, engineRoot string, ids map[string]struct{}) {
	t.Helper()
	files := []string{
		filepath.Join(engineRoot, "internal", "agent", "sdk_compatibility_test.go"),
		filepath.Join(engineRoot, "internal", "session", "sdk_compatibility_test.go"),
	}
	combined := ""
	for _, path := range files {
		combined += finalArchitectureReadText(t, path)
	}
	for id := range ids {
		marker := "// " + id + "\nfunc TestSDKCompatibility"
		if !strings.Contains(combined, marker) {
			t.Fatalf("amended SDK compatibility row %s lacks an executable named engine test", id)
		}
	}
}

func readSDKCompatibilityRegistry(t *testing.T, path string) []sdkCompatibilityRegistryCase {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // local sibling SDK registry is CI traceability input.
	if err != nil {
		t.Fatalf("read SDK compatibility registry %s: %v", path, err)
	}
	var cases []sdkCompatibilityRegistryCase
	if err := json.Unmarshal(body, &cases); err != nil {
		t.Fatalf("parse SDK compatibility registry %s: %v", path, err)
	}
	for index, registryCase := range cases {
		if registryCase.ID == "" || registryCase.Name == "" || registryCase.Surface == "" || registryCase.Status == "" || registryCase.Proof == "" || registryCase.Expectation == "" {
			t.Fatalf("SDK compatibility registry case %d must be a named, classified, non-empty proof case", index)
		}
		switch registryCase.Proof {
		case "documented", "typecheck", "integration", "rejection", "not-produced":
		default:
			t.Fatalf("SDK compatibility registry case %s has invalid proof category %q", registryCase.ID, registryCase.Proof)
		}

		switch registryCase.Status {
		case "rejected":
			if registryCase.Proof != "rejection" {
				t.Fatalf("SDK compatibility rejected case %s must use rejection proof, got %q", registryCase.ID, registryCase.Proof)
			}
		case "not-produced":
			if registryCase.Proof != "not-produced" {
				t.Fatalf("SDK compatibility not-produced case %s must use not-produced proof, got %q", registryCase.ID, registryCase.Proof)
			}
		case "supported":
			if registryCase.Proof != "documented" && registryCase.Proof != "typecheck" && registryCase.Proof != "integration" {
				t.Fatalf("SDK compatibility supported case %s must use documented, typecheck or integration proof, got %q", registryCase.ID, registryCase.Proof)
			}
		default:
			t.Fatalf("SDK compatibility registry case %s has invalid status %q", registryCase.ID, registryCase.Status)
		}
	}
	return cases
}

func assertSDKCompatibilityExecutableLocators(t *testing.T, sdkRoot string, cases []sdkCompatibilityRegistryCase) {
	t.Helper()
	manifestPath := filepath.Join(sdkRoot, "tests", "compatibility", "proof-suites.json")
	body, err := os.ReadFile(manifestPath) //nolint:gosec // local pinned SDK executable-suite manifest.
	if err != nil {
		t.Fatalf("read SDK compatibility proof suite manifest: %v", err)
	}
	var manifest map[string]sdkCompatibilityProofSuite
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatalf("parse SDK compatibility proof suite manifest: %v", err)
	}
	seenHandlers := map[string]struct{}{}
	for _, entry := range cases {
		locator := entry.Locator
		if entry.Proof != "integration" {
			if locator.Suite != "" || locator.Scenario != "" || locator.Assertion != "" || locator.Handler != "" {
				t.Fatalf("non-executable SDK compatibility case %s has an execution locator", entry.ID)
			}
			continue
		}
		if locator.Suite != "integration" || locator.Scenario == "" || locator.Assertion != entry.ID || locator.Handler != locator.Scenario+"#"+entry.ID {
			t.Fatalf("SDK integration case %s has an invalid locator: %+v", entry.ID, locator)
		}
		if _, duplicate := seenHandlers[locator.Handler]; duplicate {
			t.Fatalf("duplicate SDK integration handler %q", locator.Handler)
		}
		seenHandlers[locator.Handler] = struct{}{}
		suite, ok := manifest[locator.Scenario]
		if !ok || suite.Suite != "integration" || suite.Source == "" || suite.Export == "" {
			t.Fatalf("SDK integration case %s has no valid manifest binding", entry.ID)
		}
		if _, err := os.Stat(filepath.Join(sdkRoot, filepath.FromSlash(suite.Source))); err != nil {
			t.Fatalf("SDK integration scenario source %s: %v", suite.Source, err)
		}
	}
	if len(seenHandlers) == 0 {
		t.Fatal("SDK compatibility registry has no executable integration cases")
	}
}
