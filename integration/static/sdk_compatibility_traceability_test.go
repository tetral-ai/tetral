package static

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	if len(registryCases) != 285 {
		t.Fatalf("SDK compatibility registry cases = %d; want 285", len(registryCases))
	}

	registryIDs := make(map[string]struct{}, len(registryCases))
	for _, registryCase := range registryCases {
		if _, exists := registryIDs[registryCase.ID]; exists {
			t.Fatalf("duplicate SDK compatibility registry ID %q", registryCase.ID)
		}
		registryIDs[registryCase.ID] = struct{}{}
		if registryCase.Name != registryCase.ID+" "+registryCase.Surface {
			t.Fatalf("SDK compatibility registry case %s has non-canonical name %q", registryCase.ID, registryCase.Name)
		}
	}

	assertSDKCompatibilityIDsContinuous(t, "registry", registryCases)
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
		if registryCase.ID == "" || registryCase.Name == "" || registryCase.Surface == "" || registryCase.Status == "" || registryCase.Proof == "" || registryCase.Expectation == "" || registryCase.Locator.Suite == "" || registryCase.Locator.Scenario == "" || registryCase.Locator.Assertion == "" || registryCase.Locator.Handler == "" {
			t.Fatalf("SDK compatibility registry case %d must be a named, classified, non-empty proof case", index)
		}
		switch registryCase.Proof {
		case "live", "static", "rejection", "not-produced":
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
			if registryCase.Proof != "live" && registryCase.Proof != "static" {
				t.Fatalf("SDK compatibility supported case %s must use live or static proof, got %q", registryCase.ID, registryCase.Proof)
			}
		default:
			t.Fatalf("SDK compatibility registry case %s has invalid status %q", registryCase.ID, registryCase.Status)
		}
	}
	return cases
}

func assertSDKCompatibilityExecutableLocators(t *testing.T, sdkRoot string, cases []sdkCompatibilityRegistryCase) {
	t.Helper()
	registrationPath := filepath.Join(sdkRoot, "tests", "compatibility", "proof-registration.ts")
	registration := stripTypeScriptComments(finalArchitectureReadText(t, registrationPath))
	for _, token := range []string{
		"compatibilityProofHandlers[handler] = async",
		"const evidence = await execution.evidence(scenario)",
		"hasOwnProperty.call(evidence, assertion)",
		"evidence[assertion] !== true",
	} {
		if !strings.Contains(registration, token) {
			t.Fatalf("SDK compatibility executable registration %s lacks required code %q", registrationPath, token)
		}
	}
	manifestPath := filepath.Join(sdkRoot, "tests", "compatibility", "proof-suites.json")
	body, err := os.ReadFile(manifestPath) //nolint:gosec // local sibling SDK executable-suite manifest.
	if err != nil {
		t.Fatalf("read SDK compatibility proof suite manifest %s: %v", manifestPath, err)
	}
	var manifest map[string]sdkCompatibilityProofSuite
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatalf("parse SDK compatibility proof suite manifest %s: %v", manifestPath, err)
	}

	declaredScenarios := map[string]map[string]string{}
	sourceAssertionIDs := map[string]map[string]struct{}{}
	assignedAssertionIDs := map[string]map[string]struct{}{}
	seenHandlers := map[string]struct{}{}
	for _, compatibilityCase := range cases {
		locator := compatibilityCase.Locator
		wantSuite := "static"
		if compatibilityCase.Proof == "live" || compatibilityCase.Proof == "rejection" {
			wantSuite = "live"
		}
		if locator.Suite != wantSuite {
			t.Fatalf("SDK compatibility case %s locator suite = %q; want %q for proof %q", compatibilityCase.ID, locator.Suite, wantSuite, compatibilityCase.Proof)
		}
		if locator.Assertion != compatibilityCase.ID {
			t.Fatalf("SDK compatibility case %s locator assertion = %q; want its exact ID", compatibilityCase.ID, locator.Assertion)
		}
		if locator.Handler != locator.Scenario+"#"+compatibilityCase.ID {
			t.Fatalf("SDK compatibility case %s locator handler %q is not canonical", compatibilityCase.ID, locator.Handler)
		}
		if _, duplicate := seenHandlers[locator.Handler]; duplicate {
			t.Fatalf("duplicate SDK compatibility executable handler locator %q", locator.Handler)
		}
		seenHandlers[locator.Handler] = struct{}{}

		suite, ok := manifest[locator.Scenario]
		if !ok {
			t.Fatalf("SDK compatibility case %s references unregistered scenario %q", compatibilityCase.ID, locator.Scenario)
		}
		if suite.Suite != locator.Suite || suite.Source == "" || suite.Export == "" {
			t.Fatalf("SDK compatibility scenario %s has invalid suite manifest entry %+v", locator.Scenario, suite)
		}
		key := suite.Source + "#" + suite.Export
		if _, loaded := declaredScenarios[key]; !loaded {
			declaredScenarios[key] = parseSDKCompatibilityScenarioDeclarations(t, sdkRoot, suite)
			sourceAssertionIDs[key] = parseSDKCompatibilityAssertionIDs(t, sdkRoot, suite)
			assignedAssertionIDs[key] = map[string]struct{}{}
		}
		if _, declared := declaredScenarios[key][locator.Scenario]; !declared {
			t.Fatalf("SDK compatibility case %s scenario %q is not an executable key in %s export %s", compatibilityCase.ID, locator.Scenario, suite.Source, suite.Export)
		}
		if _, declared := sourceAssertionIDs[key][compatibilityCase.ID]; !declared {
			t.Fatalf("SDK compatibility case %s has no executable assertion literal in %s", compatibilityCase.ID, suite.Source)
		}
		assignedAssertionIDs[key][compatibilityCase.ID] = struct{}{}
	}
	for key, sourceIDs := range sourceAssertionIDs {
		assertStringSetsEqual(t, "SDK compatibility executable assertions in "+key, sourceIDs, assignedAssertionIDs[key])
	}
}

func parseSDKCompatibilityScenarioDeclarations(t *testing.T, sdkRoot string, suite sdkCompatibilityProofSuite) map[string]string {
	t.Helper()
	path := filepath.Join(sdkRoot, filepath.FromSlash(suite.Source))
	body := stripTypeScriptComments(finalArchitectureReadText(t, path))
	exportStart := strings.Index(body, "export const "+suite.Export)
	if exportStart < 0 {
		t.Fatalf("SDK compatibility suite %s lacks export const %s", suite.Source, suite.Export)
	}
	objectStart := strings.Index(body[exportStart:], "{")
	if objectStart < 0 {
		t.Fatalf("SDK compatibility suite %s export %s must be an object literal", suite.Source, suite.Export)
	}
	objectEnd := strings.Index(body[exportStart+objectStart:], "};")
	if objectEnd < 0 {
		t.Fatalf("SDK compatibility suite %s export %s must be an object literal", suite.Source, suite.Export)
	}
	object := body[exportStart+objectStart : exportStart+objectStart+objectEnd]
	declarations := map[string]string{}
	for _, line := range strings.Split(object, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "'") {
			continue
		}
		end := strings.Index(line[1:], "'")
		if end < 0 {
			continue
		}
		name := line[1 : end+1]
		remainder := strings.TrimSpace(line[end+2:])
		if !strings.HasPrefix(remainder, ":") {
			continue
		}
		runner := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(remainder[1:]), ","))
		if runner == "" || strings.ContainsAny(runner, " \t(){}[]") {
			t.Fatalf("SDK compatibility scenario %q in %s must map directly to a named runner", name, suite.Source)
		}
		if !strings.Contains(body, "async function "+runner+"(") {
			t.Fatalf("SDK compatibility scenario %q in %s references missing async runner %s", name, suite.Source, runner)
		}
		declarations[name] = runner
	}
	return declarations
}

func parseSDKCompatibilityAssertionIDs(t *testing.T, sdkRoot string, suite sdkCompatibilityProofSuite) map[string]struct{} {
	t.Helper()
	path := filepath.Join(sdkRoot, filepath.FromSlash(suite.Source))
	body := stripTypeScriptComments(finalArchitectureReadText(t, path))
	counts := map[string]int{}
	for index := 0; index < len(body); {
		quote := body[index]
		if quote != '\'' && quote != '"' && quote != '`' {
			index++
			continue
		}
		index++
		var literal strings.Builder
		for index < len(body) {
			character := body[index]
			if character == '\\' && index+1 < len(body) {
				literal.WriteByte(body[index+1])
				index += 2
				continue
			}
			index++
			if character == quote {
				break
			}
			literal.WriteByte(character)
		}
		value := literal.String()
		if strings.HasPrefix(value, "T-COMPAT-") {
			counts[value]++
		}
	}
	ids := make(map[string]struct{}, len(counts))
	for id, count := range counts {
		if count != 1 {
			t.Fatalf("SDK compatibility assertion %s occurs %d times in executable source %s; want exactly once", id, count, suite.Source)
		}
		ids[id] = struct{}{}
	}
	return ids
}

func stripTypeScriptComments(body string) string {
	var stripped strings.Builder
	stripped.Grow(len(body))
	for index := 0; index < len(body); {
		if index+1 < len(body) && body[index] == '/' && body[index+1] == '/' {
			index += 2
			for index < len(body) && body[index] != '\n' {
				index++
			}
			continue
		}
		if index+1 < len(body) && body[index] == '/' && body[index+1] == '*' {
			index += 2
			for index+1 < len(body) && (body[index] != '*' || body[index+1] != '/') {
				if body[index] == '\n' {
					stripped.WriteByte('\n')
				}
				index++
			}
			if index+1 < len(body) {
				index += 2
			}
			continue
		}
		quote := body[index]
		if quote != '\'' && quote != '"' && quote != '`' {
			stripped.WriteByte(body[index])
			index++
			continue
		}
		stripped.WriteByte(quote)
		index++
		for index < len(body) {
			character := body[index]
			stripped.WriteByte(character)
			index++
			if character == '\\' && index < len(body) {
				stripped.WriteByte(body[index])
				index++
				continue
			}
			if character == quote {
				break
			}
		}
	}
	return stripped.String()
}

func assertSDKCompatibilityIDsContinuous(t *testing.T, label string, cases []sdkCompatibilityRegistryCase) {
	t.Helper()
	families := map[string][]int{}
	for _, compatibilityCase := range cases {
		parts := strings.Split(compatibilityCase.ID, "-")
		if len(parts) != 4 || parts[0] != "T" || parts[1] != "COMPAT" {
			t.Fatalf("%s has invalid SDK compatibility ID %q", label, compatibilityCase.ID)
		}
		number, err := strconv.Atoi(parts[3])
		if err != nil || number < 1 {
			t.Fatalf("%s has invalid SDK compatibility sequence in %q", label, compatibilityCase.ID)
		}
		families[parts[2]] = append(families[parts[2]], number)
	}
	for family, numbers := range families {
		sort.Ints(numbers)
		for index, number := range numbers {
			if number != index+1 {
				t.Fatalf("%s T-COMPAT-%s numbering is not continuous: got %v", label, family, numbers)
			}
		}
	}
}

func assertStringSetsEqual(t *testing.T, label string, got map[string]struct{}, want map[string]struct{}) {
	t.Helper()
	var missing []string
	for token := range want {
		if _, ok := got[token]; !ok {
			missing = append(missing, token)
		}
	}
	var extra []string
	for token := range got {
		if _, ok := want[token]; !ok {
			extra = append(extra, token)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("%s mismatch\nmissing: %s\nextra: %s", label, strings.Join(missing, ", "), strings.Join(extra, ", "))
	}
}
