package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

const sdkPreviewCompatibilityTest = "TestPostgreSQLPublicStreamingIdentity/session-options-primary-thread-and-private-content"

// Full and Affected launch the registered SDK proofs while their declared
// dependencies are still alive. The SDK's fixed selectors exclude this
// wrapper. Both owners validate actual preview and typed OIDC observations.
func TestForkSDKIntegrationCompatibilityProofs(t *testing.T) {
	sdkRoot := strings.TrimSpace(os.Getenv("TETRAL_ENGINE_SDK_ROOT"))
	if sdkRoot == "" {
		t.Skip("set TETRAL_ENGINE_SDK_ROOT to run the registered SDK integration proofs")
	}
	engineRoot := repoRootFromBridgeTest(t)
	ctx, cancel := context.WithTimeout(t.Context(), 24*time.Minute)
	defer cancel()
	engineRevision := sdkIntegrationSourceRevision(ctx, t, engineRoot)
	sdkRevision := sdkIntegrationSourceRevision(ctx, t, sdkRoot)
	// The command and script are fixed; the runner supplies the pinned checkout.
	// Inherit its process group, descendant registry and database run capability
	// so the nested Go/Bun processes remain under the same cleanup owner.
	//nolint:gosec
	command := exec.CommandContext(ctx, "bun", "run", "test:compatibility:integration")
	command.Dir = sdkRoot
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if name != "TETRAL_ENGINE_ROOT" && name != "TETRAL_ENGINE_REVISION" {
			command.Env = append(command.Env, item)
		}
	}
	command.Env = append(command.Env, "TETRAL_ENGINE_ROOT="+engineRoot, "TETRAL_ENGINE_REVISION="+engineRevision)
	// Bound pipe joining if cancellation kills the launcher before a descendant
	// exits. The native runner retains process and database cleanup custody.
	command.WaitDelay = 5 * time.Second
	output, err := command.CombinedOutput()
	t.Logf("registered SDK integration compatibility output:\n%s", output)
	if err != nil {
		t.Fatalf("registered SDK integration compatibility failed: %v (context: %v)", err, ctx.Err())
	}
	if err := validateSDKIntegrationCompatibilityOutput(string(output), engineRevision, sdkRevision); err != nil {
		t.Fatalf("registered SDK integration compatibility evidence: %v", err)
	}
}

func sdkIntegrationSourceRevision(ctx context.Context, t *testing.T, root string) string {
	t.Helper()
	//nolint:gosec // Local source root; fixed read-only Git operation.
	output, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("resolve SDK compatibility source revision: %v", err)
	}
	return strings.TrimSpace(string(output))
}

type sdkIntegrationSourceBinding struct {
	EngineRevision string `json:"engineRevision"`
	SDKRevision    string `json:"sdkRevision"`
}

type sdkIntegrationScenario struct {
	test, marker      string
	tests, assertions []string
}

var sdkIntegrationScenarios = map[string]sdkIntegrationScenario{
	"integration-preview": {
		test:   sdkPreviewCompatibilityTest,
		marker: "public_streaming_sdk_assertion=",
		tests:  []string{"TestPostgreSQLPublicStreamingIdentity", sdkPreviewCompatibilityTest},
		assertions: []string{
			"actual-sdk-session-query-options-thread-formal-only",
			"exact-sdk-preview-wrappers-and-per-event-prefix",
			"formal-original-identities-content-end-order",
			"durable-private-reasoning-and-metadata-no-preview-leak",
			"Gateway-allocation-Runtime-submission-Bridge-receipt-SQL-SDK-identities-match",
		},
	},
	"integration-oidc": {
		test:   "TestOIDCKeycloakSDK",
		marker: "oidc_sdk_assertion=",
		tests:  []string{"TestOIDCKeycloakSDK", "TestOIDCKeycloakSDK/human", "TestOIDCKeycloakSDK/service"},
		assertions: []string{
			"human_session_memory_cached_token_one401_one_exchange_one_effect",
			"service_session_memory_cached_token_one401_one_exchange_one_effect",
			"human_typed_created_by_redacted_by_stable_identity",
			"service_typed_created_by_redacted_by_stable_identity",
		},
	},
}

type sdkIntegrationGoEvent struct {
	Action, Package, Test, Output string
}

// Exit zero admits an empty registry. Require each fixed scenario's unique
// source/pass pair, executed Go observations, exact assertions and handler result.
func validateSDKIntegrationCompatibilityOutput(output, engineRevision, sdkRevision string) error {
	want := sdkIntegrationSourceBinding{EngineRevision: engineRevision, SDKRevision: sdkRevision}
	sources, passes := map[string]bool{}, map[string]bool{}
	summaries := 0
	active := ""
	var events []sdkIntegrationGoEvent
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Executed ") && strings.Contains(line, "integration compatibility handlers:") {
			if line != "Executed 4 integration compatibility handlers: 4 passed, 0 failed" {
				return fmt.Errorf("SDK integration handler summary does not prove all four registered handlers passed: %s", line)
			}
			summaries++
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		if !strings.Contains(line, `"compatibility_integration_source"`) && !strings.Contains(line, `"compatibility_integration_pass"`) {
			if active != "" {
				var event sdkIntegrationGoEvent
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					return fmt.Errorf("decode SDK integration Go observation: %w", err)
				}
				events = append(events, event)
			}
			continue
		}
		var marker struct {
			Source     *sdkIntegrationSourceBinding `json:"compatibility_integration_source"`
			Pass       *sdkIntegrationSourceBinding `json:"compatibility_integration_pass"`
			Scenario   string                       `json:"scenario"`
			Test       string                       `json:"test"`
			Assertions []string                     `json:"assertions"`
		}
		if err := json.Unmarshal([]byte(line), &marker); err != nil {
			return fmt.Errorf("decode SDK integration marker: %w", err)
		}
		scenario, known := sdkIntegrationScenarios[marker.Scenario]
		if !known || marker.Test != scenario.test || (marker.Source == nil) == (marker.Pass == nil) {
			return fmt.Errorf("SDK integration marker does not name one fixed scenario and test")
		}
		if marker.Source != nil {
			if *marker.Source != want || sources[marker.Scenario] || active != "" {
				return fmt.Errorf("SDK integration source binding differs, duplicates or overlaps")
			}
			sources[marker.Scenario], active, events = true, marker.Scenario, nil
		} else {
			if *marker.Pass != want || active != marker.Scenario || passes[marker.Scenario] || !slices.Equal(marker.Assertions, scenario.assertions) {
				return fmt.Errorf("SDK integration pass differs from launched sources or required assertions")
			}
			if err := validateSDKIntegrationObservations(events, marker.Scenario, sdkRevision); err != nil {
				return err
			}
			passes[marker.Scenario], active = true, ""
		}
	}
	if len(sources) != len(sdkIntegrationScenarios) || len(passes) != len(sdkIntegrationScenarios) || summaries != 1 || active != "" {
		return fmt.Errorf("SDK integration evidence requires both executed source/pass pairs and one four-handler summary; got %d/%d/%d", len(sources), len(passes), summaries)
	}
	return nil
}

func validateSDKIntegrationObservations(events []sdkIntegrationGoEvent, name, sdkRevision string) error {
	scenario := sdkIntegrationScenarios[name]
	runs, passes, assertions := map[string]bool{}, map[string]bool{}, map[string]bool{}
	packagePassed := false
	for _, event := range events {
		if event.Package != "github.com/tetral-ai/tetral/integration" || event.Action == "fail" || event.Action == "skip" {
			return fmt.Errorf("SDK %s observations contain wrong package, failure or skip", name)
		}
		switch event.Action {
		case "run":
			runs[event.Test] = true
		case "pass":
			if event.Test == "" {
				packagePassed = true
			} else {
				passes[event.Test] = true
			}
		case "output":
			for _, line := range strings.Split(event.Output, "\n") {
				fields := strings.Fields(line)
				for index, field := range fields {
					assertion, ok := strings.CutPrefix(field, scenario.marker)
					if !ok || index+1 >= len(fields) || fields[index+1] != "passed=true" {
						continue
					}
					owner := scenario.test
					if name == "integration-oidc" {
						actor, _, _ := strings.Cut(assertion, "_")
						owner += "/" + actor
						if index+2 >= len(fields) || fields[index+2] != "sdk_pin="+sdkRevision {
							continue
						}
					}
					if event.Test == owner {
						assertions[assertion] = true
					}
				}
			}
		}
	}
	if !packagePassed {
		return fmt.Errorf("SDK %s observations lack executed package PASS", name)
	}
	for _, test := range scenario.tests {
		if !runs[test] || !passes[test] {
			return fmt.Errorf("SDK %s observations lack executed owning test %s", name, test)
		}
	}
	for _, assertion := range scenario.assertions {
		if !assertions[assertion] {
			return fmt.Errorf("SDK %s observations lack owning assertion %s", name, assertion)
		}
	}
	return nil
}

func TestSDKIntegrationCompatibilityOutputRequiresExecutedBoundProofs(t *testing.T) {
	engineRevision, sdkRevision := strings.Repeat("a", 40), strings.Repeat("b", 40)
	binding := sdkIntegrationSourceBinding{EngineRevision: engineRevision, SDKRevision: sdkRevision}
	encode := func(value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	var sections []string
	for _, name := range []string{"integration-preview", "integration-oidc"} {
		scenario := sdkIntegrationScenarios[name]
		lines := []string{encode(map[string]any{"compatibility_integration_source": binding, "scenario": name, "test": scenario.test})}
		appendEvent := func(action, test, output string) {
			lines = append(lines, encode(sdkIntegrationGoEvent{Action: action, Package: "github.com/tetral-ai/tetral/integration", Test: test, Output: output}))
		}
		for _, test := range scenario.tests {
			appendEvent("run", test, "")
		}
		for _, assertion := range scenario.assertions {
			owner, suffix := scenario.test, ""
			if name == "integration-oidc" {
				actor, _, _ := strings.Cut(assertion, "_")
				owner += "/" + actor
				suffix = " sdk_pin=" + sdkRevision
			}
			appendEvent("output", owner, scenario.marker+assertion+" passed=true"+suffix+"\n")
		}
		for _, test := range scenario.tests {
			appendEvent("pass", test, "")
		}
		appendEvent("pass", "", "")
		lines = append(lines, encode(map[string]any{"compatibility_integration_pass": binding, "scenario": name, "test": scenario.test, "assertions": scenario.assertions}))
		sections = append(sections, strings.Join(lines, "\n"))
	}
	summary := "Executed 4 integration compatibility handlers: 4 passed, 0 failed"
	valid := strings.Join(sections, "\n") + "\n" + summary
	lines := strings.Split(valid, "\n")
	source, pass := lines[0], lines[len(strings.Split(sections[0], "\n"))-1]
	cases := []struct {
		name, output string
		valid        bool
	}{
		{"registered proofs", valid, true},
		{"exit zero without evidence", "", false},
		{"empty registry", strings.ReplaceAll(valid, summary, "Executed 0 integration compatibility handlers: 0 passed, 0 failed"), false},
		{"failed handler", strings.ReplaceAll(valid, summary, "Executed 4 integration compatibility handlers: 3 passed, 1 failed"), false},
		{"missing source", strings.Replace(valid, source+"\n", "", 1), false},
		{"missing pass", strings.Replace(valid, pass+"\n", "", 1), false},
		{"missing summary", strings.TrimSuffix(valid, summary), false},
		{"duplicate source", source + "\n" + valid, false},
		{"duplicate pass", valid + "\n" + pass, false},
		{"duplicate summary", valid + "\n" + summary, false},
		{"wrong Engine source", strings.ReplaceAll(valid, engineRevision, strings.Repeat("c", 40)), false},
		{"wrong SDK source", strings.ReplaceAll(valid, sdkRevision, strings.Repeat("c", 40)), false},
		{"wrong test", strings.ReplaceAll(valid, sdkPreviewCompatibilityTest, "TestOther"), false},
		{"malformed marker", source[:len(source)-1] + "\n" + valid, false},
		{"missing OIDC scenario", sections[0] + "\n" + summary, false},
		{"missing preview scenario", sections[1] + "\n" + summary, false},
		{"unknown scenario", strings.ReplaceAll(valid, "integration-oidc", "integration-other"), false},
		{"two preview scenarios", sections[0] + "\n" + sections[0] + "\n" + summary, false},
		{"scenario test mismatch", strings.ReplaceAll(valid, `"test":"TestOIDCKeycloakSDK"`, `"test":"`+sdkPreviewCompatibilityTest+`"`), false},
		{"pass assertions absent", strings.ReplaceAll(valid, `"assertions":`, `"unrelated":`), false},
		{"pass assertion duplicated", strings.ReplaceAll(valid, `"human_typed_created_by_redacted_by_stable_identity","service_typed_created_by_redacted_by_stable_identity"`, `"human_typed_created_by_redacted_by_stable_identity","human_typed_created_by_redacted_by_stable_identity"`), false},
		{"names without execution", strings.ReplaceAll(valid, `"Action":"run"`, `"Action":"output"`), false},
		{"no executed test PASS", strings.ReplaceAll(valid, `"Action":"pass"`, `"Action":"output"`), false},
		{"no package PASS", strings.ReplaceAll(valid, `"Action":"pass","Package":"github.com/tetral-ai/tetral/integration","Test":""`, `"Action":"output","Package":"github.com/tetral-ai/tetral/integration","Test":""`), false},
		{"failed observation", strings.Replace(valid, `"Action":"run"`, `"Action":"fail"`, 1), false},
		{"skipped observation", strings.Replace(valid, `"Action":"run"`, `"Action":"skip"`, 1), false},
		{"wrong observation package", strings.ReplaceAll(valid, `"Package":"github.com/tetral-ai/tetral/integration"`, `"Package":"other"`), false},
		{"markers bound to root", strings.ReplaceAll(valid, `"Test":"TestOIDCKeycloakSDK/human"`, `"Test":"TestOIDCKeycloakSDK"`), false},
		{"markers bound to wrong identity", strings.ReplaceAll(valid, `"Test":"TestOIDCKeycloakSDK/human"`, `"Test":"TestOIDCKeycloakSDK/service"`), false},
		{"wrong observed SDK pin", strings.ReplaceAll(valid, "sdk_pin="+sdkRevision, "sdk_pin="+engineRevision), false},
		{"marker did not pass", strings.ReplaceAll(valid, "passed=true", "passed=false"), false},
		{"malformed Go observation", strings.Replace(valid, `{"Action":"run"`, `{"Action":`, 1), false},
	}
	for _, scenario := range sdkIntegrationScenarios {
		for _, assertion := range scenario.assertions {
			cases = append(cases, struct {
				name, output string
				valid        bool
			}{"missing observed " + assertion, strings.ReplaceAll(valid, scenario.marker+assertion, scenario.marker+"unrelated"), false})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSDKIntegrationCompatibilityOutput(tc.output, engineRevision, sdkRevision)
			if (err == nil) != tc.valid {
				t.Fatalf("evidence validity = %v; want %v: %v", err == nil, tc.valid, err)
			}
		})
	}
}
