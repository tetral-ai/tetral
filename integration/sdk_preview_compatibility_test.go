package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const sdkPreviewCompatibilityTest = "TestPostgreSQLPublicStreamingIdentity/session-options-primary-thread-and-private-content"

// Full and Affected launch the registered SDK proofs while their declared
// dependencies are still alive. The SDK's fixed Identity selector excludes this
// wrapper, and its oracle owns validation of the actual preview observations.
func TestForkSDKPreviewCompatibilityProofs(t *testing.T) {
	sdkRoot := strings.TrimSpace(os.Getenv("TETRAL_ENGINE_SDK_ROOT"))
	if sdkRoot == "" {
		t.Skip("set TETRAL_ENGINE_SDK_ROOT to run the registered SDK preview proofs")
	}
	engineRoot := repoRootFromBridgeTest(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	engineRevision := sdkPreviewSourceRevision(ctx, t, engineRoot)
	sdkRevision := sdkPreviewSourceRevision(ctx, t, sdkRoot)
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
	t.Logf("registered SDK preview compatibility output:\n%s", output)
	if err != nil {
		t.Fatalf("registered SDK preview compatibility failed: %v (context: %v)", err, ctx.Err())
	}
	if err := validateSDKPreviewCompatibilityOutput(string(output), engineRevision, sdkRevision); err != nil {
		t.Fatalf("registered SDK preview compatibility evidence: %v", err)
	}
}

func sdkPreviewSourceRevision(ctx context.Context, t *testing.T, root string) string {
	t.Helper()
	//nolint:gosec // Local source root; fixed read-only Git operation.
	output, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("resolve SDK compatibility source revision: %v", err)
	}
	return strings.TrimSpace(string(output))
}

type sdkPreviewSourceBinding struct {
	EngineRevision string `json:"engineRevision"`
	SDKRevision    string `json:"sdkRevision"`
}

// The pinned SDK launcher exposes source/pass JSON markers and a handler
// summary, rather than a report file. Exit zero also admits an empty registry,
// so require both registered handlers and their one cached scenario execution.
func validateSDKPreviewCompatibilityOutput(output, engineRevision, sdkRevision string) error {
	want := sdkPreviewSourceBinding{EngineRevision: engineRevision, SDKRevision: sdkRevision}
	sources, passes, summaries := 0, 0, 0
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Executed ") && strings.Contains(line, "integration compatibility handlers:") {
			if line != "Executed 2 integration compatibility handlers: 2 passed, 0 failed" {
				return fmt.Errorf("SDK integration handler summary does not prove both registered handlers passed: %s", line)
			}
			summaries++
		}
		if !strings.HasPrefix(line, "{") || (!strings.Contains(line, `"compatibility_integration_source"`) && !strings.Contains(line, `"compatibility_integration_pass"`)) {
			continue
		}
		var marker struct {
			Source *sdkPreviewSourceBinding `json:"compatibility_integration_source"`
			Pass   *sdkPreviewSourceBinding `json:"compatibility_integration_pass"`
			Test   string                   `json:"test"`
		}
		if err := json.Unmarshal([]byte(line), &marker); err != nil {
			return fmt.Errorf("decode SDK integration marker: %w", err)
		}
		if marker.Source != nil {
			if *marker.Source != want {
				return fmt.Errorf("SDK integration source binding differs from launched checkouts")
			}
			sources++
		}
		if marker.Pass != nil {
			if *marker.Pass != want || marker.Test != sdkPreviewCompatibilityTest {
				return fmt.Errorf("SDK integration pass marker differs from launched sources or required test")
			}
			passes++
		}
	}
	if sources != 1 || passes != 1 || summaries != 1 {
		return fmt.Errorf("SDK integration evidence requires one source marker, pass marker and two-handler summary; got %d/%d/%d", sources, passes, summaries)
	}
	return nil
}

func TestSDKPreviewCompatibilityOutputRequiresExecutedBoundProofs(t *testing.T) {
	engineRevision, sdkRevision := strings.Repeat("a", 40), strings.Repeat("b", 40)
	source := `{"compatibility_integration_source":{"engineRevision":"` + engineRevision + `","sdkRevision":"` + sdkRevision + `"}}`
	pass := `{"compatibility_integration_pass":{"engineRevision":"` + engineRevision + `","sdkRevision":"` + sdkRevision + `"},"test":"` + sdkPreviewCompatibilityTest + `"}`
	summary := "Executed 2 integration compatibility handlers: 2 passed, 0 failed"
	valid := source + "\n" + pass + "\n" + summary
	for _, tc := range []struct {
		name   string
		output string
		valid  bool
	}{
		{"registered proof", valid, true},
		{"exit zero without evidence", "", false},
		{"empty registry", source + "\n" + pass + "\nExecuted 0 integration compatibility handlers: 0 passed, 0 failed", false},
		{"failed handler", source + "\n" + pass + "\nExecuted 2 integration compatibility handlers: 1 passed, 1 failed", false},
		{"missing source", pass + "\n" + summary, false},
		{"missing pass", source + "\n" + summary, false},
		{"missing summary", source + "\n" + pass, false},
		{"duplicate source", valid + "\n" + source, false},
		{"duplicate pass", valid + "\n" + pass, false},
		{"duplicate summary", valid + "\n" + summary, false},
		{"wrong Engine source", strings.ReplaceAll(valid, engineRevision, strings.Repeat("c", 40)), false},
		{"wrong SDK source", strings.ReplaceAll(valid, sdkRevision, strings.Repeat("c", 40)), false},
		{"wrong test", strings.ReplaceAll(valid, sdkPreviewCompatibilityTest, "TestOther"), false},
		{"malformed marker", source[:len(source)-1] + "\n" + pass + "\n" + summary, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSDKPreviewCompatibilityOutput(tc.output, engineRevision, sdkRevision)
			if (err == nil) != tc.valid {
				t.Fatalf("evidence validity = %v; want %v: %v", err == nil, tc.valid, err)
			}
		})
	}
}
