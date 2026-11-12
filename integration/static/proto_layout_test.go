package static

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedTypeScriptProtocolUsesSafeNumericScalars(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	agentRuntimeBody, err := os.ReadFile(filepath.Join(root, "services/agent-runtime/packages/protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.ts")) //nolint:gosec // generated source path.
	if err != nil {
		t.Fatalf("read generated runtime pod protocol: %v", err)
	}
	text := string(agentRuntimeBody)
	for _, forbidden := range []string{"bindingGeneration: bigint", "bindingGeneration: string", "inputOrder: bigint", "inputOrder: string"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("generated Runtime Pod protocol contains forbidden scalar %q", forbidden)
		}
	}
	for _, required := range []string{"bindingGeneration: number", "inputOrder: number"} {
		if !strings.Contains(text, required) {
			t.Fatalf("generated Runtime Pod protocol missing TypeScript number scalar %q", required)
		}
	}
}

func TestRootBufWorkspaceListsOnlyServiceLocalProtoModules(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(root, "buf.yaml")) //nolint:gosec // repository-local config path.
	if err != nil {
		t.Fatalf("read root buf.yaml: %v", err)
	}
	text := string(body)
	for _, required := range []string{
		"path: services/bridge/proto",
		"path: services/agent-runtime/proto",
		"path: services/gateway/proto",
		"path: services/queue/proto",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("root buf.yaml missing service-local module %q", required)
		}
	}
}

func TestQueueServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/queue/buf.gen.yaml", "services/queue/proto") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("queue buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/queue/gen")
}

func TestRuntimePodServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/agent-runtime/buf.gen.yaml") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("runtime pod buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/agent-runtime/gen", "services/agent-runtime/packages/protocol/src/gen")
}

func TestGatewayServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/gateway/buf.gen.yaml") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gateway buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/gateway/gen", "services/gateway/packages/protocol/src/gen")
}

func TestBridgeServiceLocalBufGenerationFreshness(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	cmd := exec.Command("buf", "generate", "--template", "services/bridge/buf.gen.yaml") //nolint:gosec // repository-local generation command.
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bridge buf generate failed: %v\n%s", err, output)
	}
	assertGeneratedOutputClean(t, root, "services/bridge/gen", "services/agent-runtime/packages/protocol/src/gen-bridge", "services/gateway/packages/protocol/src/gen-bridge")
}

func assertGeneratedOutputClean(t *testing.T, root string, paths ...string) {
	t.Helper()
	diffArgs := append([]string{"diff", "--quiet", "--"}, paths...)
	diff := exec.Command("git", diffArgs...) //nolint:gosec // repository-local generated-output test pathspecs.
	diff.Dir = root
	if output, err := diff.CombinedOutput(); err != nil {
		t.Fatalf("generated proto output is dirty after buf generate: %v\n%s", err, output)
	}
	otherArgs := append([]string{"ls-files", "--others", "--exclude-standard", "--"}, paths...)
	others := exec.Command("git", otherArgs...) //nolint:gosec // repository-local generated-output test pathspecs.
	others.Dir = root
	output, err := others.CombinedOutput()
	if err != nil {
		t.Fatalf("list untracked generated proto output: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("generated proto output has untracked files after buf generate:\n%s", output)
	}
}
