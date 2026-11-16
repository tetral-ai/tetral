package static

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// buf lints and generates the modules its workspace file lists. A module
// is a service's own proto directory, and no proto file lies outside one.
func TestBufWorkspaceListsEveryProtoModule(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(root, "buf.yaml")) //nolint:gosec // repository-local config path.
	if err != nil {
		t.Fatalf("read root buf.yaml: %v", err)
	}
	var workspace struct {
		Modules []struct {
			Path string `yaml:"path"`
		} `yaml:"modules"`
	}
	if err := yaml.Unmarshal(body, &workspace); err != nil {
		t.Fatalf("decode root buf.yaml: %v", err)
	}
	var listed []string
	for _, module := range workspace.Modules {
		listed = append(listed, module.Path)
	}
	holding := map[string]bool{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(root, path, entry)
		}
		if filepath.Ext(path) != ".proto" {
			return nil
		}
		parts := strings.Split(finalArchitectureRel(t, root, path), "/")
		if len(parts) < 4 || parts[0] != "services" || parts[2] != "proto" {
			t.Errorf("%s lies outside a service's proto directory", strings.Join(parts, "/"))
			return nil
		}
		holding[strings.Join(parts[:3], "/")] = true
		return nil
	}); err != nil {
		t.Fatalf("scan proto sources: %v", err)
	}
	modules := slices.Sorted(maps.Keys(holding))
	slices.Sort(listed)
	if !slices.Equal(listed, modules) {
		t.Fatalf("buf.yaml lists %v; the proto directories are %v", listed, modules)
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
