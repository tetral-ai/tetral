package testinfra

import (
	"context"
	"debug/buildinfo"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEdgeDependenciesRemainDistinctFromMeshAndBindHelperSource(t *testing.T) {
	root := repositoryRootForTest(t)
	edge, err := PinnedEdgeEnvoyImage(root)
	if err != nil {
		t.Fatal(err)
	}
	mesh, err := PinnedEnvoyImage(root)
	if err != nil {
		t.Fatal(err)
	}
	if edge == mesh || !strings.Contains(edge, "distroless-v1.39.2@sha256:") || !strings.Contains(mesh, "proxyv2:1.31.1@sha256:") {
		t.Fatalf("edge and mesh identities are not distinct selected runtimes")
	}
	directory := t.TempDir()
	for name, body := range map[string]string{"main.go": "package main\nfunc main() {}\n", "go.mod": "module fixture\n", "go.sum": "fixture dependencies\n"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	before, err := helperSourceDigest(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "ignored.log"), []byte("process output"), 0600); err != nil {
		t.Fatal(err)
	}
	stable, err := helperSourceDigest(directory)
	if err != nil || stable != before {
		t.Fatal("non-source output changed helper source identity")
	}
	if err := os.WriteFile(filepath.Join(directory, "go.sum"), []byte("changed graph\n"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := helperSourceDigest(directory)
	if err != nil || before == after {
		t.Fatal("module graph change did not change helper source identity")
	}
	if err := os.Symlink("main.go", filepath.Join(directory, "replacement.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := helperSourceDigest(directory); err == nil {
		t.Fatal("symlink source identity accepted")
	}
}

func TestSeparateTranslatorSecuritySelection(t *testing.T) {
	for _, path := range []string{"integration/envoy-gateway-secret-helper/main.go", "integration/envoy-gateway-secret-helper/go.mod", "integration/envoy-gateway-secret-helper/go.sum", "deploy/dependencies.lock.json", "scripts/check-envoy-secret-helper.py", ".github/workflows/engine-vulncheck.yml"} {
		plan := Plan{Revision: Revision{ChangedPaths: []string{path}}}
		if !helperVulnerabilityAuditNeeded(plan, DependencyAuditChanged) {
			t.Errorf("nested module security omitted for %s", path)
		}
	}
	if helperVulnerabilityAuditNeeded(Plan{Revision: Revision{ChangedPaths: []string{"services/api/README.md"}}}, DependencyAuditChanged) {
		t.Fatal("unrelated documentation selected an online graph scan")
	}
	if !helperVulnerabilityAuditNeeded(Plan{}, DependencyAuditAlways) || helperVulnerabilityAuditNeeded(Plan{}, DependencyAuditNever) {
		t.Fatal("nested graph scan ignored explicit audit policy")
	}
}

func TestAffectedTranslatorChangesExecuteNestedGraphSecurity(t *testing.T) {
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"integration/envoy-gateway-secret-helper/main.go", "integration/envoy-gateway-secret-helper/go.mod", "scripts/check-envoy-secret-helper.py"} {
		t.Run(path, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{path}}
			selections, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := SelectPlan(Plan{Profile: ProfileAffected, Revision: revision, Selections: selections}, []string{"security"}, 0, 1)
			if err != nil {
				t.Fatal("actual Affected selection omitted the nested helper's security owner")
			}
			if len(plan.Selections) != 1 {
				t.Fatal("nested helper needs one security producer")
			}
			commands, err := commandsForSelection(plan, plan.Selections[0], root, t.TempDir(), DependencyAuditChanged)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, command := range commands {
				if slices.Equal(command.Arguments, []string{"python3", "scripts/check-envoy-secret-helper.py"}) {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("Affected security selects actual nested module/symbol scanner %d times", count)
			}
		})
	}
}

// A change confined to the nested SecretType helper module selects deployment
// evidence, whose commands run the helper's own tests with the locked toolchain.
func TestAffectedSecretHelperChangesRunNestedModuleTests(t *testing.T) {
	root := repositoryRootForTest(t)
	lock, err := loadEdgeDependencyLock(root)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"integration/envoy-gateway-secret-helper/main_test.go", "integration/envoy-gateway-secret-helper/go.mod"} {
		t.Run(path, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{path}}
			selections, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("helper change fell back to Full: %s", revision.FullFallbackCause)
			}
			plan, err := SelectPlan(Plan{Profile: ProfileAffected, Revision: revision, Selections: selections}, []string{"deployment"}, 0, 1)
			if err != nil || len(plan.Selections) != 1 {
				t.Fatalf("helper change omitted deployment evidence: %v", err)
			}
			commands, err := commandsForSelection(plan, plan.Selections[0], root, t.TempDir(), DependencyAuditChanged)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, command := range commands {
				if command.WorkingDir == "integration/envoy-gateway-secret-helper" && slices.Equal(command.Arguments, []string{"go", "test", "-mod=readonly", "-count=1", "./..."}) &&
					slices.Equal(command.Environment, []string{"GOTOOLCHAIN=" + lock.Gateway.SecretHelper.Toolchain}) {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("Affected deployment evidence runs the nested helper tests %d times", count)
			}
		})
	}
}

func TestHostNetworkFixtureRejectsConflictingNetworkControls(t *testing.T) {
	for _, spec := range []ContainerSpec{
		{HostNetwork: true, Network: "bridge"}, {HostNetwork: true, NetworkContainer: &DockerContainer{}},
		{HostNetwork: true, Aliases: []string{"backend"}}, {HostNetwork: true, Ports: []int{8080}},
		{HostNetwork: true, HostPorts: map[int]int{8080: 8080}},
	} {
		if err := validateHostNetwork(spec); err == nil {
			t.Fatal("host network accepted conflicting network controls")
		}
	}
	if err := validateHostNetwork(ContainerSpec{}); err != nil {
		t.Fatal(err)
	}
}

func TestHostNetworkFixtureAliasesRemainContainerOnlyLoopback(t *testing.T) {
	for _, spec := range []ContainerSpec{
		{ExtraHosts: map[string]string{"envoy-gateway": "127.0.0.1"}},
		{HostNetwork: true, ExtraHosts: map[string]string{"envoy-gateway": "0.0.0.0"}},
		{HostNetwork: true, ExtraHosts: map[string]string{"envoy-gateway": "192.0.2.1"}},
		{HostNetwork: true, ExtraHosts: map[string]string{"envoy-gateway": "host-gateway"}},
		{HostNetwork: true, ExtraHosts: map[string]string{"bad:hostname": "127.0.0.1"}},
		{HostNetwork: true, ExtraHosts: map[string]string{"-bad": "127.0.0.1"}},
		{HostNetwork: true, ExtraHosts: map[string]string{"bad\nname": "127.0.0.1"}},
	} {
		if err := validateHostNetwork(spec); err == nil {
			t.Fatal("fixture admitted unconfined or malformed alias")
		}
	}
	if err := validateHostNetwork(ContainerSpec{HostNetwork: true, ExtraHosts: map[string]string{"envoy-gateway": "127.0.0.1"}}); err != nil {
		t.Fatal(err)
	}
}

func TestPinnedEdgeEnvoyRuntimeAndTranslator(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	executable := os.Getenv(EnvTestEGCTL)
	helper := os.Getenv(EnvTestEGCTLSecretTranslator)
	if executable == "" || helper == "" {
		t.Fatal("native matching CLI/upstream helper prerequisites are absent")
	}
	// A local prerequisite must remain usable on CI hosts without cluster access.
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "absent-kubeconfig"))
	output, err := exec.CommandContext(ctx, executable, "version", "--remote=false").CombinedOutput() //nolint:gosec // Native checksum-verified executable and fixed local version arguments.
	if err != nil || strings.TrimSpace(string(output)) != "v1.9.2" {
		t.Fatal("actual matching CLI version differs from selected release")
	}
	info, err := buildinfo.ReadFile(helper)
	if err != nil || info.GoVersion != "go1.26.9" {
		t.Fatal("actual helper uses the wrong isolated toolchain")
	}
	found := false
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/envoyproxy/gateway" && dependency.Version == "v1.9.2" && dependency.Replace == nil {
			found = true
		}
	}
	if !found {
		t.Fatal("actual helper lacks the exact upstream translator module")
	}
	resources, err := NewDockerResources(ctx, "edge-prerequisite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := resources.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	image, err := PinnedEdgeEnvoyImage(repositoryRootForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	container, err := resources.Run(ctx, ContainerSpec{Image: image, Entrypoint: "/usr/local/bin/envoy", Command: []string{"--version"}})
	if err != nil {
		t.Fatal(err)
	}
	if code, err := container.Wait(ctx); err != nil || code != 0 {
		t.Fatalf("actual edge runtime version command exit=%d error=%v", code, err)
	}
	version, err := container.Logs(ctx)
	if err != nil || !strings.Contains(version, "1.39.2/") {
		t.Fatal("actual edge Envoy version differs from pinned compatible patch")
	}
	t.Logf("actual edge runtime=%s image=%s cli=v1.9.2 helper=%s", strings.TrimSpace(version), container.ImageID, info.GoVersion)
}
