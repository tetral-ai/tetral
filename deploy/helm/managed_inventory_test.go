package helm_test

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// This boundary executes the portable render/check commands against collected
// objects. It proves installed identities and retirement ownership independently
// of edge health; it never contacts a Kubernetes API.
func TestManagedInventoryBindsRenderedOverridesAndRetiresPreviousFleet(t *testing.T) {
	requireHelm(t)
	root := engineRoot(t)
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("Python 3 is required for managed inventory command fixtures")
	}
	directory := t.TempDir()
	run := func(name string, args ...string) ([]byte, error) {
		t.Helper()
		command := exec.Command(python, append([]string{filepath.Join(root, "deploy", "managed", name)}, args...)...)
		command.Dir = root
		return command.CombinedOutput()
	}
	defaults := filepath.Join(directory, "defaults")
	if output, err := run("render-inventory.py", "--profile", "standard-routed", "--public-edge", "--output-dir", defaults); err != nil {
		t.Fatalf("default render: %v: %s", err, output)
	}
	readExpected := func(directory string) []map[string]any {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(directory, "expected-resources.json"))
		if err != nil {
			t.Fatal(err)
		}
		var artifact struct {
			Resources []map[string]any `json:"expectedResources"`
		}
		if err := json.Unmarshal(body, &artifact); err != nil {
			t.Fatal(err)
		}
		return artifact.Resources
	}
	writeObserved := func(resources []map[string]any) string {
		t.Helper()
		body, err := json.Marshal(map[string]any{"items": resources})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, "observed.json")
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	// An empty expected directory selects the canonical default sets; flags
	// name the optional sets that the observed installation enables.
	check := func(profile, expected string, resources []map[string]any, complete bool, flags ...string) (map[string]any, error) {
		t.Helper()
		args := []string{"--profile", profile, "--observed", writeObserved(resources)}
		if expected != "" {
			args = append(args, "--expected-dir", expected)
		}
		args = append(args, flags...)
		if complete {
			args = append(args, "--require-complete")
		}
		output, err := run("validate-inventory.py", args...)
		result := map[string]any{}
		_ = json.Unmarshal(output, &result)
		return result, err
	}
	hasFailure := func(result map[string]any, reason string, identity ...string) bool {
		failures, _ := result["failures"].([]any)
		for _, item := range failures {
			failure, _ := item.(map[string]any)
			resource, _ := failure["resource"].([]any)
			if failure["reason"] != reason || len(resource) != len(identity) {
				continue
			}
			matches := true
			for index, part := range identity {
				matches = matches && resource[index] == part
			}
			if matches {
				return true
			}
		}
		return false
	}
	resources := readExpected(defaults)
	if result, err := check("standard-routed", "", resources, true, "--public-edge"); err != nil || result["coverage"] != "complete_resource_set" {
		t.Fatalf("complete canonical default inventory: %v %#v", err, result)
	}
	if result, err := check("standard-routed", defaults, resources, true); err != nil || result["coverage"] != "complete_resource_set" {
		t.Fatalf("complete bound default inventory: %v %#v", err, result)
	}
	// Chart-owned edge, routing and security objects carry no ownership label.
	// Their declared identity still makes a leftover one an owned survivor.
	t.Run("unlabeled-declared-survivor", func(t *testing.T) {
		result, err := check("standard-routed", "", resources, true)
		if err == nil || !hasFailure(result, "unexpected_owned_resource", "gateway.networking.k8s.io/v1", "Gateway", "tetral-system", "tetral-public-edge") {
			t.Fatalf("public edge Gateway left after disabling the edge accepted: %v %#v", err, result)
		}
	})
	t.Run("profile-switch-leftover", func(t *testing.T) {
		hardenedOnly := map[string]any{"apiVersion": "security.istio.io/v1", "kind": "PeerAuthentication", "metadata": map[string]any{"namespace": "tetral-system", "name": "tetral-queue-protected"}}
		observed := append(append([]map[string]any{}, resources...), hardenedOnly)
		result, err := check("standard-routed", defaults, observed, true)
		if err == nil || !hasFailure(result, "unexpected_owned_resource", "security.istio.io/v1", "PeerAuthentication", "tetral-system", "tetral-queue-protected") {
			t.Fatalf("hardened-only policy left after switching to standard accepted: %v %#v", err, result)
		}
	})
	t.Run("unrelated-unlabeled-preserved", func(t *testing.T) {
		unrelated := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "tetral-system", "name": "operator-notes"}}
		observed := append(append([]map[string]any{}, resources...), unrelated)
		if result, err := check("standard-routed", "", observed, true, "--public-edge"); err != nil {
			t.Fatalf("undeclared unlabeled resource rejected: %v %#v", err, result)
		}
	})
	t.Run("flags-with-expected-dir", func(t *testing.T) {
		for _, flag := range []string{"--public-edge", "--cilium", "--native-certificates", "--auth-issuer-network"} {
			output, err := run("validate-inventory.py", "--profile", "standard-routed", "--observed", writeObserved(resources), "--expected-dir", defaults, flag)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(output), "feature flags are fixed by the bound render") {
				t.Fatalf("%s with a bound render was not a usage error: %v %s", flag, err, output)
			}
		}
	})
	t.Run("previous-owned-fleet-survival", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(root, "deploy", "managed", "resource-inventory.json"))
		if err != nil {
			t.Fatal(err)
		}
		var inventory struct {
			Removals []map[string]any `json:"removals"`
		}
		if err := json.Unmarshal(body, &inventory); err != nil {
			t.Fatal(err)
		}
		if len(inventory.Removals) != 14 {
			t.Fatalf("previous fleet retirement count = %d; want 14", len(inventory.Removals))
		}
		for _, retired := range inventory.Removals {
			old := map[string]any{"apiVersion": retired["apiVersion"], "kind": retired["kind"], "metadata": map[string]any{"name": retired["name"], "namespace": retired["namespace"], "labels": retired["requiredLabels"]}}
			observed := append(append([]map[string]any{}, resources...), old)
			result, err := check("standard-routed", defaults, observed, true)
			encoded, _ := json.Marshal(result)
			if err == nil || !strings.Contains(string(encoded), "superseded_owned_resource_survives") {
				t.Fatalf("surviving %s/%s accepted: %v %s", retired["kind"], retired["name"], err, encoded)
			}
			old["metadata"].(map[string]any)["labels"] = map[string]any{"app.kubernetes.io/part-of": "another-installation"}
			if result, err := check("standard-routed", defaults, observed, true); err != nil {
				t.Fatalf("unrelated previous identity rejected: %v %#v", err, result)
			}
		}
	})
	t.Run("nondefault-native-certificates-and-optional-network", func(t *testing.T) {
		values := filepath.Join(directory, "overrides.yaml")
		if err := os.WriteFile(values, []byte("transport:\n  runtimeServerName: runtime.example.test\nedge:\n  clientLeafSecret: selected-edge-client\n  serverLeafSecrets:\n    auth: selected-auth-server\n"), 0600); err != nil {
			t.Fatal(err)
		}
		overridden := filepath.Join(directory, "overridden")
		output, err := run("render-inventory.py", "--profile", "hardened", "--public-edge", "--cilium", "--git-fqdn-policy", "--native-certificates", "--values", values, "--output-dir", overridden)
		if err != nil {
			t.Fatalf("overridden render: %v %s", err, output)
		}
		selected := readExpected(overridden)
		var foundClient, foundAuth, foundGit bool
		for _, resource := range selected {
			if resource["kind"] == "Certificate" && resource["name"] == "selected-edge-client" {
				foundClient = true
			}
			if resource["kind"] == "Certificate" && resource["name"] == "selected-auth-server" {
				foundAuth = true
			}
			if resource["kind"] == "CiliumNetworkPolicy" && resource["name"] == "git-proxy-github-egress" {
				foundGit = true
			}
		}
		if !foundClient || !foundAuth || !foundGit {
			t.Fatalf("actual overridden resource identities missing: client=%t auth=%t git=%t", foundClient, foundAuth, foundGit)
		}
		rendered := readManifestObjects(t, []string{filepath.Join(overridden, "rendered-resources.yaml")})
		runtime, ok := objectByKey(rendered, "cert-manager.io/v1|Certificate|tetral-agent-runtime|tetral-runtime-direct-tls")
		if !ok {
			t.Fatal("native Runtime certificate missing")
		}
		requireManifestPathString(t, runtime, "runtime.example.test", "spec", "dnsNames", 0)
		if result, err := check("hardened", overridden, selected, true); err != nil || result["expectedSource"] != "bound_installation_render" {
			t.Fatalf("valid override rejected: %v %#v", err, result)
		}
		if result, err := check("hardened", overridden, selected[1:], true); err == nil {
			t.Fatalf("missing selected object accepted: %#v", result)
		}
		result, err := check("hardened", overridden, selected[1:], false)
		if err != nil || result["coverage"] != "partial_no_completeness_claim" {
			t.Fatalf("partial evidence mislabeled: %v %#v", err, result)
		}
		path := filepath.Join(overridden, "rendered-resources.yaml")
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.WriteString("\n# changed after binding\n")
		_ = file.Close()
		if result, err := check("hardened", overridden, selected, true); err == nil {
			t.Fatalf("changed expected bytes accepted: %#v", result)
		}
	})
}
