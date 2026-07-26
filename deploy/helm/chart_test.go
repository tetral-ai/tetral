package helm_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type manifestObject struct {
	key   string
	value map[string]any
}

func TestHelmChartDefaultAndTogglesMatchCanonicalManifests(t *testing.T) {
	helm := requireHelm(t)
	engineRoot := engineRoot(t)
	chart := filepath.Join(engineRoot, "deploy", "helm", "tetral")

	canonical := readManifestObjects(t, canonicalManifestPaths(t, engineRoot))
	if len(canonical) != 62 {
		t.Fatalf("canonical object count = %d; want 62", len(canonical))
	}

	rendered := renderChart(t, helm, chart)
	requireObjectSetsEqual(t, rendered, canonical)

	withNamespaces := renderChart(t, helm, chart, "namespaces.create=true")
	requireToggleShape(t, withNamespaces, rendered, []string{
		"v1|Namespace||tetral-agent-runtime",
		"v1|Namespace||tetral-system",
	}, nil)
	for _, name := range []string{"tetral-agent-runtime", "tetral-system"} {
		key := "v1|Namespace||" + name
		got, ok := objectByKey(withNamespaces, key)
		if !ok {
			t.Fatalf("namespace render missing %s", key)
		}
		want := map[string]any{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]any{
				"name": name,
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("namespace object %s differs from the label-free shape: %#v", key, got)
		}
	}

	withoutCilium := renderChart(t, helm, chart, "cilium.enabled=false")
	requireCiliumToggleShape(t, withoutCilium, rendered, []string{
		"cilium.io/v2|CiliumNetworkPolicy|tetral-agent-runtime|agent-runtime-apiserver-egress",
		"cilium.io/v2|CiliumNetworkPolicy|tetral-system|bridge-apiserver-egress",
		"cilium.io/v2|CiliumNetworkPolicy|tetral-system|gateway-apiserver-egress",
		"cilium.io/v2|CiliumNetworkPolicy|tetral-system|git-proxy-github-egress",
	})

	withEdge := renderChart(t, helm, chart, "edge.enabled=true")
	edge := readManifestObjects(t, []string{
		filepath.Join(engineRoot, "deploy", "kubernetes", "edge-gateway", "ingress-nginx.yaml"),
	})
	var edgeKeys []string
	for _, object := range edge {
		edgeKeys = append(edgeKeys, object.key)
	}
	requireToggleShape(t, withEdge, rendered, edgeKeys, nil)
	for _, object := range edge {
		got, ok := objectByKey(withEdge, object.key)
		if !ok {
			t.Fatalf("edge render missing %s", object.key)
		}
		if !reflect.DeepEqual(got, object.value) {
			t.Fatalf("edge object %s differs from canonical:\nrendered: %#v\ncanonical: %#v", object.key, got, object.value)
		}
	}
}

func TestHelmChartCiliumAPIServerPoliciesAreExact(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	rendered := uniqueObjects(t, renderChart(t, helm, chart))

	for _, policy := range []struct {
		name      string
		namespace string
	}{
		{name: "agent-runtime", namespace: "tetral-agent-runtime"},
		{name: "bridge", namespace: "tetral-system"},
		{name: "gateway", namespace: "tetral-system"},
	} {
		key := "cilium.io/v2|CiliumNetworkPolicy|" + policy.namespace + "|" + policy.name + "-apiserver-egress"
		got, ok := rendered[key]
		if !ok {
			t.Fatalf("default render missing %s", key)
		}
		want := map[string]any{
			"apiVersion": "cilium.io/v2",
			"kind":       "CiliumNetworkPolicy",
			"metadata": map[string]any{
				"name":      policy.name + "-apiserver-egress",
				"namespace": policy.namespace,
				"labels": map[string]any{
					"app.kubernetes.io/name":    policy.name,
					"app.kubernetes.io/part-of": "tetral",
				},
			},
			"spec": map[string]any{
				"endpointSelector": map[string]any{
					"matchLabels": map[string]any{
						"app.kubernetes.io/name": policy.name,
					},
				},
				"egress": []any{
					map[string]any{
						"toEntities": []any{"kube-apiserver"},
						"toPorts": []any{
							map[string]any{
								"ports": []any{
									map[string]any{"port": "443", "protocol": "TCP"},
									map[string]any{"port": "6443", "protocol": "TCP"},
								},
							},
						},
					},
				},
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s differs from the closed apiserver policy shape:\ngot:  %#v\nwant: %#v", key, got, want)
		}
	}
}

func TestHelmChartGitProxyCiliumDNSBranchesAreL7(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	rendered := uniqueObjects(t, renderChart(t, helm, chart))
	key := "cilium.io/v2|CiliumNetworkPolicy|tetral-system|git-proxy-github-egress"
	policy, ok := rendered[key]
	if !ok {
		t.Fatalf("default render missing %s", key)
	}

	for branchIndex, branchValue := range requireManifestList(t, policy, "spec", "egress") {
		branch, ok := branchValue.(map[string]any)
		if !ok {
			t.Fatalf("%s egress[%d] has type %T; want map", key, branchIndex, branchValue)
		}
		toPorts, _ := branch["toPorts"].([]any)
		for toPortsIndex, toPortsValue := range toPorts {
			toPortsEntry, ok := toPortsValue.(map[string]any)
			if !ok {
				t.Fatalf("%s egress[%d].toPorts[%d] has type %T; want map", key, branchIndex, toPortsIndex, toPortsValue)
			}
			hasDNSPort := false
			for _, portValue := range requireManifestList(t, toPortsEntry, "ports") {
				port, ok := portValue.(map[string]any)
				if !ok {
					t.Fatalf("%s egress[%d].toPorts[%d] port has type %T; want map", key, branchIndex, toPortsIndex, portValue)
				}
				if port["port"] == "53" {
					hasDNSPort = true
				}
			}
			if !hasDNSPort {
				continue
			}
			rules, ok := toPortsEntry["rules"].(map[string]any)
			if !ok {
				t.Fatalf("%s egress[%d].toPorts[%d] carries DNS port 53 without rules.dns", key, branchIndex, toPortsIndex)
			}
			dns, ok := rules["dns"].([]any)
			if !ok || len(dns) == 0 {
				t.Fatalf("%s egress[%d].toPorts[%d] carries DNS port 53 without non-empty rules.dns", key, branchIndex, toPortsIndex)
			}
		}
	}
}

// These six peer lists and two port values are cluster properties, not Tetral
// properties: a
// managed database, an ingress that is not a pod, a service CIDR other than
// the kubeadm default, a DNS topology other than kube-system/kube-dns, and an
// egress posture narrower than the public internet all have to be expressible
// without editing the templates. Rendering the defaults proves nothing about
// that, so this test overrides every peer list and port value and asserts each override
// reaches exactly the policies that consume it - and that no policy keeps a
// default peer behind.
func TestHelmChartNetworkPeersAreOverridable(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")

	rendered := renderChart(
		t,
		helm,
		chart,
		"network.apiServerPeers[0].ipBlock.cidr=172.20.0.1/32",
		"network.databasePeers[0].ipBlock.cidr=10.7.0.0/16",
		"network.publicIngressPeers[0].namespaceSelector.matchLabels.tetral\\.ai/network-role=edge-override-fixture",
		"network.dnsPeers[0].namespaceSelector.matchLabels.kubernetes\\.io/metadata\\.name=dns-override-fixture",
		"network.externalEgressPeers[0].ipBlock.cidr=203.0.113.0/24",
		"network.ciliumDNSEndpointSelectors[0].matchLabels.k8s\\:k8s-app=cnp-selector-fixture",
		"network.databasePort=25060",
		"network.externalEgressPorts[0]=8443",
	)

	defaults := map[string]int{
		"10.96.0.1/32":          0,
		"tetral-postgres":       0,
		"public-ingress":        0,
		"k8s-app: kube-dns":     0,
		"k8s:k8s-app: kube-dns": 0,
		"0.0.0.0/0":             0,
		"port: 5432":            0,
		"port: 443":             0,
	}
	overrides := map[string]int{
		"172.20.0.1/32":         0,
		"10.7.0.0/16":           0,
		"edge-override-fixture": 0,
		"dns-override-fixture":  0,
		"203.0.113.0/24":        0,
		"cnp-selector-fixture":  0,
		"port: 25060":           0,
		"port: 8443":            0,
	}
	// Both policy kinds are scanned, and occurrences are counted rather than
	// policies: a per-policy tally cannot see a second hardcoded peer of the
	// same class inside a policy that already carries an overridden one, and
	// skipping CiliumNetworkPolicy is how the Cilium DNS selector stayed a
	// literal while every plain NetworkPolicy was parameterized.
	for _, object := range rendered {
		kind, _ := object.value["kind"].(string)
		if kind != "NetworkPolicy" && kind != "CiliumNetworkPolicy" {
			continue
		}
		encoded, err := yaml.Marshal(object.value)
		if err != nil {
			t.Fatalf("marshal %s: %v", object.key, err)
		}
		text := string(encoded)
		for needle := range defaults {
			defaults[needle] += strings.Count(text, needle)
		}
		for needle := range overrides {
			overrides[needle] += strings.Count(text, needle)
		}
	}

	for needle, count := range defaults {
		want := 0
		if needle == "port: 443" {
			want = 3
		}
		if count != want {
			t.Fatalf("default peer %q survived the override in %d policies", needle, count)
		}
	}
	want := map[string]int{
		"172.20.0.1/32":         3,
		"10.7.0.0/16":           9,
		"edge-override-fixture": 4,
		"dns-override-fixture":  9,
		"203.0.113.0/24":        4,
		"cnp-selector-fixture":  1,
		"port: 25060":           9,
		"port: 8443":            4,
	}
	for needle, expected := range want {
		if overrides[needle] != expected {
			t.Fatalf("override %q reached %d policies; want %d", needle, overrides[needle], expected)
		}
	}
}

func TestHelmChartRejectsEmptyNetworkAllowListsAndPorts(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")

	for _, test := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "apiServerPeers-empty", key: "apiServerPeers", value: "[]"},
		{name: "apiServerPeers-null-element", key: "apiServerPeers", value: "[null]"},
		{name: "apiServerPeers-empty-object", key: "apiServerPeers", value: "[{}]"},
		{name: "databasePeers-empty", key: "databasePeers", value: "[]"},
		{name: "databasePeers-null-element", key: "databasePeers", value: "[null]"},
		{name: "databasePeers-empty-object", key: "databasePeers", value: "[{}]"},
		{name: "publicIngressPeers-empty", key: "publicIngressPeers", value: "[]"},
		{name: "publicIngressPeers-null-element", key: "publicIngressPeers", value: "[null]"},
		{name: "publicIngressPeers-empty-object", key: "publicIngressPeers", value: "[{}]"},
		{name: "dnsPeers-empty", key: "dnsPeers", value: "[]"},
		{name: "dnsPeers-null-element", key: "dnsPeers", value: "[null]"},
		{name: "dnsPeers-empty-object", key: "dnsPeers", value: "[{}]"},
		{name: "externalEgressPeers-empty", key: "externalEgressPeers", value: "[]"},
		{name: "externalEgressPeers-null-element", key: "externalEgressPeers", value: "[null]"},
		{name: "externalEgressPeers-empty-object", key: "externalEgressPeers", value: "[{}]"},
		{name: "ciliumDNSEndpointSelectors-empty", key: "ciliumDNSEndpointSelectors", value: "[]"},
		{name: "ciliumDNSEndpointSelectors-null-element", key: "ciliumDNSEndpointSelectors", value: "[null]"},
		{name: "ciliumDNSEndpointSelectors-empty-object", key: "ciliumDNSEndpointSelectors", value: "[{}]"},
		{name: "databasePort-null", key: "databasePort", value: "null"},
		{name: "databasePort-empty-object", key: "databasePort", value: "{}"},
		{name: "databasePort-list", key: "databasePort", value: "[5432]"},
		{name: "externalEgressPorts-empty", key: "externalEgressPorts", value: "[]"},
		{name: "externalEgressPorts-null-element", key: "externalEgressPorts", value: "[null]"},
		{name: "externalEgressPorts-empty-object", key: "externalEgressPorts", value: "[{}]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := renderChartFailure(t, helm, chart, "network."+test.key+"="+test.value)
			if err == nil {
				t.Fatalf("invalid network.%s=%s rendered successfully", test.key, test.value)
			}
			if !strings.Contains(output, "network."+test.key) {
				t.Fatalf("invalid network.%s=%s failure does not name the key:\n%s", test.key, test.value, output)
			}
		})
	}
}

func TestHelmChartStringValuesRemainStringsForBooleanShapedOverrides(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	rendered := uniqueObjects(t, renderChart(
		t,
		helm,
		chart,
		"bootstrapWorkspaceID=true",
		"secrets.apiSecrets=true",
		"edge.enabled=true",
		"gitProxyHost=true",
		"edge.tlsSecretName=true",
		"image.registry=registry.example/tetral",
		"image.tag=false",
	))

	authConfig := rendered["v1|ConfigMap|tetral-system|auth-config"]
	requireManifestPathString(t, authConfig, "true", "data", "ENGINE_BOOTSTRAP_WORKSPACE_ID")

	foundOverriddenSecret := false
	for _, object := range rendered {
		for _, value := range nestedMapFieldValues(object, "secretKeyRef", "name") {
			name, ok := value.(string)
			if !ok {
				t.Fatalf("secretKeyRef.name has type %T; want string", value)
			}
			if name == "true" {
				foundOverriddenSecret = true
			}
		}
	}
	if !foundOverriddenSecret {
		t.Fatal("boolean-shaped secret override was not rendered as the string \"true\"")
	}

	wantImages := map[string]bool{
		"registry.example/tetral/agent-runtime:false": false,
		"registry.example/tetral/gateway:false":       false,
		"registry.example/tetral/tetral:false":        false,
	}
	for _, object := range rendered {
		for _, value := range nestedFieldValues(object, "image") {
			image, ok := value.(string)
			if !ok {
				t.Fatalf("container image has type %T; want string", value)
			}
			if _, expected := wantImages[image]; !expected {
				t.Fatalf("container image = %q; want one of %v", image, wantImages)
			}
			wantImages[image] = true
		}
	}
	for image, found := range wantImages {
		if !found {
			t.Errorf("boolean-shaped image tag did not reach %s", image)
		}
	}
	apiConfig := rendered["v1|ConfigMap|tetral-system|api-config"]
	requireManifestPathString(
		t,
		apiConfig,
		"registry.example/tetral/sandbox:false",
		"data",
		"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF",
	)

	gitProxy := rendered["networking.k8s.io/v1|Ingress|tetral-system|git-proxy"]
	requireManifestPathString(t, gitProxy, "true", "spec", "tls", 0, "hosts", 0)
	requireManifestPathString(t, gitProxy, "true", "spec", "tls", 0, "secretName")
	requireManifestPathString(t, gitProxy, "true", "spec", "rules", 0, "host")
}

func TestHelmChartRenderedManifestsPassInvariantSuites(t *testing.T) {
	helm := requireHelm(t)
	root := engineRoot(t)
	chart := filepath.Join(root, "deploy", "helm", "tetral")

	defaultOutput := renderChartToDirectory(t, helm, chart)
	renderedRoot := filepath.Join(t.TempDir(), "manifests")
	if err := os.MkdirAll(renderedRoot, 0o755); err != nil {
		t.Fatalf("create rendered manifest root: %v", err)
	}
	for _, name := range []string{
		"agent-runtime.yaml",
		"api.yaml",
		"auth.yaml",
		"bridge-rbac.yaml",
		"bridge.yaml",
		"cleanup.yaml",
		"event-stream.yaml",
		"gateway.yaml",
		"git-proxy.yaml",
		"internal-grpc-tokenreview-rbac.yaml",
		"queue.yaml",
		"sandbox.yaml",
	} {
		copyTestFile(t, filepath.Join(defaultOutput, name), filepath.Join(renderedRoot, name))
	}
	// namespaces.create=true is intentionally covered only by the object-diff
	// test: Namespace is outside the invariant suite's closed kind and file set.
	skip := exactTestPattern([]string{
		"TestKubernetesEdgeGatewayIngressNginxExternalAuthBoundary",
		"TestKubernetesManifestQueueIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestTetralAPIIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestTetralAuthIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestEventStreamIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestGatewayServiceIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestGitProxyIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestAgentRuntimePodIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestBridgeServiceIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestBridgeRBACIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestSandboxIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestCleanupIsComposedFromServiceLocalManifests",
		"TestKubernetesManifestInternalGRPCTokenReviewRBACIsComposedFromServiceLocalManifests",
	})
	runGoTest(t, root, []string{
		"TETRAL_KUBERNETES_MANIFEST_ROOT=" + renderedRoot,
	}, "./deploy/kubernetes", "-skip", skip)

	edgeOutput := renderChartToDirectory(t, helm, chart, "edge.enabled=true")
	edgeRoot := t.TempDir()
	edgeDir := filepath.Join(edgeRoot, "edge-gateway")
	if err := os.MkdirAll(edgeDir, 0o755); err != nil {
		t.Fatalf("create edge manifest root: %v", err)
	}
	copyTestFile(t, filepath.Join(edgeOutput, "edge.yaml"), filepath.Join(edgeDir, "ingress-nginx.yaml"))
	runGoTest(t, root, []string{
		"TETRAL_KUBERNETES_MANIFEST_ROOT=" + edgeRoot,
	}, "./deploy/kubernetes", "-run", "^TestKubernetesEdgeGatewayIngressNginxExternalAuthBoundary$")

	runGoTest(t, root, []string{
		"TETRAL_SCHEMA_OWNERSHIP_TOP_MANIFESTS_ROOT=" + renderedRoot,
	}, "./integration/static", "-run", "^TestSchemaOwnershipManifestDiscoveryClassifiesEveryDatabaseContainer$")
}

func requireHelm(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("helm")
	if err == nil {
		return path
	}
	if os.Getenv("CI") != "" {
		t.Fatal("helm is required in CI")
	}
	t.Skip("helm is not installed")
	return ""
}

func engineRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve engine root: %v", err)
	}
	return root
}

func canonicalManifestPaths(t *testing.T, root string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "deploy", "kubernetes", "*.yaml"))
	if err != nil {
		t.Fatalf("glob canonical manifests: %v", err)
	}
	sort.Strings(paths)
	return paths
}

func renderChart(t *testing.T, helm string, chart string, values ...string) []manifestObject {
	t.Helper()
	args := []string{"template", "tetral", chart}
	for _, value := range values {
		args = append(args, "--set", value)
	}
	command := exec.Command(helm, args...) //nolint:gosec // Helm path and chart arguments are test-owned.
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", values, err, output)
	}
	return decodeManifestObjects(t, bytes.NewReader(output), "helm template")
}

func renderChartFailure(t *testing.T, helm string, chart string, value string) (string, error) {
	t.Helper()
	command := exec.Command(helm, "template", "tetral", chart, "--set-json", value) //nolint:gosec // Helm path and chart arguments are test-owned.
	output, err := command.CombinedOutput()
	return string(output), err
}

func renderChartToDirectory(t *testing.T, helm string, chart string, values ...string) string {
	t.Helper()
	output := t.TempDir()
	args := []string{"template", "tetral", chart, "--output-dir", output}
	for _, value := range values {
		args = append(args, "--set", value)
	}
	command := exec.Command(helm, args...) //nolint:gosec // Helm path and chart arguments are test-owned.
	if result, err := command.CombinedOutput(); err != nil {
		t.Fatalf("helm template --output-dir %v: %v\n%s", values, err, result)
	}
	return filepath.Join(output, "tetral", "templates")
}

func readManifestObjects(t *testing.T, paths []string) []manifestObject {
	t.Helper()
	var objects []manifestObject
	for _, path := range paths {
		source, err := os.Open(path) //nolint:gosec // Paths are repository-local test inputs.
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		objects = append(objects, decodeManifestObjects(t, source, path)...)
		if err := source.Close(); err != nil {
			t.Fatalf("close %s: %v", path, err)
		}
	}
	return objects
}

func decodeManifestObjects(t *testing.T, source io.Reader, label string) []manifestObject {
	t.Helper()
	decoder := yaml.NewDecoder(source)
	var objects []manifestObject
	for index := 0; ; index++ {
		var value map[string]any
		err := decoder.Decode(&value)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode %s object %d: %v", label, index+1, err)
		}
		if value == nil {
			continue
		}
		objects = append(objects, manifestObject{
			key:   manifestKey(t, label, value),
			value: value,
		})
	}
	return objects
}

func manifestKey(t *testing.T, label string, value map[string]any) string {
	t.Helper()
	apiVersion, ok := value["apiVersion"].(string)
	if !ok || apiVersion == "" {
		t.Fatalf("%s object has no apiVersion: %#v", label, value)
	}
	kind, ok := value["kind"].(string)
	if !ok || kind == "" {
		t.Fatalf("%s object has no kind: %#v", label, value)
	}
	metadata, ok := value["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("%s object has no metadata: %#v", label, value)
	}
	name, ok := metadata["name"].(string)
	if !ok || name == "" {
		t.Fatalf("%s object has no metadata.name: %#v", label, value)
	}
	namespace, _ := metadata["namespace"].(string)
	return strings.Join([]string{apiVersion, kind, namespace, name}, "|")
}

func requireObjectSetsEqual(t *testing.T, got []manifestObject, want []manifestObject) {
	t.Helper()
	gotByKey := uniqueObjects(t, got)
	wantByKey := uniqueObjects(t, want)
	if len(gotByKey) != len(wantByKey) {
		t.Fatalf("rendered object count = %d; want %d", len(gotByKey), len(wantByKey))
	}
	for key, wantValue := range wantByKey {
		gotValue, ok := gotByKey[key]
		if !ok {
			t.Errorf("rendered set missing %s", key)
			continue
		}
		if !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("rendered object %s differs from canonical:\nrendered: %#v\ncanonical: %#v", key, gotValue, wantValue)
		}
	}
	for key := range gotByKey {
		if _, ok := wantByKey[key]; !ok {
			t.Errorf("rendered set has unexpected %s", key)
		}
	}
}

func requireToggleShape(t *testing.T, got []manifestObject, base []manifestObject, added []string, removed []string) {
	t.Helper()
	gotByKey := uniqueObjects(t, got)
	baseByKey := uniqueObjects(t, base)
	wantByKey := make(map[string]map[string]any, len(baseByKey)+len(added))
	for key, value := range baseByKey {
		wantByKey[key] = value
	}
	for _, key := range removed {
		if _, ok := wantByKey[key]; !ok {
			t.Fatalf("toggle removal %s is absent from the base render", key)
		}
		delete(wantByKey, key)
	}
	for _, key := range added {
		if _, ok := gotByKey[key]; !ok {
			t.Errorf("toggle render missing added object %s", key)
		}
		wantByKey[key] = gotByKey[key]
	}
	if len(gotByKey) != len(wantByKey) {
		t.Errorf("toggle object count = %d; want %d", len(gotByKey), len(wantByKey))
	}
	for key, wantValue := range wantByKey {
		gotValue, ok := gotByKey[key]
		if !ok {
			t.Errorf("toggle render missing %s", key)
			continue
		}
		if _, isAdded := stringSet(added)[key]; !isAdded && !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("toggle changed non-target object %s", key)
		}
	}
	for key := range gotByKey {
		if _, ok := wantByKey[key]; !ok {
			t.Errorf("toggle render has unexpected %s", key)
		}
	}
}

func requireCiliumToggleShape(t *testing.T, got []manifestObject, base []manifestObject, removed []string) {
	t.Helper()
	gotByKey := uniqueObjects(t, got)
	baseByKey := uniqueObjects(t, base)
	removedSet := stringSet(removed)
	gitProxyKey := "networking.k8s.io/v1|NetworkPolicy|tetral-system|git-proxy"

	if len(gotByKey) != len(baseByKey)-len(removed) {
		t.Fatalf("cilium-disabled object count = %d; want %d", len(gotByKey), len(baseByKey)-len(removed))
	}
	for key := range removedSet {
		if _, ok := baseByKey[key]; !ok {
			t.Fatalf("cilium removal %s is absent from the base render", key)
		}
		if _, ok := gotByKey[key]; ok {
			t.Errorf("cilium-disabled render retained %s", key)
		}
	}
	for key, wantValue := range baseByKey {
		if _, removed := removedSet[key]; removed {
			continue
		}
		gotValue, ok := gotByKey[key]
		if !ok {
			t.Errorf("cilium-disabled render missing %s", key)
			continue
		}
		if key == gitProxyKey {
			requireGitProxyL4DNSDifference(t, gotValue, wantValue)
			continue
		}
		if !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("cilium toggle changed non-target object %s", key)
		}
	}
	for key := range gotByKey {
		if _, ok := baseByKey[key]; !ok {
			t.Errorf("cilium-disabled render has unexpected %s", key)
		}
	}
}

func requireGitProxyL4DNSDifference(t *testing.T, got map[string]any, base map[string]any) {
	t.Helper()
	gotEgress := requireManifestList(t, got, "spec", "egress")
	baseEgress := requireManifestList(t, base, "spec", "egress")
	wantDNS := map[string]any{
		"to": []any{
			map[string]any{
				"namespaceSelector": map[string]any{
					"matchLabels": map[string]any{"kubernetes.io/metadata.name": "kube-system"},
				},
				"podSelector": map[string]any{
					"matchLabels": map[string]any{"k8s-app": "kube-dns"},
				},
			},
		},
		"ports": []any{
			map[string]any{"protocol": "UDP", "port": 53},
			map[string]any{"protocol": "TCP", "port": 53},
		},
	}
	var withoutDNS []any
	matches := 0
	for _, rule := range gotEgress {
		if reflect.DeepEqual(rule, wantDNS) {
			matches++
			continue
		}
		withoutDNS = append(withoutDNS, rule)
	}
	if matches != 1 {
		t.Fatalf("cilium-disabled git-proxy DNS rules = %d; want exactly 1", matches)
	}
	if !reflect.DeepEqual(withoutDNS, baseEgress) {
		t.Fatalf("cilium toggle changed git-proxy beyond the one DNS rule:\ngot without DNS: %#v\nbase: %#v", withoutDNS, baseEgress)
	}
}

func requireManifestList(t *testing.T, root any, path ...string) []any {
	t.Helper()
	current := root
	for _, key := range path {
		value, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("manifest path %v reached %T before key %q", path, current, key)
		}
		current, ok = value[key]
		if !ok {
			t.Fatalf("manifest path %v is missing key %q", path, key)
		}
	}
	result, ok := current.([]any)
	if !ok {
		t.Fatalf("manifest path %v has type %T; want list", path, current)
	}
	return result
}

func uniqueObjects(t *testing.T, objects []manifestObject) map[string]map[string]any {
	t.Helper()
	result := make(map[string]map[string]any, len(objects))
	for _, object := range objects {
		if _, duplicate := result[object.key]; duplicate {
			t.Fatalf("duplicate manifest identity %s", object.key)
		}
		result[object.key] = object.value
	}
	return result
}

func objectByKey(objects []manifestObject, key string) (map[string]any, bool) {
	for _, object := range objects {
		if object.key == key {
			return object.value, true
		}
	}
	return nil, false
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func requireManifestPathString(t *testing.T, root any, want string, path ...any) {
	t.Helper()
	current := root
	for _, segment := range path {
		switch segment := segment.(type) {
		case string:
			value, ok := current.(map[string]any)
			if !ok {
				t.Fatalf("manifest path %v reached %T before key %q", path, current, segment)
			}
			current, ok = value[segment]
			if !ok {
				t.Fatalf("manifest path %v is missing key %q", path, segment)
			}
		case int:
			value, ok := current.([]any)
			if !ok || segment < 0 || segment >= len(value) {
				t.Fatalf("manifest path %v cannot index %T at %d", path, current, segment)
			}
			current = value[segment]
		default:
			t.Fatalf("manifest path %v has unsupported segment type %T", path, segment)
		}
	}
	got, ok := current.(string)
	if !ok || got != want {
		t.Fatalf("manifest path %v = %#v; want string %q", path, current, want)
	}
}

func nestedMapFieldValues(root any, mapKey string, fieldKey string) []any {
	var values []any
	switch value := root.(type) {
	case map[string]any:
		if nested, ok := value[mapKey].(map[string]any); ok {
			if field, present := nested[fieldKey]; present {
				values = append(values, field)
			}
		}
		for _, child := range value {
			values = append(values, nestedMapFieldValues(child, mapKey, fieldKey)...)
		}
	case []any:
		for _, child := range value {
			values = append(values, nestedMapFieldValues(child, mapKey, fieldKey)...)
		}
	}
	return values
}

func nestedFieldValues(root any, fieldKey string) []any {
	var values []any
	switch value := root.(type) {
	case map[string]any:
		if field, present := value[fieldKey]; present {
			values = append(values, field)
		}
		for _, child := range value {
			values = append(values, nestedFieldValues(child, fieldKey)...)
		}
	case []any:
		for _, child := range value {
			values = append(values, nestedFieldValues(child, fieldKey)...)
		}
	}
	return values
}

func exactTestPattern(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, regexp.QuoteMeta(name))
	}
	return "^(" + strings.Join(quoted, "|") + ")$"
}

func copyTestFile(t *testing.T, source string, destination string) {
	t.Helper()
	body, err := os.ReadFile(source) //nolint:gosec // Paths are test-owned temporary or repository files.
	if err != nil {
		t.Fatalf("read %s: %v", source, err)
	}
	if err := os.WriteFile(destination, body, 0o600); err != nil { //nolint:gosec // test-owned rendered-output path.
		t.Fatalf("write %s: %v", destination, err)
	}
}

func runGoTest(t *testing.T, root string, environment []string, arguments ...string) {
	t.Helper()
	args := append([]string{"test", "-count=1"}, arguments...)
	command := exec.Command("go", args...) //nolint:gosec // Go test arguments are fixed by this test.
	command.Dir = root
	command.Env = append(os.Environ(), environment...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}
