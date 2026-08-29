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
	if len(canonical) != 61 {
		t.Fatalf("canonical object count = %d; want 61", len(canonical))
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
	rendered := uniqueObjects(t, renderChart(t, helm, chart, "cilium.gitProxyFQDNPolicy=true"))
	key := "cilium.io/v2|CiliumNetworkPolicy|tetral-system|git-proxy-github-egress"
	policy, ok := rendered[key]
	if !ok {
		t.Fatalf("git-proxy FQDN-policy render missing %s", key)
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

func TestHelmChartGitProxyFQDNPolicyIsMutuallyExclusive(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	defaults := uniqueObjects(t, renderChart(t, helm, chart))
	withFQDNPolicy := uniqueObjects(t, renderChart(t, helm, chart, "cilium.gitProxyFQDNPolicy=true"))

	const networkPolicyKey = "networking.k8s.io/v1|NetworkPolicy|tetral-system|git-proxy"
	const ciliumPolicyKey = "cilium.io/v2|CiliumNetworkPolicy|tetral-system|git-proxy-github-egress"
	if _, ok := defaults[ciliumPolicyKey]; ok {
		t.Fatalf("default render unexpectedly contains %s", ciliumPolicyKey)
	}
	if _, ok := withFQDNPolicy[ciliumPolicyKey]; !ok {
		t.Fatalf("git-proxy FQDN-policy render missing %s", ciliumPolicyKey)
	}
	if len(withFQDNPolicy) != len(defaults)+1 {
		t.Fatalf("git-proxy FQDN-policy object count = %d; want default %d plus one CiliumNetworkPolicy", len(withFQDNPolicy), len(defaults))
	}

	networkPolicy, ok := withFQDNPolicy[networkPolicyKey]
	if !ok {
		t.Fatalf("git-proxy FQDN-policy render missing %s", networkPolicyKey)
	}
	wantDatabaseEgress := map[string]any{
		"to": []any{
			map[string]any{
				"podSelector": map[string]any{
					"matchLabels": map[string]any{"app.kubernetes.io/name": "tetral-postgres"},
				},
			},
		},
		"ports": []any{
			map[string]any{"protocol": "TCP", "port": 5432},
		},
	}
	if got := requireManifestList(t, networkPolicy, "spec", "egress"); !reflect.DeepEqual(got, []any{wantDatabaseEgress}) {
		t.Fatalf("git-proxy FQDN-policy NetworkPolicy egress must be exactly the database rule:\ngot:  %#v\nwant: %#v", got, []any{wantDatabaseEgress})
	}

	for key, want := range defaults {
		if key == networkPolicyKey {
			continue
		}
		got, ok := withFQDNPolicy[key]
		if !ok {
			t.Errorf("git-proxy FQDN-policy render missing non-target object %s", key)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("git-proxy FQDN-policy toggle changed non-target object %s", key)
		}
	}
	for key := range withFQDNPolicy {
		if key == ciliumPolicyKey || key == networkPolicyKey {
			continue
		}
		if _, ok := defaults[key]; !ok {
			t.Errorf("git-proxy FQDN-policy render added unexpected object %s", key)
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

	overrideValues := []string{
		"network.apiServerPeers[0].ipBlock.cidr=172.20.0.1/32",
		"network.databasePeers[0].ipBlock.cidr=10.7.0.0/16",
		"network.publicIngressPeers[0].namespaceSelector.matchLabels.tetral\\.ai/network-role=edge-override-fixture",
		"network.dnsPeers[0].namespaceSelector.matchLabels.kubernetes\\.io/metadata\\.name=dns-override-fixture",
		"network.externalEgressPeers[0].ipBlock.cidr=203.0.113.0/24",
		"network.ciliumDNSEndpointSelectors[0].matchLabels.k8s\\:k8s-app=cnp-selector-fixture",
		"network.databasePort=25060",
		"network.externalEgressPorts[0]=8443",
	}
	rendered := renderChart(t, helm, chart, overrideValues...)

	defaults := map[string]int{
		"10.96.0.1/32":      0,
		"tetral-postgres":   0,
		"public-ingress":    0,
		"k8s-app: kube-dns": 0,
		"0.0.0.0/0":         0,
		"port: 5432":        0,
		"port: 443":         0,
	}
	overrides := map[string]int{
		"172.20.0.1/32":         0,
		"10.7.0.0/16":           0,
		"edge-override-fixture": 0,
		"dns-override-fixture":  0,
		"203.0.113.0/24":        0,
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
		"dns-override-fixture":  10,
		"203.0.113.0/24":        5,
		"port: 25060":           9,
		"port: 8443":            5,
	}
	for needle, expected := range want {
		if overrides[needle] != expected {
			t.Fatalf("override %q reached %d policies; want %d", needle, overrides[needle], expected)
		}
	}

	ciliumValues := append(append([]string(nil), overrideValues...), "cilium.gitProxyFQDNPolicy=true")
	ciliumRendered := uniqueObjects(t, renderChart(t, helm, chart, ciliumValues...))
	const ciliumPolicyKey = "cilium.io/v2|CiliumNetworkPolicy|tetral-system|git-proxy-github-egress"
	ciliumPolicy, ok := ciliumRendered[ciliumPolicyKey]
	if !ok {
		t.Fatalf("Cilium selector override render missing %s", ciliumPolicyKey)
	}
	encoded, err := yaml.Marshal(ciliumPolicy)
	if err != nil {
		t.Fatalf("marshal overridden git-proxy CiliumNetworkPolicy: %v", err)
	}
	if got := strings.Count(string(encoded), "k8s:k8s-app: kube-dns"); got != 0 {
		t.Fatalf("default Cilium DNS selector survived the override %d times; want 0", got)
	}
	if got := strings.Count(string(encoded), "cnp-selector-fixture"); got != 1 {
		t.Fatalf("Cilium DNS selector override reached %d policies; want 1", got)
	}
}

func TestHelmChartRejectsInvalidNetworkAndGitProxyFQDNPolicyValues(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")

	for _, test := range []struct {
		name       string
		values     []string
		wantInFail string
	}{
		{name: "apiServerPeers-empty", values: []string{"network.apiServerPeers=[]"}, wantInFail: "network.apiServerPeers"},
		{name: "apiServerPeers-null-element", values: []string{"network.apiServerPeers=[null]"}, wantInFail: "network.apiServerPeers"},
		{name: "apiServerPeers-empty-object", values: []string{"network.apiServerPeers=[{}]"}, wantInFail: "network.apiServerPeers"},
		{name: "databasePeers-empty", values: []string{"network.databasePeers=[]"}, wantInFail: "network.databasePeers"},
		{name: "databasePeers-null-element", values: []string{"network.databasePeers=[null]"}, wantInFail: "network.databasePeers"},
		{name: "databasePeers-empty-object", values: []string{"network.databasePeers=[{}]"}, wantInFail: "network.databasePeers"},
		{name: "publicIngressPeers-empty", values: []string{"network.publicIngressPeers=[]"}, wantInFail: "network.publicIngressPeers"},
		{name: "publicIngressPeers-null-element", values: []string{"network.publicIngressPeers=[null]"}, wantInFail: "network.publicIngressPeers"},
		{name: "publicIngressPeers-empty-object", values: []string{"network.publicIngressPeers=[{}]"}, wantInFail: "network.publicIngressPeers"},
		{name: "dnsPeers-empty", values: []string{"network.dnsPeers=[]"}, wantInFail: "network.dnsPeers"},
		{name: "dnsPeers-null-element", values: []string{"network.dnsPeers=[null]"}, wantInFail: "network.dnsPeers"},
		{name: "dnsPeers-empty-object", values: []string{"network.dnsPeers=[{}]"}, wantInFail: "network.dnsPeers"},
		{name: "externalEgressPeers-empty", values: []string{"network.externalEgressPeers=[]"}, wantInFail: "network.externalEgressPeers"},
		{name: "externalEgressPeers-null-element", values: []string{"network.externalEgressPeers=[null]"}, wantInFail: "network.externalEgressPeers"},
		{name: "externalEgressPeers-empty-object", values: []string{"network.externalEgressPeers=[{}]"}, wantInFail: "network.externalEgressPeers"},
		{name: "ciliumDNSEndpointSelectors-empty", values: []string{"network.ciliumDNSEndpointSelectors=[]"}, wantInFail: "network.ciliumDNSEndpointSelectors"},
		{name: "ciliumDNSEndpointSelectors-null-element", values: []string{"network.ciliumDNSEndpointSelectors=[null]"}, wantInFail: "network.ciliumDNSEndpointSelectors"},
		{name: "ciliumDNSEndpointSelectors-empty-object", values: []string{"network.ciliumDNSEndpointSelectors=[{}]"}, wantInFail: "network.ciliumDNSEndpointSelectors"},
		{name: "databasePort-null", values: []string{"network.databasePort=null"}, wantInFail: "network.databasePort"},
		{name: "databasePort-empty-object", values: []string{"network.databasePort={}"}, wantInFail: "network.databasePort"},
		{name: "databasePort-list", values: []string{"network.databasePort=[5432]"}, wantInFail: "network.databasePort"},
		{name: "externalEgressPorts-empty", values: []string{"network.externalEgressPorts=[]"}, wantInFail: "network.externalEgressPorts"},
		{name: "externalEgressPorts-null-element", values: []string{"network.externalEgressPorts=[null]"}, wantInFail: "network.externalEgressPorts"},
		{name: "externalEgressPorts-empty-object", values: []string{"network.externalEgressPorts=[{}]"}, wantInFail: "network.externalEgressPorts"},
		{name: "git-proxy-fqdn-policy-not-boolean", values: []string{`cilium.gitProxyFQDNPolicy="enabled"`}, wantInFail: "cilium.gitProxyFQDNPolicy must be a boolean"},
		// --set-json replaces the whole cilium map here; the chart currently has
		// exactly these two keys, so the case fully specifies the intended state.
		{name: "git-proxy-fqdn-policy-without-cilium", values: []string{`cilium={"enabled":false,"gitProxyFQDNPolicy":true}`}, wantInFail: "cilium.gitProxyFQDNPolicy=true requires cilium.enabled=true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := renderChartFailure(t, helm, chart, test.values...)
			if err == nil {
				t.Fatalf("invalid values %v rendered successfully", test.values)
			}
			if !strings.Contains(output, test.wantInFail) {
				t.Fatalf("invalid values %v failure does not contain %q:\n%s", test.values, test.wantInFail, output)
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
		"secrets.tetralBlob=custom-blob",
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
	var blobSecretRefs int
	for _, object := range rendered {
		for _, value := range nestedMapFieldValues(object, "secretKeyRef", "name") {
			if value == "custom-blob" {
				blobSecretRefs++
			}
		}
	}
	if blobSecretRefs != 15 {
		t.Fatalf("custom Blob Secret references = %d; want 15 across API and both Bridge containers", blobSecretRefs)
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

func TestHelmReleaseRenderSeparatesWorkloadDigestsFromDaytonaSnapshotName(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	development := uniqueObjects(t, renderChart(t, helm, chart))
	requireManifestPathString(
		t,
		development["v1|ConfigMap|tetral-system|api-config"],
		"ghcr.io/tetral-ai/sandbox:0.0.0-dev",
		"data",
		"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF",
	)

	version := "0.1.0-alpha.7"
	digest := "sha256:" + strings.Repeat("a", 64)
	output := t.TempDir()
	packageCommand := exec.Command(helm, "package", chart, "--version", version, "--app-version", version, "--destination", output) //nolint:gosec // Helm path, chart and version are test-owned.
	if result, err := packageCommand.CombinedOutput(); err != nil {
		t.Fatalf("helm package release chart: %v\n%s", err, result)
	}
	releaseValues := map[string]any{
		"image": map[string]any{"digests": map[string]string{
			"tetral": digest, "gateway": digest, "agent-runtime": digest, "sandbox": digest,
		}},
		"observability": map[string]string{"serviceVersion": version},
	}
	valuesBody, err := yaml.Marshal(releaseValues)
	if err != nil {
		t.Fatal(err)
	}
	valuesPath := filepath.Join(output, "release-values.yaml")
	if err := os.WriteFile(valuesPath, valuesBody, 0o600); err != nil {
		t.Fatal(err)
	}
	renderCommand := exec.Command(helm, "template", "tetral", filepath.Join(output, "tetral-"+version+".tgz"), "-f", valuesPath) //nolint:gosec // Helm path and test-owned package are fixed.
	renderBody, err := renderCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template release chart: %v\n%s", err, renderBody)
	}
	rendered := uniqueObjects(t, decodeManifestObjects(t, bytes.NewReader(renderBody), "packaged release chart"))
	for _, object := range rendered {
		for _, value := range nestedFieldValues(object, "image") {
			image, ok := value.(string)
			if !ok || !strings.HasSuffix(image, "@"+digest) {
				t.Fatalf("release workload image = %v; want an immutable digest reference", value)
			}
		}
	}
	apiConfig := rendered["v1|ConfigMap|tetral-system|api-config"]
	requireManifestPathString(
		t,
		apiConfig,
		"ghcr.io/tetral-ai/sandbox:"+version,
		"data",
		"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF",
	)
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

func renderChartFailure(t *testing.T, helm string, chart string, values ...string) (string, error) {
	t.Helper()
	args := []string{"template", "tetral", chart}
	for _, value := range values {
		args = append(args, "--set-json", value)
	}
	command := exec.Command(helm, args...) //nolint:gosec // Helm path and chart arguments are test-owned.
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
