package kubernetesmanifest_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type manifestDocuments []manifestDocument

func (documents manifestDocuments) byFile(file string) manifestDocuments {
	var matches manifestDocuments
	for _, document := range documents {
		if document.file == file {
			matches = append(matches, document)
		}
	}
	return matches
}

func (documents manifestDocuments) find(file string, kind string, name string) *manifestDocument {
	for index := range documents {
		document := &documents[index]
		if document.file == file && document.kind == kind && document.name == name {
			return document
		}
	}
	return nil
}

func readManifestDocuments(t *testing.T) manifestDocuments {
	t.Helper()
	root := topLevelManifestRoot()
	expectedFiles := []string{
		"database-budget.yaml",
		"internal-routing.yaml",
		"internal-security.yaml",
		"agent-runtime.yaml",
		"bridge.yaml",
		"job-runner-rbac.yaml",
		"event-stream.yaml",
		"computer-hub.yaml",
		"provider-gateway.yaml",
		"mcp-connector.yaml",
		"web-connector.yaml",
		"job-runner.yaml",
		"internal-grpc-tokenreview-rbac.yaml",
		"auth.yaml",
		"api.yaml",
		"cleanup.yaml",
		"git-proxy.yaml",
		"queue.yaml",
		"sandbox.yaml",
	}
	entries, err := filepath.Glob(filepath.Join(root, "*.yaml"))
	if err != nil {
		t.Fatalf("glob manifests: %v", err)
	}
	var present []string
	for _, entry := range entries {
		present = append(present, filepath.Base(entry))
	}
	wanted := append([]string(nil), expectedFiles...)
	sort.Strings(wanted)
	sort.Strings(present)
	if !reflect.DeepEqual(present, wanted) {
		t.Fatalf("manifest files = %v; want %v", present, wanted)
	}
	var documents manifestDocuments
	for _, file := range expectedFiles {
		documents = append(documents, manifestDocumentsOf(t, filepath.Join(root, file), file)...)
	}
	return documents
}

func readEdgeGatewayAdapterDocuments(t *testing.T, file string) manifestDocuments {
	t.Helper()
	adapterDir := filepath.Join(topLevelManifestRoot(), "edge-gateway")
	entries, err := os.ReadDir(adapterDir)
	if err != nil {
		t.Fatalf("read %s: %v", adapterDir, err)
	}
	if len(entries) != 1 || entries[0].Name() != file {
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("%s entries = %v; want exactly %s", adapterDir, names, file)
	}
	return manifestDocumentsOf(t, filepath.Join(adapterDir, file), filepath.Join("edge-gateway", file))
}

func topLevelManifestRoot() string {
	if root := os.Getenv("TETRAL_KUBERNETES_MANIFEST_ROOT"); root != "" {
		return root
	}
	return "."
}

func requireDocument(t *testing.T, documents manifestDocuments, file string, kind string, name string) *manifestDocument {
	t.Helper()
	document := documents.find(file, kind, name)
	if document == nil {
		t.Fatalf("missing %s %s in %s", kind, name, file)
	}
	return document
}

func requireContains(t *testing.T, document *manifestDocument, want string) {
	t.Helper()
	if !has(t, document.object, want) {
		t.Fatalf("%s %s/%s does not hold:\n%s", document.file, document.kind, document.name, want)
	}
}

func requireNotContains(t *testing.T, document *manifestDocument, forbidden string) {
	t.Helper()
	if has(t, document.object, forbidden) {
		t.Fatalf("%s %s/%s holds what it must not:\n%s", document.file, document.kind, document.name, forbidden)
	}
}

// count is the number of mappings under node that hold want.
func count(node any, want map[string]any) (total int) {
	switch node := node.(type) {
	case map[string]any:
		if holds(node, want) {
			total++
		}
		for _, child := range node {
			total += count(child, want)
		}
	case []any:
		for _, child := range node {
			total += count(child, want)
		}
	}
	return total
}

func validateContainerCount(t *testing.T, document *manifestDocument, want int) error {
	t.Helper()
	if got := len(items(podSpec(t, document), "containers")); got != want {
		return fmt.Errorf("has %d containers; want exactly %d", got, want)
	}
	return nil
}

func requireContainerResourceBounds(t *testing.T, file string, containerName string, container map[string]any) {
	t.Helper()
	for _, bound := range []string{"requests", "limits"} {
		for _, resource := range []string{"cpu", "memory", "ephemeral-storage"} {
			if text(container, "resources", bound, resource) == "" {
				t.Fatalf("%s container %s declares no resources.%s.%s", file, containerName, bound, resource)
			}
		}
	}
}

type volumeMount struct {
	name      string
	mountPath string
}

// mountContainingDataDir returns the mount whose mountPath strictly contains dataDir at a
// path boundary: dataDir must equal mountPath + "/" + nonEmptySuffix. A bare string-prefix
// (mountPath+"-extra") and exact equality both fail by construction.
func mountContainingDataDir(dataDir string, mounts []volumeMount) (volumeMount, bool) {
	for _, mount := range mounts {
		if mount.mountPath == "" {
			continue
		}
		boundary := mount.mountPath + "/"
		if strings.HasPrefix(dataDir, boundary) && dataDir != mount.mountPath {
			return mount, true
		}
	}
	return volumeMount{}, false
}

func mountPaths(mounts []volumeMount) []string {
	paths := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		paths = append(paths, mount.mountPath)
	}
	return paths
}

func volumeNames(volumes map[string]string) []string {
	names := make([]string, 0, len(volumes))
	for name := range volumes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// parseVolumeMounts are the volume mounts of the workload's containers.
func parseVolumeMounts(t *testing.T, document *manifestDocument) []volumeMount {
	t.Helper()
	var mounts []volumeMount
	for _, container := range items(podSpec(t, document), "containers") {
		for _, mount := range items(container, "volumeMounts") {
			mounts = append(mounts, volumeMount{name: text(mount, "name"), mountPath: text(mount, "mountPath")})
		}
	}
	return mounts
}

// parseVolumes maps each volume of the workload to its source (for example
// "engine-data" to "emptyDir").
func parseVolumes(t *testing.T, document *manifestDocument) map[string]string {
	t.Helper()
	volumes := map[string]string{}
	for _, volume := range items(podSpec(t, document), "volumes") {
		for key := range fields(volume) {
			if key != "name" {
				volumes[text(volume, "name")] = key
			}
		}
	}
	return volumes
}

func requireConfigMapDataValue(t *testing.T, documents manifestDocuments, file string, name string, key string) string {
	t.Helper()
	configMap := requireDocument(t, documents, file, "ConfigMap", name)
	if _, present := fields(configMap.object, "data")[key]; !present {
		t.Fatalf("%s ConfigMap %s missing data key %s", file, name, key)
	}
	return text(configMap.object, "data", key)
}

// requireDeploymentEnvValueOrConfigMap requires the variable to equal want,
// literally or through the workload's ConfigMap.
func requireDeploymentEnvValueOrConfigMap(
	t *testing.T,
	documents manifestDocuments,
	deployment *manifestDocument,
	workload workloadManifest,
	name string,
	want string,
) {
	t.Helper()
	variable := environment(t, deployment, name)
	if _, literal := variable["value"]; literal {
		if got := text(variable, "value"); got != want {
			t.Fatalf("%s env %s = %q; want %q", deployment.file, name, got, want)
		}
		return
	}
	if workload.configMapName == "" {
		t.Fatalf("%s env %s has no literal value and workload has no ConfigMap owner", deployment.file, name)
	}
	if reference := at(variable, "valueFrom", "configMapKeyRef"); text(reference, "name") != workload.configMapName || text(reference, "key") != name {
		t.Fatalf("%s env %s reads %v; want key %s of ConfigMap %s", deployment.file, name, reference, name, workload.configMapName)
	}
	value := requireConfigMapDataValue(t, documents, workload.file, workload.configMapName, name)
	if value != want {
		t.Fatalf("%s ConfigMap %s data %s = %q; want %q", workload.file, workload.configMapName, name, value, want)
	}
}

// requireDeploymentEnvValue returns the literal value of the variable.
func requireDeploymentEnvValue(t *testing.T, document *manifestDocument, name string) string {
	t.Helper()
	variable := environment(t, document, name)
	if variable == nil {
		t.Fatalf("%s missing env %s", document.file, name)
	}
	if _, literal := variable["value"]; !literal {
		t.Fatalf("%s env %s has no literal value", document.file, name)
	}
	return text(variable, "value")
}

func requireDeploymentEnvFieldPath(t *testing.T, document *manifestDocument, name string) string {
	t.Helper()
	variable := environment(t, document, name)
	if variable == nil {
		t.Fatalf("%s missing env %s", document.file, name)
	}
	path := text(variable, "valueFrom", "fieldRef", "fieldPath")
	if path == "" {
		t.Fatalf("%s env %s has no fieldPath valueFrom", document.file, name)
	}
	return path
}

func requireDeploymentPodLabel(t *testing.T, document *manifestDocument, key string) (string, string) {
	t.Helper()
	value := text(document.object, "spec", "template", "metadata", "labels", key)
	if value == "" {
		t.Fatalf("%s %s/%s missing pod template label %s", document.file, document.kind, document.name, key)
	}
	return key, value
}

func requireProjectedBoundedServiceAccountTokenShape(t *testing.T, document *manifestDocument, expected projectedTokenExpectation) {
	t.Helper()
	requireContains(t, document, fmt.Sprintf("- name: %s\n  mountPath: %s\n  readOnly: true", expected.volume, expected.mountPath))
	volume := named(items(podSpec(t, document), "volumes"), expected.volume)
	sources := items(volume, "projected", "sources")
	if len(sources) == 0 {
		t.Fatalf("%s has no projected volume %s", document.file, expected.volume)
	}
	token := at(sources[0], "serviceAccountToken")
	want := map[string]any{"expirationSeconds": expected.expirationSeconds, "path": expected.filePath}
	switch expected.audienceMode {
	case projectedTokenWithAudience:
		if expected.audience == "" {
			t.Fatalf("%s projected token volume %s requires a non-empty audience expectation", document.file, expected.volume)
		}
		want["audience"] = expected.audience
	case projectedTokenNoAudience:
		if mentions(volume, "audience") {
			t.Fatalf("%s projected token volume %s names an audience", document.file, expected.volume)
		}
	default:
		t.Fatalf("%s projected token volume %s has unknown audience mode %q", document.file, expected.volume, expected.audienceMode)
	}
	if !holds(token, want) {
		t.Fatalf("%s projected token volume %s = %v; want %v", document.file, expected.volume, token, want)
	}
}

func requireMetadataNamespace(t *testing.T, document *manifestDocument) string {
	t.Helper()
	namespace := text(document.object, "metadata", "namespace")
	if namespace == "" {
		t.Fatalf("%s %s/%s missing metadata.namespace", document.file, document.kind, document.name)
	}
	return namespace
}

func parseNetworkPolicyIngressRules(t *testing.T, document *manifestDocument) []networkPolicyRule {
	t.Helper()
	return parseNetworkPolicyRules(t, document, "ingress", "from")
}

func parseNetworkPolicyEgressRules(t *testing.T, document *manifestDocument) []networkPolicyRule {
	t.Helper()
	return parseNetworkPolicyRules(t, document, "egress", "to")
}

// parseNetworkPolicyRules are the rules of one direction of a NetworkPolicy.
// A form a rule could widen access through without these checks seeing it (a
// port range, a selector expression) fails the test.
func parseNetworkPolicyRules(t *testing.T, document *manifestDocument, section string, peerField string) []networkPolicyRule {
	t.Helper()
	policyNamespace := requireMetadataNamespace(t, document)
	var rules []networkPolicyRule
	for _, entry := range items(document.object, "spec", section) {
		var rule networkPolicyRule
		for _, port := range items(entry, "ports") {
			number, ok := at(port, "port").(int)
			if _, ranged := fields(port)["endPort"]; !ok || ranged {
				t.Fatalf("%s %s/%s has a port these checks do not read: %v", document.file, document.kind, document.name, port)
			}
			protocol := text(port, "protocol")
			if protocol == "" {
				protocol = "TCP"
			}
			rule.ports = append(rule.ports, number)
			rule.transports = append(rule.transports, networkPolicyTransport{protocol: protocol, port: number})
		}
		for _, peer := range items(entry, peerField) {
			if block, ok := fields(peer)["ipBlock"]; ok {
				rule.ipBlocks = append(rule.ipBlocks, text(block, "cidr"))
				continue
			}
			parsed := networkPolicyPeer{namespace: policyNamespace, namespaceLabels: map[string]string{}, podLabels: map[string]string{}}
			for selector, labels := range map[string]map[string]string{"namespaceSelector": parsed.namespaceLabels, "podSelector": parsed.podLabels} {
				if _, expressions := fields(peer, selector)["matchExpressions"]; expressions {
					t.Fatalf("%s %s/%s has a selector expression these checks do not read: %v", document.file, document.kind, document.name, peer)
				}
				matched := fields(peer, selector, "matchLabels")
				for key := range matched {
					labels[key] = text(matched, key)
				}
			}
			if namespace, named := parsed.namespaceLabels["kubernetes.io/metadata.name"]; named {
				parsed.namespace = namespace
			}
			parsed.podName, parsed.podPartOf = parsed.podLabels["app.kubernetes.io/name"], parsed.podLabels["app.kubernetes.io/part-of"]
			rule.peers = append(rule.peers, parsed)
		}
		rules = append(rules, rule)
	}
	if len(rules) == 0 {
		t.Fatalf("%s %s/%s missing %s rules", document.file, document.kind, document.name, section)
	}
	return rules
}

func gatewayGRPCServiceTransports(t *testing.T, service *manifestDocument) []networkPolicyTransport {
	t.Helper()
	var result []networkPolicyTransport
	for _, port := range items(service.object, "spec", "ports") {
		if number, ok := at(port, "port").(int); ok && strings.HasSuffix(text(port, "name"), "-grpc") {
			result = append(result, networkPolicyTransport{protocol: "TCP", port: number})
		}
	}
	if len(result) == 0 {
		t.Fatalf("%s has no named gRPC service ports", service.file)
	}
	return result
}

func requireNetworkPolicyEgressIntentHosts(t *testing.T, document *manifestDocument) []string {
	t.Helper()
	var hosts []string
	for _, raw := range strings.Split(text(document.object, "metadata", "annotations", "tetral.ai/egress-intent"), ",") {
		if host := strings.TrimSpace(raw); host != "" {
			hosts = append(hosts, host)
		}
	}
	if len(hosts) == 0 {
		t.Fatalf("%s %s/%s has broad HTTPS egress but no tetral.ai/egress-intent annotation", document.file, document.kind, document.name)
	}
	return hosts
}

func stringsAt(node any, path ...string) []string {
	var values []string
	for _, item := range items(node, path...) {
		values = append(values, fmt.Sprint(item))
	}
	return values
}

func parseRBACRules(document *manifestDocument) ([]rbacRule, error) {
	var rules []rbacRule
	for _, entry := range items(document.object, "rules") {
		rule := rbacRule{apiGroups: stringsAt(entry, "apiGroups"), resources: stringsAt(entry, "resources"), verbs: stringsAt(entry, "verbs")}
		if len(rule.apiGroups) == 0 || len(rule.resources) == 0 || len(rule.verbs) == 0 || len(fields(entry)) != 3 {
			return nil, fmt.Errorf("RBAC rule these checks do not read: %v", entry)
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func parseRBACSubjects(document *manifestDocument) ([]rbacSubject, error) {
	var subjects []rbacSubject
	for _, entry := range items(document.object, "subjects") {
		subject := rbacSubject{kind: text(entry, "kind"), name: text(entry, "name"), namespace: text(entry, "namespace")}
		if subject.kind == "" || subject.name == "" {
			return nil, fmt.Errorf("incomplete RBAC subject: %#v", subject)
		}
		subjects = append(subjects, subject)
	}
	return subjects, nil
}

func parseRBACRoleRef(document *manifestDocument) (rbacRoleRef, error) {
	reference := at(document.object, "roleRef")
	roleRef := rbacRoleRef{apiGroup: text(reference, "apiGroup"), kind: text(reference, "kind"), name: text(reference, "name")}
	if roleRef.apiGroup == "" || roleRef.kind == "" || roleRef.name == "" {
		return rbacRoleRef{}, fmt.Errorf("incomplete roleRef: %#v", roleRef)
	}
	return roleRef, nil
}

// requireManifestComposition requires the top-level file to hold the
// documents of the component files, in their order.
func requireManifestComposition(t *testing.T, topLevelFile string, componentFiles []string) {
	t.Helper()
	var composed []map[string]any
	for _, componentFile := range componentFiles {
		composed = append(composed, decodeDocuments(t, componentFile)...)
	}
	if !reflect.DeepEqual(decodeDocuments(t, topLevelFile), composed) {
		t.Fatalf("deploy/kubernetes/%s must be the composition of %v", topLevelFile, componentFiles)
	}
}
