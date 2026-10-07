package kubernetesmanifest_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

type workloadManifest struct {
	file            string
	configMapName   string
	name            string
	namespace       string
	binary          string
	httpEnv         string
	httpPortName    string
	metricsEnv      string
	metricsPortName string
	publicFacing    bool
	autoscaling     bool
	ciliumPolicy    bool
	externalCheck   bool
	internalGRPC    bool
	grpcEnv         string
	grpcPortName    string
	requiredEnvVar  []string
}

type manifestDocument struct {
	file string
	kind string
	name string
	text string
}

type rbacRule struct {
	apiGroups []string
	resources []string
	verbs     []string
}

type rbacSubject struct {
	kind      string
	name      string
	namespace string
}

type rbacRoleRef struct {
	apiGroup string
	kind     string
	name     string
}

type tokenReviewGrant struct {
	name                    string
	serviceAccountName      string
	serviceAccountNamespace string
}

type networkPolicyPeer struct {
	namespace       string
	podName         string
	podPartOf       string
	namespaceLabels map[string]string
	podLabels       map[string]string
}

type networkPolicyRule struct {
	ports      []int
	transports []networkPolicyTransport
	peers      []networkPolicyPeer
	ipBlocks   []string
}

type networkPolicyTransport struct {
	protocol string
	port     int
}

type projectedTokenAudienceMode string

const (
	projectedTokenWithAudience projectedTokenAudienceMode = "with-audience"
	projectedTokenNoAudience   projectedTokenAudienceMode = "no-audience"
)

type projectedTokenExpectation struct {
	envName           string
	volume            string
	mountPath         string
	filePath          string
	audienceMode      projectedTokenAudienceMode
	audience          string
	expirationSeconds int
}

var workloadManifests = []workloadManifest{
	{
		file:            "auth.yaml",
		configMapName:   "auth-config",
		name:            "auth",
		namespace:       "tetral-system",
		binary:          "tetral-auth",
		httpEnv:         "TETRAL_AUTH_HTTP_ADDR",
		httpPortName:    "http",
		metricsEnv:      "TETRAL_AUTH_METRICS_ADDR",
		metricsPortName: "metrics",
		externalCheck:   true,
		publicFacing:    true,
		requiredEnvVar: []string{
			"ENGINE_API_KEY",
			"ENGINE_BOOTSTRAP_WORKSPACE_ID",
			"TETRAL_DATABASE_URL",
			"TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64",
			"TETRAL_AUTH_INTERNAL_PRINCIPAL_TTL_SECONDS",
		},
	},
	{
		file:            "api.yaml",
		configMapName:   "api-config",
		name:            "api",
		namespace:       "tetral-system",
		binary:          "tetral-api",
		httpEnv:         "TETRAL_API_HTTP_ADDR",
		httpPortName:    "http",
		metricsEnv:      "TETRAL_API_METRICS_ADDR",
		metricsPortName: "metrics",
		publicFacing:    true,
		requiredEnvVar: []string{
			"ENGINE_VAULT_KEY",
			"TETRAL_DATABASE_URL",
			"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64",
			"ENGINE_DATA_DIR",
			"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF",
		},
	},
	{
		file:         "agent-runtime.yaml",
		name:         "agent-runtime",
		namespace:    "tetral-agent-runtime",
		binary:       "bun",
		httpEnv:      "TETRAL_RUNTIME_POD_HTTP_ADDR",
		httpPortName: "http",
		ciliumPolicy: false,
		internalGRPC: true,
		grpcEnv:      "TETRAL_RUNTIME_POD_GRPC_PORT",
		grpcPortName: "grpc",
		requiredEnvVar: []string{
			"TETRAL_RUNTIME_POD_NAMESPACE",
			"TETRAL_RUNTIME_POD_NAME",
			"TETRAL_RUNTIME_POD_UID",
			"TETRAL_RUNTIME_POD_IP",
			"TETRAL_DEPLOYMENT_ENVIRONMENT",
			"TETRAL_SERVICE_VERSION",
			"TETRAL_RUNTIME_POD_GRPC_AUDIENCE",
			"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
			"TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH",
			"TETRAL_BRIDGE_API_GRPC_ADDR",
			"TETRAL_GATEWAY_GRPC_ADDR",
			"TETRAL_MCP_CONNECTOR_GRPC_ADDR",
			"TETRAL_WEB_CONNECTOR_GRPC_ADDR",
			"TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL",
			"TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES",
			"TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS",
			"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
			"KUBERNETES_API_SERVER_URL",
			"KUBERNETES_API_CA_CERT_PATH",
		},
	},
	{file: "provider-gateway.yaml", name: "provider-gateway", namespace: "tetral-system", binary: "bun", httpEnv: "TETRAL_PROVIDER_GATEWAY_HTTP_ADDR", httpPortName: "http", internalGRPC: true, grpcEnv: "TETRAL_PROVIDER_GATEWAY_GRPC_ADDR", grpcPortName: "provider-grpc", autoscaling: true, ciliumPolicy: false},
	{file: "mcp-connector.yaml", name: "mcp-connector", namespace: "tetral-system", binary: "bun", httpEnv: "TETRAL_MCP_CONNECTOR_HTTP_ADDR", httpPortName: "mcp-http", internalGRPC: true, grpcEnv: "TETRAL_MCP_CONNECTOR_GRPC_ADDR", grpcPortName: "mcp-grpc", ciliumPolicy: false},
	{file: "web-connector.yaml", configMapName: "web-connector-config", name: "web-connector", namespace: "tetral-system", binary: "web-connector", httpEnv: "TETRAL_WEB_CONNECTOR_METRICS_ADDR", httpPortName: "web-metrics", internalGRPC: true, grpcEnv: "TETRAL_WEB_CONNECTOR_GRPC_ADDR", grpcPortName: "web-grpc", ciliumPolicy: false},
	{file: "job-runner.yaml", name: "job-runner", namespace: "tetral-system", binary: "job-runner", httpEnv: "TETRAL_BRIDGE_JOB_RUNNER_HTTP_ADDR", httpPortName: "http-job", ciliumPolicy: false},
	{
		file:         "git-proxy.yaml",
		name:         "git-proxy",
		namespace:    "tetral-system",
		binary:       "git-proxy",
		httpEnv:      "TETRAL_GIT_PROXY_HTTP_ADDR",
		httpPortName: "http",
		publicFacing: true,
		autoscaling:  true,
		ciliumPolicy: false,
		requiredEnvVar: []string{
			"TETRAL_DATABASE_URL",
			"ENGINE_VAULT_KEY",
			"TETRAL_GIT_PROXY_PUBLIC_BASE_URL",
			"TETRAL_GIT_PROXY_DRAIN_GRACE_SECONDS",
		},
	},
	{
		file:           "bridge.yaml",
		configMapName:  "bridge-config",
		name:           "bridge",
		namespace:      "tetral-system",
		binary:         "bridge-api",
		httpEnv:        "TETRAL_BRIDGE_API_HTTP_ADDR",
		httpPortName:   "http",
		internalGRPC:   true,
		grpcEnv:        "TETRAL_BRIDGE_API_GRPC_ADDR",
		grpcPortName:   "grpc",
		ciliumPolicy:   false,
		requiredEnvVar: []string{"TETRAL_INTERNAL_GRPC_AUDIENCE", "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS"},
	},
	{
		file:          "queue.yaml",
		configMapName: "queue-config",
		name:          "queue",
		namespace:     "tetral-system",
		binary:        "tetral-queue",
		httpEnv:       "TETRAL_QUEUE_HTTP_ADDR",
		httpPortName:  "http",
		internalGRPC:  true,
		grpcEnv:       "TETRAL_QUEUE_GRPC_ADDR",
		grpcPortName:  "grpc",
		requiredEnvVar: []string{
			"TETRAL_DATABASE_URL",
			"TETRAL_QUEUE_LEASE_RECLAIM_INTERVAL_SECONDS",
			"TETRAL_QUEUE_LEASE_RECLAIM_LIMIT",
			"TETRAL_QUEUE_RETRY_BASE_MS",
			"TETRAL_QUEUE_RETRY_CAP_MS",
			"TETRAL_QUEUE_RETRY_MAX_ATTEMPTS",
		},
	},
	{
		file:          "sandbox.yaml",
		configMapName: "sandbox-config",
		name:          "sandbox",
		namespace:     "tetral-system",
		binary:        "tetral-sandbox",
		httpEnv:       "TETRAL_SANDBOX_HTTP_ADDR",
		httpPortName:  "http",
		requiredEnvVar: []string{
			"TETRAL_POSTGRES_DSN",
			"DAYTONA_API_URL",
			"DAYTONA_API_KEY",
			"TETRAL_QUEUE_GRPC_ADDR",
			"TETRAL_GIT_PROXY_HOST",
			"TETRAL_SANDBOX_JOB_LEASE_DURATION",
			"TETRAL_SANDBOX_PROVIDER_COMMAND_TIMEOUT",
			"TETRAL_SANDBOX_WORKER_CONCURRENCY",
			"TETRAL_RESOURCE_CRED_TTL",
			"TETRAL_RESOURCE_CRED_REFRESH_MARGIN",
			"TETRAL_R2_PARENT_API_TOKEN",
			"TETRAL_R2_PARENT_ACCESS_KEY",
		},
	},
	{
		file:            "event-stream.yaml",
		name:            "event-stream",
		namespace:       "tetral-system",
		binary:          "event-stream",
		httpEnv:         "TETRAL_EVENT_STREAM_HTTP_ADDR",
		httpPortName:    "http",
		metricsEnv:      "TETRAL_EVENT_STREAM_METRICS_ADDR",
		metricsPortName: "metrics",
		publicFacing:    true,
		requiredEnvVar:  []string{"TETRAL_EVENT_STREAM_DATABASE_URL", "TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64"},
	},
}

func expectedWorkloadDocumentCount(workload workloadManifest) int {
	count := 4
	if workload.configMapName != "" {
		count++
	}
	if workload.autoscaling {
		count++
	}
	if workload.ciliumPolicy {
		count++
	}
	if workload.name == "api" || workload.name == "event-stream" || workload.name == "auth" {
		count++
	}
	return count
}

func TestKubernetesManifestWorkloadFilesInstallRequiredKinds(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		docs := documents.byFile(workload.file)
		wantCount := expectedWorkloadDocumentCount(workload)
		if len(docs) != wantCount {
			t.Fatalf("%s document count = %d; want %d namespace-scoped workload resources", workload.file, len(docs), wantCount)
		}
		for _, kind := range []string{"ServiceAccount", "Deployment", "Service", "NetworkPolicy"} {
			if documents.find(workload.file, kind, workload.name) == nil {
				t.Fatalf("%s missing %s named %s", workload.file, kind, workload.name)
			}
		}
		if workload.configMapName != "" && documents.find(workload.file, "ConfigMap", workload.configMapName) == nil {
			t.Fatalf("%s missing service-local ConfigMap %s", workload.file, workload.configMapName)
		}
		if workload.autoscaling && documents.find(workload.file, "HorizontalPodAutoscaler", workload.name) == nil {
			t.Fatalf("%s missing HorizontalPodAutoscaler named %s", workload.file, workload.name)
		}
	}
}

func TestKubernetesManifestCleanupCronJobIsComposedTopLevel(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, kind := range []string{"ServiceAccount", "CronJob", "NetworkPolicy"} {
		if documents.find("cleanup.yaml", kind, "cleanup") == nil {
			t.Fatalf("cleanup.yaml missing %s named cleanup", kind)
		}
	}
	cronJob := requireDocument(t, documents, "cleanup.yaml", "CronJob", "cleanup")
	requireContains(t, cronJob, "serviceAccountName: cleanup")
	requireContains(t, cronJob, "automountServiceAccountToken: false")
	requireContains(t, cronJob, "command:\n                - /usr/local/bin/tetral-cleanup")
	for _, envName := range []string{"TETRAL_DATABASE_URL", "TETRAL_CLEANUP_CLAIM_LIMIT"} {
		requireContains(t, cronJob, "name: "+envName)
	}
	requireNotContains(t, cronJob, "TETRAL_WORKSPACE_ID")
	for containerName, block := range containerBlocks(t, "cleanup.yaml", cronJob.text) {
		requireContainerResourceBounds(t, "cleanup.yaml", containerName, block)
	}
	networkPolicy := requireDocument(t, documents, "cleanup.yaml", "NetworkPolicy", "cleanup")
	requireNetworkPolicyEgressEdge(t, networkPolicy, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNoBroadNetworkPolicyEgressPeers(t, networkPolicy, 5432)
	requireKubeDNSEgress(t, networkPolicy)
	requireNotContains(t, networkPolicy, "0.0.0.0/0")
}

func TestKubernetesManifestLabelSelectorsAreConsistent(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		deployment := requireDocument(t, documents, workload.file, "Deployment", workload.name)
		service := requireDocument(t, documents, workload.file, "Service", workload.name)
		networkPolicy := requireDocument(t, documents, workload.file, "NetworkPolicy", workload.name)
		for _, doc := range []*manifestDocument{deployment, service, networkPolicy} {
			requireContains(t, doc, "app.kubernetes.io/name: "+workload.name)
			requireContains(t, doc, "app.kubernetes.io/part-of: tetral")
		}
		requireContains(t, deployment, "matchLabels:\n      app.kubernetes.io/name: "+workload.name)
		requireContains(t, deployment, "labels:\n        app.kubernetes.io/name: "+workload.name)
		requireContains(t, service, "selector:\n    app.kubernetes.io/name: "+workload.name)
		requireContains(t, networkPolicy, "podSelector:\n    matchLabels:\n      app.kubernetes.io/name: "+workload.name)
	}
}

func TestKubernetesManifestDeploymentTargetsCorrectWorkloadBinary(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, w := range workloadManifests {
		d := requireDocument(t, documents, w.file, "Deployment", w.name)
		requireContains(t, d, "serviceAccountName: "+w.name)
		if err := validateSingleContainer(d.text); err != nil {
			t.Fatalf("%s: %v", w.file, err)
		}
		container := w.name
		if w.name == "bridge" {
			container = "bridge-api"
		}
		requireContains(t, d, "containers:\n        - name: "+container)
		command := []string{"/usr/local/bin/" + w.binary}
		switch w.name {
		case "agent-runtime":
			command = readAgentRuntimePackageMetadata(t).Tetral.AgentRuntimePod.ContainerCommand
		case "provider-gateway":
			command = readProviderGatewayPackageMetadata(t).Tetral.ProviderGateway.ContainerCommand
		case "mcp-connector":
			command = readProviderGatewayPackageMetadata(t).Tetral.MCPConnector.ContainerCommand
		}
		requireContains(t, d, "command:\n            - "+strings.Join(command, "\n            - "))
		for _, e := range append([]string{w.httpEnv}, w.requiredEnvVar...) {
			requireContains(t, d, "name: "+e)
		}
	}
}

func TestKubernetesManifestSingleContainerGuardScopesToContainersBlock(t *testing.T) {
	documents := readManifestDocuments(t)
	base := requireDocument(t, documents, "api.yaml", "Deployment", "api")

	if err := validateSingleContainer(base.text); err != nil {
		t.Fatalf("unmutated api Deployment rejected by single-container guard: %v", err)
	}

	// A second container entry inside the containers: block must be rejected.
	secondContainer := "        - name: sidecar\n" +
		"          image: ghcr.io/tetral-ai/tetral:0.0.0-dev\n"
	withSidecar := strings.Replace(
		base.text,
		"      containers:\n        - name: api\n",
		"      containers:\n"+secondContainer+"        - name: api\n",
		1,
	)
	if withSidecar == base.text {
		t.Fatal("guard-of-the-guard setup failed: sidecar injection did not modify the Deployment text")
	}
	if err := validateSingleContainer(withSidecar); err == nil {
		t.Fatal("single-container guard accepted a Deployment with a second container in the containers: block")
	}

	// A volumes: block whose entries share the 8-space "- name:" indentation must pass:
	// this is the red proof for the rewrite — the old count-everything guard saw two
	// "- name:" entries and false-positived; the scoped guard only counts container entries.
	volumesBlock := "      volumes:\n" +
		"        - name: engine-data\n" +
		"          emptyDir: {}\n"
	withVolumes := strings.Replace(
		base.text,
		"      serviceAccountName: api\n",
		"      serviceAccountName: api\n"+volumesBlock,
		1,
	)
	if withVolumes == base.text {
		t.Fatal("guard-of-the-guard setup failed: volumes injection did not modify the Deployment text")
	}
	if err := validateSingleContainer(withVolumes); err != nil {
		t.Fatalf("single-container guard rejected a Deployment whose only extra 8-space \"- name:\" entries are volumes: %v", err)
	}
}

// validateSingleContainer scopes the sidecar count to the containers: block of a Deployment
// pod spec. Counting "        - name: " across the whole document would also match volumes:
// entries, which use the same 8-space indentation; the scoped count is the only correct one.
func validateSingleContainer(text string) error {
	return validateContainerCount(text, 1)
}

func validateContainerCount(text string, want int) error {
	block, err := podSpecBlock(text, "containers:")
	if err != nil {
		return err
	}
	count := strings.Count(block, "\n        - name: ")
	if strings.HasPrefix(block, "        - name: ") {
		// The block extracted by podSpecBlock starts at the first entry line, so the leading
		// entry has no preceding newline to match; count it explicitly.
		count++
	}
	if count != want {
		return fmt.Errorf("has %d containers; want exactly %d", count, want)
	}
	return nil
}

// podSpecBlock returns the lines belonging to a 6-space pod-spec key (for example
// "containers:" or "volumes:"): everything after that key line up to the next sibling key
// at the same 6-space indentation. The returned text starts at the first child line.
func podSpecBlock(text string, key string) (string, error) {
	lines := strings.Split(text, "\n")
	keyLine := "      " + key
	for index, line := range lines {
		if line != keyLine {
			continue
		}
		var block []string
		for _, child := range lines[index+1:] {
			if child == "" {
				block = append(block, child)
				continue
			}
			// A non-empty line that is not indented past the 6-space key marks the next
			// sibling (or a shallower block), ending this block.
			if !strings.HasPrefix(child, "       ") {
				break
			}
			block = append(block, child)
		}
		return strings.Join(block, "\n"), nil
	}
	return "", fmt.Errorf("missing pod-spec key %q", key)
}

func requireDeploymentContainerBlock(t *testing.T, document *manifestDocument, containerName string) string {
	t.Helper()
	containers, err := podSpecBlock(document.text, "containers:")
	if err != nil {
		t.Fatalf("%s %s/%s: %v", document.file, document.kind, document.name, err)
	}
	marker := "        - name: " + containerName
	start := strings.Index(containers, marker)
	if start < 0 {
		t.Fatalf("%s missing container %s", document.file, containerName)
	}
	rest := containers[start:]
	next := strings.Index(rest[len(marker):], "\n        - name: ")
	if next >= 0 {
		return rest[:len(marker)+next]
	}
	return rest
}

func requireContainerResourceBounds(t *testing.T, file string, containerName string, container string) {
	t.Helper()
	for _, required := range []string{
		"resources:",
		"requests:",
		"limits:",
		"cpu:",
		"memory:",
		"ephemeral-storage:",
	} {
		if !strings.Contains(container, required) {
			t.Fatalf("%s container %s missing resource-bound field %q", file, containerName, required)
		}
	}
}

func containerBlocks(t *testing.T, file string, text string) map[string]string {
	t.Helper()
	lines := strings.Split(text, "\n")
	blocks := map[string]string{}
	for index, line := range lines {
		if strings.TrimSpace(line) != "containers:" {
			continue
		}
		containersIndent := leadingSpaces(line)
		entryIndent := containersIndent + 2
		for cursor := index + 1; cursor < len(lines); cursor++ {
			current := lines[cursor]
			if strings.TrimSpace(current) == "" {
				continue
			}
			if leadingSpaces(current) <= containersIndent {
				break
			}
			entryPrefix := strings.Repeat(" ", entryIndent) + "- name: "
			if !strings.HasPrefix(current, entryPrefix) {
				continue
			}
			name := strings.TrimSpace(strings.TrimPrefix(current, entryPrefix))
			end := cursor + 1
			for end < len(lines) {
				next := lines[end]
				if strings.TrimSpace(next) == "" {
					end++
					continue
				}
				if leadingSpaces(next) <= containersIndent {
					break
				}
				if strings.HasPrefix(next, entryPrefix) {
					break
				}
				end++
			}
			blocks[name] = strings.Join(lines[cursor:end], "\n")
			cursor = end - 1
		}
	}
	if len(blocks) == 0 {
		t.Fatalf("%s workload document has no containers block", file)
	}
	return blocks
}

func leadingSpaces(value string) int {
	return len(value) - len(strings.TrimLeft(value, " "))
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

// parseVolumeMounts reads the container volumeMounts list (name + mountPath pairs). It scopes
// to the containers: block so a sibling volumes: entry (same indentation) is never misread.
func parseVolumeMounts(t *testing.T, document *manifestDocument) []volumeMount {
	t.Helper()
	block, err := podSpecBlock(document.text, "containers:")
	if err != nil {
		t.Fatalf("%s %s/%s: %v", document.file, document.kind, document.name, err)
	}
	var mounts []volumeMount
	var current *volumeMount
	inMounts := false
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "volumeMounts:" {
			inMounts = true
			continue
		}
		if !inMounts {
			continue
		}
		// volumeMounts entries are list items under a container key (12-space "- name:").
		if strings.HasPrefix(line, "            - name: ") {
			mounts = append(mounts, volumeMount{name: cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "- name: ")))})
			current = &mounts[len(mounts)-1]
			continue
		}
		if current != nil && strings.HasPrefix(line, "              mountPath: ") {
			current.mountPath = cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "mountPath: ")))
			continue
		}
		// A new container-level key (10-space indentation, not a list item) ends the list.
		if strings.HasPrefix(line, "          ") && !strings.HasPrefix(line, "           ") {
			inMounts = false
			current = nil
		}
	}
	return mounts
}

// parseVolumes reads the pod-spec volumes list into a name -> volume-type map (for example
// "engine-data" -> "emptyDir"). The volume type is the first child key under each entry.
func parseVolumes(t *testing.T, document *manifestDocument) map[string]string {
	t.Helper()
	block, err := podSpecBlock(document.text, "volumes:")
	if err != nil {
		// No volumes block is a valid state for workloads that declare none.
		return map[string]string{}
	}
	volumes := map[string]string{}
	currentName := ""
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(line, "        - name: ") {
			currentName = cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "- name: ")))
			continue
		}
		if currentName == "" {
			continue
		}
		// The volume source is the first 10-space child key after the entry name.
		if strings.HasPrefix(line, "          ") && !strings.HasPrefix(line, "           ") {
			key := strings.TrimSuffix(strings.SplitN(trimmed, ":", 2)[0], ":")
			key = strings.TrimSpace(key)
			if _, recorded := volumes[currentName]; !recorded && key != "" {
				volumes[currentName] = key
			}
		}
	}
	return volumes
}

// TestKubernetesManifestTetralAPIDataDirAlignsWithWritableVolume proves the api
// Deployment gives EnsureDataDir a writable location that survives readOnlyRootFilesystem.
// EnsureDataDir os.MkdirAll's ENGINE_DATA_DIR with 0700 and rejects group/world bits; the
// kubelet owns an emptyDir mount root (0777/0770), so the data dir must be a process-created
// subdirectory of the mount, not the mount root itself.
func TestKubernetesManifestTetralAPIDataDirAlignsWithWritableVolume(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "api.yaml", "Deployment", "api")

	// The read-only-rootfs precondition that motivates the writable volume is pinned here;
	// no other test asserts it.
	requireContains(t, deployment, "readOnlyRootFilesystem: true")

	dataDir, ok := deploymentEnvLiteralValue(deployment.text, "ENGINE_DATA_DIR")
	if !ok {
		requireContains(t, deployment, "name: ENGINE_DATA_DIR\n              valueFrom:\n                configMapKeyRef:\n                  name: api-config\n                  key: ENGINE_DATA_DIR")
		dataDir = requireConfigMapDataValue(t, documents, "api.yaml", "api-config", "ENGINE_DATA_DIR")
	}
	if dataDir == "" {
		t.Fatal("api ENGINE_DATA_DIR has empty configured value")
	}

	mounts := parseVolumeMounts(t, deployment)
	volumes := parseVolumes(t, deployment)

	mount, ok := mountContainingDataDir(dataDir, mounts)
	if !ok {
		t.Fatalf("ENGINE_DATA_DIR %q is not a strict path-boundary subpath of any volumeMount.mountPath %v", dataDir, mountPaths(mounts))
	}

	// guard-of-the-guard: a sibling-prefix value (mountPath+"-extra") shares a string prefix
	// with the mount path but is NOT under the mount; the validator must reject it. A
	// string-prefix match would wrongly accept it.
	siblingPrefix := mount.mountPath + "-extra"
	if _, accepted := mountContainingDataDir(siblingPrefix, mounts); accepted {
		t.Fatalf("path-boundary validator accepted sibling-prefix %q against mount %q", siblingPrefix, mount.mountPath)
	}
	// guard-of-the-guard: equality with the mount root must also fail — the leaf dir must be
	// process-created to satisfy the 0700 owner-only check.
	if _, accepted := mountContainingDataDir(mount.mountPath, mounts); accepted {
		t.Fatalf("path-boundary validator accepted ENGINE_DATA_DIR equal to mount root %q", mount.mountPath)
	}

	volume, ok := volumes[mount.name]
	if !ok {
		t.Fatalf("volumeMount %q has no matching volumes entry; declared volumes: %v", mount.name, volumeNames(volumes))
	}
	if volume != "emptyDir" {
		t.Fatalf("volume %q backing the data dir is %q; want emptyDir", mount.name, volume)
	}
}

func TestKubernetesManifestTetralAPIProvidesBlobStoreConfig(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "api.yaml", "Deployment", "api")
	for _, required := range []string{
		"name: TETRAL_BLOB_ENDPOINT",
		"name: TETRAL_BLOB_REGION",
		"name: TETRAL_BLOB_BUCKET",
		"name: TETRAL_BLOB_ACCESS_KEY",
		"name: TETRAL_BLOB_SECRET_KEY",
		"name: \"tetral-blob\"",
		"key: endpoint",
		"key: region",
		"key: bucket",
		"key: access-key",
		"key: secret-key",
	} {
		requireContains(t, deployment, required)
	}
}

func TestKubernetesManifestServicePortsAndProbePortsMatchWorkloadConfig(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		if isSeparatedWorkload(workload.name) {
			continue
		}
		deployment := requireDocument(t, documents, workload.file, "Deployment", workload.name)
		service := requireDocument(t, documents, workload.file, "Service", workload.name)
		if workload.name == "gateway" {
			requireContains(t, deployment, "containerPort: 8080")
			requireContains(t, deployment, "name: http")
			requireContains(t, service, "name: http\n      port: 8080\n      targetPort: http")
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, "TETRAL_PROVIDER_GATEWAY_HTTP_ADDR", "0.0.0.0:8080")
			requireContains(t, deployment, "containerPort: 9090")
			requireContains(t, deployment, "name: provider-grpc")
			requireContains(t, service, "name: provider-grpc\n      port: 9090\n      targetPort: provider-grpc")
			requireContains(t, deployment, "name: TETRAL_PROVIDER_GATEWAY_GRPC_ADDR\n              value: 0.0.0.0:9090")
			requireContains(t, deployment, "containerPort: 9091")
			requireContains(t, deployment, "name: mcp-grpc")
			requireContains(t, service, "name: mcp-grpc\n      port: 9091\n      targetPort: mcp-grpc")
			requireContains(t, deployment, "name: TETRAL_MCP_CONNECTOR_GRPC_ADDR\n              value: 0.0.0.0:9091")
			continue
		}
		requireContains(t, deployment, "containerPort: 8080")
		requireContains(t, deployment, "name: "+workload.httpPortName)
		requireContains(t, service, "name: "+workload.httpPortName+"\n      port: 8080\n      targetPort: "+workload.httpPortName)
		expectedHTTPAddress := ":8080"
		if workload.name == "agent-runtime" {
			expectedHTTPAddress = "0.0.0.0:8080"
		}
		requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, workload.httpEnv, expectedHTTPAddress)
		if workload.metricsEnv != "" {
			requireContains(t, deployment, "containerPort: 8081")
			requireContains(t, deployment, "name: "+workload.metricsPortName)
			metricsService := service
			if workload.name == "api" || workload.name == "event-stream" || workload.name == "auth" {
				requireNotContains(t, service, "name: "+workload.metricsPortName+"\n      port: 8081\n      targetPort: "+workload.metricsPortName)
				metricsService = requireDocument(t, documents, workload.file, "Service", workload.name+"-metrics")
				requireContains(t, metricsService, "selector:\n    app.kubernetes.io/name: "+workload.name)
				requireNotContains(t, metricsService, "tetral.ai/exposure: public")
			}
			requireContains(t, metricsService, "name: "+workload.metricsPortName+"\n      port: 8081\n      targetPort: "+workload.metricsPortName)
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, workload.metricsEnv, ":8081")
		}
		if workload.internalGRPC {
			port := 9090
			if workload.name == "agent-runtime" {
				port = 19090
			}
			requireContains(t, deployment, fmt.Sprintf("containerPort: %d", port))
			requireContains(t, deployment, "name: "+workload.grpcPortName)
			serviceGRPCPort := "name: " + workload.grpcPortName + "\n      port: 9090\n      targetPort: " + workload.grpcPortName
			if workload.name == "agent-runtime" {
				requireNotContains(t, service, serviceGRPCPort)
			} else {
				requireContains(t, service, serviceGRPCPort)
			}
			expectedGRPCAddress := ":9090"
			if workload.name == "agent-runtime" {
				expectedGRPCAddress = "19090"
			}
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, workload.grpcEnv, expectedGRPCAddress)
		} else if workload.externalCheck {
			requireContains(t, deployment, "containerPort: 9095")
			requireContains(t, service, "name: grpc-check\n      port: 9095\n      targetPort: grpc-check\n      appProtocol: kubernetes.io/h2c")
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, "TETRAL_AUTH_GRPC_ADDR", ":9095")
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, "TETRAL_AUTH_GRPC_TRANSPORT", "plaintext")
			requireNotContains(t, deployment, "containerPort: 9090")
		} else if strings.Contains(service.text, "targetPort: grpc") || strings.Contains(deployment.text, "containerPort: 9090") {
			t.Fatalf("%s exposes gRPC without being declared as an internal gRPC workload", workload.file)
		}
	}
}

func TestKubernetesManifestPublicMetricsPortsAreInternalOnly(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		if workload.metricsEnv == "" {
			continue
		}
		networkPolicy := requireDocument(t, documents, workload.file, "NetworkPolicy", workload.name)
		requireContains(t, networkPolicy, "tetral.ai/network-role: public-ingress")
		requireContains(t, networkPolicy, "port: 8080")
		requireNetworkPolicyIngressEdge(t, networkPolicy, 8081, networkPolicyPeer{namespace: "tetral-system", podPartOf: "tetral"})
		requireNoBroadNetworkPolicyIngressPeers(t, networkPolicy, 8081)
	}
}

func TestKubernetesManifestDeploymentsHaveHTTPHealthAndReadinessProbes(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		deployment := requireDocument(t, documents, workload.file, "Deployment", workload.name)
		requireContains(t, deployment, "livenessProbe:")
		healthPath := "/health"
		readyPath := "/ready"
		if workload.name == "provider-gateway" || workload.name == "mcp-connector" {
			healthPath = "/healthz"
			readyPath = "/readyz"
		}
		requireContains(t, deployment, "path: "+healthPath)
		requireContains(t, deployment, "readinessProbe:")
		requireContains(t, deployment, "path: "+readyPath)
		probePort := workload.httpPortName
		if workload.metricsPortName != "" {
			probePort = workload.metricsPortName
		}
		if workload.name == "git-proxy" {
			probePort = "metrics"
		}
		if strings.Count(deployment.text, "port: "+probePort) < 2 {
			t.Fatalf("%s Deployment probes must both target the HTTP probe port", workload.file)
		}
	}
}

func TestKubernetesManifestEventStreamHasNoBootstrapSecret(t *testing.T) {
	documents := readManifestDocuments(t)
	// Built from fragments so this assertion never self-matches a repo-wide grep for the
	// forbidden Secret name; api_keys has exactly one writer (auth).
	bootstrapSecret := "event-stream" + "-bootstrap"
	deployment := requireDocument(t, documents, "event-stream.yaml", "Deployment", "event-stream")
	for _, forbidden := range []string{bootstrapSecret, "ENGINE_API_KEY"} {
		requireNotContains(t, deployment, forbidden)
	}
	eventStreamDocuments := documents.byFile("event-stream.yaml")
	for index := range eventStreamDocuments {
		requireNotContains(t, &eventStreamDocuments[index], bootstrapSecret)
	}
}

func TestKubernetesManifestPublicAuthBoundaryKeepsRawKeysAtTetralAuth(t *testing.T) {
	documents := readManifestDocuments(t)
	authDeployment := requireDocument(t, documents, "auth.yaml", "Deployment", "auth")
	for _, required := range []string{
		"name: ENGINE_API_KEY",
		"name: ENGINE_BOOTSTRAP_WORKSPACE_ID",
		"name: TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64",
		"name: TETRAL_AUTH_INTERNAL_PRINCIPAL_TTL_SECONDS",
		"name: TETRAL_DATABASE_URL",
		"name: \"auth-bootstrap\"",
		"key: private_key_b64",
	} {
		requireContains(t, authDeployment, required)
	}
	if got := requireConfigMapDataValue(t, documents, "auth.yaml", "auth-config", "ENGINE_BOOTSTRAP_WORKSPACE_ID"); got != "existing-workspace-id" {
		t.Fatalf("auth bootstrap workspace id = %q; want existing-workspace-id", got)
	}

	for _, publicTarget := range []struct {
		file string
		name string
	}{
		{file: "api.yaml", name: "api"},
		{file: "event-stream.yaml", name: "event-stream"},
	} {
		deployment := requireDocument(t, documents, publicTarget.file, "Deployment", publicTarget.name)
		requireContains(t, deployment, "name: TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64")
		requireContains(t, deployment, "key: public_key_b64")
		for _, forbidden := range []string{
			"ENGINE_API_KEY",
			"TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64",
			"auth-bootstrap",
			"api-bootstrap",
			"key: private_key_b64",
		} {
			requireNotContains(t, deployment, forbidden)
		}
	}
}

func TestKubernetesEdgeGatewayEnvoyExternalAuthBoundary(t *testing.T) {
	documents := readEdgeGatewayAdapterDocuments(t, "envoy-gateway.yaml")
	if len(documents) != 12 {
		t.Fatalf("edge object count=%d want12 application-owned resources", len(documents))
	}
	gateway := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "Gateway", "tetral-public-edge")
	for _, required := range []string{`gatewayClassName: "tetral-envoy-gateway"`, `hostname: "api.tetral.example"`, `hostname: "git.tetral.example"`, `name: "tetral-api-public-tls"`, `name: "tetral-git-public-tls"`, "name: http", "name: api-https", "name: git-https"} {
		requireContains(t, gateway, required)
	}
	api := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "HTTPRoute", "tetral-public-api")
	routes := map[string]string{}
	rules := strings.Split(api.text, "    - matches:\n")[1:]
	for _, rule := range rules {
		split := strings.Split(rule, "      backendRefs:\n")
		if len(split) != 2 {
			t.Fatal("route must have one backend list")
		}
		matches := regexp.MustCompile(`(?m)^            value: (.+)$`).FindAllStringSubmatch(split[0], -1)
		backends := regexp.MustCompile(`(?m)^        - name: (.+)$`).FindAllStringSubmatch(split[1], -1)
		if len(backends) != 1 || !strings.Contains(split[1], "          port: 8080") {
			t.Fatal("route requires exactly one owning HTTP backend")
		}
		for _, match := range matches {
			path := cleanManifestListValue(match[1])
			if _, duplicate := routes[path]; duplicate {
				t.Fatal("duplicate route")
			}
			routes[path] = backends[0][1]
		}
	}

	want := map[string]string{`^/v1/sessions/[^/]+/events/stream$`: "event-stream", `^/v1/sessions/[^/]+/threads/[^/]+/stream$`: "event-stream", "/v1/api_keys": "auth", "/v1": "api"}
	if !reflect.DeepEqual(routes, want) {
		t.Fatalf("API backend routes=%v want%v", routes, want)
	}
	exchange := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "HTTPRoute", "tetral-token-exchange")
	for _, required := range []string{"method: POST", "type: Exact", "value: /v1/oauth/token", "name: auth", "port: 8080", "Authorization", "X-Api-Key"} {
		requireContains(t, exchange, required)
	}
	redirect := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "HTTPRoute", "tetral-public-redirect")
	for _, required := range []string{"api.tetral.example", "git.tetral.example", "type: RequestRedirect", "scheme: https", "statusCode: 301"} {
		requireContains(t, redirect, required)
	}
	requireNotContains(t, redirect, "backendRefs")
	policy := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "SecurityPolicy", "tetral-public-api")
	for _, required := range []string{"kind: HTTPRoute", "name: tetral-public-api", "grpc:", "name: auth", "port: 9095", "timeout: 5s", "failOpen: false", "statusOnError: 503", "recomputeRoute: false"} {
		requireContains(t, policy, required)
	}
	for _, forbidden := range []string{"withRequestBody", "retry", "http:"} {
		requireNotContains(t, policy, forbidden)
	}
	proxy := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "EnvoyProxy", "tetral-public-edge")
	requireContains(t, proxy, "accessLog:\n      disable: true")
	git := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "HTTPRoute", "tetral-public-git")
	for _, required := range []string{"git.tetral.example", "name: git-proxy", "port: 8080", "value: /", "Authorization", "X-Api-Key"} {
		requireContains(t, git, required)
	}
	requireNotContains(t, git, "tetral-public-api")
	for _, section := range []string{"http", "api-https", "git-https"} {
		traffic := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "ClientTrafficPolicy", "tetral-public-"+section)
		for _, required := range []string{"sectionName: " + section, "Generate", "x-forwarded-for", "forwarded", "x-real-ip", "x-envoy-external-address", "x-original-", "RejectRequest", "disableMergeSlashes: false", "requestReceivedTimeout: 0s", "streamIdleTimeout: 1800s"} {
			requireContains(t, traffic, required)
		}
		if section == "git-https" {
			requireContains(t, traffic, "git-ticket.+")
		} else {
			requireContains(t, traffic, "x-tetral-")
		}
	}
	backend := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "BackendTrafficPolicy", "tetral-public-edge")
	for _, required := range []string{"requestTimeout: 0s", "maxStreamDuration: 0s", "streamIdleTimeout: 1800s"} {
		requireContains(t, backend, required)
	}
	patch := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "EnvoyPatchPolicy", "tetral-auth-raw-headers")
	for _, required := range []string{"envoy.filters.http.ext_authz/securitypolicy/tetral-system/tetral-public-api", "path: /encode_raw_headers", "path: /validate_mutations", "name: tetral-system/tetral-public-edge/api-https"} {
		requireContains(t, patch, required)
	}
	if strings.Count(patch.text, "value: true") != 2 {
		t.Fatal("raw header patch must set exactly2 reviewed fields")
	}
	for index := range documents {
		for _, forbidden := range []string{"nginx", "/internal/auth/authorize", "Ingress", "withRequestBody", "maxRequestBytes", "buffering"} {
			requireNotContains(t, &documents[index], forbidden)
		}
	}
}

func TestKubernetesManifestTopLevelDeploymentsDeclareResourceBounds(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range []struct {
		file      string
		name      string
		container string
	}{
		{file: "agent-runtime.yaml", name: "agent-runtime", container: "agent-runtime"},
		{file: "bridge.yaml", name: "bridge", container: "bridge-api"},
		{file: "job-runner.yaml", name: "job-runner", container: "job-runner"},
		{file: "event-stream.yaml", name: "event-stream", container: "event-stream"},
		{file: "provider-gateway.yaml", name: "provider-gateway", container: "provider-gateway"},
		{file: "mcp-connector.yaml", name: "mcp-connector", container: "mcp-connector"},
		{file: "web-connector.yaml", name: "web-connector", container: "web-connector"},
		{file: "auth.yaml", name: "auth", container: "auth"},
		{file: "api.yaml", name: "api", container: "api"},
		{file: "git-proxy.yaml", name: "git-proxy", container: "git-proxy"},
		{file: "queue.yaml", name: "queue", container: "queue"},
	} {
		deployment := requireDocument(t, documents, workload.file, "Deployment", workload.name)
		container := requireDeploymentContainerBlock(t, deployment, workload.container)
		requireContainerResourceBounds(t, workload.file, workload.container, container)
	}
}

func TestKubernetesManifestServiceLocalWorkloadsDeclareResourceBounds(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "services", "*", "k8s", "*.yaml"))
	if err != nil {
		t.Fatalf("glob service-local manifests: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("service-local manifests are required")
	}
	for _, path := range paths {
		// #nosec G304 -- path comes from a repository-local glob rooted in services/*/k8s.
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, rawDocument := range strings.Split(string(source), "\n---") {
			text := strings.TrimSpace(rawDocument)
			if text == "" {
				continue
			}
			kind := requireScalar(t, path, text, "kind")
			switch kind {
			case "Deployment", "CronJob":
				for containerName, block := range containerBlocks(t, path, text) {
					requireContainerResourceBounds(t, path, containerName, block)
				}
			}
		}
	}
}

func TestKubernetesManifestNoDefaultTokenServiceAccountsDisableAutomount(t *testing.T) {
	for _, serviceName := range []string{
		"bridge",
		"agent-runtime",
		"event-stream",
		"provider-gateway", "mcp-connector", "web-connector", "job-runner",
		"api",
		"auth",
		"cleanup",
		"git-proxy",
		"queue",
		"sandbox",
	} {
		path := filepath.Join("..", "..", "services", serviceName, "k8s", "serviceaccount.yaml")
		if serviceName == "provider-gateway" || serviceName == "mcp-connector" {
			path = filepath.Join("..", "..", "services", "gateway", "k8s", serviceName, "serviceaccount.yaml")
		}
		source := string(mustReadFile(t, path))
		requireContains(t, &manifestDocument{file: path, kind: "ServiceAccount", name: serviceName, text: source}, "automountServiceAccountToken: false")
	}
}

func TestKubernetesManifestEventStreamAndCleanupRestrictEgress(t *testing.T) {
	for _, test := range []struct {
		name          string
		path          string
		allowsIngress bool
	}{
		{name: "event-stream", path: filepath.Join("..", "..", "services", "event-stream", "k8s", "networkpolicy.yaml"), allowsIngress: true},
		{name: "cleanup", path: filepath.Join("..", "..", "services", "cleanup", "k8s", "networkpolicy.yaml")},
	} {
		source := string(mustReadFile(t, test.path))
		policy := &manifestDocument{file: test.path, kind: "NetworkPolicy", name: test.name, text: source}
		requireContains(t, policy, "kind: NetworkPolicy")
		requireContains(t, policy, "name: "+test.name)
		requireContains(t, policy, "podSelector:\n    matchLabels:\n      app.kubernetes.io/name: "+test.name)
		requireContains(t, policy, "- Egress")
		requireContains(t, policy, "app.kubernetes.io/name: tetral-postgres")
		requireContains(t, policy, "port: 5432")
		requireContains(t, policy, "kubernetes.io/metadata.name: kube-system")
		requireContains(t, policy, "port: 53")
		if test.allowsIngress {
			requireContains(t, policy, "- Ingress")
			requireContains(t, policy, "tetral.ai/network-role: public-ingress")
			requireContains(t, policy, "port: 8080")
		} else {
			requireNotContains(t, policy, "- Ingress")
			requireNotContains(t, policy, "tetral.ai/network-role: public-ingress")
			requireNotContains(t, policy, "port: 8080")
		}
		for _, forbidden := range []string{
			"bridge",
			"agent-runtime",
			"gateway",
			"sandbox",
			"DAYTONA",
			"daytona",
		} {
			requireNotContains(t, policy, forbidden)
		}
	}

	documents := readManifestDocuments(t)
	eventStream := requireDocument(t, documents, "event-stream.yaml", "NetworkPolicy", "event-stream")
	for _, required := range []string{
		"- Ingress",
		"- Egress",
		"tetral.ai/network-role: public-ingress",
		"app.kubernetes.io/name: tetral-postgres",
		"port: 5432",
		"kubernetes.io/metadata.name: kube-system",
		"port: 53",
	} {
		requireContains(t, eventStream, required)
	}
	for _, forbidden := range []string{
		"bridge",
		"agent-runtime",
		"gateway",
		"sandbox",
		"DAYTONA",
		"daytona",
	} {
		requireNotContains(t, eventStream, forbidden)
	}
}

// Every workload that uses plain NetworkPolicy DNS resolves names through the
// same peer, and the shape has to be the same everywhere. This census covers
// all ten policies, including git-proxy's default-off FQDN-policy posture.
func TestKubernetesManifestDNSEgressIsUniform(t *testing.T) {
	documents := readManifestDocuments(t)

	for _, policy := range []struct{ file, name string }{
		{"agent-runtime.yaml", "agent-runtime"},
		{"api.yaml", "api"},
		{"auth.yaml", "auth"},
		{"bridge.yaml", "bridge"},
		{"cleanup.yaml", "cleanup"},
		{"event-stream.yaml", "event-stream"},
		{"provider-gateway.yaml", "provider-gateway"},
		{"mcp-connector.yaml", "mcp-connector"},
		{"web-connector.yaml", "web-connector"},
		{"job-runner.yaml", "job-runner"},
		{"git-proxy.yaml", "git-proxy"},
		{"queue.yaml", "queue"},
		{"sandbox.yaml", "sandbox"},
	} {
		requireKubeDNSEgress(t, requireDocument(t, documents, policy.file, "NetworkPolicy", policy.name))
	}
}

func TestNetworkPolicyRuleShapeRejectsUnsupportedPortForms(t *testing.T) {
	cases := map[string][]string{
		"port-first": {
			"    - to:",
			"        - ipBlock:",
			"            cidr: 192.0.2.0/24",
			"      ports:",
			"        - port: 53",
			"          protocol: UDP",
		},
		"flow-style": {
			"    - to: [{ipBlock: {cidr: 192.0.2.0/24}}]",
			"      ports: [{port: 53, protocol: UDP}]",
		},
		"port-range": {
			"    - to:",
			"        - ipBlock:",
			"            cidr: 192.0.2.0/24",
			"      ports:",
			"        - protocol: UDP",
			"          port: 52",
			"          endPort: 53",
		},
	}
	for name, lines := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateRecognizedNetworkPolicyRuleShape(lines); err == nil {
				t.Fatal("unsupported NetworkPolicy rule shape was accepted")
			}
		})
	}
}

func TestKubernetesManifestCoreControlPlaneUsesPinnedEgress(t *testing.T) {
	documents := readManifestDocuments(t)

	auth := requireDocument(t, documents, "auth.yaml", "NetworkPolicy", "auth")
	requireContains(t, auth, "policyTypes:\n    - Ingress\n    - Egress")
	requireNetworkPolicyEgressEdge(t, auth, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNoBroadNetworkPolicyEgressPeers(t, auth, 5432)
	requireKubeDNSEgress(t, auth)
	for _, forbidden := range []string{"agent-runtime", "gateway", "queue", "sandbox", "DAYTONA", "daytona", "0.0.0.0/0"} {
		requireNotContains(t, auth, forbidden)
	}

	api := requireDocument(t, documents, "api.yaml", "NetworkPolicy", "api")
	requireContains(t, api, "policyTypes:\n    - Ingress\n    - Egress")
	requireContains(t, api, `tetral.ai/egress-intent: "blob.example.internal"`)
	requireNetworkPolicyEgressEdge(t, api, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNetworkPolicyEgressEdge(t, api, 8080, networkPolicyPeer{namespace: "tetral-system", podName: "queue"})
	requireNetworkPolicyEgressEdge(t, api, 9090, networkPolicyPeer{namespace: "tetral-system", podName: "queue"})
	requireNoBroadNetworkPolicyEgressPeers(t, api, 5432)
	requireNoBroadNetworkPolicyEgressPeers(t, api, 8080)
	requireNoBroadNetworkPolicyEgressPeers(t, api, 9090)
	requireNetworkPolicyEgressIPBlock(t, api, 443, "0.0.0.0/0")
	requireKubeDNSEgress(t, api)
	for _, forbidden := range []string{"agent-runtime", "bridge", "gateway", "sandbox", "DAYTONA", "daytona"} {
		requireNotContains(t, api, forbidden)
	}

	queue := requireDocument(t, documents, "queue.yaml", "NetworkPolicy", "queue")
	requireContains(t, queue, "policyTypes:\n    - Ingress\n    - Egress")
	requireNetworkPolicyEgressEdge(t, queue, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNoBroadNetworkPolicyEgressPeers(t, queue, 5432)
	requireKubeDNSEgress(t, queue)
	for _, forbidden := range []string{"agent-runtime", "gateway", "DAYTONA", "daytona", "0.0.0.0/0", "kubernetes.default.svc"} {
		requireNotContains(t, queue, forbidden)
	}

	bridge := requireDocument(t, documents, "bridge.yaml", "NetworkPolicy", "bridge")
	requireContains(t, bridge, "policyTypes:\n    - Ingress\n    - Egress")
	requireContains(t, bridge, `tetral.ai/egress-intent: "blob.example.internal"`)
	requireNetworkPolicyEgressEdge(t, bridge, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNetworkPolicyEgressEdge(t, bridge, 9091, networkPolicyPeer{namespace: "tetral-system", podName: "mcp-connector"})
	requireNetworkPolicyEgressIPBlock(t, bridge, 443, "10.96.0.1/32")
	requireNetworkPolicyEgressIPBlock(t, bridge, 443, "0.0.0.0/0")
	requireNoBroadNetworkPolicyEgressPeers(t, bridge, 5432)
	requireNoBroadNetworkPolicyEgressPeers(t, bridge, 9090)
	requireNoBroadNetworkPolicyEgressPeers(t, bridge, 9091)
	requireKubeDNSEgress(t, bridge)

	sandbox := requireDocument(t, documents, "sandbox.yaml", "NetworkPolicy", "sandbox")
	requireContains(t, sandbox, `tetral.ai/egress-intent: "daytona.example.internal, blob.example.internal, api.cloudflare.com"`)
	requireNetworkPolicyEgressEdge(t, sandbox, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNetworkPolicyEgressEdge(t, sandbox, 9090, networkPolicyPeer{namespace: "tetral-system", podName: "queue"})
	requireNetworkPolicyEgressIPBlock(t, sandbox, 443, "0.0.0.0/0")
	requireNoBroadNetworkPolicyEgressPeers(t, sandbox, 80)
	requireKubeDNSEgress(t, sandbox)
}

func TestKubernetesManifestBroadHTTPSEgressCarriesHostIntent(t *testing.T) {
	documents := readManifestDocuments(t)
	for index := range documents {
		document := &documents[index]
		if document.kind != "NetworkPolicy" || !networkPolicyHasEgressIPBlock(t, document, 443, "0.0.0.0/0") {
			continue
		}
		hosts := requireNetworkPolicyEgressIntentHosts(t, document)
		for _, host := range hosts {
			if strings.ContainsAny(host, " \t\n") || !strings.Contains(host, ".") {
				t.Fatalf("%s %s/%s broad HTTPS egress intent host %q is not an explicit host", document.file, document.kind, document.name, host)
			}
		}
		if networkPolicyHasEgressIPBlock(t, document, 80, "0.0.0.0/0") {
			t.Fatalf("%s %s/%s has broad cleartext port-80 egress", document.file, document.kind, document.name)
		}
	}
}

func TestKubernetesManifestServiceLocalSecretExamples(t *testing.T) {
	for _, test := range []struct {
		service   string
		required  []string
		forbidden []string
	}{
		{
			service: "auth",
			required: []string{
				"name: auth-bootstrap",
				"name: auth-internal-principal",
				"name: auth-database",
				"engine-api-key:",
				"private_key_b64:",
				"public_key_b64:",
			},
		},
		{
			service: "api",
			required: []string{
				"name: api-secrets",
				"name: api-database",
				"engine-vault-key:",
				"public_key_b64",
			},
			forbidden: []string{
				"engine-api-key:",
				"private_key_b64:",
			},
		},
		{
			service: "queue",
			required: []string{
				"name: queue-database",
				"url:",
			},
			forbidden: []string{
				"engine-api-key:",
				"engine-vault-key:",
				"private_key_b64:",
			},
		},
	} {
		body, err := os.ReadFile(filepath.Join("..", "..", "services", test.service, "k8s", "secret.example.yaml")) //nolint:gosec // repository-local manifest path.
		if err != nil {
			t.Fatalf("%s service-local secret.example.yaml is required: %v", test.service, err)
		}
		text := string(body)
		for _, required := range test.required {
			if !strings.Contains(text, required) {
				t.Fatalf("%s secret.example.yaml missing %q", test.service, required)
			}
		}
		for _, forbidden := range test.forbidden {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s secret.example.yaml contains forbidden auth material %q", test.service, forbidden)
			}
		}
	}
}

// The bootstrap Secret table is the operator inventory and the Sandbox example
// carries the tetral-database keys. Every key a canonical workload reads through
// secretKeyRef must appear in both, so a new per-workload key cannot ship
// undocumented. Edge TLS Secrets are selected by values and are not referenced
// by the default manifests.
func TestBootstrapSecretTableCoversManifestSecretKeyReferences(t *testing.T) {
	reference := regexp.MustCompile(`secretKeyRef:\n +name: ([^\n]+)\n +key: ([^\n]+)`)
	referenced := map[string]map[string]bool{}
	for _, document := range readManifestDocuments(t) {
		for _, match := range reference.FindAllStringSubmatch(document.text, -1) {
			name, key := cleanManifestListValue(strings.TrimSpace(match[1])), cleanManifestListValue(strings.TrimSpace(match[2]))
			if referenced[name] == nil {
				referenced[name] = map[string]bool{}
			}
			referenced[name][key] = true
		}
	}
	if len(referenced["tetral-database"]) == 0 {
		t.Fatal("canonical manifests reference no tetral-database keys")
	}

	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "bootstrap.md"))
	if err != nil {
		t.Fatal(err)
	}
	documented := map[string]map[string]bool{}
	inTable := false
	for _, line := range strings.Split(string(body), "\n") {
		switch {
		case line == "| Secret | Required keys |":
			inTable = true
		case !inTable || strings.HasPrefix(line, "| ---"):
		case strings.HasPrefix(line, "| `"):
			cells := strings.Split(line, "|")
			if len(cells) != 4 {
				t.Fatalf("malformed bootstrap Secret row %q", line)
			}
			keys := map[string]bool{}
			for _, key := range strings.Split(cells[2], ",") {
				keys[strings.Trim(strings.TrimSpace(key), "`")] = true
			}
			documented[strings.Trim(strings.TrimSpace(cells[1]), "`")] = keys
		default:
			inTable = false
		}
	}
	for name, keys := range referenced {
		for key := range keys {
			if !documented[name][key] {
				t.Errorf("docs/bootstrap.md Secret table omits %s/%s read by a canonical manifest", name, key)
			}
		}
	}

	example, err := os.ReadFile(filepath.Join("..", "..", "services", "sandbox", "k8s", "secret.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	exampleKeys := map[string]bool{}
	for _, document := range strings.Split(string(example), "\n---\n") {
		if !strings.Contains(document, "\n  name: tetral-database\n") {
			continue
		}
		_, stringData, found := strings.Cut(document, "\nstringData:\n")
		if !found {
			t.Fatal("tetral-database example has no stringData")
		}
		for _, match := range regexp.MustCompile(`(?m)^  ([A-Za-z0-9_-]+):`).FindAllStringSubmatch(stringData, -1) {
			exampleKeys[match[1]] = true
		}
	}
	for key := range referenced["tetral-database"] {
		if !exampleKeys[key] {
			t.Errorf("services/sandbox/k8s/secret.example.yaml omits tetral-database/%s", key)
		}
	}
}

func TestKubernetesManifestTetralAPIHasNoLegacyRuntimeClientConfigOrKubernetesToken(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "api.yaml", "Deployment", "api")
	requireContains(t, deployment, "automountServiceAccountToken: false")
	oldRuntimeClient := "TETRAL_SESSION_" + "DISPATCHER"
	oldRuntimeWorkload := "session-" + "dispatcher"
	for _, forbidden := range []string{
		oldRuntimeClient,
		oldRuntimeWorkload + "-internal-grpc-token",
		"serviceAccountToken:",
		"tetral-internal-grpc/" + oldRuntimeWorkload,
	} {
		requireNotContains(t, deployment, forbidden)
	}
}

func TestKubernetesManifestTetralQueueHasNoKubernetesAPITokenOrInternalAuthConfig(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "queue.yaml", "Deployment", "queue")
	requireContains(t, deployment, "automountServiceAccountToken: false")
	for _, forbidden := range []string{
		"serviceAccountToken:",
		"TETRAL_INTERNAL_GRPC_AUDIENCE",
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
		"kubernetes.default.svc",
	} {
		requireNotContains(t, deployment, forbidden)
	}
}

func TestKubernetesManifestGitProxyPinsGitHubEgressAndSecrets(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "git-proxy.yaml", "Deployment", "git-proxy")
	service := requireDocument(t, documents, "git-proxy.yaml", "Service", "git-proxy")
	networkPolicy := requireDocument(t, documents, "git-proxy.yaml", "NetworkPolicy", "git-proxy")
	hpa := requireDocument(t, documents, "git-proxy.yaml", "HorizontalPodAutoscaler", "git-proxy")
	for index := range documents {
		document := &documents[index]
		if document.file == "git-proxy.yaml" && document.kind == "CiliumNetworkPolicy" {
			t.Fatalf("git-proxy.yaml unexpectedly contains CiliumNetworkPolicy %s", document.name)
		}
	}

	requireContains(t, deployment, "automountServiceAccountToken: false")
	requireContains(t, deployment, "replicas: 2")
	requireContains(t, deployment, "terminationGracePeriodSeconds: 1805")
	for envName, want := range map[string]string{ // #nosec G101 -- Kubernetes fixture env values, not credentials.
		"TETRAL_GIT_PROXY_HTTP_ADDR":           ":8080",
		"TETRAL_GIT_PROXY_METRICS_ADDR":        ":8081",
		"TETRAL_GIT_PROXY_PUBLIC_BASE_URL":     "https://git.tetral.example",
		"TETRAL_GIT_PROXY_DRAIN_GRACE_SECONDS": "1800",
		"TETRAL_DEPLOYMENT_ENVIRONMENT":        "local",
		"TETRAL_SERVICE_VERSION":               "dev",
	} {
		if actual := requireDeploymentEnvValue(t, deployment, envName); actual != want {
			t.Fatalf("%s value = %q; want %q", envName, actual, want)
		}
	}
	requireContains(t, deployment, "name: metrics\n              containerPort: 8081")
	requireContains(t, service, "name: http\n      port: 8080\n      targetPort: http")
	requireContains(t, service, "name: metrics\n      port: 8081\n      targetPort: metrics")
	requireContains(t, deployment, "name: TETRAL_DATABASE_URL")
	requireContains(t, deployment, "key: git-proxy-url")
	requireContains(t, deployment, "name: ENGINE_VAULT_KEY")
	requireContains(t, deployment, "key: engine-vault-key")
	for _, forbidden := range []string{
		"serviceAccountToken:",
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
		"TETRAL_INTERNAL_GRPC_AUDIENCE",
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
		// Git Proxy reads its ticket only from X-Tetral-Git-Ticket; no setting
		// enables a URL-borne ticket.
		"TETRAL_GIT_PROXY_LEGACY_PATH_CUTOVER",
	} {
		requireNotContains(t, deployment, forbidden)
	}
	requireContains(t, networkPolicy, `tetral.ai/egress-intent: "github.com"`)
	requireContains(t, networkPolicy, "policyTypes:\n    - Ingress\n    - Egress")
	requireContains(t, networkPolicy, "tetral.ai/network-role: public-ingress")
	requireNetworkPolicyIngressEdge(t, networkPolicy, 8081, networkPolicyPeer{namespace: "tetral-system", podPartOf: "tetral"})
	requireNoBroadNetworkPolicyIngressPeers(t, networkPolicy, 8081)
	egressRules := parseNetworkPolicyEgressRules(t, networkPolicy)
	wantEgressRules := []networkPolicyRule{
		{
			ports:      []int{5432},
			transports: []networkPolicyTransport{{protocol: "TCP", port: 5432}},
			peers: []networkPolicyPeer{{
				namespace:       "tetral-system",
				podName:         "tetral-postgres",
				namespaceLabels: map[string]string{},
				podLabels:       map[string]string{"app.kubernetes.io/name": "tetral-postgres"},
			}},
		},
		{
			ports: []int{53, 53},
			transports: []networkPolicyTransport{
				{protocol: "UDP", port: 53},
				{protocol: "TCP", port: 53},
			},
			peers: []networkPolicyPeer{{
				namespace:       "kube-system",
				namespaceLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"},
				podLabels:       map[string]string{"k8s-app": "kube-dns"},
			}},
		},
		{
			ports:      []int{443},
			transports: []networkPolicyTransport{{protocol: "TCP", port: 443}},
			ipBlocks:   []string{"0.0.0.0/0"},
		},
	}
	if !reflect.DeepEqual(egressRules, wantEgressRules) {
		t.Fatalf("git-proxy NetworkPolicy egress differs from the exact database, DNS, and external-HTTPS shape:\ngot:  %#v\nwant: %#v", egressRules, wantEgressRules)
	}
	requireContains(t, hpa, "apiVersion: autoscaling/v2")
	requireContains(t, hpa, "scaleTargetRef:\n    apiVersion: apps/v1\n    kind: Deployment\n    name: git-proxy")
	requireContains(t, hpa, "minReplicas: 2")
	requireContains(t, hpa, "maxReplicas: 10")
	requireContains(t, hpa, `tetral.ai/hpa-metric-source: "prometheus-adapter: gitproxy_active_connections and rate(gitproxy_bytes_relayed_total[2m])"`)
	requireContains(t, hpa, "name: gitproxy_active_connections")
	requireContains(t, hpa, "name: gitproxy_bytes_relayed_per_second")
	requireContains(t, hpa, "direction: in")
	requireContains(t, hpa, "direction: out")
	requireContains(t, hpa, "averageValue: \"100\"")
	requireContains(t, hpa, "averageValue: 20Mi")
	requireNotContains(t, hpa, "type: Resource")
	requireNotContains(t, hpa, "name: cpu")
	requireNotContains(t, hpa, "averageUtilization:")
}

func TestKubernetesManifestEngineVaultKeyMountsExactAllowList(t *testing.T) {
	documents := readManifestDocuments(t)
	allowed := map[string]struct {
		name  string
		count int
	}{
		"api.yaml":              {name: "api", count: 1},
		"provider-gateway.yaml": {name: "provider-gateway", count: 1},
		"mcp-connector.yaml":    {name: "mcp-connector", count: 1},
		"git-proxy.yaml":        {name: "git-proxy", count: 1},
	}
	for file, expectation := range allowed {
		deployment := requireDocument(t, documents, file, "Deployment", expectation.name)
		requireEngineVaultKeyEnvCount(t, deployment.file, deployment.text, expectation.count)
	}
	for _, denied := range []struct {
		file string
		name string
	}{
		{file: "auth.yaml", name: "auth"},
		{file: "queue.yaml", name: "queue"},
		{file: "sandbox.yaml", name: "sandbox"},
		{file: "event-stream.yaml", name: "event-stream"},
		{file: "bridge.yaml", name: "bridge"},
		{file: "agent-runtime.yaml", name: "agent-runtime"},
	} {
		deployment := requireDocument(t, documents, denied.file, "Deployment", denied.name)
		requireNotContains(t, deployment, "name: ENGINE_VAULT_KEY")
	}
	cleanup := requireDocument(t, documents, "cleanup.yaml", "CronJob", "cleanup")
	requireNotContains(t, cleanup, "name: ENGINE_VAULT_KEY")

	serviceLocalAllowed := map[string]int{
		filepath.Join("api", "k8s", "deployment.yaml"):                         1,
		filepath.Join("gateway", "k8s", "provider-gateway", "deployment.yaml"): 1,
		filepath.Join("gateway", "k8s", "mcp-connector", "deployment.yaml"):    1,
		filepath.Join("git-proxy", "k8s", "deployment.yaml"):                   1,
	}
	for path, count := range serviceLocalAllowed {
		text := readServiceLocalManifestText(t, path)
		requireEngineVaultKeyEnvCount(t, path, text, count)
	}
	for _, path := range []string{
		filepath.Join("auth", "k8s", "deployment.yaml"),
		filepath.Join("queue", "k8s", "deployment.yaml"),
		filepath.Join("sandbox", "k8s", "deployment.yaml"),
		filepath.Join("cleanup", "k8s", "cronjob.yaml"),
		filepath.Join("event-stream", "k8s", "deployment.yaml"),
		filepath.Join("bridge", "k8s", "deployment.yaml"),
		filepath.Join("agent-runtime", "k8s", "deployment.yaml"),
	} {
		text := readServiceLocalManifestText(t, path)
		if strings.Contains(text, "name: ENGINE_VAULT_KEY") {
			t.Fatalf("service-local manifest %s mounts forbidden ENGINE_VAULT_KEY", path)
		}
	}
}

func TestKubernetesManifestAgentRuntimeRuntimePodConfig(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "agent-runtime.yaml", "Deployment", "agent-runtime")

	for envName, fieldPath := range map[string]string{
		"TETRAL_RUNTIME_POD_NAMESPACE": "metadata.namespace",
		"TETRAL_RUNTIME_POD_NAME":      "metadata.name",
		"TETRAL_RUNTIME_POD_UID":       "metadata.uid",
		"TETRAL_RUNTIME_POD_IP":        "status.podIP",
	} {
		if actual := requireDeploymentEnvFieldPath(t, deployment, envName); actual != fieldPath {
			t.Fatalf("%s fieldPath = %q; want %q", envName, actual, fieldPath)
		}
	}
	for envName, want := range map[string]string{ // #nosec G101 -- Kubernetes fixture env values, not credentials.
		"TETRAL_RUNTIME_POD_HTTP_ADDR":                           "0.0.0.0:8080",
		"TETRAL_RUNTIME_POD_GRPC_PORT":                           "19090",
		"TETRAL_DEPLOYMENT_ENVIRONMENT":                          "local",
		"TETRAL_SERVICE_VERSION":                                 "dev",
		"TETRAL_RUNTIME_POD_GRPC_AUDIENCE":                       "tetral-internal-grpc",
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS":               "tetral-system/job-runner",
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH":            "/var/run/secrets/tetral-kubernetes-api/runtime-pod-tokenreview/token",
		"KUBERNETES_API_SERVER_URL":                              "https://kubernetes.default.svc",
		"KUBERNETES_API_CA_CERT_PATH":                            "/var/run/secrets/tetral-kubernetes-api/ca.crt",
		"TETRAL_BRIDGE_API_GRPC_ADDR":                            "bridge.tetral-system.svc.cluster.local:9090",
		"TETRAL_GATEWAY_GRPC_ADDR":                               "dns:///provider-gateway.tetral-system.svc.cluster.local:9090",
		"TETRAL_MCP_CONNECTOR_GRPC_ADDR":                         "dns:///mcp-connector.tetral-system.svc.cluster.local:9091",
		"TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES": "32768",
		"TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS":              "1800000",
	} {
		if actual := requireDeploymentEnvValue(t, deployment, envName); actual != want {
			t.Fatalf("%s value = %q; want %q", envName, actual, want)
		}
	}
	// The reviewer model is an operator cost decision, not a system invariant:
	// pinning which model runs here would turn a manifest edit into a test edit.
	// What must hold is that the workload receives a well-formed provider/model
	// reference, because it refuses to start without one.
	reviewerModel := requireDeploymentEnvValue(t, deployment, "TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL")
	if !regexp.MustCompile(`^[a-z0-9-]+/[a-zA-Z0-9._-]+$`).MatchString(reviewerModel) {
		t.Fatalf("approval reviewer model = %q; want a provider/model reference", reviewerModel)
	}
	requireContains(t, deployment, "automountServiceAccountToken: false")
	requireProjectedBoundedServiceAccountToken(t, deployment, projectedTokenExpectation{
		envName:           "TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH",
		volume:            "runtime-pod-internal-grpc-token",
		mountPath:         "/var/run/secrets/tetral-internal-grpc/runtime-pod",
		filePath:          "token",
		audienceMode:      projectedTokenWithAudience,
		audience:          "tetral-internal-grpc",
		expirationSeconds: 600,
	})
	requireProjectedBoundedServiceAccountToken(t, deployment, projectedTokenExpectation{
		envName:           "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
		volume:            "runtime-pod-kubernetes-api",
		mountPath:         "/var/run/secrets/tetral-kubernetes-api",
		filePath:          "runtime-pod-tokenreview/token",
		audienceMode:      projectedTokenNoAudience,
		expirationSeconds: 600,
	})
	requireContains(t, deployment, "- configMap:\n                  name: kube-root-ca.crt\n                  items:\n                    - key: ca.crt\n                      path: ca.crt")
	if outbound := requireDeploymentEnvValue(t, deployment, "TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH"); outbound == requireDeploymentEnvValue(t, deployment, "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH") {
		t.Fatalf("Runtime Pod outbound internal gRPC token path and TokenReview reviewer token path are both %q; want separate paths", outbound)
	}

	configSource, err := os.ReadFile(filepath.Join("..", "..", "services", "agent-runtime", "packages", "runtime-pod", "src", "config.ts"))
	if err != nil {
		t.Fatalf("read runtime pod config source: %v", err)
	}
	for _, envName := range []string{
		"TETRAL_RUNTIME_POD_NAMESPACE",
		"TETRAL_RUNTIME_POD_NAME",
		"TETRAL_RUNTIME_POD_UID",
		"TETRAL_RUNTIME_POD_IP",
		"TETRAL_RUNTIME_POD_HTTP_ADDR",
		"TETRAL_RUNTIME_POD_GRPC_PORT",
		"TETRAL_RUNTIME_POD_GRPC_AUDIENCE",
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS",
		"TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH",
		"TETRAL_BRIDGE_API_GRPC_ADDR",
		"TETRAL_GATEWAY_GRPC_ADDR",
		"TETRAL_MCP_CONNECTOR_GRPC_ADDR",
		"TETRAL_WEB_CONNECTOR_GRPC_ADDR",
		"TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL",
		"TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES",
		"TETRAL_RUNTIME_PROVIDER_STREAM_TIMEOUT_MS",
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
		"KUBERNETES_API_CA_CERT_PATH",
	} {
		if !strings.Contains(string(configSource), envName) {
			t.Fatalf("runtime pod config source does not contain manifest env %s", envName)
		}
	}
}

func TestKubernetesManifestAgentRuntimePodEgressIsBounded(t *testing.T) {
	documents := readManifestDocuments(t)
	networkPolicy := requireDocument(t, documents, "agent-runtime.yaml", "NetworkPolicy", "agent-runtime")

	requireContains(t, networkPolicy, "policyTypes:\n    - Ingress\n    - Egress")
	requireNetworkPolicyEgressEdge(t, networkPolicy, 9090, networkPolicyPeer{namespace: "tetral-system", podName: "bridge"})
	requireNetworkPolicyEgressEdge(t, networkPolicy, 9090, networkPolicyPeer{namespace: "tetral-system", podName: "provider-gateway"})
	requireNetworkPolicyEgressEdge(t, networkPolicy, 9091, networkPolicyPeer{namespace: "tetral-system", podName: "mcp-connector"})
	requireNetworkPolicyEgressEdge(t, networkPolicy, 9092, networkPolicyPeer{namespace: "tetral-system", podName: "web-connector"})
	requireNetworkPolicyEgressIPBlock(t, networkPolicy, 443, "10.96.0.1/32")
	requireNoBroadNetworkPolicyEgressPeers(t, networkPolicy, 9090)
	requireNoBroadNetworkPolicyEgressPeers(t, networkPolicy, 9091)
	requireNoBroadNetworkPolicyEgressPeers(t, networkPolicy, 9092)
	requireNoBroadNetworkPolicyEgressPeers(t, networkPolicy, 443)
	requireContains(t, networkPolicy, "kubernetes.io/metadata.name: kube-system")
	requireContains(t, networkPolicy, "k8s-app: kube-dns")
	requireContains(t, networkPolicy, "protocol: UDP\n          port: 53")
	requireContains(t, networkPolicy, "protocol: TCP\n          port: 53")
	for _, forbidden := range []string{
		"TETRAL_DATABASE_URL",
		"postgres",
		"Postgres",
		"DAYTONA",
		"daytona",
		"TETRAL_BLOB",
		"queue",
		"public-ingress",
		"0.0.0.0/0",
	} {
		requireNotContains(t, networkPolicy, forbidden)
	}
}

func TestKubernetesManifestAgentRuntimeBuildArtifactMapping(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "agent-runtime.yaml", "Deployment", "agent-runtime")
	metadata := readAgentRuntimePackageMetadata(t)
	dockerfile := readAgentRuntimeDockerfile(t)

	if metadata.Tetral.AgentRuntimePod.Entrypoint != "packages/runtime-pod/src/command.ts" {
		t.Fatalf("agent runtime package entrypoint = %q", metadata.Tetral.AgentRuntimePod.Entrypoint)
	}
	if metadata.Tetral.AgentRuntimePod.BuildArtifact != "dist/command.js" {
		t.Fatalf("agent runtime package build artifact = %q", metadata.Tetral.AgentRuntimePod.BuildArtifact)
	}
	expectedCommand := []string{"bun", "/app/" + metadata.Tetral.AgentRuntimePod.BuildArtifact}
	if !reflect.DeepEqual(metadata.Tetral.AgentRuntimePod.ContainerCommand, expectedCommand) {
		t.Fatalf("agent runtime package container command = %#v", metadata.Tetral.AgentRuntimePod.ContainerCommand)
	}
	staleNestedBuildArtifact := "dist/packages/runtime-pod/src/" + "command.js"
	if strings.Contains(metadata.Tetral.AgentRuntimePod.BuildArtifact, staleNestedBuildArtifact) {
		t.Fatal("agent runtime package build artifact still uses stale nested Runtime Pod path")
	}
	if !strings.Contains(metadata.Scripts["build"], metadata.Tetral.AgentRuntimePod.Entrypoint) {
		t.Fatalf("build script does not include runtime pod entrypoint %q", metadata.Tetral.AgentRuntimePod.Entrypoint)
	}
	if metadata.Tetral.AgentRuntimePod.Image != "ghcr.io/tetral-ai/agent-runtime:0.0.0-dev" {
		t.Fatalf("agent runtime package image = %q", metadata.Tetral.AgentRuntimePod.Image)
	}
	for _, fragment := range []string{
		// The build context is the repository root: this service's package
		// manifests carry file: dependencies reaching into services/gateway
		// and internal/ts-observability, so a narrower context cannot install.
		"COPY services/agent-runtime/packages ./packages",
		"COPY services/gateway/packages/protocol",
		"COPY internal/ts-observability",
		"bun run build",
		"CMD [\"bun\", \"/app/" + metadata.Tetral.AgentRuntimePod.BuildArtifact + "\"]",
		"USER 1000:1000",
	} {
		if !strings.Contains(dockerfile, fragment) {
			t.Fatalf("Dockerfile missing %q", fragment)
		}
	}
	requireDeploymentContainerImage(t, deployment, metadata.Tetral.AgentRuntimePod.Image)
	requireContains(t, deployment, "runAsUser: 1000")
	requireContains(t, deployment, "runAsGroup: 1000")
	requireContains(t, deployment, "command:\n            - bun\n            - /app/"+metadata.Tetral.AgentRuntimePod.BuildArtifact)
	staleNestedCommandPath := "/app/" + staleNestedBuildArtifact
	for _, source := range []struct {
		name string
		text string
	}{
		{name: "package metadata", text: string(mustReadFile(t, filepath.Join("..", "..", "services", "agent-runtime", "package.json")))},
		{name: "Dockerfile", text: dockerfile},
		{name: "agent-runtime manifest", text: deployment.text},
	} {
		if strings.Contains(source.text, staleNestedCommandPath) {
			t.Fatalf("%s still references stale nested Runtime Pod command path", source.name)
		}
	}
}

func TestKubernetesManifestGatewayRuntimePolicyGuardRejectsProtocolMismatch(t *testing.T) {
	documents := readManifestDocuments(t)
	service := requireDocument(t, documents, "web-connector.yaml", "Service", "web-connector")
	gateway := requireDocument(t, documents, "web-connector.yaml", "NetworkPolicy", "web-connector")
	runtime := *requireDocument(t, documents, "agent-runtime.yaml", "NetworkPolicy", "agent-runtime")
	runtime.text = strings.Replace(
		runtime.text,
		"- protocol: TCP\n          port: 9092",
		"- protocol: UDP\n          port: 9092",
		1,
	)
	if !strings.Contains(runtime.text, "- protocol: UDP\n          port: 9092") {
		t.Fatal("test setup did not mutate the runtime-pod web egress protocol")
	}
	if err := validateGatewayRuntimeNetworkPolicyEdgesArePaired(t, service, gateway, &runtime); err == nil {
		t.Fatal("gateway/runtime paired-edge guard accepted TCP ingress with UDP egress")
	}
}

func TestKubernetesManifestGatewayServiceBuildArtifactMapping(t *testing.T) {
	documents := readManifestDocuments(t)
	deployment := requireDocument(t, documents, "provider-gateway.yaml", "Deployment", "provider-gateway")
	metadata := readProviderGatewayPackageMetadata(t)
	dockerfile := readProviderGatewayDockerfile(t)

	if metadata.Tetral.ProviderGateway.Entrypoint != "packages/provider-gateway/src/command.ts" {
		t.Fatalf("provider gateway package entrypoint = %q", metadata.Tetral.ProviderGateway.Entrypoint)
	}
	if metadata.Tetral.ProviderGateway.BuildArtifact != "dist/provider-gateway/src-command.js" {
		t.Fatalf("provider gateway package build artifact = %q", metadata.Tetral.ProviderGateway.BuildArtifact)
	}
	expectedCommand := []string{"bun", "/app/" + metadata.Tetral.ProviderGateway.BuildArtifact}
	if !reflect.DeepEqual(metadata.Tetral.ProviderGateway.ContainerCommand, expectedCommand) {
		t.Fatalf("provider gateway package container command = %#v", metadata.Tetral.ProviderGateway.ContainerCommand)
	}
	if metadata.Tetral.MCPConnector.Entrypoint != "packages/mcp-connector/src/command.ts" {
		t.Fatalf("mcp connector package entrypoint = %q", metadata.Tetral.MCPConnector.Entrypoint)
	}
	if metadata.Tetral.MCPConnector.BuildArtifact != "dist/mcp-connector/src-command.js" {
		t.Fatalf("mcp connector package build artifact = %q", metadata.Tetral.MCPConnector.BuildArtifact)
	}
	expectedMCPCommand := []string{"bun", "/app/" + metadata.Tetral.MCPConnector.BuildArtifact}
	if !reflect.DeepEqual(metadata.Tetral.MCPConnector.ContainerCommand, expectedMCPCommand) {
		t.Fatalf("mcp connector package container command = %#v", metadata.Tetral.MCPConnector.ContainerCommand)
	}
	if !strings.Contains(metadata.Scripts["build"], metadata.Tetral.ProviderGateway.Entrypoint) {
		t.Fatalf("build script does not include Provider Gateway entrypoint %q", metadata.Tetral.ProviderGateway.Entrypoint)
	}
	if !strings.Contains(metadata.Scripts["build"], metadata.Tetral.MCPConnector.Entrypoint) {
		t.Fatalf("build script does not include MCP connector entrypoint %q", metadata.Tetral.MCPConnector.Entrypoint)
	}
	if metadata.Tetral.ProviderGateway.Image != "ghcr.io/tetral-ai/gateway:0.0.0-dev" {
		t.Fatalf("provider gateway package image = %q", metadata.Tetral.ProviderGateway.Image)
	}
	if metadata.Tetral.MCPConnector.Image != metadata.Tetral.ProviderGateway.Image {
		t.Fatalf("mcp connector image = %q; want provider image %q", metadata.Tetral.MCPConnector.Image, metadata.Tetral.ProviderGateway.Image)
	}
	for _, fragment := range []string{
		"COPY services/gateway/packages ./packages",
		"COPY internal/ts-observability /app/internal/ts-observability",
		"bun run build",
		"CMD [\"bun\", \"/app/" + metadata.Tetral.ProviderGateway.BuildArtifact + "\"]",
		"USER 1000:1000",
	} {
		if !strings.Contains(dockerfile, fragment) {
			t.Fatalf("Gateway Dockerfile missing %q", fragment)
		}
	}
	requireDeploymentContainerImage(t, deployment, metadata.Tetral.ProviderGateway.Image)
	requireContains(t, deployment, "runAsUser: 1000")
	requireContains(t, deployment, "runAsGroup: 1000")
	requireContains(t, deployment, "command:\n            - bun\n            - /app/"+metadata.Tetral.ProviderGateway.BuildArtifact)
	mcp := requireDocument(t, documents, "mcp-connector.yaml", "Deployment", "mcp-connector")
	requireDeploymentContainerImage(t, mcp, metadata.Tetral.MCPConnector.Image)
	requireContains(t, mcp, "command:\n            - bun\n            - /app/"+metadata.Tetral.MCPConnector.BuildArtifact)
}

func TestKubernetesManifestInternalGRPCClientTokensAreAudienceProjected(t *testing.T) {
	documents := readManifestDocuments(t)
	t.Run("agent-runtime", func(t *testing.T) {
		deployment := requireDocument(t, documents, "agent-runtime.yaml", "Deployment", "agent-runtime")
		requireProjectedBoundedServiceAccountToken(t, deployment, projectedTokenExpectation{
			envName:           "TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH",
			volume:            "runtime-pod-internal-grpc-token",
			mountPath:         "/var/run/secrets/tetral-internal-grpc/runtime-pod",
			filePath:          "token",
			audienceMode:      projectedTokenWithAudience,
			audience:          "tetral-internal-grpc",
			expirationSeconds: 600,
		})
		requireContains(t, deployment, "automountServiceAccountToken: false")
	})
	t.Run("job-runner", func(t *testing.T) {
		deployment := requireDocument(t, documents, "job-runner.yaml", "Deployment", "job-runner")
		requireProjectedBoundedServiceAccountToken(t, deployment, projectedTokenExpectation{
			envName:           "TETRAL_BRIDGE_RUNTIME_POD_TOKEN_PATH",
			volume:            "bridge-runtime-pod-token",
			mountPath:         "/var/run/secrets/tetral-internal-grpc/agent-runtime",
			filePath:          "token",
			audienceMode:      projectedTokenWithAudience,
			audience:          "tetral-internal-grpc",
			expirationSeconds: 600,
		})
		requireProjectedBoundedServiceAccountToken(t, deployment, projectedTokenExpectation{
			envName:           "TETRAL_BRIDGE_JOB_RUNNER_GATEWAY_TOKEN_PATH",
			volume:            "job-runner-mcp-token",
			mountPath:         "/var/run/secrets/tetral-internal-grpc/mcp-connector",
			filePath:          "token",
			audienceMode:      projectedTokenWithAudience,
			audience:          "tetral-internal-grpc",
			expirationSeconds: 600,
		})
		requireContains(t, deployment, "automountServiceAccountToken: false")
	})
}

func TestKubernetesManifestPublicExposureIsLimitedToPublicWorkloads(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		service := requireDocument(t, documents, workload.file, "Service", workload.name)
		networkPolicy := requireDocument(t, documents, workload.file, "NetworkPolicy", workload.name)
		requireContains(t, service, "type: ClusterIP")
		if workload.publicFacing {
			requireContains(t, service, "tetral.ai/exposure: public")
			requireContains(t, networkPolicy, "tetral.ai/network-role: public-ingress")
			continue
		}
		requireNotContains(t, service, "tetral.ai/exposure: public")
		requireNotContains(t, networkPolicy, "tetral.ai/network-role: public-ingress")
	}
}

func TestKubernetesManifestQueueIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "queue", "queue.yaml", []string{
		"configmap.yaml",
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestTetralAPIIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "api", "api.yaml", []string{
		"configmap.yaml",
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestTetralAuthIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "auth", "auth.yaml", []string{
		"configmap.yaml",
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestEventStreamIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "event-stream", "event-stream.yaml", []string{
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestGatewayWorkloadsAreComposedFromServiceLocalManifests(t *testing.T) {
	for _, n := range []string{"provider-gateway", "mcp-connector"} {
		files := []string{n + "/serviceaccount.yaml", n + "/deployment.yaml", n + "/service.yaml", n + "/networkpolicy.yaml"}
		if n == "provider-gateway" {
			files = append(files, n+"/hpa.yaml")
		}
		requireServiceLocalManifestComposition(t, "gateway", n+".yaml", files)
	}
}

func TestKubernetesManifestJobRunnerIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "job-runner", "job-runner.yaml", []string{"serviceaccount.yaml", "deployment.yaml", "service.yaml", "networkpolicy.yaml"})
}

func TestKubernetesManifestWebConnectorIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "web-connector", "web-connector.yaml", []string{"configmap.yaml", "serviceaccount.yaml", "deployment.yaml", "service.yaml", "networkpolicy.yaml"})
}

func TestKubernetesManifestGitProxyIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "git-proxy", "git-proxy.yaml", []string{
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
		"hpa.yaml",
	})
}

func TestKubernetesManifestAgentRuntimePodIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "agent-runtime", "agent-runtime.yaml", []string{
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
	aggregate := requireDocument(t, readManifestDocuments(t), "agent-runtime.yaml", "Deployment", "agent-runtime")
	requireContains(t, aggregate, "terminationGracePeriodSeconds: 90")
	serviceLocal := readServiceLocalManifestText(t, filepath.Join("agent-runtime", "k8s", "deployment.yaml"))
	if !strings.Contains(serviceLocal, "terminationGracePeriodSeconds: 90") {
		t.Fatal("service-local agent-runtime deployment is missing terminationGracePeriodSeconds: 90")
	}
}

func TestKubernetesManifestBridgeServiceIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "bridge", "bridge.yaml", []string{
		"configmap.yaml",
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestJobRunnerRBACIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "job-runner", "job-runner-rbac.yaml", []string{
		"rbac.yaml",
	})
}

func TestKubernetesManifestSandboxIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "sandbox", "sandbox.yaml", []string{
		"configmap.yaml",
		"serviceaccount.yaml",
		"deployment.yaml",
		"service.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestSandboxUsesPublicGitProxyHost(t *testing.T) {
	documents := readManifestDocuments(t)
	if actual := requireConfigMapDataValue(t, documents, "sandbox.yaml", "sandbox-config", "TETRAL_GIT_PROXY_HOST"); actual != "git.tetral.example" {
		t.Fatalf("sandbox git proxy host = %q; want public git proxy host", actual)
	}
	configMap := requireDocument(t, documents, "sandbox.yaml", "ConfigMap", "sandbox-config")
	requireNotContains(t, configMap, "git.tetral.example.internal")
}

func TestKubernetesManifestCleanupIsComposedFromServiceLocalManifests(t *testing.T) {
	requireServiceLocalManifestComposition(t, "cleanup", "cleanup.yaml", []string{
		"serviceaccount.yaml",
		"cronjob.yaml",
		"networkpolicy.yaml",
	})
}

func TestKubernetesManifestInternalGRPCTokenReviewRBACIsComposedFromServiceLocalManifests(t *testing.T) {
	requireManifestComposition(t, "internal-grpc-tokenreview-rbac.yaml", []string{
		filepath.Join("..", "..", "services", "agent-runtime", "k8s", "tokenreview-rbac.yaml"),
		filepath.Join("..", "..", "services", "bridge", "k8s", "tokenreview-rbac.yaml"),
		filepath.Join("..", "..", "services", "gateway", "k8s", "provider-gateway", "tokenreview-rbac.yaml"),
		filepath.Join("..", "..", "services", "gateway", "k8s", "mcp-connector", "tokenreview-rbac.yaml"),
		filepath.Join("..", "..", "services", "web-connector", "k8s", "tokenreview-rbac.yaml"),
	})
}

func requireServiceLocalManifestComposition(t *testing.T, serviceName string, topLevelFile string, manifestNames []string) {
	t.Helper()
	var serviceLocalFiles []string
	for _, manifestName := range manifestNames {
		serviceLocalFiles = append(serviceLocalFiles, filepath.Join("..", "..", "services", serviceName, "k8s", manifestName))
	}
	requireManifestComposition(t, topLevelFile, serviceLocalFiles)
}

func requireManifestComposition(t *testing.T, topLevelFile string, componentFiles []string) {
	t.Helper()
	var parts []string
	for _, componentFile := range componentFiles {
		body, err := os.ReadFile(componentFile) //nolint:gosec // repository-local manifest path.
		if err != nil {
			t.Fatalf("read service-local manifest %s: %v", componentFile, err)
		}
		parts = append(parts, strings.TrimSpace(string(body)))
	}
	composed := strings.Join(parts, "\n---\n")
	topLevel, err := os.ReadFile(topLevelFile) //nolint:gosec // repository-local manifest path.
	if err != nil {
		t.Fatalf("read top-level %s manifest: %v", topLevelFile, err)
	}
	if strings.TrimSpace(string(topLevel)) != composed {
		t.Fatalf("deploy/kubernetes/%s must be the composition of service-local manifests", topLevelFile)
	}
}

func internalGRPCTokenReviewGrants() []tokenReviewGrant {
	return []tokenReviewGrant{
		{name: "agent-runtime-tokenreview", serviceAccountName: "agent-runtime", serviceAccountNamespace: "tetral-agent-runtime"},
		{name: "bridge-tokenreview", serviceAccountName: "bridge", serviceAccountNamespace: "tetral-system"},
		{name: "provider-gateway-tokenreview", serviceAccountName: "provider-gateway", serviceAccountNamespace: "tetral-system"},
		{name: "mcp-connector-tokenreview", serviceAccountName: "mcp-connector", serviceAccountNamespace: "tetral-system"},
		{name: "web-connector-tokenreview", serviceAccountName: "web-connector", serviceAccountNamespace: "tetral-system"},
	}
}

func TestKubernetesManifestRBACPermissionsAreExact(t *testing.T) {
	documents := readManifestDocuments(t)
	runnerRole := requireDocument(t, documents, "job-runner-rbac.yaml", "Role", "job-runner-visibility")
	runnerBinding := requireDocument(t, documents, "job-runner-rbac.yaml", "RoleBinding", "job-runner-visibility")

	requireExactRBACRules(t, runnerRole, []rbacRule{
		{apiGroups: []string{""}, resources: []string{"pods"}, verbs: []string{"get", "list", "watch"}},
		{apiGroups: []string{"discovery.k8s.io"}, resources: []string{"endpointslices"}, verbs: []string{"get", "list", "watch"}},
	})
	requireExactRBACSubjects(t, runnerBinding, []rbacSubject{{kind: "ServiceAccount", name: "job-runner", namespace: "tetral-system"}})
	requireExactRBACRoleRef(t, runnerBinding, rbacRoleRef{
		apiGroup: "rbac.authorization.k8s.io",
		kind:     "Role",
		name:     "job-runner-visibility",
	})

	allowedClusterRBAC := map[string]bool{}
	for _, grant := range internalGRPCTokenReviewGrants() {
		tokenReviewRole := requireDocument(t, documents, "internal-grpc-tokenreview-rbac.yaml", "ClusterRole", grant.name)
		tokenReviewBinding := requireDocument(t, documents, "internal-grpc-tokenreview-rbac.yaml", "ClusterRoleBinding", grant.name)
		allowedClusterRBAC[grant.name] = true
		requireExactRBACRules(t, tokenReviewRole, []rbacRule{
			{apiGroups: []string{"authentication.k8s.io"}, resources: []string{"tokenreviews"}, verbs: []string{"create"}},
		})
		requireExactRBACSubjects(t, tokenReviewBinding, []rbacSubject{
			{kind: "ServiceAccount", name: grant.serviceAccountName, namespace: grant.serviceAccountNamespace},
		})
		requireExactRBACRoleRef(t, tokenReviewBinding, rbacRoleRef{
			apiGroup: "rbac.authorization.k8s.io",
			kind:     "ClusterRole",
			name:     grant.name,
		})
	}
	for _, document := range documents {
		if document.kind == "ClusterRole" && !allowedClusterRBAC[document.name] {
			t.Fatalf("unexpected cluster-scoped RBAC %s/%s in %s", document.kind, document.name, document.file)
		}
		if document.kind == "ClusterRoleBinding" && !allowedClusterRBAC[document.name] {
			t.Fatalf("unexpected cluster-scoped RBAC binding %s/%s in %s", document.kind, document.name, document.file)
		}
	}
}

func TestKubernetesManifestRBACBindingsRejectExtraSubjects(t *testing.T) {
	documents := readManifestDocuments(t)
	runnerBinding := requireDocument(t, documents, "job-runner-rbac.yaml", "RoleBinding", "job-runner-visibility")

	tests := []struct {
		name     string
		document *manifestDocument
		extra    string
		expected []rbacSubject
	}{
		{
			name:     "job-runner extra subject",
			document: runnerBinding,
			extra:    "\n  - kind: ServiceAccount\n    name: api\n    namespace: tetral-system",
			expected: []rbacSubject{{kind: "ServiceAccount", name: "job-runner", namespace: "tetral-system"}},
		},
	}
	for _, grant := range internalGRPCTokenReviewGrants() {
		tests = append(tests, struct {
			name     string
			document *manifestDocument
			extra    string
			expected []rbacSubject
		}{
			name:     grant.name + " extra subject",
			document: requireDocument(t, documents, "internal-grpc-tokenreview-rbac.yaml", "ClusterRoleBinding", grant.name),
			extra:    "\n  - kind: ServiceAccount\n    name: event-stream\n    namespace: tetral-system",
			expected: []rbacSubject{{kind: "ServiceAccount", name: grant.serviceAccountName, namespace: grant.serviceAccountNamespace}},
		})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := *test.document
			mutated.text = mutateRBACSubjects(test.document.text, test.extra)
			if err := validateExactRBACSubjects(mutated.text, test.expected); err == nil {
				t.Fatalf("mutated %s %s/%s passed exact RBAC subject validation", mutated.file, mutated.kind, mutated.name)
			}
		})
	}
}

func TestKubernetesManifestRBACRulesRejectExtraPermissions(t *testing.T) {
	documents := readManifestDocuments(t)
	runnerRole := requireDocument(t, documents, "job-runner-rbac.yaml", "Role", "job-runner-visibility")
	tokenReviewRole := requireDocument(t, documents, "internal-grpc-tokenreview-rbac.yaml", "ClusterRole", internalGRPCTokenReviewGrants()[0].name)

	tests := []struct {
		name     string
		document *manifestDocument
		extra    string
		expected []rbacRule
	}{
		{
			name:     "job-runner secrets",
			document: runnerRole,
			extra:    "\n  - apiGroups:\n      - \"\"\n    resources:\n      - secrets\n    verbs:\n      - get",
			expected: []rbacRule{
				{apiGroups: []string{""}, resources: []string{"pods"}, verbs: []string{"get", "list", "watch"}},
				{apiGroups: []string{"discovery.k8s.io"}, resources: []string{"endpointslices"}, verbs: []string{"get", "list", "watch"}},
			},
		},
		{
			name:     "tokenreview patch",
			document: tokenReviewRole,
			extra:    "\n  - apiGroups:\n      - authentication.k8s.io\n    resources:\n      - tokenreviews\n    verbs:\n      - patch",
			expected: []rbacRule{
				{apiGroups: []string{"authentication.k8s.io"}, resources: []string{"tokenreviews"}, verbs: []string{"create"}},
			},
		},
		{
			name:     "tokenreview delete",
			document: tokenReviewRole,
			extra:    "\n  - apiGroups:\n      - authentication.k8s.io\n    resources:\n      - tokenreviews\n    verbs:\n      - delete",
			expected: []rbacRule{
				{apiGroups: []string{"authentication.k8s.io"}, resources: []string{"tokenreviews"}, verbs: []string{"create"}},
			},
		},
		{
			name:     "tokenreview scale",
			document: tokenReviewRole,
			extra:    "\n  - apiGroups:\n      - apps\n    resources:\n      - deployments/scale\n    verbs:\n      - update",
			expected: []rbacRule{
				{apiGroups: []string{"authentication.k8s.io"}, resources: []string{"tokenreviews"}, verbs: []string{"create"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := *test.document
			mutated.text = mutateRBACRules(test.document.text, test.extra)
			if err := validateExactRBACRules(mutated.text, test.expected); err == nil {
				t.Fatalf("mutated %s %s/%s passed exact RBAC validation", mutated.file, mutated.kind, mutated.name)
			}
			if err := validateRBACSecurityBaseline(mutated.text); err == nil {
				t.Fatalf("mutated %s %s/%s passed RBAC security baseline", mutated.file, mutated.kind, mutated.name)
			}
		})
	}
}

// retiredAgentRuntimeLabel is the pre-fix component label form, assembled from fragments
// so the source never contains the contiguous token a repo-wide grep guards against.
var retiredAgentRuntimeLabel = "app.kubernetes.io/" + "compone" + "nt" + ": " + "agent-" + "runtime"

func TestKubernetesManifestNetworkPolicyInternalGRPCPeers(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, workload := range workloadManifests {
		if isSeparatedWorkload(workload.name) {
			continue
		}
		networkPolicy := requireDocument(t, documents, workload.file, "NetworkPolicy", workload.name)
		if workload.internalGRPC {
			port := 9090
			if workload.name == "agent-runtime" {
				port = 19090
			}
			requireContains(t, networkPolicy, fmt.Sprintf("port: %d", port))
			if workload.name == "queue" {
				for _, port := range []int{8080, 9090} {
					requireNetworkPolicyIngressEdge(t, networkPolicy, port, networkPolicyPeer{namespace: "tetral-system", podName: "api"})
					requireNetworkPolicyIngressEdge(t, networkPolicy, port, networkPolicyPeer{namespace: "tetral-system", podName: "job-runner"})
					requireNetworkPolicyIngressEdge(t, networkPolicy, port, networkPolicyPeer{namespace: "tetral-system", podName: "sandbox"})
					requireNoBroadNetworkPolicyIngressPeers(t, networkPolicy, port)
				}
				for _, forbiddenSource := range []string{"event-stream"} {
					requireNotContains(t, networkPolicy, "app.kubernetes.io/name: "+forbiddenSource)
				}
				requireNotContains(t, networkPolicy, "app.kubernetes.io/name: agent-runtime\n")
				continue
			}
			if workload.name == "agent-runtime" {
				requireNetworkPolicyIngressEdge(t, networkPolicy, 19090, networkPolicyPeer{namespace: "tetral-system", podName: "job-runner"})
				requireNoBroadNetworkPolicyIngressPeers(t, networkPolicy, 19090)
				for _, forbiddenSource := range []string{"api", "event-stream"} {
					requireNotContains(t, networkPolicy, "app.kubernetes.io/name: "+forbiddenSource)
				}
				continue
			}
			if workload.name == "bridge" {
				requireNetworkPolicyIngressEdge(t, networkPolicy, 9090, networkPolicyPeer{namespace: "tetral-agent-runtime", podName: "agent-runtime"})
				requireNetworkPolicyIngressEdge(t, networkPolicy, 9090, networkPolicyPeer{namespace: "tetral-system", podName: "gateway"})
				requireNoBroadNetworkPolicyIngressPeers(t, networkPolicy, 9090)
				continue
			}
			if workload.name == "sandbox" {
				requireNetworkPolicyIngressEdge(t, networkPolicy, 9090, networkPolicyPeer{namespace: "tetral-system", podName: "bridge"})
				requireNoBroadNetworkPolicyIngressPeers(t, networkPolicy, 9090)
				for _, forbiddenSource := range []string{"api", "event-stream", "gateway", "agent-runtime"} {
					requireNotContains(t, networkPolicy, "app.kubernetes.io/name: "+forbiddenSource)
				}
				continue
			}
			requireContains(t, networkPolicy, "kubernetes.io/metadata.name: tetral-agent-runtime")
			requireContains(t, networkPolicy, "app.kubernetes.io/name: agent-runtime")
			for _, forbiddenSource := range []string{"api", "event-stream"} {
				requireNotContains(t, networkPolicy, "app.kubernetes.io/name: "+forbiddenSource)
			}
			continue
		}
		requireNoNetworkPolicyIngressPort(t, networkPolicy, 9090)
		requireNotContains(t, networkPolicy, "app.kubernetes.io/name: agent-runtime")
		// Built from fragments so this assertion never self-matches a repo-wide grep for the
		// retired label key; the one Agent Runtime label key is name=.
		requireNotContains(t, networkPolicy, retiredAgentRuntimeLabel)
	}
}

func TestKubernetesManifestNetworkPolicyRejectsBroadGRPCIngress(t *testing.T) {
	documents := readManifestDocuments(t)
	networkPolicy := requireDocument(t, documents, "agent-runtime.yaml", "NetworkPolicy", "agent-runtime")

	mutated := *networkPolicy
	mutated.text = strings.Replace(
		networkPolicy.text,
		"  ingress:\n",
		"  ingress:\n    - ports:\n        - protocol: TCP\n          port: 9090\n",
		1,
	)
	if mutated.text == networkPolicy.text {
		t.Fatal("guard-of-the-guard setup failed: broad ingress rule injection did not modify NetworkPolicy text")
	}

	if err := validateNoBroadNetworkPolicyIngressPeers(t, &mutated, 9090); err == nil {
		t.Fatal("broad gRPC ingress rule without from peers passed NetworkPolicy broad-peer validation")
	}
}

// TestKubernetesManifestAgentRuntimeNamespaceIsConsistent proves the four places that
// declare the Agent Runtime namespace agree, reading each value from the real manifest
// or RBAC file. If any one of the four drifts, an Agent Runtime visibility cache could observe an
// empty namespace while NetworkPolicies admit callers from a different one.
func TestKubernetesManifestAgentRuntimeNamespaceIsConsistent(t *testing.T) {
	documents := readManifestDocuments(t)

	bridgeDeployment := requireDocument(t, documents, "bridge.yaml", "Deployment", "bridge")
	runner := requireDocument(t, documents, "job-runner.yaml", "Deployment", "job-runner")
	watchNamespace := requireDeploymentEnvValue(t, runner, "TETRAL_KUBERNETES_NAMESPACE")

	runtimeServiceAccount := requireDocument(t, documents, "agent-runtime.yaml", "ServiceAccount", "agent-runtime")
	gatewayDeployment := requireDocument(t, documents, "provider-gateway.yaml", "Deployment", "provider-gateway")
	runtimeNamespace := requireMetadataNamespace(t, runtimeServiceAccount)
	bridgeAllowedServiceAccounts := requireDeploymentEnvValue(t, bridgeDeployment, "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS")
	bridgeServiceAccountNamespace := serviceAccountNamespaceFromAllowlist(t, bridgeAllowedServiceAccounts, "tetral-agent-runtime/agent-runtime")
	requireServiceAccountAllowlistContains(t, bridgeAllowedServiceAccounts, "tetral-system/provider-gateway")
	requireServiceAccountAllowlistContains(t, bridgeAllowedServiceAccounts, "tetral-system/mcp-connector")
	gatewayServiceAccountNamespace := serviceAccountNamespace(t, requireDeploymentEnvValue(t, gatewayDeployment, "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS"))

	bridgePolicy := requireDocument(t, documents, "bridge.yaml", "NetworkPolicy", "bridge")
	gatewayPolicy := requireDocument(t, documents, "provider-gateway.yaml", "NetworkPolicy", "provider-gateway")
	bridgePeerNamespace := requireSelectorValue(t, bridgePolicy, "kubernetes.io/metadata.name")
	gatewayPeerNamespace := requireSelectorValue(t, gatewayPolicy, "kubernetes.io/metadata.name")

	runnerRole := requireDocument(t, documents, "job-runner-rbac.yaml", "Role", "job-runner-visibility")
	runnerBinding := requireDocument(t, documents, "job-runner-rbac.yaml", "RoleBinding", "job-runner-visibility")
	roleNamespace := requireMetadataNamespace(t, runnerRole)
	bindingNamespace := requireMetadataNamespace(t, runnerBinding)

	declarations := map[string]string{
		"job-runner watch env":              watchNamespace,
		"runtime ServiceAccount namespace":  runtimeNamespace,
		"bridge allowlist SA namespace":     bridgeServiceAccountNamespace,
		"gateway allowlist SA namespace":    gatewayServiceAccountNamespace,
		"bridge NetworkPolicy namespace":    bridgePeerNamespace,
		"gateway NetworkPolicy namespace":   gatewayPeerNamespace,
		"job-runner RBAC Role namespace":    roleNamespace,
		"job-runner RBAC binding namespace": bindingNamespace,
	}
	const want = "tetral-agent-runtime"
	for source, value := range declarations {
		if value != want {
			t.Fatalf("%s = %q; want all Agent Runtime namespace declarations to equal %q", source, value, want)
		}
	}
}

// TestKubernetesManifestWorkloadNamespacesPinRBACSubjectNamespace proves every workload
// resource pins metadata.namespace to the namespace the RBAC subjects assume. Without the
// pin, applying the manifests with a different -n silently creates identities the RBAC does
// not bind, so Job Runner watches and internal gRPC TokenReview fail at runtime, not apply.
func TestKubernetesManifestWorkloadNamespacesPinRBACSubjectNamespace(t *testing.T) {
	documents := readManifestDocuments(t)

	totalDocuments := 0
	for _, workload := range workloadManifests {
		docs := documents.byFile(workload.file)
		wantCount := expectedWorkloadDocumentCount(workload)
		if len(docs) != wantCount {
			t.Fatalf("%s document count = %d; want %d namespace-scoped resources", workload.file, len(docs), wantCount)
		}
		for index := range docs {
			namespace := requireMetadataNamespace(t, &docs[index])
			if namespace != workload.namespace {
				t.Fatalf("%s %s/%s metadata.namespace = %q; want %q", docs[index].file, docs[index].kind, docs[index].name, namespace, workload.namespace)
			}
			totalDocuments++
		}
	}
	expectedDocuments := 0
	for _, workload := range workloadManifests {
		expectedDocuments += expectedWorkloadDocumentCount(workload)
	}
	if totalDocuments != expectedDocuments {
		t.Fatalf("checked %d workload documents; want %d", totalDocuments, expectedDocuments)
	}

	// Read the RBAC side from subjects[].namespace, NOT metadata.namespace: the Job Runner
	// RoleBinding's metadata namespace is the watched Runtime namespace, while its subject
	// lives in tetral-system.
	runnerBinding := requireDocument(t, documents, "job-runner-rbac.yaml", "RoleBinding", "job-runner-visibility")

	runnerSubjects, err := parseRBACSubjects(runnerBinding.text)
	if err != nil {
		t.Fatalf("%s %s/%s parse subjects: %v", runnerBinding.file, runnerBinding.kind, runnerBinding.name, err)
	}
	requireSubjectNamespaces(t, runnerBinding, runnerSubjects, map[string]string{"job-runner": "tetral-system"})

	for _, grant := range internalGRPCTokenReviewGrants() {
		tokenReviewBinding := requireDocument(t, documents, "internal-grpc-tokenreview-rbac.yaml", "ClusterRoleBinding", grant.name)
		tokenReviewSubjects, err := parseRBACSubjects(tokenReviewBinding.text)
		if err != nil {
			t.Fatalf("%s %s/%s parse subjects: %v", tokenReviewBinding.file, tokenReviewBinding.kind, tokenReviewBinding.name, err)
		}
		requireSubjectNamespaces(t, tokenReviewBinding, tokenReviewSubjects, map[string]string{
			grant.serviceAccountName: grant.serviceAccountNamespace,
		})
	}
}

// TestKubernetesManifestAgentRuntimeLabelSelectorIsConsistent proves the Agent Runtime
// label key/value agrees across the Bridge selector env, internal gRPC NetworkPolicy
// podSelectors, and the code default in internal/kubernetes/config.go.
func TestKubernetesManifestAgentRuntimeLabelSelectorIsConsistent(t *testing.T) {
	documents := readManifestDocuments(t)

	bridgeDeployment := requireDocument(t, documents, "job-runner.yaml", "Deployment", "job-runner")
	envKey, envValue := splitLabelSelector(t, requireDeploymentEnvValue(t, bridgeDeployment, "TETRAL_AGENT_RUNTIME_LABEL_SELECTOR"))

	runtimeDeployment := requireDocument(t, documents, "agent-runtime.yaml", "Deployment", "agent-runtime")
	bridgePolicy := requireDocument(t, documents, "job-runner.yaml", "NetworkPolicy", "job-runner")
	gatewayPolicy := requireDocument(t, documents, "provider-gateway.yaml", "NetworkPolicy", "provider-gateway")
	runtimeKey, runtimeValue := requireDeploymentPodLabel(t, runtimeDeployment, "app.kubernetes.io/name")
	bridgePeerKey, bridgePeerValue := peerPodSelectorLabel(t, bridgePolicy)
	gatewayPeerKey, gatewayPeerValue := peerPodSelectorLabel(t, gatewayPolicy)

	codeDefaultKey, codeDefaultValue := splitLabelSelector(t, agentRuntimeLabelSelectorCodeDefault(t))

	type labelSite struct {
		source string
		key    string
		value  string
	}
	for _, site := range []labelSite{
		{source: "bridge job-runner selector env", key: envKey, value: envValue},
		{source: "runtime Pod label", key: runtimeKey, value: runtimeValue},
		{source: "bridge NetworkPolicy peer", key: bridgePeerKey, value: bridgePeerValue},
		{source: "gateway NetworkPolicy peer", key: gatewayPeerKey, value: gatewayPeerValue},
		{source: "config.go code default", key: codeDefaultKey, value: codeDefaultValue},
	} {
		if site.key != "app.kubernetes.io/name" || site.value != "agent-runtime" {
			t.Fatalf("%s label = %q=%q; want app.kubernetes.io/name=agent-runtime", site.source, site.key, site.value)
		}
	}
}

func requireDeploymentEnvValueOrConfigMap(
	t *testing.T,
	documents manifestDocuments,
	deployment *manifestDocument,
	workload workloadManifest,
	name string,
	want string,
) {
	t.Helper()
	if literal, ok := deploymentEnvLiteralValue(deployment.text, name); ok {
		if literal != want {
			t.Fatalf("%s env %s = %q; want %q", deployment.file, name, literal, want)
		}
		return
	}
	if workload.configMapName == "" {
		t.Fatalf("%s env %s has no literal value and workload has no ConfigMap owner", deployment.file, name)
	}
	requireContains(t, deployment, "name: "+name+"\n              valueFrom:\n                configMapKeyRef:\n                  name: "+workload.configMapName+"\n                  key: "+name)
	value := requireConfigMapDataValue(t, documents, workload.file, workload.configMapName, name)
	if value != want {
		t.Fatalf("%s ConfigMap %s data %s = %q; want %q", workload.file, workload.configMapName, name, value, want)
	}
}

func deploymentEnvLiteralValue(text string, name string) (string, bool) {
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != "- name: "+name {
			continue
		}
		for _, following := range lines[index+1:] {
			trimmed := strings.TrimSpace(following)
			if strings.HasPrefix(trimmed, "- name: ") {
				break
			}
			if strings.HasPrefix(trimmed, "value: ") {
				return cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "value: "))), true
			}
		}
		return "", false
	}
	return "", false
}

func requireConfigMapDataValue(t *testing.T, documents manifestDocuments, file string, name string, key string) string {
	t.Helper()
	configMap := requireDocument(t, documents, file, "ConfigMap", name)
	lines := strings.Split(configMap.text, "\n")
	inData := false
	for _, line := range lines {
		if line == "data:" {
			inData = true
			continue
		}
		if !inData {
			continue
		}
		if line != "" && !strings.HasPrefix(line, "  ") {
			break
		}
		prefix := "  " + key + ": "
		if strings.HasPrefix(line, prefix) {
			return cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
		}
	}
	t.Fatalf("%s ConfigMap %s missing data key %s", file, name, key)
	return ""
}

// requireDeploymentEnvValue returns the container env value for the given name. It only
// supports literal `value:` envs because cross-consistency requires a concrete string.
func requireDeploymentEnvValue(t *testing.T, document *manifestDocument, name string) string {
	t.Helper()
	lines := strings.Split(document.text, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != "- name: "+name {
			continue
		}
		for _, following := range lines[index+1:] {
			trimmed := strings.TrimSpace(following)
			if strings.HasPrefix(trimmed, "- name: ") {
				break
			}
			if strings.HasPrefix(trimmed, "value: ") {
				return cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "value: ")))
			}
		}
		t.Fatalf("%s env %s has no literal value", document.file, name)
	}
	t.Fatalf("%s missing env %s", document.file, name)
	return ""
}

func requireDeploymentEnvFieldPath(t *testing.T, document *manifestDocument, name string) string {
	t.Helper()
	lines := strings.Split(document.text, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != "- name: "+name {
			continue
		}
		for _, following := range lines[index+1:] {
			trimmed := strings.TrimSpace(following)
			if strings.HasPrefix(trimmed, "- name: ") {
				break
			}
			if strings.HasPrefix(trimmed, "fieldPath: ") {
				return cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "fieldPath: ")))
			}
		}
		t.Fatalf("%s env %s has no fieldPath valueFrom", document.file, name)
	}
	t.Fatalf("%s missing env %s", document.file, name)
	return ""
}

func requireDeploymentPodLabel(t *testing.T, document *manifestDocument, key string) (string, string) {
	t.Helper()
	lines := strings.Split(document.text, "\n")
	inTemplateLabels := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if line == "      labels:" {
			inTemplateLabels = true
			continue
		}
		if inTemplateLabels {
			if strings.HasPrefix(line, "      ") && !strings.HasPrefix(line, "        ") {
				break
			}
			prefix := key + ": "
			if strings.HasPrefix(trimmed, prefix) {
				return key, cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, prefix)))
			}
		}
	}
	t.Fatalf("%s %s/%s missing pod template label %s", document.file, document.kind, document.name, key)
	return "", ""
}

func requireDeploymentContainerImage(t *testing.T, document *manifestDocument, expected string) {
	t.Helper()
	block, err := podSpecBlock(document.text, "containers:")
	if err != nil {
		t.Fatalf("%s %s/%s: %v", document.file, document.kind, document.name, err)
	}
	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(line, "          image: ") {
			actual := cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(line, "          image: ")))
			if actual != expected {
				t.Fatalf("%s %s/%s container image = %q; want %q", document.file, document.kind, document.name, actual, expected)
			}
			return
		}
	}
	t.Fatalf("%s %s/%s missing container image", document.file, document.kind, document.name)
}

func requireProjectedBoundedServiceAccountToken(t *testing.T, document *manifestDocument, expected projectedTokenExpectation) {
	t.Helper()
	tokenPath := expected.mountPath + "/" + expected.filePath
	if actual := requireDeploymentEnvValue(t, document, expected.envName); actual != tokenPath {
		t.Fatalf("%s env %s = %q; want projected token path %q", document.file, expected.envName, actual, tokenPath)
	}
	defaultTokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token" // #nosec G101 -- Kubernetes service-account fixture path.
	if tokenPath == defaultTokenPath {
		t.Fatalf("%s env %s uses default service-account token path", document.file, expected.envName)
	}
	requireProjectedBoundedServiceAccountTokenShape(t, document, expected)
}

func requireProjectedBoundedServiceAccountTokenShape(t *testing.T, document *manifestDocument, expected projectedTokenExpectation) {
	t.Helper()
	requireContains(t, document, "- name: "+expected.volume+"\n              mountPath: "+expected.mountPath+"\n              readOnly: true")
	switch expected.audienceMode {
	case projectedTokenWithAudience:
		if expected.audience == "" {
			t.Fatalf("%s projected token volume %s requires a non-empty audience expectation", document.file, expected.volume)
		}
		requireContains(t, document, fmt.Sprintf("- name: %s\n          projected:\n            sources:\n              - serviceAccountToken:\n                  audience: %s\n                  expirationSeconds: %d\n                  path: %s", expected.volume, expected.audience, expected.expirationSeconds, expected.filePath))
	case projectedTokenNoAudience:
		requireContains(t, document, fmt.Sprintf("- name: %s\n          projected:\n            sources:\n              - serviceAccountToken:\n                  expirationSeconds: %d\n                  path: %s", expected.volume, expected.expirationSeconds, expected.filePath))
		volumeBlock := requireDeploymentVolumeBlock(t, document, expected.volume)
		for lineNumber, line := range strings.Split(volumeBlock, "\n") {
			item := strings.TrimSpace(line)
			item = strings.TrimSpace(strings.TrimPrefix(item, "- "))
			key, _, ok := splitManifestScalar(item)
			if ok && key == "audience" {
				t.Fatalf("%s projected token volume %s contains forbidden audience at block line %d: %s", document.file, expected.volume, lineNumber+1, strings.TrimSpace(line))
			}
		}
	default:
		t.Fatalf("%s projected token volume %s has unknown audience mode %q", document.file, expected.volume, expected.audienceMode)
	}
}

func requireDeploymentVolumeBlock(t *testing.T, document *manifestDocument, volumeName string) string {
	t.Helper()
	volumes, err := podSpecBlock(document.text, "volumes:")
	if err != nil {
		t.Fatalf("%s %s/%s: %v", document.file, document.kind, document.name, err)
	}
	marker := "        - name: " + volumeName + "\n"
	start := strings.Index(volumes, marker)
	if start < 0 {
		t.Fatalf("%s missing volume %s", document.file, volumeName)
	}
	rest := volumes[start:]
	next := strings.Index(rest[len(marker):], "\n        - name: ")
	if next >= 0 {
		return rest[:len(marker)+next]
	}
	return rest
}

func requireNetworkPolicyIngressEdge(t *testing.T, document *manifestDocument, port int, peer networkPolicyPeer) {
	t.Helper()
	for _, rule := range parseNetworkPolicyIngressRules(t, document) {
		if !networkPolicyTransportSliceContains(rule.transports, networkPolicyTransport{protocol: "TCP", port: port}) {
			continue
		}
		for _, actual := range rule.peers {
			if networkPolicyPeersEqual(actual, peer) {
				return
			}
		}
	}
	t.Fatalf("%s %s/%s missing ingress edge %s/%s/%s on port %d", document.file, document.kind, document.name, peer.namespace, peer.podName, peer.podPartOf, port)
}

func requireNetworkPolicyEgressEdge(t *testing.T, document *manifestDocument, port int, peer networkPolicyPeer) {
	t.Helper()
	for _, rule := range parseNetworkPolicyEgressRules(t, document) {
		if !networkPolicyTransportSliceContains(rule.transports, networkPolicyTransport{protocol: "TCP", port: port}) {
			continue
		}
		for _, actual := range rule.peers {
			if networkPolicyPeersEqual(actual, peer) {
				return
			}
		}
	}
	t.Fatalf("%s %s/%s missing egress edge %s/%s/%s on port %d", document.file, document.kind, document.name, peer.namespace, peer.podName, peer.podPartOf, port)
}

func validateGatewayRuntimeNetworkPolicyEdgesArePaired(t *testing.T, service *manifestDocument, gateway *manifestDocument, runtime *manifestDocument) error {
	t.Helper()
	gatewayPeer := networkPolicyPeer{namespace: "tetral-system", podName: gateway.name}
	runtimePeer := networkPolicyPeer{namespace: "tetral-agent-runtime", podName: "agent-runtime"}
	ingressTransports := networkPolicyTransportsForPeer(t, parseNetworkPolicyIngressRules(t, gateway), runtimePeer)
	egressTransports := networkPolicyTransportsForPeer(t, parseNetworkPolicyEgressRules(t, runtime), gatewayPeer)
	serviceTransports := gatewayGRPCServiceTransports(t, service)
	if len(ingressTransports) == 0 {
		return fmt.Errorf("%s has no gateway ingress ports for the runtime pod", gateway.file)
	}
	for _, transport := range serviceTransports {
		if !networkPolicyTransportSliceContains(ingressTransports, transport) {
			return fmt.Errorf("%s exposes %s/%d but %s omits the matching runtime-pod ingress edge", service.file, transport.protocol, transport.port, gateway.file)
		}
	}
	for _, transport := range ingressTransports {
		if !networkPolicyTransportSliceContains(egressTransports, transport) {
			return fmt.Errorf("%s admits runtime-pod ingress %s/%d but %s omits the matching gateway egress edge", gateway.file, transport.protocol, transport.port, runtime.file)
		}
	}
	return nil
}

func networkPolicyTransportsForPeer(t *testing.T, rules []networkPolicyRule, peer networkPolicyPeer) []networkPolicyTransport {
	t.Helper()
	transports := map[networkPolicyTransport]struct{}{}
	for _, rule := range rules {
		for _, actual := range rule.peers {
			if !networkPolicyPeersEqual(actual, peer) {
				continue
			}
			for _, transport := range rule.transports {
				transports[transport] = struct{}{}
			}
		}
	}
	result := make([]networkPolicyTransport, 0, len(transports))
	for transport := range transports {
		result = append(result, transport)
	}
	sort.Slice(result, func(left int, right int) bool {
		if result[left].port != result[right].port {
			return result[left].port < result[right].port
		}
		return result[left].protocol < result[right].protocol
	})
	return result
}

func networkPolicyPeersEqual(left networkPolicyPeer, right networkPolicyPeer) bool {
	return left.namespace == right.namespace &&
		left.podName == right.podName &&
		left.podPartOf == right.podPartOf
}

func gatewayGRPCServiceTransports(t *testing.T, service *manifestDocument) []networkPolicyTransport {
	t.Helper()
	var result []networkPolicyTransport
	var name string
	for _, line := range strings.Split(service.text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- name: ") {
			name = cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "- name: ")))
			continue
		}
		if !strings.HasPrefix(trimmed, "port: ") || !strings.HasSuffix(name, "-grpc") {
			continue
		}
		var port int
		if _, err := fmt.Sscanf(trimmed, "port: %d", &port); err != nil {
			t.Fatalf("parse gateway Service port %q: %v", trimmed, err)
		}
		result = append(result, networkPolicyTransport{protocol: "TCP", port: port})
		name = ""
	}
	if len(result) == 0 {
		t.Fatalf("%s has no named gRPC service ports", service.file)
	}
	return result
}

func networkPolicyTransportSliceContains(values []networkPolicyTransport, expected networkPolicyTransport) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func requireNetworkPolicyEgressIPBlock(t *testing.T, document *manifestDocument, port int, cidr string) {
	t.Helper()
	if networkPolicyHasEgressIPBlock(t, document, port, cidr) {
		return
	}
	t.Fatalf("%s %s/%s missing egress ipBlock %s on port %d", document.file, document.kind, document.name, cidr, port)
}

func networkPolicyHasEgressIPBlock(t *testing.T, document *manifestDocument, port int, cidr string) bool {
	t.Helper()
	for _, rule := range parseNetworkPolicyEgressRules(t, document) {
		if !intSliceContains(rule.ports, port) {
			continue
		}
		for _, actual := range rule.ipBlocks {
			if actual == cidr {
				return true
			}
		}
	}
	return false
}

func requireNetworkPolicyEgressIntentHosts(t *testing.T, document *manifestDocument) []string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^\s*tetral\.ai/egress-intent:\s*"([^"]+)"\s*$`)
	matches := re.FindStringSubmatch(document.text)
	if len(matches) != 2 {
		t.Fatalf("%s %s/%s has broad HTTPS egress but no tetral.ai/egress-intent annotation", document.file, document.kind, document.name)
	}
	var hosts []string
	for _, raw := range strings.Split(matches[1], ",") {
		host := strings.TrimSpace(raw)
		if host != "" {
			hosts = append(hosts, host)
		}
	}
	if len(hosts) == 0 {
		t.Fatalf("%s %s/%s has empty tetral.ai/egress-intent annotation", document.file, document.kind, document.name)
	}
	return hosts
}

func requireNoNetworkPolicyIngressPort(t *testing.T, document *manifestDocument, port int) {
	t.Helper()
	for _, rule := range parseNetworkPolicyIngressRules(t, document) {
		if intSliceContains(rule.ports, port) {
			t.Fatalf("%s %s/%s unexpectedly exposes ingress port %d", document.file, document.kind, document.name, port)
		}
	}
}

func requireKubeDNSEgress(t *testing.T, document *manifestDocument) {
	t.Helper()
	var dnsRules []networkPolicyRule
	for _, rule := range parseNetworkPolicyEgressRules(t, document) {
		for _, transport := range rule.transports {
			if transport.port == 53 {
				dnsRules = append(dnsRules, rule)
				break
			}
		}
	}
	if len(dnsRules) != 1 {
		t.Fatalf("%s %s/%s DNS egress rules = %d; want exactly 1", document.file, document.kind, document.name, len(dnsRules))
	}
	rule := dnsRules[0]
	wantTransports := []networkPolicyTransport{
		{protocol: "TCP", port: 53},
		{protocol: "UDP", port: 53},
	}
	gotTransports := append([]networkPolicyTransport(nil), rule.transports...)
	sort.Slice(gotTransports, func(i int, j int) bool {
		if gotTransports[i].protocol != gotTransports[j].protocol {
			return gotTransports[i].protocol < gotTransports[j].protocol
		}
		return gotTransports[i].port < gotTransports[j].port
	})
	if !reflect.DeepEqual(gotTransports, wantTransports) {
		t.Fatalf("%s %s/%s DNS transports = %#v; want %#v", document.file, document.kind, document.name, gotTransports, wantTransports)
	}
	if len(rule.peers) == 0 {
		t.Fatalf("%s %s/%s DNS rule has no peers", document.file, document.kind, document.name)
	}
	if len(rule.ipBlocks) != 0 {
		t.Fatalf("%s %s/%s DNS rule has ipBlock peers %#v; every peer must carry both DNS selectors", document.file, document.kind, document.name, rule.ipBlocks)
	}
	for _, peer := range rule.peers {
		if peer.namespaceLabels["kubernetes.io/metadata.name"] != "kube-system" {
			t.Fatalf("%s %s/%s DNS peer namespaceSelector labels = %#v; want kubernetes.io/metadata.name=kube-system", document.file, document.kind, document.name, peer.namespaceLabels)
		}
		if peer.podLabels["k8s-app"] != "kube-dns" {
			t.Fatalf("%s %s/%s DNS peer podSelector labels = %#v; want k8s-app=kube-dns", document.file, document.kind, document.name, peer.podLabels)
		}
	}
}

func requireNoBroadNetworkPolicyIngressPeers(t *testing.T, document *manifestDocument, port int) {
	t.Helper()
	if err := validateNoBroadNetworkPolicyIngressPeers(t, document, port); err != nil {
		t.Fatal(err)
	}
}

func requireNoBroadNetworkPolicyEgressPeers(t *testing.T, document *manifestDocument, port int) {
	t.Helper()
	if err := validateNoBroadNetworkPolicyEgressPeers(t, document, port); err != nil {
		t.Fatal(err)
	}
}

func validateNoBroadNetworkPolicyIngressPeers(t *testing.T, document *manifestDocument, port int) error {
	t.Helper()
	for _, rule := range parseNetworkPolicyIngressRules(t, document) {
		if !intSliceContains(rule.ports, port) {
			continue
		}
		if len(rule.peers) == 0 {
			return fmt.Errorf("%s %s/%s has broad ingress rule without peers on port %d", document.file, document.kind, document.name, port)
		}
		for _, peer := range rule.peers {
			if peer.namespace == "" || (peer.podName == "" && peer.podPartOf == "") {
				return fmt.Errorf("%s %s/%s has broad ingress peer %#v on port %d", document.file, document.kind, document.name, peer, port)
			}
		}
	}
	return nil
}

func validateNoBroadNetworkPolicyEgressPeers(t *testing.T, document *manifestDocument, port int) error {
	t.Helper()
	for _, rule := range parseNetworkPolicyEgressRules(t, document) {
		if !intSliceContains(rule.ports, port) {
			continue
		}
		if len(rule.peers) == 0 && len(rule.ipBlocks) == 0 {
			return fmt.Errorf("%s %s/%s has broad egress rule without peers on port %d", document.file, document.kind, document.name, port)
		}
		for _, peer := range rule.peers {
			if peer.namespace == "" || (peer.podName == "" && peer.podPartOf == "") {
				return fmt.Errorf("%s %s/%s has broad egress peer %#v on port %d", document.file, document.kind, document.name, peer, port)
			}
		}
		for _, cidr := range rule.ipBlocks {
			if cidr == "" || cidr == "0.0.0.0/0" {
				return fmt.Errorf("%s %s/%s has broad egress ipBlock %q on port %d", document.file, document.kind, document.name, cidr, port)
			}
		}
	}
	return nil
}

func parseNetworkPolicyIngressRules(t *testing.T, document *manifestDocument) []networkPolicyRule {
	t.Helper()
	return parseNetworkPolicyRules(t, document, "ingress", "from")
}

func parseNetworkPolicyEgressRules(t *testing.T, document *manifestDocument) []networkPolicyRule {
	t.Helper()
	return parseNetworkPolicyRules(t, document, "egress", "to")
}

func parseNetworkPolicyRules(t *testing.T, document *manifestDocument, section string, peerField string) []networkPolicyRule {
	t.Helper()
	lines := strings.Split(document.text, "\n")
	policyNamespace := requireMetadataNamespace(t, document)
	var rules []networkPolicyRule
	inSection := false
	var current []string
	for _, line := range lines {
		if strings.TrimSpace(line) == section+":" {
			inSection = true
			continue
		}
		if !inSection {
			continue
		}
		if line != "" && strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") {
			break
		}
		if line != "" && !strings.HasPrefix(line, " ") {
			break
		}
		if strings.HasPrefix(line, "    - ") {
			if len(current) > 0 {
				rules = append(rules, parseNetworkPolicyRule(t, current, policyNamespace, peerField))
			}
			current = []string{line}
			continue
		}
		if current != nil {
			current = append(current, line)
		}
	}
	if len(current) > 0 {
		rules = append(rules, parseNetworkPolicyRule(t, current, policyNamespace, peerField))
	}
	if len(rules) == 0 {
		t.Fatalf("%s %s/%s missing %s rules", document.file, document.kind, document.name, section)
	}
	return rules
}

func parseNetworkPolicyRule(t *testing.T, lines []string, policyNamespace string, peerField string) networkPolicyRule {
	t.Helper()
	if err := validateRecognizedNetworkPolicyRuleShape(lines); err != nil {
		t.Fatalf("unrecognized NetworkPolicy rule shape: %v", err)
	}
	var rule networkPolicyRule
	var peerLines []string
	inPeers := false
	protocol := "TCP"
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		listValue := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		if trimmed == peerField+":" || strings.HasPrefix(trimmed, "- "+peerField+":") {
			inPeers = true
			continue
		}
		if trimmed == "ports:" {
			inPeers = false
			if len(peerLines) > 0 {
				appendNetworkPolicyPeerOrIPBlock(t, &rule, peerLines, policyNamespace)
				peerLines = nil
			}
			continue
		}
		if inPeers && strings.HasPrefix(line, "        - ") {
			if len(peerLines) > 0 {
				appendNetworkPolicyPeerOrIPBlock(t, &rule, peerLines, policyNamespace)
			}
			peerLines = []string{line}
			continue
		}
		if inPeers && peerLines != nil {
			peerLines = append(peerLines, line)
		}
		if strings.HasPrefix(listValue, "protocol: ") {
			protocol = cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(listValue, "protocol: ")))
			continue
		}
		if strings.HasPrefix(trimmed, "port: ") {
			var port int
			if _, err := fmt.Sscanf(trimmed, "port: %d", &port); err != nil {
				t.Fatalf("parse NetworkPolicy port %q: %v", trimmed, err)
			}
			rule.ports = append(rule.ports, port)
			rule.transports = append(rule.transports, networkPolicyTransport{protocol: protocol, port: port})
			protocol = "TCP"
		}
	}
	if len(peerLines) > 0 {
		appendNetworkPolicyPeerOrIPBlock(t, &rule, peerLines, policyNamespace)
	}
	return rule
}

func validateRecognizedNetworkPolicyRuleShape(lines []string) error {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.ContainsAny(trimmed, "{}[]"):
			return fmt.Errorf("flow-style YAML is not supported: %q", trimmed)
		case strings.HasPrefix(trimmed, "- port:"):
			return fmt.Errorf("port-first entries are not supported: %q", trimmed)
		case strings.HasPrefix(trimmed, "endPort:"):
			return fmt.Errorf("port ranges are not supported: %q", trimmed)
		case strings.HasPrefix(trimmed, "matchExpressions:"):
			return fmt.Errorf("selector expressions are not supported: %q", trimmed)
		}
	}
	return nil
}

func appendNetworkPolicyPeerOrIPBlock(t *testing.T, rule *networkPolicyRule, lines []string, policyNamespace string) {
	t.Helper()
	hasIPBlock := false
	cidr := ""
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		listValue := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		if listValue == "ipBlock:" || strings.HasPrefix(listValue, "ipBlock: ") {
			hasIPBlock = true
		}
		if index := strings.Index(listValue, "cidr:"); index >= 0 {
			value := strings.TrimSpace(listValue[index+len("cidr:"):])
			value = strings.Trim(value, "{},")
			cidr = cleanManifestListValue(value)
		}
	}
	if hasIPBlock {
		rule.ipBlocks = append(rule.ipBlocks, cidr)
		return
	}
	rule.peers = append(rule.peers, parseNetworkPolicyPeer(t, lines, policyNamespace))
}

func parseNetworkPolicyPeer(t *testing.T, lines []string, policyNamespace string) networkPolicyPeer {
	t.Helper()
	peer := networkPolicyPeer{
		namespace:       policyNamespace,
		namespaceLabels: map[string]string{},
		podLabels:       map[string]string{},
	}
	selector := ""
	selectorIndent := -1
	matchLabelsIndent := -1
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		isListItem := strings.HasPrefix(trimmed, "- ")
		listValue := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		indent := len(line) - len(strings.TrimLeft(line, " "))
		if isListItem {
			indent += 2
		}
		switch listValue {
		case "namespaceSelector:":
			selector = "namespace"
			selectorIndent = indent
			matchLabelsIndent = -1
			continue
		case "podSelector:":
			selector = "pod"
			selectorIndent = indent
			matchLabelsIndent = -1
			continue
		case "matchLabels:":
			if selector != "" && indent == selectorIndent+2 {
				matchLabelsIndent = indent
			}
			continue
		}
		if selector != "" && indent <= selectorIndent {
			selector = ""
			selectorIndent = -1
			matchLabelsIndent = -1
		}
		if matchLabelsIndent < 0 || indent != matchLabelsIndent+2 {
			continue
		}
		key, value, ok := strings.Cut(listValue, ": ")
		if !ok {
			continue
		}
		value = cleanManifestListValue(value)
		if selector == "namespace" {
			peer.namespaceLabels[key] = value
			if key == "kubernetes.io/metadata.name" {
				peer.namespace = value
			}
			continue
		}
		peer.podLabels[key] = value
		switch key {
		case "app.kubernetes.io/name":
			peer.podName = value
		case "app.kubernetes.io/part-of":
			peer.podPartOf = value
		}
	}
	return peer
}

func intSliceContains(values []int, expected int) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func requireSubjectNamespaces(t *testing.T, document *manifestDocument, subjects []rbacSubject, expected map[string]string) {
	t.Helper()
	if len(subjects) != len(expected) {
		t.Fatalf("%s %s/%s subjects = %#v; want names/namespaces %#v", document.file, document.kind, document.name, subjects, expected)
	}
	for _, subject := range subjects {
		want, ok := expected[subject.name]
		if !ok {
			t.Fatalf("%s %s/%s unexpected subject %s/%s", document.file, document.kind, document.name, subject.namespace, subject.name)
		}
		if subject.namespace != want {
			t.Fatalf("%s %s/%s subject %s namespace = %q; want %q", document.file, document.kind, document.name, subject.name, subject.namespace, want)
		}
	}
}

type agentRuntimePackageMetadata struct {
	Scripts map[string]string `json:"scripts"`
	Tetral  struct {
		AgentRuntimePod struct {
			Entrypoint       string   `json:"entrypoint"`
			BuildArtifact    string   `json:"buildArtifact"`
			Image            string   `json:"image"`
			ContainerCommand []string `json:"containerCommand"`
		} `json:"agentRuntimePod"`
	} `json:"tetral"`
}

type providerGatewayPackageMetadata struct {
	Scripts map[string]string `json:"scripts"`
	Tetral  struct {
		ProviderGateway struct {
			Entrypoint       string   `json:"entrypoint"`
			BuildArtifact    string   `json:"buildArtifact"`
			Image            string   `json:"image"`
			ContainerCommand []string `json:"containerCommand"`
		} `json:"providerGateway"`
		MCPConnector struct {
			Entrypoint       string   `json:"entrypoint"`
			BuildArtifact    string   `json:"buildArtifact"`
			Image            string   `json:"image"`
			ContainerCommand []string `json:"containerCommand"`
		} `json:"mcpConnector"`
	} `json:"tetral"`
}

func readAgentRuntimePackageMetadata(t *testing.T) agentRuntimePackageMetadata {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("..", "..", "services", "agent-runtime", "package.json"))
	if err != nil {
		t.Fatalf("read agent-runtime package.json: %v", err)
	}
	var metadata agentRuntimePackageMetadata
	if err := json.Unmarshal(source, &metadata); err != nil {
		t.Fatalf("parse agent-runtime package.json: %v", err)
	}
	return metadata
}

func readProviderGatewayPackageMetadata(t *testing.T) providerGatewayPackageMetadata {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("..", "..", "services", "gateway", "package.json"))
	if err != nil {
		t.Fatalf("read gateway package.json: %v", err)
	}
	var metadata providerGatewayPackageMetadata
	if err := json.Unmarshal(source, &metadata); err != nil {
		t.Fatalf("parse gateway package.json: %v", err)
	}
	return metadata
}

func readAgentRuntimeDockerfile(t *testing.T) string {
	t.Helper()
	return string(mustReadFile(t, filepath.Join("..", "..", "services", "agent-runtime", "Dockerfile")))
}

func readProviderGatewayDockerfile(t *testing.T) string {
	t.Helper()
	return string(mustReadFile(t, filepath.Join("..", "..", "services", "gateway", "Dockerfile")))
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	source, err := os.ReadFile(path) // #nosec G304 -- tests read repository fixture paths only.
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return source
}

func serviceAccountNamespace(t *testing.T, value string) string {
	t.Helper()
	namespace, _, ok := strings.Cut(value, "/")
	if !ok || namespace == "" {
		t.Fatalf("service account %q is not namespace/name", value)
	}
	return namespace
}

func serviceAccountNamespaceFromAllowlist(t *testing.T, value string, account string) string {
	t.Helper()
	requireServiceAccountAllowlistContains(t, value, account)
	return serviceAccountNamespace(t, account)
}

func requireServiceAccountAllowlistContains(t *testing.T, value string, account string) {
	t.Helper()
	for _, part := range strings.Split(value, ",") {
		if strings.TrimSpace(part) == account {
			return
		}
	}
	t.Fatalf("service account allowlist %q does not contain %q", value, account)
}

func requireSelectorValue(t *testing.T, document *manifestDocument, key string) string {
	t.Helper()
	prefix := key + ": "
	for _, line := range strings.Split(document.text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) {
			return cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, prefix)))
		}
	}
	t.Fatalf("%s %s/%s missing selector %s", document.file, document.kind, document.name, key)
	return ""
}

func requireMetadataNamespace(t *testing.T, document *manifestDocument) string {
	t.Helper()
	lines := strings.Split(document.text, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != "metadata:" {
			continue
		}
		for _, metadataLine := range lines[index+1:] {
			if metadataLine != "" && !strings.HasPrefix(metadataLine, "  ") {
				break
			}
			if strings.HasPrefix(metadataLine, "  namespace: ") {
				return strings.TrimSpace(strings.TrimPrefix(metadataLine, "  namespace: "))
			}
		}
	}
	t.Fatalf("%s %s/%s missing metadata.namespace", document.file, document.kind, document.name)
	return ""
}

// peerPodSelectorLabel returns the single app.kubernetes.io/* podSelector matchLabel that
// appears under an internal gRPC NetworkPolicy peer (the namespaceSelector branch).
func peerPodSelectorLabel(t *testing.T, document *manifestDocument) (string, string) {
	t.Helper()
	for _, line := range strings.Split(document.text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "app.kubernetes.io/") && strings.Contains(trimmed, ": ") {
			key, value, _ := strings.Cut(trimmed, ": ")
			if value == "agent-runtime" {
				return strings.TrimSpace(key), strings.TrimSpace(value)
			}
		}
	}
	t.Fatalf("%s %s/%s missing Agent Runtime peer podSelector label", document.file, document.kind, document.name)
	return "", ""
}

func splitLabelSelector(t *testing.T, selector string) (string, string) {
	t.Helper()
	key, value, ok := strings.Cut(selector, "=")
	if !ok {
		t.Fatalf("label selector %q is not key=value", selector)
	}
	return strings.TrimSpace(key), strings.TrimSpace(value)
}

// agentRuntimeLabelSelectorCodeDefault reads the defaultAgentRuntimeLabelSelector literal
// straight from internal/kubernetes/config.go so the test tracks the real source rather
// than a duplicated constant.
func agentRuntimeLabelSelectorCodeDefault(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("..", "..", "internal", "kubernetes", "config.go"))
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	re := regexp.MustCompile(`defaultAgentRuntimeLabelSelector\s*=\s*"([^"]+)"`)
	matches := re.FindStringSubmatch(string(source))
	if len(matches) != 2 {
		t.Fatalf("config.go missing defaultAgentRuntimeLabelSelector literal")
	}
	return matches[1]
}

func TestKubernetesManifestSecurityBaseline(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, document := range documents {
		if document.kind == "Role" || document.kind == "ClusterRole" {
			if err := validateRBACSecurityBaseline(document.text); err != nil {
				t.Fatalf("%s %s/%s violates RBAC security baseline: %v", document.file, document.kind, document.name, err)
			}
		}
		for _, forbidden := range []string{
			"hostPath:",
			"hostNetwork: true",
			"privileged: true",
			"nodeName:",
			"kind: Ingress",
			"kind: Gateway",
			"kind: Kustomization",
			"kind: HelmRelease",
			"type: LoadBalancer",
			"external-dns.alpha.kubernetes.io",
		} {
			if strings.Contains(document.text, forbidden) {
				t.Fatalf("%s %s/%s contains forbidden manifest surface %q", document.file, document.kind, document.name, forbidden)
			}
		}
	}
}

func TestKubernetesManifestValidationUsesOnlyStdlib(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read manifest tests: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		for _, forbidden := range forbiddenManifestValidationDependencies() {
			if strings.Contains(string(source), forbidden) {
				t.Fatalf("%s imports or references %s; manifest validation must stay stdlib-only", entry.Name(), forbidden)
			}
		}
	}
}

func forbiddenManifestValidationDependencies() []string {
	return []string{
		"gopkg.in/" + "yaml.v3",
		"sigs.k8s.io/" + "yaml",
		"k8s.io/" + "api",
		"k8s.io/" + "apimachinery",
		"k8s.io/" + "client-go",
	}
}

func requireExactRBACRules(t *testing.T, document *manifestDocument, expected []rbacRule) {
	t.Helper()
	if err := validateExactRBACRules(document.text, expected); err != nil {
		t.Fatalf("%s %s/%s RBAC rules mismatch: %v", document.file, document.kind, document.name, err)
	}
}

func requireExactRBACSubjects(t *testing.T, document *manifestDocument, expected []rbacSubject) {
	t.Helper()
	if err := validateExactRBACSubjects(document.text, expected); err != nil {
		t.Fatalf("%s %s/%s RBAC subjects mismatch: %v", document.file, document.kind, document.name, err)
	}
}

func validateExactRBACSubjects(text string, expected []rbacSubject) error {
	actual, err := parseRBACSubjects(text)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("subjects = %#v; want %#v", actual, expected)
	}
	return nil
}

func requireExactRBACRoleRef(t *testing.T, document *manifestDocument, expected rbacRoleRef) {
	t.Helper()
	actual, err := parseRBACRoleRef(document.text)
	if err != nil {
		t.Fatalf("%s %s/%s RBAC roleRef parse failed: %v", document.file, document.kind, document.name, err)
	}
	if actual != expected {
		t.Fatalf("%s %s/%s roleRef = %#v; want %#v", document.file, document.kind, document.name, actual, expected)
	}
}

func validateExactRBACRules(text string, expected []rbacRule) error {
	actual, err := parseRBACRules(text)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("rules = %#v; want %#v", actual, expected)
	}
	return nil
}

func validateRBACSecurityBaseline(text string) error {
	rules, err := parseRBACRules(text)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		for _, resource := range rule.resources {
			if resource == "secrets" || resource == "*" {
				return fmt.Errorf("forbidden RBAC resource %q", resource)
			}
			if strings.Contains(resource, "/scale") {
				return fmt.Errorf("forbidden RBAC scale resource %q", resource)
			}
		}
		for _, verb := range rule.verbs {
			switch verb {
			case "*", "patch", "delete", "deletecollection", "update":
				return fmt.Errorf("forbidden RBAC verb %q", verb)
			case "create":
				if !rbacRuleAllowsOnlyTokenReviewCreate(rule) {
					return fmt.Errorf("forbidden RBAC create outside tokenreviews: %#v", rule)
				}
			}
		}
	}
	return nil
}

func rbacRuleAllowsOnlyTokenReviewCreate(rule rbacRule) bool {
	return reflect.DeepEqual(rule.apiGroups, []string{"authentication.k8s.io"}) &&
		reflect.DeepEqual(rule.resources, []string{"tokenreviews"}) &&
		reflect.DeepEqual(rule.verbs, []string{"create"})
}

func parseRBACRules(text string) ([]rbacRule, error) {
	var rules []rbacRule
	var current *rbacRule
	currentKey := ""
	inRules := false
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "rules:" {
			inRules = true
			continue
		}
		if !inRules {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			break
		}
		if strings.HasPrefix(line, "  - ") {
			rules = append(rules, rbacRule{})
			current = &rules[len(rules)-1]
			currentKey = strings.TrimSuffix(strings.TrimSpace(strings.TrimPrefix(line, "  - ")), ":")
			continue
		}
		if current == nil {
			return nil, fmt.Errorf("rules block has field before first rule: %s", line)
		}
		if strings.HasPrefix(line, "    ") && strings.HasSuffix(trimmed, ":") {
			currentKey = strings.TrimSuffix(trimmed, ":")
			continue
		}
		if strings.HasPrefix(line, "      - ") {
			value := cleanManifestListValue(strings.TrimSpace(strings.TrimPrefix(trimmed, "- ")))
			switch currentKey {
			case "apiGroups":
				current.apiGroups = append(current.apiGroups, value)
			case "resources":
				current.resources = append(current.resources, value)
			case "verbs":
				current.verbs = append(current.verbs, value)
			default:
				return nil, fmt.Errorf("unsupported RBAC rule key %q", currentKey)
			}
		}
	}
	for _, rule := range rules {
		if len(rule.apiGroups) == 0 || len(rule.resources) == 0 || len(rule.verbs) == 0 {
			return nil, fmt.Errorf("incomplete RBAC rule: %#v", rule)
		}
	}
	return rules, nil
}

func parseRBACSubjects(text string) ([]rbacSubject, error) {
	var subjects []rbacSubject
	var current *rbacSubject
	inSubjects := false
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "subjects:" {
			inSubjects = true
			continue
		}
		if !inSubjects {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			break
		}
		if strings.HasPrefix(line, "  - ") {
			subjects = append(subjects, rbacSubject{})
			current = &subjects[len(subjects)-1]
			key, value, ok := splitManifestScalar(strings.TrimSpace(strings.TrimPrefix(line, "  - ")))
			if !ok {
				return nil, fmt.Errorf("invalid subject line: %s", line)
			}
			assignRBACSubjectField(current, key, value)
			continue
		}
		if current == nil {
			return nil, fmt.Errorf("subjects block has field before first subject: %s", line)
		}
		if strings.HasPrefix(line, "    ") {
			key, value, ok := splitManifestScalar(trimmed)
			if !ok {
				return nil, fmt.Errorf("invalid subject field: %s", line)
			}
			assignRBACSubjectField(current, key, value)
		}
	}
	for _, subject := range subjects {
		if subject.kind == "" || subject.name == "" {
			return nil, fmt.Errorf("incomplete RBAC subject: %#v", subject)
		}
	}
	return subjects, nil
}

func assignRBACSubjectField(subject *rbacSubject, key string, value string) {
	switch key {
	case "kind":
		subject.kind = value
	case "name":
		subject.name = value
	case "namespace":
		subject.namespace = value
	}
}

func parseRBACRoleRef(text string) (rbacRoleRef, error) {
	var roleRef rbacRoleRef
	inRoleRef := false
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "roleRef:" {
			inRoleRef = true
			continue
		}
		if !inRoleRef {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			break
		}
		key, value, ok := splitManifestScalar(trimmed)
		if !ok {
			return rbacRoleRef{}, fmt.Errorf("invalid roleRef field: %s", line)
		}
		switch key {
		case "apiGroup":
			roleRef.apiGroup = value
		case "kind":
			roleRef.kind = value
		case "name":
			roleRef.name = value
		}
	}
	if roleRef.apiGroup == "" || roleRef.kind == "" || roleRef.name == "" {
		return rbacRoleRef{}, fmt.Errorf("incomplete roleRef: %#v", roleRef)
	}
	return roleRef, nil
}

func splitManifestScalar(text string) (string, string, bool) {
	key, value, ok := strings.Cut(text, ":")
	if !ok {
		return "", "", false
	}
	return strings.TrimSpace(key), cleanManifestListValue(strings.TrimSpace(value)), true
}

func cleanManifestListValue(value string) string {
	return strings.Trim(value, `"'`)
}

func mutateRBACRules(text string, extraRule string) string {
	index := strings.Index(text, "\n---")
	if index >= 0 {
		return text[:index] + extraRule + text[index:]
	}
	return text + extraRule
}

func mutateRBACSubjects(text string, extraSubject string) string {
	index := strings.Index(text, "\nroleRef:")
	if index >= 0 {
		return text[:index] + extraSubject + text[index:]
	}
	return text + extraSubject
}

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
	seen := map[string]bool{}
	for _, expected := range expectedFiles {
		if _, err := os.Stat(filepath.Join(root, expected)); err != nil {
			t.Fatalf("required manifest %s is missing: %v", expected, err)
		}
		seen[expected] = true
	}
	entries, err := filepath.Glob(filepath.Join(root, "*.yaml"))
	if err != nil {
		t.Fatalf("glob manifests: %v", err)
	}
	sort.Strings(entries)
	for _, entry := range entries {
		name := filepath.Base(entry)
		if !seen[name] {
			t.Fatalf("unexpected manifest file %s", name)
		}
	}

	var documents manifestDocuments
	for _, file := range expectedFiles {
		// #nosec G304 -- file comes from the hard-coded expected manifest list above.
		source, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, rawDocument := range strings.Split(string(source), "\n---") {
			text := strings.TrimSpace(rawDocument)
			if text == "" {
				continue
			}
			documents = append(documents, manifestDocument{
				file: file,
				kind: requireScalar(t, file, text, "kind"),
				name: requireMetadataName(t, file, text),
				text: text,
			})
		}
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
	path := filepath.Join(adapterDir, file)
	// #nosec G304 -- file is the hard-coded edge-gateway adapter manifest under test.
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var documents manifestDocuments
	for _, rawDocument := range strings.Split(string(source), "\n---") {
		text := strings.TrimSpace(rawDocument)
		if text == "" {
			continue
		}
		documents = append(documents, manifestDocument{
			file: filepath.Join("edge-gateway", file),
			kind: requireScalar(t, path, text, "kind"),
			name: requireMetadataName(t, path, text),
			text: text,
		})
	}
	return documents
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

func requireScalar(t *testing.T, file string, text string, key string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `:\s*([^#\n]+)$`)
	matches := re.FindStringSubmatch(text)
	if len(matches) != 2 {
		t.Fatalf("%s document missing top-level %s", file, key)
	}
	return strings.TrimSpace(matches[1])
}

func requireMetadataName(t *testing.T, file string, text string) string {
	t.Helper()
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != "metadata:" {
			continue
		}
		for _, metadataLine := range lines[index+1:] {
			if metadataLine != "" && !strings.HasPrefix(metadataLine, "  ") {
				break
			}
			if strings.HasPrefix(metadataLine, "  name: ") {
				return strings.TrimSpace(strings.TrimPrefix(metadataLine, "  name: "))
			}
		}
	}
	t.Fatalf("%s document missing metadata.name:\n%s", file, text)
	return ""
}

func requireContains(t *testing.T, document *manifestDocument, want string) {
	t.Helper()
	if !manifestTextContains(document.text, want) {
		t.Fatalf("%s %s/%s missing:\n%s", document.file, document.kind, document.name, want)
	}
}

func requireNotContains(t *testing.T, document *manifestDocument, forbidden string) {
	t.Helper()
	if strings.Contains(document.text, forbidden) {
		t.Fatalf("%s %s/%s contains forbidden text:\n%s", document.file, document.kind, document.name, forbidden)
	}
}

func readServiceLocalManifestText(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "services", path)) //nolint:gosec // repository-local manifest path.
	if err != nil {
		t.Fatalf("read service-local manifest %s: %v", path, err)
	}
	return string(body)
}

func requireEngineVaultKeyEnvCount(t *testing.T, source string, text string, want int) {
	t.Helper()
	if got := countManifestScalar(text, "name", "ENGINE_VAULT_KEY"); got != want {
		t.Fatalf("%s ENGINE_VAULT_KEY env count = %d; want %d", source, got, want)
	}
	if got := countManifestScalar(text, "key", "engine-vault-key"); got != want {
		t.Fatalf("%s engine-vault-key secret key count = %d; want %d", source, got, want)
	}
	if got := countManifestScalar(text, "name", "api-secrets"); got != want {
		t.Fatalf("%s api-secrets secret reference count = %d; want %d", source, got, want)
	}
}

var quotedManifestStringFieldPattern = regexp.MustCompile(`(?m)^([ \t]*(?:-[ \t]+)?(?:host|name|secretName):[ \t]*)["']([A-Za-z0-9_./:@+*=-]+)["']([ \t]*)$`)

func manifestTextContains(text string, want string) bool {
	if strings.Contains(text, want) {
		return true
	}
	if os.Getenv("TETRAL_KUBERNETES_MANIFEST_ROOT") == "" {
		return false
	}
	return strings.Contains(
		quotedManifestStringFieldPattern.ReplaceAllString(text, `$1$2$3`),
		quotedManifestStringFieldPattern.ReplaceAllString(want, `$1$2$3`),
	)
}

func countManifestScalar(text string, wantKey string, wantValue string) int {
	count := 0
	for _, line := range strings.Split(text, "\n") {
		item := strings.TrimSpace(line)
		item = strings.TrimSpace(strings.TrimPrefix(item, "- "))
		key, value, ok := splitManifestScalar(item)
		if ok && key == wantKey && value == wantValue {
			count++
		}
	}
	return count
}

func TestKubernetesManifestAudiencesAreAllowListed(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, document := range documents {
		for lineNumber, line := range strings.Split(document.text, "\n") {
			item := strings.TrimSpace(line)
			item = strings.TrimSpace(strings.TrimPrefix(item, "- "))
			key, value, parsed := splitManifestScalar(item)
			isAllowListedAudience := parsed && key == "audience" && value == "tetral-internal-grpc"
			if parsed && key == "audience" && !isAllowListedAudience {
				t.Fatalf("%s %s/%s line %d has non-allow-listed audience %q", document.file, document.kind, document.name, lineNumber+1, value)
			}
			if strings.Contains(line, "audience") && !isAllowListedAudience {
				t.Fatalf("%s %s/%s line %d has an audience declaration outside the allow-listed scalar form: %s", document.file, document.kind, document.name, lineNumber+1, strings.TrimSpace(line))
			}
			if parsed && key == "serviceAccountToken" && value != "" {
				t.Fatalf("%s %s/%s line %d must express serviceAccountToken as a block mapping: %s", document.file, document.kind, document.name, lineNumber+1, strings.TrimSpace(line))
			}
		}
	}
}

func TestKubernetesManifestDocumentsHaveRecognizedKindAndName(t *testing.T) {
	documents := readManifestDocuments(t)
	allowed := map[string]bool{
		"CiliumNetworkPolicy": true,
		"ClusterRole":         true,
		"ClusterRoleBinding":  true,
		"ConfigMap":           true,
		"CronJob":             true,
		"Deployment":          true,
		"DestinationRule":     true, "VirtualService": true, "PeerAuthentication": true, "AuthorizationPolicy": true,
		"HorizontalPodAutoscaler": true,
		"NetworkPolicy":           true,
		"Role":                    true,
		"RoleBinding":             true,
		"Service":                 true,
		"ServiceAccount":          true,
	}
	for _, document := range documents {
		if !allowed[document.kind] {
			t.Fatalf("%s contains unsupported kind %s", document.file, document.kind)
		}
		if document.name == "" {
			t.Fatalf("%s %s has empty metadata.name", document.file, document.kind)
		}
	}
}

func Example_manifestValidationScope() {
	fmt.Println("stdlib-only static Kubernetes manifest validation")
	// Output: stdlib-only static Kubernetes manifest validation
}
