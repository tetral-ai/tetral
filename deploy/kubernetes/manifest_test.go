package kubernetesmanifest_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
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
	file   string
	kind   string
	name   string
	object map[string]any
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
	{
		file:            "computer-hub.yaml",
		name:            "computer-hub",
		namespace:       "tetral-system",
		binary:          "tetral-computer-hub",
		httpEnv:         "TETRAL_COMPUTER_HUB_HTTP_ADDR",
		httpPortName:    "http",
		metricsEnv:      "TETRAL_COMPUTER_HUB_METRICS_ADDR",
		metricsPortName: "metrics",
		publicFacing:    true,
		autoscaling:     true,
		requiredEnvVar:  []string{"TETRAL_DATABASE_URL", "TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64", "TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY", "TETRAL_COMPUTER_HUB_POD_NAME", "TETRAL_COMPUTER_HUB_SERVER_URL", "TETRAL_COMPUTER_HUB_GIT_PROXY_URL", "TETRAL_BLOB_ENDPOINT", "TETRAL_BLOB_BUCKET"},
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
	if workload.name == "api" || workload.name == "event-stream" || workload.name == "auth" || workload.name == "computer-hub" {
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
	requireContains(t, cronJob, "command:\n  - /usr/local/bin/tetral-cleanup")
	for _, envName := range []string{"TETRAL_DATABASE_URL", "TETRAL_CLEANUP_CLAIM_LIMIT"} {
		requireContains(t, cronJob, "name: "+envName)
	}
	// Every limit the cleanup phases enforce is at most one run late, in the
	// composed and the service-local manifest alike.
	local := manifestDocumentsOf(t, filepath.Join("..", "..", "services", "cleanup", "k8s", "cronjob.yaml"), "services/cleanup/k8s/cronjob.yaml")
	for _, document := range []*manifestDocument{cronJob, &local[0]} {
		for _, want := range []string{`schedule: "*/1 * * * *"`, "concurrencyPolicy: Forbid", "name: TETRAL_CLEANUP_REQUEST_CLAIM_TIMEOUT", "name: TETRAL_CLEANUP_REQUEST_PENDING_EXPIRY"} {
			requireContains(t, document, want)
		}
	}
	requireNotContains(t, cronJob, "TETRAL_WORKSPACE_ID")
	for _, container := range items(podSpec(t, cronJob), "containers") {
		requireContainerResourceBounds(t, "cleanup.yaml", text(container, "name"), container.(map[string]any))
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
		requireContains(t, deployment, "matchLabels:\n  app.kubernetes.io/name: "+workload.name)
		requireContains(t, deployment, "labels:\n  app.kubernetes.io/name: "+workload.name)
		requireContains(t, service, "selector:\n  app.kubernetes.io/name: "+workload.name)
		requireContains(t, networkPolicy, "podSelector:\n  matchLabels:\n    app.kubernetes.io/name: "+workload.name)
	}
}

func TestKubernetesManifestDeploymentTargetsCorrectWorkloadBinary(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, w := range workloadManifests {
		d := requireDocument(t, documents, w.file, "Deployment", w.name)
		requireContains(t, d, "serviceAccountName: "+w.name)
		if err := validateContainerCount(t, d, 1); err != nil {
			t.Fatalf("%s: %v", w.file, err)
		}
		container := w.name
		if w.name == "bridge" {
			container = "bridge-api"
		}
		requireContains(t, d, "containers:\n  - name: "+container)
		command := []string{"/usr/local/bin/" + w.binary}
		switch w.name {
		case "agent-runtime":
			command = readAgentRuntimePackageMetadata(t).Tetral.AgentRuntimePod.ContainerCommand
		case "provider-gateway":
			command = readProviderGatewayPackageMetadata(t).Tetral.ProviderGateway.ContainerCommand
		case "mcp-connector":
			command = readProviderGatewayPackageMetadata(t).Tetral.MCPConnector.ContainerCommand
		}
		requireContains(t, d, "command:\n  - "+strings.Join(command, "\n  - "))
		for _, e := range append([]string{w.httpEnv}, w.requiredEnvVar...) {
			requireContains(t, d, "name: "+e)
		}
	}
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

	dataDir := text(environment(t, deployment, "ENGINE_DATA_DIR"), "value")
	if dataDir == "" {
		requireContains(t, deployment, "name: ENGINE_DATA_DIR\nvalueFrom:\n  configMapKeyRef:\n    name: api-config\n    key: ENGINE_DATA_DIR")
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
			requireContains(t, service, "name: http\nport: 8080\ntargetPort: http")
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, "TETRAL_PROVIDER_GATEWAY_HTTP_ADDR", "0.0.0.0:8080")
			requireContains(t, deployment, "containerPort: 9090")
			requireContains(t, deployment, "name: provider-grpc")
			requireContains(t, service, "name: provider-grpc\nport: 9090\ntargetPort: provider-grpc")
			requireContains(t, deployment, "name: TETRAL_PROVIDER_GATEWAY_GRPC_ADDR\nvalue: 0.0.0.0:9090")
			requireContains(t, deployment, "containerPort: 9091")
			requireContains(t, deployment, "name: mcp-grpc")
			requireContains(t, service, "name: mcp-grpc\nport: 9091\ntargetPort: mcp-grpc")
			requireContains(t, deployment, "name: TETRAL_MCP_CONNECTOR_GRPC_ADDR\nvalue: 0.0.0.0:9091")
			continue
		}
		requireContains(t, deployment, "containerPort: 8080")
		requireContains(t, deployment, "name: "+workload.httpPortName)
		requireContains(t, service, "name: "+workload.httpPortName+"\nport: 8080\ntargetPort: "+workload.httpPortName)
		expectedHTTPAddress := ":8080"
		if workload.name == "agent-runtime" {
			expectedHTTPAddress = "0.0.0.0:8080"
		}
		requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, workload.httpEnv, expectedHTTPAddress)
		if workload.metricsEnv != "" {
			requireContains(t, deployment, "containerPort: 8081")
			requireContains(t, deployment, "name: "+workload.metricsPortName)
			metricsService := service
			if workload.name == "api" || workload.name == "event-stream" || workload.name == "auth" || workload.name == "computer-hub" {
				requireNotContains(t, service, "name: "+workload.metricsPortName+"\nport: 8081\ntargetPort: "+workload.metricsPortName)
				metricsService = requireDocument(t, documents, workload.file, "Service", workload.name+"-metrics")
				requireContains(t, metricsService, "selector:\n  app.kubernetes.io/name: "+workload.name)
				requireNotContains(t, metricsService, "tetral.ai/exposure: public")
			}
			requireContains(t, metricsService, "name: "+workload.metricsPortName+"\nport: 8081\ntargetPort: "+workload.metricsPortName)
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, workload.metricsEnv, ":8081")
		}
		if workload.internalGRPC {
			port := 9090
			if workload.name == "agent-runtime" {
				port = 19090
			}
			requireContains(t, deployment, fmt.Sprintf("containerPort: %d", port))
			requireContains(t, deployment, "name: "+workload.grpcPortName)
			serviceGRPCPort := "name: " + workload.grpcPortName + "\nport: 9090\ntargetPort: " + workload.grpcPortName
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
			requireContains(t, service, "name: grpc-check\nport: 9095\ntargetPort: grpc-check\nappProtocol: kubernetes.io/h2c")
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, "TETRAL_AUTH_GRPC_ADDR", ":9095")
			requireDeploymentEnvValueOrConfigMap(t, documents, deployment, workload, "TETRAL_AUTH_GRPC_TRANSPORT", "plaintext")
			requireNotContains(t, deployment, "containerPort: 9090")
		} else if slices.ContainsFunc(items(service.object, "spec", "ports"), func(port any) bool { return strings.HasPrefix(text(port, "targetPort"), "grpc") }) || has(t, deployment.object, "containerPort: 9090") {
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
		if count(deployment.object, map[string]any{"port": probePort}) < 2 {
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
		t.Fatalf("edge object count=%d want 12 application-owned resources", len(documents))
	}
	gateway := requireDocument(t, documents, "edge-gateway/envoy-gateway.yaml", "Gateway", "tetral-public-edge")
	for _, required := range []string{`gatewayClassName: "tetral-envoy-gateway"`, `hostname: "api.tetral.example"`, `hostname: "git.tetral.example"`, `name: "tetral-api-public-tls"`, `name: "tetral-git-public-tls"`, "name: http", "name: api-https", "name: git-https"} {
		requireContains(t, gateway, required)
	}
	requirePublicAPIRoutes(t, documents, "edge-gateway/envoy-gateway.yaml")
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
	requireContains(t, proxy, "accessLog:\n  disable: true")
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
	if count(patch.object, map[string]any{"value": true}) != 2 {
		t.Fatal("raw header patch must set exactly 2 reviewed fields")
	}
	for index := range documents {
		for _, forbidden := range []string{"nginx", "/internal/auth/authorize", "Ingress", "withRequestBody", "maxRequestBytes", "buffering"} {
			requireNotContains(t, &documents[index], forbidden)
		}
	}
}

func TestKubernetesHardenedEdgeRoutesComputerHubUnderExternalAuth(t *testing.T) {
	path := filepath.Join(topLevelManifestRoot(), "profiles", "hardened", "edge-gateway.yaml")
	documents := manifestDocumentsOf(t, path, "profiles/hardened/edge-gateway.yaml")
	requirePublicAPIRoutes(t, documents, "profiles/hardened/edge-gateway.yaml")
	for _, name := range []string{"auth", "api", "event-stream", "git-proxy", "computer-hub"} {
		policy := requireDocument(t, documents, "profiles/hardened/edge-gateway.yaml", "BackendTLSPolicy", name+"-edge")
		requireContains(t, policy, "name: "+name)
		requireContains(t, policy, "hostname: "+name+".tetral-system.svc.cluster.local")
	}
}

// edgeRouteRule is one rule of an HTTPRoute: its path matches in order and
// its single backend.
type edgeRouteRule struct {
	prefixes []string
	regexes  []string
	backend  string
}

func parseEdgeRouteRules(t *testing.T, route *manifestDocument) []edgeRouteRule {
	t.Helper()
	var rules []edgeRouteRule
	for _, rule := range items(route.object, "spec", "rules") {
		backends := items(rule, "backendRefs")
		if len(backends) != 1 || at(backends[0], "port") != 8080 {
			t.Fatal("route requires exactly one owning HTTP backend")
		}
		parsed := edgeRouteRule{backend: text(backends[0], "name")}
		for _, match := range items(rule, "matches") {
			switch value := text(match, "path", "value"); text(match, "path", "type") {
			case "PathPrefix":
				parsed.prefixes = append(parsed.prefixes, value)
			case "RegularExpression":
				parsed.regexes = append(parsed.regexes, value)
			default:
				t.Fatalf("unexpected path match %v", match)
			}
		}
		rules = append(rules, parsed)
	}
	return rules
}

// edgeBackendFor applies Gateway API path precedence to the PathPrefix rules:
// a prefix matches whole path elements and the longest match wins.
func edgeBackendFor(rules []edgeRouteRule, path string) string {
	backend, longest := "", -1
	for _, rule := range rules {
		for _, prefix := range rule.prefixes {
			if (path == prefix || strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/")) && len(prefix) > longest {
				backend, longest = rule.backend, len(prefix)
			}
		}
	}
	return backend
}

// requirePublicAPIRoutes checks the public API route of one edge rendering:
// each path match's backend, the computer-hub prefixes in a rule ahead of the
// /v1 rule, path precedence that keeps every instance and client path off api,
// and the ext_authz SecurityPolicy targeting that route.
func requirePublicAPIRoutes(t *testing.T, documents manifestDocuments, file string) {
	t.Helper()
	rules := parseEdgeRouteRules(t, requireDocument(t, documents, file, "HTTPRoute", "tetral-public-api"))
	routes := map[string]string{}
	hubRule, apiRule := -1, -1
	for index, rule := range rules {
		for _, path := range append(append([]string{}, rule.prefixes...), rule.regexes...) {
			if _, duplicate := routes[path]; duplicate {
				t.Fatal("duplicate route")
			}
			routes[path] = rule.backend
		}
		if slices.Equal(rule.prefixes, []string{"/v1/client", "/v1/instance_requests", "/v1/instances"}) && rule.backend == "computer-hub" {
			hubRule = index
		}
		if slices.Equal(rule.prefixes, []string{"/v1"}) {
			apiRule = index
		}
	}
	want := map[string]string{`^/v1/sessions/[^/]+/events/stream$`: "event-stream", `^/v1/sessions/[^/]+/threads/[^/]+/stream$`: "event-stream", "/v1/api_keys": "auth", "/v1/client": "computer-hub", "/v1/instance_requests": "computer-hub", "/v1/instances": "computer-hub", "/v1": "api"}
	if !reflect.DeepEqual(routes, want) {
		t.Fatalf("%s API backend routes=%v want %v", file, routes, want)
	}
	if hubRule < 0 || apiRule < 0 || hubRule > apiRule {
		t.Fatalf("%s computer-hub rule %d must precede the /v1 rule %d", file, hubRule, apiRule)
	}
	for path, backend := range map[string]string{
		"/v1/client/events":                  "computer-hub",
		"/v1/client/ready":                   "computer-hub",
		"/v1/instances":                      "computer-hub",
		"/v1/instances/inst_a":               "computer-hub",
		"/v1/instances/inst_a/credentials":   "computer-hub",
		"/v1/instance_requests":              "computer-hub",
		"/v1/instance_requests/ireq_a/claim": "computer-hub",
		"/v1/instancesx":                     "api",
		"/v1/sessions":                       "api",
	} {
		if got := edgeBackendFor(rules, path); got != backend {
			t.Fatalf("%s routes %s to %s; want %s", file, path, got, backend)
		}
	}
	policy := requireDocument(t, documents, file, "SecurityPolicy", "tetral-public-api")
	requireContains(t, policy, "kind: HTTPRoute\nname: tetral-public-api")
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
		requireContainerResourceBounds(t, workload.file, workload.container, requireContainer(t, deployment, workload.container))
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
		documents := manifestDocumentsOf(t, path, path)
		for index := range documents {
			if kind := documents[index].kind; kind != "Deployment" && kind != "CronJob" {
				continue
			}
			for _, container := range items(podSpec(t, &documents[index]), "containers") {
				requireContainerResourceBounds(t, path, text(container, "name"), container.(map[string]any))
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
		requireContains(t, serviceLocalDocument(t, path, "ServiceAccount"), "automountServiceAccountToken: false")
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
		policy := serviceLocalDocument(t, test.path, "NetworkPolicy")
		requireContains(t, policy, "kind: NetworkPolicy")
		requireContains(t, policy, "name: "+test.name)
		requireContains(t, policy, "podSelector:\n  matchLabels:\n    app.kubernetes.io/name: "+test.name)
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
		{"cleanup.yaml", "cleanup-drives"},
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

func TestKubernetesManifestCoreControlPlaneUsesPinnedEgress(t *testing.T) {
	documents := readManifestDocuments(t)

	auth := requireDocument(t, documents, "auth.yaml", "NetworkPolicy", "auth")
	requireContains(t, auth, "policyTypes:\n  - Ingress\n  - Egress")
	requireNetworkPolicyEgressEdge(t, auth, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNoBroadNetworkPolicyEgressPeers(t, auth, 5432)
	requireKubeDNSEgress(t, auth)
	for _, forbidden := range []string{"agent-runtime", "gateway", "queue", "sandbox", "DAYTONA", "daytona", "0.0.0.0/0"} {
		requireNotContains(t, auth, forbidden)
	}

	api := requireDocument(t, documents, "api.yaml", "NetworkPolicy", "api")
	requireContains(t, api, "policyTypes:\n  - Ingress\n  - Egress")
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
	requireContains(t, queue, "policyTypes:\n  - Ingress\n  - Egress")
	requireNetworkPolicyEgressEdge(t, queue, 5432, networkPolicyPeer{namespace: "tetral-system", podName: "tetral-postgres"})
	requireNoBroadNetworkPolicyEgressPeers(t, queue, 5432)
	requireKubeDNSEgress(t, queue)
	for _, forbidden := range []string{"agent-runtime", "gateway", "DAYTONA", "daytona", "0.0.0.0/0", "kubernetes.default.svc"} {
		requireNotContains(t, queue, forbidden)
	}

	bridge := requireDocument(t, documents, "bridge.yaml", "NetworkPolicy", "bridge")
	requireContains(t, bridge, "policyTypes:\n  - Ingress\n  - Egress")
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
		var examples []any
		for _, example := range decodeDocuments(t, filepath.Join("..", "..", "services", test.service, "k8s", "secret.example.yaml")) {
			examples = append(examples, example)
		}
		for _, required := range test.required {
			if !has(t, examples, required) {
				t.Fatalf("%s secret.example.yaml missing %q", test.service, required)
			}
		}
		for _, forbidden := range test.forbidden {
			if has(t, examples, forbidden) {
				t.Fatalf("%s secret.example.yaml contains forbidden auth material %q", test.service, forbidden)
			}
		}
	}
}

// The Sandbox example Secret carries every tetral-database key a canonical
// workload reads through secretKeyRef, so a new per-workload key cannot ship
// without its example.
func TestDatabaseSecretExampleCoversTheKeysManifestsRead(t *testing.T) {
	read := map[string]bool{}
	var collect func(node any)
	collect = func(node any) {
		switch node := node.(type) {
		case map[string]any:
			if reference := at(node, "secretKeyRef"); text(reference, "name") == "tetral-database" {
				read[text(reference, "key")] = true
			}
			for _, child := range node {
				collect(child)
			}
		case []any:
			for _, child := range node {
				collect(child)
			}
		}
	}
	for _, document := range readManifestDocuments(t) {
		collect(document.object)
	}
	if len(read) == 0 {
		t.Fatal("canonical manifests reference no tetral-database keys")
	}
	for _, example := range decodeDocuments(t, filepath.Join("..", "..", "services", "sandbox", "k8s", "secret.example.yaml")) {
		if text(example, "metadata", "name") != "tetral-database" {
			continue
		}
		for key := range read {
			if _, present := fields(example, "stringData")[key]; !present {
				t.Errorf("services/sandbox/k8s/secret.example.yaml omits tetral-database/%s", key)
			}
		}
		return
	}
	t.Fatal("services/sandbox/k8s/secret.example.yaml has no tetral-database Secret")
}

// Every one-shot command the bootstrap guide runs from the release image must
// be one of the binaries that image builds; the build list is declared, not
// globbed, so a documented command can otherwise be missing from the image.
// The release image builds every command of the repository, the one-shot
// commands an operator runs among them: the packages of its build
// instruction are the main packages under cmd and services.
func TestReleaseImageBuildsEveryCommand(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	list := exec.Command("go", "list", "-f", `{{if eq .Name "main"}}{{.Dir}}{{end}}`, "./cmd/...", "./services/...")
	list.Dir = root
	output, err := list.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	var commands, built []string
	for _, dir := range strings.Fields(string(output)) {
		relative, err := filepath.Rel(root, dir)
		if err != nil {
			t.Fatal(err)
		}
		commands = append(commands, "./"+filepath.ToSlash(relative))
	}
	for _, instruction := range dockerfileInstructions(t, filepath.Join(root, "Dockerfile")) {
		if !strings.Contains(instruction, "go build ") {
			continue
		}
		for _, field := range strings.Fields(instruction) {
			if strings.HasPrefix(field, "./") {
				built = append(built, field)
			}
		}
	}
	sort.Strings(commands)
	sort.Strings(built)
	if len(commands) == 0 || !reflect.DeepEqual(built, commands) {
		t.Fatalf("the release image builds %v; the repository's commands are %v", built, commands)
	}
}

// dockerfileInstructions are the instructions of the Dockerfile at path,
// each on one line.
func dockerfileInstructions(t *testing.T, path string) []string {
	t.Helper()
	return strings.Split(strings.ReplaceAll(string(mustReadFile(t, path)), "\\\n", " "), "\n")
}

// requireImageRunsCommand requires the image the Dockerfile builds and the
// Deployment to agree with the package: the image's command and the
// container's are the package's, built from its entrypoint into its
// artifact, and the container runs as the image's user.
func requireImageRunsCommand(t *testing.T, dockerfile string, deployment *manifestDocument, buildScript, entrypoint, artifact, image string, command []string, imageDefault bool) {
	t.Helper()
	if !reflect.DeepEqual(command, []string{"bun", "/app/" + artifact}) {
		t.Fatalf("%s package command = %v; it does not run its build artifact %s", deployment.name, command, artifact)
	}
	if !strings.Contains(buildScript, entrypoint) {
		t.Fatalf("%s build script does not build its entrypoint %s", deployment.name, entrypoint)
	}
	var user string
	var imageCommand []string
	for _, instruction := range dockerfileInstructions(t, dockerfile) {
		if arguments, found := strings.CutPrefix(instruction, "CMD "); found {
			if err := json.Unmarshal([]byte(arguments), &imageCommand); err != nil {
				t.Fatalf("%s: CMD %s: %v", dockerfile, arguments, err)
			}
		}
		if arguments, found := strings.CutPrefix(instruction, "USER "); found {
			user = strings.TrimSpace(arguments)
		}
	}
	if imageDefault && !reflect.DeepEqual(imageCommand, command) {
		t.Fatalf("%s runs %v; the package's command is %v", dockerfile, imageCommand, command)
	}
	container := items(podSpec(t, deployment), "containers")[0]
	if got := stringsAt(container, "command"); !reflect.DeepEqual(got, command) {
		t.Fatalf("%s container runs %v; the package's command is %v", deployment.name, got, command)
	}
	if got := text(container, "image"); got != image {
		t.Fatalf("%s container image = %q; the package's image is %q", deployment.name, got, image)
	}
	if got := text(container, "securityContext", "runAsUser") + ":" + text(container, "securityContext", "runAsGroup"); got != user {
		t.Fatalf("%s container runs as %s; its image's user is %s", deployment.name, got, user)
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
	requireContains(t, deployment, "name: metrics\ncontainerPort: 8081")
	requireContains(t, service, "name: http\nport: 8080\ntargetPort: http")
	requireContains(t, service, "name: metrics\nport: 8081\ntargetPort: metrics")
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
	requireContains(t, networkPolicy, "policyTypes:\n  - Ingress\n  - Egress")
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
	requireContains(t, hpa, "scaleTargetRef:\n  apiVersion: apps/v1\n  kind: Deployment\n  name: git-proxy")
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
		requireEngineVaultKeyEnvCount(t, deployment.file, deployment.object, expectation.count)
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
		requireEngineVaultKeyEnvCount(t, path, serviceLocalWorkload(t, path).object, count)
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
		requireNotContains(t, serviceLocalWorkload(t, path), "name: ENGINE_VAULT_KEY")
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
		"TETRAL_WEB_CONNECTOR_GRPC_ADDR":                         "dns:///web-connector.tetral-system.svc.cluster.local:9092",
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
	requireContains(t, deployment, "- configMap:\n    name: kube-root-ca.crt\n    items:\n      - key: ca.crt\n        path: ca.crt")
	if outbound := requireDeploymentEnvValue(t, deployment, "TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH"); outbound == requireDeploymentEnvValue(t, deployment, "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH") {
		t.Fatalf("Runtime Pod outbound internal gRPC token path and TokenReview reviewer token path are both %q; want separate paths", outbound)
	}
}

func TestKubernetesManifestAgentRuntimePodEgressIsBounded(t *testing.T) {
	documents := readManifestDocuments(t)
	networkPolicy := requireDocument(t, documents, "agent-runtime.yaml", "NetworkPolicy", "agent-runtime")

	requireContains(t, networkPolicy, "policyTypes:\n  - Ingress\n  - Egress")
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
	requireContains(t, networkPolicy, "protocol: UDP\nport: 53")
	requireContains(t, networkPolicy, "protocol: TCP\nport: 53")
	// A gateway's gRPC ports, its ingress from the Runtime pods and their
	// egress to it name the same transports.
	for _, name := range []string{"provider-gateway", "mcp-connector", "web-connector"} {
		service := requireDocument(t, documents, name+".yaml", "Service", name)
		gateway := requireDocument(t, documents, name+".yaml", "NetworkPolicy", name)
		if err := validateGatewayRuntimeNetworkPolicyEdgesArePaired(t, service, gateway, networkPolicy); err != nil {
			t.Fatal(err)
		}
	}
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
	deployment := requireDocument(t, readManifestDocuments(t), "agent-runtime.yaml", "Deployment", "agent-runtime")
	metadata := readAgentRuntimePackageMetadata(t)
	pod := metadata.Tetral.AgentRuntimePod
	requireImageRunsCommand(t, filepath.Join("..", "..", "services", "agent-runtime", "Dockerfile"), deployment,
		metadata.Scripts["build"], pod.Entrypoint, pod.BuildArtifact, pod.Image, pod.ContainerCommand, true)
}

func TestKubernetesManifestGatewayServiceBuildArtifactMapping(t *testing.T) {
	documents := readManifestDocuments(t)
	metadata := readProviderGatewayPackageMetadata(t)
	dockerfile := filepath.Join("..", "..", "services", "gateway", "Dockerfile")
	// Both commands ship in one image, whose own command is Provider Gateway's.
	provider, connector := metadata.Tetral.ProviderGateway, metadata.Tetral.MCPConnector
	requireImageRunsCommand(t, dockerfile, requireDocument(t, documents, "provider-gateway.yaml", "Deployment", "provider-gateway"),
		metadata.Scripts["build"], provider.Entrypoint, provider.BuildArtifact, provider.Image, provider.ContainerCommand, true)
	requireImageRunsCommand(t, dockerfile, requireDocument(t, documents, "mcp-connector.yaml", "Deployment", "mcp-connector"),
		metadata.Scripts["build"], connector.Entrypoint, connector.BuildArtifact, provider.Image, connector.ContainerCommand, false)
	if connector.Image != provider.Image {
		t.Fatalf("mcp connector image = %q; want provider image %q", connector.Image, provider.Image)
	}
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
	requireContains(t, serviceLocalWorkload(t, filepath.Join("agent-runtime", "k8s", "deployment.yaml")), "terminationGracePeriodSeconds: 90")
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
		"cronjob-drives.yaml",
		"networkpolicy.yaml",
		"networkpolicy-drives.yaml",
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
	bridgePeerNamespace, _, _ := agentRuntimePeer(t, bridgePolicy)
	gatewayPeerNamespace, _, _ := agentRuntimePeer(t, gatewayPolicy)

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

	runnerSubjects, err := parseRBACSubjects(runnerBinding)
	if err != nil {
		t.Fatalf("%s %s/%s parse subjects: %v", runnerBinding.file, runnerBinding.kind, runnerBinding.name, err)
	}
	requireSubjectNamespaces(t, runnerBinding, runnerSubjects, map[string]string{"job-runner": "tetral-system"})

	for _, grant := range internalGRPCTokenReviewGrants() {
		tokenReviewBinding := requireDocument(t, documents, "internal-grpc-tokenreview-rbac.yaml", "ClusterRoleBinding", grant.name)
		tokenReviewSubjects, err := parseRBACSubjects(tokenReviewBinding)
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
	_, bridgePeerKey, bridgePeerValue := agentRuntimePeer(t, bridgePolicy)
	_, gatewayPeerKey, gatewayPeerValue := agentRuntimePeer(t, gatewayPolicy)

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
	} {
		if site.key != "app.kubernetes.io/name" || site.value != "agent-runtime" {
			t.Fatalf("%s label = %q=%q; want app.kubernetes.io/name=agent-runtime", site.source, site.key, site.value)
		}
	}
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

// agentRuntimePeer is the peer of the policy's rules that selects the Agent
// Runtime pods: the namespace it names and the label that selects them.
func agentRuntimePeer(t *testing.T, document *manifestDocument) (namespace string, key string, value string) {
	t.Helper()
	rules := parseNetworkPolicyRules(t, document, "ingress", "from")
	if len(items(document.object, "spec", "egress")) > 0 {
		rules = append(rules, parseNetworkPolicyRules(t, document, "egress", "to")...)
	}
	for _, rule := range rules {
		for _, peer := range rule.peers {
			for key, value := range peer.podLabels {
				if value == "agent-runtime" {
					return peer.namespace, key, value
				}
			}
		}
	}
	t.Fatalf("%s %s/%s has no Agent Runtime peer", document.file, document.kind, document.name)
	return "", "", ""
}

func splitLabelSelector(t *testing.T, selector string) (string, string) {
	t.Helper()
	key, value, ok := strings.Cut(selector, "=")
	if !ok {
		t.Fatalf("label selector %q is not key=value", selector)
	}
	return strings.TrimSpace(key), strings.TrimSpace(value)
}

func TestKubernetesManifestSecurityBaseline(t *testing.T) {
	documents := readManifestDocuments(t)
	for _, document := range documents {
		if document.kind == "Role" || document.kind == "ClusterRole" {
			if err := validateRBACSecurityBaseline(&document); err != nil {
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
			if has(t, document.object, forbidden) {
				t.Fatalf("%s %s/%s contains forbidden manifest surface %q", document.file, document.kind, document.name, forbidden)
			}
		}
	}
}

func requireExactRBACRules(t *testing.T, document *manifestDocument, expected []rbacRule) {
	t.Helper()
	if err := validateExactRBACRules(document, expected); err != nil {
		t.Fatalf("%s %s/%s RBAC rules mismatch: %v", document.file, document.kind, document.name, err)
	}
}

func requireExactRBACSubjects(t *testing.T, document *manifestDocument, expected []rbacSubject) {
	t.Helper()
	if err := validateExactRBACSubjects(document, expected); err != nil {
		t.Fatalf("%s %s/%s RBAC subjects mismatch: %v", document.file, document.kind, document.name, err)
	}
}

func validateExactRBACSubjects(document *manifestDocument, expected []rbacSubject) error {
	actual, err := parseRBACSubjects(document)
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
	actual, err := parseRBACRoleRef(document)
	if err != nil {
		t.Fatalf("%s %s/%s RBAC roleRef parse failed: %v", document.file, document.kind, document.name, err)
	}
	if actual != expected {
		t.Fatalf("%s %s/%s roleRef = %#v; want %#v", document.file, document.kind, document.name, actual, expected)
	}
}

func validateExactRBACRules(document *manifestDocument, expected []rbacRule) error {
	actual, err := parseRBACRules(document)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("rules = %#v; want %#v", actual, expected)
	}
	return nil
}

func validateRBACSecurityBaseline(document *manifestDocument) error {
	rules, err := parseRBACRules(document)
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

func requireEngineVaultKeyEnvCount(t *testing.T, source string, node any, want int) {
	t.Helper()
	for _, reference := range []map[string]any{{"name": "ENGINE_VAULT_KEY"}, {"key": "engine-vault-key"}, {"name": "api-secrets"}} {
		if got := count(node, reference); got != want {
			t.Fatalf("%s holds %v %d times; want %d", source, reference, got, want)
		}
	}
}

func TestKubernetesManifestAudiencesAreAllowListed(t *testing.T) {
	var check func(document *manifestDocument, node any)
	check = func(document *manifestDocument, node any) {
		switch node := node.(type) {
		case map[string]any:
			for key, child := range node {
				if key == "audience" && child != "tetral-internal-grpc" || key != "audience" && strings.Contains(key, "audience") {
					t.Fatalf("%s %s/%s has an audience outside the allow list: %s: %v", document.file, document.kind, document.name, key, child)
				}
				if _, mapping := child.(map[string]any); key == "serviceAccountToken" && !mapping {
					t.Fatalf("%s %s/%s has a serviceAccountToken that is no mapping: %v", document.file, document.kind, document.name, child)
				}
				check(document, child)
			}
		case []any:
			for _, child := range node {
				check(document, child)
			}
		case string:
			if strings.Contains(node, "audience") {
				t.Fatalf("%s %s/%s names an audience in a value: %s", document.file, document.kind, document.name, node)
			}
		}
	}
	documents := readManifestDocuments(t)
	for index := range documents {
		check(&documents[index], documents[index].object)
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
