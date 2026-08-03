package tetralsandbox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTetralSandboxManifestUsesQueueWorkersWithoutKubernetesAPIToken(t *testing.T) {
	deployment := readServiceLocalManifest(t, "deployment.yaml")
	for _, required := range []string{
		"serviceAccountName: sandbox",
		"automountServiceAccountToken: false",
		"name: TETRAL_R2_ACCOUNT_ID",
		"name: TETRAL_R2_PARENT_API_TOKEN",
		"name: TETRAL_R2_PARENT_ACCESS_KEY",
		"name: TETRAL_SANDBOX_JOB_LEASE_DURATION",
		"name: TETRAL_SANDBOX_PROVIDER_COMMAND_TIMEOUT",
		"name: TETRAL_SANDBOX_WORKER_CONCURRENCY",
		"name: TETRAL_RESOURCE_CRED_TTL",
		"name: TETRAL_RESOURCE_CRED_REFRESH_MARGIN",
		"name: TETRAL_RCLONE_VFS_CACHE_MAX_SIZE",
		"name: TETRAL_RCLONE_VFS_MIN_FREE",
		"name: TETRAL_GIT_PROXY_HOST",
	} {
		if !strings.Contains(deployment, required) {
			t.Fatalf("sandbox deployment missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"TETRAL_WORKSPACE_ID",
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH",
		"KUBERNETES_API_SERVER_URL",
		"KUBERNETES_API_CA_CERT_PATH",
		"kubernetes.default.svc",
		"serviceAccountToken:",
		"sandbox-kubernetes-api",
		"/var/run/secrets/kubernetes.io/serviceaccount",
	} {
		if strings.Contains(deployment, forbidden) {
			t.Fatalf("sandbox deployment must not contain %q", forbidden)
		}
	}
}

func TestTetralSandboxConfigMapCarriesResourceProjectionKnobs(t *testing.T) {
	configMap := readServiceLocalManifest(t, "configmap.yaml")
	for _, required := range []string{
		"TETRAL_BLOB_BUCKET: tetral-files",
		"TETRAL_R2_ACCOUNT_ID:",
		"TETRAL_RESOURCE_CRED_TTL: 24h",
		"TETRAL_RESOURCE_CRED_REFRESH_MARGIN: 30m",
		"TETRAL_RCLONE_VFS_CACHE_MAX_SIZE: 2G",
		"TETRAL_RCLONE_VFS_MIN_FREE: 1G",
		"TETRAL_GIT_PROXY_HOST: git.tetral.example",
	} {
		if !strings.Contains(configMap, required) {
			t.Fatalf("sandbox configmap missing %q", required)
		}
	}
	if strings.Contains(configMap, "TETRAL_WORKSPACE_ID") {
		t.Fatal("sandbox configmap must not pin a serving workspace")
	}
}

func TestTetralSandboxSecretExampleCarriesProviderCredentials(t *testing.T) {
	secret := readServiceLocalManifest(t, "secret.example.yaml")
	for _, required := range []string{
		"TETRAL_POSTGRES_DSN:",
		"name: sandbox-r2-parent",
		"TETRAL_R2_PARENT_API_TOKEN:",
		"TETRAL_R2_PARENT_ACCESS_KEY:",
	} {
		if !strings.Contains(secret, required) {
			t.Fatalf("sandbox secret example missing %q", required)
		}
	}
}

func TestTetralSandboxNetworkPolicyDocumentsR2MintingEgress(t *testing.T) {
	networkPolicy := readServiceLocalManifest(t, "networkpolicy.yaml")
	for _, required := range []string{
		"tetral.ai/egress-intent:",
		"api.cloudflare.com",
		"R2 temporary credential minting",
		"cidr: 0.0.0.0/0",
		"port: 443",
	} {
		if !strings.Contains(networkPolicy, required) {
			t.Fatalf("sandbox network policy missing %q", required)
		}
	}
	if strings.Contains(networkPolicy, "\n          port: 80\n") {
		t.Fatalf("sandbox network policy still allows cleartext broad egress: %s", networkPolicy)
	}
}

func readServiceLocalManifest(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("k8s", name)) //nolint:gosec // repository-local manifest fixture.
	if err != nil {
		t.Fatalf("read service-local manifest %s: %v", name, err)
	}
	return string(body)
}
