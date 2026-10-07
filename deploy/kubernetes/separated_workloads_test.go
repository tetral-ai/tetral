package kubernetesmanifest_test

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func isSeparatedWorkload(name string) bool {
	switch name {
	case "bridge", "job-runner", "provider-gateway", "mcp-connector", "web-connector":
		return true
	}
	return false
}

type separatedContract struct {
	container                                   string
	ports                                       map[string]int
	servicePorts                                map[string]int
	httpEnv, httpAddr, probePort, health, ready string
	env                                         map[string]string
	secrets                                     map[string]string
	ingress, egress                             map[int][]networkPolicyPeer
}

func separatedContracts() map[string]separatedContract {
	runtime := networkPolicyPeer{namespace: "tetral-agent-runtime", podName: "agent-runtime"}
	sys := func(name string) networkPolicyPeer {
		return networkPolicyPeer{namespace: "tetral-system", podName: name}
	}
	metrics := networkPolicyPeer{namespace: "tetral-system", podPartOf: "tetral"}
	return map[string]separatedContract{
		"bridge": {container: "bridge-api", ports: map[string]int{"http": 8080, "grpc": 9090}, servicePorts: map[string]int{"http": 8080, "grpc": 9090}, httpEnv: "TETRAL_BRIDGE_API_HTTP_ADDR", httpAddr: ":8080", probePort: "http", health: "/health", ready: "/ready",
			env:     map[string]string{"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime,tetral-system/provider-gateway,tetral-system/mcp-connector", "TETRAL_BRIDGE_MCP_CONNECTOR_GRPC_ADDR": "mcp-connector.tetral-system.svc.cluster.local:9091", "TETRAL_BRIDGE_GATEWAY_TOKEN_PATH": "/var/run/secrets/tetral-internal-grpc/mcp-connector/token"},
			secrets: map[string]string{"TETRAL_DATABASE_URL": "tetral-database/bridge-url", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "runtime-binding-token/hmac-key", "TETRAL_BLOB_ENDPOINT": "tetral-blob/endpoint", "TETRAL_BLOB_REGION": "tetral-blob/region", "TETRAL_BLOB_BUCKET": "tetral-blob/bucket", "TETRAL_BLOB_ACCESS_KEY": "tetral-blob/access-key", "TETRAL_BLOB_SECRET_KEY": "tetral-blob/secret-key"},
			ingress: map[int][]networkPolicyPeer{8080: {metrics}, 9090: {runtime, sys("provider-gateway"), sys("mcp-connector")}}, egress: map[int][]networkPolicyPeer{5432: {sys("tetral-postgres")}, 9091: {sys("mcp-connector")}}},
		"job-runner": {container: "job-runner", ports: map[string]int{"http-job": 8081}, servicePorts: map[string]int{"metrics-job": 8081}, httpEnv: "TETRAL_BRIDGE_JOB_RUNNER_HTTP_ADDR", httpAddr: ":8081", probePort: "http-job", health: "/health", ready: "/ready",
			env:     map[string]string{"TETRAL_QUEUE_GRPC_ADDR": "queue.tetral-system.svc.cluster.local:9090", "TETRAL_KUBERNETES_NAMESPACE": "tetral-agent-runtime", "TETRAL_AGENT_RUNTIME_LABEL_SELECTOR": "app.kubernetes.io/name=agent-runtime", "TETRAL_BRIDGE_RUNTIME_POD_TOKEN_PATH": "/var/run/secrets/tetral-internal-grpc/agent-runtime/token", "TETRAL_BRIDGE_JOB_RUNNER_MCP_CONNECTOR_GRPC_ADDR": "mcp-connector.tetral-system.svc.cluster.local:9091", "TETRAL_BRIDGE_JOB_RUNNER_GATEWAY_TOKEN_PATH": "/var/run/secrets/tetral-internal-grpc/mcp-connector/token"},
			secrets: map[string]string{"TETRAL_DATABASE_URL": "tetral-database/job-runner-url", "TETRAL_BLOB_ENDPOINT": "tetral-blob/endpoint", "TETRAL_BLOB_REGION": "tetral-blob/region", "TETRAL_BLOB_BUCKET": "tetral-blob/bucket", "TETRAL_BLOB_ACCESS_KEY": "tetral-blob/access-key", "TETRAL_BLOB_SECRET_KEY": "tetral-blob/secret-key"},
			ingress: map[int][]networkPolicyPeer{8081: {metrics}}, egress: map[int][]networkPolicyPeer{5432: {sys("tetral-postgres")}, 8080: {runtime}, 9090: {sys("queue")}, 19090: {runtime}, 9091: {sys("mcp-connector")}}},
		"provider-gateway": {container: "provider-gateway", ports: map[string]int{"http": 8080, "provider-grpc": 9090}, servicePorts: map[string]int{"http": 8080, "provider-grpc": 9090}, httpEnv: "TETRAL_PROVIDER_GATEWAY_HTTP_ADDR", httpAddr: "0.0.0.0:8080", probePort: "http", health: "/healthz", ready: "/readyz",
			env:     map[string]string{"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime", "TETRAL_PROVIDER_GATEWAY_GRPC_ADDR": "0.0.0.0:9090", "TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH": "/var/run/secrets/tetral-internal-grpc/bridge/token", "TETRAL_BRIDGE_API_GRPC_ADDR": "bridge.tetral-system.svc.cluster.local:9090"},
			secrets: map[string]string{"TETRAL_DATABASE_URL": "tetral-database/provider-gateway-url", "ENGINE_VAULT_KEY": "api-secrets/engine-vault-key", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "runtime-binding-token/hmac-key"},
			ingress: map[int][]networkPolicyPeer{8080: {metrics}, 9090: {runtime}}, egress: map[int][]networkPolicyPeer{5432: {sys("tetral-postgres")}, 9090: {sys("bridge")}}},
		"mcp-connector": {container: "mcp-connector", ports: map[string]int{"mcp-http": 8081, "mcp-grpc": 9091}, servicePorts: map[string]int{"mcp-http": 8081, "mcp-grpc": 9091}, httpEnv: "TETRAL_MCP_CONNECTOR_HTTP_ADDR", httpAddr: "0.0.0.0:8081", probePort: "mcp-http", health: "/healthz", ready: "/readyz",
			env:     map[string]string{"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime", "TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS": "tetral-system/bridge,tetral-system/job-runner", "TETRAL_MCP_CONNECTOR_GRPC_ADDR": "0.0.0.0:9091", "TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH": "/var/run/secrets/tetral-internal-grpc/bridge/token", "TETRAL_BRIDGE_API_GRPC_ADDR": "bridge.tetral-system.svc.cluster.local:9090"},
			secrets: map[string]string{"TETRAL_DATABASE_URL": "tetral-database/mcp-connector-url", "ENGINE_VAULT_KEY": "api-secrets/engine-vault-key", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "runtime-binding-token/hmac-key"},
			ingress: map[int][]networkPolicyPeer{8081: {metrics}, 9091: {runtime, sys("bridge"), sys("job-runner")}}, egress: map[int][]networkPolicyPeer{5432: {sys("tetral-postgres")}, 9090: {sys("bridge")}}},
		"web-connector": {container: "web-connector", ports: map[string]int{"web-metrics": 9464, "web-grpc": 9092}, servicePorts: map[string]int{"web-metrics": 9464, "web-grpc": 9092}, httpEnv: "TETRAL_WEB_CONNECTOR_METRICS_ADDR", httpAddr: "0.0.0.0:9464", probePort: "web-metrics", health: "/health", ready: "/ready",
			env:     map[string]string{"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime", "TETRAL_WEB_CONNECTOR_GRPC_ADDR": "0.0.0.0:9092"},
			secrets: map[string]string{"TETRAL_WEB_API_KEYS": "gateway-web-keypool/TETRAL_WEB_API_KEYS", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "runtime-binding-token/hmac-key", "TETRAL_BLOB_ENDPOINT": "gateway-web-blob/TETRAL_BLOB_ENDPOINT", "TETRAL_BLOB_REGION": "gateway-web-blob/TETRAL_BLOB_REGION", "TETRAL_BLOB_BUCKET": "gateway-web-blob/TETRAL_BLOB_BUCKET", "TETRAL_BLOB_ACCESS_KEY": "gateway-web-blob/TETRAL_BLOB_ACCESS_KEY", "TETRAL_BLOB_SECRET_KEY": "gateway-web-blob/TETRAL_BLOB_SECRET_KEY"},
			ingress: map[int][]networkPolicyPeer{9464: {metrics}, 9092: {runtime}}, egress: map[int][]networkPolicyPeer{}},
	}
}

func TestSeparatedWorkloadIdentityAndAccess(t *testing.T) {
	docs := readManifestDocuments(t)
	for _, d := range docs {
		if d.name == "gateway" || d.name == "gateway-tokenreview" || d.name == "bridge-visibility" {
			t.Fatalf("retired combined resource remains: %s/%s", d.kind, d.name)
		}
		// Provider Gateway and MCP Connector each authenticate with their own
		// database role; neither may fall back to the shared Gateway key.
		if strings.Contains(d.text, "key: gateway-url") {
			t.Fatalf("%s/%s reads the retired shared Gateway database key", d.kind, d.name)
		}
	}
	for name, c := range separatedContracts() {
		t.Run(name, func(t *testing.T) {
			d := requireDocument(t, docs, name+".yaml", "Deployment", name)
			s := requireDocument(t, docs, name+".yaml", "Service", name)
			n := requireDocument(t, docs, name+".yaml", "NetworkPolicy", name)
			if err := validateSeparatedDeployment(d.text, name, c); err != nil {
				t.Fatal(err)
			}
			ports := map[string]int{}
			for _, m := range regexp.MustCompile(`(?m)^    - name: ([^\n]+)\n      port: ([0-9]+)\n      targetPort: ([^\n]+)`).FindAllStringSubmatch(s.text, -1) {
				port, _ := strconv.Atoi(m[2])
				ports[m[1]] = port
				target := m[1]
				if name == "job-runner" {
					target = "http-job"
				}
				if m[3] != target {
					t.Fatalf("%s targetPort=%s", m[1], m[3])
				}
			}
			if !reflect.DeepEqual(ports, c.servicePorts) {
				t.Fatalf("service ports=%v; want%v", ports, c.servicePorts)
			}
			requireContains(t, s, "selector:\n    app.kubernetes.io/name: "+name)
			requireNotContains(t, s, "clusterIP: None")
			if name == "provider-gateway" {
				requireContains(t, s, "sessionAffinity: None")
			}

			requireExactSeparatedPeers(t, parseNetworkPolicyIngressRules(t, n), c.ingress, false)
			requireExactSeparatedPeers(t, parseNetworkPolicyEgressRules(t, n), c.egress, true)
			requireKubeDNSEgress(t, n)
			requireNetworkPolicyEgressIPBlock(t, n, 443, "10.96.0.1/32")
			requireNetworkPolicyEgressIPBlock(t, n, 443, "0.0.0.0/0")
			// Token audience and consuming paths are checked through the projected volume mounted by this process.
			requireSeparatedTokenPaths(t, d, name)
		})
	}
	hpa := requireDocument(t, docs, "provider-gateway.yaml", "HorizontalPodAutoscaler", "provider-gateway")
	for _, text := range []string{"name: provider-gateway", "minReplicas: 2", "maxReplicas: 10", "averageUtilization: 70", "name: cpu"} {
		requireContains(t, hpa, text)
	}
	for _, name := range []string{"bridge", "job-runner", "mcp-connector", "web-connector"} {
		for _, d := range docs.byFile(name + ".yaml") {
			if d.kind == "HorizontalPodAutoscaler" {
				t.Fatalf("%s inherits provider HPA", name)
			}
		}
	}
}

func validateSeparatedDeployment(text, name string, c separatedContract) error {
	if err := validateSingleContainer(text); err != nil {
		return err
	}
	if !strings.Contains(text, "containers:\n        - name: "+c.container+"\n") {
		return fmt.Errorf("%s has wrong business owner", name)
	}
	if !strings.Contains(text, "serviceAccountName: "+name+"\n") || !strings.Contains(text, "automountServiceAccountToken: false") {
		return fmt.Errorf("%s shares or automounts identity", name)
	}
	// Exact occurrence count includes metadata, Deployment selector and owning Pod label.
	if strings.Count(text, "app.kubernetes.io/name: "+name+"\n") != 3 {
		return fmt.Errorf("%s selector or labels differ", name)
	}
	if strings.Contains(text, "envFrom:") || strings.Contains(text, "secret:\n") {
		return fmt.Errorf("%s inherits unscoped Secret mounts", name)
	}
	ports := map[string]int{}
	for _, m := range regexp.MustCompile(`(?m)^            - name: ([^\n]+)\n              containerPort: ([0-9]+)`).FindAllStringSubmatch(text, -1) {
		port, _ := strconv.Atoi(m[2])
		ports[m[1]] = port
	}
	if !reflect.DeepEqual(ports, c.ports) {
		return fmt.Errorf("%s ports=%v; want%v", name, ports, c.ports)
	}
	secrets := map[string]string{}
	lines := strings.Split(text, "\n")
	env := map[string]string{}
	for i, line := range lines {
		if !strings.HasPrefix(line, "            - name: ") {
			continue
		}
		key := cleanManifestListValue(strings.TrimPrefix(line, "            - name: "))
		end := i + 1
		for end < len(lines) && leadingSpaces(lines[end]) > 12 {
			end++
		}
		block := strings.Join(lines[i+1:end], "\n")
		for _, ln := range lines[i+1 : end] {
			if strings.HasPrefix(ln, "              value: ") {
				env[key] = cleanManifestListValue(strings.TrimPrefix(ln, "              value: "))
			}
		}
		if strings.Contains(block, "secretKeyRef:") {
			ref := regexp.MustCompile(`(?m)^                  name: ([^\n]+)\n                  key: ([^\n]+)`).FindStringSubmatch(block)
			if len(ref) != 3 {
				return fmt.Errorf("%s malformed Secret grant", name)
			}
			secrets[key] = cleanManifestListValue(ref[1]) + "/" + cleanManifestListValue(ref[2])
		}
	}
	if !reflect.DeepEqual(secrets, c.secrets) {
		return fmt.Errorf("%s Secret grants=%v; want%v", name, secrets, c.secrets)
	}
	if env[c.httpEnv] != c.httpAddr {
		return fmt.Errorf("%s health address=%q", name, env[c.httpEnv])
	}
	for k, v := range c.env {
		if env[k] != v {
			return fmt.Errorf("%s %s=%q; want%q", name, k, env[k], v)
		}
	}
	if name != "job-runner" && env["TETRAL_INTERNAL_GRPC_AUDIENCE"] != "tetral-internal-grpc" {
		return fmt.Errorf("%s has wrong internal audience", name)
	}
	if name == "job-runner" {
		for _, k := range []string{"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH", "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS", "TETRAL_WORKSPACE_ID", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY"} {
			if _, ok := env[k]; ok {
				return fmt.Errorf("Runner inherits %s", k)
			}
		}
	}
	for _, k := range []string{"TETRAL_SANDBOX_DRIVER", "DAYTONA_API_KEY"} {
		if _, ok := env[k]; ok {
			return fmt.Errorf("%s receives sandbox provider configuration", name)
		}
	}
	for k, want := range map[string]string{"livenessProbe": c.health, "readinessProbe": c.ready} {
		if !strings.Contains(text, k+":\n            httpGet:\n              path: "+want+"\n              port: "+c.probePort) {
			return fmt.Errorf("%s %s targets wrong port or route", name, k)
		}
	}
	return nil
}

func requireExactSeparatedPeers(t *testing.T, rules []networkPolicyRule, want map[int][]networkPolicyPeer, egress bool) {
	t.Helper()
	actual := map[int][]networkPolicyPeer{}
	for _, r := range rules {
		for _, transport := range r.transports {
			if transport.port != 53 && transport.protocol != "TCP" {
				t.Fatalf("business edge has wrong protocol: %v", transport)
			}
		}
		for _, p := range r.peers {
			if len(p.podLabels) > 1 || len(p.namespaceLabels) > 1 {
				t.Fatalf("peer includes extra selector labels: %v", p)
			}
		}
		for _, port := range r.ports {
			if egress && (port == 53 || port == 443) {
				continue
			}
			actual[port] = append(actual[port], r.peers...)
			if len(r.ipBlocks) > 0 {
				t.Fatalf("unexpected broad peer at %d", port)
			}
		}
	}
	keys := func(peers []networkPolicyPeer) []string {
		a := []string{}
		for _, p := range peers {
			a = append(a, fmt.Sprintf("%s/%s/%s", p.namespace, p.podName, p.podPartOf))
		}
		sort.Strings(a)
		return a
	}
	if len(actual) != len(want) {
		t.Fatalf("peer ports=%v; want%v", actual, want)
	}
	for port, peers := range want {
		if !reflect.DeepEqual(keys(actual[port]), keys(peers)) {
			t.Fatalf("port%d peers=%v; want%v", port, actual[port], peers)
		}
	}
}

func requireSeparatedTokenPaths(t *testing.T, d *manifestDocument, name string) {
	t.Helper()
	reviewerVolume := name + "-kubernetes-api"
	reviewerFile := name + "-tokenreview/token"
	if name == "bridge" {
		reviewerVolume = "bridge-api-kubernetes-api"
		reviewerFile = "bridge-api-tokenreview/token"
	}
	if name == "job-runner" {
		requireProjectedBoundedServiceAccountTokenShape(t, d, projectedTokenExpectation{volume: "job-runner-kubernetes-api", mountPath: "/var/run/secrets/kubernetes.io/serviceaccount", filePath: "token", audienceMode: projectedTokenNoAudience, expirationSeconds: 600})
	} else {
		requireProjectedBoundedServiceAccountToken(t, d, projectedTokenExpectation{envName: "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH", volume: reviewerVolume, mountPath: "/var/run/secrets/tetral-kubernetes-api", filePath: reviewerFile, audienceMode: projectedTokenNoAudience, expirationSeconds: 600})
	}
	outbound := []projectedTokenExpectation{}
	switch name {
	case "bridge":
		outbound = append(outbound, projectedTokenExpectation{envName: "TETRAL_BRIDGE_GATEWAY_TOKEN_PATH", volume: "bridge-api-mcp-token", mountPath: "/var/run/secrets/tetral-internal-grpc/mcp-connector"})
	case "job-runner":
		outbound = append(outbound, projectedTokenExpectation{envName: "TETRAL_BRIDGE_RUNTIME_POD_TOKEN_PATH", volume: "bridge-runtime-pod-token", mountPath: "/var/run/secrets/tetral-internal-grpc/agent-runtime"}, projectedTokenExpectation{envName: "TETRAL_BRIDGE_JOB_RUNNER_GATEWAY_TOKEN_PATH", volume: "job-runner-mcp-token", mountPath: "/var/run/secrets/tetral-internal-grpc/mcp-connector"})
	case "provider-gateway":
		outbound = append(outbound, projectedTokenExpectation{envName: "TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH", volume: "provider-gateway-bridge-token", mountPath: "/var/run/secrets/tetral-internal-grpc/bridge"})
	case "mcp-connector":
		outbound = append(outbound, projectedTokenExpectation{envName: "TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH", volume: "mcp-connector-bridge-token", mountPath: "/var/run/secrets/tetral-internal-grpc/bridge"})
	}
	for _, e := range outbound {
		e.filePath = "token"
		e.audienceMode = projectedTokenWithAudience
		e.audience = "tetral-internal-grpc"
		e.expirationSeconds = 600
		requireProjectedBoundedServiceAccountToken(t, d, e)
	}
	if count := strings.Count(d.text, "- serviceAccountToken:"); count != len(outbound)+1 {
		t.Fatalf("%s projected token count=%d; want%d", name, count, len(outbound)+1)
	}
}

func TestSeparatedWorkloadContractRejectsInheritedAccess(t *testing.T) {
	docs := readManifestDocuments(t)
	base := requireDocument(t, docs, "web-connector.yaml", "Deployment", "web-connector")
	c := separatedContracts()["web-connector"]
	for _, tc := range []struct{ name, old, new string }{
		{"shared identity", "serviceAccountName: web-connector", "serviceAccountName: gateway"},
		{"wrong business owner", "- name: web-connector", "- name: provider-gateway"},
		{"provider credential", "name: gateway-web-keypool", "name: api-secrets"},
		{"sibling port", "containerPort: 9092", "containerPort: 9090"},
		{"shared selector", "app.kubernetes.io/name: web-connector", "app.kubernetes.io/name: gateway"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mut := strings.Replace(base.text, tc.old, tc.new, 1)
			if tc.name == "provider credential" {
				mut = regexp.MustCompile(`name: ["']?gateway-web-keypool["']?`).ReplaceAllString(base.text, "name: api-secrets")
			}
			if mut == base.text {
				t.Fatal("mutation did not apply")
			}
			if validateSeparatedDeployment(mut, "web-connector", c) == nil {
				t.Fatal("inherited access accepted")
			}
		})
	}
}
