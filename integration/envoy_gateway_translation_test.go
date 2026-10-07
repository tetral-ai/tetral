package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

// Ports change only fixture socket plumbing. Policy ports at each real backend
// retain their production Service values on distinct owned loopback addresses.
type envoyGatewayFixturePorts struct{ HTTP, HTTPS, Ready, Admin, Stats int }
type envoyGatewayTranslation struct {
	Snapshot         *translatedEnvoySnapshot
	Authority        *transporttest.Authority
	Leaves           map[string]transporttest.Leaf
	Ports            envoyGatewayFixturePorts
	Directory        string
	Control          *translatedEnvoyControl
	ProcessDrainArgs []string
	// ReleasePorts, when set by the caller that reserved Ports, frees the held
	// loopback ports immediately before the Envoy container dispatch. It is
	// idempotent, so later Envoy generations rebind the same ports unchanged.
	ReleasePorts func()
}

func translateProductionEnvoyGateway(t *testing.T, profile string, ports envoyGatewayFixturePorts) *envoyGatewayTranslation {
	return translateProductionEnvoyGatewayHosts(t, profile, ports, "api.localhost", "git.localhost")
}

func translateProductionEnvoyGatewayHosts(t *testing.T, profile string, ports envoyGatewayFixturePorts, apiHost, gitHost string) *envoyGatewayTranslation {
	t.Helper()
	if profile != "hardened" && profile != "standard-routed" {
		t.Fatal("unsupported production transport profile")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	directory := t.TempDir()
	command := exec.CommandContext(ctx, "helm", "template", "tetral", filepath.Join(transporttest.RepositoryRoot(t), "deploy/helm/tetral"), "--set", "edge.enabled=true", "--set", "transport.profile="+profile, "--set", "edge.apiHost="+apiHost, "--set", "gitProxyHost="+gitHost) //nolint:gosec // Repository-owned chart and bounded fixture host arguments, no shell.
	rendered, err := command.Output()
	if err != nil {
		t.Fatal("render production edge resources")
	}
	authority := transporttest.Must(transporttest.NewAuthority("local-production-edge"))
	leaves := map[string]transporttest.Leaf{}
	for _, role := range []string{"public-api", "public-git", "edge", "auth", "api", "event-stream", "git-proxy"} {
		dns, uri := role+".tetral-system.svc.cluster.local", "spiffe://cluster.local/ns/tetral-system/sa/"+role
		switch role {
		case "public-api":
			dns = apiHost
		case "public-git":
			dns = gitHost
		case "edge":
			dns, uri = "tetral-public-edge.envoy-gateway-system.svc.cluster.local", "spiffe://cluster.local/ns/envoy-gateway-system/sa/tetral-public-edge"
		}
		leaves[role] = transporttest.Must(authority.ValidLeaf(dns, uri))
	}
	policyKinds := map[string]bool{"Gateway": true, "EnvoyProxy": true, "HTTPRoute": true, "ClientTrafficPolicy": true, "BackendTrafficPolicy": true, "SecurityPolicy": true, "EnvoyPatchPolicy": true, "BackendTLSPolicy": true}
	var objects []map[string]any
	counts := map[string]int{}
	services := map[string]map[string]any{}
	decoder := yaml.NewDecoder(bytes.NewReader(rendered))
	for {
		var object map[string]any
		if err := decoder.Decode(&object); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal("decode actual rendered resources")
		}
		if object == nil {
			continue
		}
		kind, _ := object["kind"].(string)
		metadata := object["metadata"].(map[string]any)
		if kind == "Service" {
			services[metadata["name"].(string)] = object
		}
		if !policyKinds[kind] {
			continue
		}
		if kind == "Gateway" {
			for _, raw := range object["spec"].(map[string]any)["listeners"].([]any) {
				listener := raw.(map[string]any)
				listener["port"] = ports.HTTPS
				if listener["protocol"] == "HTTP" {
					listener["port"] = ports.HTTP
				}
			}
		}
		if kind == "EnvoyProxy" {
			// Both translations receive the same official, process-only Bootstrap patch.
			// The xds_cluster assignment/TLS remains the original upstream value.
			object["spec"].(map[string]any)["bootstrap"] = map[string]any{"type": "JSONPatch", "jsonPatches": []any{
				map[string]any{"op": "add", "path": "/node/id", "value": "tetral-local-official-snapshot"},
				map[string]any{"op": "replace", "path": "/admin/address/socket_address/port_value", "value": ports.Admin},
				map[string]any{"op": "replace", "path": "/static_resources/clusters/0/load_assignment/endpoints/0/lb_endpoints/0/endpoint/address/socket_address/port_value", "value": ports.Admin},
				map[string]any{"op": "replace", "path": "/static_resources/listeners/0/address/socket_address/address", "value": "127.0.0.1"},
				map[string]any{"op": "replace", "path": "/static_resources/listeners/0/address/socket_address/port_value", "value": ports.Stats},
			}}
		}
		objects = append(objects, object)
		counts[kind]++
	}
	wanted := map[string]int{"Gateway": 1, "EnvoyProxy": 1, "HTTPRoute": 4, "ClientTrafficPolicy": 3, "BackendTrafficPolicy": 1, "SecurityPolicy": 1, "EnvoyPatchPolicy": 1}
	if profile == "hardened" {
		wanted["BackendTLSPolicy"] = 4
	}
	if !reflect.DeepEqual(counts, wanted) {
		t.Fatal("actual production render omitted or added an owning edge policy object")
	}
	for index, name := range []string{"auth", "api", "event-stream", "git-proxy"} {
		service := services[name]
		if service == nil {
			t.Fatal("actual render omitted edge Service")
		}
		address := fmt.Sprintf("127.0.0.%d", index+2)
		service["spec"].(map[string]any)["clusterIP"] = address
		objects = append(objects, service)
		endpointPorts := []any{map[string]any{"name": "http", "protocol": "TCP", "port": 8080}}
		if name == "auth" {
			endpointPorts = append(endpointPorts, map[string]any{"name": "grpc-check", "protocol": "TCP", "port": 9095})
		}
		objects = append(objects, map[string]any{"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "metadata": map[string]any{"name": name + "-fixture", "namespace": "tetral-system", "labels": map[string]any{"kubernetes.io/service-name": name}}, "addressType": "IPv4", "ports": endpointPorts, "endpoints": []any{map[string]any{"addresses": []any{address}, "conditions": map[string]any{"ready": true}}}})
	}
	for name, role := range map[string]string{"tetral-api-public-tls": "public-api", "tetral-git-public-tls": "public-git", "tetral-edge-client-tls": "edge"} {
		objects = append(objects, map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": name, "namespace": "tetral-system"}, "type": "kubernetes.io/tls", "data": map[string]any{"tls.crt": base64.StdEncoding.EncodeToString(leaves[role].Certificate), "tls.key": base64.StdEncoding.EncodeToString(leaves[role].Key)}})
	}
	objects = append(objects, map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "tetral-edge-native-trust", "namespace": "tetral-system"}, "data": map[string]any{"ca.crt": string(authority.PEM)}})
	classBody := transporttest.Must(os.ReadFile(filepath.Join(transporttest.RepositoryRoot(t), "deploy/envoy-gateway/gateway-class.yaml")))
	var class map[string]any
	if err := yaml.Unmarshal(classBody, &class); err != nil {
		t.Fatal(err)
	}
	objects = append(objects, class)
	baseline := runOfficialEnvoyTranslation(ctx, t, directory, "baseline", objects)
	names := []string{"tetral-system/tetral-public-edge/http", "tetral-system/tetral-public-edge/api-https", "envoy-gateway-proxy-ready-0.0.0.0-19003"}
	patches := []any{}
	for _, name := range names {
		patches = append(patches, map[string]any{"type": resourcev3.ListenerType, "name": name, "operation": map[string]any{"op": "replace", "path": "/address/socket_address/address", "value": "127.0.0.1"}})
	}
	patches = append(patches, map[string]any{"type": resourcev3.ListenerType, "name": names[2], "operation": map[string]any{"op": "replace", "path": "/address/socket_address/port_value", "value": ports.Ready}})
	objects = append(objects, map[string]any{"apiVersion": "gateway.envoyproxy.io/v1alpha1", "kind": "EnvoyPatchPolicy", "metadata": map[string]any{"name": "tetral-fixture-loopback", "namespace": "tetral-system"}, "spec": map[string]any{"targetRef": map[string]any{"group": "gateway.networking.k8s.io", "kind": "Gateway", "name": "tetral-public-edge"}, "type": "JSONPatch", "jsonPatches": patches}})
	confined := runOfficialEnvoyTranslation(ctx, t, directory, "confined", objects)
	requireOnlyFixtureSocketDelta(t, baseline, confined, ports)
	completeEnvoyGatewaySecrets(ctx, t, confined)
	wantedSecrets := 2
	if profile == "hardened" {
		wantedSecrets = 7
	}
	if len(confined.Resources[resourcev3.SecretType]) != wantedSecrets {
		t.Fatal("actual policy SDS set/count differs from selected profile")
	}
	requireConfinedEnvoyListeners(t, confined)
	return &envoyGatewayTranslation{Snapshot: confined, Authority: authority, Leaves: leaves, Ports: ports, Directory: directory}
}

func runOfficialEnvoyTranslation(ctx context.Context, t *testing.T, directory, label string, objects []map[string]any) *translatedEnvoySnapshot {
	t.Helper()
	var input bytes.Buffer
	encoder := yaml.NewEncoder(&input)
	for _, object := range objects {
		if err := encoder.Encode(object); err != nil {
			t.Fatal("encode exact fixture resources")
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, label+".yaml")
	if err := os.WriteFile(path, input.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	executable := os.Getenv(testinfra.EnvTestEGCTL)
	if executable == "" {
		t.Fatal("native checksum-verified matching egctl is absent")
	}
	command := exec.CommandContext(ctx, executable, "x", "translate", "--from", "gateway-api", "--to", "gateway-api,xds,ir", "--file", path, "--output", "json") //nolint:gosec // Native verified tool, test-owned path, no shell.
	body, err := command.Output()
	if err != nil {
		t.Fatal("matching upstream translator failed")
	}
	// The CLI may return exit zero with Accepted=False. Enumerate the actual input
	// policy objects and require their complete owning status before reading xDS.
	requireAllEnvoyTranslationStatuses(t, body, objects)
	//nolint:gosec // G703: test-owned temporary directory and generation labels supplied only by the local fixture, not public requests.
	if err := os.WriteFile(filepath.Join(directory, label+".json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeEnvoyGatewaySnapshot(body)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.FixtureYAML = append([]byte(nil), input.Bytes()...)
	return snapshot
}

func requireAllEnvoyTranslationStatuses(t *testing.T, body []byte, objects []map[string]any) {
	t.Helper()
	var output map[string]json.RawMessage
	if err := json.Unmarshal(body, &output); err != nil {
		t.Fatal("decode complete translation statuses")
	}
	fields := map[string]string{"GatewayClass": "gatewayClass", "EnvoyProxy": "envoyProxiesForGateways", "Gateway": "gateways", "HTTPRoute": "httpRoutes", "ClientTrafficPolicy": "clientTrafficPolicies", "BackendTrafficPolicy": "backendTrafficPolicies", "SecurityPolicy": "securityPolicies", "EnvoyPatchPolicy": "envoyPatchPolicies", "BackendTLSPolicy": "backendTLSPolicies"}
	for _, expected := range objects {
		kind := expected["kind"].(string)
		field, checked := fields[kind]
		if !checked {
			continue
		}
		var decoded any
		if err := json.Unmarshal(output[field], &decoded); err != nil {
			t.Fatalf("translation omitted owning status kind=%s", kind)
		}
		var candidates []map[string]any
		switch values := decoded.(type) {
		case []any:
			for _, raw := range values {
				candidates = append(candidates, raw.(map[string]any))
			}
		case map[string]any:
			if _, ok := values["metadata"]; ok {
				candidates = append(candidates, values)
			} else {
				for _, raw := range values {
					candidates = append(candidates, raw.(map[string]any))
				}
			}
		}
		metadata := expected["metadata"].(map[string]any)
		matched := 0
		for _, candidate := range candidates {
			actual := candidate["metadata"].(map[string]any)
			if actual["name"] != metadata["name"] || actual["namespace"] != metadata["namespace"] {
				continue
			}
			matched++
			status, ok := candidate["status"].(map[string]any)
			if !ok {
				t.Fatalf("status absent kind=%s name=%s", kind, metadata["name"])
			}
			groups := []any{status}
			if kind == "Gateway" {
				top, _ := status["conditions"].([]any)
				seen := map[string]bool{}
				for _, raw := range top {
					condition := raw.(map[string]any)
					if condition["status"] != "True" {
						t.Fatalf("rejected top-level Gateway condition=%s", condition["type"])
					}
					seen[condition["type"].(string)] = true
				}
				if len(top) != 0 && (!seen["Accepted"] || !seen["Programmed"]) {
					t.Fatal("top-level Gateway status omitted Accepted/Programmed")
				}
			}
			for _, key := range []string{"listeners", "parents", "ancestors"} {
				if value, ok := status[key].([]any); ok {
					groups = value
					break
				}
			}
			if len(groups) == 0 {
				t.Fatalf("empty status group kind=%s", kind)
			}
			for _, raw := range groups {
				conditions, _ := raw.(map[string]any)["conditions"].([]any)
				seen := map[string]bool{}
				for _, raw := range conditions {
					condition := raw.(map[string]any)
					if condition["status"] != "True" {
						t.Fatalf("rejected translated object kind=%s name=%s condition=%s", kind, metadata["name"], condition["type"])
					}
					seen[condition["type"].(string)] = true
				}
				required := []string{"Accepted"}
				if kind == "HTTPRoute" || kind == "BackendTLSPolicy" {
					required = append(required, "ResolvedRefs")
				}
				if kind == "Gateway" {
					required = append(required, "Programmed", "ResolvedRefs")
				}
				for _, name := range required {
					if !seen[name] {
						t.Fatalf("translation omitted required status kind=%s condition=%s", kind, name)
					}
				}
			}
		}
		if matched != 1 {
			t.Fatalf("translation owning identity count kind=%s name=%s count=%d", kind, metadata["name"], matched)
		}
	}
}

func requireOnlyFixtureSocketDelta(t *testing.T, baseline, confined *translatedEnvoySnapshot, ports envoyGatewayFixturePorts) {
	t.Helper()
	if !reflect.DeepEqual(baseline.Bootstrap, confined.Bootstrap) {
		t.Fatal("fixture policy patch changed official process Bootstrap")
	}
	changes := 0
	for kind, resources := range baseline.Resources {
		originals := map[string]types.Resource{}
		for _, resource := range resources {
			originals[cachev3.GetResourceName(resource)] = resource
		}
		if len(confined.Resources[kind]) != len(originals) {
			t.Fatal("fixture patch changed original resource count")
		}
		for _, resource := range confined.Resources[kind] {
			name := cachev3.GetResourceName(resource)
			original := originals[name]
			if original == nil {
				t.Fatal("fixture patch renamed a policy resource")
			}
			normalized := proto.Clone(resource)
			if kind == resourcev3.ListenerType {
				listener := normalized.(*listenerv3.Listener)
				before := original.(*listenerv3.Listener)
				socket := listener.GetAddress().GetSocketAddress()
				if socket.GetAddress() != "127.0.0.1" || before.GetAddress().GetSocketAddress().GetAddress() != "0.0.0.0" {
					t.Fatal("fixture listener address change differs from exact loopback substitution")
				}
				socket.Address = "0.0.0.0"
				changes++
				if name == "envoy-gateway-proxy-ready-0.0.0.0-19003" {
					if socket.GetPortValue() != uint32(ports.Ready) {
						t.Fatal("fixture readiness port mismatch")
					}
					socket.PortSpecifier = before.GetAddress().GetSocketAddress().PortSpecifier
					changes++
				}
			}
			if !proto.Equal(original, normalized) {
				t.Fatalf("fixture changed production policy kind=%s name=%s", kind, name)
			}
		}
	}
	if changes != 4 {
		t.Fatal("fixture socket patch did not change exactly three addresses and one readiness port")
	}
	t.Log("actual upstream graph parity: only three listener addresses and one readiness port changed")
}
