package helm_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func transportMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected mapping, got %T", v)
	}
	return m
}
func transportAt(t *testing.T, v any, keys ...string) any {
	t.Helper()
	for _, k := range keys {
		v = transportMap(t, v)[k]
	}
	return v
}
func transportList(t *testing.T, v any) []any {
	t.Helper()
	a, ok := v.([]any)
	if !ok {
		t.Fatalf("expected list, got %T", v)
	}
	return a
}
func transportStrings(t *testing.T, v any) []string {
	t.Helper()
	a := transportList(t, v)
	s := make([]string, len(a))
	for i, x := range a {
		s[i] = fmt.Sprint(x)
	}
	sort.Strings(s)
	return s
}
func transportEnv(t *testing.T, d any) map[string]string {
	t.Helper()
	c := transportList(t, transportAt(t, d, "spec", "template", "spec", "containers"))[0]
	env := map[string]string{}
	for _, x := range transportList(t, transportAt(t, c, "env")) {
		m := transportMap(t, x)
		if s, ok := m["value"]; ok {
			env[fmt.Sprint(m["name"])] = fmt.Sprint(s)
		}
	}
	return env
}

func TestInternalTransportDeploymentContracts(t *testing.T) {
	root := engineRoot(t)
	helm := requireHelm(t)
	chart := filepath.Join(root, "deploy/helm/tetral")
	for _, pair := range [][2]string{{"deploy/dependencies.lock.json", "deploy/helm/tetral/files/dependencies.lock.json"}, {"services/agent-runtime/packages/runtime-pod/src/bridge-method-policy.json", "deploy/helm/tetral/files/bridge-method-policy.json"}} {
		original, err := os.ReadFile(filepath.Join(root, pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		projection, err := os.ReadFile(filepath.Join(root, pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		var a, b any
		if json.Unmarshal(original, &a) != nil || json.Unmarshal(projection, &b) != nil || !reflect.DeepEqual(a, b) {
			t.Fatalf("canonical projection drift: %s", pair[1])
		}
	}
	lockData, err := os.ReadFile(filepath.Join(root, "deploy/dependencies.lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	var lock struct {
		Istio struct {
			Version string
			Images  []struct{ Component, Reference string }
		}
	}
	if err = json.Unmarshal(lockData, &lock); err != nil {
		t.Fatal(err)
	}
	proxy := ""
	for _, img := range lock.Istio.Images {
		if img.Component == "proxyv2" {
			proxy = img.Reference
		}
	}
	if lock.Istio.Version != "1.31.1" || !strings.Contains(proxy, ":1.31.1@sha256:") {
		t.Fatal("routing dependency is not immutable and aligned")
	}
	methodsData, _ := os.ReadFile(filepath.Join(root, "services/agent-runtime/packages/runtime-pod/src/bridge-method-policy.json"))
	var methods []struct{ Method, Path string }
	if err = json.Unmarshal(methodsData, &methods); err != nil || len(methods) != 40 {
		t.Fatalf("method inventory %d: %v", len(methods), err)
	}
	paths := []struct {
		name, source, ns, destination string
		port                          int
		protected                     bool
	}{
		{"runner-queue", "job-runner", "tetral-system", "queue", 9090, false}, {"sandbox-queue", "sandbox", "tetral-system", "queue", 9090, false},
		{"runtime-bridge", "agent-runtime", "tetral-agent-runtime", "bridge", 9090, true}, {"runtime-mcp", "agent-runtime", "tetral-agent-runtime", "mcp-connector", 9091, true},
		{"runner-mcp", "job-runner", "tetral-system", "mcp-connector", 9091, true}, {"bridge-mcp", "bridge", "tetral-system", "mcp-connector", 9091, true},
		{"mcp-bridge", "mcp-connector", "tetral-system", "bridge", 9090, true}, {"runtime-web", "agent-runtime", "tetral-agent-runtime", "web-connector", 9092, false},
		{"runtime-provider", "agent-runtime", "tetral-agent-runtime", "provider-gateway", 9090, true}, {"provider-bridge", "provider-gateway", "tetral-system", "bridge", 9090, true},
	}
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			objects := uniqueObjects(t, renderChart(t, helm, chart, "transport.profile="+profile, "routing.trustDomain=transport.example"))
			receivers := map[string][]string{}
			routeCount := 0
			for _, p := range paths {
				key := "networking.istio.io/v1|DestinationRule|" + p.ns + "|tetral-" + p.name
				dr := objects[key]
				if dr == nil {
					t.Fatal("missing scoped destination", key)
				}
				host := p.destination + ".tetral-system.svc.cluster.local"
				requireManifestPathString(t, dr, host, "spec", "host")
				requireManifestPathString(t, dr, p.source, "spec", "workloadSelector", "matchLabels", "app.kubernetes.io/name")
				if !reflect.DeepEqual(transportStrings(t, transportAt(t, dr, "spec", "exportTo")), []string{"."}) {
					t.Fatal("route exported beyond owner namespace")
				}
				settings := transportList(t, transportAt(t, dr, "spec", "trafficPolicy", "portLevelSettings"))
				if len(settings) != 1 {
					t.Fatal("policy must be port scoped")
				}
				s := settings[0]
				if fmt.Sprint(transportAt(t, s, "port", "number")) != fmt.Sprint(p.port) {
					t.Fatal("wrong port")
				}
				requireManifestPathString(t, s, "LEAST_REQUEST", "loadBalancer", "simple")
				requireManifestPathString(t, s, "1s", "connectionPool", "tcp", "connectTimeout")
				od := transportMap(t, transportAt(t, s, "outlierDetection"))
				for k, v := range map[string]any{"splitExternalLocalOriginErrors": true, "consecutiveLocalOriginFailures": 2, "consecutive5xxErrors": 0, "consecutiveGatewayErrors": 0, "maxEjectionPercent": 100, "minHealthPercent": 0, "baseEjectionTime": "10s"} {
					if !reflect.DeepEqual(od[k], v) {
						t.Fatalf("outlier setting %s=%v want%v", k, od[k], v)
					}
				}
				protected := p.protected || profile == "hardened"
				mode := "DISABLE"
				if protected {
					mode = "ISTIO_MUTUAL"
				}
				requireManifestPathString(t, s, mode, "tls", "mode")
				if protected {
					wantURI := "spiffe://transport.example/ns/tetral-system/sa/" + p.destination
					if !reflect.DeepEqual(transportStrings(t, transportAt(t, s, "tls", "subjectAltNames")), []string{wantURI}) {
						t.Fatal("server role SAN allowlist changed")
					}
					receivers[p.destination] = append(receivers[p.destination], "transport.example/ns/"+p.ns+"/sa/"+p.source)
				}
				vs := objects["networking.istio.io/v1|VirtualService|"+p.ns+"|tetral-"+p.name]
				rules := transportList(t, transportAt(t, vs, "spec", "http"))
				want := 1
				if p.destination == "bridge" {
					want = len(methods)
				}
				if len(rules) != want {
					t.Fatal("RPC routes not descriptor exhaustive")
				}
				routeCount++
				for i, r := range rules {
					match := transportList(t, transportAt(t, r, "match"))
					if len(match) != 1 {
						t.Fatal("ambiguous route match")
					}
					requireManifestPathString(t, match[0], p.ns, "sourceNamespace")
					requireManifestPathString(t, match[0], p.source, "sourceLabels", "app.kubernetes.io/name")
					if fmt.Sprint(transportAt(t, match[0], "port")) != fmt.Sprint(p.port) {
						t.Fatal("route changed port")
					}
					if fmt.Sprint(transportAt(t, r, "retries", "attempts")) != "0" {
						t.Fatal("generic retries enabled")
					}
					timeout := "0s"
					if p.destination == "queue" {
						timeout = "5s"
					}
					requireManifestPathString(t, r, timeout, "timeout")
					if p.destination == "bridge" {
						requireManifestPathString(t, match[0], methods[i].Path, "uri", "exact")
					}
				}
			}
			if routeCount != 10 {
				t.Fatal("fixed routing inventory changed")
			}
			for receiver, callers := range receivers {
				sort.Strings(callers)
				a := objects["security.istio.io/v1|AuthorizationPolicy|tetral-system|tetral-"+receiver+"-protected"]
				rules := transportList(t, transportAt(t, a, "spec", "rules"))
				from := transportList(t, transportAt(t, rules[0], "from"))
				actual := transportStrings(t, transportAt(t, from[0], "source", "principals"))
				if !reflect.DeepEqual(actual, callers) {
					t.Fatalf("receiver%s exactcaller allowlist=%v want%v", receiver, actual, callers)
				}
			}
			roles := []string{"agent-runtime", "job-runner", "bridge", "sandbox", "mcp-connector", "provider-gateway"}
			if profile == "hardened" {
				roles = append(roles, "queue", "web-connector")
			}
			for _, role := range roles {
				ns := "tetral-system"
				if role == "agent-runtime" {
					ns = "tetral-agent-runtime"
				}
				d := objects["apps/v1|Deployment|"+ns+"|"+role]
				annotations := transportMap(t, transportAt(t, d, "spec", "template", "metadata", "annotations"))
				for k, v := range map[string]string{"sidecar.istio.io/inject": "true", "sidecar.istio.io/nativeSidecar": "true", "sidecar.istio.io/proxyImage": proxy} {
					if annotations[k] != v {
						t.Fatalf("mandatory%s annotation%s=%v", role, k, annotations[k])
					}
				}
				env := transportEnv(t, d)
				if env["TETRAL_ROUTING_PROXY_REQUIRED"] != "true" || env["TETRAL_TRANSPORT_PROFILE"] != profile {
					t.Fatalf("mandatory proxy startup setting missing for%s", role)
				}
			}
			if profile == "standard-routed" {
				for _, role := range []string{"queue", "web-connector"} {
					d := objects["apps/v1|Deployment|tetral-system|"+role]
					requireManifestPathString(t, d, "false", "spec", "template", "metadata", "annotations", "sidecar.istio.io/inject")
					labels := transportMap(t, transportAt(t, d, "spec", "template", "metadata", "labels"))
					if _, exists := labels["istio.io/rev"]; exists {
						t.Fatal("standard optional receiver selected revision injection")
					}
				}
			}
			runtime := objects["apps/v1|Deployment|tetral-agent-runtime|agent-runtime"]
			runner := objects["apps/v1|Deployment|tetral-system|job-runner"]
			port := "19090"
			if profile == "hardened" {
				port = "19443"
			}
			requireManifestPathString(t, runtime, port, "spec", "template", "metadata", "annotations", "traffic.sidecar.istio.io/excludeInboundPorts")
			requireManifestPathString(t, runner, port, "spec", "template", "metadata", "annotations", "traffic.sidecar.istio.io/excludeOutboundPorts")
			if profile == "hardened" {
				var config map[string]any
				if err := json.Unmarshal([]byte(fmt.Sprint(transportAt(t, runtime, "spec", "template", "metadata", "annotations", "proxy.istio.io/config"))), &config); err != nil {
					t.Fatal(err)
				}
				prefixes := transportStrings(t, transportAt(t, config, "proxyStatsMatcher", "inclusionPrefixes"))
				if !reflect.DeepEqual(prefixes, []string{"sds.tetral.runtime.direct_leaf.", "sds.tetral.runtime.direct_validation."}) {
					t.Fatal("named SDS readiness statistics omitted")
				}
				env := transportEnv(t, runtime)
				if strings.Contains(fmt.Sprint(env), "TLS_KEY_PATH") {
					t.Fatal("Bun Runtime received TLS privatekey")
				}
			}
		})
	}
	for _, bad := range []string{"routing.enabled=false", "routing.version=1.30.0", "transport.profile=plain", "transport.databaseServerName="} {
		requireRenderError(t, helm, chart, []string{bad})
	}
}

func TestDeploymentLifecycleBudgets(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	objects := uniqueObjects(t, renderChart(t, helm, chart))
	for role, settings := range map[string]map[string]string{"bridge": {"TETRAL_DRAIN_TIMEOUT_MS": "40000", "TETRAL_CANCEL_JOIN_TIMEOUT_MS": "5000"}, "job-runner": {"TETRAL_DRAIN_TIMEOUT_MS": "30000", "TETRAL_CANCEL_JOIN_TIMEOUT_MS": "5000"}, "sandbox": {"TETRAL_DRAIN_TIMEOUT_MS": "30000", "TETRAL_CANCEL_JOIN_TIMEOUT_MS": "5000"}, "provider-gateway": {"TETRAL_SERVICE_DRAIN_TIMEOUT_MS": "30000", "TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS": "5000"}, "mcp-connector": {"TETRAL_SERVICE_DRAIN_TIMEOUT_MS": "30000", "TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS": "5000"}, "web-connector": {"TETRAL_SERVICE_DRAIN_TIMEOUT_MS": "10000"}, "queue": {"TETRAL_QUEUE_DRAIN_TIMEOUT_MS": "10000"}} {
		env := transportEnv(t, objects["apps/v1|Deployment|tetral-system|"+role])
		for k, v := range settings {
			if env[k] != v {
				t.Fatalf("%s actualparser env%s=%s want%s", role, k, env[k], v)
			}
		}
	}
	for _, bad := range []string{"lifecycle.runnerDrainMs=35001", "lifecycle.bridgeDrainMs=45001", "lifecycle.sandboxDrainMs=45001", "lifecycle.webDrainMs=20001", "lifecycle.queueDrainMs=25001", "lifecycle.cancelJoinMs=0", "lifecycle.providerJoinMs=0", "lifecycle.mcpJoinMs=0", "lifecycle.providerDrainMs=50000", "lifecycle.mcpDrainMs=50000", "lifecycle.providerJoinMs=25000", "lifecycle.mcpJoinMs=25000"} {
		requireRenderError(t, helm, chart, []string{bad})
	}
	requireRenderError(t, helm, chart, []string{"transport.profile=hardened", "lifecycle.queueDrainMs=20001"})
	for _, owner := range []string{"provider", "mcp"} {
		renderChart(t, helm, chart, "lifecycle."+owner+"DrainMs=49999")
	}
	changed := uniqueObjects(t, renderChart(t, helm, chart, "lifecycle.providerDrainMs=200", "lifecycle.providerJoinMs=1000", "lifecycle.mcpDrainMs=250", "lifecycle.mcpJoinMs=1200"))
	for role, expected := range map[string]map[string]string{"provider-gateway": {"TETRAL_SERVICE_DRAIN_TIMEOUT_MS": "200", "TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS": "1000"}, "mcp-connector": {"TETRAL_SERVICE_DRAIN_TIMEOUT_MS": "250", "TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS": "1200"}} {
		env := transportEnv(t, changed["apps/v1|Deployment|tetral-system|"+role])
		for key, value := range expected {
			if env[key] != value {
				t.Fatalf("%s configured%s=%s want%s", role, key, env[key], value)
			}
		}
	}
	renderChart(t, helm, chart, "lifecycle.queueDrainMs=25000")
	renderChart(t, helm, chart, "transport.profile=hardened", "lifecycle.queueDrainMs=20000")
}
