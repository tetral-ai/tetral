package helm_test

import (
	"fmt"
	"net/netip"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

type policyPod struct {
	namespace string
	labels    map[string]string
	ip        netip.Addr
}

// This matcher covers the numeric ports and matchLabels used by these owning
// manifests. Unsupported selector/port shapes fail explicitly. It validates
// rendered configuration, not live CNI enforcement.
func policySelectorMatches(raw any, labels map[string]string) (bool, error) {
	selector, ok := raw.(map[string]any)
	if !ok {
		return false, fmt.Errorf("selector is not a mapping")
	}
	for key := range selector {
		if key != "matchLabels" {
			return false, fmt.Errorf("unsupported selector field %s", key)
		}
	}
	if selector["matchLabels"] == nil {
		return true, nil
	}
	required, ok := selector["matchLabels"].(map[string]any)
	if !ok {
		return false, fmt.Errorf("matchLabels is not a mapping")
	}
	for key, value := range required {
		if labels[key] != fmt.Sprint(value) {
			return false, nil
		}
	}
	return true, nil
}

func policyPeerMatches(peer map[string]any, namespace string, pod policyPod) (bool, error) {
	for key := range peer {
		if key != "namespaceSelector" && key != "podSelector" && key != "ipBlock" {
			return false, fmt.Errorf("unsupported peer field %s", key)
		}
	}
	if raw, ok := peer["ipBlock"]; ok {
		if len(peer) != 1 {
			return false, fmt.Errorf("ipBlock combined with selectors")
		}
		block, ok := raw.(map[string]any)
		if !ok {
			return false, fmt.Errorf("ipBlock is not a mapping")
		}
		prefix, err := netip.ParsePrefix(fmt.Sprint(block["cidr"]))
		if err != nil {
			return false, err
		}
		if !prefix.Contains(pod.ip) {
			return false, nil
		}
		if exceptions, ok := block["except"].([]any); ok {
			for _, exception := range exceptions {
				prefix, err := netip.ParsePrefix(fmt.Sprint(exception))
				if err != nil {
					return false, err
				}
				if prefix.Contains(pod.ip) {
					return false, nil
				}
			}
		}
		return true, nil
	}
	if raw, ok := peer["namespaceSelector"]; ok {
		matches, err := policySelectorMatches(raw, map[string]string{"kubernetes.io/metadata.name": pod.namespace})
		if err != nil || !matches {
			return false, err
		}
	} else if _, ok := peer["podSelector"]; ok && pod.namespace != namespace {
		return false, nil
	}
	if raw, ok := peer["podSelector"]; ok {
		return policySelectorMatches(raw, pod.labels)
	}
	return true, nil
}

func policyPortMatches(raw any, protocol string, port int) (bool, error) {
	if raw == nil {
		return true, nil // An absent port list grants all ports.
	}
	ports, ok := raw.([]any)
	if !ok {
		return false, fmt.Errorf("ports is not a list")
	}
	if len(ports) == 0 {
		return true, nil
	}
	matched := false
	for _, raw := range ports {
		entry, ok := raw.(map[string]any)
		if !ok {
			return false, fmt.Errorf("port is not a mapping")
		}
		for key := range entry {
			if key != "port" && key != "protocol" {
				return false, fmt.Errorf("unsupported port field %s", key)
			}
		}
		entryProtocol := "TCP"
		if entry["protocol"] != nil {
			entryProtocol = fmt.Sprint(entry["protocol"])
		}
		if entry["port"] == nil {
			matched = matched || entryProtocol == protocol
			continue
		}
		number, err := strconv.Atoi(fmt.Sprint(entry["port"]))
		if err != nil {
			return false, fmt.Errorf("unsupported nonnumeric port: %w", err)
		}
		matched = matched || (entryProtocol == protocol && number == port)
	}
	return matched, nil
}

// Isolation is established only by policies selecting this Pod. Grants from
// those policies add together; a nonselecting policy does not deny traffic.
func policyDirectionAllows(t *testing.T, objects map[string]map[string]any, local, remote policyPod, direction, protocol string, port int) (bool, bool, error) {
	t.Helper()
	isolated, allowed := false, false
	peerField, policyType := "to", "Egress"
	if direction == "ingress" {
		peerField, policyType = "from", "Ingress"
	}
	for _, object := range objects {
		if object["apiVersion"] != "networking.k8s.io/v1" || object["kind"] != "NetworkPolicy" {
			continue
		}
		if fmt.Sprint(transportAt(t, object, "metadata", "namespace")) != local.namespace {
			continue
		}
		spec := transportMap(t, object["spec"])
		selected, err := policySelectorMatches(spec["podSelector"], local.labels)
		if err != nil {
			return false, false, err
		}
		if !selected {
			continue
		}
		types := spec["policyTypes"]
		applies := policyType == "Ingress" || spec["egress"] != nil
		if types != nil {
			applies = false
			for _, kind := range transportList(t, types) {
				applies = applies || kind == policyType
			}
		}
		if !applies {
			continue
		}
		isolated = true
		if spec[direction] == nil {
			continue
		}
		for _, raw := range transportList(t, spec[direction]) {
			rule := transportMap(t, raw)
			matches, err := policyPortMatches(rule["ports"], protocol, port)
			if err != nil {
				return false, false, err
			}
			if !matches {
				continue
			}
			if rule[peerField] == nil || len(transportList(t, rule[peerField])) == 0 {
				allowed = true
				continue
			}
			for _, peer := range transportList(t, rule[peerField]) {
				matches, err := policyPeerMatches(transportMap(t, peer), local.namespace, remote)
				if err != nil {
					return false, false, err
				}
				allowed = allowed || matches
			}
		}
	}
	return isolated, !isolated || allowed, nil
}

func runtimePolicyPods(t *testing.T, objects map[string]map[string]any) (policyPod, policyPod) {
	t.Helper()
	pod := func(namespace, name, ip string) policyPod {
		d := objects["apps/v1|Deployment|"+namespace+"|"+name]
		labels := map[string]string{}
		for key, value := range transportMap(t, transportAt(t, d, "spec", "template", "metadata", "labels")) {
			labels[key] = fmt.Sprint(value)
		}
		return policyPod{namespace: namespace, labels: labels, ip: netip.MustParseAddr(ip)}
	}
	return pod("tetral-system", "job-runner", "10.0.0.10"), pod("tetral-agent-runtime", "agent-runtime", "10.0.0.20")
}

func validateRuntimeMetricsPolicyPair(t *testing.T, objects map[string]map[string]any, commandPort int) error {
	t.Helper()
	runner, runtime := runtimePolicyPods(t, objects)
	for _, port := range []int{8080, commandPort} {
		for _, pair := range []struct {
			local, remote policyPod
			direction     string
		}{{runner, runtime, "egress"}, {runtime, runner, "ingress"}} {
			isolated, allowed, err := policyDirectionAllows(t, objects, pair.local, pair.remote, pair.direction, "TCP", port)
			if err != nil {
				return err
			}
			if !isolated || !allowed {
				return fmt.Errorf("%s %s TCP%d: isolated=%t allowed=%t", pair.local.labels["app.kubernetes.io/name"], pair.direction, port, isolated, allowed)
			}
		}
	}
	// The owner must use one combined namespace+pod peer and only the two
	// required ports. Pair admission alone would not detect a broad OR grant.
	policy := objects["networking.k8s.io/v1|NetworkPolicy|tetral-system|job-runner"]
	wantPeer := map[string]any{
		"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": runtime.namespace}},
		"podSelector":       map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/name": "agent-runtime"}},
	}
	exact := false
	for _, raw := range transportList(t, transportAt(t, policy, "spec", "egress")) {
		rule := transportMap(t, raw)
		if !reflect.DeepEqual(rule["to"], []any{wantPeer}) {
			continue
		}
		ports, ok := rule["ports"].([]any)
		if !ok {
			return fmt.Errorf("Runner Runtime grant lacks an explicit port list")
		}
		metrics, command := false, false
		for _, port := range ports {
			metrics = metrics || reflect.DeepEqual(port, map[string]any{"protocol": "TCP", "port": 8080})
			command = command || reflect.DeepEqual(port, map[string]any{"protocol": "TCP", "port": commandPort})
		}
		exact = exact || (len(ports) == 2 && metrics && command)
	}
	if !exact {
		return fmt.Errorf("Runner metrics/command egress must have one exact combined Runtime peer and only TCP8080/TCP%d", commandPort)
	}
	otherPort := 19443
	if commandPort == otherPort {
		otherPort = 19090
	}
	wrongNamespace, wrongPod := runtime, runtime
	wrongNamespace.namespace = "unrelated-runtime"
	wrongPod.labels = map[string]string{"app.kubernetes.io/name": "unrelated", "app.kubernetes.io/part-of": "tetral"}
	for _, negative := range []struct {
		pod      policyPod
		protocol string
		port     int
	}{{wrongNamespace, "TCP", 8080}, {wrongPod, "TCP", 8080}, {runtime, "UDP", 8080}, {runtime, "TCP", 9090}, {runtime, "TCP", otherPort}, {runtime, "TCP", 8081}} {
		isolated, allowed, err := policyDirectionAllows(t, objects, runner, negative.pod, "egress", negative.protocol, negative.port)
		if err != nil {
			return err
		}
		if !isolated || allowed {
			return fmt.Errorf("Runner unexpectedly permits %s/%s %s%d", negative.pod.namespace, negative.pod.labels["app.kubernetes.io/name"], negative.protocol, negative.port)
		}
	}
	return nil
}

func TestRuntimeMetricsNetworkPolicyPair(t *testing.T) {
	root, helm := engineRoot(t), requireHelm(t)
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			commandPort := 19090
			if profile == "hardened" {
				commandPort = 19443
			}
			objects := uniqueObjects(t, renderChart(t, helm, filepath.Join(root, "deploy/helm/tetral"), "transport.profile="+profile))
			if err := validateRuntimeMetricsPolicyPair(t, objects, commandPort); err != nil {
				t.Fatal(err)
			}
			reordered := clonePolicyObjects(objects)
			ports := transportList(t, runtimeMetricsEgressRule(t, reordered)["ports"])
			ports[0], ports[1] = ports[1], ports[0]
			if err := validateRuntimeMetricsPolicyPair(t, reordered, commandPort); err != nil {
				t.Fatalf("equivalent port ordering rejected: %v", err)
			}
			paths := canonicalManifestPaths(t, root)
			if profile == "hardened" {
				paths = []string{filepath.Join(root, "deploy/kubernetes/profiles/hardened/workloads.yaml")}
			}
			if err := validateRuntimeMetricsPolicyPair(t, uniqueObjects(t, readManifestObjects(t, paths)), commandPort); err != nil {
				t.Fatalf("raw %s projection: %v", profile, err)
			}
			if profile == "standard-routed" {
				paths := []string{filepath.Join(root, "deploy/kubernetes/internal-security.yaml")}
				for _, service := range []string{"job-runner", "agent-runtime"} {
					for _, fragment := range []string{"deployment.yaml", "networkpolicy.yaml"} {
						paths = append(paths, filepath.Join(root, "services", service, "k8s", fragment))
					}
				}
				if err := validateRuntimeMetricsPolicyPair(t, uniqueObjects(t, readManifestObjects(t, paths)), commandPort); err != nil {
					t.Fatalf("service-owned projection: %v", err)
				}
			}
		})
	}
}

func runtimeMetricsEgressRule(t *testing.T, objects map[string]map[string]any) map[string]any {
	t.Helper()
	policy := objects["networking.k8s.io/v1|NetworkPolicy|tetral-system|job-runner"]
	for _, raw := range transportList(t, transportAt(t, policy, "spec", "egress")) {
		rule := transportMap(t, raw)
		matches, err := policyPortMatches(rule["ports"], "TCP", 8080)
		if err != nil {
			t.Fatal(err)
		}
		if matches {
			return rule
		}
	}
	t.Fatal("test setup lacks Runtime metrics egress rule")
	return nil
}

func TestRuntimeMetricsNetworkPolicyControls(t *testing.T) {
	root, helm := engineRoot(t), requireHelm(t)
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			commandPort, otherPort := 19090, 19443
			if profile == "hardened" {
				commandPort, otherPort = otherPort, commandPort
			}
			objects := uniqueObjects(t, renderChart(t, helm, filepath.Join(root, "deploy/helm/tetral"), "transport.profile="+profile))
			if err := validateRuntimeMetricsPolicyPair(t, objects, commandPort); err != nil {
				t.Fatal(err)
			}
			mutations := []struct {
				name   string
				mutate func(map[string]map[string]any)
			}{
				{"missing-egress", func(objects map[string]map[string]any) {
					runtimeMetricsEgressRule(t, objects)["ports"] = []any{map[string]any{"protocol": "TCP", "port": commandPort}}
				}},
				{"missing-ingress", func(objects map[string]map[string]any) {
					policy := objects["networking.k8s.io/v1|NetworkPolicy|tetral-agent-runtime|agent-runtime"]
					for _, raw := range transportList(t, transportAt(t, policy, "spec", "ingress")) {
						rule := transportMap(t, raw)
						matches, err := policyPortMatches(rule["ports"], "TCP", 8080)
						if err != nil {
							t.Fatal(err)
						}
						if matches {
							rule["ports"] = []any{map[string]any{"protocol": "TCP", "port": 8082}}
						}
					}
				}},
				{"missing-namespace-selector", func(objects map[string]map[string]any) {
					peer := transportMap(t, transportList(t, runtimeMetricsEgressRule(t, objects)["to"])[0])
					delete(peer, "namespaceSelector")
				}},
				{"missing-pod-selector", func(objects map[string]map[string]any) {
					peer := transportMap(t, transportList(t, runtimeMetricsEgressRule(t, objects)["to"])[0])
					delete(peer, "podSelector")
				}},
				{"split-or-selectors", func(objects map[string]map[string]any) {
					rule := runtimeMetricsEgressRule(t, objects)
					peer := transportMap(t, transportList(t, rule["to"])[0])
					rule["to"] = []any{map[string]any{"namespaceSelector": peer["namespaceSelector"]}, map[string]any{"podSelector": peer["podSelector"]}}
				}},
				{"wrong-destination-namespace", func(objects map[string]map[string]any) {
					peer := transportList(t, runtimeMetricsEgressRule(t, objects)["to"])[0]
					transportMap(t, transportAt(t, peer, "namespaceSelector", "matchLabels"))["kubernetes.io/metadata.name"] = "unrelated"
				}},
				{"wrong-destination-pod", func(objects map[string]map[string]any) {
					peer := transportList(t, runtimeMetricsEgressRule(t, objects)["to"])[0]
					transportMap(t, transportAt(t, peer, "podSelector", "matchLabels"))["app.kubernetes.io/name"] = "unrelated"
				}},
				{"udp-instead-of-tcp", func(objects map[string]map[string]any) {
					transportMap(t, transportList(t, runtimeMetricsEgressRule(t, objects)["ports"])[0])["protocol"] = "UDP"
				}},
				{"forbidden-9090", func(objects map[string]map[string]any) {
					rule := runtimeMetricsEgressRule(t, objects)
					rule["ports"] = append(transportList(t, rule["ports"]), map[string]any{"protocol": "TCP", "port": 9090})
				}},
				{"wrong-profile-command", func(objects map[string]map[string]any) {
					rule := runtimeMetricsEgressRule(t, objects)
					rule["ports"] = append(transportList(t, rule["ports"]), map[string]any{"protocol": "TCP", "port": otherPort})
				}},
				{"all-ports", func(objects map[string]map[string]any) {
					delete(runtimeMetricsEgressRule(t, objects), "ports")
				}},
				{"additional-policy-forbidden-port", func(objects map[string]map[string]any) {
					extra := clonePolicyObjects(objects)["networking.k8s.io/v1|NetworkPolicy|tetral-system|job-runner"]
					transportMap(t, extra["metadata"])["name"] = "extra-runner-egress"
					transportMap(t, extra["spec"])["egress"] = []any{map[string]any{"to": runtimeMetricsEgressRule(t, objects)["to"], "ports": []any{map[string]any{"protocol": "TCP", "port": 9090}}}}
					objects["networking.k8s.io/v1|NetworkPolicy|tetral-system|extra-runner-egress"] = extra
				}},
			}
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					changed := clonePolicyObjects(objects)
					mutation.mutate(changed)
					if err := validateRuntimeMetricsPolicyPair(t, changed, commandPort); err == nil {
						t.Fatal("invalid policy pair passed validation")
					}
				})
			}
			runner, runtime := runtimePolicyPods(t, objects)
			// Move the8080 grant into a second selected policy. Effective egress
			// still permits it: selecting policies add, rather than intersect.
			additive := clonePolicyObjects(objects)
			rule := runtimeMetricsEgressRule(t, additive)
			extra := clonePolicyObjects(additive)["networking.k8s.io/v1|NetworkPolicy|tetral-system|job-runner"]
			transportMap(t, extra["metadata"])["name"] = "metrics-additive"
			transportMap(t, extra["spec"])["egress"] = []any{rule}
			additive["networking.k8s.io/v1|NetworkPolicy|tetral-system|metrics-additive"] = extra
			transportMap(t, additive["networking.k8s.io/v1|NetworkPolicy|tetral-system|job-runner"]["spec"])["egress"] = []any{}
			isolated, allowed, err := policyDirectionAllows(t, additive, runner, runtime, "egress", "TCP", 8080)
			if err != nil || !isolated || !allowed {
				t.Fatalf("additive grant not honored: isolated=%t allowed=%t err=%v", isolated, allowed, err)
			}
			// No policy selects this unrelated source. Nonselection is not a
			// deny, and is never used as a forbidden-source oracle here.
			unselected := runner
			unselected.labels = map[string]string{"app.kubernetes.io/name": "unrelated"}
			isolated, allowed, err = policyDirectionAllows(t, objects, unselected, runtime, "egress", "TCP", 8080)
			if err != nil || isolated || !allowed {
				t.Fatalf("nonselection incorrectly treated as denial: isolated=%t allowed=%t err=%v", isolated, allowed, err)
			}
		})
	}
}

func clonePolicyObjects(objects map[string]map[string]any) map[string]map[string]any {
	var clone func(any) any
	clone = func(value any) any {
		switch value := value.(type) {
		case map[string]any:
			copied := make(map[string]any, len(value))
			for key, item := range value {
				copied[key] = clone(item)
			}
			return copied
		case []any:
			copied := make([]any, len(value))
			for i, item := range value {
				copied[i] = clone(item)
			}
			return copied
		default:
			return value
		}
	}
	copied := make(map[string]map[string]any, len(objects))
	for key, object := range objects {
		copied[key] = clone(object).(map[string]any)
	}
	return copied
}
