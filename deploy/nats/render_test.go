package nats_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestPublicStreamingBrokerConfiguration(t *testing.T) {
	for _, executable := range []string{"python3", "helm"} {
		if _, err := exec.LookPath(executable); err != nil {
			t.Fatalf("required render prerequisite %s unavailable", executable)
		}
	}
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []struct {
		profile  string
		replicas int
	}{{"standard-routed", 1}, {"hardened", 3}, {"hardened", 4}} {
		t.Run(fmt.Sprintf("%s-%d", variant.profile, variant.replicas), func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "nats.yaml")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", filepath.Join(directory, "render.py"), "--charts-dir", filepath.Join(directory, "charts"), "--output", output, "--profile", variant.profile, "--replicas", fmt.Sprint(variant.replicas))
			if data, err := command.CombinedOutput(); err != nil {
				t.Fatalf("locked official broker render: %v %s", err, data)
			}
			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			objects := decode(t, data)
			stateful := objects["StatefulSet"]
			if stateful == nil {
				t.Fatal("official broker StatefulSet missing")
			}
			spec := stateful["spec"].(map[string]any)
			if spec["replicas"] != variant.replicas {
				t.Fatal("replica count differs from route generation")
			}
			if claims, _ := spec["volumeClaimTemplates"].([]any); len(claims) != 0 {
				t.Fatal("Core preview broker must not store deltas on a PVC")
			}
			pod := spec["template"].(map[string]any)["spec"].(map[string]any)
			if pod["terminationGracePeriodSeconds"] != 60 {
				t.Fatal("lame-duck grace budget changed")
			}
			if pod["serviceAccountName"] != "tetral-nats" || pod["automountServiceAccountToken"] != false || objects["ServiceAccount"] == nil {
				t.Fatal("broker must own an explicit tokenless role identity")
			}
			if pod["affinity"] == nil || pod["topologySpreadConstraints"] == nil {
				t.Fatal("cross-node placement constraints missing")
			}
			container := pod["containers"].([]any)[0].(map[string]any)
			if container["image"] != "docker.io/nats:2.15.0-alpine@sha256:ac8f88a6494bffc2c2a5289a0ca61cb28a9145c11ba5677cf24265d07f46d8d4" {
				t.Fatal("server image is not the independently verified pin")
			}
			lifecycle := container["lifecycle"].(map[string]any)["preStop"].(map[string]any)["exec"].(map[string]any)["command"]
			if !reflect.DeepEqual(lifecycle, []any{"nats-server", "-sl=ldm=/var/run/nats/nats.pid"}) {
				t.Fatal("official lame-duck stop hook missing")
			}
			if objects["PodDisruptionBudget"]["spec"].(map[string]any)["maxUnavailable"] != 1 {
				t.Fatal("voluntary eviction bound missing")
			}
			config := objects["ConfigMap"]["data"].(map[string]any)["nats.conf"].(string)
			if strings.Contains(config, "jetstream") || strings.Contains(string(data), "PersistentVolumeClaim") {
				t.Fatal("JetStream/durable delta state enabled")
			}
			for _, required := range []string{"$NATS_CLIENT_ADVERTISE", "$NATS_CLUSTER_USER", "$NATS_CLUSTER_PASSWORD", "\"no_advertise\": false", "\"lame_duck_grace_period\": \"10s\"", "\"lame_duck_duration\": \"30s\""} {
				if !strings.Contains(config, required) {
					t.Errorf("missing broker contract %s", required)
				}
			}
			// Empty allow lists are unrestricted in NATS. Parse the rendered
			// role permissions to require an explicit opposite-operation deny.
			var parsed struct {
				Authorization struct {
					Users []struct {
						Permissions map[string]any `json:"permissions"`
					} `json:"users"`
				} `json:"authorization"`
			}
			resolved := regexp.MustCompile(`\$[A-Z_0-9]+`).ReplaceAllString(config, `"fixture"`)
			if err := json.Unmarshal([]byte(resolved), &parsed); err != nil {
				t.Fatal(err)
			}
			if len(parsed.Authorization.Users) != 2 {
				t.Fatal("expected exactly the publisher/subscriber roles")
			}
			deny := map[string]any{"deny": []any{">"}}
			allow := []any{"preview.v1.>"}
			if !reflect.DeepEqual(parsed.Authorization.Users[0].Permissions, map[string]any{"publish": allow, "subscribe": deny}) ||
				!reflect.DeepEqual(parsed.Authorization.Users[1].Permissions, map[string]any{"publish": deny, "subscribe": allow}) {
				t.Fatal("broker role ACLs must allow only the preview direction and explicitly deny the opposite operation")
			}
			for i := range variant.replicas {
				if !strings.Contains(config, fmt.Sprintf("$NATS_ROUTE_%d", i)) {
					t.Fatal("missing route for declared broker")
				}
			}
			if strings.Contains(config, fmt.Sprintf("$NATS_ROUTE_%d", variant.replicas)) {
				t.Fatal("route set exceeds replicas")
			}
			if variant.profile == "hardened" {
				for _, required := range []string{"\"handshake_first\": true", "\"verify\": true", "/etc/nats-certs/nats/tls.crt", "/etc/nats-certs/cluster/tls.crt", "tetral-nats-server-tls", "tetral-nats-route-tls", "tetral-nats-broker-trust"} {
					if !strings.Contains(string(data), required) {
						t.Errorf("missing protected broker setting %s", required)
					}
				}
				if strings.Contains(config, "verify_and_map") || strings.Contains(config, "handshake_first: auto") {
					t.Fatal("protected broker replaced credentials or added plaintext fallback")
				}
			}
			if variant.replicas == 1 || variant.replicas == 3 {
				canonical := "manifests.yaml"
				if variant.profile == "hardened" {
					canonical = "manifests-hardened.yaml"
				}
				want, err := os.ReadFile(filepath.Join(directory, canonical))
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(manifestDocuments(t, data), manifestDocuments(t, want)) {
					t.Fatal("official release render differs from checked raw broker manifests")
				}
			}
		})
	}
	corrupt := t.TempDir()
	archive, err := os.ReadFile(filepath.Join(directory, "charts", "nats-2.15.0.tgz"))
	if err != nil {
		t.Fatal(err)
	}
	// #nosec G703 -- corrupt is a test-owned TempDir and the copied archive name is fixed.
	if err := os.WriteFile(filepath.Join(corrupt, "nats-2.15.0.tgz"), append(archive, 0), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "python3", filepath.Join(directory, "render.py"), "--charts-dir", corrupt, "--output", filepath.Join(t.TempDir(), "denied.yaml"))
	if data, err := command.CombinedOutput(); err == nil || !bytes.Contains(data, []byte("archive digest differs for nats")) {
		t.Fatal("modified official chart archive accepted")
	}
}

func decode(t *testing.T, data []byte) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var obj map[string]any
		err := decoder.Decode(&obj)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if obj == nil {
			continue
		}
		kind := obj["kind"].(string)
		if kind != "Service" {
			out[kind] = obj
		}
	}
	return out
}

// Issuance is an operator-owned prerequisite; this guard checks the requested
// role identities and renewal contract, not Kubernetes reconciliation.
func TestNativePreviewCertificatePackaging(t *testing.T) {
	data, err := os.ReadFile("certificates.yaml")
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	expected := map[string]string{
		"tetral-nats-server-tls":     "tetral-nats",
		"tetral-nats-route-tls":      "tetral-nats",
		"tetral-nats-publisher-tls":  "provider-gateway",
		"tetral-nats-subscriber-tls": "event-stream",
	}
	for {
		var certificate map[string]any
		if err := decoder.Decode(&certificate); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		metadata := certificate["metadata"].(map[string]any)
		name := metadata["name"].(string)
		role, ok := expected[name]
		if !ok {
			t.Fatalf("unexpected or duplicate role leaf %s", name)
		}
		delete(expected, name)
		spec := certificate["spec"].(map[string]any)
		issuer := spec["issuerRef"].(map[string]any)
		if certificate["apiVersion"] != "cert-manager.io/v1" || certificate["kind"] != "Certificate" || metadata["namespace"] != "tetral-system" || spec["secretName"] != name || issuer["name"] != "tetral-native" || issuer["kind"] != "ClusterIssuer" || spec["duration"] != "24h" || spec["renewBefore"] != "8h" || spec["privateKey"].(map[string]any)["rotationPolicy"] != "Always" {
			t.Fatalf("native role leaf issuance contract differs: %s", name)
		}
		if !reflect.DeepEqual(spec["uris"], []any{"spiffe://cluster.local/ns/tetral-system/sa/" + role}) {
			t.Fatal("wrong native role URI identity")
		}
		dns := fmt.Sprint(spec["dnsNames"])
		usages := fmt.Sprint(spec["usages"])
		if strings.HasPrefix(name, "tetral-nats-server") || strings.HasPrefix(name, "tetral-nats-route") {
			if !strings.Contains(dns, "*.tetral-nats-headless.tetral-system.svc.cluster.local") || !strings.Contains(usages, "server auth") {
				t.Fatal("broker leaf misses advertised DNS or server EKU")
			}
		}
		if name != "tetral-nats-server-tls" && !strings.Contains(usages, "client auth") {
			t.Fatal("native client role misses client EKU")
		}
	}
	if len(expected) != 0 {
		t.Fatalf("missing role Certificates: %v", expected)
	}
}

// Compare every document in order, including Services and exact ConfigMap
// strings. Helm versions may change document-separator padding without changing
// the resources; decoding must not drop or overwrite any rendered object.
func manifestDocuments(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	documents, err := parseManifestDocuments(data)
	if err != nil {
		t.Fatal(err)
	}
	return documents
}

func parseManifestDocuments(data []byte) ([]map[string]any, error) {
	var documents []map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var object map[string]any
		if err := decoder.Decode(&object); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("manifest document %d: %w", len(documents)+1, err)
		}
		if len(object) == 0 {
			return nil, fmt.Errorf("empty manifest document %d", len(documents)+1)
		}
		documents = append(documents, object)
	}
	if len(documents) == 0 {
		return nil, fmt.Errorf("empty manifest set")
	}
	return documents, nil
}

func TestManifestDocumentParity(t *testing.T) {
	service := "apiVersion: v1\nkind: Service\nmetadata:\n  name: broker\nspec:\n  ports:\n    - port: 4222\n"
	config := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: broker\ndata:\n  nats.conf: |+\n    first\n    ---\n    last\n\n"
	original := service + "---\n" + config
	for _, test := range []struct {
		name  string
		input string
		equal bool
	}{
		{"separator-padding", service + "\n\n---\n" + config, true},
		{"service-change", strings.Replace(original, "port: 4222", "port: 6222", 1), false},
		{"same-kind-object-added", service + "---\n" + original, false},
		{"object-removed", service, false},
		{"document-order", config + "---\n" + service, false},
		{"config-string-change", strings.Replace(original, "    first", "    changed", 1), false},
		{"config-trailing-newline", strings.TrimSuffix(original, "\n"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if equal := reflect.DeepEqual(manifestDocuments(t, []byte(original)), manifestDocuments(t, []byte(test.input))); equal != test.equal {
				t.Fatalf("document equality = %v, want %v", equal, test.equal)
			}
		})
	}
	for _, input := range []string{"", "# no resources\n", "{}\n", "kind: Service\nkind: ConfigMap\n"} {
		if _, err := parseManifestDocuments([]byte(input)); err == nil {
			t.Fatalf("invalid or empty manifest set accepted: %q", input)
		}
	}
}
