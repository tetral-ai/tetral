package transporttest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

type RuntimePair struct {
	Directory, Address, PlainAddress, LoopbackAddress, Control, Admin, PodUID, ProcessID string
	Resources                                                                            *testinfra.DockerResources
	Runtime, Proxy                                                                       *testinfra.DockerContainer
	Leaf                                                                                 Leaf
}

// RenderDirectResources reads the exact selected chart resources at test time.
// Fixture substitution is limited to Docker mounts and published host addresses;
// the protected listener and static loopback cluster remain byte-for-byte values.
func RenderDirectResources(t *testing.T) (map[string]any, map[string]any, map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// #nosec G204 -- fixed Helm command/flags and repository-owned chart path, with no shell.
	command := exec.CommandContext(ctx, "helm", "template", "tetral", filepath.Join(RepositoryRoot(t), "deploy/helm/tetral"), "--set", "transport.profile=hardened")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("render actual Runtime EnvoyFilter: %v", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(output))
	var listener, cluster map[string]any
	var files map[string]string
	for {
		var object map[string]any
		if err := decoder.Decode(&object); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if object == nil {
			continue
		}
		metadata, _ := object["metadata"].(map[string]any)
		if object["kind"] == "ConfigMap" && metadata["name"] == "tetral-runtime-direct-sds" {
			files = map[string]string{}
			for name, value := range object["data"].(map[string]any) {
				files[name] = value.(string)
			}
		}
		if object["kind"] != "EnvoyFilter" || metadata["name"] != "tetral-runtime-direct-tls" {
			continue
		}
		patches := object["spec"].(map[string]any)["configPatches"].([]any)
		if len(patches) != 2 {
			t.Fatal("direct Runtime filter must have exactly two resources")
		}
		for _, raw := range patches {
			patch := raw.(map[string]any)
			if patch["match"].(map[string]any)["context"] != "SIDECAR_INBOUND" || patch["patch"].(map[string]any)["operation"] != "ADD" {
				t.Fatal("direct Runtime filter must be additive inbound configuration")
			}
			value := patch["patch"].(map[string]any)["value"].(map[string]any)
			switch patch["applyTo"] {
			case "LISTENER":
				listener = value
			case "CLUSTER":
				cluster = value
			default:
				t.Fatal("unexpected direct Runtime resource")
			}
		}
	}
	if listener == nil || cluster == nil || len(files) != 2 {
		t.Fatal("rendered direct Runtime resources are incomplete")
	}
	return listener, cluster, files
}

func NewRuntimePair(t *testing.T, profile, uid string, root *Authority, startProxy bool) *RuntimePair {
	return newRuntimePair(t, profile, uid, root, startProxy, "")
}

// NewRuntimeWithoutInitialCredentials keeps the proxy mandatory while the
// complete initial server credential generation is unavailable.
func NewRuntimeWithoutInitialCredentials(t *testing.T, uid string, root *Authority, missingResource string) *RuntimePair {
	if missingResource != "leaf" && missingResource != "validation" {
		t.Fatal("unknown missing initial SDS resource")
	}
	return newRuntimePair(t, "hardened", uid, root, true, missingResource)
}
func newRuntimePair(t *testing.T, profile, uid string, root *Authority, startProxy bool, missingResource string) *RuntimePair {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := &RuntimePair{Directory: t.TempDir(), PodUID: uid, ProcessID: "process-" + uid}
	p.Resources = Must(testinfra.NewDockerResources(ctx, "runtime-tls"))
	network := Must(p.Resources.Network(ctx))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if p.Control != "" {
			if startProxy && missingResource == "" {
				if err := p.Command(cleanup, "release"); err != nil {
					t.Errorf("Runtime release fixture join: %v", err)
				}
				if err := p.Command(cleanup, "stop"); err != nil {
					t.Errorf("Runtime stop fixture join: %v", err)
				}
				if exit, err := p.Runtime.Wait(cleanup); err != nil || exit != 0 {
					t.Errorf("Runtime child exit=%d err=%v", exit, err)
				}
			}
		}
		if err := p.Resources.Close(cleanup); err != nil {
			t.Errorf("Runtime transport cleanup: %v", err)
		}
	})
	if err := WriteJSON(filepath.Join(p.Directory, "runtime.json"), map[string]string{"profile": profile, "podUid": p.PodUID, "processId": p.ProcessID}); err != nil {
		t.Fatal(err)
	}
	bun := Must(testinfra.PinnedBunImage())
	p.Runtime = Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: bun, Network: network, Mounts: []testinfra.DockerMount{{Source: RepositoryRoot(t), Target: "/workspace", ReadOnly: true}, {Source: filepath.Join(p.Directory, "runtime.json"), Target: "/fixture/runtime.json", ReadOnly: true}}, Ports: []int{8888, 9901, 9090, 19090, 19443}, Entrypoint: "bun", Command: []string{"/workspace/services/agent-runtime/packages/runtime-pod/test/fixtures/direct-transport.ts", "/fixture/runtime.json"}, User: "0"}))
	p.Control = "http://" + Must(p.Runtime.Address(ctx, 8888))
	p.Admin = "http://" + Must(p.Runtime.Address(ctx, 9901))
	p.PlainAddress = Must(p.Runtime.Address(ctx, 19090))
	p.LoopbackAddress = Must(p.Runtime.Address(ctx, 9090))
	p.Address = p.PlainAddress
	if startProxy {
		bootstrap := map[string]any{"node": map[string]any{"id": uid, "cluster": "runtime-fixture"}, "admin": map[string]any{"address": socket("0.0.0.0", 9901)}, "static_resources": map[string]any{"listeners": []any{}, "clusters": []any{}}}
		mounts := []testinfra.DockerMount{}
		if profile == "hardened" {
			listener, cluster, files := RenderDirectResources(t)
			bootstrap["static_resources"] = map[string]any{"listeners": []any{listener}, "clusters": []any{cluster}}
			p.Leaf = Must(root.ValidLeaf("agent-runtime.tetral-agent-runtime.svc.cluster.local", "spiffe://cluster.local/ns/tetral-agent-runtime/sa/agent-runtime"))
			if err := Project(filepath.Join(p.Directory, "leaf"), "g1", map[string][]byte{"tls.crt": p.Leaf.Certificate, "tls.key": p.Leaf.Key}); err != nil {
				t.Fatal(err)
			}
			if err := Project(filepath.Join(p.Directory, "trust"), "g1", map[string][]byte{"ca.crt": root.PEM}); err != nil {
				t.Fatal(err)
			}
			if missingResource != "" {
				missingPath := filepath.Join(p.Directory, "leaf", "g1", "tls.key")
				if missingResource == "validation" {
					missingPath = filepath.Join(p.Directory, "trust", "g1", "ca.crt")
				}
				if err := os.Remove(missingPath); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Mkdir(filepath.Join(p.Directory, "sds"), 0700); err != nil {
				t.Fatal(err)
			}
			for name, value := range files {
				if err := os.WriteFile(filepath.Join(p.Directory, "sds", name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, part := range []string{"leaf", "trust", "sds"} {
				mounts = append(mounts, testinfra.DockerMount{Source: filepath.Join(p.Directory, part), Target: "/var/run/tetral/runtime-direct/" + part, ReadOnly: true})
			}
			p.Address = Must(p.Runtime.Address(ctx, 19443))
		}
		if err := WriteJSON(filepath.Join(p.Directory, "bootstrap.json"), bootstrap); err != nil {
			t.Fatal(err)
		}
		mounts = append(mounts, testinfra.DockerMount{Source: filepath.Join(p.Directory, "bootstrap.json"), Target: "/bootstrap.json", ReadOnly: true})
		image := Must(testinfra.PinnedEnvoyImage(RepositoryRoot(t)))
		p.Proxy = Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: image, NetworkContainer: p.Runtime, Mounts: mounts, Entrypoint: "/usr/local/bin/envoy", Command: []string{"-c", "/bootstrap.json", "--concurrency", "1", "--log-level", "warning"}, User: "0"}))
		t.Logf("direct Runtime locked Bun=%s actual=%s; proxy=%s actual=%s", bun, p.Runtime.ImageID, image, p.Proxy.ImageID)
		if missingResource == "" {
			if err := Await(ctx, func() bool { state, err := p.State(ctx); return err == nil && state.Ready }); err != nil {
				logs, _ := p.Runtime.Logs(ctx)
				t.Fatalf("actual Bun Runtime did not become ready: %v\n%s", err, logs)
			}
		}
	}
	return p
}

type RuntimeState struct {
	Operations int64 `json:"operations"`
	Ready      bool  `json:"ready"`
	Held       bool  `json:"held"`
}

func (p *RuntimePair) State(ctx context.Context) (RuntimeState, error) {
	var result RuntimeState
	body, err := p.request(ctx, "operations")
	if err == nil {
		err = json.Unmarshal(body, &result)
	}
	return result, err
}
func (p *RuntimePair) Command(ctx context.Context, command string) error {
	_, err := p.request(ctx, command)
	return err
}
func (p *RuntimePair) request(ctx context.Context, path string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.Control+"/"+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		return nil, errors.New("runtime fixture control failed")
	}
	return io.ReadAll(io.LimitReader(response.Body, 4096))
}
