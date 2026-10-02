package transporttest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

const CallerURI = "spiffe://transport.test/ns/test/sa/caller"
const ReceiverURI = "spiffe://transport.test/ns/test/sa/receiver"

type ProxyPair struct {
	Directory, Network, ReceiverAddress, SourceAddress, ReceiverAdmin, SourceAdmin, BackendControl string
	Resources                                                                                      *testinfra.DockerResources
	Receiver, Source                                                                               *testinfra.DockerContainer
	Root                                                                                           *Authority
	Caller, ReceiverLeaf                                                                           Leaf
}

func RepositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root is unavailable")
		}
		dir = parent
	}
}
func Must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}
func WriteJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func NewProxyPair(t *testing.T) *ProxyPair {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := &ProxyPair{Directory: t.TempDir()}
	p.Resources = Must(testinfra.NewDockerResources(ctx, "transport"))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Resources.Close(cleanup); err != nil {
			t.Errorf("transport fixture cleanup: %v", err)
		}
	})
	image := Must(testinfra.PinnedEnvoyImage(RepositoryRoot(t)))
	network := Must(p.Resources.Network(ctx))
	p.Network = network
	p.Root = Must(NewAuthority("R1"))
	p.Caller = Must(p.Root.ValidLeaf("caller.transport.test", CallerURI))
	p.ReceiverLeaf = Must(p.Root.ValidLeaf("receiver.transport.test", ReceiverURI))
	for _, entry := range []struct {
		name string
		leaf Leaf
	}{{"receiver", p.ReceiverLeaf}, {"caller", p.Caller}} {
		if err := Project(filepath.Join(p.Directory, entry.name, "leaf"), "g1", map[string][]byte{"tls.crt": entry.leaf.Certificate, "tls.key": entry.leaf.Key}); err != nil {
			t.Fatal(err)
		}
		if err := Project(filepath.Join(p.Directory, entry.name, "trust"), "g1", map[string][]byte{"ca.crt": p.Root.PEM}); err != nil {
			t.Fatal(err)
		}
		if err := WriteSDS(p.Directory, entry.name, map[string]string{"receiver": CallerURI, "caller": ReceiverURI}[entry.name]); err != nil {
			t.Fatal(err)
		}
	}
	binary := filepath.Join(p.Directory, "backend")
	// #nosec G204 -- fixed Go backend package and flags; only a test-owned output directory varies.
	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, "./integration/transporttest/cmd/backend")
	build.Dir = RepositoryRoot(t)
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pinned-tool fixture backend: %v\n%s", err, output)
	}
	backend := Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: image, Network: network, Aliases: []string{"backend"}, Mounts: []testinfra.DockerMount{{Source: binary, Target: "/backend", ReadOnly: true}}, Ports: []int{8888}, Entrypoint: "/backend", User: "0"}))
	p.BackendControl = "http://" + Must(backend.Address(ctx, 8888))
	for _, role := range []string{"receiver", "caller"} {
		port := 10000
		if role == "caller" {
			port = 10001
		}
		config := Bootstrap(role, port)
		if err := WriteJSON(filepath.Join(p.Directory, role, "bootstrap.json"), config); err != nil {
			t.Fatal(err)
		}
		container := Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: image, Network: network, Aliases: []string{role}, Mounts: []testinfra.DockerMount{{Source: p.Directory, Target: "/fixture", ReadOnly: true}}, Ports: []int{port, 9901}, Entrypoint: "/usr/local/bin/envoy", Command: []string{"-c", "/fixture/" + role + "/bootstrap.json", "--concurrency", "1", "--log-level", "warning"}, User: "0"}))
		if role == "receiver" {
			p.Receiver = container
			p.ReceiverAddress = Must(container.Address(ctx, port))
			p.ReceiverAdmin = "http://" + Must(container.Address(ctx, 9901))
		} else {
			p.Source = container
			p.SourceAddress = Must(container.Address(ctx, port))
			p.SourceAdmin = "http://" + Must(container.Address(ctx, 9901))
		}
		t.Logf("local %s proxy locked reference=%s actual image=%s", role, image, container.ImageID)
	}
	for _, address := range []string{p.ReceiverAdmin, p.SourceAdmin, p.BackendControl} {
		if err := Await(ctx, func() bool {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, address+map[bool]string{true: "/operations", false: "/ready"}[address == p.BackendControl], nil)
			if err != nil {
				return false
			}
			response, err := (&http.Client{Timeout: time.Second}).Do(request)
			if err != nil {
				return false
			}
			defer func() { _ = response.Body.Close() }()
			return response.StatusCode == 200
		}); err != nil {
			logs, _ := p.Receiver.Logs(ctx)
			t.Fatalf("local proxy fixture startup: %v\n%s", err, logs)
		}
	}
	return p
}

// ReplaceSource performs the explicit trust-retirement procedure after callers
// have joined admitted RPCs: stop the old connection owner, observe its exit,
// and start a fresh proxy with only the mounted current trust generation.
func (p *ProxyPair) ReplaceSource(ctx context.Context, t *testing.T) {
	t.Helper()
	if err := p.Source.Signal(ctx, "TERM"); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Source.Wait(ctx); err != nil || code != 0 {
		t.Fatalf("old source proxy did not join: code=%d err=%v", code, err)
	}
	image := Must(testinfra.PinnedEnvoyImage(RepositoryRoot(t)))
	p.Source = Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: image, Network: p.Network, Mounts: []testinfra.DockerMount{{Source: p.Directory, Target: "/fixture", ReadOnly: true}}, Ports: []int{10001, 9901}, Entrypoint: "/usr/local/bin/envoy", Command: []string{"-c", "/fixture/caller/bootstrap.json", "--concurrency", "1", "--log-level", "warning"}, User: "0"}))
	p.SourceAddress = Must(p.Source.Address(ctx, 10001))
	p.SourceAdmin = "http://" + Must(p.Source.Address(ctx, 9901))
}

func Await(ctx context.Context, condition func() bool) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (p *ProxyPair) Operations(ctx context.Context) (int64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.BackendControl+"/operations", nil)
	if err != nil {
		return 0, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	var count int64
	err = json.NewDecoder(response.Body).Decode(&count)
	return count, err
}

func (p *ProxyPair) ReleaseResponse(ctx context.Context, operationID string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.BackendControl+"/release-response?id="+url.QueryEscape(operationID), nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return errors.New("stream operation barrier did not match")
	}
	return nil
}

func WriteSDS(directory, role, peerURI string) error {
	path := filepath.Join(directory, role, "sds")
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	leaf := map[string]any{"resources": []any{map[string]any{"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret", "name": role + ".leaf", "tls_certificate": map[string]any{"certificate_chain": map[string]any{"filename": "/fixture/" + role + "/leaf/tls.crt"}, "private_key": map[string]any{"filename": "/fixture/" + role + "/leaf/tls.key"}, "watched_directory": map[string]any{"path": "/fixture/" + role + "/leaf"}}}}}
	validation := map[string]any{"resources": []any{map[string]any{"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret", "name": role + ".validation", "validation_context": map[string]any{"trusted_ca": map[string]any{"filename": "/fixture/" + role + "/trust/ca.crt"}, "watched_directory": map[string]any{"path": "/fixture/" + role + "/trust"}, "match_typed_subject_alt_names": []any{map[string]any{"san_type": "URI", "matcher": map[string]any{"exact": peerURI}}}}}}}
	if err := WriteJSON(filepath.Join(path, "leaf.json"), leaf); err != nil {
		return err
	}
	return WriteJSON(filepath.Join(path, "validation.json"), validation)
}
func sds(role, name string) map[string]any {
	return map[string]any{"name": role + "." + name, "sds_config": map[string]any{"path_config_source": map[string]any{"path": "/fixture/" + role + "/sds/" + map[string]string{"leaf": "leaf.json", "validation": "validation.json"}[name], "watched_directory": map[string]any{"path": "/fixture/" + role + "/sds"}}}}
}
func commonTLS(role string) map[string]any {
	return map[string]any{"tls_params": map[string]any{"tls_minimum_protocol_version": "TLSv1_3", "tls_maximum_protocol_version": "TLSv1_3"}, "alpn_protocols": []string{"h2"}, "tls_certificate_sds_secret_configs": []any{sds(role, "leaf")}, "validation_context_sds_secret_config": sds(role, "validation")}
}
func socket(host string, port int) map[string]any {
	return map[string]any{"socket_address": map[string]any{"address": host, "port_value": port}}
}

// Bootstrap uses actual file SDS and a single-attempt proxy. The fixture's
// backend is test-only; deployment route effectiveness has separate owners.
func Bootstrap(role string, port int) map[string]any {
	cluster := map[string]any{"name": "backend", "type": "STRICT_DNS", "connect_timeout": "1s", "lb_policy": "LEAST_REQUEST", "common_lb_config": map[string]any{"healthy_panic_threshold": map[string]any{"value": 0}}, "load_assignment": map[string]any{"cluster_name": "backend", "endpoints": []any{map[string]any{"lb_endpoints": []any{map[string]any{"endpoint": map[string]any{"address": socket("backend", 9090)}}}}}}}
	chain := map[string]any{"filters": []any{map[string]any{"name": "envoy.filters.network.tcp_proxy", "typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy", "stat_prefix": "transport", "cluster": "backend", "max_connect_attempts": 1, "idle_timeout": "0s"}}}}
	if role == "receiver" {
		chain["transport_socket"] = map[string]any{"name": "envoy.transport_sockets.tls", "typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext", "common_tls_context": commonTLS(role), "require_client_certificate": true, "disable_stateless_session_resumption": true, "disable_stateful_session_resumption": true}}
	} else {
		cluster["load_assignment"] = map[string]any{"cluster_name": "backend", "endpoints": []any{map[string]any{"lb_endpoints": []any{map[string]any{"endpoint": map[string]any{"address": socket("receiver", 10000)}}}}}}
		cluster["transport_socket"] = map[string]any{"name": "envoy.transport_sockets.tls", "typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext", "common_tls_context": commonTLS(role), "sni": "receiver.transport.test"}}
		// A real HTTP/2 source proxy chooses an upstream per RPC; TLS still
		// terminates only at the receiver, before the plaintext fixture app.
		cluster["typed_extension_protocol_options"] = map[string]any{"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": map[string]any{"@type": "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions", "explicit_http_config": map[string]any{"http2_protocol_options": map[string]any{}}}}
		cluster["outlier_detection"] = map[string]any{"split_external_local_origin_errors": true, "consecutive_local_origin_failure": 2, "consecutive_5xx": 0, "consecutive_gateway_failure": 0, "base_ejection_time": "10s", "max_ejection_percent": 100}
		chain["filters"] = []any{map[string]any{"name": "envoy.filters.network.http_connection_manager", "typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager", "stat_prefix": "transport", "codec_type": "HTTP2", "stream_idle_timeout": "0s", "route_config": map[string]any{"name": "scoped-fixture", "virtual_hosts": []any{map[string]any{"name": "receiver", "domains": []any{"*"}, "routes": []any{map[string]any{"match": map[string]any{"prefix": "/"}, "route": map[string]any{"cluster": "backend", "timeout": "0s"}}}}}}, "http_filters": []any{map[string]any{"name": "envoy.filters.http.router", "typed_config": map[string]any{"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"}}}}}}

	}
	return map[string]any{"node": map[string]any{"id": role, "cluster": "transport-fixture"}, "admin": map[string]any{"address": socket("0.0.0.0", 9901)}, "static_resources": map[string]any{"listeners": []any{map[string]any{"name": fmt.Sprintf("%s.transport", role), "address": socket("0.0.0.0", port), "filter_chains": []any{chain}}}, "clusters": []any{cluster}}}
}

// RejectedUpdates observes the actual file-SDS consumer before testing retained
// credentials, so a racing test cannot mistake an unread malformed file for LKG.
func RejectedUpdates(ctx context.Context, admin string) (uint64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, admin+"/stats?format=json&filter=update_rejected", nil)
	if err != nil {
		return 0, err
	}
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	var out struct {
		Stats []struct {
			Name  string `json:"name"`
			Value uint64 `json:"value"`
		} `json:"stats"`
	}
	if err := json.NewDecoder(response.Body).Decode(&out); err != nil {
		return 0, err
	}
	var total uint64
	for _, stat := range out.Stats {
		total += stat.Value
	}
	return total, nil
}

func (p *ProxyPair) BackendState(ctx context.Context) (map[string]int64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.BackendControl+"/state", nil)
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	var state map[string]int64
	err = json.NewDecoder(response.Body).Decode(&state)
	return state, err
}
