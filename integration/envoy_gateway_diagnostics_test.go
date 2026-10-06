package integration

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

// Observe control-plane stages without recording resource bodies, peer
// certificates, header values, or arbitrary vendor error text in test output.
type envoyControlTrace struct {
	mu                                                                                   sync.Mutex
	hellos, h2Offered, verified, h2Negotiated, streams, requests, responses, acks, nacks int
	nackKinds                                                                            map[string]int
	privateNacks                                                                         []map[string]any
	listenerResponses                                                                    map[envoyListenerNonce]string
	listenerACKVersions                                                                  map[string]bool
	secretResponses                                                                      map[envoyListenerNonce]envoySecretResponse
	secretACKNames                                                                       map[string]map[string]bool
}

type envoySecretResponse struct {
	version string
	names   []string
}

type envoyListenerNonce struct {
	streamID int64
	nonce    string
}

func (trace *envoyControlTrace) observeTLS(config *tls.Config) *tls.Config {
	result := config.Clone()
	hello := result.GetConfigForClient
	verify := result.VerifyConnection
	result.GetConfigForClient = func(info *tls.ClientHelloInfo) (*tls.Config, error) {
		trace.mu.Lock()
		trace.hellos++
		for _, protocol := range info.SupportedProtos {
			if protocol == "h2" {
				trace.h2Offered++
				break
			}
		}
		trace.mu.Unlock()
		if hello != nil {
			return hello(info)
		}
		return nil, nil
	}
	result.VerifyConnection = func(state tls.ConnectionState) error {
		if verify != nil {
			if err := verify(state); err != nil {
				return err
			}
		}
		trace.mu.Lock()
		trace.verified++
		if state.NegotiatedProtocol == "h2" {
			trace.h2Negotiated++
		}
		trace.mu.Unlock()
		return nil
	}
	return result
}
func (trace *envoyControlTrace) callbacks() serverv3.CallbackFuncs {
	return serverv3.CallbackFuncs{DeltaStreamOpenFunc: func(context.Context, int64, string) error {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		trace.streams++
		return nil
	}, DeltaStreamClosedFunc: func(streamID int64, _ *corev3.Node) {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		for key := range trace.listenerResponses {
			if key.streamID == streamID {
				delete(trace.listenerResponses, key)
			}
		}
		for key := range trace.secretResponses {
			if key.streamID == streamID {
				delete(trace.secretResponses, key)
			}
		}
	}, StreamDeltaRequestFunc: func(streamID int64, request *discoveryv3.DeltaDiscoveryRequest) error {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		trace.requests++
		if request.ErrorDetail != nil && request.ErrorDetail.Code != 0 {
			trace.nacks++
			if request.TypeUrl == resourcev3.ListenerType {
				delete(trace.listenerResponses, envoyListenerNonce{streamID, request.ResponseNonce})
			}
			if request.TypeUrl == resourcev3.SecretType {
				delete(trace.secretResponses, envoyListenerNonce{streamID, request.ResponseNonce})
			}
			kind := "other"
			switch request.TypeUrl {
			case resourcev3.ClusterType:
				kind = "cluster"
			case resourcev3.EndpointType:
				kind = "endpoint"
			case resourcev3.ListenerType:
				kind = "listener"
			case resourcev3.RouteType:
				kind = "route"
			case resourcev3.SecretType:
				kind = "secret"
			}
			if trace.nackKinds == nil {
				trace.nackKinds = map[string]int{}
			}
			trace.nackKinds[kind]++
			if len(trace.privateNacks) < 20 {
				trace.privateNacks = append(trace.privateNacks, map[string]any{"kind": kind, "code": request.ErrorDetail.Code, "message": request.ErrorDetail.Message[:min(len(request.ErrorDetail.Message), 4096)]})
			}
		} else if request.ResponseNonce != "" {
			trace.acks++
			if request.TypeUrl == resourcev3.ListenerType {
				if version, ok := trace.listenerResponses[envoyListenerNonce{streamID, request.ResponseNonce}]; ok {
					if trace.listenerACKVersions == nil {
						trace.listenerACKVersions = map[string]bool{}
					}
					trace.listenerACKVersions[version] = true
					delete(trace.listenerResponses, envoyListenerNonce{streamID, request.ResponseNonce})
				}
			}
			if request.TypeUrl == resourcev3.SecretType {
				if issued, ok := trace.secretResponses[envoyListenerNonce{streamID, request.ResponseNonce}]; ok {
					if trace.secretACKNames == nil {
						trace.secretACKNames = map[string]map[string]bool{}
					}
					if trace.secretACKNames[issued.version] == nil {
						trace.secretACKNames[issued.version] = map[string]bool{}
					}
					for _, name := range issued.names {
						trace.secretACKNames[issued.version][name] = true
					}
					delete(trace.secretResponses, envoyListenerNonce{streamID, request.ResponseNonce})
				}
			}
		}
		return nil
	}, StreamDeltaResponseFunc: func(streamID int64, _ *discoveryv3.DeltaDiscoveryRequest, response *discoveryv3.DeltaDiscoveryResponse) {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		trace.responses++
		if response.TypeUrl == resourcev3.ListenerType && len(trace.listenerResponses) < 64 {
			if trace.listenerResponses == nil {
				trace.listenerResponses = map[envoyListenerNonce]string{}
			}
			trace.listenerResponses[envoyListenerNonce{streamID, response.Nonce}] = response.SystemVersionInfo
		}
		if response.TypeUrl == resourcev3.SecretType && len(trace.secretResponses) < 64 && len(response.Resources) > 0 && len(response.Resources) <= 64 {
			if trace.secretResponses == nil {
				trace.secretResponses = map[envoyListenerNonce]envoySecretResponse{}
			}
			names := []string{}
			seen := map[string]bool{}
			for _, resource := range response.Resources {
				name := resource.GetName()
				if name == "" || seen[name] {
					return
				}
				seen[name] = true
				names = append(names, name)
			}
			trace.secretResponses[envoyListenerNonce{streamID, response.Nonce}] = envoySecretResponse{response.SystemVersionInfo, names}
		}
	}}
}
func (trace *envoyControlTrace) report(t *testing.T) {
	t.Helper()
	trace.mu.Lock()
	defer trace.mu.Unlock()
	t.Logf("control-plane receipts: tls_hellos=%d h2_offered=%d verified=%d h2_negotiated=%d streams=%d requests=%d responses=%d acks=%d nacks=%d nack_kinds=%v", trace.hellos, trace.h2Offered, trace.verified, trace.h2Negotiated, trace.streams, trace.requests, trace.responses, trace.acks, trace.nacks, trace.nackKinds)
	if directory := os.Getenv("TETRAL_TEST_EDGE_DIAGNOSTICS_DIR"); directory != "" && len(trace.privateNacks) != 0 {
		if !filepath.IsAbs(directory) {
			t.Error("native private diagnostic directory must be absolute")
			return
		}
		//nolint:gosec // G703: absolute private artifact directory explicitly selected by the native test runner, not a request path.
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Error("create native private diagnostic directory")
			return
		}
		name := sha256.Sum256([]byte(t.Name()))
		body, err := json.Marshal(trace.privateNacks)
		if err != nil {
			t.Error("encode bounded private NACK diagnostics")
			return
		}
		//nolint:gosec // G703: runner-owned private directory and SHA-256 test-name basename with a fixed suffix.
		if err := os.WriteFile(filepath.Join(directory, hex.EncodeToString(name[:])+".xds-nacks.json"), body, 0600); err != nil {
			t.Error("retain bounded private NACK diagnostics")
		}
	}

}
func recordEnvoyStartupDiagnostics(t *testing.T, container *testinfra.DockerContainer, adminPort int) {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	state, stateErr := container.State(ctx)
	if stateErr != nil {
		t.Log("pinned Envoy finite process state unavailable")
	} else {
		t.Logf("pinned Envoy process state: running=%t exit_code=%d oom_killed=%t", state.Running, state.ExitCode, state.OOMKilled)
	}
	recordEnvoyControlAdmin(ctx, t, adminPort)
	body, err := container.Logs(ctx)
	if err != nil {
		t.Log("pinned Envoy startup diagnostic read failed")
		return
	}
	class := "no_classified_error"
	lower := strings.ToLower(body)
	for _, candidate := range []struct{ needle, class string }{{"'id' and 'cluster'", "missing_node_identity"}, {"error initializing configuration", "bootstrap_rejected"}, {"tls_error", "tls_error"}, {"grpc config stream", "xds_stream_error"}, {"unknown field", "unknown_field"}, {"not found", "missing_dependency"}} {
		if strings.Contains(lower, candidate.needle) {
			class = candidate.class
			break
		}
	}
	digest := sha256.Sum256([]byte(body))
	t.Logf("pinned Envoy startup diagnostics: class=%s bytes=%d sha256=%s", class, len(body), hex.EncodeToString(digest[:]))
	// Optional native diagnostics remain private, mode0600, and are never rendered.
	// The standard proof needs only the safe stage receipt above.
	if directory := os.Getenv("TETRAL_TEST_EDGE_DIAGNOSTICS_DIR"); directory != "" {
		if !filepath.IsAbs(directory) {
			t.Error("native private diagnostic directory must be absolute")
			return
		}
		//nolint:gosec // G703: absolute private artifact directory explicitly selected by the native test runner, not a request path.
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Error("create native private diagnostic directory")
			return
		}
		name := sha256.Sum256([]byte(t.Name()))
		//nolint:gosec // G703: runner-owned private directory and SHA-256 test-name basename with a fixed suffix.
		if err := os.WriteFile(filepath.Join(directory, hex.EncodeToString(name[:])+".envoy-startup.log"), []byte(body), 0600); err != nil {
			t.Error("retain native private startup diagnostic")
		}
	}
}

func recordEnvoyControlAdmin(ctx context.Context, t *testing.T, port int) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	read := func(path string) ([]byte, int) {
		request, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		if err != nil {
			return nil, 0
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, 0
		}
		defer func() { _ = response.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			return nil, response.StatusCode
		}
		return body, response.StatusCode
	}
	body, status := read("/stats?format=json&filter=cluster.xds_cluster")
	var stats struct {
		Stats []struct {
			Name  string      `json:"name"`
			Value json.Number `json:"value"`
		} `json:"stats"`
	}
	safe := map[string]string{}
	allowed := map[string]bool{"update_attempt": true, "update_success": true, "update_failure": true, "membership_total": true, "upstream_cx_total": true, "upstream_cx_connect_fail": true, "ssl.handshake": true, "ssl.connection_error": true, "ssl.fail_verify_cert_hash": true, "ssl.fail_verify_san": true, "ssl.fail_verify_error": true}
	if json.Unmarshal(body, &stats) == nil {
		for _, stat := range stats.Stats {
			if strings.HasPrefix(stat.Name, "cluster.xds_cluster.") {
				name := strings.TrimPrefix(stat.Name, "cluster.xds_cluster.")
				if allowed[name] {
					safe[name] = stat.Value.String()
				}
			}
		}
	}
	t.Logf("pinned Envoy admin control stats: http_status=%d finite_stats=%v", status, safe)
	body, status = read("/clusters?format=json")
	var clusters struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Address struct {
					Socket struct {
						Address string `json:"address"`
						Port    int    `json:"port_value"`
					} `json:"socket_address"`
				} `json:"address"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	endpoints := []string{}
	nonLoopback := 0
	if json.Unmarshal(body, &clusters) == nil {
		for _, cluster := range clusters.Clusters {
			if cluster.Name != "xds_cluster" {
				continue
			}
			for _, host := range cluster.Hosts {
				ip := net.ParseIP(host.Address.Socket.Address)
				if ip == nil || !ip.IsLoopback() {
					nonLoopback++
				} else {
					endpoints = append(endpoints, net.JoinHostPort(ip.String(), fmt.Sprint(host.Address.Socket.Port)))
				}
			}
		}
	}
	t.Logf("pinned Envoy resolved control endpoints: http_status=%d loopback=%v other_count=%d", status, endpoints, nonLoopback)
}

func TestEnvoyGatewayListenerGenerationAcknowledgment(t *testing.T) {
	trace := &envoyControlTrace{}
	callbacks := trace.callbacks()
	callbacks.StreamDeltaResponseFunc(1, nil, &discoveryv3.DeltaDiscoveryResponse{TypeUrl: resourcev3.ListenerType, Nonce: "listener-second", SystemVersionInfo: "official-egctl-2"})
	if err := callbacks.StreamDeltaRequestFunc(1, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ClusterType, ResponseNonce: "listener-second"}); err != nil {
		t.Fatal(err)
	}
	if trace.listenerACKVersions["official-egctl-2"] {
		t.Fatal("an unrelated resource ACK admitted the listener generation")
	}
	if err := callbacks.StreamDeltaRequestFunc(2, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ListenerType, ResponseNonce: "listener-second"}); err != nil {
		t.Fatal(err)
	}
	if trace.listenerACKVersions["official-egctl-2"] {
		t.Fatal("a foreign stream with the same nonce admitted the listener generation")
	}
	if err := callbacks.StreamDeltaRequestFunc(1, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ListenerType, ResponseNonce: "unissued"}); err != nil {
		t.Fatal(err)
	}
	if trace.listenerACKVersions["official-egctl-2"] {
		t.Fatal("an unissued nonce admitted the listener generation")
	}
	if err := callbacks.StreamDeltaRequestFunc(1, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ListenerType, ResponseNonce: "listener-second"}); err != nil {
		t.Fatal(err)
	}
	if !trace.listenerACKVersions["official-egctl-2"] || trace.listenerACKVersions["official-egctl-3"] {
		t.Fatal("the exact listener ACK did not bind its own generation")
	}
}

func TestEnvoyGatewayListenerRejectedAndClosedStreamCustody(t *testing.T) {
	trace := &envoyControlTrace{}
	callbacks := trace.callbacks()
	callbacks.StreamDeltaResponseFunc(1, nil, &discoveryv3.DeltaDiscoveryResponse{TypeUrl: resourcev3.ListenerType, Nonce: "rejected", SystemVersionInfo: "official-egctl-3"})
	if err := callbacks.StreamDeltaRequestFunc(1, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ListenerType, ResponseNonce: "rejected", ErrorDetail: &statuspb.Status{Code: 3}}); err != nil {
		t.Fatal(err)
	}
	if trace.listenerACKVersions["official-egctl-3"] || len(trace.listenerResponses) != 0 || trace.nacks != 1 {
		t.Fatal("NACK admitted a listener or retained stale nonce custody")
	}
	callbacks.StreamDeltaResponseFunc(1, nil, &discoveryv3.DeltaDiscoveryResponse{TypeUrl: resourcev3.ListenerType, Nonce: "closed", SystemVersionInfo: "official-egctl-4"})
	callbacks.StreamDeltaResponseFunc(2, nil, &discoveryv3.DeltaDiscoveryResponse{TypeUrl: resourcev3.ListenerType, Nonce: "closed", SystemVersionInfo: "official-egctl-5"})
	callbacks.DeltaStreamClosedFunc(1, nil)
	if len(trace.listenerResponses) != 1 {
		t.Fatal("closed stream did not release only its own nonce custody")
	}
	if err := callbacks.StreamDeltaRequestFunc(1, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ListenerType, ResponseNonce: "closed"}); err != nil {
		t.Fatal(err)
	}
	if trace.listenerACKVersions["official-egctl-4"] {
		t.Fatal("closed stream's nonce admitted a listener generation")
	}
	if err := callbacks.StreamDeltaRequestFunc(2, &discoveryv3.DeltaDiscoveryRequest{TypeUrl: resourcev3.ListenerType, ResponseNonce: "closed"}); err != nil {
		t.Fatal(err)
	}
	if !trace.listenerACKVersions["official-egctl-5"] {
		t.Fatal("closing one stream discarded another stream's nonce custody")
	}
}

func TestEnvoyGatewaySecretGenerationAcknowledgment(t *testing.T) {
	trace := &envoyControlTrace{}
	callbacks := trace.callbacks()
	response := &discoveryv3.DeltaDiscoveryResponse{TypeUrl: resourcev3.SecretType, Nonce: "issued", SystemVersionInfo: "secret-generation", Resources: []*discoveryv3.Resource{{Name: "first"}}}
	callbacks.StreamDeltaResponseFunc(7, nil, response)
	request := func(stream int64, kind, nonce string, rejected bool) {
		r := &discoveryv3.DeltaDiscoveryRequest{TypeUrl: kind, ResponseNonce: nonce}
		if rejected {
			r.ErrorDetail = &statuspb.Status{Code: 3}
		}
		if err := callbacks.StreamDeltaRequestFunc(stream, r); err != nil {
			t.Fatal(err)
		}
	}
	request(8, resourcev3.SecretType, "issued", false)
	request(7, resourcev3.ListenerType, "issued", false)
	request(7, resourcev3.SecretType, "unissued", false)
	if trace.secretACKNames["secret-generation"]["first"] {
		t.Fatal("foreign stream/type/nonce admitted Secret generation")
	}
	request(7, resourcev3.SecretType, "issued", false)
	if !trace.secretACKNames["secret-generation"]["first"] {
		t.Fatal("exact Secret generation ACK missing")
	}
	if trace.secretACKNames["secret-generation"]["second"] {
		t.Fatal("partial response admitted an absent Secret")
	}
	response.Nonce = "second-issued"
	response.Resources = []*discoveryv3.Resource{{Name: "second"}}
	callbacks.StreamDeltaResponseFunc(7, nil, response)
	request(7, resourcev3.SecretType, "second-issued", false)
	if !trace.secretACKNames["secret-generation"]["first"] || !trace.secretACKNames["secret-generation"]["second"] {
		t.Fatal("complete Secret response set was not acknowledged")
	}
	response.Resources = []*discoveryv3.Resource{{Name: "first"}}
	response.Nonce = "rejected"
	response.SystemVersionInfo = "nacked"
	callbacks.StreamDeltaResponseFunc(7, nil, response)
	request(7, resourcev3.SecretType, "rejected", true)
	request(7, resourcev3.SecretType, "rejected", false)
	if trace.secretACKNames["nacked"]["first"] {
		t.Fatal("NACK admitted Secret generation")
	}
	response.Nonce = "closed"
	response.SystemVersionInfo = "closed-stream"
	callbacks.StreamDeltaResponseFunc(7, nil, response)
	callbacks.DeltaStreamClosedFunc(7, nil)
	request(7, resourcev3.SecretType, "closed", false)
	if trace.secretACKNames["closed-stream"]["first"] {
		t.Fatal("closed stream admitted Secret generation")
	}
}
