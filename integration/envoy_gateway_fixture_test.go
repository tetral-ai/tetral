package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	adminv3 "github.com/envoyproxy/go-control-plane/envoy/admin/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/testinfra"

	// The CLI serializes typed Any messages. Register every extension used by the
	// production edge; unknown extension types fail decoding rather than disappear.
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_authz/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/health_check/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/listener/tls_inspector/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/http/early_header_mutation/header_mutation/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/load_balancing_policies/least_request/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
)

type translatedEnvoySnapshot struct {
	Bootstrap      map[string]any
	Resources      map[resourcev3.Type][]types.Resource
	OriginalJSON   []byte
	FixtureYAML    []byte
	ProxyDrainArgs []string
}

// Decode the actual matching CLI's resources. The original listener/route/
// cluster/endpoint messages become the served snapshot without policy edits.
func decodeEnvoyGatewaySnapshot(body []byte) (*translatedEnvoySnapshot, error) {
	var output struct {
		XDS map[string]struct {
			Configs []json.RawMessage `json:"configs"`
		} `json:"xds"`
	}
	if err := json.Unmarshal(body, &output); err != nil {
		return nil, errors.New("decode matching CLI output")
	}
	if len(output.XDS) != 1 {
		return nil, errors.New("translation must contain exactly one Gateway")
	}
	result := &translatedEnvoySnapshot{Resources: map[resourcev3.Type][]types.Resource{}, OriginalJSON: body}
	seen := map[string]bool{}
	add := func(kind resourcev3.Type, value *anypb.Any) error {
		decoded, err := anypb.UnmarshalNew(value, proto.UnmarshalOptions{})
		if err != nil {
			return errors.New("decode original policy resource")
		}
		key := kind + "/" + cachev3.GetResourceName(decoded)
		if seen[key] {
			return errors.New("duplicate original policy resource")
		}
		seen[key] = true
		result.Resources[kind] = append(result.Resources[kind], decoded)
		return nil
	}
	for _, dump := range output.XDS {
		for _, raw := range dump.Configs {
			var kind struct {
				Type string `json:"@type"`
			}
			if err := json.Unmarshal(raw, &kind); err != nil {
				return nil, errors.New("decode original resource type")
			}
			if kind.Type == "type.googleapis.com/envoy.admin.v3.BootstrapConfigDump" {
				var config struct {
					Bootstrap map[string]any `json:"bootstrap"`
				}
				if err := json.Unmarshal(raw, &config); err != nil || result.Bootstrap != nil {
					return nil, errors.New("missing or duplicate original bootstrap")
				}
				result.Bootstrap = config.Bootstrap
				continue
			}
			wrapped := &anypb.Any{}
			if err := protojson.Unmarshal(raw, wrapped); err != nil {
				return nil, errors.New("decode original policy dump")
			}
			decoded, err := anypb.UnmarshalNew(wrapped, proto.UnmarshalOptions{})
			if err != nil {
				return nil, errors.New("decode original policy dump message")
			}
			switch config := decoded.(type) {
			case *adminv3.EndpointsConfigDump:
				for _, value := range config.DynamicEndpointConfigs {
					if err := add(resourcev3.EndpointType, value.EndpointConfig); err != nil {
						return nil, err
					}
				}
			case *adminv3.ClustersConfigDump:
				for _, value := range config.DynamicActiveClusters {
					if err := add(resourcev3.ClusterType, value.Cluster); err != nil {
						return nil, err
					}
				}
			case *adminv3.ListenersConfigDump:
				for _, value := range config.DynamicListeners {
					if err := add(resourcev3.ListenerType, value.GetActiveState().GetListener()); err != nil {
						return nil, err
					}
				}
			case *adminv3.RoutesConfigDump:
				for _, value := range config.DynamicRouteConfigs {
					if err := add(resourcev3.RouteType, value.RouteConfig); err != nil {
						return nil, err
					}
				}
			default:
				return nil, errors.New("unexpected original policy dump")
			}
		}
	}
	if result.Bootstrap == nil || len(result.Resources) != 4 {
		return nil, errors.New("incomplete original translated resource graph")
	}
	return result, nil
}

func completeEnvoyGatewaySecrets(ctx context.Context, t *testing.T, snapshot *translatedEnvoySnapshot) {
	t.Helper()
	executable := os.Getenv(testinfra.EnvTestEGCTLSecretTranslator)
	if executable == "" {
		t.Fatal("native matching upstream SecretType helper prerequisite is absent")
	}
	command := exec.CommandContext(ctx, executable) //nolint:gosec // Native prerequisite checksum/build metadata verified by testinfra.
	digest := sha256.Sum256(snapshot.FixtureYAML)
	fixtureSHA := hex.EncodeToString(digest[:])
	input, err := json.Marshal(struct {
		Output        json.RawMessage `json:"output"`
		FixtureYAML   []byte          `json:"fixtureYAML"`
		FixtureSHA256 string          `json:"fixtureSHA256"`
	}{snapshot.OriginalJSON, snapshot.FixtureYAML, fixtureSHA})
	if err != nil {
		t.Fatal("encode original fixture binding")
	}
	command.Stdin = bytes.NewReader(input)
	var failure bytes.Buffer
	command.Stderr = &failure
	body, err := command.Output()
	if err != nil {
		t.Fatalf("upstream SecretType parity gate failed: %s", failure.String())
	}
	var output struct {
		Common                map[string]int    `json:"commonResourceCounts"`
		Secrets               []json.RawMessage `json:"secrets"`
		FixtureSHA256         string            `json:"fixtureSHA256"`
		UnusedControlPlaneTLS bool              `json:"unusedControlPlaneTLS"`
		ProxyDrainArgs        []string          `json:"proxyDrainArgs"`
	}
	if err := json.Unmarshal(body, &output); err != nil {
		t.Fatal("decode upstream SecretType parity output")
	}
	if output.FixtureSHA256 != fixtureSHA {
		t.Fatal("upstream helper fixture identity differs")
	}
	snapshot.ProxyDrainArgs = append([]string(nil), output.ProxyDrainArgs...)
	if output.UnusedControlPlaneTLS {
		t.Log("upstream CLI unused controller TLS lookup: no global rate-limit or Wasm consumers; exact missing-secret error classified")
	}
	if len(output.Common) != 4 {
		t.Fatal("upstream parity omitted a common resource type")
	}
	for kind, resources := range snapshot.Resources {
		if output.Common[kind] != len(resources) {
			t.Fatal("upstream parity common-resource count differs")
		}
	}
	names := map[string]bool{}
	for _, raw := range output.Secrets {
		secret := &tlsv3.Secret{}
		if err := protojson.Unmarshal(raw, secret); err != nil || secret.Name == "" || names[secret.Name] {
			t.Fatal("invalid or duplicate upstream SecretType output")
		}
		names[secret.Name] = true
		snapshot.Resources[resourcev3.SecretType] = append(snapshot.Resources[resourcev3.SecretType], secret)
	}
	if len(names) == 0 {
		t.Fatal("TLS policy translation omitted SecretType")
	}
	// Neither stdout nor the payload is logged: it contains fixture private keys.
	t.Logf("upstream policy parity: clusters=%d endpoints=%d listeners=%d routes=%d secrets=%d", len(snapshot.Resources[resourcev3.ClusterType]), len(snapshot.Resources[resourcev3.EndpointType]), len(snapshot.Resources[resourcev3.ListenerType]), len(snapshot.Resources[resourcev3.RouteType]), len(names))
}

// Host-network execution is admitted only after all actual generated listener
// sockets, including the translator's readiness listener, are explicit loopback.
func requireConfinedEnvoyListeners(t *testing.T, snapshot *translatedEnvoySnapshot) map[string]int {
	t.Helper()
	result := map[string]int{}
	ports := map[int]bool{}
	check := func(name string, socket map[string]any) {
		address, _ := socket["address"].(string)
		value, _ := socket["portValue"].(float64)
		port := int(value)
		ip := net.ParseIP(address)
		if ip == nil || !ip.IsLoopback() || value != float64(port) || port <= 1024 || port > 65535 || ports[port] {
			t.Fatal("actual emitted fixture socket is unconfined, privileged, duplicate, or invalid")
		}
		ports[port] = true
		result[name] = port
		t.Logf("actual emitted socket: name=%s address=%s port=%d", name, ip.String(), port)
	}
	for _, resource := range snapshot.Resources[resourcev3.ListenerType] {
		listener, ok := resource.(*listenerv3.Listener)
		if !ok || len(listener.AdditionalAddresses) != 0 {
			t.Fatal("unexpected or additional fixture listener")
		}
		socket := listener.GetAddress().GetSocketAddress()
		if socket == nil {
			t.Fatal("fixture listener requires an explicit socket")
		}
		check(listener.Name, map[string]any{"address": socket.GetAddress(), "portValue": float64(socket.GetPortValue())})
	}
	if len(result) != 3 {
		t.Fatal("actual translated fixture must contain three dynamic listeners")
	}
	static, ok := snapshot.Bootstrap["staticResources"].(map[string]any)
	if !ok {
		t.Fatal("official Bootstrap omitted static resources")
	}
	listeners, ok := static["listeners"].([]any)
	if !ok || len(listeners) != 1 {
		t.Fatal("official Bootstrap must contain exactly one stats listener")
	}
	for _, raw := range listeners {
		listener, ok := raw.(map[string]any)
		if !ok || listener["additionalAddresses"] != nil {
			t.Fatal("invalid or additional static socket")
		}
		address, ok := listener["address"].(map[string]any)
		if !ok {
			t.Fatal("static socket omitted")
		}
		socket, ok := address["socketAddress"].(map[string]any)
		if !ok {
			t.Fatal("static socket omitted")
		}
		name, _ := listener["name"].(string)
		check(name, socket)
	}
	admin, ok := snapshot.Bootstrap["admin"].(map[string]any)
	if !ok {
		t.Fatal("official Bootstrap admin omitted")
	}
	address, ok := admin["address"].(map[string]any)
	if !ok {
		t.Fatal("official Bootstrap admin address omitted")
	}
	socket, ok := address["socketAddress"].(map[string]any)
	if !ok {
		t.Fatal("official Bootstrap admin socket omitted")
	}
	check("admin", socket)
	return result
}

func serveTranslatedEnvoy(ownerCtx context.Context, t *testing.T, snapshot *translatedEnvoySnapshot, xdsListeners []net.Listener, xdsTLS *tls.Config, directory, nodeID string, drainArgs ...string) *translatedEnvoyControl {
	t.Helper()
	sockets := requireConfinedEnvoyListeners(t, snapshot)
	deadline, ok := ownerCtx.Deadline()
	if !ok {
		t.Fatal("actual Envoy fixture requires a bounded test deadline")
	}
	ctx, cancel := context.WithDeadline(ownerCtx, deadline)
	t.Cleanup(cancel)
	cache := cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil)
	version, err := cachev3.NewSnapshot("production-egctl-policy", snapshot.Resources)
	if err != nil {
		t.Fatal("build translated policy snapshot")
	}
	if err := version.Consistent(); err != nil {
		t.Fatal("actual translated policy references are inconsistent")
	}
	if err := cache.SetSnapshot(ctx, nodeID, version); err != nil {
		t.Fatal("publish actual translated policy snapshot")
	}
	trace := &envoyControlTrace{}
	t.Cleanup(func() { trace.report(t) })
	ads := serverv3.NewServer(ctx, cache, trace.callbacks())
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(trace.observeTLS(xdsTLS))))
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(server, ads)
	joined := make(chan error, len(xdsListeners))
	for _, listener := range xdsListeners {
		go func() { joined <- server.Serve(listener) }()
	}
	adsStopped := false
	stopADS := func() {
		if adsStopped {
			return
		}
		adsStopped = true
		server.Stop()
		for range xdsListeners {
			if err := <-joined; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Error("actual translated xDS server did not join")
			}
		}
		cancel()
	}
	t.Cleanup(stopADS)
	body, err := json.Marshal(snapshot.Bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := filepath.Join(directory, "bootstrap.json")
	if err := os.WriteFile(bootstrap, body, 0600); err != nil {
		t.Fatal(err)
	}
	resources, err := testinfra.NewDockerResources(ctx, "public-edge")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := resources.Close(cleanup); err != nil {
			t.Error("actual pinned edge Envoy cleanup failed")
		}
	})
	image, err := testinfra.PinnedEdgeEnvoyImage(transporttest.RepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	arguments := []string{"--service-cluster", "tetral-local-public-edge", "--service-node", nodeID, "-c", "/fixture/bootstrap.json", "--concurrency", "1", "--log-level", "error"}
	if len(drainArgs) != 0 {
		if len(drainArgs) != 4 || drainArgs[0] != "--drain-strategy" || drainArgs[1] != "immediate" || drainArgs[2] != "--drain-time-s" || drainArgs[3] != "60" {
			t.Fatal("selected upstream process drain arguments differ")
		}
		arguments = append(arguments, drainArgs...)
	}
	container, err := resources.Run(ctx, testinfra.ContainerSpec{HostNetwork: true, ExtraHosts: map[string]string{"envoy-gateway": "127.0.0.1"}, Image: image, User: "0", Entrypoint: "/usr/local/bin/envoy", Mounts: []testinfra.DockerMount{{Source: directory, Target: "/fixture", ReadOnly: true}, {Source: filepath.Join(directory, "sds"), Target: "/sds", ReadOnly: true}}, Command: arguments})
	if err != nil {
		t.Fatal("actual pinned edge Envoy startup failed")
	}
	t.Cleanup(func() {
		if t.Failed() {
			recordEnvoyStartupDiagnostics(t, container, sockets["admin"])
		}
	})
	t.Logf("actual pinned edge Envoy image=%s runtime=%s", image, container.ImageID)
	return &translatedEnvoyControl{context: ctx, cache: cache, trace: trace, nodeID: nodeID, generation: 1, container: container, stopADS: stopADS, sockets: sockets, resources: resources, activeSnapshot: snapshot}
}

// Updates publish another complete official translation; policy resources are
// never edited by the serving harness. The same original Bootstrap stays live.
type translatedEnvoyControl struct {
	context        context.Context
	cache          cachev3.SnapshotCache
	trace          *envoyControlTrace
	nodeID         string
	generation     int
	container      *testinfra.DockerContainer
	stopADS        func()
	sockets        map[string]int
	resources      *testinfra.DockerResources
	activeSnapshot *translatedEnvoySnapshot
}

func (control *translatedEnvoyControl) publish(t *testing.T, snapshot *translatedEnvoySnapshot) {
	t.Helper()
	control.publishType(t, snapshot, resourcev3.ListenerType)
}

func (control *translatedEnvoyControl) publishSecrets(t *testing.T, snapshot *translatedEnvoySnapshot) {
	t.Helper()
	control.publishType(t, snapshot, resourcev3.SecretType)
}

func (control *translatedEnvoyControl) publishType(t *testing.T, snapshot *translatedEnvoySnapshot, requiredType resourcev3.Type) {
	t.Helper()
	requireConfinedEnvoyListeners(t, snapshot)
	changedSecrets := map[string]bool{}
	if requiredType == resourcev3.SecretType {
		prior := map[string]types.Resource{}
		for _, resource := range control.activeSnapshot.Resources[resourcev3.SecretType] {
			prior[cachev3.GetResourceName(resource)] = resource
		}
		for _, resource := range snapshot.Resources[resourcev3.SecretType] {
			name := cachev3.GetResourceName(resource)
			if !proto.Equal(prior[name], resource) {
				changedSecrets[name] = true
			}
		}
		if len(changedSecrets) == 0 {
			t.Fatal("Secret generation changes no declared resource")
		}
	}
	control.generation++
	versionID := fmt.Sprintf("official-egctl-%d", control.generation)
	version, err := cachev3.NewSnapshot(versionID, snapshot.Resources)
	if err != nil {
		t.Fatal("build updated official translation snapshot")
	}
	if err := version.Consistent(); err != nil {
		t.Fatal("updated official translation is inconsistent")
	}
	control.trace.mu.Lock()
	nacks := control.trace.nacks
	control.trace.mu.Unlock()
	if err := control.cache.SetSnapshot(control.context, control.nodeID, version); err != nil {
		t.Fatal("publish updated official translation")
	}
	ctx, stop := context.WithTimeout(control.context, 5*time.Second)
	defer stop()
	for {
		control.trace.mu.Lock()
		acknowledged := control.trace.listenerACKVersions[versionID]
		if requiredType == resourcev3.SecretType {
			acknowledged = true
			for name := range changedSecrets {
				if !control.trace.secretACKNames[versionID][name] {
					acknowledged = false
				}
			}
		}
		rejected := control.trace.nacks > nacks
		control.trace.mu.Unlock()
		if rejected {
			t.Fatal("Envoy rejected updated official translation")
		}
		if acknowledged {
			control.activeSnapshot = snapshot
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("Envoy did not acknowledge the updated official resource generation")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
