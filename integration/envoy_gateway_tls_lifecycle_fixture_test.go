package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	authservice "github.com/tetral-ai/tetral/services/auth"
)

// Optional lifecycle controls belong only to the edge TLS composition. Existing
// traffic/authority fixtures retain their original listener ownership.
type edgeTLSLifecycle struct {
	context        context.Context
	fixture        *envoyGatewayTranslation
	profile        string
	http           map[string]*edgeTLSHTTPBackend
	retired        []*edgeTLSHTTPBackend
	adapter        *authservice.ExternalAuthorization
	stopCheck      func()
	checkDirectory string
}

type edgeTLSHTTPBackend struct {
	context                           context.Context
	role, address, directory, profile string
	handler                           http.Handler
	server                            *http.Server
	owner                             *transportsecurity.Owner
	serveDone                         chan error
	shutdownDone                      chan error
	shutdownCancel                    context.CancelFunc
	shutdownJoined                    bool
	joined                            bool
	mu                                sync.Mutex
	requests                          int64
	peerSerial                        string
}

func (l *edgeTLSLifecycle) bind(ctx context.Context, fixture *envoyGatewayTranslation, profile string) {
	l.context, l.fixture, l.profile = ctx, fixture, profile
	l.http = map[string]*edgeTLSHTTPBackend{}
	fixture.ProcessDrainArgs = append([]string(nil), fixture.Snapshot.ProxyDrainArgs...)
}

func (edge *translatedPublicEdge) startHTTP(ctx context.Context, t *testing.T, fixture *envoyGatewayTranslation, profile, role, address string, handler http.Handler) {
	t.Helper()
	if edge.lifecycle == nil {
		startEdgeHTTPBackend(ctx, t, fixture, profile, role, address, handler)
		return
	}
	l := edge.lifecycle
	directory := filepath.Join(fixture.Directory, role+"-http")
	if profile == "hardened" {
		projectEdgeBackendCredentials(t, directory, fixture, role)
	}
	l.http[role] = openEdgeTLSHTTPBackend(ctx, t, profile, role, address, directory, handler)
}

func openEdgeTLSHTTPBackend(ctx context.Context, t *testing.T, profile, role, address, directory string, handler http.Handler) *edgeTLSHTTPBackend {
	t.Helper()
	b := &edgeTLSHTTPBackend{context: ctx, role: role, address: address, directory: directory, profile: profile, handler: handler, serveDone: make(chan error, 1)}
	listener := transporttest.Must(net.Listen("tcp", address))
	t.Cleanup(func() { _ = listener.Close() })
	b.server = &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		b.requests++
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			b.peerSerial = r.TLS.PeerCertificates[0].SerialNumber.String()
		}
		b.mu.Unlock()
		handler.ServeHTTP(w, r)
	})}
	if profile == "hardened" {
		config := transportsecurity.HTTPConfig{Mode: "native-mtls", CAPath: filepath.Join(directory, "ca.crt"), CertPath: filepath.Join(directory, "tls.crt"), KeyPath: filepath.Join(directory, "tls.key"), EdgeClientURI: "spiffe://cluster.local/ns/envoy-gateway-system/sa/tetral-public-edge"}
		var err error
		b.owner, b.server.TLSConfig, err = config.Open(ctx)
		if err != nil {
			t.Fatal("open lifecycle backend mounted native credentials")
		}
	}
	go func() {
		if b.owner == nil {
			b.serveDone <- b.server.Serve(listener)
		} else {
			b.serveDone <- b.server.ServeTLS(listener, "", "")
		}
	}()
	t.Cleanup(func() { b.close(t) })
	return b
}

func (b *edgeTLSHTTPBackend) receipt() (int64, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requests, b.peerSerial
}

func (b *edgeTLSHTTPBackend) beginDrain() {
	if b.shutdownDone != nil {
		return
	}
	ctx, cancel := context.WithTimeout(b.context, 45*time.Second)
	b.shutdownCancel = cancel
	b.shutdownDone = make(chan error, 1)
	go func() { b.shutdownDone <- b.server.Shutdown(ctx) }()
}

func (b *edgeTLSHTTPBackend) join(t *testing.T) {
	t.Helper()
	if b.joined {
		return
	}
	b.beginDrain()
	select {
	case err := <-b.shutdownDone:
		b.shutdownJoined = true
		if err != nil {
			t.Fatal("lifecycle backend graceful drain did not complete")
		}
	case <-b.context.Done():
		t.Fatal("lifecycle backend drain exceeded fixture owner")
	}
	b.shutdownCancel()
	b.finish(t)
}

func (b *edgeTLSHTTPBackend) finish(t *testing.T) {
	t.Helper()
	if b.joined {
		return
	}
	select {
	case err := <-b.serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error("lifecycle HTTP listener did not join")
		}
	case <-time.After(5 * time.Second):
		t.Error("lifecycle HTTP listener join exceeded cleanup bound")
	}
	if b.owner != nil && b.owner.Close() != nil {
		t.Error("lifecycle HTTP credentials did not join")
	}
	b.joined = true
}

func (b *edgeTLSHTTPBackend) close(t *testing.T) {
	t.Helper()
	if b.joined {
		return
	}
	_ = b.server.Close()
	if b.shutdownDone != nil && !b.shutdownJoined {
		select {
		case <-b.shutdownDone:
			b.shutdownJoined = true
		case <-time.After(5 * time.Second):
			t.Error("lifecycle HTTP shutdown worker did not join")
		}
	}
	if b.shutdownCancel != nil {
		b.shutdownCancel()
	}
	b.finish(t)
}

func (l *edgeTLSLifecycle) projectHTTP(t *testing.T, role, name string, trust []byte, leaf transporttest.Leaf) {
	t.Helper()
	b := l.http[role]
	if err := transporttest.Project(b.directory, name, map[string][]byte{"ca.crt": trust, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal("project complete backend TLS generation")
	}
	// Apply through the same owning loader; no test-only TLS configuration edit.
	if !b.joined && b.owner.Reload() != nil {
		t.Fatal("activate backend TLS generation")
	}
}

func (l *edgeTLSLifecycle) restartHTTP(t *testing.T, role string) {
	t.Helper()
	old := l.http[role]
	old.join(t)
	l.http[role] = openEdgeTLSHTTPBackend(l.context, t, l.profile, role, old.address, old.directory, old.handler)
}

func (l *edgeTLSLifecycle) handOverHTTP(t *testing.T, role string) {
	t.Helper()
	old := l.http[role]
	// net/http Shutdown closes the old listener and sends HTTP/2 GOAWAY while
	// admitted handlers keep their contexts. Rebind only after that owner closes.
	old.beginDrain()
	deadline := time.Now().Add(2 * time.Second)
	for {
		probe, err := net.Listen("tcp", old.address)
		if err == nil {
			_ = probe.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old backend did not withdraw its listener")
		}
		time.Sleep(5 * time.Millisecond)
	}
	l.retired = append(l.retired, old)
	l.http[role] = openEdgeTLSHTTPBackend(l.context, t, l.profile, role, old.address, old.directory, old.handler)
}

func (edge *translatedPublicEdge) startCheck(ctx context.Context, t *testing.T, fixture *envoyGatewayTranslation, profile string, adapter *authservice.ExternalAuthorization) {
	t.Helper()
	if edge.lifecycle == nil {
		startEnvoyGatewayAuthCheck(ctx, t, fixture, profile, adapter)
		return
	}
	l := edge.lifecycle
	l.adapter = adapter
	l.checkDirectory = filepath.Join(fixture.Directory, "auth-check")
	if profile == "hardened" {
		projectEdgeBackendCredentials(t, l.checkDirectory, fixture, "auth")
	}
	l.restartCheck(t)
}

func (l *edgeTLSLifecycle) restartCheck(t *testing.T) {
	t.Helper()
	if l.stopCheck != nil {
		l.stopCheck()
	}
	cfg := authservice.Config{GRPCAddress: "127.0.0.2:9095", GRPCTransport: "plaintext"}
	if l.profile == "hardened" {
		cfg.GRPCTransport = "native-mtls"
		cfg.GRPCTLSCAPath = filepath.Join(l.checkDirectory, "ca.crt")
		cfg.GRPCTLSCertPath = filepath.Join(l.checkDirectory, "tls.crt")
		cfg.GRPCTLSKeyPath = filepath.Join(l.checkDirectory, "tls.key")
		cfg.GRPCTLSEdgeClientURI = "spiffe://cluster.local/ns/envoy-gateway-system/sa/tetral-public-edge"
	}
	server := transporttest.Must(authservice.OpenExternalAuthorizationServer(l.context, cfg, l.adapter, nil))
	ctx, cancel := context.WithCancel(l.context)
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, nil) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error("lifecycle Check did not join")
			}
		case <-time.After(15 * time.Second):
			t.Error("lifecycle Check exceeded join bound")
		}
		if server.Close() != nil {
			t.Error("lifecycle Check credentials did not close")
		}
	}
	l.stopCheck = stop
	t.Cleanup(stop)
}

func (l *edgeTLSLifecycle) projectCheck(t *testing.T, name string, trust []byte, leaf transporttest.Leaf) {
	t.Helper()
	if err := transporttest.Project(l.checkDirectory, name, map[string][]byte{"ca.crt": trust, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal("project complete Check generation")
	}
}

func edgeTLSAdmin(ctx context.Context, t *testing.T, port int, method, path string) []byte {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	request := transporttest.Must(http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://127.0.0.1:%d/%s", port, path), nil))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("actual edge admin request failed")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		t.Fatal("actual edge admin operation was rejected")
	}
	return transporttest.Must(io.ReadAll(io.LimitReader(response.Body, 1<<20)))
}

type edgeTLSDrainStat struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func (control *translatedEnvoyControl) drainConnections(ctx context.Context, t *testing.T, stage string) (int, map[string]int) {
	t.Helper()
	body := edgeTLSAdmin(ctx, t, control.sockets["admin"], "GET", "stats?filter="+url.QueryEscape(`^(listener\..*\.downstream_cx_active|udp\..*\.downstream_sess_active)$`)+"&format=json")
	var counters struct {
		Stats []edgeTLSDrainStat `json:"stats"`
	}
	if json.Unmarshal(body, &counters) != nil || len(counters.Stats) == 0 || len(counters.Stats) > 128 {
		t.Fatal("edge drain connection census missing or unbounded")
	}
	// Envoy uses each listener's address for its stat scope when stat_prefix is
	// absent, then sanitizes ':' to '_'. The official fixture substitutes ports;
	// translate only the two upstream readiness/stats exclusions through that
	// already-verified confinement map, retaining every application connection.
	expected := map[string]string{
		"listener.admin.downstream_cx_active":             "admin",
		"listener.admin.main_thread.downstream_cx_active": "admin",
	}
	applications := map[string]bool{}
	for name, port := range control.sockets {
		metric := fmt.Sprintf("listener.127.0.0.1_%d.downstream_cx_active", port)
		group := "application"
		switch name {
		case "admin":
			group = "admin"
		case "envoy-gateway-proxy-ready-0.0.0.0-19003":
			group = "readiness"
		case "envoy-gateway-proxy-stats-0.0.0.0-19001":
			group = "stats"
		default:
			applications[metric] = false
		}
		if _, exists := expected[metric]; exists {
			t.Fatal("edge drain socket scopes overlap")
		}
		expected[metric] = group
	}
	worker := regexp.MustCompile(`\.worker_[0-9]+(\.downstream_cx_active)$`)
	values := map[string]int{}
	total := 0
	for _, stat := range counters.Stats {
		if len(stat.Name) > 256 || stat.Value < 0 {
			t.Fatal("invalid edge drain counter")
		}
		if _, duplicate := values[stat.Name]; duplicate {
			t.Fatal("duplicate edge drain counter")
		}
		values[stat.Name] = stat.Value
		if _, known := expected[worker.ReplaceAllString(stat.Name, "$1")]; known && worker.MatchString(stat.Name) {
			continue
		}
		group := expected[stat.Name]
		if group == "application" {
			applications[stat.Name] = true
		}
		// An unknown scope never gains a monitoring exemption.
		if group == "application" || group == "" {
			total += stat.Value
		}
	}
	if stage != "" {
		t.Logf("envoy_gateway_tls_drain stage=%s application_active=%d exact_counters=%v", stage, total, values)
	}
	if len(applications) == 0 {
		t.Fatal("edge drain has no declared application scope")
	}
	for _, present := range applications {
		if !present {
			t.Fatal("edge drain omitted an application connection counter")
		}
	}
	return total, values
}

func (control *translatedEnvoyControl) drain(t *testing.T, closeStreams func()) {
	t.Helper()
	// The pinned Shutdown defaults are health delay0, minimum10s, timeout60s,
	// threshold0 (internal/cmd/envoy.go44–53). It fails health checks, then counts
	// listener connections excluding admin/readiness/stats/worker observations.
	started := time.Now()
	active, _ := control.drainConnections(control.context, t, "held-stream-before-drain")
	if active == 0 {
		t.Fatal("held SDK stream has no actual application connection")
	}
	edgeTLSAdmin(control.context, t, control.sockets["admin"], "POST", "healthcheck/fail")
	readyTransport := &http.Transport{}
	defer readyTransport.CloseIdleConnections()
	readyClient := &http.Client{Timeout: time.Second, Transport: readyTransport}
	request := transporttest.Must(http.NewRequestWithContext(control.context, "GET", fmt.Sprintf("http://127.0.0.1:%d/ready", control.sockets["envoy-gateway-proxy-ready-0.0.0.0-19003"]), nil))
	response, err := readyClient.Do(request)
	if err != nil {
		t.Fatal("readiness withdrawal probe failed")
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 4096)); err != nil {
		_ = response.Body.Close()
		t.Fatal("readiness withdrawal body did not complete")
	}
	_ = response.Body.Close()
	readyTransport.CloseIdleConnections()
	if response.StatusCode != 503 {
		t.Fatal("edge drain did not withdraw readiness")
	}
	closeStreams()
	deadline, stop := context.WithTimeout(control.context, 60*time.Second)
	defer stop()
	firstCensus, nearDeadlineLogged := true, false
	for {
		stage := ""
		if firstCensus {
			stage = "after-owned-stream-close"
			firstCensus = false
		}
		total, _ := control.drainConnections(deadline, t, stage)
		if time.Since(started) >= 10*time.Second && total == 0 {
			confirmed, _ := control.drainConnections(deadline, t, "zero-confirmation")
			if confirmed == 0 {
				break
			}
		}
		if remaining, ok := deadline.Deadline(); ok && time.Until(remaining) < 500*time.Millisecond && !nearDeadlineLogged {
			nearDeadlineLogged = true
			control.drainConnections(deadline, t, "near-deadline")
		}
		select {
		case <-deadline.Done():
			t.Fatal("edge drain retained downstream connections at selected timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
	edgeTLSAdmin(control.context, t, control.sockets["admin"], "POST", "quitquitquit")
	waitCtx, stopWait := context.WithTimeout(control.context, 10*time.Second)
	defer stopWait()
	if code, err := control.container.Wait(waitCtx); err != nil || code != 0 {
		t.Fatal("drained Envoy process did not join normally")
	}
	control.stopADS()
	cleanup, stopCleanup := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCleanup()
	if control.resources.Close(cleanup) != nil {
		t.Fatal("drained Envoy Docker owner did not join")
	}
	t.Log("envoy_gateway_tls_drain readiness=503 sdk_joined=true downstream_connections=0 minimum_seconds=10 process_exit=0 passed=true")
}
