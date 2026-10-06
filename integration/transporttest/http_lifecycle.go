package transporttest

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

// HTTPServiceRun supplies the owning service's real public/metrics composition.
// The observer below supplies ephemeral listeners, then executes workload.Run.
type HTTPServiceRun func(context.Context, transportsecurity.HTTPConfig, *workload.Readiness, func(context.Context, workload.Config) error, http.Handler, http.Handler) error

// HTTPServiceLifecycle exercises service wiring and credential lifetime. It is
// separate from translated-proxy, business authorization and deployed issuance.
func HTTPServiceLifecycle(t *testing.T, run HTTPServiceRun) {
	t.Helper()
	const serverDNS = "backend.edge.test"
	const edgeURI = "spiffe://edge.test/ns/edge/sa/proxy"
	ca := Must(NewAuthority("http-service-old"))
	leaf := Must(ca.ValidLeaf(serverDNS, "spiffe://edge.test/ns/system/sa/backend"))
	caller := Must(ca.ValidLeaf("edge.test", edgeURI))
	config := func(dir string) transportsecurity.HTTPConfig {
		return transportsecurity.HTTPConfig{Mode: "native-mtls", CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key"), EdgeClientURI: edgeURI}
	}
	for _, malformed := range []bool{false, true} {
		name := "missing-initial-material"
		if malformed {
			name = "malformed-initial-material"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if malformed {
				if err := Project(dir, "bad", map[string][]byte{"ca.crt": ca.PEM, "tls.crt": []byte("invalid certificate"), "tls.key": leaf.Key}); err != nil {
					t.Fatal(err)
				}
			}
			var listeners atomic.Int64
			err := run(t.Context(), config(dir), workload.NewReadiness(), func(context.Context, workload.Config) error { listeners.Add(1); return nil }, http.NotFoundHandler(), http.NotFoundHandler())
			if err == nil || listeners.Load() != 0 {
				t.Fatal("invalid initial native material reached listener startup")
			}
		})
	}
	t.Run("renewal-overlap-and-owned-drain", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		dir := t.TempDir()
		project := func(name string, trust []byte, server Leaf) {
			t.Helper()
			if err := Project(dir, name, map[string][]byte{"ca.crt": trust, "tls.crt": server.Certificate, "tls.key": server.Key}); err != nil {
				t.Fatal(err)
			}
		}
		project("initial", ca.PEM, leaf)
		readiness := workload.NewReadiness()
		readiness.MarkReady()
		type observed struct {
			address string
			config  workload.Config
		}
		started := make(chan observed, 2)
		observe := func(ctx context.Context, cfg workload.Config) error {
			listener, err := net.Listen("tcp", cfg.ListenAddress)
			if err != nil {
				return err
			}
			cfg.Listener = listener
			started <- observed{listener.Addr().String(), cfg}
			return workload.Run(ctx, cfg)
		}
		var admitted atomic.Int64
		release := make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		public := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			admitted.Add(1)
			if r.URL.Path != "/stream" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-release:
				_, _ = io.WriteString(w, "data: last\n\n")
			case <-r.Context().Done():
			}
		})
		metrics := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/metrics" {
				http.NotFound(w, r)
				return
			}
			_, _ = io.WriteString(w, "fixture_requests 1\n")
		})
		done := make(chan error, 1)
		go func() { done <- run(ctx, config(dir), readiness, observe, public, metrics) }()
		joined := false
		t.Cleanup(func() {
			unblock()
			cancel()
			if !joined {
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(12 * time.Second):
					t.Error("service pair did not join cleanup")
				}
			}
		})
		var business, metric observed
		for range 2 {
			select {
			case item := <-started:
				if strings.HasSuffix(item.config.ListenConfigKey, "_METRICS_ADDR") {
					metric = item
				} else {
					business = item
				}
			case err := <-done:
				joined = true
				t.Fatalf("service exited before listeners: %v", err)
			case <-ctx.Done():
				t.Fatal("service listener startup exceeded bound")
			}
		}
		if business.config.TLSConfig == nil || metric.config.TLSConfig != nil {
			t.Fatal("service did not separate native business and internal metrics transports")
		}
		makeClient := func(trust []byte, peer *Leaf, dns string) *http.Client {
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(trust) {
				t.Fatal("invalid client fixture trust")
			}
			c := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: dns}
			if peer != nil {
				c.Certificates = []tls.Certificate{Must(tls.X509KeyPair(peer.Certificate, peer.Key))}
			}
			client := &http.Client{Transport: &http.Transport{TLSClientConfig: c, ForceAttemptHTTP2: true}, Timeout: 15 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			return client
		}
		request := func(client *http.Client, path string) (*http.Response, error) {
			r, err := http.NewRequestWithContext(t.Context(), "GET", "https://"+business.address+path, nil)
			if err != nil {
				return nil, err
			}
			return client.Do(r)
		}
		fresh := func(trust []byte, peer Leaf, serial string) bool {
			client := makeClient(trust, &peer, serverDNS)
			client.Timeout = time.Second
			defer client.CloseIdleConnections()
			response, err := request(client, "/protected")
			if err != nil {
				return false
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusNoContent || response.TLS == nil || response.TLS.DidResume || response.TLS.Version != tls.VersionTLS13 || response.ProtoMajor != 2 {
				t.Fatal("fresh protected handshake/protocol failed")
			}
			return response.TLS.PeerCertificates[0].SerialNumber.String() == serial
		}
		if !fresh(ca.PEM, caller, leaf.Parsed.SerialNumber.String()) {
			t.Fatal("initial service native handshake failed")
		}
		wrongCA := Must(NewAuthority("http-service-untrusted"))
		wrongRole := Must(ca.ValidLeaf("wrong.edge.test", "spiffe://edge.test/ns/edge/sa/controller"))
		wrongPeer := Must(wrongCA.ValidLeaf("edge.test", edgeURI))
		expired := Must(ca.Issue("edge.test", edgeURI, time.Now().Add(-time.Hour), time.Now().Add(-time.Minute)))
		for _, bad := range []struct {
			name  string
			trust []byte
			peer  *Leaf
			dns   string
		}{
			{"role", ca.PEM, &wrongRole, serverDNS}, {"peer-ca", ca.PEM, &wrongPeer, serverDNS}, {"server-ca", wrongCA.PEM, &caller, serverDNS},
			{"name", ca.PEM, &caller, "wrong.edge.test"}, {"missing-peer", ca.PEM, nil, serverDNS}, {"expired-peer", ca.PEM, &expired, serverDNS},
		} {
			before := admitted.Load()
			response, err := request(makeClient(bad.trust, bad.peer, bad.dns), "/protected")
			if response != nil {
				_ = response.Body.Close()
			}
			if err == nil || admitted.Load() != before {
				t.Fatalf("invalid %s admitted protected request", bad.name)
			}
		}
		oldProtocol := makeClient(ca.PEM, &caller, serverDNS)
		oldTLS := oldProtocol.Transport.(*http.Transport).TLSClientConfig
		oldTLS.MinVersion, oldTLS.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
		beforeProtocol := admitted.Load()
		oldResponse, oldErr := request(oldProtocol, "/protected")
		if oldResponse != nil {
			_ = oldResponse.Body.Close()
		}
		if oldErr == nil || admitted.Load() != beforeProtocol {
			t.Fatal("native mutual TLS accepted an older protocol")
		}
		plain := &http.Client{Transport: &http.Transport{}, Timeout: time.Second}
		defer plain.CloseIdleConnections()
		before := admitted.Load()
		// Plaintext can fail at transport or receive an HTTP error before admission.
		response, _ := plain.Get("http://" + business.address + "/protected")
		if response != nil {
			_ = response.Body.Close()
			if response.StatusCode < 400 {
				t.Fatal("native business accepted plaintext")
			}
		}
		if admitted.Load() != before {
			t.Fatal("plaintext reached protected handler")
		}
		response, err := plain.Get("http://" + metric.address + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != 200 || string(body) != "fixture_requests 1\n" {
			t.Fatal("internal metrics unavailable")
		}
		response, err = plain.Get("http://" + metric.address + "/protected")
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != 404 || admitted.Load() != before {
			t.Fatal("metrics admitted business request")
		}
		held, err := request(makeClient(ca.PEM, &caller, serverDNS), "/stream")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Body.Close() }()
		reader := bufio.NewReader(held.Body)
		first, err := reader.ReadString('\n')
		if err != nil || first != "data: first\n" {
			t.Fatal("stream did not deliver before EOF")
		}
		nextCA := Must(NewAuthority("http-service-new"))
		nextLeaf := Must(nextCA.ValidLeaf(serverDNS, "spiffe://edge.test/ns/system/sa/backend"))
		nextCaller := Must(nextCA.ValidLeaf("edge.test", edgeURI))
		overlap := append(append([]byte{}, ca.PEM...), nextCA.PEM...)
		project("overlap", overlap, leaf)
		deadline := time.Now().Add(5 * time.Second)
		for !fresh(overlap, nextCaller, leaf.Parsed.SerialNumber.String()) {
			if time.Now().After(deadline) {
				t.Fatal("service did not activate overlap trust before leaf replacement")
			}
			time.Sleep(20 * time.Millisecond)
		}
		project("renewed", overlap, nextLeaf)
		deadline = time.Now().Add(5 * time.Second)
		for !fresh(overlap, nextCaller, nextLeaf.Parsed.SerialNumber.String()) {
			if time.Now().After(deadline) {
				t.Fatal("service watcher did not activate complete projected generation in five seconds")
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !fresh(overlap, caller, nextLeaf.Parsed.SerialNumber.String()) {
			t.Fatal("overlap retired previous client trust prematurely")
		}
		// Explicit shutdown withdraws readiness but retains credentials while the
		// previously admitted stream drains under its owning service deadline.
		cancel()
		deadline = time.Now().Add(time.Second)
		for readiness.Ready() {
			if time.Now().After(deadline) {
				t.Fatal("service did not withdraw readiness")
			}
			time.Sleep(time.Millisecond)
		}
		select {
		case err := <-done:
			joined = true
			t.Fatalf("service exited before held handler joined: %v", err)
		default:
		}
		if _, err := business.config.TLSConfig.GetConfigForClient(&tls.ClientHelloInfo{}); err != nil {
			t.Fatal("service closed credentials before admitted handler joined")
		}
		unblock()
		rest, err := io.ReadAll(reader)
		if err != nil || string(rest) != "\ndata: last\n\n" {
			t.Fatalf("held stream did not survive renewal and drain: %v", err)
		}
		select {
		case err := <-done:
			joined = true
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(12 * time.Second):
			t.Fatal("service listeners did not join")
		}
		if _, err := business.config.TLSConfig.GetConfigForClient(&tls.ClientHelloInfo{}); err == nil {
			t.Fatal("service retained credential owner after listeners joined")
		}
		for _, address := range []string{business.address, metric.address} {
			connection, err := net.DialTimeout("tcp", address, time.Second)
			if err == nil {
				_ = connection.Close()
				t.Fatal("service listener remained open after join")
			}
		}
		// Old trust is retired only after the actual owned process drain above.
		project("retired", nextCA.PEM, nextLeaf)
		restartCtx, restartCancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer restartCancel()
		readiness = workload.NewReadiness()
		readiness.MarkReady()
		done = make(chan error, 1)
		joined = false
		go func() { done <- run(restartCtx, config(dir), readiness, observe, public, metrics) }()
		for range 2 {
			select {
			case item := <-started:
				if strings.HasSuffix(item.config.ListenConfigKey, "_METRICS_ADDR") {
					metric = item
				} else {
					business = item
				}
			case err := <-done:
				joined = true
				t.Fatalf("service restart failed: %v", err)
			case <-restartCtx.Done():
				t.Fatal("service restart exceeded bound")
			}
		}
		if !fresh(nextCA.PEM, nextCaller, nextLeaf.Parsed.SerialNumber.String()) {
			t.Fatal("new trust/leaf could not serve after drained restart")
		}
		before = admitted.Load()
		response, err = request(makeClient(overlap, &caller, serverDNS), "/protected")
		if response != nil {
			_ = response.Body.Close()
		}
		if err == nil || admitted.Load() != before {
			t.Fatal("retired client trust admitted after drained restart")
		}
		restartCancel()
		select {
		case err := <-done:
			joined = true
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(12 * time.Second):
			t.Fatal("restarted service did not join")
		}
		if _, err := business.config.TLSConfig.GetConfigForClient(&tls.ClientHelloInfo{}); err == nil {
			t.Fatal("restarted service retained credential owner")
		}
	})
}
