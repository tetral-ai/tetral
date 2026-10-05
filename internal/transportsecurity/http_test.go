package transportsecurity_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestNativeHTTPConfigurationFailsClosed(t *testing.T) {
	for _, values := range []map[string]string{
		{"TETRAL_HTTP_TRANSPORT": "unknown"},
		{"TETRAL_HTTP_TLS_CA_PATH": "unselected"},
		{"TETRAL_HTTP_TRANSPORT": "native-mtls"},
		{"TETRAL_HTTP_TRANSPORT": "native-mtls", "TETRAL_HTTP_TLS_CA_PATH": "ca", "TETRAL_HTTP_TLS_CERT_PATH": "cert", "TETRAL_HTTP_TLS_KEY_PATH": "key", "TETRAL_HTTP_TLS_EDGE_CLIENT_URI": "spiffe://edge.test/ns/eg/sa/edge?caller=untrusted"},
	} {
		if _, err := transportsecurity.HTTPConfigFromEnv(func(key string) string { return values[key] }); err == nil {
			t.Fatal("incomplete or conflicting native configuration admitted")
		}
	}
	cfg, err := transportsecurity.HTTPConfigFromEnv(func(string) string { return "" })
	if err != nil || cfg.Mode != "plaintext" {
		t.Fatalf("local default: %v %v", cfg, err)
	}
	owner, config, err := cfg.Open(t.Context())
	if err != nil || owner != nil || config != nil {
		t.Fatal("plaintext selected native material")
	}
}

func TestNativeHTTPListenerIdentityAndFreshGeneration(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("native-http"))
	server := transporttest.Must(root.ValidLeaf("api.edge.test", "spiffe://edge.test/ns/system/sa/api"))
	caller := transporttest.Must(root.ValidLeaf("edge.edge.test", "spiffe://edge.test/ns/eg/sa/edge"))
	dir := t.TempDir()
	if err := transporttest.Project(dir, "initial", map[string][]byte{"ca.crt": root.PEM, "tls.crt": server.Certificate, "tls.key": server.Key}); err != nil {
		t.Fatal(err)
	}
	cfg := transportsecurity.HTTPConfig{Mode: "native-mtls", CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key"), EdgeClientURI: "spiffe://edge.test/ns/eg/sa/edge"}
	owner, config, err := cfg.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	var admitted atomic.Int64
	go func() {
		done <- workload.Run(ctx, workload.Config{ServiceName: "native-http-test", Listener: listener, TLSConfig: config, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { admitted.Add(1); w.WriteHeader(http.StatusNoContent) }), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ShutdownTimeout: time.Second})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("native HTTP listener did not join")
		}
	}()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(root.PEM)
	request := func(leaf transporttest.Leaf, expectedSerial string) bool {
		pair := transporttest.Must(tls.X509KeyPair(leaf.Certificate, leaf.Key))
		transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "api.edge.test", Certificates: []tls.Certificate{pair}}, ForceAttemptHTTP2: true}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
		response, err := client.Get("https://" + listener.Addr().String() + "/v1/protected")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent || response.ProtoMajor != 2 || response.TLS.NegotiatedProtocol != "h2" || response.TLS.PeerCertificates[0].SerialNumber.String() != expectedSerial {
			t.Fatalf("native HTTP protocol/generation mismatch: status=%d protocol=%s", response.StatusCode, response.Proto)
		}
		return true
	}
	if !request(caller, server.Parsed.SerialNumber.String()) {
		t.Fatal("correct edge identity not admitted")
	}
	wrong := transporttest.Must(root.ValidLeaf("wrong.edge.test", "spiffe://edge.test/ns/eg/sa/controller"))
	if request(wrong, server.Parsed.SerialNumber.String()) {
		t.Fatal("controller identity admitted as edge")
	}
	if admitted.Load() != 1 {
		t.Fatal("wrong-identity request reached business handler")
	}
	plain := &http.Client{Timeout: 2 * time.Second}
	response, err := plain.Get("http://" + listener.Addr().String() + "/v1/protected")
	if err == nil {
		response.Body.Close()
		if response.StatusCode < 400 {
			t.Fatal("plaintext admitted")
		}
	}
	if admitted.Load() != 1 {
		t.Fatal("plaintext reached business handler")
	}
	renewed := transporttest.Must(root.ValidLeaf("api.edge.test", "spiffe://edge.test/ns/system/sa/api"))
	if err := transporttest.Project(dir, "renewed", map[string][]byte{"ca.crt": root.PEM, "tls.crt": renewed.Certificate, "tls.key": renewed.Key}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Reload(); err != nil {
		t.Fatal(err)
	}
	if !request(caller, renewed.Parsed.SerialNumber.String()) {
		t.Fatal("renewed native generation not admitted")
	}
	if err := transporttest.Project(dir, "malformed", map[string][]byte{"ca.crt": root.PEM, "tls.crt": []byte("do-not-log-secret"), "tls.key": renewed.Key}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Reload(); err == nil {
		t.Fatal("malformed native update accepted")
	}
	if !request(caller, renewed.Parsed.SerialNumber.String()) {
		t.Fatal("last good native generation lost")
	}
}
