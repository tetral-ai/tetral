package eventstream

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

func fixtureNATSCredentials(t *testing.T) NATSConfig {
	t.Helper()
	dir := t.TempDir()
	user, password := filepath.Join(dir, "user"), filepath.Join(dir, "password")
	if err := os.WriteFile(user, []byte("subscriber"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(password, []byte("fixture-password"), 0600); err != nil {
		t.Fatal(err)
	}
	return NATSConfig{UserPath: user, PasswordPath: password, ConnectTimeout: 200 * time.Millisecond, ReconnectWait: time.Second}
}

func TestNATSSubscriberConnectBudgetCoversTLSAndINFO(t *testing.T) {
	authority := transporttest.Must(transporttest.NewAuthority("subscriber-budget"))
	serverLeaf := transporttest.Must(authority.ValidLeaf("localhost", ""))
	clientLeaf := transporttest.Must(authority.ValidLeaf("client.nats.test", ""))
	dir := t.TempDir()
	if err := transporttest.Project(dir, "tls", map[string][]byte{"ca.crt": authority.PEM, "tls.crt": clientLeaf.Certificate, "tls.key": clientLeaf.Key}); err != nil {
		t.Fatal(err)
	}
	trust, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key"), Purpose: "nats"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = trust.Close() }()
	certificate, err := tls.X509KeyPair(serverLeaf.Certificate, serverLeaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{})
	serverDone := make(chan struct{})
	handshaken := make(chan struct{})
	gate := make(chan struct{})
	go func() {
		defer close(serverDone)
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = raw.Close() }()
		close(accepted)
		<-gate
		connection := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
		if err := connection.Handshake(); err != nil {
			return
		}
		close(handshaken)
		_, _ = io.Copy(io.Discard, connection)
	}()
	config := fixtureNATSCredentials(t)
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	config.Servers = []string{"tls://localhost:" + port}
	transport := &NATSPreviewTransport{config: config, servers: config.Servers, ctx: t.Context(), trust: trust, metrics: NewPreviewMetrics(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	result := make(chan error, 1)
	started := time.Now()
	go func() {
		connection, err := transport.connect()
		if connection != nil {
			connection.Close()
		}
		result <- err
	}()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("TLS client did not connect")
	}
	// This fixture spends most of the one connection budget in the handshake,
	// then deliberately withholds INFO. INFO must use only the remaining budget.
	timer := time.AfterFunc(150*time.Millisecond, func() { close(gate) })
	defer timer.Stop()
	select {
	case <-handshaken:
	case <-time.After(5 * time.Second):
		t.Fatal("TLS handshake control did not complete")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("connection failure=%v want shared deadline", err)
		}
		if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
			t.Fatalf("TLS and INFO consumed separate budgets: %s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection budget did not interrupt INFO")
	}
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("provisional TLS socket not joined")
	}
}

func TestNATSSubscriberShutdownJoinsUnavailableConnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = raw.Close() }()
		close(accepted)
		_, _ = io.Copy(io.Discard, raw)
	}()
	config := fixtureNATSCredentials(t)
	config.Servers = []string{"nats://" + listener.Addr().String()}
	transport, err := NewNATSPreviewTransport(t.Context(), config, DefaultStreamConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("connect fixture not entered")
	}
	closed := make(chan struct{})
	go func() { transport.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber supervisor did not join")
	}
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("late-created socket survived shutdown")
	}
	transport.Close()
}
