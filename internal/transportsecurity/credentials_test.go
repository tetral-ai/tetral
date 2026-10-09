package transportsecurity_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

func nativeOwner(t *testing.T, root *transporttest.Authority, leaf transporttest.Leaf) (*transportsecurity.Owner, string) {
	t.Helper()
	dir := t.TempDir()
	if err := transporttest.Project(dir, "initial", map[string][]byte{"ca.crt": root.PEM, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	owner, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	return owner, dir
}
func nativeServer(t *testing.T, owner *transportsecurity.Owner) string {
	t.Helper()
	config, err := owner.ServerTLSConfig(transporttest.CallerURI)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() { _ = listener.Close(); <-done })
	go func() {
		defer close(done)
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = raw.Close() }()
				_ = raw.SetDeadline(time.Now().Add(2 * time.Second))
				connection := tls.Server(raw, config)
				if err := connection.Handshake(); err != nil {
					return
				}
				_, _ = connection.Write([]byte("verified"))
			}()
		}
	}()
	return listener.Addr().String()
}
func nativeHandshake(ctx context.Context, address string, owner *transportsecurity.Owner) (tls.ConnectionState, error) {
	cfg, err := owner.ClientTLSConfig("receiver.transport.test", transporttest.ReceiverURI)
	if err != nil {
		return tls.ConnectionState{}, err
	}
	dialer := &tls.Dialer{Config: cfg}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return tls.ConnectionState{}, err
	}
	defer func() { _ = connection.Close() }()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadAll(connection)
	return connection.(*tls.Conn).ConnectionState(), err
}

func TestCredentialGenerationAtomicActivationAndProjectedReload(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("loader"))
	serverLeaf := transporttest.Must(root.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	callerLeaf := transporttest.Must(root.ValidLeaf("caller.transport.test", transporttest.CallerURI))
	server, dir := nativeOwner(t, root, serverLeaf)
	caller, _ := nativeOwner(t, root, callerLeaf)
	// Every native listener is mutual; an absent client role identity is not a
	// weaker server-only mode.
	if _, err := server.ServerTLSConfig(""); err == nil {
		t.Fatal("server configuration accepted an absent client role identity")
	}
	if _, err := server.GRPCServerCredentials(""); err == nil {
		t.Fatal("gRPC server credentials accepted an absent client role identity")
	}
	address := nativeServer(t, server)
	before, err := nativeHandshake(t.Context(), address, caller)
	if err != nil {
		t.Fatal(err)
	}
	next := transporttest.Must(root.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	if err := transporttest.Project(dir, "replacement", map[string][]byte{"ca.crt": root.PEM, "tls.crt": next.Certificate, "tls.key": next.Key}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := transporttest.Await(ctx, func() bool {
		state, err := nativeHandshake(ctx, address, caller)
		return err == nil && !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(next.Parsed.Raw)
	}); err != nil {
		t.Fatal("projected generation was not activated within observation bound")
	}
	if sha256.Sum256(before.PeerCertificates[0].Raw) == sha256.Sum256(next.Parsed.Raw) {
		t.Fatal("fixture did not change its leaf")
	}
	if err := transporttest.Project(dir, "mismatched", map[string][]byte{"ca.crt": root.PEM, "tls.crt": next.Certificate, "tls.key": callerLeaf.Key}); err != nil {
		t.Fatal(err)
	}
	if err := server.Reload(); err == nil {
		t.Fatal("mismatched generation activated")
	}
	state, err := nativeHandshake(t.Context(), address, caller)
	if err != nil || sha256.Sum256(state.PeerCertificates[0].Raw) != sha256.Sum256(next.Parsed.Raw) {
		t.Fatal("malformed update replaced the valid last-known generation")
	}
}

func trustBundle(roots ...*transporttest.Authority) []byte {
	var bundle []byte
	for _, root := range roots {
		bundle = append(bundle, root.PEM...)
	}
	return bundle
}

func projectedOwner(t *testing.T, trust []byte, leaf transporttest.Leaf) (*transportsecurity.Owner, string, error) {
	t.Helper()
	dir := t.TempDir()
	transporttest.Must(0, transporttest.Project(dir, "initial", map[string][]byte{"ca.crt": trust, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}))
	owner, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key")})
	if err == nil {
		t.Cleanup(func() { _ = owner.Close() })
	}
	return owner, dir, err
}

// An expired anchor cannot validate a chain, so the loader leaves it out
// instead of disabling the generation; a not-yet-valid anchor keeps the update
// invalid so the last-known-good generation stays active.
func TestCredentialTrustBundleAnchorValidity(t *testing.T) {
	now := time.Now()
	expired := transporttest.Must(transporttest.NewAuthorityWithValidity("expired", now.Add(-2*time.Hour), now.Add(-time.Hour)))
	current := transporttest.Must(transporttest.NewAuthority("current"))
	future := transporttest.Must(transporttest.NewAuthorityWithValidity("future", now.Add(time.Hour), now.Add(2*time.Hour)))
	serverLeaf := transporttest.Must(current.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	callerLeaf := transporttest.Must(current.ValidLeaf("caller.transport.test", transporttest.CallerURI))

	t.Run("ExpiredAnchorIgnored", func(t *testing.T) {
		server, _, err := projectedOwner(t, trustBundle(expired, current), serverLeaf)
		if err != nil {
			t.Fatal("expired anchor disabled a bundle with a current CA:", err)
		}
		caller, _, err := projectedOwner(t, trustBundle(expired, current), callerLeaf)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nativeHandshake(t.Context(), nativeServer(t, server), caller); err != nil {
			t.Fatal("handshake under the current anchor failed:", err)
		}
	})
	t.Run("OnlyExpiredAnchors", func(t *testing.T) {
		expiredLeaf := transporttest.Must(expired.Issue("receiver.transport.test", transporttest.ReceiverURI, now.Add(-2*time.Hour), now.Add(-time.Hour-time.Minute)))
		if _, _, err := projectedOwner(t, expired.PEM, expiredLeaf); err == nil {
			t.Fatal("bundle without a currently valid CA was accepted")
		}
	})
	t.Run("NotYetValidAnchorKeepsLastKnownGood", func(t *testing.T) {
		if _, _, err := projectedOwner(t, trustBundle(current, future), serverLeaf); err == nil {
			t.Fatal("not-yet-valid anchor was accepted")
		}
		owner, dir, err := projectedOwner(t, current.PEM, serverLeaf)
		if err != nil {
			t.Fatal(err)
		}
		transporttest.Must(0, transporttest.Project(dir, "future", map[string][]byte{"ca.crt": trustBundle(current, future), "tls.crt": serverLeaf.Certificate, "tls.key": serverLeaf.Key}))
		if err := owner.Reload(); err == nil {
			t.Fatal("not-yet-valid anchor activated")
		}
		if _, err := owner.ServerTLSConfig(transporttest.CallerURI); err != nil {
			t.Fatal("valid last-known generation was not retained:", err)
		}
	})
	t.Run("OverlapSurvivesOldAnchorExpiry", func(t *testing.T) {
		// Old trust R1 expires during an R1+R2 overlap. Generations whose leaf
		// chains to R2 stay usable; a generation whose leaf chains only to R1
		// stops admitting handshakes when R1 expires.
		r1 := transporttest.Must(transporttest.NewAuthorityWithValidity("expiring", time.Now().Add(-30*time.Second), time.Now().Add(2*time.Second)))
		r2 := transporttest.Must(transporttest.NewAuthority("replacement"))
		overlap := trustBundle(r1, r2)
		server, _, err := projectedOwner(t, overlap, transporttest.Must(r2.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI)))
		if err != nil {
			t.Fatal(err)
		}
		caller, _, err := projectedOwner(t, overlap, transporttest.Must(r2.ValidLeaf("caller.transport.test", transporttest.CallerURI)))
		if err != nil {
			t.Fatal(err)
		}
		retiring, _, err := projectedOwner(t, overlap, transporttest.Must(r1.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI)))
		if err != nil {
			t.Fatal(err)
		}
		address := nativeServer(t, server)
		if _, err := nativeHandshake(t.Context(), address, caller); err != nil {
			t.Fatal("overlap handshake failed before R1 expiry:", err)
		}
		ctx, cancel := context.WithDeadline(t.Context(), r1.Certificate.NotAfter.Add(100*time.Millisecond))
		defer cancel()
		<-ctx.Done()
		if _, err := caller.ClientTLSConfig("receiver.transport.test", transporttest.ReceiverURI); err != nil {
			t.Fatal("expired old anchor disabled the R2 generation:", err)
		}
		if _, err := nativeHandshake(t.Context(), address, caller); err != nil {
			t.Fatal("R2 handshake failed after R1 expiry:", err)
		}
		if _, err := retiring.ServerTLSConfig(transporttest.CallerURI); err == nil {
			t.Fatal("leaf chained only to an expired anchor admitted a new handshake")
		}
	})
}

func TestCredentialLastKnownGoodExpiryFailsNewHandshake(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("expiry"))
	leaf := transporttest.Must(root.Issue("receiver.transport.test", transporttest.ReceiverURI, time.Now().Add(-30*time.Second), time.Now().Add(2*time.Second)))
	owner, dir := nativeOwner(t, root, leaf)
	if err := transporttest.Project(dir, "malformed", map[string][]byte{"ca.crt": root.PEM, "tls.crt": []byte("invalid"), "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Reload(); err == nil {
		t.Fatal("malformed update did not fail")
	}
	if _, err := owner.ServerTLSConfig(transporttest.CallerURI); err != nil {
		t.Fatal("valid last-known generation was not retained")
	}
	ctx, cancel := context.WithDeadline(t.Context(), leaf.Parsed.NotAfter.Add(100*time.Millisecond))
	defer cancel()
	<-ctx.Done()
	if _, err := owner.ServerTLSConfig(transporttest.CallerURI); err == nil {
		t.Fatal("expired retained server leaf admitted a new handshake")
	}
	if _, err := owner.ClientTLSConfig("receiver.transport.test", transporttest.ReceiverURI); err == nil {
		t.Fatal("expired retained client leaf admitted a new handshake")
	}
}

func TestCredentialWatcherCloseJoinsAndBlocksNewConnections(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("close"))
	leaf := transporttest.Must(root.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	owner, _ := nativeOwner(t, root, leaf)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.ClientTLSConfig("receiver.transport.test", transporttest.ReceiverURI); err == nil {
		t.Fatal("closed credential owner admitted a connection")
	}
}

type failingReloadHandler struct {
	panicSink bool
	silent    bool
}

func (h failingReloadHandler) Enabled(context.Context, slog.Level) bool { return !h.silent }
func (h failingReloadHandler) Handle(context.Context, slog.Record) error {
	if h.panicSink {
		panic("fixture secret-shaped sink failure")
	}
	return errors.New("fixture sink failure")
}
func (h failingReloadHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h failingReloadHandler) WithGroup(string) slog.Handler      { return h }

func TestCredentialReloadDiagnosticsAreBoundedAndIndependent(t *testing.T) {
	previous := slog.Default()
	defer slog.SetDefault(previous)
	root := transporttest.Must(transporttest.NewAuthority("diagnostics"))
	leaf := transporttest.Must(root.ValidLeaf("receiver.transport.test", transporttest.ReceiverURI))
	for _, mode := range []string{"json", "silent", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			var records bytes.Buffer
			var handler slog.Handler = slog.NewJSONHandler(&records, nil)
			if mode != "json" {
				handler = failingReloadHandler{panicSink: mode == "panic", silent: mode == "silent"}
			}
			slog.SetDefault(slog.New(handler))
			dir := t.TempDir()
			transporttest.Must(0, transporttest.Project(dir, "valid", map[string][]byte{"ca.crt": root.PEM, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}))
			owner := transporttest.Must(transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key"), Purpose: "runtime-direct"}))
			defer func() { _ = owner.Close() }()
			transporttest.Must(0, transporttest.Project(dir, "malformed", map[string][]byte{"ca.crt": []byte("fixture-secret-never-log"), "tls.crt": leaf.Certificate, "tls.key": leaf.Key}))
			for range 20 {
				if err := owner.Reload(); err == nil {
					t.Fatal("malformed generation accepted")
				}
			}
			if _, err := owner.ClientTLSConfig("receiver.transport.test", transporttest.ReceiverURI); err != nil {
				t.Fatal("diagnostics changed retained credential", err)
			}
			transporttest.Must(0, transporttest.Project(dir, "recovered", map[string][]byte{"ca.crt": root.PEM, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}))
			if err := owner.Reload(); err != nil {
				t.Fatal(err)
			}
			if count := transportsecurity.ReloadFailureCount(owner); count != 20 {
				t.Fatalf("rejected observations=%d", count)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Reload(); err == nil {
				t.Fatal("closed owner reloaded credential work")
			}
			if mode == "json" {
				decoder := json.NewDecoder(&records)
				rows := []map[string]any{}
				for {
					var row map[string]any
					if err := decoder.Decode(&row); err == io.EOF {
						break
					} else if err != nil {
						t.Fatal(err)
					}
					rows = append(rows, row)
				}
				if len(rows) != 2 || rows[0]["msg"] != "transport.credential_reload_failed" || rows[1]["msg"] != "transport.credential_reload_recovered" || rows[1]["failed.count"] != float64(20) {
					t.Fatalf("bounded failure/recovery records=%v", rows)
				}
				for _, row := range rows {
					for key, value := range row {
						if strings.Contains(fmt.Sprint(value), "fixture-secret") || strings.Contains(fmt.Sprint(value), dir) {
							t.Fatalf("credential diagnostic exposed material/path in%s", key)
						}
					}
				}
			}
		})
	}
}
