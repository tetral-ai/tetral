package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/blob/blobtest"
)

func TestObjectStoreProtectedConnections(t *testing.T) {
	m := transporttest.NewMinIO(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	bucket := "transport-fixture"

	cfg := &blob.Config{Endpoint: m.Endpoint, Region: "us-east-1", Bucket: bucket, AccessKey: "fixture-access", SecretKey: "fixture-password", TLSCAPath: filepath.Join(m.Directory, "ca.pem"), TLSServerName: "minio.transport.test"}
	if err := blobtest.CreateBucket(ctx, &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: m.TLS(t)}}, *cfg); err != nil {
		t.Fatal(err)
	}
	// Environment proxy is deliberately outside the native store owner. A
	// hostile CONNECT endpoint must see no traffic while real store bytes move.
	var proxyCalls atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		http.Error(w, "denied", http.StatusBadGateway)
	}))
	defer proxy.Close()
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	store := transporttest.Must(blob.NewProtectedS3BlobStore(ctx, cfg))
	defer func() { _ = store.Close() }()
	if err := store.Put(ctx, "native/object", bytes.NewReader([]byte("verified bytes")), 14); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "native/object", bytes.NewReader([]byte("overwrite")), 9); err == nil {
		t.Fatal("native TLS changed create-only semantics")
	} else {
		var duplicate *blob.DuplicateKeyError
		if !errors.As(err, &duplicate) {
			t.Fatalf("CAS duplicate classification: %v", err)
		}
	}
	reader := transporttest.Must(store.Get(ctx, "native/object"))
	content := transporttest.Must(io.ReadAll(reader))
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if string(content) != "verified bytes" {
		t.Fatalf("store bytes changed %q", content)
	}
	if proxyCalls.Load() != 0 {
		t.Fatal("native protected transport delegated TLS to environment proxy")
	}

	t.Run("ActiveResponseRetirement", func(t *testing.T) {
		trust := filepath.Join(t.TempDir(), "trust")
		transporttest.Must(0, transporttest.Project(trust, "initial", map[string][]byte{"ca.pem": m.Authority.PEM}))
		cfg := *cfg
		cfg.TLSCAPath = filepath.Join(trust, "ca.pem")
		consumer := transporttest.Must(blob.NewProtectedS3BlobStore(ctx, &cfg))
		defer func() { _ = consumer.Close() }()
		held := transporttest.Must(consumer.Get(ctx, "native/object"))
		defer func() { _ = held.Close() }()
		replacement := transporttest.Must(transporttest.NewAuthority("retired-object-root"))
		transporttest.Must(0, transporttest.Project(trust, "retired", map[string][]byte{"ca.pem": replacement.PEM}))
		// R1 remains valid on the server, so an R2-only fresh connection rejects it.
		awaitStore(t, 4*time.Second, func() bool {
			probe, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			reader, err := consumer.Get(probe, "native/object")
			if err == nil {
				_ = reader.Close()
			}
			return err != nil
		})
		content, err := io.ReadAll(held)
		if err != nil || string(content) != "verified bytes" {
			t.Fatalf("admitted body lost during trust retirement: %q %v", content, err)
		}
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
		probe, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		reader, err := consumer.Get(probe, "native/object")
		if err == nil {
			_ = reader.Close()
			t.Fatal("retired active response returned an R1 socket to new admission")
		}
	})
	wrong := transporttest.Must(transporttest.NewAuthority("wrong-minio"))
	wrongPath := filepath.Join(t.TempDir(), "wrong.pem")
	transporttest.Must(0, os.WriteFile(wrongPath, wrong.PEM, 0600))
	for name, mutate := range map[string]func(*blob.Config){"WrongDNS": func(c *blob.Config) { c.TLSServerName = "wrong.transport.test" }, "WrongIssuer": func(c *blob.Config) { c.TLSCAPath = wrongPath }, "MissingTrust": func(c *blob.Config) { c.TLSCAPath = "" }, "PlaintextEndpoint": func(c *blob.Config) { c.Endpoint = "http://127.0.0.1:1" }, "LocalDowngrade": func(c *blob.Config) { c.LocalTestMode = true; c.AllowInsecure = true }} {
		t.Run(name, func(t *testing.T) {
			bad := *cfg
			mutate(&bad)
			s, err := blob.NewProtectedS3BlobStore(ctx, &bad)
			if err == nil {
				defer func() { _ = s.Close() }()
				probe, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				err = s.Put(probe, "negative/"+name, bytes.NewReader([]byte("denied")), 6)
			}
			if err == nil {
				t.Fatal("protected object store accepted invalid transport")
			}
		})
	}
	t.Run("LeafRenewalIssuerLossAndRootRetirement", func(t *testing.T) {
		address := transporttest.Must(url.Parse(m.Endpoint)).Host
		observe := func(leaf transporttest.Leaf) {
			t.Helper()
			tlsConfig := m.TLS(t)
			awaitStore(t, 4*time.Second, func() bool {
				probe, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				conn, err := (&tls.Dialer{Config: tlsConfig}).DialContext(probe, "tcp", address)
				if err != nil {
					return false
				}
				defer func() { _ = conn.Close() }()
				state := conn.(*tls.Conn).ConnectionState()
				return !state.DidResume && len(state.PeerCertificates) > 0 && sha256.Sum256(state.PeerCertificates[0].Raw) == sha256.Sum256(leaf.Parsed.Raw)
			})
		}
		renewed := transporttest.Must(m.Authority.ValidLeaf("minio.transport.test", ""))
		m.Reload(t, "renewed", renewed)
		observe(renewed)
		short := transporttest.Must(m.Authority.Issue("minio.transport.test", "", time.Now().Add(-time.Second), time.Now().Add(5*time.Second)))
		m.Reload(t, "issuer-short", short)
		observe(short)
		awaitStore(t, 7*time.Second, func() bool { return time.Now().After(short.Parsed.NotAfter) })
		fresh := transporttest.Must(blob.NewProtectedS3BlobStore(ctx, cfg))
		defer func() { _ = fresh.Close() }()
		probe, cancelProbe := context.WithTimeout(ctx, 2*time.Second)
		defer cancelProbe()
		if response, err := fresh.Get(probe, "native/object"); err == nil {
			_ = response.Close()
			t.Fatal("issuing-service loss extended expired MinIO leaf on a fresh connection")
		}
		m.Reload(t, "issuer-restored", renewed)
		observe(renewed)
		reader := transporttest.Must(fresh.Get(ctx, "native/object"))
		if bytes, err := io.ReadAll(reader); err != nil || string(bytes) != "verified bytes" {
			t.Fatalf("issuer restoration bytes=%q err=%v", bytes, err)
		}
		_ = reader.Close()
		r2 := transporttest.Must(transporttest.NewAuthority("minio-R2"))
		transporttest.Must(0, os.WriteFile(cfg.TLSCAPath, append(append([]byte{}, m.Authority.PEM...), r2.PEM...), 0600))
		leafR2 := transporttest.Must(r2.ValidLeaf("minio.transport.test", ""))
		m.Reload(t, "R2-leaf", leafR2)
		observe(leafR2)
		transporttest.Must(0, os.WriteFile(cfg.TLSCAPath, r2.PEM, 0600))
		final := transporttest.Must(blob.NewProtectedS3BlobStore(ctx, cfg))
		defer func() { _ = final.Close() }()
		reader = transporttest.Must(final.Get(ctx, "native/object"))
		if bytes, err := io.ReadAll(reader); err != nil || string(bytes) != "verified bytes" {
			t.Fatalf("R2-only store bytes=%q err=%v", bytes, err)
		}
		_ = reader.Close()
		old := *cfg
		old.TLSCAPath = filepath.Join(t.TempDir(), "R1.pem")
		transporttest.Must(0, os.WriteFile(old.TLSCAPath, m.Authority.PEM, 0600))
		retired := transporttest.Must(blob.NewProtectedS3BlobStore(ctx, &old))
		defer func() { _ = retired.Close() }()
		retiredProbe, cancelRetired := context.WithTimeout(ctx, 2*time.Second)
		defer cancelRetired()
		if reader, err := retired.Get(retiredProbe, "native/object"); err == nil {
			_ = reader.Close()
			t.Fatal("R1-only native owner accepted replacement R2 server")
		}
	})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := store.Put(probe, "after-close", bytes.NewReader([]byte("denied")), 6); err == nil {
		t.Fatal("owned store admitted a new connection after close")
	}
}
