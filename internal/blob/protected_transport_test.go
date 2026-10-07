package blob

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

type storeAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func makeStoreAuthority(t *testing.T, name string, until time.Time) storeAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: until, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return storeAuthority{parsed, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}
func (a storeAuthority) leaf(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"store.transport.test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestProtectedHTTPGenerationsPinBodiesAndPendingTrust(t *testing.T) {
	// Every anchor of the first two generations expires within seconds; a
	// generation stays valid until its latest retained anchor expires.
	r1 := makeStoreAuthority(t, "R1", time.Now().Add(3*time.Second))
	short := makeStoreAuthority(t, "short", time.Now().Add(3*time.Second))
	r3 := makeStoreAuthority(t, "R3", time.Now().Add(time.Hour))
	var serving atomic.Pointer[tls.Certificate]
	r1Leaf, r3Leaf := r1.leaf(t), r3.leaf(t)
	serving.Store(&r1Leaf)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(bytes.Repeat([]byte("x"), 8192)) }))
	server.TLS = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return serving.Load(), nil }}
	server.StartTLS()
	defer server.Close()
	trust := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(trust, r1.pem, 0600); err != nil {
		t.Fatal(err)
	}
	owner, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: trust, Purpose: "blob"})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newProtectedTransport(owner, "store.transport.test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close() }()
	client := &http.Client{Transport: manager, Timeout: 5 * time.Second}
	first, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Body.Close() }()
	update := func(pem []byte) {
		t.Helper()
		if err := os.WriteFile(trust, pem, 0600); err != nil {
			t.Fatal(err)
		}
		if err := owner.Reload(); err != nil {
			t.Fatal(err)
		}
	}
	update(append(append([]byte{}, r1.pem...), short.pem...))
	second, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Body.Close() }()
	manager.mu.Lock()
	old, active := manager.retired, manager.active
	ownedBodies := old != nil && old.users == 1 && active.users == 1
	manager.mu.Unlock()
	if !ownedBodies {
		t.Fatal("held bodies did not pin their transport generations")
	}
	update(append(append([]byte{}, r1.pem...), r3.pem...))
	manager.mu.Lock()
	pendingLatest := manager.pending && manager.active == active && manager.retired == old
	manager.mu.Unlock()
	if !pendingLatest {
		t.Fatal("third update allocated another transport or lost pending latest")
	}
	// The owner already has valid latest trust, but every anchor of the active
	// generation expires while the older admitted body keeps both transport
	// generations occupied.
	activeExpiry := short.cert.NotAfter
	if r1.cert.NotAfter.After(activeExpiry) {
		activeExpiry = r1.cert.NotAfter
	}
	ctx, cancel := context.WithDeadline(t.Context(), activeExpiry.Add(time.Second))
	defer cancel()
	for time.Now().Before(activeExpiry) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	if response, err := client.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("pending latest incorrectly extended active generation validity")
	}
	if _, err := io.Copy(io.Discard, first.Body); err != nil {
		t.Fatal("admitted old body was interrupted", err)
	}
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	activatedLatest := old.users == 0 && manager.active != active && manager.retired == active && !manager.pending
	manager.mu.Unlock()
	if !activatedLatest {
		t.Fatal("body EOF/Close release or latest activation failed")
	}
	serving.Store(&r3Leaf)
	third, err := client.Get(server.URL)
	if err != nil {
		t.Fatal("latest valid trust was not activated", err)
	}
	if _, err := io.Copy(io.Discard, third.Body); err != nil {
		t.Fatal(err)
	}
	_ = third.Body.Close()
	if _, err := io.Copy(io.Discard, second.Body); err != nil {
		t.Fatal("second admitted body was interrupted", err)
	}
	_ = second.Body.Close()
	manager.mu.Lock()
	bodiesJoined := active.users == 0 && manager.retired == nil && manager.active.users == 0
	manager.mu.Unlock()
	if !bodiesJoined {
		t.Fatal("transport body ownership leaked or released twice")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if response, err := client.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("closed owner admitted operation")
	}
}
