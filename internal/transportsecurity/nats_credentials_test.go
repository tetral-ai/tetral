package transportsecurity_test

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

func TestNATSCredentialPurposeUsesIndependentMountAndTrustRetirement(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("nats-native"))
	leaf := transporttest.Must(root.ValidLeaf("client.nats.test", ""))
	dir := t.TempDir()
	if err := transporttest.Project(dir, "first", map[string][]byte{"ca.crt": root.PEM, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	owner, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(dir, "ca.crt"), CertPath: filepath.Join(dir, "tls.crt"), KeyPath: filepath.Join(dir, "tls.key"), Purpose: "nats"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	config, err := owner.ClientTLSConfig("broker.nats.test", "")
	if err != nil || config.ServerName != "broker.nats.test" || len(config.Certificates) != 1 || config.InsecureSkipVerify || len(config.NextProtos) != 0 {
		t.Fatalf("native NATS TLS snapshot=%+v err=%v", config, err)
	}
	if _, err := owner.ClientTLSConfig("127.0.0.1", ""); err == nil {
		t.Fatal("IP destination bypassed required DNS verification")
	}
	var retire atomic.Int64
	if err := owner.SetTrustActivationObserver(func() { retire.Add(1) }); err != nil {
		t.Fatal(err)
	}
	otherRoot := transporttest.Must(transporttest.NewAuthority("nats-retirement"))
	otherLeaf := transporttest.Must(otherRoot.ValidLeaf("client.nats.test", ""))
	if err := transporttest.Project(dir, "replacement", map[string][]byte{"ca.crt": otherRoot.PEM, "tls.crt": otherLeaf.Certificate, "tls.key": otherLeaf.Key}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Reload(); err != nil {
		t.Fatal(err)
	}
	if retire.Load() != 1 {
		t.Fatalf("trust retirement callbacks=%d", retire.Load())
	}
	if owner.IsCurrentTLSConfig(config) {
		t.Fatal("removed trust remains current")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.ClientTLSConfig("broker.nats.test", ""); err == nil {
		t.Fatal("closed NATS credential owner admits connection")
	}
}
