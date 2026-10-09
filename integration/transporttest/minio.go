package transporttest

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

type MinIO struct {
	Resources           *testinfra.DockerResources
	Container           *testinfra.DockerContainer
	Directory, Endpoint string
	Authority           *Authority
	Leaf                Leaf
}

func NewMinIO(t *testing.T) *MinIO {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	m := &MinIO{Directory: t.TempDir(), Authority: Must(NewAuthority("object-store"))}
	m.Leaf = Must(m.Authority.ValidLeaf("minio.transport.test", ""))
	m.Resources = Must(testinfra.NewDockerResources(ctx, "minio-tls"))
	network := Must(m.Resources.Network(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := m.Resources.Close(ctx); err != nil {
			t.Errorf("MinIO fixture cleanup: %v", err)
		}
	})
	// MinIO interprets certificate subdirectories as additional SNI identities.
	// Keep retained projected generations outside that directory, so retirement
	// does not accidentally leave an old certificate selectable by SNI.
	Must(0, Project(filepath.Join(m.Directory, "leaf"), "initial", map[string][]byte{"public.crt": m.Leaf.Certificate, "private.key": m.Leaf.Key}))
	Must(0, os.Mkdir(filepath.Join(m.Directory, "certs"), 0700))
	for _, file := range []string{"public.crt", "private.key"} {
		Must(0, os.Symlink(filepath.Join("..", "leaf", file), filepath.Join(m.Directory, "certs", file)))
	}
	Must(0, os.WriteFile(filepath.Join(m.Directory, "ca.pem"), m.Authority.PEM, 0600))
	m.Container = Must(m.Resources.Run(ctx, testinfra.ContainerSpec{Image: testinfra.MinIOImage(), Network: network, Mounts: []testinfra.DockerMount{{Source: m.Directory, Target: "/fixture", ReadOnly: true}}, Ports: []int{9000}, User: "0", Env: map[string]string{"MINIO_ROOT_USER": "fixture-access", "MINIO_ROOT_PASSWORD": "fixture-password"}, Command: []string{"server", "/data", "--address", ":9000", "--certs-dir", "/fixture/certs"}}))
	m.Endpoint = "https://" + Must(m.Container.Address(ctx, 9000))
	owner := Must(transportsecurity.Open(ctx, transportsecurity.Config{CAPath: filepath.Join(m.Directory, "ca.pem")}))
	defer func() { _ = owner.Close() }()
	config := Must(owner.ClientTLSConfig("minio.transport.test", ""))
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: config}}
	if err := Await(ctx, func() bool {
		request, err := http.NewRequestWithContext(ctx, "GET", m.Endpoint+"/minio/health/live", nil)
		if err != nil {
			return false
		}
		res, err := client.Do(request)
		if err != nil {
			return false
		}
		defer func() { _ = res.Body.Close() }()
		return res.StatusCode == 200
	}); err != nil {
		logs, _ := m.Container.Logs(ctx)
		t.Fatalf("TLS object store startup: %v logs=%s", err, logs)
	}
	client.CloseIdleConnections()
	t.Logf("TLS MinIO image %s actual %s", testinfra.MinIOImage(), m.Container.ImageID)
	return m
}
func (m *MinIO) TLS(t *testing.T) *tls.Config {
	t.Helper()
	owner := Must(transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: filepath.Join(m.Directory, "ca.pem")}))
	t.Cleanup(func() { _ = owner.Close() })
	return Must(owner.ClientTLSConfig("minio.transport.test", ""))
}

// Reload uses the selected MinIO release's certificate reload signal after a
// complete projected leaf/key generation. Tests observe a fresh handshake.
func (m *MinIO) Reload(t *testing.T, name string, leaf Leaf) {
	t.Helper()
	Must(0, Project(filepath.Join(m.Directory, "leaf"), name, map[string][]byte{"public.crt": leaf.Certificate, "private.key": leaf.Key}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := m.Container.Signal(ctx, "HUP"); err != nil {
		t.Fatal(err)
	}
}
