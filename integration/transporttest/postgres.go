package transporttest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

// PostgreSQL uses a private TLS-only TCP listener, never the shared plain fixture.
type PostgreSQL struct {
	Resources                            *testinfra.DockerResources
	Container                            *testinfra.DockerContainer
	Directory, Network, URL, InternalURL string
	Authority                            *Authority
	Leaf                                 Leaf
}

func NewPostgreSQL(t *testing.T) *PostgreSQL {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	p := &PostgreSQL{Directory: t.TempDir(), Authority: Must(NewAuthority("postgres-store"))}
	p.Leaf = Must(p.Authority.ValidLeaf("postgres.transport.test", ""))
	p.Resources = Must(testinfra.NewDockerResources(ctx, "postgres-tls"))
	p.Network = Must(p.Resources.Network(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Resources.Close(ctx); err != nil {
			t.Errorf("PostgreSQL fixture cleanup: %v", err)
		}
	})
	p.Project(t, "initial", p.Leaf)
	// #nosec G306 -- public fixture HBA rules must be readable by the non-root postgres process; no secrets.
	Must(0, os.WriteFile(filepath.Join(p.Directory, "pg_hba.conf"), []byte("local all all trust\nhostssl all all 0.0.0.0/0 scram-sha-256\nhostssl all all ::/0 scram-sha-256\nhostnossl all all 0.0.0.0/0 reject\nhostnossl all all ::/0 reject\n"), 0644))
	// #nosec G302 -- fixture mount directory must be traversable by the container postgres user; private keys remain 0640.
	Must(0, os.Chmod(p.Directory, 0755))
	Must(0, os.WriteFile(filepath.Join(p.Directory, "ca.pem"), p.Authority.PEM, 0600))
	p.Container = Must(p.Resources.Run(ctx, testinfra.ContainerSpec{Image: testinfra.PostgreSQLImage(), Network: p.Network, Aliases: []string{"postgres.transport.test", "wrong.transport.test"}, Env: map[string]string{"POSTGRES_PASSWORD": "fixture-password", "POSTGRES_DB": "tetral"}, Mounts: []testinfra.DockerMount{{Source: p.Directory, Target: "/fixture"}}, Ports: []int{5432}, User: "0", Entrypoint: "sh", Command: []string{"-c", "chown root:postgres /fixture/leaf/*/tls.key && chmod 640 /fixture/leaf/*/tls.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/fixture/leaf/tls.crt -c ssl_key_file=/fixture/leaf/tls.key -c hba_file=/fixture/pg_hba.conf"}}))
	p.URL = "postgres://postgres:fixture-password@" + Must(p.Container.Address(ctx, 5432)) + "/tetral?sslmode=disable"
	p.InternalURL = "postgres://postgres:fixture-password@postgres.transport.test:5432/tetral?sslmode=disable"
	var lastErr error
	if err := Await(ctx, func() bool {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		result, err := dbconnect.OpenProtectedDSN(probe, p.URL, filepath.Join(p.Directory, "ca.pem"), "postgres.transport.test")
		if err != nil {
			lastErr = err
			return false
		}
		_ = result.Client.Close()
		return true
	}); err != nil {
		logs, _ := p.Container.Logs(ctx)
		t.Fatalf("TLS PostgreSQL startup: %v last=%v logs=%s", err, lastErr, logs)
	}
	t.Logf("TLS PostgreSQL image %s actual %s", testinfra.PostgreSQLImage(), p.Container.ImageID)
	return p
}
func (p *PostgreSQL) Project(t *testing.T, name string, leaf Leaf) {
	t.Helper()
	if err := Project(filepath.Join(p.Directory, "leaf"), name, map[string][]byte{"tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
	// #nosec G302 -- postgres must traverse projected public leaf directories.
	Must(0, os.Chmod(filepath.Join(p.Directory, "leaf"), 0755))
	// #nosec G302 -- postgres must traverse the selected projected leaf directory.
	Must(0, os.Chmod(filepath.Join(p.Directory, "leaf", name), 0755))
	// #nosec G302 -- public certificate is readable by postgres; its private key is separately restricted to 0640.
	Must(0, os.Chmod(filepath.Join(p.Directory, "leaf", name, "tls.crt"), 0644))
}
func (p *PostgreSQL) Reload(t *testing.T, name string, leaf Leaf) {
	t.Helper()
	p.Project(t, name, leaf)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := p.Container.Exec(ctx, "sh", "-c", "chown root:postgres /fixture/leaf/*/tls.key && chmod 640 /fixture/leaf/*/tls.key"); err != nil {
		t.Fatal(err)
	}
	if err := p.Container.Signal(ctx, "HUP"); err != nil {
		t.Fatal(err)
	}
}
func (p *PostgreSQL) Trust(t *testing.T, parent, name string, pem []byte) {
	t.Helper()
	if err := Project(parent, name, map[string][]byte{"ca.pem": pem}); err != nil {
		t.Fatal(fmt.Errorf("project PostgreSQL client trust: %w", err))
	}
}
