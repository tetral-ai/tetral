package dbconnect_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// The production opener reads only TETRAL_DATABASE_URL. A populated test
// variable must neither be used as a fallback nor appear in the diagnostic.
func TestOpenProtectedDSNFromEnvDoesNotFallbackToTestVariable(t *testing.T) {
	const testDSN = "postgres://tetral:test-variable-must-not-be-used@127.0.0.1:1/tetral"
	root := transporttest.Must(transporttest.NewAuthority("dbconnect-env"))
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, root.PEM, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TETRAL_DATABASE_URL", "")
	t.Setenv("TETRAL_TEST_DATABASE_URL", testDSN)
	t.Setenv("TETRAL_DATABASE_TLS_CA_PATH", caPath)
	t.Setenv("TETRAL_DATABASE_TLS_SERVER_NAME", "postgres.test")

	result, err := dbconnect.OpenProtectedDSNFromEnv(context.Background())
	if err == nil {
		_ = result.Client.Close()
		t.Fatal("protected opener used a database URL other than TETRAL_DATABASE_URL")
	}
	var diagnostic *dbconnect.DiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic.Phase != dbconnect.PhaseParseConfig || diagnostic.Kind != dbconnect.KindInvalidConfig || diagnostic.Provider != dbconnect.ProviderProtectedDSN {
		t.Fatalf("missing production URL diagnostic = %#v (%v)", diagnostic, err)
	}
	if strings.Contains(err.Error(), "test-variable-must-not-be-used") || strings.Contains(err.Error(), "TETRAL_TEST_DATABASE_URL") {
		t.Fatalf("diagnostic exposed the test variable: %q", err.Error())
	}
}

// A DSN that does not parse is refused with a sentence that quotes none of it;
// the other refusals of the administrative opener keep their own error.
func TestOpenProtectedConfigKeepsTheErrorOfEachRefusal(t *testing.T) {
	root := transporttest.Must(transporttest.NewAuthority("dbconnect-config"))
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, root.PEM, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, dsn, serverName, want string }{
		{name: "a DSN that does not parse", dsn: "postgres://tetral:pa/ss%zz@db.test/tetral", serverName: "postgres.test", want: "protected PostgreSQL configuration is invalid"},
		{name: "a socket endpoint", dsn: "postgres://tetral@/tetral?host=/var/run/postgresql", serverName: "postgres.test", want: "protected PostgreSQL requires a TCP endpoint"},
		{name: "no server name", dsn: "postgres://tetral@db.test/tetral", serverName: "", want: "TLS expected server DNS name is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, owner, err := dbconnect.OpenProtectedConfig(context.Background(), test.dsn, caPath, test.serverName)
			if err == nil {
				_ = owner.Close()
				t.Fatalf("OpenProtectedConfig = %v; want a refusal", config)
			}
			if err.Error() != test.want {
				t.Fatalf("OpenProtectedConfig error = %q; want %q", err, test.want)
			}
		})
	}
}
