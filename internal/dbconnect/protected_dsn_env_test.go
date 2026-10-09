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
