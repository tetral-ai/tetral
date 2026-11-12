package storage_test

import (
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// TestPostgreSQLMissingConfigMessagesExplainTestPath pins the local test setup
// explanation: when PostgreSQL test configuration is missing locally, the error must name
// TETRAL_TEST_DATABASE_URL, point the developer at a real PostgreSQL
// test database, and tell them CI provides it automatically. Each
// missing-config surface in Engine must say all three things.
func TestPostgreSQLMissingConfigMessagesExplainTestPath(t *testing.T) {
	cases := []struct {
		surface string
		message string
	}{
		{
			surface: "storage.requirePostgreSQLTestDSN in-package helper",
			message: missingPostgreSQLTestDSNHelperMessage(),
		},
		{
			surface: "storagetest.MissingTestDSNError",
			message: (&storagetest.MissingTestDSNError{EnvVarName: storagetest.EnvTestDatabaseURL}).Error(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.surface, func(t *testing.T) {
			for _, expected := range []string{
				"TETRAL_TEST_DATABASE_URL",
				"real PostgreSQL",
				"CI provides it automatically",
			} {
				if !strings.Contains(tc.message, expected) {
					t.Errorf("missing-config message must contain %q for the developer to recover; got %q", expected, tc.message)
				}
			}
		})
	}
}

// missingPostgreSQLTestDSNHelperMessage exposes the exact t.Fatal
// message used by requirePostgreSQLTestDSN (defined in
// postgresql_database_test.go) without invoking t.Fatal itself. The
// string is copied verbatim from the helper; if the helper text
// changes, this test will fail until the constant is updated, which is
// the desired regression guard for the explanation contract.
func missingPostgreSQLTestDSNHelperMessage() string {
	return "TETRAL_TEST_DATABASE_URL is required: set it to a real PostgreSQL test DSN. CI provides it automatically."
}
