package storage_test

import (
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// A test that needs PostgreSQL and finds no DSN tells the developer which
// variable to set, to what, and that CI sets it.
func TestMissingTestDSNErrorExplainsHowToRecover(t *testing.T) {
	message := (&storagetest.MissingTestDSNError{EnvVarName: storagetest.EnvTestDatabaseURL}).Error()
	for _, expected := range []string{"TETRAL_TEST_DATABASE_URL", "real PostgreSQL", "CI provides it automatically"} {
		if !strings.Contains(message, expected) {
			t.Errorf("missing-DSN message %q; want it to contain %q", message, expected)
		}
	}
}
