package agentruntimebridge

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/testvector"
)

// This package pins vectors other components read; see testvector.Main.
func TestMain(m *testing.M) {
	testvector.Main(m)
}
