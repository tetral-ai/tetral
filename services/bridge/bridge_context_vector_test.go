package agentruntimebridge

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/testvector"
)

// pinBridgeContext pins the envelope LoadContext returned as the Runtime's
// input vector of that name; the Runtime's tests hold what it must restore.
func pinBridgeContext(t *testing.T, name, contextJSON string) {
	t.Helper()
	testvector.PinJSON(t, "services/agent-runtime/packages/protocol/testdata/bridge-context/"+name+".json", []byte(contextJSON))
}
