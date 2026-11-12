package eventstream

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/httpapi/httpapitest"
)

// Every route of the stream router is a public operation that the
// authorization table knows under its method and pattern.
func TestStreamRoutesArePublicOperationsTheAuthorizationTableKnows(t *testing.T) {
	if err := httpapitest.PublicRouteCoverage(NewRouter(nil, nil)); err != nil {
		t.Fatal(err)
	}
}
