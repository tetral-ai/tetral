package integration

import (
	"net/http"
	"testing"

	"github.com/tetral-ai/tetral/integration/enginetest"
)

// publicClient is client, http.DefaultClient when nil, with every request it
// sends to a public route, and that request's answer, held to Engine's
// OpenAPI file.
func publicClient(t testing.TB, client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	held := *client
	base := held.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	held.Transport = enginetest.NewDeveloper(t).Transport(base)
	return &held
}
