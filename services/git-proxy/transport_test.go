package gitproxy

import (
	"net/http"
	"testing"
)

func TestDefaultGitHubTransportIgnoresAmbientProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	transport, ok := defaultGitHubTransport().(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T; want *http.Transport", defaultGitHubTransport())
	}
	if transport.Proxy != nil {
		t.Fatal("default GitHub transport honors ambient proxy configuration")
	}
}
