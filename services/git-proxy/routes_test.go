package gitproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseGitRequestWhitelist(t *testing.T) {
	cases := []struct {
		name     string
		method   string
		target   string
		endpoint gitEndpoint
		path     string
	}{
		{"refs upload suffixless", http.MethodGet, "/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", endpointRefsUpload, "/tetral-ai/tetral/info/refs"},
		{"refs receive git suffix", http.MethodGet, "/github.com/tetral-ai/tetral.git/info/refs?service=git-receive-pack", endpointRefsReceive, "/tetral-ai/tetral.git/info/refs"},
		{"upload pack", http.MethodPost, "/github.com/tetral-ai/tetral/git-upload-pack", endpointUploadPack, "/tetral-ai/tetral/git-upload-pack"},
		{"receive pack", http.MethodPost, "/github.com/tetral-ai/tetral.git/git-receive-pack", endpointReceivePack, "/tetral-ai/tetral.git/git-receive-pack"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, ok := parseGitRequest(httptest.NewRequest(tc.method, tc.target, nil), false)
			if !ok {
				t.Fatal("parseGitRequest rejected a whitelisted smart-HTTP endpoint")
			}
			if parsed.Endpoint != tc.endpoint || parsed.UpstreamPath != tc.path || parsed.Owner != "tetral-ai" || parsed.Repo != "tetral" {
				t.Fatalf("parsed = %+v", parsed)
			}
		})
	}
}

func TestParseGitRequestRejectsNonWhitelistedEndpoints(t *testing.T) {
	cases := []struct {
		name   string
		method string
		target string
	}{
		{"dumb refs", http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs"},
		{"lfs batch", http.MethodPost, "/ticket/github.com/tetral-ai/tetral.git/info/lfs/objects/batch"},
		{"wrong host segment", http.MethodGet, "/ticket/gitlab.com/tetral-ai/tetral/info/refs?service=git-upload-pack"},
		{"owner traversal", http.MethodGet, "/ticket/github.com/../tetral/info/refs?service=git-upload-pack"},
		{"repo traversal", http.MethodGet, "/ticket/github.com/tetral-ai/../info/refs?service=git-upload-pack"},
		{"extra query", http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack&extra=1"},
		{"duplicate service query", http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack&service=bad"},
		{"malformed query separator", http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack&bad=x;y"},
		{"malformed query escape", http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack&bad=%zz"},
		{"unknown method", http.MethodPut, "/ticket/github.com/tetral-ai/tetral/git-upload-pack"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if parsed, ok := parseGitRequest(httptest.NewRequest(tc.method, tc.target, nil), true); ok {
				t.Fatalf("parseGitRequest accepted %+v", parsed)
			}
		})
	}
}

func TestParseGitRequestLegacyPathRequiresCutover(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", nil)
	if _, ok := parseGitRequest(request, false); ok {
		t.Fatal("legacy ticket path parsed after cutover close")
	}
	parsed, ok := parseGitRequest(request, true)
	if !ok || parsed.LegacyTicket != "ticket" {
		t.Fatalf("legacy cutover parse = %+v, %v", parsed, ok)
	}
}
