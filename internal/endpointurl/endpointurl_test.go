package endpointurl

import (
	"strings"
	"testing"
)

func TestCanonicalizePublicURLAcceptedAndCanonicalized(t *testing.T) {
	got, err := CanonicalizePublicURL("https://Example.COM:8443/mcp", "auth.mcp_server_url")
	if err != nil {
		t.Fatalf("CanonicalizePublicURL: %v", err)
	}
	if got != "https://example.com:8443/mcp" {
		t.Fatalf("canonical URL = %q; want https://example.com:8443/mcp", got)
	}
}

func TestCanonicalizePublicURLRejectsUnsafeInputsWithoutEchoingSecrets(t *testing.T) {
	longLabel := strings.Repeat("a", 64)
	longHostname := strings.Join([]string{
		strings.Repeat("a", 63),
		strings.Repeat("b", 63),
		strings.Repeat("c", 63),
		strings.Repeat("d", 62),
	}, ".")
	cases := []struct {
		name        string
		raw         string
		mustMention string
		mustOmit    []string
	}{
		{
			name:        "http scheme",
			raw:         "http://example.com/mcp",
			mustMention: "https",
			mustOmit:    []string{"http://example.com/mcp"},
		},
		{ //nolint:gosec // deliberate fixture proving embedded credential redaction
			name:        "embedded credentials",
			raw:         "https://user:pass@example.com/mcp",
			mustMention: "credentials",
			mustOmit:    []string{"user", "pass", "user:pass", "https://user:pass@example.com/mcp"},
		},
		{
			name:        "query",
			raw:         "https://example.com/mcp?token=secret",
			mustMention: "query",
			mustOmit:    []string{"secret", "token=secret", "https://example.com/mcp?token=secret"},
		},
		{
			name:        "fragment",
			raw:         "https://example.com/mcp#secret",
			mustMention: "fragment",
			mustOmit:    []string{"secret", "#secret", "https://example.com/mcp#secret"},
		},
		{
			name:        "empty host",
			raw:         "https:///mcp",
			mustMention: "host is empty",
			mustOmit:    []string{"https:///mcp"},
		},
		{
			name:        "malformed host",
			raw:         "https://[::1/mcp",
			mustMention: "valid URL",
			mustOmit:    []string{"[::1", "https://[::1/mcp"},
		},
		{
			name:        "localhost",
			raw:         "https://localhost/mcp",
			mustMention: "localhost",
			mustOmit:    []string{"https://localhost/mcp"},
		},
		{
			name:        "localhost suffix",
			raw:         "https://foo.localhost/mcp",
			mustMention: "localhost",
			mustOmit:    []string{"https://foo.localhost/mcp"},
		},
		{
			name:        "non public ip",
			raw:         "https://127.0.0.1/mcp",
			mustMention: "globally reachable",
			mustOmit:    []string{"127.0.0.1", "https://127.0.0.1/mcp"},
		},
		{
			name:        "legacy ipv4",
			raw:         "https://0177.0.0.1/mcp",
			mustMention: "legacy IPv4",
			mustOmit:    []string{"0177.0.0.1", "https://0177.0.0.1/mcp"},
		},
		{
			name:        "empty dns label",
			raw:         "https://example..com/mcp",
			mustMention: "valid DNS name",
			mustOmit:    []string{"example..com", "https://example..com/mcp"},
		},
		{
			name:        "leading label hyphen",
			raw:         "https://-example.com/mcp",
			mustMention: "valid DNS name",
			mustOmit:    []string{"-example.com", "https://-example.com/mcp"},
		},
		{
			name:        "trailing label hyphen",
			raw:         "https://example-.com/mcp",
			mustMention: "valid DNS name",
			mustOmit:    []string{"example-.com", "https://example-.com/mcp"},
		},
		{
			name:        "underscore",
			raw:         "https://exa_mple.com/mcp",
			mustMention: "valid DNS name",
			mustOmit:    []string{"exa_mple.com", "https://exa_mple.com/mcp"},
		},
		{
			name:        "overlong dns label",
			raw:         "https://" + longLabel + ".com/mcp",
			mustMention: "valid DNS name",
			mustOmit:    []string{longLabel, "https://" + longLabel + ".com/mcp"},
		},
		{
			name:        "overlong hostname",
			raw:         "https://" + longHostname + "/mcp",
			mustMention: "valid DNS name",
			mustOmit:    []string{longHostname, "https://" + longHostname + "/mcp"},
		},
		{
			name:        "ipv6 zone",
			raw:         "https://[fe80::1%25eth0]/mcp",
			mustMention: "zone",
			mustOmit:    []string{"eth0", "fe80::1%25eth0", "https://[fe80::1%25eth0]/mcp"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, fieldPath := range []string{"auth.mcp_server_url", "auth.refresh.token_endpoint"} {
				t.Run(fieldPath, func(t *testing.T) {
					_, err := CanonicalizePublicURL(tc.raw, fieldPath)
					if err == nil {
						t.Fatalf("must reject %s", tc.raw)
					}
					if !strings.Contains(err.Error(), fieldPath) {
						t.Fatalf("error %q must name field path %q", err.Error(), fieldPath)
					}
					if !strings.Contains(err.Error(), tc.mustMention) {
						t.Fatalf("error %q must mention %q", err.Error(), tc.mustMention)
					}
					for _, forbidden := range tc.mustOmit {
						if strings.Contains(err.Error(), forbidden) {
							t.Fatalf("error %q must not echo %q", err.Error(), forbidden)
						}
					}
				})
			}
		})
	}
}

func TestCanonicalizePublicURLAcceptsPublicLiterals(t *testing.T) {
	for _, raw := range []string{
		"https://192.0.0.9/mcp",
		"https://[2001:1::1]/mcp",
		"https://[2606:4700::6810:84e5]/mcp",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := CanonicalizePublicURL(raw, "auth.mcp_server_url"); err != nil {
				t.Fatalf("public URL %q must be accepted: %v", raw, err)
			}
		})
	}
}

func TestCanonicalizePublicURLAcceptsValidPublicDNSNames(t *testing.T) {
	for _, raw := range []string{
		"https://example.com/mcp",
		"https://foo-bar.example.com/mcp",
		"https://a2345678901234567890123456789012345678901234567890123456789012.example/mcp",
		"https://Example.COM./mcp",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := CanonicalizePublicURL(raw, "auth.mcp_server_url"); err != nil {
				t.Fatalf("public DNS URL %q must be accepted: %v", raw, err)
			}
		})
	}
}
