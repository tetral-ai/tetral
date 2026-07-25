package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type validationResolverFunc func(context.Context, string, string) ([]net.IPAddr, error)

func (f validationResolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, "ip", host)
}

type validationTestConn struct {
	remote net.Addr
	writes atomic.Int64
	closed atomic.Bool
}

func (c *validationTestConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *validationTestConn) Write(p []byte) (int, error) {
	c.writes.Add(int64(len(p)))
	return len(p), nil
}
func (c *validationTestConn) Close() error                     { c.closed.Store(true); return nil }
func (c *validationTestConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *validationTestConn) RemoteAddr() net.Addr             { return c.remote }
func (c *validationTestConn) SetDeadline(time.Time) error      { return nil }
func (c *validationTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *validationTestConn) SetWriteDeadline(time.Time) error { return nil }

func TestHTTPMCPOAuthValidatorRejectsReboundPrivateDestinationBeforeSecretWrite(t *testing.T) {
	var resolverCalls int
	var dialCalls int
	resolver := validationResolverFunc(func(_ context.Context, _, host string) ([]net.IPAddr, error) {
		resolverCalls++
		if host != "rebind.example" {
			t.Fatalf("resolver host = %q", host)
		}
		if resolverCalls == 1 {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	})
	validator := HTTPMCPOAuthValidator{
		Resolver: resolver,
		DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			dialCalls++
			if address != "93.184.216.34:443" {
				t.Fatalf("dial address = %q; want pinned public address", address)
			}
			return nil, errors.New("test dial stopped before TLS")
		},
	}

	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_rebind", VaultID: "vlt_rebind"},
		Auth:       CredentialAuth{Type: credentialAuthTypeMCPOAuth, MCPServerURL: "https://rebind.example/mcp", AccessToken: "rebind-access-sentinel"},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if result.Validation.Status != "unknown" || resolverCalls != 1 || dialCalls != 1 {
		t.Fatalf("status=%s resolver calls=%d dial calls=%d; want unknown,1,1", result.Validation.Status, resolverCalls, dialCalls)
	}
}

func TestHTTPMCPOAuthValidatorRejectsDisallowedDestinationClassesBeforeDial(t *testing.T) {
	tests := []struct {
		name string
		host string
		ips  []string
	}{
		{name: "ipv4 loopback", host: "blocked.example", ips: []string{"127.0.0.1"}},
		{name: "ipv4 private", host: "blocked.example", ips: []string{"10.1.2.3"}},
		{name: "ipv4 link local", host: "blocked.example", ips: []string{"169.254.169.254"}},
		{name: "ipv4 unspecified", host: "blocked.example", ips: []string{"0.0.0.0"}},
		{name: "ipv4 multicast", host: "blocked.example", ips: []string{"224.0.0.1"}},
		{name: "ipv4 carrier grade NAT", host: "blocked.example", ips: []string{"100.64.0.1"}},
		{name: "ipv4 documentation", host: "blocked.example", ips: []string{"192.0.2.1"}},
		{name: "ipv6 loopback", host: "blocked.example", ips: []string{"::1"}},
		{name: "ipv6 private", host: "blocked.example", ips: []string{"fd00::1"}},
		{name: "ipv6 link local", host: "blocked.example", ips: []string{"fe80::1"}},
		{name: "ipv6 unspecified", host: "blocked.example", ips: []string{"::"}},
		{name: "ipv6 multicast", host: "blocked.example", ips: []string{"ff02::1"}},
		{name: "ipv6 documentation", host: "blocked.example", ips: []string{"2001:db8::1"}},
		{name: "ipv6 documentation 3fff", host: "blocked.example", ips: []string{"3fff::1"}},
		{name: "mixed public private", host: "mixed.example", ips: []string{"93.184.216.34", "10.0.0.7"}},
		{name: "cluster service host", host: "kubernetes.default.svc", ips: []string{"93.184.216.34"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dialCalls int
			resolver := validationResolverFunc(func(context.Context, string, string) ([]net.IPAddr, error) {
				addresses := make([]net.IPAddr, 0, len(test.ips))
				for _, raw := range test.ips {
					addresses = append(addresses, net.IPAddr{IP: net.ParseIP(raw)})
				}
				return addresses, nil
			})
			validator := HTTPMCPOAuthValidator{
				Resolver: resolver,
				DialContext: func(context.Context, string, string) (net.Conn, error) {
					dialCalls++
					return nil, errors.New("unexpected dial")
				},
			}
			result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
				Credential: CredentialMetadata{ID: "cred_blocked", VaultID: "vlt_blocked"},
				Auth:       CredentialAuth{Type: credentialAuthTypeMCPOAuth, MCPServerURL: "https://" + test.host + "/mcp", AccessToken: "blocked-access-sentinel"},
			})
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if (result.Validation.Status != "unknown" && result.Validation.Status != "invalid") || dialCalls != 0 {
				t.Fatalf("status=%s dial calls=%d; want safe failure and 0 dials", result.Validation.Status, dialCalls)
			}
		})
	}
}

func TestHTTPMCPOAuthValidatorRejectsUnsafeURLsBeforeDial(t *testing.T) {
	for _, rawURL := range []string{
		"http://example.com/mcp",
		"https://user:password@example.com/mcp",
		"https://example.com:0/mcp",
		"https://example.com:65536/mcp",
		"https://example.com:invalid/mcp",
		"https://[::1/mcp",
		"https://service.default.svc/mcp",
	} {
		t.Run(rawURL, func(t *testing.T) {
			var resolverCalls, dialCalls int
			validator := HTTPMCPOAuthValidator{
				Resolver: validationResolverFunc(func(context.Context, string, string) ([]net.IPAddr, error) {
					resolverCalls++
					return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
				}),
				DialContext: func(context.Context, string, string) (net.Conn, error) {
					dialCalls++
					return nil, errors.New("unexpected dial")
				},
			}
			result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
				Credential: CredentialMetadata{ID: "cred_unsafe_url", VaultID: "vlt_unsafe_url"},
				Auth: CredentialAuth{
					Type:         credentialAuthTypeMCPOAuth,
					MCPServerURL: rawURL,
					AccessToken:  "unsafe-url-access-sentinel",
					ExpiresAt:    "2000-01-01T00:00:00Z",
					Refresh: &CredentialOAuthRefresh{
						ClientID:      "public-client",
						RefreshToken:  "unsafe-url-refresh-sentinel",
						TokenEndpoint: rawURL,
						TokenEndpointAuth: &CredentialTokenEndpointAuth{
							Type: "none",
						},
					},
				},
			})
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if result.Validation.Status == "valid" || resolverCalls != 0 || dialCalls != 0 {
				t.Fatalf("status=%s resolver=%d dial=%d; want fail closed before resolution", result.Validation.Status, resolverCalls, dialCalls)
			}
		})
	}
}

func TestHTTPMCPOAuthValidatorRejectsConnectedAddressMismatchBeforeTLSBytes(t *testing.T) {
	connection := &validationTestConn{remote: &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 443}}
	validator := HTTPMCPOAuthValidator{
		Resolver: validationResolverFunc(func(context.Context, string, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		}),
		DialContext: func(context.Context, string, string) (net.Conn, error) { return connection, nil },
	}
	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_mismatch", VaultID: "vlt_mismatch"},
		Auth:       CredentialAuth{Type: credentialAuthTypeMCPOAuth, MCPServerURL: "https://mismatch.example/mcp", AccessToken: "mismatch-access-sentinel"},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if result.Validation.Status != "unknown" || connection.writes.Load() != 0 || !connection.closed.Load() {
		t.Fatalf("status=%s writes=%d closed=%v; want unknown,0,true", result.Validation.Status, connection.writes.Load(), connection.closed.Load())
	}
}

func TestHTTPMCPOAuthValidatorIgnoresUnvalidatedProxyEnvironment(t *testing.T) {
	for _, proxyVariable := range []string{"HTTP_PROXY", "HTTPS_PROXY"} {
		t.Run(proxyVariable, func(t *testing.T) {
			var proxyDials, proxyRequests, proxyApplicationBytes atomic.Int64
			proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				proxyRequests.Add(1)
				body, _ := io.ReadAll(request.Body)
				proxyApplicationBytes.Add(int64(len(body)))
				w.WriteHeader(http.StatusBadGateway)
			}))
			proxy.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					proxyDials.Add(1)
				}
			}
			proxy.Start()
			defer proxy.Close()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":"validation","result":{}}`)
			}))
			defer server.Close()
			trustValidationTestServer(t, server)
			t.Setenv(proxyVariable, proxy.URL)
			t.Setenv("NO_PROXY", "")

			var resolverCalls, pinnedDialCalls atomic.Int64
			validator := HTTPMCPOAuthValidator{
				Resolver: validationResolverFunc(func(_ context.Context, _, host string) ([]net.IPAddr, error) {
					resolverCalls.Add(1)
					if host != "example.com" {
						return nil, errors.New("proxy host resolution attempted")
					}
					return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
				}),
				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					if address != "93.184.216.34:443" {
						return nil, errors.New("proxy dial attempted")
					}
					pinnedDialCalls.Add(1)
					conn, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
					if err != nil {
						return nil, err
					}
					return &validationRemoteAddrConn{Conn: conn, remote: &net.TCPAddr{IP: net.ParseIP("93.184.216.34"), Port: 443}}, nil
				},
			}
			result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
				Credential: CredentialMetadata{ID: "cred_proxy", VaultID: "vlt_proxy"},
				Auth:       CredentialAuth{Type: credentialAuthTypeMCPOAuth, MCPServerURL: "https://example.com/mcp", AccessToken: "proxy-access-sentinel"},
			})
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if result.Validation.Status != "valid" || resolverCalls.Load() != 1 || pinnedDialCalls.Load() != 1 ||
				proxyDials.Load() != 0 || proxyRequests.Load() != 0 || proxyApplicationBytes.Load() != 0 {
				t.Fatalf("status=%s resolver=%d pinned=%d proxy dial/request/bytes=%d/%d/%d",
					result.Validation.Status, resolverCalls.Load(), pinnedDialCalls.Load(), proxyDials.Load(), proxyRequests.Load(), proxyApplicationBytes.Load())
			}
		})
	}
}

func TestHTTPMCPOAuthValidatorDoesNotFollowRedirectOrExposeLocation(t *testing.T) {
	const sentinel = "redirect-location-secret-sentinel"
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Location", "https://127.0.0.1/"+sentinel)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	trustValidationTestServer(t, server)
	validator := pinnedValidationTestValidator(t, server)
	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_redirect", VaultID: "vlt_redirect"},
		Auth:       CredentialAuth{Type: credentialAuthTypeMCPOAuth, MCPServerURL: "https://example.com/mcp", AccessToken: "redirect-access-sentinel"},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	serialized := mustMarshalValidation(t, result.Validation)
	if requests.Load() != 1 || bytes.Contains(serialized, []byte(sentinel)) {
		t.Fatalf("requests=%d validation=%s; want one request and no Location sentinel", requests.Load(), serialized)
	}
}

func TestCredentialValidationDiagnosticsScrubBeforePublicBounding(t *testing.T) {
	secret := "boundary-secret-sentinel"
	prefix := strings.Repeat("x", mcpOAuthValidationBodyCap-len(secret)/2)
	response := &http.Response{
		StatusCode: http.StatusBadGateway,
		Header:     http.Header{"Content-Type": []string{"text/" + secret + "; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(prefix + secret + strings.Repeat("z", 64))),
	}
	httpResponse, _, err := readCredentialValidationHTTPResponse(response, credentialValidationRedactionCorpus(CredentialAuth{AccessToken: secret}, nil))
	if err != nil {
		t.Fatalf("readCredentialValidationHTTPResponse: %v", err)
	}
	serialized := httpResponse.Body + httpResponse.ContentType
	if strings.Contains(serialized, secret) || strings.Contains(serialized, secret[:len(secret)/2]) {
		t.Fatalf("bounded diagnostic leaked secret material at the old cap boundary")
	}
	if len(httpResponse.Body) > mcpOAuthValidationBodyCap || !httpResponse.BodyTruncated {
		t.Fatalf("body bytes=%d truncated=%v; want <=%d,true", len(httpResponse.Body), httpResponse.BodyTruncated, mcpOAuthValidationBodyCap)
	}
}

func TestHTTPMCPOAuthValidatorScrubsStoredAndNewlyIssuedSecretCorpus(t *testing.T) {
	const (
		oldAccess    = "old-access-secret-sentinel"
		oldRefresh   = "old-refresh-secret-sentinel"
		clientID     = "public-client"
		clientSecret = "client-secret-sentinel"
		staticToken  = "static-token-secret-sentinel"
		newAccess    = "new-access-secret-sentinel"
		newRefresh   = "new-refresh-secret-sentinel"
	)
	storedAuth := CredentialAuth{
		Type:         credentialAuthTypeMCPOAuth,
		MCPServerURL: "https://example.com/mcp",
		AccessToken:  oldAccess,
		Token:        staticToken,
		ExpiresAt:    "2000-01-01T00:00:00Z",
		Refresh: &CredentialOAuthRefresh{
			ClientID:      clientID,
			RefreshToken:  oldRefresh,
			TokenEndpoint: "https://example.com/token",
			TokenEndpointAuth: &CredentialTokenEndpointAuth{
				Type:         "client_secret_basic",
				ClientSecret: clientSecret,
			},
		},
	}
	allCorpus := credentialValidationRedactionCorpus(storedAuth, []string{newAccess, newRefresh})
	reflected := strings.Join(allCorpus, "|")
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/"+oldAccess+"; reflected="+newAccess)
		switch request.URL.Path {
		case "/token":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"access_token":  newAccess,
				"refresh_token": newRefresh,
				"expires_in":    3600,
				"benign_note":   reflected,
			})
		case "/mcp":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      "validation",
				"result":  map[string]any{},
				"note":    reflected,
			})
		default:
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
	}))
	defer server.Close()
	trustValidationTestServer(t, server)
	validator := pinnedValidationTestValidator(t, server)
	validator.Now = func() time.Time { return time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC) }
	var result MCPOAuthValidationResult
	capturedLogs, err := captureValidationLogs(func() error {
		var validationErr error
		result, validationErr = validator.Validate(context.Background(), MCPOAuthValidationInput{
			Credential: CredentialMetadata{ID: "cred_corpus", VaultID: "vlt_corpus"},
			Auth:       storedAuth,
		})
		return validationErr
	})
	if err != nil {
		assertValidationOutputsExclude(t, allCorpus, map[string]string{
			"captured logs":  capturedLogs,
			"returned error": validationErrorText(err),
		})
		t.Fatalf("Validate: %v", err)
	}
	serialized := string(mustMarshalValidation(t, result.Validation))
	assertValidationOutputsExclude(t, allCorpus, map[string]string{
		"serialized DTO": serialized,
		"captured logs":  capturedLogs,
		"returned error": validationErrorText(err),
	})
	assertValidationOutputsExclude(t, []string{oldAccess, oldRefresh, clientSecret, staticToken, newAccess, newRefresh}, map[string]string{
		"serialized DTO": serialized,
		"captured logs":  capturedLogs,
		"returned error": validationErrorText(err),
	})
	for _, check := range []*CredentialValidationCheck{result.Validation.Refresh, result.Validation.MCPProbe} {
		if check == nil || check.HTTPResponse == nil || check.HTTPResponse.ContentType != "" {
			t.Fatalf("malicious Content-Type survived: %+v", check)
		}
	}
}

func TestHTTPMCPOAuthValidatorOmitsTruncatedUnknownIssuedToken(t *testing.T) {
	const issuedPrefix = "oversized-new-token-secret-prefix"
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/"+issuedPrefix)
		if request.URL.Path == "/token" {
			_, _ = io.WriteString(writer, `{"access_token":"`+issuedPrefix+strings.Repeat("x", mcpOAuthValidationBodyCap*2))
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":"validation","result":{}}`)
	}))
	defer server.Close()
	trustValidationTestServer(t, server)
	validator := pinnedValidationTestValidator(t, server)
	validator.Now = func() time.Time { return time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC) }
	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_oversized", VaultID: "vlt_oversized"},
		Auth: CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://example.com/mcp",
			AccessToken:  "stored-access",
			ExpiresAt:    "2000-01-01T00:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				ClientID:          "public-client",
				RefreshToken:      "stored-refresh",
				TokenEndpoint:     "https://example.com/token",
				TokenEndpointAuth: &CredentialTokenEndpointAuth{Type: "none"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	serialized := mustMarshalValidation(t, result.Validation)
	if bytes.Contains(serialized, []byte(issuedPrefix)) {
		t.Fatalf("truncated issuer diagnostic leaked an unknown issued-token prefix: %s", serialized)
	}
	if result.Validation.Refresh == nil || result.Validation.Refresh.HTTPResponse == nil ||
		!result.Validation.Refresh.HTTPResponse.BodyTruncated || result.Validation.Refresh.HTTPResponse.Body != "" || result.Validation.Refresh.HTTPResponse.ContentType != "" {
		t.Fatalf("truncated issuer diagnostic = %+v; want omitted body/header with truncation marker", result.Validation.Refresh)
	}
}

type boundedCountingBody struct {
	payload  []byte
	consumed int
	max      int
}

func (b *boundedCountingBody) Read(p []byte) (int, error) {
	if b.consumed >= b.max {
		return 0, errors.New("body over-read")
	}
	if b.consumed == len(b.payload) {
		return 0, io.EOF
	}
	n := len(p)
	if n > len(b.payload)-b.consumed {
		n = len(b.payload) - b.consumed
	}
	if n > b.max-b.consumed {
		n = b.max - b.consumed
	}
	copy(p[:n], b.payload[b.consumed:b.consumed+n])
	b.consumed += n
	return n, nil
}

func (b *boundedCountingBody) Close() error { return nil }

func TestCredentialValidationDiagnosticReadUsesCorpusOverlapBound(t *testing.T) {
	secret := strings.Repeat("s", 257)
	staticToken := "counting-static-token-secret-sentinel"
	corpus := credentialValidationRedactionCorpus(CredentialAuth{AccessToken: secret, Token: staticToken}, nil)
	maximum := 0
	for _, entry := range corpus {
		if len(entry) > maximum {
			maximum = len(entry)
		}
	}
	limit := mcpOAuthValidationBodyCap + maximum + 1
	prefixLength := mcpOAuthValidationBodyCap - len(staticToken) - 1 - len(secret)/2
	payload := staticToken + "|" + strings.Repeat("x", prefixLength) + secret + strings.Repeat("z", maximum+4096)
	body := &boundedCountingBody{payload: []byte(payload), max: limit}
	response := &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header), Body: body}
	var diagnostic *CredentialValidationHTTPResponse
	capturedLogs, err := captureValidationLogs(func() error {
		var readErr error
		diagnostic, _, readErr = readCredentialValidationHTTPResponse(response, corpus)
		return readErr
	})
	if body.consumed != limit || !diagnostic.BodyTruncated || len(diagnostic.Body) > mcpOAuthValidationBodyCap {
		t.Fatalf("consumed=%d limit=%d truncated=%v body=%d", body.consumed, limit, diagnostic.BodyTruncated, len(diagnostic.Body))
	}
	serialized, marshalErr := json.Marshal(diagnostic)
	if marshalErr != nil {
		t.Fatalf("marshal bounded diagnostic: %v", marshalErr)
	}
	assertValidationOutputsExclude(t, append(corpus, secret[:len(secret)/2], secret[len(secret)/2:]), map[string]string{
		"serialized DTO": string(serialized),
		"captured logs":  capturedLogs,
		"returned error": validationErrorText(err),
	})
	if err != nil {
		t.Fatalf("bounded diagnostic read: %v", err)
	}
}

type validationRemoteAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c *validationRemoteAddrConn) RemoteAddr() net.Addr { return c.remote }

func pinnedValidationTestValidator(t *testing.T, server *httptest.Server) HTTPMCPOAuthValidator {
	t.Helper()
	return HTTPMCPOAuthValidator{
		Resolver: validationResolverFunc(func(context.Context, string, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		}),
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			if err != nil {
				return nil, err
			}
			return &validationRemoteAddrConn{Conn: conn, remote: &net.TCPAddr{IP: net.ParseIP("93.184.216.34"), Port: 443}}, nil
		},
	}
}

func trustValidationTestServer(t *testing.T, server *httptest.Server) {
	t.Helper()
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]})
	path := filepath.Join(t.TempDir(), "validation-test-root.pem")
	if err := os.WriteFile(path, certificate, 0o600); err != nil {
		t.Fatalf("write validation test root: %v", err)
	}
	t.Setenv("SSL_CERT_FILE", path)
}

func mustMarshalValidation(t *testing.T, validation *CredentialValidation) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(validation); err != nil {
		t.Fatalf("encode validation: %v", err)
	}
	return buffer.Bytes()
}

func captureValidationLogs(run func() error) (string, error) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	err := run()
	return logs.String(), err
}

func validationErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func assertValidationOutputsExclude(t *testing.T, forbidden []string, channels map[string]string) {
	t.Helper()
	for _, value := range forbidden {
		if value == "" {
			continue
		}
		for channel, output := range channels {
			if strings.Contains(output, value) {
				t.Fatalf("%s leaked credential representation %q", channel, value)
			}
		}
	}
}
