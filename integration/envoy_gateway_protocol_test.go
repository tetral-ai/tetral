package integration

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	authservice "github.com/tetral-ai/tetral/services/auth"
)

// This root owns actual translated Envoy protocol behavior and actual Auth Check
// on a role-separated PostgreSQL authority. Controlled HTTP receivers record the
// edge's exact request mutation; SDK/business persistence has its own composition.
func TestEnvoyGatewayTranslatedProtocol(t *testing.T) {
	oidcIsolatedTLSCaseWithMarker(t, "envoy_gateway_assertion=", func(t *testing.T) {
		for _, profile := range []string{"standard-routed", "hardened"} {
			t.Run(profile, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
				t.Cleanup(cancel)
				_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
				authDB := storagetest.OpenWorkloadDB(t, admin, "auth").DB
				key := strings.Repeat("p", auth.MinBootstrapKeyBytes)
				private := transporttest.Must(auth.GenerateEd25519PrivateKeyBase64())
				signer := transporttest.Must(auth.NewInternalPrincipalSignerFromBase64(private))
				handler := transporttest.Must(authservice.BuildRouter(ctx, authservice.RouterBuildConfig{RawDatabase: authDB, Config: authservice.Config{BootstrapAPIKey: key, BootstrapWorkspaceID: workspace.DefaultID, InternalPrincipalPrivateKeyB64: private, InternalPrincipalTTL: time.Minute}}))
				adapter := transporttest.Must(authservice.NewExternalAuthorization(authservice.ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: auth.NewAuthorityResolver(authDB, workspace.DefaultID)}, Signer: signer, PrincipalTTL: time.Minute}))
				ports := envoyGatewayFixturePorts{HTTP: edgeFixturePort(t), HTTPS: edgeFixturePort(t), Ready: edgeFixturePort(t), Admin: edgeFixturePort(t), Stats: edgeFixturePort(t)}
				fixture := translateProductionEnvoyGateway(t, profile, ports)
				joinCheck := startEnvoyGatewayAuthCheck(ctx, t, fixture, profile, adapter)
				var authRequests, apiRequests, eventRequests, gitRequests atomic.Int64
				var lastAuthPath atomic.Pointer[string]
				startEdgeHTTPBackend(ctx, t, fixture, profile, "auth", "127.0.0.2:8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					authRequests.Add(1)
					path := r.URL.Path
					lastAuthPath.Store(&path)
					handler.ServeHTTP(w, r)
				}))
				receiver := func(role string, count *atomic.Int64) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						count.Add(1)
						if role != "git" {
							_, claims, err := signer.Verify(r.Header.Get("X-Tetral-Internal-Principal"), r.Method, r.URL.Path)
							if err != nil {
								http.Error(w, "invalid trusted principal", 500)
								return
							}
							if claims.RequestID != r.Header.Get("X-Request-Id") || claims.ForwardedFor != r.Header.Get("X-Forwarded-For") {
								http.Error(w, "metadata binding differs", 500)
								return
							}
						}
						_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 2<<20))
						safe := map[string]any{"role": role, "path": r.URL.Path, "query": r.URL.RawQuery, "requestID": r.Header.Get("X-Request-Id"), "forwardedFor": r.Header.Get("X-Forwarded-For"), "credentialsRemoved": r.Header.Get("Authorization") == "" && r.Header.Get("X-Api-Key") == "", "unknownRemoved": r.Header.Get("X-Tetral-Future-Authority") == "" && r.Header.Get("X-Original-Future-Metadata") == "", "ticketCount": len(r.Header.Values("X-Tetral-Git-Ticket")), "ticketPreserved": strings.Contains(r.Header.Get("X-Tetral-Git-Ticket"), "fixture-ticket"), "nearTicketRemoved": r.Header.Get("X-Tetral-Git-Ticke") == "" && r.Header.Get("X-Tetral-Git-Ticketx") == ""}
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(safe)
					})
				}
				startEdgeHTTPBackend(ctx, t, fixture, profile, "api", "127.0.0.3:8080", receiver("api", &apiRequests))
				startEdgeHTTPBackend(ctx, t, fixture, profile, "event-stream", "127.0.0.4:8080", receiver("event", &eventRequests))
				startEdgeHTTPBackend(ctx, t, fixture, profile, "git-proxy", "127.0.0.5:8080", receiver("git", &gitRequests))
				startEnvoyGatewayProxy(ctx, t, fixture)
				edgeAwaitReady(ctx, t, ports.Ready)
				roots := x509.NewCertPool()
				roots.AppendCertsFromPEM(fixture.Authority.PEM)
				client := &http.Client{Transport: &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					// Pinned Envoy substitutes its host localAddress for the canonical
					// 127.0.0.1 peer. A known noncanonical loopback source avoids that
					// special case while keeping every socket confined locally.
					connection, err := (&net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.9")}}).DialContext(ctx, network, fmt.Sprintf("127.0.0.1:%d", ports.HTTPS))
					if err != nil {
						return nil, err
					}
					local, ok := connection.LocalAddr().(*net.TCPAddr)
					if !ok || !local.IP.Equal(net.ParseIP("127.0.0.9")) {
						_ = connection.Close()
						return nil, fmt.Errorf("edge fixture connection source differs")
					}
					return connection, nil
				}}, Timeout: 10 * time.Second}
				t.Cleanup(client.CloseIdleConnections)
				request := func(method, host, path, credential string) (*http.Response, map[string]any) {
					r := transporttest.Must(http.NewRequestWithContext(ctx, method, "https://"+host+path, nil))
					if credential != "" {
						r.Header.Set("X-Api-Key", credential)
					}
					r.Header.Set("X-Tetral-Future-Authority", "forged")
					r.Header.Set("X-Original-Future-Metadata", "forged")
					r.Header.Set("X-Request-Id", "forged-id")
					r.Header.Set("X-Forwarded-For", "192.0.2.66")
					response, err := client.Do(r)
					if err != nil {
						t.Fatalf("actual edge HTTP request failed: %v", err)
					}
					defer func() { _ = response.Body.Close() }()
					body := transporttest.Must(io.ReadAll(io.LimitReader(response.Body, 1<<20)))
					var safe map[string]any
					if response.StatusCode != 503 {
						if err := json.Unmarshal(body, &safe); err != nil || safe == nil {
							t.Fatal("actual edge response omitted a valid JSON object")
						}
					}
					return response, safe
				}
				response, safe := request("GET", "api.localhost", "/v1/ping", key)
				if response.StatusCode != 200 || response.ProtoMajor != 2 || safe["role"] != "api" || safe["requestID"] == "forged-id" || safe["credentialsRemoved"] != true || safe["unknownRemoved"] != true || safe["forwardedFor"] != "127.0.0.9" {
					t.Fatalf("actual edge protected/h2/header contract status=%d protocol=%d error_kind=%s api_role=%t fresh_edge_request_id=%t credentials_removed=%t unknown_prefixes_removed=%t single_known_forwarded_peer=%t", response.StatusCode, response.ProtoMajor, edgeSafeErrorKind(safe), safe["role"] == "api", safe["requestID"] != "forged-id", safe["credentialsRemoved"] == true, safe["unknownRemoved"] == true, safe["forwardedFor"] == "127.0.0.9")
				}
				for _, path := range []string{"/v1/sessions/session-fixture/events/stream", "/v1/sessions/session-fixture/threads/thread-fixture/stream"} {
					response, safe = request("GET", "api.localhost", path, key)
					if response.StatusCode != 200 || safe["role"] != "event" {
						t.Fatal("actual production stream URL did not route to Event Stream")
					}
				}
				response, _ = request("GET", "api.localhost", "/v1/api_keys", key)
				if response.StatusCode != 200 {
					t.Fatalf("protected API-key route status=%d", response.StatusCode)
				}
				for _, method := range []string{"GET", "PUT", "PATCH", "DELETE"} {
					response, _ = request(method, "api.localhost", "/v1/oauth/token", "")
					if response.StatusCode != 401 {
						t.Fatalf("near-method OAuth bypass status=%d", response.StatusCode)
					}
				}
				for _, path := range []string{"/v1/oauth/token/", "/v1/oauth/token-extra", "/v1/oauth/Token", "/v1/oauth/token/anything"} {
					response, _ = request("POST", "api.localhost", path, "")
					if response.StatusCode != 401 {
						t.Fatalf("near-path OAuth bypass status=%d", response.StatusCode)
					}
				}
				// Host-only HTTP redirect must not forward a request body or invoke
				// Check. Invalid escaped paths reject before redirecting a POST.
				beforeHTTP := apiRequests.Load() + eventRequests.Load() + authRequests.Load() + gitRequests.Load()
				for _, test := range []struct {
					path string
					want int
				}{{"/v1/ping", 301}, {"/v1/path%2fescape", 400}, {"/v1/path%5cescape", 400}} {
					redirect := edgeRawHTTPRequest(ctx, t, ports.HTTP, "POST", "api.localhost", test.path)
					_ = redirect.Body.Close()
					if redirect.StatusCode != test.want {
						t.Fatalf("actual HTTP path/redirect status=%d want=%d", redirect.StatusCode, test.want)
					}
				}
				if apiRequests.Load()+eventRequests.Load()+authRequests.Load()+gitRequests.Load() != beforeHTTP {
					t.Fatal("HTTP redirect/rejection forwarded to a backend")
				}
				before := authRequests.Load()
				response, _ = request("POST", "api.localhost", "/v1/oauth/token", "")
				if response.StatusCode != 400 || authRequests.Load() != before+1 {
					t.Fatalf("exact POST token exchange bypass status=%d", response.StatusCode)
				}
				beforeAPI := apiRequests.Load()
				response, _ = request("GET", "api.localhost", "/v1/ping", "invalid")
				if response.StatusCode != 401 || apiRequests.Load() != beforeAPI {
					t.Fatal("invalid credential reached a backend")
				}
				// Use actual TLS HTTP/1.1 bytes so duplicate header lines cannot be flattened
				// by a higher-level HTTP client before reaching encode_raw_headers.
				raw := func(method, host, path, headers string) *http.Response {
					return edgeRawTLSRequest(ctx, t, ports.HTTPS, roots, method, host, path, headers)
				}
				// Normalization precedes route selection and Check. These aliases
				// become the exact exchange path and reach only assertion-verifying
				// Auth; paths still near after normalization remain protected above.
				for _, target := range []string{"/v1/oauth/%74oken", "/v1//oauth/token", "/v1/oauth/segment/../token"} {
					checks := edgeAuthCheckCount(ctx, t, ports.Admin)
					authBefore, businessBefore := authRequests.Load(), apiRequests.Load()+eventRequests.Load()
					response = raw("POST", "api.localhost", target, "")
					_ = response.Body.Close()
					checkDelta := edgeAuthCheckCount(ctx, t, ports.Admin) - checks
					authDelta := authRequests.Load() - authBefore
					normalized := "not_dispatched"
					if authDelta != 0 && lastAuthPath.Load() != nil {
						normalized = *lastAuthPath.Load()
					}
					t.Logf("oauth_normalization_observation raw=%s normalized=%s status=%d check_delta=%d auth_http_delta=%d", target, normalized, response.StatusCode, checkDelta, authDelta)
					if apiRequests.Load()+eventRequests.Load() != businessBefore || response.StatusCode != 400 || checkDelta != 0 || authDelta != 1 || normalized != "/v1/oauth/token" {
						t.Fatal("normalized OAuth alias crossed an unexpected authorization or business boundary")
					}
				}
				// Raw targets preserve the distinction between caller spelling and
				// Envoy normalization; the receiver independently verifies binding.
				for _, test := range []struct {
					path, received, query, role string
				}{
					{"/v1/ping?label=a%2Fb&empty=", "/v1/ping", "label=a%2Fb&empty=", "api"},
					{"/v1/p%69ng", "/v1/ping", "", "api"},
					{"/v1/segment/../ping", "/v1/ping", "", "api"},
					{"/v1//ping", "/v1/ping", "", "api"},
					{"/v1/path%252fsegment", "/v1/path%2fsegment", "", "api"},
					{"/v1/sessions/session-fixture/events/stream?after=a%2Fb", "/v1/sessions/session-fixture/events/stream", "after=a%2Fb", "event"},
					{"/v1/sessions/session-fixture/threads/thread-fixture/stream?after=a%2Fb", "/v1/sessions/session-fixture/threads/thread-fixture/stream", "after=a%2Fb", "event"},
					{"/v1/sessions/session-fixture/threads/thread-fixture/events/stream", "/v1/sessions/session-fixture/threads/thread-fixture/events/stream", "", "api"},
				} {
					before := apiRequests.Load() + eventRequests.Load()
					response = raw("GET", "api.localhost", test.path, "X-Api-Key: "+key+"\r\nX-Original-Method: POST\r\nX-Original-URI: /forged\r\nX-Tetral-Internal-Principal: forged\r\n")
					body := transporttest.Must(io.ReadAll(response.Body))
					_ = response.Body.Close()
					receipt := decodeEdgeReceiverReceipt(t, body)
					if response.StatusCode != 200 || receipt["role"] != test.role || receipt["path"] != test.received || receipt["query"] != test.query || apiRequests.Load()+eventRequests.Load() != before+1 {
						t.Fatalf("raw normalized path or independently verified binding differs vector=%s status=%d role_matches=%t path_matches=%t query_matches=%t", test.path, response.StatusCode, receipt["role"] == test.role, receipt["path"] == test.received, receipt["query"] == test.query)
					}
				}
				for _, target := range []string{"/outside", "/internal/health", "/metrics", "/healthz"} {
					before := apiRequests.Load() + eventRequests.Load() + authRequests.Load() + gitRequests.Load()
					response = raw("GET", "api.localhost", target, "X-Api-Key: "+key+"\r\n")
					_ = response.Body.Close()
					if response.StatusCode != 404 || apiRequests.Load()+eventRequests.Load()+authRequests.Load()+gitRequests.Load() != before {
						t.Fatal("unmatched public target reached a backend")
					}
				}
				// Gateway API PathPrefix matches complete slash-delimited elements.
				// api_keys-extra belongs to the ordinary API route; this protocol
				// receiver proves dispatch and binding, not the API's missing-route404.
				beforeAuth := authRequests.Load()
				beforeAPI = apiRequests.Load()
				prefixChecks := edgeAuthCheckCount(ctx, t, ports.Admin)
				response = raw("GET", "api.localhost", "/v1/api_keys-extra", "X-Api-Key: "+key+"\r\n")
				prefixBody := transporttest.Must(io.ReadAll(response.Body))
				_ = response.Body.Close()
				prefixReceipt := decodeEdgeReceiverReceipt(t, prefixBody)
				if response.StatusCode != 200 || prefixReceipt["role"] != "api" || prefixReceipt["path"] != "/v1/api_keys-extra" || authRequests.Load() != beforeAuth || apiRequests.Load() != beforeAPI+1 || edgeAuthCheckCount(ctx, t, ports.Admin) != prefixChecks+1 {
					t.Fatalf("segment-prefix dispatch differs status=%d api_delta=%d auth_http_delta=%d check_delta=%d", response.StatusCode, apiRequests.Load()-beforeAPI, authRequests.Load()-beforeAuth, edgeAuthCheckCount(ctx, t, ports.Admin)-prefixChecks)
				}
				for _, target := range []string{"/v1/bad%", "/v1/bad%xy", "/v1/path%2Fescape", "/v1/path%5Cescape"} {
					before := apiRequests.Load() + eventRequests.Load() + authRequests.Load() + gitRequests.Load()
					response = raw("POST", "api.localhost", target, "X-Api-Key: "+key+"\r\n")
					_ = response.Body.Close()
					if response.StatusCode != 400 || apiRequests.Load()+eventRequests.Load()+authRequests.Load()+gitRequests.Load() != before {
						t.Fatal("malformed raw path reached a backend")
					}
				}
				for _, test := range []struct {
					headers string
					want    int
				}{
					{"X-Api-Key: " + key + "\r\nX-Api-Key: invalid\r\n", 200},
					{"Authorization: Bearer invalid\r\nAuthorization: Bearer other\r\nX-Api-Key: " + key + "\r\n", 200},
					{"X-Api-Key: invalid\r\nX-Api-Key: " + key + "\r\n", 401},
					{"Authorization: Bearer invalid\r\nAuthorization: Bearer other\r\n", 401},
				} {
					before := apiRequests.Load()
					response = raw("GET", "api.localhost", "/v1/ping", test.headers)
					body := transporttest.Must(io.ReadAll(response.Body))
					_ = response.Body.Close()
					if response.StatusCode != test.want {
						t.Fatalf("raw credential precedence status=%d want=%d", response.StatusCode, test.want)
					}
					if test.want == 200 {
						safe := decodeEdgeReceiverReceipt(t, body)
						if safe["role"] != "api" || apiRequests.Load() != before+1 {
							t.Fatal("raw selected credential did not admit exactly one valid backend principal")
						}
					} else if apiRequests.Load() != before {
						t.Fatal("denied raw credential reached a backend")
					}
				}
				response = raw("GET", "api.localhost", "/v1/ping", "X-Api-Key: "+key+"\r\nX-Request-Id: forged-a\r\nX-Request-Id: forged-b\r\n")
				if response.StatusCode != 200 {
					t.Fatalf("fresh duplicate request ID status=%d", response.StatusCode)
				}
				body := transporttest.Must(io.ReadAll(response.Body))
				_ = response.Body.Close()
				safe = decodeEdgeReceiverReceipt(t, body)
				if safe["requestID"] == "forged-a" || safe["requestID"] == "forged-b" || strings.Contains(fmt.Sprint(safe["requestID"]), "forged") {
					t.Fatal("actual edge retained forged caller request ID")
				}
				for _, path := range []string{"/v1/path%2fescape", "/v1/path%5cescape"} {
					response = raw("POST", "api.localhost", path, "X-Api-Key: "+key+"\r\n")
					_ = response.Body.Close()
					if response.StatusCode != 400 {
						t.Fatalf("escaped separator status=%d", response.StatusCode)
					}
				}
				for _, ticketName := range []string{"X-Tetral-Git-Ticket", "x-tetral-git-ticket", "X-TETRAL-GIT-TICKET"} {
					response = raw("GET", "git.localhost", "/repo/info/refs", ticketName+": fixture-ticket\r\nX-Tetral-Git-Ticke: near\r\nX-Tetral-Git-Ticketx: near\r\nX-Tetral-Future-Authority: forged\r\nX-Original-Future-Metadata: forged\r\nAuthorization: Bearer invalid\r\nX-Api-Key: invalid\r\n")
					body = transporttest.Must(io.ReadAll(response.Body))
					_ = response.Body.Close()
					safe = decodeEdgeReceiverReceipt(t, body)
					if response.StatusCode != 200 || safe["role"] != "git" || safe["ticketPreserved"] != true || safe["nearTicketRemoved"] != true || safe["unknownRemoved"] != true || safe["credentialsRemoved"] != true {
						t.Fatalf("Git ticket exception/scrub status=%d", response.StatusCode)
					}
				}
				// Omit only raw header encoding through the official production
				// resource translator. Auth rejects the resulting flattened metadata
				// with its typed400 rather than attempting credential selection.
				negative := translateEdgeWithoutRawHeaders(t, fixture)
				fixture.Control.publish(t, negative)
				beforeAPI, beforeAuth = apiRequests.Load(), authRequests.Load()
				checks := edgeAuthCheckCount(ctx, t, ports.Admin)
				response = raw("GET", "api.localhost", "/v1/ping", "X-Api-Key: "+key+"\r\nX-Api-Key: invalid\r\n")
				body = transporttest.Must(io.ReadAll(response.Body))
				_ = response.Body.Close()
				var denial map[string]any
				if err := json.Unmarshal(body, &denial); err != nil || response.StatusCode != 400 || edgeSafeErrorKind(denial) != "invalid_request_error" || apiRequests.Load() != beforeAPI || authRequests.Load() != beforeAuth || edgeAuthCheckCount(ctx, t, ports.Admin) != checks+1 {
					t.Fatal("omitted raw-header encoding did not fail closed at actual Check metadata boundary")
				}
				fixture.Control.publish(t, fixture.Snapshot)
				checks = edgeAuthCheckCount(ctx, t, ports.Admin)
				response = raw("GET", "api.localhost", "/v1/ping", "X-Api-Key: "+key+"\r\nX-Api-Key: invalid\r\n")
				body = transporttest.Must(io.ReadAll(response.Body))
				_ = response.Body.Close()
				safe = decodeEdgeReceiverReceipt(t, body)
				if response.StatusCode != 200 || safe["role"] != "api" || apiRequests.Load() != beforeAPI+1 || authRequests.Load() != beforeAuth || edgeAuthCheckCount(ctx, t, ports.Admin) != checks+1 {
					t.Fatal("restored exact production raw encoding did not restore selected-first-key admission")
				}
				t.Logf("envoy_gateway_assertion=actual_raw_header_omission_control profile=%s baseline=200 omitted=400 restored=200 passed=true", profile)
				// Dependency failures are the actual Auth domain's typed500, while
				// an unavailable transport is Envoy's separate fail-closed503.
				beforeAPI = apiRequests.Load()
				if err := authDB.Close(); err != nil {
					t.Fatal("close Auth authority role pool")
				}
				response, denied := request("GET", "api.localhost", "/v1/ping", key)
				if response.StatusCode != 500 || edgeSafeErrorKind(denied) != "api_error" || apiRequests.Load() != beforeAPI {
					t.Fatal("actual authority failure did not preserve typed500 without backend forwarding")
				}
				beforeAPI = apiRequests.Load()
				joinCheck()
				response, _ = request("GET", "api.localhost", "/v1/ping", key)
				if response.StatusCode != 503 || apiRequests.Load() != beforeAPI {
					t.Fatalf("unavailable actual Check did not fail closed status=%d", response.StatusCode)
				}
				t.Logf("envoy_gateway_assertion=actual_translated_protocol profile=%s streams=%d git=%d passed=true", profile, eventRequests.Load(), gitRequests.Load())
			})
		}
	})
}

func edgeFixturePort(t *testing.T) int {
	t.Helper()
	listener := transporttest.Must(net.Listen("tcp", "127.0.0.1:0"))
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}
func projectEdgeBackendCredentials(t *testing.T, directory string, fixture *envoyGatewayTranslation, role string) {
	t.Helper()
	leaf := fixture.Leaves[role]
	if err := transporttest.Project(directory, "initial", map[string][]byte{"ca.crt": fixture.Authority.PEM, "tls.crt": leaf.Certificate, "tls.key": leaf.Key}); err != nil {
		t.Fatal(err)
	}
}
func startEdgeHTTPBackend(ctx context.Context, t *testing.T, fixture *envoyGatewayTranslation, profile, role, address string, handler http.Handler) {
	t.Helper()
	listener := transporttest.Must(net.Listen("tcp", address))
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	var owner *transportsecurity.Owner
	if profile == "hardened" {
		directory := filepath.Join(fixture.Directory, role+"-http")
		projectEdgeBackendCredentials(t, directory, fixture, role)
		cfg := transportsecurity.HTTPConfig{Mode: "native-mtls", CAPath: filepath.Join(directory, "ca.crt"), CertPath: filepath.Join(directory, "tls.crt"), KeyPath: filepath.Join(directory, "tls.key"), EdgeClientURI: "spiffe://cluster.local/ns/envoy-gateway-system/sa/tetral-public-edge"}
		var err error
		owner, server.TLSConfig, err = cfg.Open(ctx)
		if err != nil {
			_ = listener.Close()
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		if owner == nil {
			done <- server.Serve(listener)
		} else {
			done <- server.ServeTLS(listener, "", "")
		}
	}()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-done; err != nil && err != http.ErrServerClosed {
			t.Error("actual edge backend did not join")
		}
		if owner != nil {
			if err := owner.Close(); err != nil {
				t.Error("native HTTP credentials did not join")
			}
		}
	})
}
func edgeAwaitReady(ctx context.Context, t *testing.T, port int) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	lastStatus := 0
	requests := 0
	ready, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	for {
		r := transporttest.Must(http.NewRequestWithContext(ready, "GET", fmt.Sprintf("http://127.0.0.1:%d/ready", port), nil))
		response, err := client.Do(r)
		if err == nil {
			_ = response.Body.Close()
			requests++
			lastStatus = response.StatusCode
			if response.StatusCode == 200 {
				return
			}
		}
		select {
		case <-ready.Done():
			t.Fatalf("actual Envoy did not acknowledge ready policy by its deadline: responses=%d last_status=%d", requests, lastStatus)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func edgeRawTLSRequest(ctx context.Context, t *testing.T, port int, roots *x509.CertPool, method, host, path, headers string) *http.Response {
	t.Helper()
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: &tls.Config{RootCAs: roots, ServerName: host, MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}}}
	connection, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("raw public TLS connection failed: %v", err)
	}
	_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := fmt.Fprintf(connection, "%s %s HTTP/1.1\r\nHost: %s\r\n%sConnection: close\r\nContent-Length: 0\r\n\r\n", method, path, host, headers); err != nil {
		t.Fatal("write raw public TLS request")
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatalf("read raw public TLS response failed: %v", err)
	}
	return response
}

func decodeEdgeReceiverReceipt(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil || result == nil || result["role"] == nil {
		t.Fatal("actual receiver omitted a valid fresh mutation receipt")
	}
	return result
}

func startEnvoyGatewayAuthCheck(ctx context.Context, t *testing.T, fixture *envoyGatewayTranslation, profile string, adapter *authservice.ExternalAuthorization) func() {
	t.Helper()
	cfg := authservice.Config{GRPCAddress: "127.0.0.2:9095", GRPCTransport: "plaintext"}
	if profile == "hardened" {
		directory := filepath.Join(fixture.Directory, "auth-check")
		projectEdgeBackendCredentials(t, directory, fixture, "auth")
		cfg.GRPCTransport = "native-mtls"
		cfg.GRPCTLSCAPath = filepath.Join(directory, "ca.crt")
		cfg.GRPCTLSCertPath = filepath.Join(directory, "tls.crt")
		cfg.GRPCTLSKeyPath = filepath.Join(directory, "tls.key")
		cfg.GRPCTLSEdgeClientURI = "spiffe://cluster.local/ns/envoy-gateway-system/sa/tetral-public-edge"
	}
	check := transporttest.Must(authservice.OpenExternalAuthorizationServer(ctx, cfg, adapter, nil, workload.NewOperationMetrics("auth")))
	checkCtx, stopCheck := context.WithCancel(ctx)
	checkJoined := make(chan error, 1)
	go func() { checkJoined <- check.Run(checkCtx, nil) }()
	checkStopped := false
	joinCheck := func() {
		if checkStopped {
			return
		}
		checkStopped = true
		stopCheck()
		select {
		case err := <-checkJoined:
			if err != nil {
				t.Error("actual Auth Check did not join")
			}
		case <-time.After(15 * time.Second):
			t.Error("actual Auth Check join exceeded owner bound")
		}
		if err := check.Close(); err != nil {
			t.Error("actual Auth Check owner close failed")
		}
	}
	t.Cleanup(joinCheck)
	return joinCheck
}

func startEnvoyGatewayProxy(ctx context.Context, t *testing.T, fixture *envoyGatewayTranslation) {
	t.Helper()
	// Each process generation owns its bootstrap/control credentials. Preserve
	// the separate translation and native backend directory across restarts.
	processDirectory := t.TempDir()
	controlTLS := writeEnvoyControlSDS(t, processDirectory)
	// Keep both exact loopback families available for the unmodified upstream
	// DNS endpoint. Fail occupied ports; never bind wildcard or replace owners.
	xdsListeners := make([]net.Listener, 0, 2)
	for _, address := range []string{"127.0.0.1:18000", "[::1]:18000"} {
		listener := transporttest.Must(net.Listen("tcp", address))
		t.Cleanup(func() { _ = listener.Close() })
		xdsListeners = append(xdsListeners, listener)
	}
	fixture.Control = serveTranslatedEnvoy(ctx, t, fixture.Snapshot, xdsListeners, controlTLS, processDirectory, "tetral-local-official-snapshot", fixture.ProcessDrainArgs...)
}

func edgeSafeErrorKind(body map[string]any) string {
	detail, _ := body["error"].(map[string]any)
	kind, _ := detail["type"].(string)
	switch kind {
	case "api_error", "invalid_request_error", "authentication_error":
		return kind
	}
	return "other_or_absent"
}

func edgeRawHTTPRequest(ctx context.Context, t *testing.T, port int, method, host, path string) *http.Response {
	t.Helper()
	connection, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal("connect local HTTP edge")
	}
	t.Cleanup(func() { _ = connection.Close() })
	_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := fmt.Fprintf(connection, "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nContent-Length: 0\r\n\r\n", method, path, host); err != nil {
		t.Fatal("write local HTTP edge request")
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatal("read local HTTP edge response")
	}
	return response
}

// Sum only outcomes from the actual API ext_authz filter. Disabled/bypass and
// backend transport counters are excluded; all values are bounded numeric data.
func edgeAuthCheckCount(ctx context.Context, t *testing.T, port int) int64 {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{}, Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	request := transporttest.Must(http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d/stats?format=json&filter=ext_authz", port), nil))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("read actual external-authorization outcome counters")
	}
	defer func() { _ = response.Body.Close() }()
	var payload struct {
		Stats []struct {
			Name  string      `json:"name"`
			Value json.Number `json:"value"`
		} `json:"stats"`
	}
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload) != nil {
		t.Fatal("invalid external-authorization counter response")
	}
	var total int64
	matched := 0
	for _, stat := range payload.Stats {
		if !strings.HasPrefix(stat.Name, "http.") || !strings.Contains(stat.Name, ".ext_authz.") {
			continue
		}
		if !strings.HasSuffix(stat.Name, ".ok") && !strings.HasSuffix(stat.Name, ".denied") && !strings.HasSuffix(stat.Name, ".error") {
			continue
		}
		value, err := stat.Value.Int64()
		if err != nil || value < 0 {
			t.Fatal("invalid external-authorization outcome value")
		}
		total += value
		matched++
	}
	if matched == 0 {
		t.Fatal("actual ext_authz filter omitted owning outcome counters")
	}
	return total
}
