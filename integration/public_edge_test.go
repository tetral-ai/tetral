// Layer 1: the tests' public edge, with in-process stand-ins for the services
// behind it.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"

	"github.com/tetral-ai/tetral/internal/auth"
)

const (
	publicEdgeVaultKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

type publicEdgeEnv map[string]string

func (env publicEdgeEnv) Getenv(name string) string {
	return env[name]
}

type publicEdge struct {
	client          *http.Client
	authGRPCAddress string
	authBaseURL     *url.URL
	apiBaseURL      *url.URL
	eventBaseURL    *url.URL
	requestIDPrefix string

	mu        sync.Mutex
	requestID uint64
}

func newPublicEdge(authBaseURL string, apiBaseURL string, eventBaseURL string, authGRPCAddress string) (http.Handler, error) {
	authURL, err := url.Parse(authBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse auth base URL: %w", err)
	}
	apiURL, err := url.Parse(apiBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse API base URL: %w", err)
	}
	eventURL, err := url.Parse(eventBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse event-stream base URL: %w", err)
	}
	return &publicEdge{
		client:          http.DefaultClient,
		authGRPCAddress: authGRPCAddress,
		authBaseURL:     authURL,
		apiBaseURL:      apiURL,
		eventBaseURL:    eventURL,
		requestIDPrefix: "req_public_edge_",
	}, nil
}

func (edge *publicEdge) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/v1" && !strings.HasPrefix(request.URL.Path, "/v1/") {
		http.NotFound(w, request)
		return
	}

	edge.mu.Lock()
	edge.requestID++
	requestID := fmt.Sprintf("%s%d", edge.requestIDPrefix, edge.requestID)
	edge.mu.Unlock()

	principal, ok := edge.authorize(w, request, requestID)
	if !ok {
		return
	}
	target := edge.apiBaseURL
	switch {
	case isPublicEdgeAPIKeyPath(request.URL.Path):
		target = edge.authBaseURL
	case isPublicEdgeStreamPath(request.URL.Path):
		target = edge.eventBaseURL
	}

	outbound := request.Clone(request.Context())
	outbound.URL.Scheme = target.Scheme
	outbound.URL.Host = target.Host
	outbound.Host = target.Host
	outbound.RequestURI = ""
	outbound.Header = request.Header.Clone()
	stripPublicEdgeClientHeaders(outbound.Header)
	outbound.Header.Set("X-Tetral-Internal-Principal", principal)
	outbound.Header.Set("X-Request-Id", requestID)

	//nolint:gosec // G704: the URL is this test's own in-process edge proxy target, not caller input.
	response, err := edge.client.Do(outbound)
	if err != nil {
		http.Error(w, "upstream connect error or disconnect/reset before headers", http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = response.Body.Close() }()
	copyPublicEdgeResponseHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		// This test edge must forward live SSE as it arrives. Buffered io.Copy
		// only happened to work for older fixtures that immediately closed.
		controller := http.NewResponseController(w)
		if controller.Flush() != nil {
			return
		}
		_, _ = io.Copy(publicEdgeFlushWriter{writer: w, controller: controller}, response.Body)
		return
	}
	_, _ = io.Copy(w, response.Body)
}

type publicEdgeFlushWriter struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
}

func (w publicEdgeFlushWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err == nil {
		err = w.controller.Flush()
	}
	return n, err
}

func (edge *publicEdge) authorize(w http.ResponseWriter, request *http.Request, requestID string) (string, bool) {
	headers := request.Header.Clone()
	stripPublicEdgeUntrustedMetadata(headers)
	response, err := directEdgeCheck(request.Context(), edge.authGRPCAddress, request.Method, request.URL.RequestURI(), requestID, headers)
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return "", false
	}
	status, principal := directEdgeCheckStatus(response)
	if status != http.StatusOK {
		forwardFixtureCheckDenial(w, response)
		return "", false
	}
	if principal == "" {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return "", false
	}
	return principal, true
}

func stripPublicEdgeUntrustedMetadata(header http.Header) {
	for name := range header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-tetral-") || strings.HasPrefix(lower, "x-original-") || lower == "x-request-id" || lower == "x-forwarded-for" || lower == "forwarded" || lower == "x-real-ip" || lower == "x-envoy-external-address" {
			header.Del(name)
		}
	}
}

func isPublicEdgeAPIKeyPath(path string) bool {
	return path == "/v1/api_keys" || strings.HasPrefix(path, "/v1/api_keys/")
}

func isPublicEdgeStreamPath(path string) bool {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 5 {
		return parts[0] == "v1" &&
			parts[1] == "sessions" &&
			parts[2] != "" &&
			parts[3] == "events" &&
			parts[4] == "stream"
	}
	return len(parts) == 6 &&
		parts[0] == "v1" &&
		parts[1] == "sessions" &&
		parts[2] != "" &&
		parts[3] == "threads" &&
		parts[4] != "" &&
		parts[5] == "stream"
}

func stripPublicEdgeClientHeaders(header http.Header) {
	for name := range header {
		lower := strings.ToLower(name)
		if lower == "authorization" || lower == "x-api-key" || strings.HasPrefix(lower, "x-original-") || strings.HasPrefix(lower, "x-tetral-") {
			header.Del(name)
		}
	}
}

func copyPublicEdgeResponseHeaders(destination http.Header, source http.Header) {
	for name, values := range source {
		if strings.HasPrefix(strings.ToLower(name), "x-tetral-") {
			continue
		}
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}

func TestPublicEdgeAuthenticatesSanitizesAndRoutesPublicRequests(t *testing.T) {
	t.Parallel()

	type observation struct {
		path      string
		apiKey    string
		principal string
		forged    string
	}
	var (
		mu           sync.Mutex
		observations = map[string]observation{}
	)
	upstream := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			mu.Lock()
			observations[name] = observation{
				path:      request.URL.Path,
				apiKey:    request.Header.Get("X-Api-Key"),
				principal: request.Header.Get("X-Tetral-Internal-Principal"),
				forged:    request.Header.Get("X-Tetral-Forged"),
			}
			mu.Unlock()
			_, _ = io.WriteString(w, name)
		}))
	}

	apiServer := upstream("api")
	defer apiServer.Close()
	eventServer := upstream("event")
	defer eventServer.Close()
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {

		mu.Lock()
		observations["auth"] = observation{
			path:      request.URL.Path,
			apiKey:    request.Header.Get("X-Api-Key"),
			principal: request.Header.Get("X-Tetral-Internal-Principal"),
			forged:    request.Header.Get("X-Tetral-Forged"),
		}
		mu.Unlock()
		_, _ = io.WriteString(w, "auth")
	}))
	defer authServer.Close()

	checkAddress := startCheckFixture(t, checkFixture{check: func(_ context.Context, request *authv3.CheckRequest) (*authv3.CheckResponse, error) {
		headers := fixtureCheckHeaders(request)
		if headers.Get("X-Api-Key") != "test-key" {
			t.Error("authorization key was not preserved")
		}
		if request.GetAttributes().GetRequest().GetHttp().GetPath() == "" || request.GetAttributes().GetRequest().GetHttp().GetMethod() == "" {
			t.Error("authorization request missing request identity")
		}
		return fixtureCheckAllowed("signed-principal"), nil
	}})
	edge, err := newPublicEdge(authServer.URL, apiServer.URL, eventServer.URL, checkAddress)
	if err != nil {
		t.Fatalf("new integration edge: %v", err)
	}
	server := httptest.NewServer(edge)
	defer server.Close()

	for _, test := range []struct {
		name string
		path string
		want string
	}{
		{name: "API key ownership", path: "/v1/api_keys", want: "auth"},
		{name: "session stream precedence", path: "/v1/sessions/ses_1/events/stream", want: "event"},
		{name: "thread stream precedence", path: "/v1/sessions/ses_1/threads/thr_1/stream", want: "event"},
		{name: "ordinary API route", path: "/v1/sessions/ses_1/events", want: "api"},
		{name: "stream-like suffix stays API-owned", path: "/v1/sessions/ses_1/events/streaming", want: "api"},
		{name: "stream child path stays API-owned", path: "/v1/sessions/ses_1/threads/thr_1/stream/extra", want: "api"},
		{name: "session stream trailing slash stays API-owned", path: "/v1/sessions/ses_1/events/stream/", want: "api"},
		{name: "thread stream trailing slash stays API-owned", path: "/v1/sessions/ses_1/threads/thr_1/stream/", want: "api"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, server.URL+test.path, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			request.Header.Set("X-Api-Key", "test-key")
			request.Header.Set("X-Tetral-Internal-Principal", "forged-principal")
			request.Header.Set("X-Tetral-Forged", "forged-value")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("edge request: %v", err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				t.Fatalf("read response: %v", readErr)
			}
			if response.StatusCode != http.StatusOK || string(body) != test.want {
				t.Fatalf("response = %d/%q; want 200/%q", response.StatusCode, body, test.want)
			}
			mu.Lock()
			got := observations[test.want]
			mu.Unlock()
			if got.path != test.path {
				t.Fatalf("%s upstream path = %q; want %q", test.want, got.path, test.path)
			}
			if got.apiKey != "" || got.forged != "" || got.principal != "signed-principal" {
				t.Fatalf("%s upstream headers = %+v; want sanitized credentials and signed principal", test.want, got)
			}
		})
	}
}

func mintPublicEdgeAPIKey(t *testing.T, baseURL string, bootstrapKey string) string {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/v1/api_keys",
		bytes.NewBufferString(`{"name":"public edge"}`),
	)
	if err != nil {
		t.Fatalf("create API key request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", bootstrapKey)
	response, err := publicClient(t, nil).Do(request)
	if err != nil {
		t.Fatalf("mint API key through production auth path: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read API key response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mint API key status = %d body=%s", response.StatusCode, body)
	}
	var result auth.CreateAPIKeyResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode API key response: %v", err)
	}
	if result.APIKey == "" || result.KeyKind != auth.KindStandard {
		t.Fatalf("minted API key metadata = %+v; want one standard key", result.APIKeyMetadata)
	}
	return result.APIKey
}
