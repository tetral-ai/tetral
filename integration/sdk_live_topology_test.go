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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralapi "github.com/tetral-ai/tetral/services/api"
	tetralauth "github.com/tetral-ai/tetral/services/auth"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

const (
	sdkIntegrationVaultKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	sdkIntegrationTimeout  = 3 * time.Minute
	sdkIntegrationYarn     = "yarn@1.22.22"
)

type sdkIntegrationEnv map[string]string

func (env sdkIntegrationEnv) Getenv(name string) string {
	return env[name]
}

type sdkIntegrationEdge struct {
	client          *http.Client
	authBaseURL     *url.URL
	apiBaseURL      *url.URL
	eventBaseURL    *url.URL
	requestIDPrefix string

	mu        sync.Mutex
	requestID uint64
}

func newSDKIntegrationEdge(authBaseURL string, apiBaseURL string, eventBaseURL string) (http.Handler, error) {
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
	return &sdkIntegrationEdge{
		client:          http.DefaultClient,
		authBaseURL:     authURL,
		apiBaseURL:      apiURL,
		eventBaseURL:    eventURL,
		requestIDPrefix: "req_sdk_topology_",
	}, nil
}

func (edge *sdkIntegrationEdge) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/v1" && !strings.HasPrefix(request.URL.Path, "/v1/") {
		http.NotFound(w, request)
		return
	}

	requestID := request.Header.Get("X-Request-Id")
	if requestID == "" {
		edge.mu.Lock()
		edge.requestID++
		requestID = fmt.Sprintf("%s%d", edge.requestIDPrefix, edge.requestID)
		edge.mu.Unlock()
	}

	principal, ok := edge.authorize(w, request, requestID)
	if !ok {
		return
	}
	target := edge.apiBaseURL
	switch {
	case isSDKIntegrationAPIKeyPath(request.URL.Path):
		target = edge.authBaseURL
	case isSDKIntegrationStreamPath(request.URL.Path):
		target = edge.eventBaseURL
	}

	outbound := request.Clone(request.Context())
	outbound.URL.Scheme = target.Scheme
	outbound.URL.Host = target.Host
	outbound.Host = target.Host
	outbound.RequestURI = ""
	outbound.Header = request.Header.Clone()
	stripSDKIntegrationClientHeaders(outbound.Header)
	outbound.Header.Set("X-Tetral-Internal-Principal", principal)
	outbound.Header.Set("X-Request-Id", requestID)

	//nolint:gosec // G704: the URL is this test's own in-process edge proxy target, not caller input.
	response, err := edge.client.Do(outbound)
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	copySDKIntegrationResponseHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}

func (edge *sdkIntegrationEdge) authorize(w http.ResponseWriter, request *http.Request, requestID string) (string, bool) {
	authorizeURL := edge.authBaseURL.ResolveReference(&url.URL{Path: "/internal/auth/authorize"})
	authorizeRequest, err := http.NewRequestWithContext(request.Context(), http.MethodPost, authorizeURL.String(), nil)
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusBadGateway)
		return "", false
	}
	authorizeRequest.Header.Set("X-Api-Key", request.Header.Get("X-Api-Key"))
	authorizeRequest.Header.Set("X-Original-Method", request.Method)
	authorizeRequest.Header.Set("X-Original-Path", request.URL.Path)
	authorizeRequest.Header.Set("X-Request-Id", requestID)
	authorizeRequest.Header.Set("X-Forwarded-For", "127.0.0.1")

	response, err := edge.client.Do(authorizeRequest)
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusBadGateway)
		return "", false
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		copySDKIntegrationResponseHeaders(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return "", false
	}
	principal := response.Header.Get("X-Tetral-Internal-Principal")
	if principal == "" {
		http.Error(w, "authorization unavailable", http.StatusBadGateway)
		return "", false
	}
	return principal, true
}

func isSDKIntegrationAPIKeyPath(path string) bool {
	return path == "/v1/api_keys" || strings.HasPrefix(path, "/v1/api_keys/")
}

func isSDKIntegrationStreamPath(path string) bool {
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

func stripSDKIntegrationClientHeaders(header http.Header) {
	for name := range header {
		lower := strings.ToLower(name)
		if lower == "x-api-key" || strings.HasPrefix(lower, "x-tetral-") {
			header.Del(name)
		}
	}
}

func copySDKIntegrationResponseHeaders(destination http.Header, source http.Header) {
	for name, values := range source {
		if strings.HasPrefix(strings.ToLower(name), "x-tetral-") {
			continue
		}
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}

func TestSDKIntegrationEdgeAuthenticatesSanitizesAndRoutesPublicRequests(t *testing.T) {
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
		if request.URL.Path == "/internal/auth/authorize" {
			if request.Header.Get("X-Api-Key") != "test-key" {
				t.Errorf("authorization key = %q; want test-key", request.Header.Get("X-Api-Key"))
			}
			if request.Header.Get("X-Original-Path") == "" || request.Header.Get("X-Original-Method") == "" {
				t.Error("authorization request missing original request identity")
			}
			w.Header().Set("X-Tetral-Internal-Principal", "signed-principal")
			_, _ = io.WriteString(w, `{"allow":true}`)
			return
		}
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

	edge, err := newSDKIntegrationEdge(authServer.URL, apiServer.URL, eventServer.URL)
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

func TestForkSDKIntegrationSuiteRunsAgainstLocalEngineTopology(t *testing.T) {
	sdkRoot := strings.TrimSpace(os.Getenv("TETRAL_ENGINE_SDK_ROOT"))
	if sdkRoot == "" {
		t.Skip("set TETRAL_ENGINE_SDK_ROOT to run the fork SDK integration suite")
	}
	suitePath := filepath.Join(sdkRoot, "tests", "integration", "tetral.integration.test.ts")
	//nolint:gosec // G703: path is built from TETRAL_ENGINE_SDK_ROOT, a developer-supplied local checkout.
	if _, err := os.Stat(suitePath); err != nil {
		t.Fatalf("fork SDK integration suite is unavailable at %s: %v", suitePath, err)
	}
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("TETRAL_ENGINE_SDK_ROOT is set but Bun is unavailable: %v", err)
	}

	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	runtimeClient := dbconnect.NewClientForTesting(runtimeDB)
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate internal principal key: %v", err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("build internal principal signer: %v", err)
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatalf("build internal principal verifier: %v", err)
	}
	bootstrapKey := strings.Repeat("b", auth.MinBootstrapKeyBytes)

	authRouter, err := tetralauth.BuildRouter(context.Background(), tetralauth.RouterBuildConfig{
		RawDatabase: runtimeDB,
		Config: tetralauth.Config{
			BootstrapAPIKey:                bootstrapKey,
			BootstrapWorkspaceID:           workspace.DefaultID,
			InternalPrincipalPrivateKeyB64: privateKey,
			InternalPrincipalTTL:           time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("build auth router: %v", err)
	}
	authServer := httptest.NewServer(authRouter)
	defer authServer.Close()

	dataDir := t.TempDir()
	if err := os.Chmod(dataDir, 0o700); err != nil {
		t.Fatalf("secure API data directory: %v", err)
	}
	apiRouter, err := tetralapi.BuildRouter(context.Background(), tetralapi.RouterConfig{
		RuntimeClient:     runtimeClient,
		RawDatabase:       adminDB,
		VaultKey:          sdkIntegrationVaultKey,
		DataDir:           dataDir,
		Env:               sdkIntegrationEnv{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_sdk_integration"},
		BlobStore:         blob.NewFakeBlobStore(),
		PrincipalVerifier: verifier,
	})
	if err != nil {
		t.Fatalf("build api router: %v", err)
	}
	apiServer := httptest.NewServer(apiRouter)
	defer apiServer.Close()

	eventReader := internaleventstream.NewPostgreSQLReader(
		runtimeClient,
		internaleventstream.WithPageTokenSecret([]byte(sdkIntegrationVaultKey)),
	)
	eventServer := httptest.NewServer(eventstream.NewRouter(
		eventReader,
		verifier,
		eventstream.WithStreamPollInterval(time.Millisecond),
		eventstream.WithStreamMaxEmptyPolls(1),
	))
	defer eventServer.Close()

	edge, err := newSDKIntegrationEdge(authServer.URL, apiServer.URL, eventServer.URL)
	if err != nil {
		t.Fatalf("build integration edge: %v", err)
	}
	edgeServer := httptest.NewServer(edge)
	defer edgeServer.Close()

	standardAPIKey := mintSDKIntegrationAPIKey(t, edgeServer.URL, bootstrapKey)
	ctx, cancel := context.WithTimeout(context.Background(), sdkIntegrationTimeout)
	defer cancel()
	jestPath := filepath.Join(sdkRoot, "node_modules", "jest", "bin", "jest.js")
	//nolint:gosec // G703: path is built from TETRAL_ENGINE_SDK_ROOT, a developer-supplied local checkout.
	if _, err := os.Stat(jestPath); err != nil {
		install := exec.CommandContext(
			ctx,
			bunPath,
			"x",
			sdkIntegrationYarn,
			"install",
			"--frozen-lockfile",
			"--ignore-scripts",
			"--non-interactive",
		) //nolint:gosec // repository-pinned SDK root, package-manager version, and fixed arguments.
		install.Dir = sdkRoot
		installOutput, installErr := install.CombinedOutput()
		if installErr != nil {
			t.Fatalf("install fork SDK integration dependencies: %v\n%s", installErr, installOutput)
		}
	}
	command := exec.CommandContext(ctx, bunPath, "x", "jest", "tests/integration/tetral.integration.test.ts", "--runInBand") //nolint:gosec // repository-pinned SDK root and fixed arguments.
	command.Dir = sdkRoot
	command.Env = append(filteredSDKIntegrationEnvironment(os.Environ()),
		"TETRAL_COMPAT_LIVE=1",
		"TETRAL_BASE_URL="+edgeServer.URL,
		"TETRAL_API_KEY="+standardAPIKey,
	)
	output, err := command.CombinedOutput()
	safeOutput := strings.ReplaceAll(string(output), standardAPIKey, "<redacted-api-key>")
	if ctx.Err() != nil {
		t.Fatalf("fork SDK integration suite timed out: %v\n%s", ctx.Err(), safeOutput)
	}
	if err != nil {
		t.Fatalf("fork SDK integration suite failed: %v\n%s", err, safeOutput)
	}
	if strings.Contains(strings.ToLower(safeOutput), " skip") {
		t.Fatalf("fork SDK integration suite skipped cases despite TETRAL_COMPAT_LIVE=1:\n%s", safeOutput)
	}
}

func mintSDKIntegrationAPIKey(t *testing.T, baseURL string, bootstrapKey string) string {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/v1/api_keys",
		bytes.NewBufferString(`{"name":"sdk integration topology"}`),
	)
	if err != nil {
		t.Fatalf("create API key request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", bootstrapKey)
	response, err := http.DefaultClient.Do(request)
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

func filteredSDKIntegrationEnvironment(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, item := range environment {
		name, _, _ := strings.Cut(item, "=")
		switch name {
		case "TETRAL_COMPAT_LIVE", "TETRAL_BASE_URL", "TETRAL_API_KEY":
			continue
		default:
			filtered = append(filtered, item)
		}
	}
	return filtered
}
