package integration

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const oidcSDKPin = "b9c64659ac3c0773415ad463abfe52d20c6d1e28"

// Like the accepted public process cases, the child selects TLS PostgreSQL
// before the process-global storage registry initializes. Actual Auth also
// verifies this server's explicit CA and hostname through its production opener.
func oidcIsolatedTLSCase(t *testing.T, body func(*testing.T)) {
	t.Helper()
	oidcIsolatedTLSCaseWithMarker(t, "oidc_sdk_assertion=", body)
}

func oidcIsolatedTLSCaseWithMarker(t *testing.T, marker string, body func(*testing.T)) {
	t.Helper()
	if os.Getenv("TETRAL_OIDC_PROCESS_CASE") == t.Name() {
		dsn, err := url.Parse(os.Getenv("TETRAL_OIDC_PROCESS_PG_URL"))
		if err != nil || dsn.Host == "" {
			t.Fatal("isolated OIDC TLS PostgreSQL is absent")
		}
		query := dsn.Query()
		query.Set("sslmode", "require")
		dsn.RawQuery = query.Encode()
		t.Setenv(storagetest.EnvTestDatabaseURL, dsn.String())
		t.Setenv(storagetest.EnvTestRunID, "")
		body(t)
		return
	}
	postgres := transporttest.NewPostgreSQL(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^"+t.Name()+"$", "-test.v") //nolint:gosec // Current test executable, fixed owning root.
	command.Env = append(os.Environ(), "TETRAL_OIDC_PROCESS_CASE="+t.Name(), "TETRAL_OIDC_PROCESS_PG_CERTS="+postgres.Directory, "TETRAL_OIDC_PROCESS_PG_URL="+postgres.URL)
	output, err := command.CombinedOutput()
	t.Logf("isolated OIDC process assertions:\n%s", output)
	if err != nil {
		t.Fatalf("actual OIDC process composition failed: %v", err)
	}
	if !strings.Contains(string(output), "--- PASS: "+t.Name()) || !strings.Contains(string(output), marker) {
		t.Fatal("OIDC composition lacks executed owning assertions")
	}
}

func oidcVerifySDKPin(t *testing.T) {
	t.Helper()
	root := os.Getenv("TETRAL_ENGINE_SDK_ROOT")
	if root == "" {
		t.Fatal("OIDC SDK test requires its native pinned SDK dependency")
	}
	for _, check := range []struct {
		arguments []string
		expected  string
	}{
		{[]string{"-C", root, "rev-parse", "HEAD"}, oidcSDKPin},
		{[]string{"-C", root, "status", "--porcelain"}, ""},
	} {
		command := exec.Command("git", check.arguments...) //nolint:gosec // Fixed git inspections of runner-owned SDK path.
		output, err := command.Output()
		if err != nil || strings.TrimSpace(string(output)) != check.expected {
			t.Fatal("actual SDK source must remain clean at the exact native pin")
		}
	}
}

// Reuse the existing bounded NDJSON child control and cleanup implementation.
// Only this constructor owns the new OIDC bootstrap and fixed driver path.
func startOIDCSDKChild(ctx context.Context, t *testing.T, config map[string]string) *contentSDKChild {
	t.Helper()
	oidcVerifySDKPin(t)
	directory := t.TempDir()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "oidc-sdk-bootstrap.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	child := &contentSDKChild{lines: make(chan []byte, 8), joined: make(chan error, 1)}
	child.command = exec.CommandContext(ctx, "bun", "testdata/oidc-client.ts", path) //nolint:gosec // Fixed Engine driver and private credential handle.
	child.command.Env = oidcChildEnvironment(nil)
	child.command.Stderr = &child.output
	child.input, err = child.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		for scanner.Scan() {
			child.lines <- append([]byte(nil), scanner.Bytes()...)
		}
		close(child.lines)
	}()
	go func() { child.joined <- child.command.Wait() }()
	t.Cleanup(func() { child.stop(t) })
	return child
}

// Explicit SDK config must not inherit a workstation credential/profile. The
// runner-owned SDK checkout handle and native dependency descriptors survive.
func oidcChildEnvironment(values map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, overridden := values[key]; overridden {
			continue
		}
		if strings.HasPrefix(key, "ANTHROPIC_") || key == "TETRAL_API_KEY" || key == "TETRAL_AUTH_TOKEN" || key == "TETRAL_BASE_URL" {
			continue
		}
		environment = append(environment, entry)
	}
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	return environment
}

type oidcAuthProcess struct {
	command         *exec.Cmd
	joined          chan error
	output          lockedBuffer
	URL, MetricsURL string
	verifier        *auth.InternalPrincipalVerifier
	once            sync.Once
}

func startOIDCAuthProcess(ctx context.Context, t *testing.T, database *sql.DB, privateKey string) *oidcAuthProcess {
	t.Helper()
	return startOIDCAuthProcessFromBinary(ctx, t, buildOIDCAuthCommand(ctx, t), database, privateKey, nil)
}

func buildOIDCAuthCommand(ctx context.Context, t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "tetral-auth")
	build, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	command := exec.CommandContext(build, "go", "build", "-race", "-o", binary, "./services/auth/cmd/tetral-auth") //nolint:gosec // Actual repository-owned Auth command.
	command.Dir = ".."
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build actual Auth: %v: %s", err, output)
	}
	return binary
}

// Multiple actual Auth replicas share this immutable built binary and database,
// while every process retains distinct listeners, PID, join and shutdown owner.
func startOIDCAuthProcessFromBinary(ctx context.Context, t *testing.T, binary string, database *sql.DB, privateKey string, overrides map[string]string) *oidcAuthProcess {
	t.Helper()
	process := &oidcAuthProcess{joined: make(chan error, 1), URL: "http://" + publicFreeAddress(t), MetricsURL: "http://" + publicFreeAddress(t)}
	values := map[string]string{
		"TETRAL_AUTH_HTTP_ADDR":                          strings.TrimPrefix(process.URL, "http://"),
		"TETRAL_AUTH_METRICS_ADDR":                       strings.TrimPrefix(process.MetricsURL, "http://"),
		"TETRAL_DATABASE_URL":                            storagetest.RuntimeDatabaseURL(t, database),
		"TETRAL_DATABASE_TLS_CA_PATH":                    filepath.Join(os.Getenv("TETRAL_OIDC_PROCESS_PG_CERTS"), "ca.pem"),
		"TETRAL_DATABASE_TLS_SERVER_NAME":                "postgres.transport.test",
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64": privateKey,
		"ENGINE_API_KEY":                                 strings.Repeat("o", auth.MinBootstrapKeyBytes),
		"ENGINE_BOOTSTRAP_WORKSPACE_ID":                  string(workspace.DefaultID),
		"TETRAL_DEPLOYMENT_ENVIRONMENT":                  "test",
		"TETRAL_SERVICE_VERSION":                         "oidc-sdk-fixture",
		"TETRAL_AUTH_EXCHANGE_REQUESTS_PER_MINUTE":       "600",
		"TETRAL_AUTH_EXCHANGE_CONCURRENCY":               "32",
		"TETRAL_AUTH_EXCHANGE_BODY_BYTES":                "32768",
		"TETRAL_AUTH_EXCHANGE_BODY_READ_TIMEOUT_MS":      "5000",
		"TETRAL_AUTH_JWKS_CACHE_TTL_SECONDS":             "600",
	}
	for name, value := range overrides {
		if name == "TETRAL_AUTH_HTTP_ADDR" || name == "TETRAL_AUTH_METRICS_ADDR" {
			t.Fatal("Auth fixture listeners are owned independently per process")
		}
		values[name] = value
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	process.verifier, err = auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatal(err)
	}
	process.command = exec.Command(binary) //nolint:gosec // Previously built actual repository-owned Auth command.
	process.command.Env = oidcChildEnvironment(values)
	process.command.Stdout, process.command.Stderr = &process.output, &process.output
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.joined <- process.command.Wait() }()
	t.Cleanup(func() { process.stop(t) })
	readiness, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	client := &http.Client{Timeout: time.Second}
	for {
		request, err := http.NewRequestWithContext(readiness, http.MethodGet, process.URL+"/ready", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case err := <-process.joined:
			process.joined <- err
			t.Fatalf("actual Auth exited before readiness: %v", err)
		case <-readiness.Done():
			t.Fatal("actual Auth did not become ready by its fixture deadline")
		case <-time.After(10 * time.Millisecond):
		}
	}
	return process
}

func (p *oidcAuthProcess) stop(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Error("actual Auth shutdown signal failed")
		}
		select {
		case err := <-p.joined:
			if err != nil {
				t.Error("actual Auth did not shut down cleanly")
			}
		case <-time.After(10 * time.Second):
			_ = p.command.Process.Kill()
			<-p.joined
			t.Error("actual Auth exceeded shutdown deadline")
		}
	})
}

// This acceptance-only edge models the local exchange/Auth boundary.
// Deployment edge routing is separately owned. No issuer JWT or bearer is
// emitted in the assertion oracle; token bytes remain only in private memory.
type oidcEdge struct {
	authURL, apiURL *url.URL
	client          *http.Client
	mu              sync.Mutex
	exchanges       []oidcExchange
	attempts        []oidcAttempt
	sequence        int
	ordinal         int
}
type oidcExchange struct {
	token, tokenType string
	ordinal          int
	status, expires  int
	noStore, noCache bool
}
type oidcAttempt struct {
	method, path, bearer string
	status, ordinal      int
}
type oidcEdgeMark struct{ exchanges, attempts int }

func newOIDCEdge(t *testing.T, authURL, apiURL string) *oidcEdge {
	t.Helper()
	a, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	b, err := url.Parse(apiURL)
	if err != nil {
		t.Fatal(err)
	}
	return &oidcEdge{authURL: a, apiURL: b, client: &http.Client{Timeout: 30 * time.Second}}
}
func (edge *oidcEdge) mark() oidcEdgeMark {
	edge.mu.Lock()
	defer edge.mu.Unlock()
	return oidcEdgeMark{len(edge.exchanges), len(edge.attempts)}
}

func (edge *oidcEdge) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodPost && request.URL.Path == "/v1/oauth/token" {
		edge.exchange(w, request)
		return
	}
	if !strings.HasPrefix(request.URL.Path, "/v1/") {
		http.NotFound(w, request)
		return
	}
	edge.mu.Lock()
	edge.sequence++
	id := fmt.Sprintf("req_oidc_%d", edge.sequence)
	edge.mu.Unlock()
	check, err := http.NewRequestWithContext(request.Context(), http.MethodPost, edge.authURL.ResolveReference(&url.URL{Path: "/internal/auth/authorize"}).String(), nil)
	if err != nil {
		http.Error(w, "fixture authorization unavailable", http.StatusBadGateway)
		return
	}
	for _, name := range []string{"Authorization", "X-Api-Key"} {
		check.Header[name] = append([]string(nil), request.Header.Values(name)...)
	}
	check.Header.Set("X-Original-Method", request.Method)
	check.Header.Set("X-Original-Path", request.URL.Path)
	check.Header.Set("X-Request-Id", id)
	// This acceptance edge only serves local fixture clients. Supply its trusted
	// loopback address rather than forwarding a client-selected value.
	check.Header.Set("X-Forwarded-For", "127.0.0.1")
	response, err := edge.client.Do(check)
	if err != nil {
		http.Error(w, "fixture authorization unavailable", http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		edge.record(request, response.StatusCode)
		copySDKIntegrationResponseHeaders(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}
	principal := response.Header.Get("X-Tetral-Internal-Principal")
	if principal == "" {
		http.Error(w, "fixture signed principal missing", http.StatusBadGateway)
		return
	}
	outbound := request.Clone(request.Context())
	outbound.URL.Scheme, outbound.URL.Host = edge.apiURL.Scheme, edge.apiURL.Host
	outbound.Host = edge.apiURL.Host
	outbound.RequestURI = ""
	outbound.Header = request.Header.Clone()
	stripSDKIntegrationClientHeaders(outbound.Header)
	for _, name := range []string{"Authorization", "X-Original-Method", "X-Original-Path", "X-Forwarded-For"} {
		outbound.Header.Del(name)
	}
	outbound.Header.Set("X-Tetral-Internal-Principal", principal)
	outbound.Header.Set("X-Request-Id", id)
	upstream, err := edge.client.Do(outbound) //nolint:gosec // Fixture-owned actual API URL.
	if err != nil {
		http.Error(w, "fixture upstream unavailable", http.StatusBadGateway)
		return
	}
	defer func() { _ = upstream.Body.Close() }()
	edge.record(request, upstream.StatusCode)
	copySDKIntegrationResponseHeaders(w.Header(), upstream.Header)
	w.WriteHeader(upstream.StatusCode)
	_, _ = io.Copy(w, upstream.Body)
}
func (edge *oidcEdge) record(request *http.Request, status int) {
	edge.mu.Lock()
	defer edge.mu.Unlock()
	edge.ordinal++
	edge.attempts = append(edge.attempts, oidcAttempt{method: request.Method, path: request.URL.Path, bearer: request.Header.Get("Authorization"), status: status, ordinal: edge.ordinal})
}
func (edge *oidcEdge) exchange(w http.ResponseWriter, request *http.Request) {
	outbound := request.Clone(request.Context())
	outbound.URL.Scheme, outbound.URL.Host = edge.authURL.Scheme, edge.authURL.Host
	outbound.Host = edge.authURL.Host
	outbound.RequestURI = ""
	outbound.Header = request.Header.Clone()
	stripSDKIntegrationClientHeaders(outbound.Header)
	outbound.Header.Del("Authorization")
	response, err := edge.client.Do(outbound) //nolint:gosec // Fixture-owned actual Auth URL.
	if err != nil {
		http.Error(w, "fixture exchange unavailable", http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		http.Error(w, "fixture exchange unreadable", http.StatusBadGateway)
		return
	}
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		Expires     int    `json:"expires_in"`
	}
	_ = json.Unmarshal(body, &token)
	edge.mu.Lock()
	edge.ordinal++
	edge.exchanges = append(edge.exchanges, oidcExchange{token: token.AccessToken, tokenType: token.TokenType, status: response.StatusCode, expires: token.Expires, noStore: response.Header.Get("Cache-Control") == "no-store", noCache: response.Header.Get("Pragma") == "no-cache", ordinal: edge.ordinal})
	edge.mu.Unlock()
	copySDKIntegrationResponseHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(body)
}

func TestOIDCEdgePreservesExchangeAndBearerBoundary(t *testing.T) {
	const bearer = "Bearer fixture-token"
	var mu sync.Mutex
	var exchanges, authentications, business int
	var trustedInputs, sanitizedInputs bool
	actualAuth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if request.URL.Path == "/v1/oauth/token" {
			exchanges++
			body, err := io.ReadAll(io.LimitReader(request.Body, 4096))
			if err != nil || string(body) != `{"grant_type":"fixture"}` || request.Method != http.MethodPost || request.Header.Get("Authorization") != "" || request.Header.Get("X-Tetral-Internal-Principal") != "" {
				http.Error(w, "invalid fixture exchange forwarding", http.StatusBadRequest)
				return
			}
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Pragma", "no-cache")
			_, _ = io.WriteString(w, `{"access_token":"fixture-token","token_type":"Bearer","expires_in":600}`)
			return
		}
		authentications++
		if request.Header.Get("X-Original-Path") != "/v1/models" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		trustedInputs = request.Method == http.MethodPost && request.Header.Get("X-Original-Method") == http.MethodGet && request.Header.Get("Authorization") == bearer && request.Header.Get("X-Tetral-Internal-Principal") == "" && request.Header.Get("X-Request-Id") != "" && request.Header.Get("X-Forwarded-For") == "127.0.0.1"
		w.Header().Set("X-Tetral-Internal-Principal", "fixture-signed-principal")
	}))
	defer actualAuth.Close()
	actualAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		business++
		sanitizedInputs = request.Header.Get("Authorization") == "" && request.Header.Get("X-Api-Key") == "" && request.Header.Get("X-Tetral-Forged") == "" && request.Header.Get("X-Tetral-Internal-Principal") == "fixture-signed-principal" && request.Header.Get("X-Original-Method") == "" && request.Header.Get("X-Original-Path") == "" && request.Header.Get("X-Forwarded-For") == "" && request.Header.Get("X-Request-Id") != ""
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	defer actualAPI.Close()
	edge := newOIDCEdge(t, actualAuth.URL, actualAPI.URL)
	call := func(method, path, body string) int {
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		request.Header.Set("Authorization", bearer)
		request.Header.Set("X-Tetral-Internal-Principal", "forged")
		request.Header.Set("X-Tetral-Forged", "forged")
		request.Header.Set("X-Original-Method", http.MethodDelete)
		request.Header.Set("X-Original-Path", "/v1/forged")
		request.Header.Set("X-Forwarded-For", "untrusted-client-value")
		response := httptest.NewRecorder()
		edge.ServeHTTP(response, request)
		return response.Code
	}
	if code := call(http.MethodPost, "/v1/oauth/token", `{"grant_type":"fixture"}`); code != http.StatusOK {
		t.Fatalf("exact exchange status=%d", code)
	}
	if code := call(http.MethodGet, "/v1/models", ""); code != http.StatusOK {
		t.Fatalf("authenticated business status=%d", code)
	}
	for _, route := range []struct{ method, path string }{{http.MethodGet, "/v1/oauth/token"}, {http.MethodPost, "/v1/oauth/token/extra"}} {
		if code := call(route.method, route.path, ""); code != http.StatusUnauthorized {
			t.Fatalf("adjacent exchange route admission status=%d", code)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if exchanges != 1 || authentications != 3 || business != 1 || !trustedInputs || !sanitizedInputs {
		t.Fatal("test edge bypassed adjacent route authentication, lost Bearer binding, or leaked client credentials")
	}
}

// Failure diagnostics expose only completed operation/status order. Credentials,
// digests, exchange bodies and assertions have no representation in this DTO.
func (edge *oidcEdge) safeJournal(mark oidcEdgeMark) string {
	edge.mu.Lock()
	defer edge.mu.Unlock()
	type event struct {
		Ordinal int    `json:"ordinal"`
		Kind    string `json:"kind"`
		Status  int    `json:"status"`
		Method  string `json:"method,omitempty"`
		Path    string `json:"path,omitempty"`
	}
	events := make([]event, 0, len(edge.exchanges)+len(edge.attempts))
	for _, exchange := range edge.exchanges[mark.exchanges:] {
		events = append(events, event{Ordinal: exchange.ordinal, Kind: "exchange", Status: exchange.status})
	}
	for _, attempt := range edge.attempts[mark.attempts:] {
		events = append(events, event{Ordinal: attempt.ordinal, Kind: "business", Status: attempt.status, Method: attempt.method, Path: attempt.path})
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Ordinal < events[j].Ordinal })
	encoded, _ := json.Marshal(events)
	return string(encoded)
}
