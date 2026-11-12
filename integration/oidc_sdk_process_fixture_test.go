package integration

import (
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
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Give each reported identity subtest custody of its actual isolated flow. The
// child selects TLS PostgreSQL before global storage initialization; its fixed
// selector runs the owning human/service body rather than a report stub.
func oidcIsolatedSDKIdentityCases(t *testing.T, body func(*testing.T)) {
	t.Helper()
	if os.Getenv(envIsolatedTLSRoot) == t.Name() {
		runIsolatedTLSChild(t, body)
		return
	}
	postgres := transporttest.NewPostgreSQL(t)
	root := t.Name()
	for _, actor := range []string{"human", "service"} {
		t.Run(actor, func(t *testing.T) {
			output, err := runIsolatedTLSProcess(t.Context(), t, root, "^"+root+"$/^"+actor+"$", postgres, isolatedTLSRootBudget)
			if err != nil {
				t.Fatalf("isolated TLS root %s failed: %v", t.Name(), err)
			}
			for _, required := range []string{
				"--- PASS: " + root + "/" + actor + " (",
				"oidc_assertion=" + actor + "_session_memory_one_exchange_one401_one_exchange_one_effect passed=true",
				"oidc_assertion=" + actor + "_created_by_redacted_by_stable_identity passed=true",
			} {
				if !strings.Contains(output, required) {
					t.Fatalf("isolated TLS root %s lacks marker %q", t.Name(), required)
				}
			}
		})
	}
}

// oidcDeveloper is a developer who signs in through a federation rule: it
// exchanges the assertion in its file for an access token, and sends that
// token with every request until it exchanges again.
type oidcDeveloper struct {
	*publicDeveloper
	assertionPath string
	selectors     map[string]string
	token         string
}

// newOIDCDeveloper returns a developer of the edge at baseURL whose exchange
// sends the assertion at assertionPath with selectors, the rule, the
// organization and, when set, the workspace and the service account.
func newOIDCDeveloper(t *testing.T, baseURL, assertionPath string, selectors map[string]string, caPaths ...string) *oidcDeveloper {
	t.Helper()
	d := &oidcDeveloper{publicDeveloper: newPublicDeveloper(t, baseURL, "", caPaths...), assertionPath: assertionPath, selectors: selectors}
	d.client.Authorize = func(request *http.Request) error {
		if request.URL.Path != "/v1/oauth/token" {
			request.Header.Set("Authorization", "Bearer "+d.token)
		}
		return nil
	}
	return d
}

// exchange exchanges the assertion now in the file for the token d sends
// from here on.
func (d *oidcDeveloper) exchange(t *testing.T) {
	t.Helper()
	assertion, err := os.ReadFile(d.assertionPath)
	if err != nil {
		t.Fatal(err)
	}
	body := map[string]string{"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": string(assertion)}
	for name, value := range d.selectors {
		if value != "" {
			body[name] = value
		}
	}
	var issued struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(d.request(t, http.MethodPost, "/v1/oauth/token", body), &issued); err != nil || issued.AccessToken == "" {
		t.Fatal("the exchange issued no access token")
	}
	d.token = issued.AccessToken
}

// oidcMemory names what a memory step made: the memory, its version, and
// the version as its read answers it.
type oidcMemory struct {
	MemoryID  string
	VersionID string
	Version   memory.MemoryVersion
}

// listed reports whether the list at path holds an object of id.
func (d *oidcDeveloper) listed(t *testing.T, path, id string) bool {
	t.Helper()
	var page struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(d.request(t, http.MethodGet, path, nil), &page); err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(page.Data, func(row struct {
		ID string `json:"id"`
	}) bool {
		return row.ID == id
	})
}

func (d *oidcDeveloper) created(t *testing.T, path string, body any) string {
	t.Helper()
	var object struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(d.request(t, http.MethodPost, path, body), &object); err != nil || object.ID == "" {
		t.Fatalf("POST %s answered no id", path)
	}
	return object.ID
}

// session creates an Environment, an agent and a Session of them, reads the
// Session back alone and in the list, and returns its ID.
func (d *oidcDeveloper) session(t *testing.T, name string) string {
	t.Helper()
	environment := d.created(t, "/v1/environments?beta=true", map[string]any{"name": name + "-environment",
		"config": map[string]any{"type": "cloud", "networking": map[string]any{"type": "blocked"}}})
	var agent struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(d.request(t, http.MethodPost, "/v1/agents?beta=true", map[string]any{"name": name + "-agent", "model": "anthropic/claude-opus-4-8",
		"approval_mode": "ask_for_approval", "tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}}), &agent); err != nil {
		t.Fatal(err)
	}
	session := d.created(t, "/v1/sessions?beta=true", map[string]any{"agent": map[string]any{"type": "agent", "id": agent.ID, "version": agent.Version},
		"environment_id": environment, "vault_ids": []string{}})
	var read struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(d.publicDeveloper.session(t, session), &read); err != nil || read.ID != session || !d.listed(t, "/v1/sessions?beta=true", session) {
		t.Fatalf("Session %s is not read back alone and in the list", session)
	}
	return session
}

// store creates a memory store, reads it back alone and in the list, and
// returns its ID.
func (d *oidcDeveloper) store(t *testing.T, name string) string {
	t.Helper()
	store := d.created(t, "/v1/memory_stores?beta=true", map[string]any{"name": name + "-memory"})
	var read struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(d.request(t, http.MethodGet, "/v1/memory_stores/"+store+"?beta=true", nil), &read); err != nil || read.ID != store ||
		!d.listed(t, "/v1/memory_stores?beta=true", store) {
		t.Fatalf("memory store %s is not read back alone and in the list", store)
	}
	return store
}

// written decodes the memory a create or an update answered and reads its
// version.
func (d *oidcDeveloper) written(t *testing.T, storeID string, answer json.RawMessage) oidcMemory {
	t.Helper()
	var written struct {
		ID        string `json:"id"`
		VersionID string `json:"memory_version_id"`
	}
	if err := json.Unmarshal(answer, &written); err != nil || written.ID == "" || written.VersionID == "" {
		t.Fatalf("memory write answered %s", answer)
	}
	result := oidcMemory{MemoryID: written.ID, VersionID: written.VersionID}
	if err := json.Unmarshal(d.request(t, http.MethodGet, "/v1/memory_stores/"+storeID+"/memory_versions/"+written.VersionID+"?beta=true", nil), &result.Version); err != nil {
		t.Fatal(err)
	}
	return result
}

func (d *oidcDeveloper) create(t *testing.T, storeID, path, content string) oidcMemory {
	t.Helper()
	return d.written(t, storeID, d.request(t, http.MethodPost, "/v1/memory_stores/"+storeID+"/memories?beta=true", map[string]any{"path": path, "content": content}))
}

func (d *oidcDeveloper) update(t *testing.T, storeID, memoryID, content string) oidcMemory {
	t.Helper()
	return d.written(t, storeID, d.request(t, http.MethodPost, "/v1/memory_stores/"+storeID+"/memories/"+memoryID+"?beta=true", map[string]any{"content": content}))
}

// read reads the memory alone, in the store's list, and its version in the
// store's versions.
func (d *oidcDeveloper) read(t *testing.T, storeID, memoryID string) {
	t.Helper()
	var read struct {
		ID        string `json:"id"`
		VersionID string `json:"memory_version_id"`
	}
	store := "/v1/memory_stores/" + storeID
	if err := json.Unmarshal(d.request(t, http.MethodGet, store+"/memories/"+memoryID+"?beta=true", nil), &read); err != nil || read.ID != memoryID ||
		!d.listed(t, store+"/memories?beta=true", memoryID) || !d.listed(t, store+"/memory_versions?beta=true", read.VersionID) {
		t.Fatalf("memory %s is not read back alone, in the list and in the versions", memoryID)
	}
}

func (d *oidcDeveloper) delete(t *testing.T, storeID, memoryID string) {
	t.Helper()
	d.request(t, http.MethodDelete, "/v1/memory_stores/"+storeID+"/memories/"+memoryID+"?beta=true", nil)
}

// redact redacts the version and returns it as the redaction answers it.
func (d *oidcDeveloper) redact(t *testing.T, storeID, versionID string) memory.MemoryVersion {
	t.Helper()
	var redacted memory.MemoryVersion
	if err := json.Unmarshal(d.request(t, http.MethodPost, "/v1/memory_stores/"+storeID+"/memory_versions/"+versionID+"/redact?beta=true", nil), &redacted); err != nil {
		t.Fatal(err)
	}
	return redacted
}

// A child process must not inherit a workstation credential or profile; the
// runner's dependency descriptors survive.
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
	GRPCAddress     string
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
	if output, err := buildCommand(build, binary, "./services/auth/cmd/tetral-auth").CombinedOutput(); err != nil {
		t.Fatalf("build actual Auth: %v: %s", err, output)
	}
	return binary
}

// Multiple actual Auth replicas share this immutable built binary and database,
// while every process retains distinct listeners, PID, join and shutdown owner.
func startOIDCAuthProcessFromBinary(ctx context.Context, t *testing.T, binary string, database *sql.DB, privateKey string, overrides map[string]string) *oidcAuthProcess {
	t.Helper()
	addresses, release := publicReserveAddresses(t, 3)
	process := &oidcAuthProcess{joined: make(chan error, 1), URL: "http://" + addresses[0], MetricsURL: "http://" + addresses[1], GRPCAddress: addresses[2]}
	values := map[string]string{
		"TETRAL_AUTH_GRPC_ADDR":                          process.GRPCAddress,
		"TETRAL_AUTH_GRPC_TRANSPORT":                     "plaintext",
		"TETRAL_AUTH_HTTP_ADDR":                          strings.TrimPrefix(process.URL, "http://"),
		"TETRAL_AUTH_METRICS_ADDR":                       strings.TrimPrefix(process.MetricsURL, "http://"),
		"TETRAL_DATABASE_URL":                            storagetest.RuntimeDatabaseURL(t, database),
		"TETRAL_DATABASE_TLS_CA_PATH":                    filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
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
		if name == "TETRAL_AUTH_HTTP_ADDR" || name == "TETRAL_AUTH_METRICS_ADDR" || name == "TETRAL_AUTH_GRPC_ADDR" {
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
	release()
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
			t.Fatalf("actual Auth exited before readiness: %v; %s", err, publicStartupDiagnostic(process.output.String()))
		case <-readiness.Done():
			t.Fatalf("actual Auth did not become ready by its fixture deadline; %s", publicStartupDiagnostic(process.output.String()))
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
	authGRPCAddress string
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

func newOIDCEdge(t *testing.T, authURL, apiURL string, authGRPCAddress string) *oidcEdge {
	t.Helper()
	a, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	b, err := url.Parse(apiURL)
	if err != nil {
		t.Fatal(err)
	}
	return &oidcEdge{authURL: a, apiURL: b, authGRPCAddress: authGRPCAddress, client: publicClient(t, &http.Client{Timeout: 30 * time.Second})}
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
	headers := request.Header.Clone()
	stripSDKIntegrationUntrustedMetadata(headers)
	response, err := directEdgeCheck(request.Context(), edge.authGRPCAddress, request.Method, request.URL.RequestURI(), id, headers)
	if err != nil {
		http.Error(w, "fixture authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	status, principal := directEdgeCheckStatus(response)
	if status != http.StatusOK {
		edge.record(request, status)
		forwardFixtureCheckDenial(w, response)
		return
	}
	if principal == "" {
		http.Error(w, "fixture signed principal missing", http.StatusServiceUnavailable)
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
		http.NotFound(w, request)
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
	checkAddress := startCheckFixture(t, checkFixture{check: func(_ context.Context, request *authv3.CheckRequest) (*authv3.CheckResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		authentications++
		httpRequest := request.GetAttributes().GetRequest().GetHttp()
		headers := fixtureCheckHeaders(request)
		if httpRequest.GetPath() != "/v1/models" {
			return fixtureCheckDenied(http.StatusUnauthorized), nil
		}
		trustedInputs = httpRequest.GetMethod() == http.MethodGet && headers.Get("Authorization") == bearer && headers.Get("X-Tetral-Internal-Principal") == "" && headers.Get("X-Request-Id") != "" && headers.Get("X-Forwarded-For") == "127.0.0.1" && headers.Get("X-Original-Method") == "" && headers.Get("X-Original-Path") == ""
		return fixtureCheckAllowed("fixture-signed-principal"), nil
	}})
	edge := newOIDCEdge(t, actualAuth.URL, actualAPI.URL, checkAddress)
	// Stand-ins answer behind this edge, so no answer here is Engine's to
	// hold to its OpenAPI file.
	edge.client = &http.Client{Timeout: 30 * time.Second}
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
