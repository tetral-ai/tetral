package httpapi_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	publicstream "github.com/tetral-ai/tetral/services/event-stream"
)

// Inspect the actual router's leaf handlers, including conditional routes and
// stubs. There is no duplicated expected business-route inventory here.
func checkPublicRouteCoverage(router http.Handler) error {
	routes, ok := router.(chi.Routes)
	if !ok {
		return fmt.Errorf("router does not expose registrations")
	}
	return chi.Walk(routes, func(method, pattern string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
		if pattern == "/health" && method == http.MethodGet {
			return nil
		}
		classified, ok := handler.(*httpapi.PublicOperationHandler)
		if !ok {
			return fmt.Errorf("unclassified route %s %s", method, pattern)
		}
		operation, ok := auth.OperationForRoute(method, pattern)
		if !ok || classified.Method != method || classified.Pattern != pattern || classified.Operation != operation || !auth.IsRegisteredOperation(operation) {
			return fmt.Errorf("invalid route classification %s %s", method, pattern)
		}
		return nil
	})
}

func TestPublicOperationRouteCoverage(t *testing.T) {
	routers := map[string]http.Handler{
		"api stubs": httpapi.NewRouter(nil, ""),
		"api configured": httpapi.NewRouter(httpapi.NewSessionHandler(nil), "",
			httpapi.WithAgentHandler(httpapi.NewAgentHandler(nil)),
			httpapi.WithEnvironmentHandler(httpapi.NewEnvironmentHandler(nil)),
			httpapi.WithVaultHandler(httpapi.NewVaultHandler(nil)),
			httpapi.WithSkillHandler(httpapi.NewSkillHandler(nil, "")),
			httpapi.WithFileHandler(httpapi.NewFileHandler(nil, "", httpapi.FileHandlerLimits{})),
			httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(nil)),
			httpapi.WithSessionEventHandler(httpapi.NewSessionEventHandler(nil)),
			httpapi.WithSessionEventListHandler(eventstream.NewListHandler(nil))),
		"sse": publicstream.NewRouter(nil, nil),
	}
	for name, router := range routers {
		t.Run(name, func(t *testing.T) {
			if err := checkPublicRouteCoverage(router); err != nil {
				t.Fatal(err)
			}
		})
	}
	unclassified := chi.NewRouter()
	unclassified.Get("/v1/new_business", func(http.ResponseWriter, *http.Request) {})
	if err := checkPublicRouteCoverage(unclassified); err == nil {
		t.Fatal("unclassified registration was accepted")
	}
	invalid := chi.NewRouter()
	handler := httpapi.DeclarePublicOperation(http.MethodGet, "/v1/models", func(http.ResponseWriter, *http.Request) {})
	handler.(*httpapi.PublicOperationHandler).Operation = "unknown.operation"
	invalid.Method(http.MethodGet, "/v1/models", handler)
	if err := checkPublicRouteCoverage(invalid); err == nil {
		t.Fatal("unknown operation was accepted")
	}
}

func identityAdmissionFixture(kind string) auth.Principal {
	ws := workspace.Workspace{ID: workspace.DefaultID}
	return auth.Principal{Workspace: ws, Identity: &auth.Identity{ID: "identity_" + kind, Kind: kind}, Credential: auth.Credential{Kind: auth.CredentialAccessToken, ID: "token_fixture_" + kind}, Authority: auth.Authority{Kind: auth.AuthorityIdentityGrant, RuleID: "rule_fixture", IdentityID: "identity_" + kind, GrantID: "grant_fixture", RuleRevision: 1, IdentityRevision: 1, GrantRevision: 1, PolicyVersion: auth.PolicyVersion, Operations: auth.RegisteredOperations(), Scope: auth.ResourceReference{WorkspaceID: ws.ID, Type: "workspace"}}}
}

func signedMemoryRouter(t *testing.T, service *memory.Service, options ...httpapi.RouterOption) (http.Handler, *auth.InternalPrincipalSigner) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	options = append(options, httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)), httpapi.WithInternalPrincipalVerifier(verifier))
	return httpapi.NewRouter(nil, "", options...), signer
}

func signedPublicCall(t *testing.T, router http.Handler, signer *auth.InternalPrincipalSigner, principal auth.Principal, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	token, err := signer.Mint(principal, method, request.URL.Path, "req_actor_fixture", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	// Public header actor/scope claims cannot change a verified principal.
	request.Header.Set("X-Tetral-Workspace-ID", "workspace_forged")
	request.Header.Set("X-Tetral-API-Key-ID", "ak_forged")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func decodeHTTPResult[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", response.Code, response.Body.String())
	}
	var result T
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPostgreSQLPublicMemoryTypedActors(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router, signer := signedMemoryRouter(t, service)
	key, err := auth.NewAuthorityResolver(env.runtime, "", nil).AuthenticateKey(context.Background(), env.envKey)
	if err != nil {
		t.Fatal(err)
	}
	derived := identityAdmissionFixture(auth.IdentityService)
	derived.Credential = auth.Credential{Kind: auth.CredentialAPIKey, ID: key.APIKeyID}
	derived.APIKeyID = key.APIKeyID
	actors := []struct {
		name      string
		principal auth.Principal
		want      memory.Actor
	}{
		{"key", key, memory.Actor{Type: memory.ActorAPI, APIKeyID: key.APIKeyID}},
		{"derived key", derived, memory.Actor{Type: memory.ActorAPI, APIKeyID: key.APIKeyID}},
		{"human", identityAdmissionFixture(auth.IdentityHuman), memory.Actor{Type: memory.ActorUser, UserID: "identity_human"}},
		{"service", identityAdmissionFixture(auth.IdentityService), memory.Actor{Type: memory.ActorService, ServiceID: "identity_service"}},
	}
	for _, actor := range actors {
		t.Run(actor.name, func(t *testing.T) {
			call := func(method, path, body string) *httptest.ResponseRecorder {
				return signedPublicCall(t, router, signer, actor.principal, method, path, body)
			}
			store := decodeHTTPResult[memory.Store](t, call(http.MethodPost, "/v1/memory_stores?beta=true", `{"name":"actor fixture"}`))
			base := "/v1/memory_stores/" + store.ID
			decodeHTTPResult[memory.Store](t, call(http.MethodGet, base+"?beta=true", ""))
			decodeHTTPResult[memory.StoreListResult](t, call(http.MethodGet, "/v1/memory_stores?beta=true", ""))
			created := decodeHTTPResult[memory.Memory](t, call(http.MethodPost, base+"/memories?beta=true&view=full", `{"path":"/actor.txt","content":"before"}`))
			updated := decodeHTTPResult[memory.Memory](t, call(http.MethodPost, base+"/memories/"+created.ID+"?beta=true&view=full", `{"content":"after"}`))
			decodeHTTPResult[memory.Memory](t, call(http.MethodGet, base+"/memories/"+created.ID+"?beta=true", ""))
			decodeHTTPResult[memory.MemoryListResult](t, call(http.MethodGet, base+"/memories?beta=true", ""))
			first := decodeHTTPResult[memory.MemoryVersion](t, call(http.MethodGet, base+"/memory_versions/"+created.MemoryVersionID+"?beta=true", ""))
			if first.CreatedBy != actor.want {
				t.Fatalf("created actor=%+v want=%+v", first.CreatedBy, actor.want)
			}
			redacted := decodeHTTPResult[memory.MemoryVersion](t, call(http.MethodPost, base+"/memory_versions/"+created.MemoryVersionID+"/redact?beta=true", ""))
			if redacted.RedactedBy == nil || *redacted.RedactedBy != actor.want {
				t.Fatalf("redacted actor=%+v want=%+v", redacted.RedactedBy, actor.want)
			}
			decodeHTTPResult[memory.DeleteMemoryResult](t, call(http.MethodDelete, base+"/memories/"+created.ID+"?beta=true", ""))
			versions := decodeHTTPResult[memory.MemoryVersionListResult](t, call(http.MethodGet, base+"/memory_versions?beta=true", ""))
			if len(versions.Data) != 3 {
				t.Fatalf("versions=%d want 3", len(versions.Data))
			}
			for _, version := range versions.Data {
				if version.CreatedBy != actor.want {
					t.Fatalf("persisted actor=%+v want=%+v", version.CreatedBy, actor.want)
				}
			}
			var createdType string
			var api, user, serviceID *string
			if err := env.admin.QueryRowContext(t.Context(), `SELECT created_actor_type,created_api_key_id,created_user_id,created_service_id FROM memory_versions WHERE memory_version_id=$1`, updated.MemoryVersionID).Scan(&createdType, &api, &user, &serviceID); err != nil {
				t.Fatal(err)
			}
			got := memory.Actor{Type: createdType}
			if api != nil {
				got.APIKeyID = *api
			}
			if user != nil {
				got.UserID = *user
			}
			if serviceID != nil {
				got.ServiceID = *serviceID
			}
			if got != actor.want {
				t.Fatalf("SQL actor=%+v want=%+v", got, actor.want)
			}
			// The database independently enforces exclusive attribution on both
			// creation and redaction, even when HTTP validation is bypassed.
			createdAssignment := "UPDATE memory_versions SET created_service_id='forged_service' WHERE memory_version_id=$1"
			redactedAssignment := "UPDATE memory_versions SET redacted_service_id='forged_service' WHERE memory_version_id=$1"
			if actor.want.Type == memory.ActorService {
				createdAssignment = "UPDATE memory_versions SET created_service_id=NULL WHERE memory_version_id=$1"
				redactedAssignment = "UPDATE memory_versions SET redacted_service_id=NULL WHERE memory_version_id=$1"
			}
			for _, rejected := range []struct{ statement, versionID string }{
				{createdAssignment, updated.MemoryVersionID},
				{redactedAssignment, created.MemoryVersionID},
				{"UPDATE memory_versions SET redacted_actor_type=NULL,redacted_api_key_id=NULL,redacted_session_id=NULL,redacted_user_id=NULL,redacted_service_id='forged_service' WHERE memory_version_id=$1", created.MemoryVersionID},
				{"UPDATE memory_versions SET created_actor_type='unknown_actor' WHERE memory_version_id=$1", updated.MemoryVersionID},
				{"UPDATE memory_versions SET redacted_actor_type='unknown_actor' WHERE memory_version_id=$1", created.MemoryVersionID},
				{"UPDATE memory_versions SET created_actor_type='service_actor',created_api_key_id=NULL,created_session_id=NULL,created_user_id=NULL,created_service_id=NULL WHERE memory_version_id=$1", updated.MemoryVersionID},
				{"UPDATE memory_versions SET redacted_actor_type='service_actor',redacted_api_key_id=NULL,redacted_session_id=NULL,redacted_user_id=NULL,redacted_service_id=NULL WHERE memory_version_id=$1", created.MemoryVersionID},
				{"UPDATE memory_versions SET created_actor_type='service_actor',created_api_key_id=NULL,created_session_id=NULL,created_user_id='extra_user',created_service_id='forged_service' WHERE memory_version_id=$1", updated.MemoryVersionID},
				{"UPDATE memory_versions SET redacted_actor_type='service_actor',redacted_api_key_id=NULL,redacted_session_id=NULL,redacted_user_id='extra_user',redacted_service_id='forged_service' WHERE memory_version_id=$1", created.MemoryVersionID},
			} {
				_, err := env.admin.ExecContext(t.Context(), rejected.statement, rejected.versionID)
				var constraint *pgconn.PgError
				if !errors.As(err, &constraint) || constraint.Code != "23514" {
					t.Fatalf("actor constraint result=%v; want check violation", err)
				}
			}
		})
	}
	// Runtime Session attribution remains its own unchanged actor variant.
	store, err := service.CreateStore(t.Context(), workspace.DefaultID, memory.CreateStoreRequest{Name: "session actor"})
	if err != nil {
		t.Fatal(err)
	}
	expected := memory.Actor{Type: memory.ActorSession, SessionID: "sesn_actor_control"}
	created, err := service.CreateMemory(t.Context(), workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/session.txt", Content: "session", ContentSet: true}, expected)
	if err != nil {
		t.Fatal(err)
	}
	version, err := service.GetMemoryVersion(t.Context(), workspace.DefaultID, store.ID, created.MemoryVersionID, memory.ViewFull)
	if err != nil {
		t.Fatal(err)
	}
	if version.CreatedBy != expected {
		t.Fatalf("session actor=%+v", version.CreatedBy)
	}
}

func TestPublicAuthorizationMemoryDeniesBeforeDurableEffects(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	var diagnosticOutput bytes.Buffer
	processLogger := workload.NewProcessLogger(&diagnosticOutput, "api", "test", "unit", workload.DefaultDiagnosticConfig())
	defer processLogger.CloseWithBudget()
	router, signer := signedMemoryRouter(t, service, httpapi.WithLogger(processLogger.Logger))
	full := identityAdmissionFixture(auth.IdentityHuman)
	call := func(principal auth.Principal, method, path, body string) *httptest.ResponseRecorder {
		return signedPublicCall(t, router, signer, principal, method, path, body)
	}
	store := decodeHTTPResult[memory.Store](t, call(full, http.MethodPost, "/v1/memory_stores?beta=true", `{"name":"restricted fixture"}`))
	base := "/v1/memory_stores/" + store.ID
	created := decodeHTTPResult[memory.Memory](t, call(full, http.MethodPost, base+"/memories?beta=true", `{"path":"/restricted.txt","content":"before"}`))
	decodeHTTPResult[memory.Memory](t, call(full, http.MethodPost, base+"/memories/"+created.ID+"?beta=true", `{"content":"current"}`))
	// This is a signed bounded admission fixture, not an assignable product role.
	restricted := identityAdmissionFixture(auth.IdentityHuman)
	restricted.Authority.Operations = []auth.Operation{auth.OperationModelsList}
	before, err := service.ListMemoryVersions(t.Context(), workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{View: memory.ViewFull})
	if err != nil {
		t.Fatal(err)
	}
	var storesBefore int
	if err := env.admin.QueryRowContext(t.Context(), `SELECT count(*) FROM memory_stores`).Scan(&storesBefore); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/memory_stores?beta=true", `{"name":"denied store"}`},
		{http.MethodGet, "/v1/memory_stores?beta=true", ""},
		{http.MethodGet, base + "?beta=true", ""},
		{http.MethodPost, base + "?beta=true", `{"name":"denied update"}`},
		{http.MethodPost, base + "/archive?beta=true", ""},
		{http.MethodDelete, base + "?beta=true", ""},
		{http.MethodPost, base + "/memories?beta=true", `{"path":"/denied.txt","content":"denied"}`},
		{http.MethodGet, base + "/memories?beta=true", ""},
		{http.MethodGet, base + "/memories/" + created.ID + "?beta=true", ""},
		{http.MethodPost, base + "/memories/" + created.ID + "?beta=true", `{"content":"denied"}`},
		{http.MethodDelete, base + "/memories/" + created.ID + "?beta=true", ""},
		{http.MethodGet, base + "/memory_versions?beta=true", ""},
		{http.MethodGet, base + "/memory_versions/" + created.MemoryVersionID + "?beta=true", ""},
		{http.MethodPost, base + "/memory_versions/" + created.MemoryVersionID + "/redact?beta=true", ""},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			response := call(restricted, test.method, test.path, test.body)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			assertErrorType(t, response, "permission_error")
			if strings.Contains(response.Body.String(), "before") || strings.Contains(response.Body.String(), "current") {
				t.Fatal("denial disclosed stored content")
			}
		})
	}
	for _, missing := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/memory_stores/memstore_absent?beta=true", ""},
		{http.MethodGet, base + "/memories/mem_absent?beta=true", ""},
		{http.MethodPost, base + "/memories/mem_absent?beta=true", `{"content":"denied"}`},
		{http.MethodDelete, base + "/memories/mem_absent?beta=true", ""},
		{http.MethodGet, base + "/memory_versions/memver_absent?beta=true", ""},
		{http.MethodPost, base + "/memory_versions/memver_absent/redact?beta=true", ""},
	} {
		response := call(restricted, missing.method, missing.path, missing.body)
		if response.Code != http.StatusForbidden {
			t.Fatalf("absent denial status=%d body=%s", response.Code, response.Body.String())
		}
	}
	after, err := service.ListMemoryVersions(t.Context(), workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{View: memory.ViewFull})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("denied operations changed durable memory versions")
	}
	var storesAfter int
	if err := env.admin.QueryRowContext(t.Context(), `SELECT count(*) FROM memory_stores`).Scan(&storesAfter); err != nil {
		t.Fatal(err)
	}
	if storesBefore != storesAfter {
		t.Fatal("denied store operation changed durable count")
	}
	env.seedWorkspace(t, "workspace_b", "foreign")
	foreign, err := service.CreateStore(t.Context(), "workspace_b", memory.CreateStoreRequest{Name: "foreign"})
	if err != nil {
		t.Fatal(err)
	}
	for _, principal := range []auth.Principal{full, restricted} {
		response := call(principal, http.MethodGet, "/v1/memory_stores/"+foreign.ID+"?beta=true", "")
		want := http.StatusNotFound
		if len(principal.Authority.Operations) == 1 {
			want = http.StatusForbidden
		}
		if response.Code != want {
			t.Fatalf("foreign status=%d body=%s", response.Code, response.Body.String())
		}
	}
	other, err := service.CreateStore(t.Context(), workspace.DefaultID, memory.CreateStoreRequest{Name: "other parent"})
	if err != nil {
		t.Fatal(err)
	}
	response := call(full, http.MethodGet, "/v1/memory_stores/"+other.ID+"/memories/"+created.ID+"?beta=true", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("wrong parent status=%d body=%s", response.Code, response.Body.String())
	}
	processLogger.CloseWithBudget()
	found := false
	for _, line := range bytes.Split(bytes.TrimSpace(diagnosticOutput.Bytes()), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if event["event"] != "auth.operation.denied" {
			continue
		}
		found = true
		if value, ok := event["auth.operation"].(string); !ok || auth.Operation(value) != auth.OperationMemoryStoresCreate {
			t.Fatal("operation denial omitted actual registered action")
		}
		if event["auth.stage"] != "operation" || event["auth.result"] != "denied" || event["auth.identity.kind"] != auth.IdentityHuman {
			t.Fatal("signed operation denial lost trusted classification")
		}
		for _, field := range []string{"auth.rule.revision", "auth.identity.revision", "auth.grant.revision"} {
			if event[field] != float64(1) {
				t.Fatalf("signed operation missing trusted %s", field)
			}
		}
	}
	if !found {
		t.Fatal("restricted production gate denied silently")
	}
	for _, forbidden := range []string{"identity_human", "token_fixture_", "rule_fixture", "grant_fixture", created.ID, store.ID, "workspace_forged", "ak_forged"} {
		if strings.Contains(diagnosticOutput.String(), forbidden) {
			t.Fatal("operation diagnostic leaked identities/selectors")
		}
	}

}

func TestPublicAuthorizationPreservesIngressValidationBeforeLookup(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	facts := &sessionEventHTTPFacts{}
	service := &recordingSessionEventHTTPService{}
	router := httpapi.NewRouter(httpapi.NewSessionHandler(facts), "",
		httpapi.WithSessionEventHandler(httpapi.NewSessionEventHandler(service)),
		httpapi.WithSkillHandler(httpapi.NewSkillHandler(nil, t.TempDir())),
		httpapi.WithInternalPrincipalVerifier(verifier))
	full := identityAdmissionFixture(auth.IdentityHuman)
	restricted := identityAdmissionFixture(auth.IdentityHuman)
	restricted.Authority.Operations = []auth.Operation{auth.OperationModelsList}
	for _, test := range []struct{ name, path, body, idempotency string }{
		{"event body", "/v1/sessions/sesn_fixture/events?beta=true", "{", ""},
		{"event header", "/v1/sessions/sesn_fixture/events?beta=true", `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`, " "},
		{"skill multipart", "/v1/skills/skill_fixture/versions?beta=true", "malformed upload", ""},
	} {
		for _, principal := range []auth.Principal{full, restricted} {
			t.Run(test.name+"/"+fmt.Sprint(len(principal.Authority.Operations)), func(t *testing.T) {
				request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
				if test.idempotency != "" {
					request.Header.Set("Idempotency-Key", test.idempotency)
				}
				token, err := signer.Mint(principal, request.Method, request.URL.Path, "req_validation_fixture", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("X-Tetral-Internal-Principal", token)
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)
				want := http.StatusBadRequest
				if len(principal.Authority.Operations) == 1 {
					want = http.StatusForbidden
				}
				if response.Code != want {
					t.Fatalf("status=%d body=%s; want %d", response.Code, response.Body.String(), want)
				}
				if facts.lookupCalls != 0 || len(service.calls) != 0 {
					t.Fatalf("validation/denial reached lookup or effects: lookups=%d effects=%d", facts.lookupCalls, len(service.calls))
				}
			})
		}
	}
	// An implemented ingress without its Session owner cannot normalize missing
	// facts to a workspace resource and invoke the mutator.
	misconfigured := httpapi.NewRouter(nil, "", httpapi.WithSessionEventHandler(httpapi.NewSessionEventHandler(service)), httpapi.WithInternalPrincipalVerifier(verifier))
	response := signedPublicCall(t, misconfigured, signer, full, http.MethodPost, "/v1/sessions/sesn_fixture/events?beta=true", `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`)
	if response.Code != http.StatusInternalServerError || len(service.calls) != 0 {
		t.Fatalf("missing owner status=%d effects=%d", response.Code, len(service.calls))
	}
}

// sessionGateOwner logs every Session owner call in order, so the registered
// gate's lookup is distinguishable from the handler's later business call.
type sessionGateOwner struct {
	fakeSessionService
	calls     []string
	lookupErr error
}

func (s *sessionGateOwner) record(name string, ws workspace.ID, ids ...string) {
	s.calls = append(s.calls, name+" "+string(ws)+" "+strings.Join(ids, "/"))
}

func (s *sessionGateOwner) lookup(name string, ws workspace.ID, ids ...string) (string, error) {
	s.record(name, ws, ids...)
	if s.lookupErr != nil {
		return "", s.lookupErr
	}
	return ids[len(ids)-1], nil
}

func (s *sessionGateOwner) LookupSession(_ context.Context, ws workspace.ID, sessionID string) (string, error) {
	return s.lookup("LookupSession", ws, sessionID)
}

func (s *sessionGateOwner) LookupThread(_ context.Context, ws workspace.ID, sessionID, threadID string) (string, error) {
	return s.lookup("LookupThread", ws, sessionID, threadID)
}

func (s *sessionGateOwner) LookupResource(_ context.Context, ws workspace.ID, sessionID, resourceID string) (string, error) {
	return s.lookup("LookupResource", ws, sessionID, resourceID)
}

func (s *sessionGateOwner) LookupSessionDeletion(_ context.Context, ws workspace.ID, sessionID string) (string, error) {
	return s.lookup("LookupSessionDeletion", ws, sessionID)
}

func (s *sessionGateOwner) LookupResourceDeletion(_ context.Context, ws workspace.ID, sessionID, resourceID string) (string, error) {
	return s.lookup("LookupResourceDeletion", ws, sessionID, resourceID)
}

func (s *sessionGateOwner) Create(ctx context.Context, ws workspace.ID, request session.CreateRequest) (*session.Response, error) {
	s.record("Create", ws)
	return s.fakeSessionService.Create(ctx, ws, request)
}

func (s *sessionGateOwner) Get(ctx context.Context, ws workspace.ID, sessionID string) (*session.Response, error) {
	s.record("Get", ws, sessionID)
	return s.fakeSessionService.Get(ctx, ws, sessionID)
}

func (s *sessionGateOwner) List(ctx context.Context, ws workspace.ID, options session.ListOptions) (*session.ListResult, error) {
	s.record("List", ws)
	return s.fakeSessionService.List(ctx, ws, options)
}

func (s *sessionGateOwner) ListThreads(ctx context.Context, ws workspace.ID, sessionID string, options session.ThreadListOptions) (*session.ThreadListResult, error) {
	s.record("ListThreads", ws, sessionID)
	return s.fakeSessionService.ListThreads(ctx, ws, sessionID, options)
}

func (s *sessionGateOwner) GetThread(ctx context.Context, ws workspace.ID, sessionID, threadID string) (*session.ThreadResponse, error) {
	s.record("GetThread", ws, sessionID, threadID)
	return s.fakeSessionService.GetThread(ctx, ws, sessionID, threadID)
}

func (s *sessionGateOwner) ArchiveThread(ctx context.Context, ws workspace.ID, sessionID, threadID string) (*session.ThreadResponse, error) {
	s.record("ArchiveThread", ws, sessionID, threadID)
	return s.fakeSessionService.ArchiveThread(ctx, ws, sessionID, threadID)
}

func (s *sessionGateOwner) Update(ctx context.Context, ws workspace.ID, sessionID string, request session.UpdateRequest) (*session.Response, error) {
	s.record("Update", ws, sessionID)
	return s.fakeSessionService.Update(ctx, ws, sessionID, request)
}

func (s *sessionGateOwner) Archive(ctx context.Context, ws workspace.ID, sessionID string) (*session.Response, error) {
	s.record("Archive", ws, sessionID)
	return s.fakeSessionService.Archive(ctx, ws, sessionID)
}

func (s *sessionGateOwner) Delete(ctx context.Context, ws workspace.ID, sessionID string) (*session.DeleteResponse, error) {
	s.record("Delete", ws, sessionID)
	return s.fakeSessionService.Delete(ctx, ws, sessionID)
}

func (s *sessionGateOwner) AddResource(ctx context.Context, ws workspace.ID, sessionID string, request session.ResourceRequest) (*session.ResourceResponse, error) {
	s.record("AddResource", ws, sessionID)
	return s.fakeSessionService.AddResource(ctx, ws, sessionID, request)
}

func (s *sessionGateOwner) ListResources(ctx context.Context, ws workspace.ID, sessionID string, options session.ResourceListOptions) (*session.ResourceListResult, error) {
	s.record("ListResources", ws, sessionID)
	return s.fakeSessionService.ListResources(ctx, ws, sessionID, options)
}

func (s *sessionGateOwner) GetResource(ctx context.Context, ws workspace.ID, sessionID, resourceID string) (*session.ResourceResponse, error) {
	s.record("GetResource", ws, sessionID, resourceID)
	return s.fakeSessionService.GetResource(ctx, ws, sessionID, resourceID)
}

func (s *sessionGateOwner) UpdateResource(ctx context.Context, ws workspace.ID, sessionID, resourceID, token string) (*session.ResourceResponse, error) {
	s.record("UpdateResource", ws, sessionID, resourceID)
	return s.fakeSessionService.UpdateResource(ctx, ws, sessionID, resourceID, token)
}

func (s *sessionGateOwner) DeleteResource(ctx context.Context, ws workspace.ID, sessionID, resourceID string) (*session.ResourceDeleteResponse, error) {
	s.record("DeleteResource", ws, sessionID, resourceID)
	return s.fakeSessionService.DeleteResource(ctx, ws, sessionID, resourceID)
}

// sessionGateEvents is the event admission and event-list owner on the same log.
type sessionGateEvents struct{ owner *sessionGateOwner }

func (e sessionGateEvents) AppendClientEvents(_ context.Context, ws workspace.ID, sessionID string, _ string, _ sessionevent.AppendRequest) (*sessionevent.AppendResult, error) {
	e.owner.record("AppendClientEvents", ws, sessionID)
	return &sessionevent.AppendResult{}, nil
}

func (e sessionGateEvents) ListSessionEvents(_ context.Context, ws workspace.ID, sessionID string, _ eventstream.ListOptions) (eventstream.ListResult, error) {
	e.owner.record("ListSessionEvents", ws, sessionID)
	return eventstream.ListResult{}, nil
}

func (e sessionGateEvents) ListThreadEvents(_ context.Context, ws workspace.ID, sessionID, threadID string, _ eventstream.ListOptions) (eventstream.ListResult, error) {
	e.owner.record("ListThreadEvents", ws, sessionID, threadID)
	return eventstream.ListResult{}, nil
}

// Every registered Session-family route, through the production router and
// gate: an existing-target mutation resolves its target with exactly one
// identity-only lookup before its business call, never with the full public
// Get/GetThread/GetResource; reads and creation use no lookup. A failed lookup
// keeps its owner's error class and never reaches the business call.
func TestPublicAuthorizationSessionRoutesResolveIdentityOnly(t *testing.T) {
	owner := &sessionGateOwner{}
	authenticator := httpapi.WithAuthenticator(auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID}, "ak_session_gate"), nil
	}))
	events := sessionGateEvents{owner: owner}
	router := httpapi.NewRouter(httpapi.NewSessionHandler(owner), "", authenticator,
		httpapi.WithSessionEventHandler(httpapi.NewSessionEventHandler(events)),
		httpapi.WithSessionEventListHandler(eventstream.NewListHandler(events)))
	stubEvents := httpapi.NewRouter(httpapi.NewSessionHandler(owner), "", authenticator)
	const (
		sessionPath  = "/v1/sessions/sesn_gate"
		threadPath   = sessionPath + "/threads/thread_gate"
		resourcePath = sessionPath + "/resources/sesrsc_gate"
	)
	call := func(name string, ids ...string) string {
		return name + " " + string(workspace.DefaultID) + " " + strings.Join(ids, "/")
	}
	sessionOnly := []string{"sesn_gate"}
	threadIDs := []string{"sesn_gate", "thread_gate"}
	resourceIDs := []string{"sesn_gate", "sesrsc_gate"}
	routes := []struct {
		name, method, path, body string
		router                   http.Handler
		lookup                   string // empty when the route performs no lookup
		business                 string
		status                   int
	}{
		{"create", http.MethodPost, "/v1/sessions", `{"agent":"agent_gate","environment_id":"env_gate","vault_ids":[]}`, router, "", call("Create"), http.StatusOK},
		{"list", http.MethodGet, "/v1/sessions", "", router, "", call("List"), http.StatusOK},
		{"retrieve", http.MethodGet, sessionPath, "", router, "", call("Get", sessionOnly...), http.StatusOK},
		{"update", http.MethodPost, sessionPath, `{"title":"gate"}`, router, call("LookupSession", sessionOnly...), call("Update", sessionOnly...), http.StatusOK},
		{"archive", http.MethodPost, sessionPath + "/archive", "", router, call("LookupSession", sessionOnly...), call("Archive", sessionOnly...), http.StatusOK},
		{"delete", http.MethodDelete, sessionPath, "", router, call("LookupSessionDeletion", sessionOnly...), call("Delete", sessionOnly...), http.StatusOK},
		{"append events", http.MethodPost, sessionPath + "/events", `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`, router, call("LookupSession", sessionOnly...), call("AppendClientEvents", sessionOnly...), http.StatusOK},
		{"append events stub", http.MethodPost, sessionPath + "/events", `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`, stubEvents, "", "", http.StatusNotImplemented},
		{"list events", http.MethodGet, sessionPath + "/events", "", router, "", call("ListSessionEvents", sessionOnly...), http.StatusOK},
		{"list threads", http.MethodGet, sessionPath + "/threads", "", router, "", call("ListThreads", sessionOnly...), http.StatusOK},
		{"retrieve thread", http.MethodGet, threadPath, "", router, "", call("GetThread", threadIDs...), http.StatusOK},
		{"list thread events", http.MethodGet, threadPath + "/events", "", router, "", call("ListThreadEvents", threadIDs...), http.StatusOK},
		{"archive thread", http.MethodPost, threadPath + "/archive", "", router, call("LookupThread", threadIDs...), call("ArchiveThread", threadIDs...), http.StatusOK},
		{"add resource", http.MethodPost, sessionPath + "/resources", `{"type":"file","file_id":"file_gate","mount_path":"/workspace/gate.txt"}`, router, call("LookupSession", sessionOnly...), call("AddResource", sessionOnly...), http.StatusOK},
		{"list resources", http.MethodGet, sessionPath + "/resources", "", router, "", call("ListResources", sessionOnly...), http.StatusOK},
		{"retrieve resource", http.MethodGet, resourcePath, "", router, "", call("GetResource", resourceIDs...), http.StatusOK},
		{"update resource", http.MethodPost, resourcePath, `{"authorization_token":"gate_token"}`, router, call("LookupResource", resourceIDs...), call("UpdateResource", resourceIDs...), http.StatusOK},
		{"delete resource", http.MethodDelete, resourcePath, "", router, call("LookupResourceDeletion", resourceIDs...), call("DeleteResource", resourceIDs...), http.StatusOK},
	}
	serve := func(t *testing.T, route http.Handler, method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		setAuthHeader(request)
		response := httptest.NewRecorder()
		route.ServeHTTP(response, request)
		return response
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			owner.calls, owner.lookupErr = nil, nil
			response := serve(t, route.router, route.method, route.path, route.body)
			if response.Code != route.status {
				t.Fatalf("status=%d body=%s; want %d", response.Code, response.Body.String(), route.status)
			}
			var want []string
			for _, step := range []string{route.lookup, route.business} {
				if step != "" {
					want = append(want, step)
				}
			}
			if !reflect.DeepEqual(owner.calls, want) {
				t.Fatalf("owner calls = %q; want %q", owner.calls, want)
			}
			if route.lookup == "" {
				return
			}
			for _, failure := range []struct {
				err    error
				status int
			}{
				{&session.NotFoundError{Message: "session not found"}, http.StatusNotFound},
				{errors.New("lookup store unavailable"), http.StatusInternalServerError},
			} {
				owner.calls, owner.lookupErr = nil, failure.err
				response := serve(t, route.router, route.method, route.path, route.body)
				if response.Code != failure.status || !reflect.DeepEqual(owner.calls, []string{route.lookup}) {
					t.Fatalf("lookup error %T: status=%d calls=%q; want %d and only %q", failure.err, response.Code, owner.calls, failure.status, route.lookup)
				}
			}
		})
	}
}
