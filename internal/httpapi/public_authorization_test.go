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
		"standalone lists": eventstream.NewListRouter(nil, nil),
		"sse":              publicstream.NewRouter(nil, nil),
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
	key, err := env.store.AuthenticateRawKey(context.Background(), env.envKey)
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
		{"key", auth.IndependentKeyPrincipal(key.Workspace, key.APIKeyID), memory.Actor{Type: memory.ActorAPI, APIKeyID: key.APIKeyID}},
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
				if facts.getCalls != 0 || len(service.calls) != 0 {
					t.Fatalf("validation/denial reached lookup or effects: reads=%d effects=%d", facts.getCalls, len(service.calls))
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
