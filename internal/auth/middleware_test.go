package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// staticAuthenticator is a test-only authenticator that resolves a
// fixed key to a fixed workspace.
type staticAuthenticator struct {
	expected string
	resolved auth.Principal
}

func (s staticAuthenticator) Authenticate(_ context.Context, raw string) (auth.Principal, error) {
	if raw != s.expected {
		return auth.Principal{}, &auth.AuthenticationError{Message: "invalid api key"}
	}
	return s.resolved, nil
}

// errorWriterRecorder captures the typed error passed to the
// middleware's ErrorWriter so tests can assert on it without pulling
// in httpapi's writeError.
type errorWriterRecorder struct {
	calls []error
	body  string
	code  int
}

func (r *errorWriterRecorder) writeError(w http.ResponseWriter, _ *http.Request, err error) {
	r.calls = append(r.calls, err)
	r.code = http.StatusUnauthorized
	r.body = err.Error()
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(err.Error()))
}

type recordingAudit struct {
	events []auth.AuditEvent
}

func (r *recordingAudit) RecordAuthEvent(_ context.Context, event auth.AuditEvent) {
	r.events = append(r.events, event)
}

func TestMiddlewareRejectsMissingHeader(t *testing.T) {
	rec := &errorWriterRecorder{}
	audit := &recordingAudit{}
	authFn := staticAuthenticator{expected: "secret", resolved: auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}}
	handler := auth.MiddlewareWithAudit(authFn, rec.writeError, nil, audit)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("downstream handler must not run when header is missing")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/things", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if len(rec.calls) != 1 {
		t.Fatalf("expected one writeError call, got %d", len(rec.calls))
	}
	var authErr *auth.AuthenticationError
	if !errors.As(rec.calls[0], &authErr) {
		t.Fatalf("error must be *auth.AuthenticationError, got %T", rec.calls[0])
	}
	if len(audit.events) != 1 || audit.events[0].Result != "failure" {
		t.Fatalf("audit events = %#v; want one failure", audit.events)
	}
}

func TestMiddlewareRejectsInvalidKey(t *testing.T) {
	rec := &errorWriterRecorder{}
	audit := &recordingAudit{}
	authFn := staticAuthenticator{expected: "secret", resolved: auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}}
	handler := auth.MiddlewareWithAudit(authFn, rec.writeError, nil, audit)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("downstream handler must not run for invalid key")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/things", nil)
	req.Header.Set("x-api-key", "wrong")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if len(rec.calls) != 1 {
		t.Fatalf("expected one writeError call, got %d", len(rec.calls))
	}
	var authErr *auth.AuthenticationError
	if !errors.As(rec.calls[0], &authErr) {
		t.Fatalf("error must be *auth.AuthenticationError, got %T", rec.calls[0])
	}
	if len(audit.events) != 1 || audit.events[0].Result != "failure" {
		t.Fatalf("audit events = %#v; want one failure", audit.events)
	}
	if strings.Contains(audit.events[0].Path, "wrong") || strings.Contains(audit.events[0].Path, "secret") {
		t.Fatalf("audit path leaked key material: %#v", audit.events[0])
	}
}

func TestMiddlewareIgnoresQueryStringAPIKey(t *testing.T) {
	rec := &errorWriterRecorder{}
	authFn := staticAuthenticator{expected: "secret", resolved: auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}}
	handler := auth.Middleware(authFn, rec.writeError, nil)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("downstream handler must not run when key is in query string only")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/things?api_key=secret", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if len(rec.calls) != 1 {
		t.Fatalf("expected query-string key to be rejected; got %d writeError calls", len(rec.calls))
	}
}

func TestMiddlewareAttachesWorkspaceAndPrincipalOnSuccess(t *testing.T) {
	rec := &errorWriterRecorder{}
	audit := &recordingAudit{}
	expectedWorkspace := workspace.Workspace{
		ID: workspace.DefaultID, Type: "workspace", Name: "Default",
	}
	expectedPrincipal := auth.Principal{Workspace: expectedWorkspace, APIKeyID: "ak_context"}
	authFn := staticAuthenticator{expected: "secret", resolved: expectedPrincipal}
	called := false
	handler := auth.MiddlewareWithAudit(authFn, rec.writeError, nil, audit)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		got, ok := workspace.FromContext(r.Context())
		if !ok {
			t.Error("downstream handler did not see workspace in context")
			return
		}
		if got != expectedWorkspace {
			t.Errorf("workspace in context = %+v; want %+v", got, expectedWorkspace)
		}
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			t.Error("downstream handler did not see auth principal in context")
			return
		}
		if principal != expectedPrincipal {
			t.Errorf("principal in context = %+v; want %+v", principal, expectedPrincipal)
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/things", nil)
	req.Header.Set("x-api-key", "secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if !called {
		t.Fatal("downstream handler was not invoked on successful auth")
	}
	if len(rec.calls) != 0 {
		t.Errorf("writeError must not be called on success; got %d", len(rec.calls))
	}
	if len(audit.events) != 0 {
		t.Fatalf("success should not emit auth audit events by default; got %#v", audit.events)
	}
}

func TestMiddlewareDoesNotLogProvidedKeyOnSuccess(t *testing.T) {
	rec := &errorWriterRecorder{}
	audit := &recordingAudit{}
	authFn := staticAuthenticator{expected: "supersecret_key_material", resolved: auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_secret"}}
	handler := auth.MiddlewareWithAudit(authFn, rec.writeError, nil, audit)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/things", nil)
	req.Header.Set("x-api-key", "supersecret_key_material")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if len(audit.events) != 0 {
		t.Fatalf("success emitted audit events with key material risk: %#v", audit.events)
	}
}

func TestMiddlewarePassesThroughNonAuthErrors(t *testing.T) {
	rec := &errorWriterRecorder{}
	audit := &recordingAudit{}
	infraErr := errors.New("infra exploded with leaked-key-material")
	// Authenticator that returns a generic error (not AuthenticationError).
	authFn := auth.AuthenticatorFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{}, infraErr
	})
	handler := auth.MiddlewareWithAudit(authFn, rec.writeError, nil, audit)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("downstream handler must not run when auth errors")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/things", nil)
	req.Header.Set("x-api-key", "raw-key-material")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if len(rec.calls) != 1 {
		t.Fatalf("expected one writeError call, got %d", len(rec.calls))
	}
	var authErr *auth.AuthenticationError
	if errors.As(rec.calls[0], &authErr) {
		t.Fatalf("non-auth errors must not be normalized to *auth.AuthenticationError; got %T (%v)", rec.calls[0], rec.calls[0])
	}
	if !errors.Is(rec.calls[0], infraErr) {
		t.Fatalf("middleware must pass the original non-auth error to writeError; got %T (%v)", rec.calls[0], rec.calls[0])
	}
	if len(audit.events) != 1 || audit.events[0].Result != "error" || audit.events[0].ErrorType == "" {
		t.Fatalf("audit events = %#v; want one error with safe type", audit.events)
	}
	if strings.Contains(audit.events[0].Path, "raw-key-material") {
		t.Fatalf("audit event leaked raw x-api-key material: %#v", audit.events[0])
	}
	if strings.Contains(audit.events[0].ErrorType, "leaked-key-material") {
		t.Fatalf("audit event leaked arbitrary error text: %#v", audit.events[0])
	}
}
