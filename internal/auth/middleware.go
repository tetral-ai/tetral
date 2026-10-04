package auth

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type principalContextKey struct{}

// WithPrincipal attaches the authenticated principal to ctx.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext returns the authenticated principal attached by
// Middleware.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

// Authenticator resolves a raw API key string to the public-safe
// principal it belongs to. The middleware uses this interface so
// production wiring (an APIKeyStore-backed implementation) and test
// fakes share one shape. Authenticate must not log or echo rawKey.
type Authenticator interface {
	Authenticate(ctx context.Context, rawKey string) (Principal, error)
}

// AuthenticatorFunc adapts a plain function to the Authenticator
// interface. Useful for tests and for the static wrapper used by
// the legacy NewRouter signature.
type AuthenticatorFunc func(ctx context.Context, rawKey string) (Principal, error)

// Authenticate calls f.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, rawKey string) (Principal, error) {
	return f(ctx, rawKey)
}

// StoreAuthenticator adapts the shared resolver to local key-only routers.
type StoreAuthenticator struct {
	Store *APIKeyStore
}

// Authenticate preserves the typed authority returned by the current resolver.
func (a *StoreAuthenticator) Authenticate(ctx context.Context, rawKey string) (Principal, error) {
	if a == nil || a.Store == nil {
		return Principal{}, &AuthenticationError{Message: "authentication unavailable"}
	}
	result, err := a.Store.AuthenticateRawKey(ctx, rawKey)
	if err != nil {
		return Principal{}, err
	}
	return result.Principal, nil
}

// requestIDFromContext is the reflective accessor used by the audit
// log line. The httpapi package owns the request_id key but exposes
// it through an exported function.
type requestIDExtractor func(ctx context.Context) string

// AuditEvent is the public-safe authentication outcome emitted to the HTTP
// boundary logger. It intentionally carries no raw key material.
type AuditEvent struct {
	RequestID        string
	Method           string
	Path             string
	Result           string
	ErrorType        string
	Stage            string
	Operation        Operation
	Code             string
	IdentityKind     string
	RuleRevision     int64
	IdentityRevision int64
	GrantRevision    int64
}

// AuditRecorder receives public-safe auth events.
type AuditRecorder interface {
	RecordAuthEvent(context.Context, AuditEvent)
}

// Middleware constructs the HTTP middleware that authenticates
// `x-api-key` and attaches the resolved workspace.Workspace to the
// request context. The errorWriter parameter is supplied by the
// httpapi package so the middleware uses the same centralized error
// envelope as every other Engine handler. requestID extracts the
// chi request id from context for the audit log line; pass a noop
// returning "" if the caller does not propagate request ids.
//
// Successful admission stays quiet. Failure/error events carry no raw key
// material; the owning HTTP recorder uses the process logger suppression policy.
// Credential failures return authentication_error responses; unexpected
// authenticator/store failures keep their server-error semantics while
// logging only a safe error type marker.
func Middleware(authenticator Authenticator, errorWriter ErrorWriter, requestID requestIDExtractor) func(http.Handler) http.Handler {
	return MiddlewareWithAudit(authenticator, errorWriter, requestID, nil)
}

// MiddlewareWithAudit constructs the auth middleware and emits failures/errors
// through recorder when supplied. Successes are not logged by default.
func MiddlewareWithAudit(authenticator Authenticator, errorWriter ErrorWriter, requestID requestIDExtractor, recorder AuditRecorder) func(http.Handler) http.Handler {
	if requestID == nil {
		requestID = func(_ context.Context) string { return "" }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			id := requestID(ctx)
			provided := r.Header.Get("x-api-key")
			path := safeAuditPath(r)
			if provided == "" {
				recordAuthEvent(ctx, recorder, AuditEvent{RequestID: id, Method: r.Method, Path: path, Result: "failure"})
				errorWriter(w, r, &AuthenticationError{Message: "missing x-api-key header"})
				return
			}
			principal, err := authenticator.Authenticate(ctx, provided)
			if err == nil {
				err = principal.Validate()
			}
			if err != nil {
				var authErr *AuthenticationError
				if errors.As(err, &authErr) {
					recordAuthEvent(ctx, recorder, AuditEvent{RequestID: id, Method: r.Method, Path: path, Result: "failure"})
					errorWriter(w, r, authErr)
					return
				}
				recordAuthEvent(ctx, recorder, AuditEvent{RequestID: id, Method: r.Method, Path: path, Result: "error", ErrorType: safeErrorType(err)})
				errorWriter(w, r, err)
				return
			}
			ctx = WithPrincipal(ctx, principal)
			ctx = workspace.WithContext(ctx, principal.Workspace)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func recordAuthEvent(ctx context.Context, recorder AuditRecorder, event AuditEvent) {
	if recorder != nil {
		recorder.RecordAuthEvent(ctx, event)
	}
}

func safeErrorType(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimPrefix(reflect.TypeOf(err).String(), "*")
}

func safeAuditPath(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, "/v1/api_keys/") {
		return "/v1/api_keys/{api_key_id}"
	}
	return r.URL.Path
}

// ErrorWriter is the function shape used by httpapi.writeError. The
// auth package depends on the shape so it can report errors through
// the centralized HTTP envelope without importing httpapi.
type ErrorWriter func(w http.ResponseWriter, r *http.Request, err error)
