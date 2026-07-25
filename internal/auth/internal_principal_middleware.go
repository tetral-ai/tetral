package auth

import (
	"net/http"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func InternalPrincipalMiddleware(verifier *InternalPrincipalVerifier, errorWriter ErrorWriter, _ requestIDExtractor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("X-Tetral-Internal-Principal")
			if token == "" {
				errorWriter(w, r, &AuthenticationError{Message: "missing internal principal"})
				return
			}
			principal, _, err := verifier.Verify(token, r.Method, r.URL.Path)
			if err != nil {
				errorWriter(w, r, err)
				return
			}
			ctx := WithPrincipal(r.Context(), principal)
			ctx = workspace.WithContext(ctx, principal.Workspace)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
