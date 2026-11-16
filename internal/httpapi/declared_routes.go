package httpapi

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
)

// tokenExchangeRoute is the one public route outside auth's operation table.
const tokenExchangeRoute = "/v1/oauth/token" //nolint:gosec // G101: a route path, not credential material.

var declaredRoutes = sync.OnceValue(func() *chi.Mux {
	routes := chi.NewRouter()
	ignore := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, route := range auth.OperationRoutes() {
		routes.Method(route.Method, route.Pattern, ignore)
	}
	routes.Method(http.MethodPost, tokenExchangeRoute, ignore)
	return routes
})

var routeMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions}

// DeclaresOnlyOtherMethods reports whether Engine's public route table
// declares the request's path for other methods and not for its own: the
// case the OpenAPI file answers with 405.
func DeclaresOnlyOtherMethods(r *http.Request) bool {
	routes := declaredRoutes()
	if routes.Match(chi.NewRouteContext(), r.Method, r.URL.Path) {
		return false
	}
	for _, method := range routeMethods {
		if method != r.Method && routes.Match(chi.NewRouteContext(), method, r.URL.Path) {
			return true
		}
	}
	return false
}

// WriteMethodNotAllowed answers a declared path called with a method it does
// not declare.
func WriteMethodNotAllowed(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}
