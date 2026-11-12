// Package httpapitest holds the checks that the tests of every service with
// public routes share; it is imported only by tests.
package httpapitest

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
)

// PublicRouteCoverage returns an error for the first registration of router,
// its health route apart, that is not a public operation the authorization
// table knows under that method and pattern.
func PublicRouteCoverage(router http.Handler) error {
	routes, ok := router.(chi.Routes)
	if !ok {
		return fmt.Errorf("router does not expose registrations")
	}
	return chi.Walk(routes, func(method, pattern string, handler http.Handler, _ ...func(http.Handler) http.Handler) error {
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
