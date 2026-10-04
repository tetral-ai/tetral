package tetralauth

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
)

func TestAuthPublicOperationRouteCoverage(t *testing.T) {
	// Walk actual registrations; introducing a business leaf without the shared
	// production authorizer wrapper must fail even if a copied route list omits it.
	router := NewRouter(RouterConfig{})
	checked := 0
	err := chi.Walk(router.(chi.Routes), func(method, path string, handler http.Handler, _ ...func(http.Handler) http.Handler) error {
		if method == http.MethodPost && path == "/internal/auth/authorize" {
			return nil
		}
		if method == http.MethodPost && path == "/v1/oauth/token" {
			if _, ok := handler.(*exchangeHandler); !ok {
				return fmt.Errorf("exchange route lost its exact public boundary")
			}
			return nil
		}
		classified, ok := handler.(*httpapi.PublicOperationHandler)
		if !ok {
			return fmt.Errorf("unclassified Auth business route %s %s", method, path)
		}
		operation, ok := auth.OperationForRoute(method, path)
		if !ok || operation != classified.Operation || classified.Method != method || classified.Pattern != path || !auth.IsRegisteredOperation(operation) {
			return fmt.Errorf("invalid Auth operation classification %s %s", method, path)
		}
		checked++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("Auth business routes were not inspected")
	}
}
