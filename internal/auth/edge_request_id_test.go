package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// A verified principal's signed request_id is recorded as the edge request ID
// for both the handler and the enclosing request boundary. An incoming
// X-Request-Id header never supplies it, and a rejected principal records none.
func TestInternalPrincipalMiddlewareRecordsOnlyVerifiedEdgeRequestID(t *testing.T) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewInternalPrincipalVerifier(signer.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	principal := IndependentKeyPrincipal(workspace.Workspace{ID: "workspace-a"}, "ak_actual")
	valid, err := signer.Mint(principal, http.MethodGet, "/v1/sessions", "edge-request-signed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	otherPath, err := signer.Mint(principal, http.MethodGet, "/v1/agents", "edge-request-other-path", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	var handlerEdgeID string
	handlerCalls := 0
	handler := InternalPrincipalMiddleware(verifier, func(w http.ResponseWriter, _ *http.Request, _ error) {
		w.WriteHeader(http.StatusUnauthorized)
	}, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls++
		handlerEdgeID = EdgeRequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	serve := func(token string) (boundaryEdgeID string, status int) {
		request := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
		request.Header.Set("X-Request-Id", "edge-request-forged")
		if token != "" {
			request.Header.Set("X-Tetral-Internal-Principal", token)
		}
		ctx := WithEdgeRequestIDRecorder(request.Context())
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request.WithContext(ctx))
		return EdgeRequestIDFromContext(ctx), recorder.Code
	}

	boundaryEdgeID, status := serve(valid)
	if status != http.StatusNoContent || handlerCalls != 1 {
		t.Fatalf("verified principal status/calls = %d/%d; want 204/1", status, handlerCalls)
	}
	if handlerEdgeID != "edge-request-signed" || boundaryEdgeID != "edge-request-signed" {
		t.Fatalf("edge request ID handler/boundary = %q/%q; want the signed claim", handlerEdgeID, boundaryEdgeID)
	}

	for name, token := range map[string]string{"missing principal": "", "principal bound to another path": otherPath} {
		t.Run(name, func(t *testing.T) {
			boundaryEdgeID, status := serve(token)
			if status != http.StatusUnauthorized || handlerCalls != 1 {
				t.Fatalf("rejected principal status/calls = %d/%d; want 401/1", status, handlerCalls)
			}
			if boundaryEdgeID != "" {
				t.Fatalf("rejected principal recorded edge request ID %q", boundaryEdgeID)
			}
		})
	}
}
