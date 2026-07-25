package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/session"
)

func TestSDKCompatibilityGitHubCheckoutAdmissionIsNestedOnly(t *testing.T) {
	const sha = "abcdef0123456789abcdef0123456789abcdef01"
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCalls  int
		wantType   string
		wantRef    string
	}{
		{
			name:       "branch nested",
			body:       `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","authorization_token":"github_token_branch","checkout":{"type":"branch","name":"main"}}]}`,
			wantStatus: http.StatusOK,
			wantCalls:  1,
			wantType:   "branch",
			wantRef:    "main",
		},
		{
			name:       "commit nested",
			body:       `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","authorization_token":"github_token_commit","checkout":{"type":"commit","sha":"` + sha + `"}}]}`,
			wantStatus: http.StatusOK,
			wantCalls:  1,
			wantType:   "commit",
			wantRef:    sha,
		},
		{
			name:       "flat legacy rejected",
			body:       `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","checkout_type":"branch","checkout_ref":"main"}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "nested unknown rejected",
			body:       `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","checkout":{"type":"branch","name":"main","extra":true}}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "branch sha rejected",
			body:       `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","checkout":{"type":"branch","sha":"` + sha + `"}}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "commit name rejected",
			body:       `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","checkout":{"type":"commit","name":"main"}}]}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingCreateSessionService{}
			router := newSessionHTTPTestRouterWithCreateService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)

			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d; want %d body=%s", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if service.calls != tc.wantCalls {
				t.Fatalf("Create calls = %d; want %d", service.calls, tc.wantCalls)
			}
			if tc.wantCalls == 0 {
				return
			}
			checkout := service.request.Resources[0].Checkout
			if checkout == nil || checkout.Type != tc.wantType {
				t.Fatalf("checkout = %+v; want type %s", checkout, tc.wantType)
			}
			gotRef := checkout.Name
			if checkout.Type == "commit" {
				gotRef = checkout.SHA
			}
			if gotRef != tc.wantRef {
				t.Fatalf("checkout ref = %q; want %q", gotRef, tc.wantRef)
			}
		})
	}
}

func TestSDKCompatibilityGitHubCheckoutResponsesAreNestedOnly(t *testing.T) {
	const sha = "abcdef0123456789abcdef0123456789abcdef01"
	body, err := json.Marshal(session.ResourceResponse{
		Type:         string(session.ResourceTypeGitHubRepository),
		CheckoutType: "commit",
		CheckoutRef:  sha,
	})
	if err != nil {
		t.Fatalf("Marshal ResourceResponse: %v", err)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Unmarshal ResourceResponse: %v", err)
	}
	if _, ok := response["checkout_type"]; ok {
		t.Fatalf("response leaked checkout_type: %s", body)
	}
	if _, ok := response["checkout_ref"]; ok {
		t.Fatalf("response leaked checkout_ref: %s", body)
	}
	checkout, ok := response["checkout"].(map[string]any)
	if !ok || checkout["type"] != "commit" || checkout["sha"] != sha || len(checkout) != 2 {
		t.Fatalf("checkout = %#v; want exact nested commit union (%s)", response["checkout"], body)
	}
}

func TestSDKCompatibilitySessionExplicitNullsReachUpdateAsClear(t *testing.T) {
	for _, body := range []string{`{"title":null}`, `{"metadata":null}`} {
		t.Run(body, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
			}
			if service.updateCalls != 1 {
				t.Fatalf("Update calls = %d; want 1", service.updateCalls)
			}
		})
	}
}

func TestSDKCompatibilitySessionMemoryResourceNullOverridesAreDropped(t *testing.T) {
	service := &recordingCreateSessionService{}
	router := newSessionHTTPTestRouterWithCreateService(service)
	recorder := httptest.NewRecorder()
	body := `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"memory_store","memory_store_id":"memstore_test","access":null,"instructions":null}]}`
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.calls != 1 || len(service.request.Resources) != 1 {
		t.Fatalf("Create calls/resources = %d/%+v; want one resource", service.calls, service.request.Resources)
	}
	resource := service.request.Resources[0]
	if resource.Access != "" || resource.Instructions != "" {
		t.Fatalf("resource overrides = access %q instructions %q; want dropped", resource.Access, resource.Instructions)
	}
}
