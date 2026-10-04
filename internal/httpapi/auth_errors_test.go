package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
)

func TestAuthenticationDependencyFailureUsesSafeUnavailableEnvelope(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeError(recorder, httptest.NewRequest(http.MethodPost, "/v1/oauth/token", nil), &auth.UnavailableError{})
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want503", recorder.Code)
	}
	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Type != "api_error" || response.Error.Message != "authentication unavailable" {
		t.Fatalf("unsafe or misclassified dependency envelope: %#v", response.Error)
	}
}
