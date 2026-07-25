package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type modelListResponse struct {
	Data    []modelResponse `json:"data"`
	HasMore bool            `json:"has_more"`
	FirstID *string         `json:"first_id"`
	LastID  *string         `json:"last_id"`
}

type modelResponse struct {
	ID                    string   `json:"id"`
	Type                  string   `json:"type"`
	DisplayName           string   `json:"display_name"`
	CreatedAt             string   `json:"created_at"`
	MaxTokens             int      `json:"max_tokens"`
	MaxInputTokens        *int     `json:"max_input_tokens"`
	Capabilities          any      `json:"capabilities"`
	AllowedFallbackModels []string `json:"allowed_fallback_models"`
}

func TestModelsListServesTheStableSevenModelCatalog(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response modelListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	wantIDs := []string{
		"openai/gpt-5.5",
		"openai/gpt-5.6-sol",
		"anthropic/claude-opus-4-8",
		"anthropic/claude-fable-5",
		"deepseek/deepseek-v4-pro",
		"moonshotai/kimi-k3",
		"zai/glm-5.2",
	}
	if len(response.Data) != len(wantIDs) {
		t.Fatalf("model count = %d; want %d body=%s", len(response.Data), len(wantIDs), recorder.Body.String())
	}
	for i, wantID := range wantIDs {
		if response.Data[i].ID != wantID {
			t.Fatalf("model[%d].id = %q; want %q", i, response.Data[i].ID, wantID)
		}
		if response.Data[i].Type != "model" || response.Data[i].DisplayName == "" || response.Data[i].CreatedAt == "" {
			t.Fatalf("model[%d] missing fixed fields: %#v", i, response.Data[i])
		}
		if response.Data[i].MaxTokens <= 0 || response.Data[i].MaxInputTokens == nil || *response.Data[i].MaxInputTokens <= 0 {
			t.Fatalf("model[%d] missing token limits: %#v", i, response.Data[i])
		}
		if response.Data[i].Capabilities != nil {
			t.Fatalf("model[%d].capabilities = %#v; want null", i, response.Data[i].Capabilities)
		}
	}
	if response.HasMore {
		t.Fatal("has_more = true; want false")
	}
	if response.FirstID == nil || *response.FirstID != wantIDs[0] {
		t.Fatalf("first_id = %#v; want %q", response.FirstID, wantIDs[0])
	}
	if response.LastID == nil || *response.LastID != wantIDs[len(wantIDs)-1] {
		t.Fatalf("last_id = %#v; want %q", response.LastID, wantIDs[len(wantIDs)-1])
	}
}

func TestModelsListHonorsStableCatalogCursorsAndLimit(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/models?after_id=openai%2Fgpt-5.5&before_id=deepseek%2Fdeepseek-v4-pro&limit=2",
		nil,
	)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response modelListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Data) != 2 ||
		response.Data[0].ID != "anthropic/claude-opus-4-8" ||
		response.Data[1].ID != "anthropic/claude-fable-5" {
		t.Fatalf("page = %#v; want the two entries nearest before_id", response.Data)
	}
	if !response.HasMore {
		t.Fatal("has_more = false; want true")
	}
}

func TestModelsListWalksTheSevenModelCatalogAcrossShortPages(t *testing.T) {
	router := newTestRouter(t)
	wantIDs := []string{
		"openai/gpt-5.5",
		"openai/gpt-5.6-sol",
		"anthropic/claude-opus-4-8",
		"anthropic/claude-fable-5",
		"deepseek/deepseek-v4-pro",
		"moonshotai/kimi-k3",
		"zai/glm-5.2",
	}

	firstPage := requestModelPage(t, router, "/v1/models?limit=4")
	if !firstPage.HasMore {
		t.Fatal("first page has_more = false; want true")
	}
	if firstPage.LastID == nil {
		t.Fatal("first page last_id = nil")
	}
	secondPage := requestModelPage(
		t,
		router,
		"/v1/models?limit=4&after_id="+url.QueryEscape(*firstPage.LastID),
	)
	if secondPage.HasMore {
		t.Fatal("final page has_more = true; want false")
	}

	gotIDs := make([]string, 0, len(firstPage.Data)+len(secondPage.Data))
	for _, page := range []modelListResponse{firstPage, secondPage} {
		for _, model := range page.Data {
			gotIDs = append(gotIDs, model.ID)
		}
	}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("walked model count = %d; want %d (%#v)", len(gotIDs), len(wantIDs), gotIDs)
	}
	for index, wantID := range wantIDs {
		if gotIDs[index] != wantID {
			t.Fatalf("walked model[%d] = %q; want %q", index, gotIDs[index], wantID)
		}
	}
}

func requestModelPage(t *testing.T, router http.Handler, path string) modelListResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("%s status = %d; want 200 body=%s", path, recorder.Code, recorder.Body.String())
	}
	var response modelListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("%s decode response: %v", path, err)
	}
	return response
}

func TestBetaModelsRetrieveAddsFallbackFieldWithoutChangingCatalogData(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/models/moonshotai%2Fkimi-k3?beta=true", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if string(raw["id"]) != `"moonshotai/kimi-k3"` || string(raw["max_tokens"]) != "131072" {
		t.Fatalf("model response = %s", recorder.Body.String())
	}
	if string(raw["allowed_fallback_models"]) != "null" {
		t.Fatalf("allowed_fallback_models = %s; want null", raw["allowed_fallback_models"])
	}
	if string(raw["capabilities"]) != "null" {
		t.Fatalf("capabilities = %s; want null", raw["capabilities"])
	}
}
