package httpapi

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-chi/chi/v5"
)

type modelCatalogEntry struct {
	ID             string
	DisplayName    string
	CreatedAt      string
	MaxTokens      int
	MaxInputTokens int
}

const modelCatalogSize = 7

func currentModelCatalog() [modelCatalogSize]modelCatalogEntry {
	return [modelCatalogSize]modelCatalogEntry{
		{ID: "openai/gpt-5.5", DisplayName: "GPT-5.5", CreatedAt: "2026-04-23T00:00:00Z", MaxTokens: 128000, MaxInputTokens: 1050000},
		{ID: "openai/gpt-5.6-sol", DisplayName: "GPT-5.6 Sol", CreatedAt: "2026-07-09T00:00:00Z", MaxTokens: 128000, MaxInputTokens: 1050000},
		{ID: "anthropic/claude-opus-4-8", DisplayName: "Claude Opus 4.8", CreatedAt: "2026-05-28T00:00:00Z", MaxTokens: 128000, MaxInputTokens: 1000000},
		{ID: "anthropic/claude-fable-5", DisplayName: "Claude Fable 5", CreatedAt: "2026-06-07T00:00:00Z", MaxTokens: 128000, MaxInputTokens: 1000000},
		{ID: "deepseek/deepseek-v4-pro", DisplayName: "DeepSeek V4 Pro", CreatedAt: "2026-04-24T00:00:00Z", MaxTokens: 384000, MaxInputTokens: 1000000},
		{ID: "moonshotai/kimi-k3", DisplayName: "Kimi K3", CreatedAt: "2026-07-16T00:00:00Z", MaxTokens: 131072, MaxInputTokens: 1048576},
		{ID: "zai/glm-5.2", DisplayName: "GLM-5.2", CreatedAt: "2026-06-13T00:00:00Z", MaxTokens: 131072, MaxInputTokens: 1048576},
	}
}

type modelInfoResponse struct {
	ID                    string    `json:"id"`
	Type                  string    `json:"type"`
	DisplayName           string    `json:"display_name"`
	CreatedAt             string    `json:"created_at"`
	MaxTokens             int       `json:"max_tokens"`
	MaxInputTokens        *int      `json:"max_input_tokens"`
	Capabilities          any       `json:"capabilities"`
	AllowedFallbackModels *[]string `json:"allowed_fallback_models,omitempty"`
}

type modelListPage struct {
	Data    []modelInfoResponse `json:"data"`
	HasMore bool                `json:"has_more"`
	FirstID *string             `json:"first_id"`
	LastID  *string             `json:"last_id"`
}

func listModels(w http.ResponseWriter, r *http.Request) {
	catalog := currentModelCatalog()
	query, beta, err := parseModelsQuery(r, "limit", "before_id", "after_id")
	if err != nil {
		writeError(w, r, err)
		return
	}
	limit, err := parseModelLimit(query)
	if err != nil {
		writeError(w, r, err)
		return
	}

	start, end, hasMore := modelCatalogPageBounds(catalog[:], query, limit)
	data := make([]modelInfoResponse, 0, end-start)
	for _, entry := range catalog[start:end] {
		data = append(data, modelResponse(entry, beta))
	}

	var firstID, lastID *string
	if len(data) > 0 {
		first := data[0].ID
		last := data[len(data)-1].ID
		firstID = &first
		lastID = &last
	}
	writeJSON(w, http.StatusOK, modelListPage{
		Data:    data,
		HasMore: hasMore,
		FirstID: firstID,
		LastID:  lastID,
	})
}

func retrieveModel(w http.ResponseWriter, r *http.Request) {
	catalog := currentModelCatalog()
	_, beta, err := parseModelsQuery(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	id, err := url.PathUnescape(chi.URLParam(r, "model_id"))
	if err != nil {
		writeError(w, r, &ValidationError{Message: "invalid model id"})
		return
	}
	for _, entry := range catalog {
		if entry.ID == id {
			writeJSON(w, http.StatusOK, modelResponse(entry, beta))
			return
		}
	}
	writeError(w, r, &NotFoundError{Message: "model " + id + " not found"})
}

func modelResponse(entry modelCatalogEntry, beta bool) modelInfoResponse {
	maxInputTokens := entry.MaxInputTokens
	response := modelInfoResponse{
		ID:             entry.ID,
		Type:           "model",
		DisplayName:    entry.DisplayName,
		CreatedAt:      entry.CreatedAt,
		MaxTokens:      entry.MaxTokens,
		MaxInputTokens: &maxInputTokens,
		Capabilities:   nil,
	}
	if beta {
		var none []string
		response.AllowedFallbackModels = &none
	}
	return response
}

func parseModelsQuery(r *http.Request, allowed ...string) (url.Values, bool, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, false, &ValidationError{Message: "invalid query string"}
	}
	allowedSet := map[string]struct{}{"beta": {}}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if _, ok := allowedSet[key]; !ok {
			return nil, false, &ValidationError{Message: "unknown query parameter " + key}
		}
		if len(rawValues) != 1 {
			return nil, false, &ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" {
			return nil, false, &ValidationError{Message: "empty query parameter " + key}
		}
	}
	beta := false
	if rawValues, ok := values["beta"]; ok {
		if rawValues[0] != "true" {
			return nil, false, &ValidationError{Message: "beta must be true"}
		}
		beta = true
		values.Del("beta")
	}
	return values, beta, nil
}

func parseModelLimit(values url.Values) (int, error) {
	rawValues, ok := values["limit"]
	if !ok {
		return modelCatalogSize, nil
	}
	parsed, err := strconv.Atoi(rawValues[0])
	if err != nil || parsed <= 0 {
		return 0, &ValidationError{Message: "limit must be a positive integer"}
	}
	if parsed > 100 {
		return 100, nil
	}
	return parsed, nil
}

func modelCatalogBounds(catalog []modelCatalogEntry, values url.Values) (int, int) {
	start, end := 0, len(catalog)
	if afterID := values.Get("after_id"); afterID != "" {
		start = len(catalog)
		for index, entry := range catalog {
			if entry.ID == afterID {
				start = index + 1
				break
			}
		}
	}
	if beforeID := values.Get("before_id"); beforeID != "" {
		end = 0
		for index, entry := range catalog {
			if entry.ID == beforeID {
				end = index
				break
			}
		}
	}
	return start, end
}

func modelCatalogPageBounds(catalog []modelCatalogEntry, values url.Values, limit int) (int, int, bool) {
	start, end := modelCatalogBounds(catalog, values)
	if start > end {
		start = end
	}
	if values.Get("before_id") != "" {
		pageStart := end - limit
		if pageStart < start {
			pageStart = start
		}
		return pageStart, end, pageStart > start
	}
	pageEnd := start + limit
	if pageEnd > end {
		pageEnd = end
	}
	return start, pageEnd, pageEnd < end
}
