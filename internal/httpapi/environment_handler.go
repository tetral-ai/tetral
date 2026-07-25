package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// EnvironmentService is the workspace-aware Environment service surface
// used by the HTTP handler. Tests may substitute their own implementation
// as long as it preserves the workspace-scoped signatures.
type EnvironmentService interface {
	Create(ctx context.Context, ws workspace.ID, request environment.CreateEnvironmentRequest) (*environment.Environment, error)
	Get(ctx context.Context, ws workspace.ID, environmentID string) (*environment.Environment, error)
	List(ctx context.Context, ws workspace.ID, options environment.ListOptions) (environment.ListResult, error)
	Update(ctx context.Context, ws workspace.ID, environmentID string, patch environment.EnvironmentPatch) (*environment.Environment, error)
	Archive(ctx context.Context, ws workspace.ID, environmentID string) (*environment.Environment, error)
	Delete(ctx context.Context, ws workspace.ID, environmentID string) (*environment.DeleteResult, error)
}

// EnvironmentHandler groups HTTP handler dependencies for environment routes.
type EnvironmentHandler struct {
	service EnvironmentService
}

// NewEnvironmentHandler creates a new EnvironmentHandler.
func NewEnvironmentHandler(service EnvironmentService) *EnvironmentHandler {
	return &EnvironmentHandler{service: service}
}

func (h *EnvironmentHandler) createEnvironment(w http.ResponseWriter, r *http.Request) {
	if err := validateEnvironmentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readEnvironmentBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := environment.DecodeCreateEnvironmentRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.Create(r.Context(), ws, request)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *EnvironmentHandler) getEnvironment(w http.ResponseWriter, r *http.Request) {
	if err := validateEnvironmentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	environmentID := chi.URLParam(r, "environment_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.Get(r.Context(), ws, environmentID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *EnvironmentHandler) listEnvironments(w http.ResponseWriter, r *http.Request) {
	options, err := parseEnvironmentListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.List(r.Context(), ws, options)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, environmentListResponse{
		Data:     result.Data,
		NextPage: result.NextPage,
	})
}

func (h *EnvironmentHandler) updateEnvironment(w http.ResponseWriter, r *http.Request) {
	if err := validateEnvironmentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	environmentID := chi.URLParam(r, "environment_id")

	body, err := readEnvironmentBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	patch, err := environment.DecodeUpdateEnvironmentRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.Update(r.Context(), ws, environmentID, patch)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *EnvironmentHandler) deleteEnvironment(w http.ResponseWriter, r *http.Request) {
	if err := validateEnvironmentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	environmentID := chi.URLParam(r, "environment_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.Delete(r.Context(), ws, environmentID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *EnvironmentHandler) archiveEnvironment(w http.ResponseWriter, r *http.Request) {
	if err := validateEnvironmentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	environmentID := chi.URLParam(r, "environment_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.Archive(r.Context(), ws, environmentID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func readEnvironmentBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &RequestTooLargeError{Message: "request body too large"}
		}
		return nil, err
	}
	return body, nil
}

type environmentListResponse struct {
	Data     []*environment.Environment `json:"data"`
	NextPage *string                    `json:"next_page"`
}

func parseEnvironmentListOptions(r *http.Request) (environment.ListOptions, error) {
	values, err := parseStrictEnvironmentQuery(r, []string{"limit", "page", "include_archived"})
	if err != nil {
		return environment.ListOptions{}, err
	}

	options := environment.ListOptions{Limit: 20}
	if rawValues, ok := values["limit"]; ok {
		raw := rawValues[0]
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			return environment.ListOptions{}, &environment.ValidationError{Message: "limit must be a positive integer"}
		}
		if limit > 100 {
			limit = 100
		}
		options.Limit = limit
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
		if options.Page == "" {
			return environment.ListOptions{}, &environment.ValidationError{Message: "page must be a non-empty token"}
		}
	}
	if rawValues, ok := values["include_archived"]; ok {
		raw := rawValues[0]
		switch raw {
		case "true":
			options.IncludeArchived = true
		case "false":
			options.IncludeArchived = false
		default:
			return environment.ListOptions{}, &environment.ValidationError{Message: "include_archived must be true or false"}
		}
	}
	return options, nil
}

func validateEnvironmentBetaRoute(r *http.Request) error {
	_, err := parseStrictEnvironmentQuery(r, nil)
	return err
}

func parseStrictEnvironmentQuery(r *http.Request, allowed []string) (url.Values, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, &environment.ValidationError{Message: "invalid query string"}
	}
	allowedSet := make(map[string]struct{}, len(allowed)+1)
	allowedSet["beta"] = struct{}{}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if len(rawValues) != 1 {
			return nil, &environment.ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" {
			return nil, &environment.ValidationError{Message: "empty query parameter " + key}
		}
		if _, ok := allowedSet[key]; !ok {
			return nil, &environment.ValidationError{Message: "unknown query parameter " + key}
		}
	}
	if rawValues, ok := values["beta"]; !ok || rawValues[0] != "true" {
		return nil, &environment.ValidationError{Message: "beta must be true"}
	}
	delete(values, "beta")
	return values, nil
}
