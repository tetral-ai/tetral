package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// agentBodyByteCap is the per-request body limit for create + update.
// Centralized here so both handlers share one ceiling and a future
// move to centralized middleware has a single source of truth.
const agentBodyByteCap = 1 << 20

// AgentHandler groups HTTP handler dependencies for agent routes.
type AgentHandler struct {
	service *agent.Service
}

// NewAgentHandler creates a new AgentHandler.
func NewAgentHandler(service *agent.Service) *AgentHandler {
	return &AgentHandler{service: service}
}

// requestWorkspace extracts the authenticated workspace from ctx.
// Real resource handlers fail closed when auth did not attach one.
func requestWorkspace(ctx context.Context) (workspace.ID, error) {
	return workspace.MustIDFromContext(ctx)
}

// readStrictAgentBody applies the 1 MiB MaxBytesReader cap, reads the
// raw bytes, and rejects oversized requests via the typed
// RequestTooLargeError so writeError surfaces the centralized 413
// envelope. Trailing-data rejection runs in decodeStrictTrailingBytes after the caller's primary
// unmarshal; that secondary read uses the same cap so an otherwise
// valid JSON object followed by oversized padding lands on the same
// 413 response.
func readStrictAgentBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, agentBodyByteCap)
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

// decodeStrictTrailingBytes enforces the Agent request-body invariant:
// at most one JSON value per request. The caller has already accepted
// the bytes via readStrictAgentBody, so this only checks that nothing
// follows the first decoded value besides whitespace.
func decodeStrictTrailingBytes(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	var first json.RawMessage
	if err := decoder.Decode(&first); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return &ValidationError{Message: "request body must contain exactly one JSON value"}
		}
		return err
	}
	return nil
}

func (h *AgentHandler) createAgent(w http.ResponseWriter, r *http.Request) {
	if err := validateAgentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readStrictAgentBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := decodeStrictTrailingBytes(body); err != nil {
		var tooLarge *RequestTooLargeError
		if errors.As(err, &tooLarge) {
			writeError(w, r, tooLarge)
			return
		}
		var vErr *agent.ValidationError
		if errors.As(err, &vErr) {
			writeError(w, r, vErr)
			return
		}
		writeError(w, r, &ValidationError{Message: "invalid request body"})
		return
	}

	request, err := agent.DecodeCreateAgentRequest(body)
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

func (h *AgentHandler) getAgent(w http.ResponseWriter, r *http.Request) {
	agentID := chi.URLParam(r, "agent_id")
	version, hasVersion, err := parseAgentGetVersion(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	var result *agent.Agent
	if hasVersion {
		result, err = h.service.GetVersion(r.Context(), ws, agentID, version)
	} else {
		result, err = h.service.Get(r.Context(), ws, agentID)
	}
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *AgentHandler) listAgents(w http.ResponseWriter, r *http.Request) {
	options, err := parseAgentListOptions(r)
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

	writeJSON(w, http.StatusOK, result)
}

// updateAgent decodes a flat patch body into agent.AgentPatch
// (preserving omitted/null/value distinction), prevalidates fields
// whose validity is state-independent, then delegates business patch
// semantics to agent.Service.UpdatePatch. The route MUST NOT treat the body as a full
// replacement object; Service materializes the patch, validates and
// canonicalizes it, coordinates Skill validation, and performs no-op
// detection before calling Store persistence primitives.
func (h *AgentHandler) updateAgent(w http.ResponseWriter, r *http.Request) {
	agentID := chi.URLParam(r, "agent_id")

	if err := validateAgentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readStrictAgentBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := decodeStrictTrailingBytes(body); err != nil {
		var tooLarge *RequestTooLargeError
		if errors.As(err, &tooLarge) {
			writeError(w, r, tooLarge)
			return
		}
		var vErr *agent.ValidationError
		if errors.As(err, &vErr) {
			writeError(w, r, vErr)
			return
		}
		writeError(w, r, &ValidationError{Message: "invalid request body"})
		return
	}

	var patch agent.AgentPatch
	if err := json.Unmarshal(body, &patch); err != nil {
		// AgentPatch.UnmarshalJSON returns *agent.ValidationError for
		// missing version, null version, non-integer version, and
		// malformed JSON. Surface those messages verbatim through the
		// standard writeError envelope so clients see the specific
		// reason instead of a generic "invalid request body".
		var vErr *agent.ValidationError
		if errors.As(err, &vErr) {
			writeError(w, r, vErr)
			return
		}
		writeError(w, r, &ValidationError{Message: "invalid request body"})
		return
	}
	if err := patch.Prevalidate(); err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.UpdatePatch(r.Context(), ws, agentID, patch)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *AgentHandler) archiveAgent(w http.ResponseWriter, r *http.Request) {
	if err := validateAgentBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.Archive(r.Context(), ws, chi.URLParam(r, "agent_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *AgentHandler) listAgentVersions(w http.ResponseWriter, r *http.Request) {
	options, err := parseAgentVersionListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListVersions(r.Context(), ws, chi.URLParam(r, "agent_id"), options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func parseAgentGetVersion(r *http.Request) (int, bool, error) {
	values, err := parseStrictAgentQuery(r, []string{"version"})
	if err != nil {
		return 0, false, err
	}
	rawValues, ok := values["version"]
	if !ok {
		return 0, false, nil
	}
	version, err := strconv.Atoi(rawValues[0])
	if err != nil || version <= 0 {
		return 0, false, &agent.ValidationError{Message: "version must be a positive integer"}
	}
	return version, true, nil
}

func parseAgentListOptions(r *http.Request) (agent.ListOptions, error) {
	values, err := parseStrictAgentQuery(r, []string{"limit", "page", "include_archived", "created_at[gte]", "created_at[lte]"})
	if err != nil {
		return agent.ListOptions{}, err
	}
	options := agent.ListOptions{Limit: 20}
	if rawValues, ok := values["limit"]; ok {
		limit, err := strconv.Atoi(rawValues[0])
		if err != nil || limit <= 0 {
			return agent.ListOptions{}, &agent.ValidationError{Message: "limit must be a positive integer"}
		}
		if limit > 100 {
			limit = 100
		}
		options.Limit = limit
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
	}
	if rawValues, ok := values["include_archived"]; ok {
		switch rawValues[0] {
		case "true":
			options.IncludeArchived = true
		case "false":
			options.IncludeArchived = false
		default:
			return agent.ListOptions{}, &agent.ValidationError{Message: "include_archived must be true or false"}
		}
	}
	if rawValues, ok := values["created_at[gte]"]; ok {
		parsed, err := time.Parse(time.RFC3339, rawValues[0])
		if err != nil {
			return agent.ListOptions{}, &agent.ValidationError{Message: "created_at[gte] must be an RFC3339 timestamp"}
		}
		parsed = parsed.UTC()
		options.CreatedAtGTE = &parsed
	}
	if rawValues, ok := values["created_at[lte]"]; ok {
		parsed, err := time.Parse(time.RFC3339, rawValues[0])
		if err != nil {
			return agent.ListOptions{}, &agent.ValidationError{Message: "created_at[lte] must be an RFC3339 timestamp"}
		}
		parsed = parsed.UTC()
		options.CreatedAtLTE = &parsed
	}
	return options, nil
}

func parseAgentVersionListOptions(r *http.Request) (agent.ListVersionsOptions, error) {
	values, err := parseStrictAgentQuery(r, []string{"limit", "page"})
	if err != nil {
		return agent.ListVersionsOptions{}, err
	}
	options := agent.ListVersionsOptions{Limit: 20}
	if rawValues, ok := values["limit"]; ok {
		limit, err := strconv.Atoi(rawValues[0])
		if err != nil || limit <= 0 {
			return agent.ListVersionsOptions{}, &agent.ValidationError{Message: "limit must be a positive integer"}
		}
		if limit > 100 {
			limit = 100
		}
		options.Limit = limit
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
	}
	return options, nil
}

func validateAgentBetaRoute(r *http.Request) error {
	_, err := parseStrictAgentQuery(r, nil)
	return err
}

func parseStrictAgentQuery(r *http.Request, allowed []string) (url.Values, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, &agent.ValidationError{Message: "invalid query string"}
	}
	allowedSet := make(map[string]struct{}, len(allowed)+1)
	allowedSet["beta"] = struct{}{}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if len(rawValues) != 1 {
			return nil, &agent.ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" {
			return nil, &agent.ValidationError{Message: "empty query parameter " + key}
		}
		if _, ok := allowedSet[key]; !ok {
			return nil, &agent.ValidationError{Message: "unknown query parameter " + key}
		}
	}
	if rawValues, ok := values["beta"]; !ok || rawValues[0] != "true" {
		return nil, &agent.ValidationError{Message: "beta must be true"}
	}
	delete(values, "beta")
	return values, nil
}
