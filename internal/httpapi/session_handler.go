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
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const sessionBodyByteCap = 1 << 20

type sessionService interface {
	Create(rctx context.Context, ws workspace.ID, request session.CreateRequest) (*session.Response, error)
	Get(rctx context.Context, ws workspace.ID, sessionID string) (*session.Response, error)
	List(rctx context.Context, ws workspace.ID, options session.ListOptions) (*session.ListResult, error)
	ListThreads(rctx context.Context, ws workspace.ID, sessionID string, options session.ThreadListOptions) (*session.ThreadListResult, error)
	GetThread(rctx context.Context, ws workspace.ID, sessionID string, threadID string) (*session.ThreadResponse, error)
	ArchiveThread(rctx context.Context, ws workspace.ID, sessionID string, threadID string) (*session.ThreadResponse, error)
	Update(rctx context.Context, ws workspace.ID, sessionID string, request session.UpdateRequest) (*session.Response, error)
	Archive(rctx context.Context, ws workspace.ID, sessionID string) (*session.Response, error)
	Delete(rctx context.Context, ws workspace.ID, sessionID string) (*session.DeleteResponse, error)
	AddResource(rctx context.Context, ws workspace.ID, sessionID string, request session.ResourceRequest) (*session.ResourceResponse, error)
	ListResources(rctx context.Context, ws workspace.ID, sessionID string, options session.ResourceListOptions) (*session.ResourceListResult, error)
	GetResource(rctx context.Context, ws workspace.ID, sessionID string, resourceID string) (*session.ResourceResponse, error)
	UpdateResource(rctx context.Context, ws workspace.ID, sessionID string, resourceID string, token string) (*session.ResourceResponse, error)
	DeleteResource(rctx context.Context, ws workspace.ID, sessionID string, resourceID string) (*session.ResourceDeleteResponse, error)
}

type SessionHandler struct {
	service sessionService
}

func NewSessionHandler(service sessionService) *SessionHandler {
	return &SessionHandler{service: service}
}

type createSessionHTTPRequest struct {
	Agent         json.RawMessage         `json:"agent"`
	EnvironmentID strictSessionJSONString `json:"environment_id"`
	Title         *string                 `json:"title"`
	Metadata      json.RawMessage         `json:"metadata"`
	Resources     []resourceHTTPRequest   `json:"resources"`
	VaultIDs      json.RawMessage         `json:"vault_ids"`
	Providers     json.RawMessage         `json:"providers"`
}

type agentReferenceHTTPRequest struct {
	Type    json.RawMessage `json:"type"`
	ID      json.RawMessage `json:"id"`
	Version json.RawMessage `json:"version,omitempty"`
}

type resourceHTTPRequest struct {
	Type               strictSessionJSONString `json:"type"`
	FileID             strictSessionJSONString `json:"file_id"`
	MountPath          *string                 `json:"mount_path"`
	MemoryStoreID      strictSessionJSONString `json:"memory_store_id"`
	Access             strictSessionJSONString `json:"access"`
	Instructions       strictSessionJSONString `json:"instructions"`
	URL                strictSessionJSONString `json:"url"`
	AuthorizationToken strictSessionJSONString `json:"authorization_token"`
	Checkout           *session.GitHubCheckout `json:"checkout"`
}

func (r *resourceHTTPRequest) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("invalid resource object")
	}
	if fields == nil {
		return errors.New("resource must be an object")
	}
	for field := range fields {
		if !knownSessionResourceHTTPField(field) {
			return errors.New("unknown resource field")
		}
	}
	if err := unmarshalSessionResourceStringField(fields, "type", &r.Type); err != nil {
		return err
	}
	if err := rejectSessionHTTPResourceTypeClosure(string(r.Type), fields); err != nil {
		return err
	}
	if err := unmarshalSessionResourceStringField(fields, "file_id", &r.FileID); err != nil {
		return err
	}
	if err := unmarshalSessionResourceStringField(fields, "memory_store_id", &r.MemoryStoreID); err != nil {
		return err
	}
	if rawAccess, ok := fields["access"]; ok && !bytes.Equal(bytes.TrimSpace(rawAccess), []byte("null")) {
		if err := json.Unmarshal(rawAccess, &r.Access); err != nil {
			return err
		}
	}
	if rawInstructions, ok := fields["instructions"]; ok && !bytes.Equal(bytes.TrimSpace(rawInstructions), []byte("null")) {
		if err := json.Unmarshal(rawInstructions, &r.Instructions); err != nil {
			return err
		}
	}
	if err := unmarshalSessionResourceStringField(fields, "url", &r.URL); err != nil {
		return err
	}
	if err := unmarshalSessionResourceStringField(fields, "authorization_token", &r.AuthorizationToken); err != nil {
		return err
	}
	if rawMountPath, ok := fields["mount_path"]; ok {
		if !bytes.Equal(bytes.TrimSpace(rawMountPath), []byte("null")) {
			var mountPath string
			if err := json.Unmarshal(rawMountPath, &mountPath); err != nil {
				return err
			}
			r.MountPath = &mountPath
		}
	}
	if rawCheckout, ok := fields["checkout"]; ok {
		checkout, err := decodeSessionGitHubCheckout(rawCheckout)
		if err != nil {
			return err
		}
		r.Checkout = checkout
	}
	return nil
}

func knownSessionResourceHTTPField(field string) bool {
	switch field {
	case "type", "file_id", "mount_path", "memory_store_id", "access", "instructions", "url", "authorization_token", "checkout":
		return true
	default:
		return false
	}
}

func unmarshalSessionResourceStringField(fields map[string]json.RawMessage, field string, target *strictSessionJSONString) error {
	raw, ok := fields[field]
	if !ok {
		return nil
	}
	return json.Unmarshal(raw, target)
}

func rejectSessionHTTPResourceTypeClosure(resourceType string, fields map[string]json.RawMessage) error {
	if resourceType == "" {
		return nil
	}
	for field := range fields {
		if !sessionHTTPResourceFieldAllowed(resourceType, field) {
			return errors.New("resource field is not allowed for type")
		}
	}
	return nil
}

func sessionHTTPResourceFieldAllowed(resourceType string, field string) bool {
	switch resourceType {
	case string(session.ResourceTypeFile):
		switch field {
		case "type", "file_id", "mount_path":
			return true
		}
	case string(session.ResourceTypeMemoryStore):
		switch field {
		case "type", "memory_store_id", "access", "instructions":
			return true
		}
	case string(session.ResourceTypeGitHubRepository):
		switch field {
		case "type", "url", "authorization_token", "mount_path", "checkout":
			return true
		}
	default:
		return true
	}
	return false
}

func decodeSessionGitHubCheckout(raw json.RawMessage) (*session.GitHubCheckout, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("checkout must be an object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("checkout must be an object")
	}
	for field := range fields {
		if field != "type" && field != "name" && field != "sha" {
			return nil, errors.New("unknown checkout field")
		}
	}
	rawType, ok := fields["type"]
	if !ok || bytes.Equal(bytes.TrimSpace(rawType), []byte("null")) {
		return nil, errors.New("checkout.type is required")
	}
	var checkoutType string
	if err := json.Unmarshal(rawType, &checkoutType); err != nil {
		return nil, errors.New("checkout.type must be a string")
	}
	decodeRequired := func(field string) (string, error) {
		valueRaw, present := fields[field]
		if !present || bytes.Equal(bytes.TrimSpace(valueRaw), []byte("null")) {
			return "", errors.New("checkout." + field + " is required")
		}
		var value string
		if err := json.Unmarshal(valueRaw, &value); err != nil || value == "" {
			return "", errors.New("checkout." + field + " must be a non-empty string")
		}
		return value, nil
	}
	switch checkoutType {
	case "branch":
		if _, ok := fields["sha"]; ok {
			return nil, errors.New("checkout.sha is not allowed for branch")
		}
		name, err := decodeRequired("name")
		if err != nil {
			return nil, err
		}
		return &session.GitHubCheckout{Type: checkoutType, Name: name}, nil
	case "commit":
		if _, ok := fields["name"]; ok {
			return nil, errors.New("checkout.name is not allowed for commit")
		}
		sha, err := decodeRequired("sha")
		if err != nil {
			return nil, err
		}
		return &session.GitHubCheckout{Type: checkoutType, SHA: sha}, nil
	default:
		return nil, errors.New("checkout.type is unsupported")
	}
}

type strictSessionJSONString string

func (s *strictSessionJSONString) UnmarshalJSON(raw []byte) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("string field cannot be null")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*s = strictSessionJSONString(value)
	return nil
}

func (h *SessionHandler) createSession(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	var httpRequest createSessionHTTPRequest
	if err := decodeStrictSessionBody(w, r, &httpRequest); err != nil {
		writeError(w, r, err)
		return
	}
	metadata, err := parseCreateSessionMetadata(httpRequest.Metadata)
	if err != nil {
		writeError(w, r, err)
		return
	}
	environmentID := string(httpRequest.EnvironmentID)
	if environmentID == "" {
		writeError(w, r, &ValidationError{Message: "environment_id is required"})
		return
	}
	agentRef, err := parseSessionAgentReference(httpRequest.Agent)
	if err != nil {
		writeError(w, r, err)
		return
	}
	resources := make([]session.ResourceRequest, 0, len(httpRequest.Resources))
	for _, resource := range httpRequest.Resources {
		resources = append(resources, resource.toServiceRequest())
	}
	providers, _, err := parseSessionProviderSelectors(httpRequest.Providers)
	if err != nil {
		writeError(w, r, err)
		return
	}
	vaultIDs, err := parseCreateSessionVaultIDs(httpRequest.VaultIDs)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.Create(r.Context(), ws, session.CreateRequest{
		Agent:         agentRef,
		EnvironmentID: environmentID,
		Title:         httpRequest.Title,
		Metadata:      metadata,
		Resources:     resources,
		VaultIDs:      vaultIDs,
		Providers:     providers,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) getSession(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.Get(r.Context(), ws, chi.URLParam(r, "session_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) listSessions(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	options, err := parseSessionListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.List(r.Context(), ws, options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) listThreads(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	options, err := parseSessionThreadListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.ListThreads(r.Context(), ws, chi.URLParam(r, "session_id"), options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) getThread(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.GetThread(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "thread_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) archiveThread(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptySessionBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.ArchiveThread(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "thread_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) updateSession(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	var raw map[string]json.RawMessage
	if err := decodeStrictSessionBody(w, r, &raw); err != nil {
		writeError(w, r, err)
		return
	}
	if raw == nil {
		writeError(w, r, &ValidationError{Message: "request body must be an object"})
		return
	}
	request := session.UpdateRequest{}
	for key, value := range raw {
		switch key {
		case "title":
			request.TitlePresent = true
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				request.Title = nil
				continue
			}
			var title string
			if err := json.Unmarshal(value, &title); err != nil {
				writeError(w, r, &ValidationError{Message: "title is invalid"})
				return
			}
			request.Title = &title
		case "metadata":
			request.MetadataPresent = true
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				request.MetadataPatch = nil
				continue
			}
			var metadata map[string]json.RawMessage
			if err := json.Unmarshal(value, &metadata); err != nil || metadata == nil {
				writeError(w, r, &ValidationError{Message: "metadata must be an object"})
				return
			}
			request.MetadataPatch = make(map[string]*string, len(metadata))
			for metadataKey, rawValue := range metadata {
				if bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) {
					request.MetadataPatch[metadataKey] = nil
					continue
				}
				var metadataValue string
				if err := json.Unmarshal(rawValue, &metadataValue); err != nil {
					writeError(w, r, &ValidationError{Message: "metadata values must be strings"})
					return
				}
				request.MetadataPatch[metadataKey] = &metadataValue
			}
		case "agent":
			if err := parseSessionAgentUpdate(value, &request); err != nil {
				writeError(w, r, err)
				return
			}
		default:
			writeError(w, r, &ValidationError{Message: "field is immutable"})
			return
		}
	}
	response, err := h.service.Update(r.Context(), ws, chi.URLParam(r, "session_id"), request)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func parseSessionAgentUpdate(raw json.RawMessage, request *session.UpdateRequest) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return &ValidationError{Message: "agent must be an object"}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return &ValidationError{Message: "agent must be an object"}
	}
	for key, value := range fields {
		switch key {
		case "approval_mode":
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return &ValidationError{Message: "agent.approval_mode cannot be null"}
			}
			var rawMode string
			if err := json.Unmarshal(value, &rawMode); err != nil {
				return &ValidationError{Message: "agent.approval_mode must be a string"}
			}
			mode := session.ApprovalMode(strings.TrimSpace(rawMode))
			switch mode {
			case session.ApprovalModeFullAccess, session.ApprovalModeAskForApproval, session.ApprovalModeApproveForMe:
				request.ApprovalMode = &mode
			default:
				return &ValidationError{Message: "agent.approval_mode must be one of full_access, ask_for_approval, approve_for_me"}
			}
		case "tools":
			tools, err := parseSessionAgentRawArray("agent.tools", value)
			if err != nil {
				return err
			}
			request.ToolsPatch = &tools
		case "mcp_servers":
			servers, err := parseSessionAgentRawArray("agent.mcp_servers", value)
			if err != nil {
				return err
			}
			request.MCPServersPatch = &servers
		default:
			return &ValidationError{Message: "agent." + key + " is immutable"}
		}
	}
	return nil
}

func parseSessionAgentRawArray(field string, raw json.RawMessage) (agent.RawArray, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, &ValidationError{Message: field + " must be an array"}
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, &ValidationError{Message: field + " must be an array"}
	}
	out := make(agent.RawArray, 0, len(items))
	for _, item := range items {
		out = append(out, append(json.RawMessage(nil), item...))
	}
	return out, nil
}

func (h *SessionHandler) archiveSession(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptySessionBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.Archive(r.Context(), ws, chi.URLParam(r, "session_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) deleteSession(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptySessionBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.Delete(r.Context(), ws, chi.URLParam(r, "session_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) addResource(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	var httpRequest resourceHTTPRequest
	if err := decodeStrictSessionBody(w, r, &httpRequest); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.AddResource(r.Context(), ws, chi.URLParam(r, "session_id"), httpRequest.toServiceRequest())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) updateResource(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	var raw map[string]json.RawMessage
	if err := decodeStrictSessionBody(w, r, &raw); err != nil {
		writeError(w, r, err)
		return
	}
	if len(raw) != 1 {
		writeError(w, r, &ValidationError{Message: "only authorization_token can be updated"})
		return
	}
	value, ok := raw["authorization_token"]
	if !ok {
		writeError(w, r, &ValidationError{Message: "only authorization_token can be updated"})
		return
	}
	var token string
	if err := json.Unmarshal(value, &token); err != nil || token == "" {
		writeError(w, r, &ValidationError{Message: "authorization_token is required"})
		return
	}
	response, err := h.service.UpdateResource(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "resource_id"), token)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) listResources(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	options, err := parseSessionResourceListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.ListResources(r.Context(), ws, chi.URLParam(r, "session_id"), options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) getResource(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.GetResource(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "resource_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *SessionHandler) deleteResource(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptySessionBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	response, err := h.service.DeleteResource(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "resource_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (r resourceHTTPRequest) toServiceRequest() session.ResourceRequest {
	return session.ResourceRequest{
		Type:               string(r.Type),
		FileID:             string(r.FileID),
		MountPath:          r.MountPath,
		MemoryStoreID:      string(r.MemoryStoreID),
		Access:             string(r.Access),
		Instructions:       string(r.Instructions),
		GitHubURL:          string(r.URL),
		AuthorizationToken: string(r.AuthorizationToken),
		Checkout:           cloneSessionGitHubCheckout(r.Checkout),
	}
}

func cloneSessionGitHubCheckout(checkout *session.GitHubCheckout) *session.GitHubCheckout {
	if checkout == nil {
		return nil
	}
	copyValue := *checkout
	return &copyValue
}

func parseCreateSessionMetadata(raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	metadata := make(map[string]string, len(fields))
	for key, valueRaw := range fields {
		if bytes.Equal(bytes.TrimSpace(valueRaw), []byte("null")) {
			return nil, &ValidationError{Message: "metadata values must be strings"}
		}
		var value string
		if err := json.Unmarshal(valueRaw, &value); err != nil {
			return nil, &ValidationError{Message: "metadata values must be strings"}
		}
		metadata[key] = value
	}
	return metadata, nil
}

func parseCreateSessionVaultIDs(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, &ValidationError{Message: "vault_ids is required"}
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, &ValidationError{Message: "vault_ids must be an array"}
	}
	var rawVaultIDs []json.RawMessage
	if err := json.Unmarshal(raw, &rawVaultIDs); err != nil {
		return nil, &ValidationError{Message: "vault_ids must be an array of strings"}
	}
	if rawVaultIDs == nil {
		return []string{}, nil
	}
	vaultIDs := make([]string, 0, len(rawVaultIDs))
	for _, rawVaultID := range rawVaultIDs {
		if bytes.Equal(bytes.TrimSpace(rawVaultID), []byte("null")) {
			return nil, &ValidationError{Message: "vault_ids must be an array of strings"}
		}
		var vaultID string
		if err := json.Unmarshal(rawVaultID, &vaultID); err != nil {
			return nil, &ValidationError{Message: "vault_ids must be an array of strings"}
		}
		vaultIDs = append(vaultIDs, vaultID)
	}
	return vaultIDs, nil
}

func parseSessionProviderSelectors(raw json.RawMessage) (session.ProviderSelectors, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, &ValidationError{Message: "providers must be an object"}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, true, &ValidationError{Message: "providers must be an object"}
	}
	out := make(session.ProviderSelectors, len(fields))
	for providerID, rawSelector := range fields {
		if providerID == "" {
			return nil, true, &ValidationError{Message: "providers provider_id is required"}
		}
		if bytes.Equal(bytes.TrimSpace(rawSelector), []byte("null")) {
			return nil, true, &ValidationError{Message: "providers." + providerID + " must be an object"}
		}
		selector, err := parseSessionProviderSelector(providerID, rawSelector)
		if err != nil {
			return nil, true, err
		}
		out[providerID] = selector
	}
	return out, true, nil
}

func parseSessionProviderSelector(providerID string, raw json.RawMessage) (session.ProviderCredentialSelector, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return session.ProviderCredentialSelector{}, &ValidationError{Message: "providers." + providerID + " must be an object"}
	}
	if len(fields) != 1 {
		return session.ProviderCredentialSelector{}, &ValidationError{Message: "providers." + providerID + " must contain only credential_id"}
	}
	rawCredentialID, ok := fields["credential_id"]
	if !ok {
		return session.ProviderCredentialSelector{}, &ValidationError{Message: "providers." + providerID + ".credential_id is required"}
	}
	var credentialID string
	if err := json.Unmarshal(rawCredentialID, &credentialID); err != nil || credentialID == "" {
		return session.ProviderCredentialSelector{}, &ValidationError{Message: "providers." + providerID + ".credential_id is required"}
	}
	return session.ProviderCredentialSelector{CredentialID: credentialID}, nil
}

func parseSessionAgentReference(raw json.RawMessage) (session.AgentReference, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return session.AgentReference{}, &ValidationError{Message: "agent is required"}
	}
	var agentID string
	if err := json.Unmarshal(raw, &agentID); err == nil {
		return session.AgentReference{ID: agentID}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var ref agentReferenceHTTPRequest
	if err := decoder.Decode(&ref); err != nil {
		return session.AgentReference{}, &ValidationError{Message: "invalid agent field"}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return session.AgentReference{}, &ValidationError{Message: "invalid agent field"}
	}
	agentType, hasType, err := parseOptionalSessionJSONString(ref.Type, "agent.type")
	if err != nil {
		return session.AgentReference{}, err
	}
	if hasType && agentType != "agent" {
		return session.AgentReference{}, &ValidationError{Message: "agent.type must be agent"}
	}
	agentID, _, err = parseRequiredSessionJSONString(ref.ID, "agent.id")
	if err != nil {
		return session.AgentReference{}, err
	}
	version, err := parseOptionalPositiveSessionInt(ref.Version, "agent.version")
	if err != nil {
		return session.AgentReference{}, err
	}
	if agentID == "" {
		return session.AgentReference{}, &ValidationError{Message: "agent.id is required"}
	}
	return session.AgentReference{ID: agentID, Version: version}, nil
}

func parseRequiredSessionJSONString(raw json.RawMessage, field string) (string, bool, error) {
	value, present, err := parseOptionalSessionJSONString(raw, field)
	if err != nil {
		return "", false, err
	}
	if !present {
		return "", false, &ValidationError{Message: field + " is required"}
	}
	return value, true, nil
}

func parseOptionalSessionJSONString(raw json.RawMessage, field string) (string, bool, error) {
	if len(raw) == 0 {
		return "", false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false, &ValidationError{Message: field + " must be a string"}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false, &ValidationError{Message: field + " must be a string"}
	}
	return value, true, nil
}

func parseOptionalPositiveSessionInt(raw json.RawMessage, field string) (*int, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, &ValidationError{Message: field + " must be a positive integer"}
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, &ValidationError{Message: field + " must be a positive integer"}
	}
	if value <= 0 {
		return nil, &ValidationError{Message: field + " must be a positive integer"}
	}
	return &value, nil
}

func parseSessionListOptions(r *http.Request) (session.ListOptions, error) {
	values, err := parseStrictSessionQuery(r, []string{
		"limit",
		"page",
		"include_archived",
		"agent_id",
		"agent_version",
		"memory_store_id",
		"deployment_id",
		"statuses[]",
		"created_at[gt]",
		"created_at[gte]",
		"created_at[lt]",
		"created_at[lte]",
		"order",
		"beta",
	})
	if err != nil {
		return session.ListOptions{}, err
	}
	if err := requireSessionBetaQuery(values); err != nil {
		return session.ListOptions{}, err
	}
	limit, err := parseSessionLimit(values)
	if err != nil {
		return session.ListOptions{}, err
	}
	order := session.ListOrderDescending
	if rawValues, ok := values["order"]; ok {
		switch rawValues[0] {
		case string(session.ListOrderAscending):
			order = session.ListOrderAscending
		case string(session.ListOrderDescending):
			order = session.ListOrderDescending
		default:
			return session.ListOptions{}, &ValidationError{Message: "order must be asc or desc"}
		}
	}
	options := session.ListOptions{
		Limit: limit,
		Order: order,
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
	}
	if rawValues, ok := values["agent_id"]; ok {
		options.AgentID = rawValues[0]
	}
	if rawValues, ok := values["memory_store_id"]; ok {
		options.MemoryStoreID = rawValues[0]
	}
	if rawValues, ok := values["deployment_id"]; ok {
		options.DeploymentID = rawValues[0]
	}
	if rawValues, ok := values["statuses[]"]; ok {
		seen := map[session.Status]bool{}
		for _, raw := range rawValues {
			statusValue := session.Status(raw)
			switch statusValue {
			case session.StatusRescheduling, session.StatusRunning, session.StatusIdle, session.StatusTerminated:
			default:
				return session.ListOptions{}, &ValidationError{Message: "statuses[] contains an invalid status"}
			}
			seen[statusValue] = true
		}
		for _, statusValue := range []session.Status{session.StatusRescheduling, session.StatusRunning, session.StatusIdle, session.StatusTerminated} {
			if seen[statusValue] {
				options.Statuses = append(options.Statuses, statusValue)
			}
		}
	}
	if rawValues, ok := values["include_archived"]; ok {
		switch rawValues[0] {
		case "true":
			options.IncludeArchived = true
		case "false":
			options.IncludeArchived = false
		default:
			return session.ListOptions{}, &ValidationError{Message: "include_archived must be true or false"}
		}
	}
	if rawValues, ok := values["agent_version"]; ok {
		if options.AgentID == "" {
			return session.ListOptions{}, &ValidationError{Message: "agent_version requires agent_id"}
		}
		value, err := strconv.Atoi(rawValues[0])
		if err != nil || value <= 0 {
			return session.ListOptions{}, &ValidationError{Message: "agent_version is invalid"}
		}
		options.AgentVersion = value
	}
	for key, target := range map[string]**time.Time{
		"created_at[gt]":  &options.CreatedAtGT,
		"created_at[gte]": &options.CreatedAtGTE,
		"created_at[lt]":  &options.CreatedAtLT,
		"created_at[lte]": &options.CreatedAtLTE,
	} {
		if rawValues, ok := values[key]; ok {
			raw := rawValues[0]
			parsed, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				return session.ListOptions{}, &ValidationError{Message: key + " is invalid"}
			}
			*target = &parsed
		}
	}
	return options, nil
}

func parseSessionResourceListOptions(r *http.Request) (session.ResourceListOptions, error) {
	values, err := parseStrictSessionQuery(r, []string{"limit", "page", "beta"})
	if err != nil {
		return session.ResourceListOptions{}, err
	}
	if err := requireSessionBetaQuery(values); err != nil {
		return session.ResourceListOptions{}, err
	}
	limit, err := parseSessionLimit(values)
	if err != nil {
		return session.ResourceListOptions{}, err
	}
	options := session.ResourceListOptions{Limit: limit}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
	}
	return options, nil
}

func parseSessionThreadListOptions(r *http.Request) (session.ThreadListOptions, error) {
	values, err := parseStrictSessionQuery(r, []string{"limit", "page", "beta"})
	if err != nil {
		return session.ThreadListOptions{}, err
	}
	if err := requireSessionBetaQuery(values); err != nil {
		return session.ThreadListOptions{}, err
	}
	limit, err := parseSessionLimit(values)
	if err != nil {
		return session.ThreadListOptions{}, err
	}
	options := session.ThreadListOptions{Limit: limit}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
	}
	return options, nil
}

func validateSessionBetaQuery(values url.Values) error {
	rawValues, ok := values["beta"]
	if !ok {
		return nil
	}
	if rawValues[0] != "true" {
		return &ValidationError{Message: "beta must be true"}
	}
	return nil
}

func requireSessionBetaQuery(values url.Values) error {
	if _, ok := values["beta"]; !ok {
		return &ValidationError{Message: "beta must be true"}
	}
	return validateSessionBetaQuery(values)
}

func validateSessionBetaRoute(r *http.Request) error {
	values, err := parseStrictSessionQuery(r, []string{"beta"})
	if err != nil {
		return err
	}
	return requireSessionBetaQuery(values)
}

func parseStrictSessionQuery(r *http.Request, allowed []string) (url.Values, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, &ValidationError{Message: "invalid query string"}
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if len(rawValues) != 1 && key != "statuses[]" {
			return nil, &ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" {
			return nil, &ValidationError{Message: "empty query parameter " + key}
		}
		if _, ok := allowedSet[key]; !ok {
			return nil, &ValidationError{Message: "unknown query parameter " + key}
		}
	}
	return values, nil
}

func parseSessionLimit(values url.Values) (int, error) {
	rawValues, ok := values["limit"]
	if !ok {
		return 20, nil
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

func readStrictSessionBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, sessionBodyByteCap)
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

func requireEmptySessionBody(w http.ResponseWriter, r *http.Request) error {
	body, err := readStrictSessionBody(w, r)
	if err != nil {
		return err
	}
	if len(body) != 0 {
		return &ValidationError{Message: "request body must be empty"}
	}
	return nil
}

func decodeStrictSessionBody(w http.ResponseWriter, r *http.Request, target any) error {
	body, err := readStrictSessionBody(w, r)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return &ValidationError{Message: "invalid request body"}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return &ValidationError{Message: "invalid request body: unexpected trailing data"}
		}
		return &ValidationError{Message: "invalid request body"}
	}
	return nil
}
