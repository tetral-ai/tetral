package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const memoryBodyByteCap = 1 << 20

type MemoryService interface {
	CreateStore(context.Context, workspace.ID, memory.CreateStoreRequest) (*memory.Store, error)
	GetStore(context.Context, workspace.ID, string) (*memory.Store, error)
	UpdateStore(context.Context, workspace.ID, string, memory.StorePatch) (*memory.Store, error)
	ListStores(context.Context, workspace.ID, memory.ListStoresOptions) (memory.StoreListResult, error)
	ArchiveStore(context.Context, workspace.ID, string) (*memory.Store, error)
	DeleteStore(context.Context, workspace.ID, string) error
	CreateMemory(context.Context, workspace.ID, string, memory.CreateMemoryRequest, memory.Actor) (*memory.Memory, error)
	GetMemory(context.Context, workspace.ID, string, string, string) (*memory.Memory, error)
	UpdateMemory(context.Context, workspace.ID, string, string, memory.UpdateMemoryRequest, memory.Actor) (*memory.Memory, error)
	DeleteMemory(context.Context, workspace.ID, string, string, *string, memory.Actor) (*memory.DeleteMemoryResult, error)
	ListMemories(context.Context, workspace.ID, string, memory.ListMemoriesOptions) (memory.MemoryListResult, error)
	ListMemoryVersions(context.Context, workspace.ID, string, memory.ListMemoryVersionsOptions) (memory.MemoryVersionListResult, error)
	GetMemoryVersion(context.Context, workspace.ID, string, string, string) (*memory.MemoryVersion, error)
	RedactMemoryVersion(context.Context, workspace.ID, string, string, memory.Actor) (*memory.MemoryVersion, error)
}

type MemoryHandler struct {
	service MemoryService
}

func NewMemoryHandler(service MemoryService) *MemoryHandler {
	return &MemoryHandler{service: service}
}

func (h *MemoryHandler) createStore(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readMemoryBody(w, r, memoryBodyByteCap)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := memory.DecodeCreateStoreRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.CreateStore(r.Context(), workspaceID, request)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) listStores(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"limit", "page", "include_archived", "created_at[gte]", "created_at[lte]"}); err != nil {
		writeError(w, r, err)
		return
	}
	options, err := memory.DecodeListStoresOptions(memoryQueryWithoutBeta(r))
	if err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListStores(r.Context(), workspaceID, options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) getStore(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.GetStore(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) updateStore(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readMemoryBody(w, r, memoryBodyByteCap)
	if err != nil {
		writeError(w, r, err)
		return
	}
	patch, err := memory.DecodeUpdateStoreRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.UpdateStore(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), patch)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) deleteStore(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptyMemoryBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	storeID := chi.URLParam(r, "memory_store_id")
	if err := h.service.DeleteStore(r.Context(), workspaceID, storeID); err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}{ID: storeID, Type: "memory_store_deleted"})
}

func (h *MemoryHandler) archiveStore(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptyMemoryBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ArchiveStore(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) createMemory(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"view"}); err != nil {
		writeError(w, r, err)
		return
	}
	view, err := parseMemoryView(r.URL.Query().Get("view"), memory.ViewBasic)
	if err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readMemoryBody(w, r, memoryBodyByteCap)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := memory.DecodeCreateMemoryRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request.View = view
	workspaceID, actor, err := memoryPrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.CreateMemory(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), request, actor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) listMemories(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"limit", "page", "path_prefix", "depth", "view", "order_by", "order"}); err != nil {
		writeError(w, r, err)
		return
	}
	options, err := memory.DecodeListMemoriesOptions(memoryQueryWithoutBeta(r))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if options.View, err = parseMemoryView(options.View, memory.ViewBasic); err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateMemoryListOrder(options.OrderBy, options.Order); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListMemories(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) getMemory(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"view"}); err != nil {
		writeError(w, r, err)
		return
	}
	view, err := parseMemoryView(r.URL.Query().Get("view"), memory.ViewFull)
	if err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.GetMemory(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), chi.URLParam(r, "memory_id"), view)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) updateMemory(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"view"}); err != nil {
		writeError(w, r, err)
		return
	}
	view, err := parseMemoryView(r.URL.Query().Get("view"), memory.ViewBasic)
	if err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readMemoryBody(w, r, memoryBodyByteCap)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := memory.DecodeUpdateMemoryRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request.View = view
	workspaceID, actor, err := memoryPrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.UpdateMemory(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), chi.URLParam(r, "memory_id"), request, actor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) deleteMemory(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"expected_content_sha256"}); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptyMemoryBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	var expectedHash *string
	if raw := r.URL.Query().Get("expected_content_sha256"); raw != "" {
		expectedHash = &raw
	}
	workspaceID, actor, err := memoryPrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.DeleteMemory(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), chi.URLParam(r, "memory_id"), expectedHash, actor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) listMemoryVersions(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"limit", "page", "memory_id", "operation", "session_id", "api_key_id", "created_at[gte]", "created_at[lte]", "view"}); err != nil {
		writeError(w, r, err)
		return
	}
	options, err := memory.DecodeListMemoryVersionsOptions(memoryQueryWithoutBeta(r))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if options.View, err = parseMemoryView(options.View, memory.ViewBasic); err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateMemoryOperation(options.Operation); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListMemoryVersions(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) getMemoryVersion(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, []string{"view"}); err != nil {
		writeError(w, r, err)
		return
	}
	view, err := parseMemoryView(r.URL.Query().Get("view"), memory.ViewFull)
	if err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := memoryWorkspaceID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.GetMemoryVersion(r.Context(), workspaceID, chi.URLParam(r, "memory_store_id"), chi.URLParam(r, "memory_version_id"), view)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *MemoryHandler) redactMemoryVersion(w http.ResponseWriter, r *http.Request) {
	if err := requireStrictQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	if err := requireEmptyMemoryBody(w, r); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, actor, err := memoryPrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.RedactMemoryVersion(
		r.Context(),
		workspaceID,
		chi.URLParam(r, "memory_store_id"),
		chi.URLParam(r, "memory_version_id"),
		actor,
	)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func readMemoryBody(w http.ResponseWriter, r *http.Request, capBytes int64) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, capBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &memory.RequestTooLargeError{Message: "request body too large"}
		}
		return nil, err
	}
	return body, nil
}

func requireEmptyMemoryBody(w http.ResponseWriter, r *http.Request) error {
	body, err := readMemoryBody(w, r, 1024)
	if err != nil {
		var tooLarge *memory.RequestTooLargeError
		if errors.As(err, &tooLarge) {
			return &memory.ValidationError{Message: "request body must be empty"}
		}
		return err
	}
	if strings.TrimSpace(string(body)) != "" {
		return &memory.ValidationError{Message: "request body must be empty"}
	}
	return nil
}

func memoryWorkspaceID(r *http.Request) (workspace.ID, error) {
	principal, ok := auth.PrincipalFromContext(r.Context())
	if !ok || principal.APIKeyID == "" {
		return "", &auth.AuthenticationError{Message: "missing authenticated principal"}
	}
	return principal.Workspace.ID, nil
}

func memoryPrincipal(r *http.Request) (workspace.ID, memory.Actor, error) {
	principal, ok := auth.PrincipalFromContext(r.Context())
	if !ok || principal.APIKeyID == "" {
		return "", memory.Actor{}, &auth.AuthenticationError{Message: "missing authenticated principal"}
	}
	return principal.Workspace.ID, memory.Actor{Type: memory.ActorAPI, APIKeyID: principal.APIKeyID}, nil
}

func requireStrictQuery(r *http.Request, allowed []string) error {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &memory.ValidationError{Message: "invalid query string"}
	}
	allowedSet := make(map[string]struct{}, len(allowed)+1)
	allowedSet["beta"] = struct{}{}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if len(rawValues) != 1 {
			return &memory.ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" {
			return &memory.ValidationError{Message: "empty query parameter " + key}
		}
		if _, ok := allowedSet[key]; !ok {
			return &memory.ValidationError{Message: "unknown query parameter " + key}
		}
	}
	if rawValues, ok := values["beta"]; !ok || rawValues[0] != "true" {
		return &memory.ValidationError{Message: "beta must be true"}
	}
	return nil
}

func memoryQueryWithoutBeta(r *http.Request) url.Values {
	values := r.URL.Query()
	values.Del("beta")
	return values
}

func parseMemoryView(raw string, defaultView string) (string, error) {
	if raw == "" {
		return defaultView, nil
	}
	if raw != memory.ViewBasic && raw != memory.ViewFull {
		return "", &memory.ValidationError{Message: "view must be basic or full"}
	}
	return raw, nil
}

func validateMemoryListOrder(orderBy string, order string) error {
	switch orderBy {
	case "", memory.MemoryOrderByPath, memory.MemoryOrderByCreatedAt, memory.MemoryOrderByUpdatedAt:
	default:
		return &memory.ValidationError{Message: "order_by must be path, created_at, or updated_at"}
	}
	switch order {
	case "", memory.SortAscending, memory.SortDescending:
	default:
		return &memory.ValidationError{Message: "order must be asc or desc"}
	}
	return nil
}

func validateMemoryOperation(operation string) error {
	switch operation {
	case "", memory.OperationCreated, memory.OperationModified, memory.OperationDeleted:
		return nil
	default:
		return &memory.ValidationError{Message: "operation must be created, modified, or deleted"}
	}
}
