package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type operationContextKey struct{}
type authorizationContextKey struct{}

// PublicOperationHandler records the classification on the actual registered
// handler, so route coverage can inspect the router rather than a parallel list.
type PublicOperationHandler struct {
	Method    string
	Pattern   string
	Operation auth.Operation
	next      http.HandlerFunc
}

func (h *PublicOperationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), operationContextKey{}, h.Operation)
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		err := &auth.AuthenticationError{Message: "missing authenticated principal"}
		auth.RecordDecision(ctx, "signed_principal", err, auth.AuditEvent{})
		writeError(w, r, err)
		return
	}
	// Admission checks the trusted workspace operation first. A denied action
	// cannot probe resource existence by triggering its owner lookup.
	if err := auth.AuthorizeWithAudit(ctx, principal, h.Operation, workspaceReference(principal.Workspace.ID)); err != nil {
		writeError(w, r, err)
		return
	}
	h.next(w, r.WithContext(ctx))
}

// DeclarePublicOperation binds a registration to the reviewed route policy.
// The business owner must authorize trusted resources before effect/disclosure.
func DeclarePublicOperation(method, pattern string, handler http.HandlerFunc) http.Handler {
	operation, ok := auth.OperationForRoute(method, pattern)
	if !ok {
		panic("unclassified public business route: " + method + " " + pattern)
	}
	return &PublicOperationHandler{Method: method, Pattern: pattern, Operation: operation, next: handler}
}

// AuthorizePublicRequest invokes the shared policy with owner-resolved facts.
func AuthorizePublicRequest(ctx context.Context, resource auth.ResourceReference) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return &auth.AuthenticationError{Message: "missing authenticated principal"}
	}
	operation, ok := ctx.Value(operationContextKey{}).(auth.Operation)
	if !ok {
		err := &auth.PermissionError{}
		auth.RecordDecision(ctx, "operation", err, auth.AuditFacts(principal))
		return err
	}
	return auth.AuthorizeWithAudit(ctx, principal, operation, resource)
}

// authorizeWorkspace applies the per-request gate that registerPublicRoute
// installs (the route's operation plus an owner-resolved resource reference)
// and returns the authorized workspace. Handlers reach it through
// requestWorkspace. Resource loaders remain local to their HTTP owner and
// retain its tenant-safe repository semantics.
func authorizeWorkspace(ctx context.Context) (workspace.ID, error) {
	ws, err := workspace.MustIDFromContext(ctx)
	if err != nil {
		return "", err
	}
	gate, ok := ctx.Value(authorizationContextKey{}).(func(workspace.ID) error)
	if !ok {
		return "", &auth.PermissionError{}
	}
	if err := gate(ws); err != nil {
		return "", err
	}
	return ws, nil
}

func registerPublicRoute(r chi.Router, method, pattern string, handler http.HandlerFunc, sessions *SessionHandler, opts *routerOptions, stub bool) {
	// Unimplemented routes authorize the typed workspace here. Real handlers
	// invoke the gate through requestWorkspace: GET requests authorize the
	// workspace and read handlers then authorize their owner result before
	// disclosure; mutations first resolve trusted owner facts. The GET
	// event-list handlers from internal/eventstream do not invoke this gate; they
	// call AuthorizePublicRequest after their tenant-safe list.
	r.Method(method, pattern, DeclarePublicOperation(method, "/v1"+pattern, func(w http.ResponseWriter, req *http.Request) {
		gate := func(ws workspace.ID) error {
			if stub || req.Method == http.MethodGet {
				return AuthorizePublicRequest(req.Context(), workspaceReference(ws))
			}
			reference, err := resolvePublicResource(req, ws, sessions, opts)
			if err != nil {
				return err
			}
			return AuthorizePublicRequest(req.Context(), reference)
		}
		req = req.WithContext(context.WithValue(req.Context(), authorizationContextKey{}, gate))
		if stub {
			if _, err := authorizeWorkspace(req.Context()); err != nil {
				writeError(w, req, err)
				return
			}
		}
		handler(w, req)
	}))
}

func workspaceReference(ws workspace.ID) auth.ResourceReference {
	return auth.ResourceReference{WorkspaceID: ws, Type: "workspace"}
}
func resourceReference(ws workspace.ID, kind, id string, err error) (auth.ResourceReference, error) {
	if err != nil {
		return auth.ResourceReference{}, err
	}
	if id == "" {
		return auth.ResourceReference{}, errors.New("resource lookup returned no identity")
	}
	return auth.ResourceReference{WorkspaceID: ws, Type: kind, ID: id}, nil
}

// authorizeReadResource uses a canonical owner result, or a parent identity
// proven by the successful tenant-safe nested list. It never treats a route
// selector alone as evidence that the resource exists in the workspace.
func authorizeReadResource(w http.ResponseWriter, r *http.Request, ws workspace.ID, kind, id string) bool {
	reference, err := resourceReference(ws, kind, id, nil)
	if err == nil {
		err = AuthorizePublicRequest(r.Context(), reference)
	}
	if err != nil {
		writeError(w, r, err)
		return false
	}
	return true
}

func resolvePublicResource(r *http.Request, ws workspace.ID, sessions *SessionHandler, opts *routerOptions) (auth.ResourceReference, error) {
	ctx := r.Context()
	if id := chi.URLParam(r, "session_id"); id != "" {
		if sessions == nil {
			return auth.ResourceReference{}, errors.New("session resource owner is not configured")
		}
		if threadID := chi.URLParam(r, "thread_id"); threadID != "" {
			value, err := sessions.service.GetThread(ctx, ws, id, threadID)
			if err != nil {
				return auth.ResourceReference{}, err
			}
			return resourceReference(ws, "thread", value.ID, nil)
		}
		if resourceID := chi.URLParam(r, "resource_id"); resourceID != "" {
			if r.Method == http.MethodDelete {
				resolved, err := sessions.service.LookupResourceDeletion(ctx, ws, id, resourceID)
				return resourceReference(ws, "session_resource", resolved, err)
			}
			value, err := sessions.service.GetResource(ctx, ws, id, resourceID)
			if err != nil {
				return auth.ResourceReference{}, err
			}
			return resourceReference(ws, "session_resource", value.ID, nil)
		}
		if r.Method == http.MethodDelete {
			resolved, err := sessions.service.LookupSessionDeletion(ctx, ws, id)
			return resourceReference(ws, "session", resolved, err)
		}
		value, err := sessions.service.Get(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "session", value.ID, nil)
	}
	if id := chi.URLParam(r, "agent_id"); id != "" && opts.agentHandler != nil {
		value, err := opts.agentHandler.service.Get(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "agent", value.ID, nil)
	}
	if id := chi.URLParam(r, "environment_id"); id != "" && opts.environmentHandler != nil {
		value, err := opts.environmentHandler.service.Get(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "environment", value.ID, nil)
	}
	if id := chi.URLParam(r, "vault_id"); id != "" && opts.vaultHandler != nil {
		if credentialID := chi.URLParam(r, "credential_id"); credentialID != "" {
			value, err := opts.vaultHandler.service.GetCredential(ctx, ws, id, credentialID)
			if err != nil {
				return auth.ResourceReference{}, err
			}
			return resourceReference(ws, "credential", value.ID, nil)
		}
		value, err := opts.vaultHandler.service.GetVault(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "vault", value.ID, nil)
	}
	if id := chi.URLParam(r, "file_id"); id != "" && opts.fileHandler != nil {
		value, err := opts.fileHandler.service.GetFile(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "file", value.ID, nil)
	}
	if id := chi.URLParam(r, "skill_id"); id != "" && opts.skillHandler != nil {
		if version := chi.URLParam(r, "version"); version != "" {
			value, err := opts.skillHandler.service.GetVersion(ctx, ws, id, version)
			if err != nil {
				return auth.ResourceReference{}, err
			}
			return resourceReference(ws, "skill_version", value.ID, nil)
		}
		value, err := opts.skillHandler.service.GetSkill(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "skill", value.ID, nil)
	}
	if id := chi.URLParam(r, "memory_store_id"); id != "" && opts.memoryHandler != nil {
		if memoryID := chi.URLParam(r, "memory_id"); memoryID != "" {
			value, err := opts.memoryHandler.service.GetMemory(ctx, ws, id, memoryID, memory.ViewBasic)
			if err != nil {
				return auth.ResourceReference{}, err
			}
			return resourceReference(ws, "memory", value.ID, nil)
		}
		if versionID := chi.URLParam(r, "memory_version_id"); versionID != "" {
			value, err := opts.memoryHandler.service.GetMemoryVersion(ctx, ws, id, versionID, memory.ViewBasic)
			if err != nil {
				return auth.ResourceReference{}, err
			}
			return resourceReference(ws, "memory_version", value.ID, nil)
		}
		value, err := opts.memoryHandler.service.GetStore(ctx, ws, id)
		if err != nil {
			return auth.ResourceReference{}, err
		}
		return resourceReference(ws, "memory_store", value.ID, nil)
	}
	return workspaceReference(ws), nil
}
