package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
)

// NewRouter builds a chi router with all middleware and routes
// registered. Callers that serve /v1 routes must pass
// WithAuthenticator(...) so the auth middleware can attach an
// authenticated workspace to the request context. The apiKey
// parameter is retained for source compatibility but no longer
// creates a default-workspace production fallback.
func NewRouter(sessionHandler *SessionHandler, apiKey string, options ...RouterOption) http.Handler {
	router := chi.NewRouter()

	opts := &routerOptions{}
	for _, o := range options {
		o(opts)
	}
	applyRouterOptionDefaults(opts)

	router.Use(RequestIDMiddleware)
	router.Use(RequestLogMiddleware(opts.logger, opts.slowRequestThreshold, WithRequestLogMetrics(opts.requestMetrics)))
	router.Use(recoveryMiddleware(opts.logger))

	registerRoutes(router, sessionHandler, resolveAuthenticator(apiKey, opts), opts)

	return router
}

// NewRouterWithPanicHandler builds a router with an extra test-only panic route
// so that recovery middleware can be tested without exposing it in production.
func NewRouterWithPanicHandler(sessionHandler *SessionHandler, apiKey string, options ...RouterOption) http.Handler {
	router := chi.NewRouter()

	opts := &routerOptions{}
	for _, o := range options {
		o(opts)
	}
	applyRouterOptionDefaults(opts)

	router.Use(RequestIDMiddleware)
	router.Use(RequestLogMiddleware(opts.logger, opts.slowRequestThreshold, WithRequestLogMetrics(opts.requestMetrics)))
	router.Use(recoveryMiddleware(opts.logger))

	registerRoutes(router, sessionHandler, resolveAuthenticator(apiKey, opts), opts)

	router.Get("/__test_panic", func(_ http.ResponseWriter, _ *http.Request) {
		panic("test panic")
	})

	return router
}

// RouterOption configures optional handlers for NewRouter.
type RouterOption func(*routerOptions)

type routerOptions struct {
	agentHandler         *AgentHandler
	environmentHandler   *EnvironmentHandler
	vaultHandler         *VaultHandler
	skillHandler         *SkillHandler
	fileHandler          *FileHandler
	memoryHandler        *MemoryHandler
	sessionEventHandler  *SessionEventHandler
	sessionEventList     SessionEventListHandler
	authenticator        auth.Authenticator
	internalVerifier     *auth.InternalPrincipalVerifier
	logger               *slog.Logger
	slowRequestThreshold time.Duration
	requestMetrics       RequestMetricsRecorder
}

type SessionEventListHandler interface {
	ServeSessionEvents(http.ResponseWriter, *http.Request)
	ServeThreadEvents(http.ResponseWriter, *http.Request)
}

// WithAgentHandler adds real agent handlers to the router.
func WithAgentHandler(h *AgentHandler) RouterOption {
	return func(o *routerOptions) { o.agentHandler = h }
}

// WithEnvironmentHandler adds real environment handlers to the router.
func WithEnvironmentHandler(h *EnvironmentHandler) RouterOption {
	return func(o *routerOptions) { o.environmentHandler = h }
}

// WithVaultHandler adds real vault and credential handlers to the router.
func WithVaultHandler(h *VaultHandler) RouterOption {
	return func(o *routerOptions) { o.vaultHandler = h }
}

// WithSkillHandler installs the /v1/skills route family handlers.
// Without this option the nine /v1/skills routes return 501
// not_implemented stubs. Production wiring constructs the SkillHandler
// only when the BlobStore configuration is present and valid; without
// the BlobStore the upload pipeline cannot run.
func WithSkillHandler(h *SkillHandler) RouterOption {
	return func(o *routerOptions) { o.skillHandler = h }
}

// WithFileHandler installs the /v1/files route family handlers.
// Without this option the five /v1/files routes return 501
// not_implemented stubs. Production wiring constructs the
// FileHandler only when BlobStore configuration is present and valid;
// without BlobStore the upload and content pipeline cannot run.
func WithFileHandler(h *FileHandler) RouterOption {
	return func(o *routerOptions) { o.fileHandler = h }
}

// WithMemoryHandler installs implemented /v1/memory_stores route handlers.
func WithMemoryHandler(h *MemoryHandler) RouterOption {
	return func(o *routerOptions) { o.memoryHandler = h }
}

// WithSessionEventHandler installs the public session-event ingress
// endpoint. Without this option /v1/sessions/{session_id}/events
// returns the not_implemented stub.
func WithSessionEventHandler(h *SessionEventHandler) RouterOption {
	return func(o *routerOptions) { o.sessionEventHandler = h }
}

func WithSessionEventListHandler(h SessionEventListHandler) RouterOption {
	return func(o *routerOptions) { o.sessionEventList = h }
}

// WithAuthenticator installs the auth.Authenticator used by /v1
// routes. Production wiring uses this option so bootstrap and
// standard PostgreSQL-backed keys authenticate.
func WithAuthenticator(a auth.Authenticator) RouterOption {
	return func(o *routerOptions) { o.authenticator = a }
}

// WithInternalPrincipalVerifier installs the auth signed-principal
// verifier used by public target services behind the Edge/Auth boundary.
func WithInternalPrincipalVerifier(v *auth.InternalPrincipalVerifier) RouterOption {
	return func(o *routerOptions) { o.internalVerifier = v }
}

// WithLogger installs the boundary logger used by middleware.
func WithLogger(logger *slog.Logger) RouterOption {
	return func(o *routerOptions) { o.logger = logger }
}

func WithRequestMetrics(metrics RequestMetricsRecorder) RouterOption {
	return func(o *routerOptions) { o.requestMetrics = metrics }
}

func applyRouterOptionDefaults(opts *routerOptions) {
	if opts.logger == nil {
		opts.logger = workload.ComponentLogger("api")
	}
	if opts.slowRequestThreshold == 0 {
		opts.slowRequestThreshold = DefaultSlowRequestThreshold
	}
}

// resolveAuthenticator picks the explicit authenticator supplied via
// WithAuthenticator if present. Without one, /v1 routes fail closed
// during authentication rather than manufacturing workspace.DefaultID.
func resolveAuthenticator(_ string, opts *routerOptions) auth.Authenticator {
	if opts.authenticator != nil {
		return opts.authenticator
	}
	return auth.AuthenticatorFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{}, &auth.AuthenticationError{Message: "authentication unavailable"}
	})
}

func registerRoutes(router chi.Router, sessionHandler *SessionHandler, authenticator auth.Authenticator, opts *routerOptions) {
	router.Get("/health", healthHandler)

	router.Route("/v1", func(r chi.Router) {
		if opts.internalVerifier != nil {
			r.Use(internalPrincipalMiddleware(opts.internalVerifier))
		} else {
			r.Use(authMiddleware(authenticator, opts.logger))
		}
		// Sessions — real handlers
		registerPublicRoute(r, http.MethodPost, "/sessions", sessionHandler.createSession, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/sessions", sessionHandler.listSessions, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}", sessionHandler.getSession, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}", sessionHandler.updateSession, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodDelete, "/sessions/{session_id}", sessionHandler.deleteSession, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}/archive", sessionHandler.archiveSession, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}/threads", sessionHandler.listThreads, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}/threads/{thread_id}", sessionHandler.getThread, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}/threads/{thread_id}/archive", sessionHandler.archiveThread, sessionHandler, opts, false)
		if opts.sessionEventHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}/events", opts.sessionEventHandler.appendClientEvents, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}/events", stubHandler, sessionHandler, opts, true)
		}
		if opts.sessionEventList != nil {
			registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}/events", opts.sessionEventList.ServeSessionEvents, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}/threads/{thread_id}/events", opts.sessionEventList.ServeThreadEvents, sessionHandler, opts, false)
		}

		// Models.
		registerPublicRoute(r, http.MethodGet, "/models", listModels, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/models/{model_id}", retrieveModel, sessionHandler, opts, false)

		// Resources
		registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}/resources", sessionHandler.addResource, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}/resources", sessionHandler.listResources, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodGet, "/sessions/{session_id}/resources/{resource_id}", sessionHandler.getResource, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodPost, "/sessions/{session_id}/resources/{resource_id}", sessionHandler.updateResource, sessionHandler, opts, false)
		registerPublicRoute(r, http.MethodDelete, "/sessions/{session_id}/resources/{resource_id}", sessionHandler.deleteResource, sessionHandler, opts, false)

		// Agents
		if opts.agentHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/agents", opts.agentHandler.createAgent, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/agents", opts.agentHandler.listAgents, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/agents/{agent_id}", opts.agentHandler.getAgent, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/agents/{agent_id}", opts.agentHandler.updateAgent, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/agents/{agent_id}/archive", opts.agentHandler.archiveAgent, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/agents/{agent_id}/versions", opts.agentHandler.listAgentVersions, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/agents", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/agents", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/agents/{agent_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/agents/{agent_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/agents/{agent_id}/archive", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/agents/{agent_id}/versions", stubHandler, sessionHandler, opts, true)
		}

		// Environments
		if opts.environmentHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/environments", opts.environmentHandler.createEnvironment, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/environments", opts.environmentHandler.listEnvironments, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/environments/{environment_id}", opts.environmentHandler.getEnvironment, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/environments/{environment_id}", opts.environmentHandler.updateEnvironment, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/environments/{environment_id}", opts.environmentHandler.deleteEnvironment, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/environments/{environment_id}/archive", opts.environmentHandler.archiveEnvironment, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/environments", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/environments", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/environments/{environment_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/environments/{environment_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/environments/{environment_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/environments/{environment_id}/archive", stubHandler, sessionHandler, opts, true)
		}

		// Vaults
		if opts.vaultHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/vaults", opts.vaultHandler.createVault, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/vaults", opts.vaultHandler.listVaults, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/vaults/{vault_id}", opts.vaultHandler.getVault, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}", opts.vaultHandler.updateVault, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/vaults/{vault_id}", opts.vaultHandler.deleteVault, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/vaults", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/vaults", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/vaults/{vault_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/vaults/{vault_id}", stubHandler, sessionHandler, opts, true)
		}
		if opts.vaultHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/archive", opts.vaultHandler.archiveVault, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/archive", stubHandler, sessionHandler, opts, true)
		}

		// Credentials
		if opts.vaultHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials", opts.vaultHandler.createCredential, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/vaults/{vault_id}/credentials", opts.vaultHandler.listCredentials, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/vaults/{vault_id}/credentials/{credential_id}", opts.vaultHandler.getCredential, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials/{credential_id}", opts.vaultHandler.updateCredential, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials/{credential_id}/mcp_oauth_validate", opts.vaultHandler.validateMCPOAuthCredential, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/vaults/{vault_id}/credentials/{credential_id}", opts.vaultHandler.deleteCredential, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/vaults/{vault_id}/credentials", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/vaults/{vault_id}/credentials/{credential_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials/{credential_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials/{credential_id}/mcp_oauth_validate", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/vaults/{vault_id}/credentials/{credential_id}", stubHandler, sessionHandler, opts, true)
		}
		if opts.vaultHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials/{credential_id}/archive", opts.vaultHandler.archiveCredential, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/vaults/{vault_id}/credentials/{credential_id}/archive", stubHandler, sessionHandler, opts, true)
		}

		// Files
		if opts.fileHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/files", opts.fileHandler.createFile, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/files", opts.fileHandler.listFiles, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/files/{file_id}", opts.fileHandler.getFile, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/files/{file_id}", opts.fileHandler.deleteFile, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/files/{file_id}/content", opts.fileHandler.getFileContent, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/files", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/files", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/files/{file_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/files/{file_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/files/{file_id}/content", stubHandler, sessionHandler, opts, true)
		}

		// Memory stores.
		if opts.memoryHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/memory_stores", opts.memoryHandler.createStore, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/memory_stores", opts.memoryHandler.listStores, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}", opts.memoryHandler.getStore, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}", opts.memoryHandler.updateStore, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/memory_stores/{memory_store_id}", opts.memoryHandler.deleteStore, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/archive", opts.memoryHandler.archiveStore, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/memories", opts.memoryHandler.createMemory, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memories", opts.memoryHandler.listMemories, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memories/{memory_id}", opts.memoryHandler.getMemory, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/memories/{memory_id}", opts.memoryHandler.updateMemory, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/memory_stores/{memory_store_id}/memories/{memory_id}", opts.memoryHandler.deleteMemory, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memory_versions", opts.memoryHandler.listMemoryVersions, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}", opts.memoryHandler.getMemoryVersion, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}/redact", opts.memoryHandler.redactMemoryVersion, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/memory_stores", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/memory_stores", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/memory_stores/{memory_store_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/archive", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/memories", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memories", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memories/{memory_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/memories/{memory_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/memory_stores/{memory_store_id}/memories/{memory_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memory_versions", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}/redact", stubHandler, sessionHandler, opts, true)
		}

		// Skills
		if opts.skillHandler != nil {
			registerPublicRoute(r, http.MethodPost, "/skills", opts.skillHandler.createSkill, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/skills", opts.skillHandler.listSkills, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}", opts.skillHandler.getSkill, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/skills/{skill_id}", opts.skillHandler.deleteSkill, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodPost, "/skills/{skill_id}/versions", opts.skillHandler.createVersion, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}/versions", opts.skillHandler.listVersions, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}/versions/{version}", opts.skillHandler.getVersion, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}/versions/{version}/content", opts.skillHandler.getVersionContent, sessionHandler, opts, false)
			registerPublicRoute(r, http.MethodDelete, "/skills/{skill_id}/versions/{version}", opts.skillHandler.deleteVersion, sessionHandler, opts, false)
		} else {
			registerPublicRoute(r, http.MethodPost, "/skills", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/skills", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/skills/{skill_id}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodPost, "/skills/{skill_id}/versions", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}/versions", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}/versions/{version}", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodGet, "/skills/{skill_id}/versions/{version}/content", stubHandler, sessionHandler, opts, true)
			registerPublicRoute(r, http.MethodDelete, "/skills/{skill_id}/versions/{version}", stubHandler, sessionHandler, opts, true)
		}

		r.NotFound(unsupportedV1SurfaceHandler)
		r.MethodNotAllowed(unsupportedV1SurfaceMethodNotAllowedHandler)
	})
}

func unsupportedV1SurfaceHandler(w http.ResponseWriter, r *http.Request) {
	if isUnsupportedSDKSurfacePath(strings.TrimPrefix(r.URL.Path, "/v1")) {
		writeError(w, r, &ValidationError{Message: "unsupported SDK surface for this stage"})
		return
	}
	http.NotFound(w, r)
}

func unsupportedV1SurfaceMethodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	if isUnsupportedSDKSurfacePath(strings.TrimPrefix(r.URL.Path, "/v1")) {
		writeError(w, r, &ValidationError{Message: "unsupported SDK surface for this stage"})
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func isUnsupportedSDKSurfacePath(path string) bool {
	switch {
	case path == "/messages" || strings.HasPrefix(path, "/messages/"):
		return true
	case path == "/user_profiles" || strings.HasPrefix(path, "/user_profiles/"):
		return true
	case path == "/webhooks" || strings.HasPrefix(path, "/webhooks/"):
		return true
	case path == "/deployments" || strings.HasPrefix(path, "/deployments/"):
		return true
	case path == "/deployment_runs" || strings.HasPrefix(path, "/deployment_runs/"):
		return true
	case path == "/multiagent" || strings.HasPrefix(path, "/multiagent/") || strings.Contains(path, "/multiagent/"):
		return true
	case strings.HasPrefix(path, "/environments/") && strings.Contains(path, "/work"):
		return true
	default:
		return false
	}
}
