package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
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
		opts.logger = defaultRouterLogger(os.Stderr)
	}
	if opts.slowRequestThreshold == 0 {
		opts.slowRequestThreshold = DefaultSlowRequestThreshold
	}
}

func defaultRouterLogger(writer io.Writer) *slog.Logger {
	return workload.NewLogger(writer, "api", "", "")
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
		r.Post("/sessions", sessionHandler.createSession)
		r.Get("/sessions", sessionHandler.listSessions)
		r.Get("/sessions/{session_id}", sessionHandler.getSession)
		r.Post("/sessions/{session_id}", sessionHandler.updateSession)
		r.Delete("/sessions/{session_id}", sessionHandler.deleteSession)
		r.Post("/sessions/{session_id}/archive", sessionHandler.archiveSession)
		r.Get("/sessions/{session_id}/threads", sessionHandler.listThreads)
		r.Get("/sessions/{session_id}/threads/{thread_id}", sessionHandler.getThread)
		r.Post("/sessions/{session_id}/threads/{thread_id}/archive", sessionHandler.archiveThread)
		if opts.sessionEventHandler != nil {
			r.Post("/sessions/{session_id}/events", opts.sessionEventHandler.appendClientEvents)
		} else {
			r.Post("/sessions/{session_id}/events", stubHandler)
		}
		if opts.sessionEventList != nil {
			r.Get("/sessions/{session_id}/events", opts.sessionEventList.ServeSessionEvents)
			r.Get("/sessions/{session_id}/threads/{thread_id}/events", opts.sessionEventList.ServeThreadEvents)
		}

		// Models.
		r.Get("/models", listModels)
		r.Get("/models/{model_id}", retrieveModel)

		// Resources
		r.Post("/sessions/{session_id}/resources", sessionHandler.addResource)
		r.Get("/sessions/{session_id}/resources", sessionHandler.listResources)
		r.Get("/sessions/{session_id}/resources/{resource_id}", sessionHandler.getResource)
		r.Post("/sessions/{session_id}/resources/{resource_id}", sessionHandler.updateResource)
		r.Delete("/sessions/{session_id}/resources/{resource_id}", sessionHandler.deleteResource)

		// Agents
		if opts.agentHandler != nil {
			r.Post("/agents", opts.agentHandler.createAgent)
			r.Get("/agents", opts.agentHandler.listAgents)
			r.Get("/agents/{agent_id}", opts.agentHandler.getAgent)
			r.Post("/agents/{agent_id}", opts.agentHandler.updateAgent)
			r.Post("/agents/{agent_id}/archive", opts.agentHandler.archiveAgent)
			r.Get("/agents/{agent_id}/versions", opts.agentHandler.listAgentVersions)
		} else {
			r.Post("/agents", stubHandler)
			r.Get("/agents", stubHandler)
			r.Get("/agents/{agent_id}", stubHandler)
			r.Post("/agents/{agent_id}", stubHandler)
			r.Post("/agents/{agent_id}/archive", stubHandler)
			r.Get("/agents/{agent_id}/versions", stubHandler)
		}

		// Environments
		if opts.environmentHandler != nil {
			r.Post("/environments", opts.environmentHandler.createEnvironment)
			r.Get("/environments", opts.environmentHandler.listEnvironments)
			r.Get("/environments/{environment_id}", opts.environmentHandler.getEnvironment)
			r.Post("/environments/{environment_id}", opts.environmentHandler.updateEnvironment)
			r.Delete("/environments/{environment_id}", opts.environmentHandler.deleteEnvironment)
			r.Post("/environments/{environment_id}/archive", opts.environmentHandler.archiveEnvironment)
		} else {
			r.Post("/environments", stubHandler)
			r.Get("/environments", stubHandler)
			r.Get("/environments/{environment_id}", stubHandler)
			r.Post("/environments/{environment_id}", stubHandler)
			r.Delete("/environments/{environment_id}", stubHandler)
			r.Post("/environments/{environment_id}/archive", stubHandler)
		}

		// Vaults
		if opts.vaultHandler != nil {
			r.Post("/vaults", opts.vaultHandler.createVault)
			r.Get("/vaults", opts.vaultHandler.listVaults)
			r.Get("/vaults/{vault_id}", opts.vaultHandler.getVault)
			r.Post("/vaults/{vault_id}", opts.vaultHandler.updateVault)
			r.Delete("/vaults/{vault_id}", opts.vaultHandler.deleteVault)
		} else {
			r.Post("/vaults", stubHandler)
			r.Get("/vaults", stubHandler)
			r.Get("/vaults/{vault_id}", stubHandler)
			r.Post("/vaults/{vault_id}", stubHandler)
			r.Delete("/vaults/{vault_id}", stubHandler)
		}
		if opts.vaultHandler != nil {
			r.Post("/vaults/{vault_id}/archive", opts.vaultHandler.archiveVault)
		} else {
			r.Post("/vaults/{vault_id}/archive", stubHandler)
		}

		// Credentials
		if opts.vaultHandler != nil {
			r.Post("/vaults/{vault_id}/credentials", opts.vaultHandler.createCredential)
			r.Get("/vaults/{vault_id}/credentials", opts.vaultHandler.listCredentials)
			r.Get("/vaults/{vault_id}/credentials/{credential_id}", opts.vaultHandler.getCredential)
			r.Post("/vaults/{vault_id}/credentials/{credential_id}", opts.vaultHandler.updateCredential)
			r.Post("/vaults/{vault_id}/credentials/{credential_id}/mcp_oauth_validate", opts.vaultHandler.validateMCPOAuthCredential)
			r.Delete("/vaults/{vault_id}/credentials/{credential_id}", opts.vaultHandler.deleteCredential)
		} else {
			r.Post("/vaults/{vault_id}/credentials", stubHandler)
			r.Get("/vaults/{vault_id}/credentials", stubHandler)
			r.Get("/vaults/{vault_id}/credentials/{credential_id}", stubHandler)
			r.Post("/vaults/{vault_id}/credentials/{credential_id}", stubHandler)
			r.Post("/vaults/{vault_id}/credentials/{credential_id}/mcp_oauth_validate", stubHandler)
			r.Delete("/vaults/{vault_id}/credentials/{credential_id}", stubHandler)
		}
		if opts.vaultHandler != nil {
			r.Post("/vaults/{vault_id}/credentials/{credential_id}/archive", opts.vaultHandler.archiveCredential)
		} else {
			r.Post("/vaults/{vault_id}/credentials/{credential_id}/archive", stubHandler)
		}

		// Files
		if opts.fileHandler != nil {
			r.Post("/files", opts.fileHandler.createFile)
			r.Get("/files", opts.fileHandler.listFiles)
			r.Get("/files/{file_id}", opts.fileHandler.getFile)
			r.Delete("/files/{file_id}", opts.fileHandler.deleteFile)
			r.Get("/files/{file_id}/content", opts.fileHandler.getFileContent)
		} else {
			r.Post("/files", stubHandler)
			r.Get("/files", stubHandler)
			r.Get("/files/{file_id}", stubHandler)
			r.Delete("/files/{file_id}", stubHandler)
			r.Get("/files/{file_id}/content", stubHandler)
		}

		// Memory stores.
		if opts.memoryHandler != nil {
			r.Post("/memory_stores", opts.memoryHandler.createStore)
			r.Get("/memory_stores", opts.memoryHandler.listStores)
			r.Get("/memory_stores/{memory_store_id}", opts.memoryHandler.getStore)
			r.Post("/memory_stores/{memory_store_id}", opts.memoryHandler.updateStore)
			r.Delete("/memory_stores/{memory_store_id}", opts.memoryHandler.deleteStore)
			r.Post("/memory_stores/{memory_store_id}/archive", opts.memoryHandler.archiveStore)
			r.Post("/memory_stores/{memory_store_id}/memories", opts.memoryHandler.createMemory)
			r.Get("/memory_stores/{memory_store_id}/memories", opts.memoryHandler.listMemories)
			r.Get("/memory_stores/{memory_store_id}/memories/{memory_id}", opts.memoryHandler.getMemory)
			r.Post("/memory_stores/{memory_store_id}/memories/{memory_id}", opts.memoryHandler.updateMemory)
			r.Delete("/memory_stores/{memory_store_id}/memories/{memory_id}", opts.memoryHandler.deleteMemory)
			r.Get("/memory_stores/{memory_store_id}/memory_versions", opts.memoryHandler.listMemoryVersions)
			r.Get("/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}", opts.memoryHandler.getMemoryVersion)
			r.Post("/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}/redact", opts.memoryHandler.redactMemoryVersion)
		} else {
			r.Post("/memory_stores", stubHandler)
			r.Get("/memory_stores", stubHandler)
			r.Get("/memory_stores/{memory_store_id}", stubHandler)
			r.Post("/memory_stores/{memory_store_id}", stubHandler)
			r.Delete("/memory_stores/{memory_store_id}", stubHandler)
			r.Post("/memory_stores/{memory_store_id}/archive", stubHandler)
			r.Post("/memory_stores/{memory_store_id}/memories", stubHandler)
			r.Get("/memory_stores/{memory_store_id}/memories", stubHandler)
			r.Get("/memory_stores/{memory_store_id}/memories/{memory_id}", stubHandler)
			r.Post("/memory_stores/{memory_store_id}/memories/{memory_id}", stubHandler)
			r.Delete("/memory_stores/{memory_store_id}/memories/{memory_id}", stubHandler)
			r.Get("/memory_stores/{memory_store_id}/memory_versions", stubHandler)
			r.Get("/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}", stubHandler)
			r.Post("/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}/redact", stubHandler)
		}

		// Skills
		if opts.skillHandler != nil {
			r.Post("/skills", opts.skillHandler.createSkill)
			r.Get("/skills", opts.skillHandler.listSkills)
			r.Get("/skills/{skill_id}", opts.skillHandler.getSkill)
			r.Delete("/skills/{skill_id}", opts.skillHandler.deleteSkill)
			r.Post("/skills/{skill_id}/versions", opts.skillHandler.createVersion)
			r.Get("/skills/{skill_id}/versions", opts.skillHandler.listVersions)
			r.Get("/skills/{skill_id}/versions/{version}", opts.skillHandler.getVersion)
			r.Get("/skills/{skill_id}/versions/{version}/content", opts.skillHandler.getVersionContent)
			r.Delete("/skills/{skill_id}/versions/{version}", opts.skillHandler.deleteVersion)
		} else {
			r.Post("/skills", stubHandler)
			r.Get("/skills", stubHandler)
			r.Get("/skills/{skill_id}", stubHandler)
			r.Delete("/skills/{skill_id}", stubHandler)
			r.Post("/skills/{skill_id}/versions", stubHandler)
			r.Get("/skills/{skill_id}/versions", stubHandler)
			r.Get("/skills/{skill_id}/versions/{version}", stubHandler)
			r.Get("/skills/{skill_id}/versions/{version}/content", stubHandler)
			r.Delete("/skills/{skill_id}/versions/{version}", stubHandler)
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
