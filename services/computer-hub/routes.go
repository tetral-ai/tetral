package computerhub

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload"
)

// ClientRoutePrefix holds the routes a CLI calls with its connection key.
const ClientRoutePrefix = "/v1/client/"

// ManagementRoutePrefixes are the developer and Controller routes the hub
// serves under the API envelope.
var ManagementRoutePrefixes = []string{"/v1/instance_requests", "/v1/instances"}

type routerOptions struct {
	logger    *slog.Logger
	metrics   httpapi.RequestMetricsRecorder
	instances *InstanceRoutes
	clients   *ClientRoutes
}

type RouterOption func(*routerOptions)

func WithLogger(logger *slog.Logger) RouterOption {
	return func(o *routerOptions) { o.logger = logger }
}

func WithRequestMetrics(metrics httpapi.RequestMetricsRecorder) RouterOption {
	return func(o *routerOptions) { o.metrics = metrics }
}

// WithInstanceRoutes serves the instance and instance credential routes over
// the given database client.
func WithInstanceRoutes(routes InstanceRoutes) RouterOption {
	return func(o *routerOptions) { o.instances = &routes }
}

// WithClientRoutes serves the CLI connection routes over the given database
// client and the pod's streams.
func WithClientRoutes(routes ClientRoutes) RouterOption {
	return func(o *routerOptions) { o.clients = &routes }
}

// NewRouter returns the public handler. Every route first verifies the
// signed internal principal against the actual method and path. A client
// route then accepts only a connection_key principal and answers any other
// 401 credential_invalid. A management path answers a connection_key
// principal 401 authentication_error, also where no route is registered, and
// each management route is wrapped by the shared operation gate.
func NewRouter(verifier *auth.InternalPrincipalVerifier, opts ...RouterOption) http.Handler {
	options := &routerOptions{}
	for _, option := range opts {
		option(options)
	}
	if options.logger == nil {
		options.logger = workload.ComponentLogger(ServiceName)
	}
	router := chi.NewRouter()
	router.Use(httpapi.RequestIDMiddleware)
	router.Use(httpapi.RequestLogMiddleware(options.logger, httpapi.WithRequestLogMetrics(options.metrics), httpapi.WithLongRoutes(http.MethodGet+" "+routeInstanceStream, http.MethodGet+" "+routeClientEvents)))
	router.Use(httpapi.PublicRecoveryMiddleware(options.logger))
	router.Group(func(r chi.Router) {
		r.Use(principalMiddleware(verifier, writeClientPrincipalError))
		r.Use(requireConnectionKey)
		r.Handle(ClientRoutePrefix+"*", http.HandlerFunc(writeClientNotFound))
		if options.clients != nil {
			(&clientHandler{ClientRoutes: *options.clients, logger: options.logger}).register(r)
		}
	})
	router.Group(func(r chi.Router) {
		r.Use(principalMiddleware(verifier, httpapi.WriteError))
		r.Use(rejectConnectionKey)
		for _, prefix := range ManagementRoutePrefixes {
			r.Handle(prefix, http.HandlerFunc(writeManagementNotFound))
			r.Handle(prefix+"/*", http.HandlerFunc(writeManagementNotFound))
		}
		if options.instances != nil {
			(&instanceHandler{InstanceRoutes: *options.instances, logger: options.logger}).register(r)
		}
	})
	router.NotFound(writeManagementNotFound)
	router.MethodNotAllowed(writeManagementNotFound)
	return router
}

func principalMiddleware(verifier *auth.InternalPrincipalVerifier, writer auth.ErrorWriter) func(http.Handler) http.Handler {
	if verifier == nil {
		return func(http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writer(w, r, &auth.AuthenticationError{Message: "authentication unavailable"})
			})
		}
	}
	return auth.InternalPrincipalMiddleware(verifier, writer, httpapi.RequestIDFromContext)
}

func requireConnectionKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if principal, ok := auth.PrincipalFromContext(r.Context()); !ok || principal.Credential.Kind != auth.CredentialConnectionKey {
			writeCredentialInvalid(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func rejectConnectionKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if principal, ok := auth.PrincipalFromContext(r.Context()); ok && principal.Credential.Kind == auth.CredentialConnectionKey {
			err := &auth.AuthenticationError{Message: "connection keys authenticate only client connection routes"}
			auth.RecordDecision(r.Context(), "operation", err, auth.AuditFacts(principal))
			httpapi.WriteError(w, r, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}
