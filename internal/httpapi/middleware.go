package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workload"
)

// RequestIDFromContext exposes the request_id stored on the context
// by RequestIDMiddleware. Other internal packages (notably the auth
// middleware) consume this so the audit log line includes the same
// request id loggingMiddleware emits.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// contextKey is an unexported type for context keys in this package.
type contextKey string

// requestIDKey is the context key for the request ID.
const requestIDKey contextKey = "request_id"

// RequestIDMiddleware generates a unique req_ prefixed ID for each request
// and stores it in the request context.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := id.New("req_")
		w.Header().Set("request-id", requestID)
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestMetricsRecorder observes one request by its route pattern.
type RequestMetricsRecorder interface {
	ObserveHTTPRoute(route, method string, statusCode int, duration time.Duration, long bool)
}

type requestLogOptions struct {
	metrics  RequestMetricsRecorder
	quiet    map[string]bool
	untraced map[string]bool
	long     map[string]bool
}

type RequestLogOption func(*requestLogOptions)

func WithRequestLogMetrics(metrics RequestMetricsRecorder) RequestLogOption {
	return func(opts *requestLogOptions) {
		opts.metrics = metrics
	}
}

// WithQuietRequests names requests, as "METHOD /path", that start no trace
// and observe nothing, such as a client heartbeat.
func WithQuietRequests(requests ...string) RequestLogOption {
	return func(opts *requestLogOptions) {
		for _, request := range requests {
			opts.quiet[request] = true
		}
	}
}

// WithUntracedRequests names requests, as "METHOD /path", that start no span
// but are recorded and observed, such as a stream that outlives any trace.
func WithUntracedRequests(requests ...string) RequestLogOption {
	return func(opts *requestLogOptions) {
		for _, request := range requests {
			opts.untraced[request] = true
		}
	}
}

// WithLongRoutes names route patterns, as "METHOD pattern", observed in the
// stream histogram with the long-operation buckets.
func WithLongRoutes(routes ...string) RequestLogOption {
	return func(opts *requestLogOptions) {
		for _, route := range routes {
			opts.long[route] = true
		}
	}
}

// RequestLogMiddleware is the public HTTP boundary's recording: one INFO
// http.request.completed per request, with its route pattern and never its
// path; one http_server_request_duration_seconds sample, or a stream sample
// for a long route; and a server span that starts a new trace, an incoming
// traceparent becoming only a link of it. A 5xx also writes the handled
// failure record. Health checks and /metrics are never recorded.
func RequestLogMiddleware(logger *slog.Logger, options ...RequestLogOption) func(http.Handler) http.Handler {
	opts := requestLogOptions{quiet: map[string]bool{}, untraced: map[string]bool{}, long: map[string]bool{}}
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := r.Method + " " + r.URL.Path
			if opts.quiet[request] || quietPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			tracker := &statusTrackingWriter{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			failure := &requestFailure{}
			ctx := context.WithValue(r.Context(), requestFailureKey{}, failure)
			ctx = auth.WithEdgeRequestIDRecorder(ctx)
			ctx = auth.WithAuditRecorder(ctx, authLogRecorder{logger: logger})
			method := workload.HTTPMethodLabel(r.Method)
			var span *workload.Span
			if !opts.untraced[request] {
				ctx, span = workload.StartRequestSpan(ctx, method, r.Header.Get("traceparent"))
				span.SetString("http.request.method", method)
			}
			next.ServeHTTP(tracker, r.WithContext(ctx))
			duration := time.Since(start)
			route := matchedRoute(r)
			if opts.metrics != nil {
				opts.metrics.ObserveHTTPRoute(route, r.Method, tracker.status, duration, opts.long[r.Method+" "+route])
			}
			attrs := []slog.Attr{
				slog.String("http.request.method", method),
				slog.String("http.route", route),
				slog.Int("http.response.status_code", tracker.status),
				slog.Int64("duration_ms", duration.Milliseconds()),
				slog.String("request.id", RequestIDFromContext(r.Context())),
			}
			if edgeRequestID := auth.EdgeRequestIDFromContext(ctx); edgeRequestID != "" {
				attrs = append(attrs, slog.String("edge.request.id", edgeRequestID))
			}
			if workspaceID := auth.VerifiedWorkspaceIDFromContext(ctx); workspaceID != "" {
				attrs = append(attrs, slog.String("workspace.id", workspaceID))
			}
			logger.LogAttrs(ctx, slog.LevelInfo, "http.request.completed", attrs...)
			if tracker.status >= http.StatusInternalServerError {
				fields := make([]any, len(attrs))
				for i, attr := range attrs {
					fields[i] = attr
				}
				workload.WriteFailure(ctx, logger, "http.request.failed", "server_error", workload.CauseOf(failure.cause(tracker.status)), fields...)
			}
			span.SetName(method + " " + route)
			span.SetString("http.route", route)
			span.SetInt("http.response.status_code", int64(tracker.status))
			if tracker.status >= http.StatusInternalServerError {
				span.SetFailure("http_error", "server_error")
			}
			span.End()
		})
	}
}

type requestFailureKey struct{}

// requestFailure holds, for the request's summary, the error its handler
// answered a 5xx with.
type requestFailure struct {
	mu  sync.Mutex
	err error
}

func (f *requestFailure) cause(status int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	return fmt.Errorf("the handler answered %d without naming an error", status)
}

// NoteRequestFailure gives the request's summary record the error that a
// handler is answering a 5xx with. WriteError calls it; a handler that writes
// its own 5xx calls it itself.
func NoteRequestFailure(r *http.Request, err error) {
	failure, _ := r.Context().Value(requestFailureKey{}).(*requestFailure)
	if failure == nil || err == nil {
		return
	}
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if failure.err == nil {
		failure.err = err
	}
}

// quietPath reports the health checks and /metrics, which are never
// recorded wherever a service mounts them.
func quietPath(path string) bool {
	switch path {
	case "/health", "/ready", "/metrics", "/metrics/":
		return true
	default:
		return false
	}
}

type statusTrackingWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusTrackingWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusTrackingWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

// Unwrap exposes the wrapped ResponseWriter so http.ResponseController can reach
// optional capabilities (Flush, Hijack, SetReadDeadline, SetWriteDeadline)
// through the status/size tracker. Status and size tracking are unaffected
// because those capabilities bypass Write/WriteHeader.
func (w *statusTrackingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// authMiddleware composes the raw-key test harness with the centralized
// writeError envelope and the local RequestIDFromContext extractor. Production
// /v1 routes use internalPrincipalMiddleware instead.
func authMiddleware(authenticator auth.Authenticator, logger *slog.Logger) func(http.Handler) http.Handler {
	return PublicAuthMiddleware(authenticator, logger)
}

// PublicAuthMiddleware is the raw x-api-key test harness. It authenticates the
// key and reports failures through the standard public error envelope.
func PublicAuthMiddleware(authenticator auth.Authenticator, logger *slog.Logger) func(http.Handler) http.Handler {
	return auth.MiddlewareWithAudit(authenticator, WriteError, RequestIDFromContext, authLogRecorder{logger: logger})
}

func internalPrincipalMiddleware(verifier *auth.InternalPrincipalVerifier) func(http.Handler) http.Handler {
	return auth.InternalPrincipalMiddleware(verifier, WriteError, RequestIDFromContext)
}

type authLogRecorder struct {
	logger *slog.Logger
}

func (r authLogRecorder) RecordAuthEvent(ctx context.Context, event auth.AuditEvent) {
	if event.Stage != "" {
		r.recordDecision(ctx, event)
		return
	}
	eventKind := "auth_failure"
	if event.Result == "error" {
		eventKind = "auth_error"
	}
	attrs := []any{
		slog.String("operation", "http.auth"),
		slog.String("event.kind", eventKind),
		slog.String("component", "http-api"),
		slog.String("auth.result", event.Result),
		slog.String("request.id", event.RequestID),
		slog.String("http.request.method", event.Method),
	}
	code := "authentication_error"
	if event.ErrorType != "" {
		code = "authentication_backend_error"
	}
	workload.WriteFailure(ctx, r.logger, "http.auth", code, workload.CauseOf(event.Failure()), attrs...)
}

// recoveryMiddleware catches panics and returns a standard error response via writeError.
func recoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return PublicRecoveryMiddleware(logger)
}

// PublicRecoveryMiddleware catches panics and returns the standard public error envelope.
func PublicRecoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					requestID := RequestIDFromContext(r.Context())
					workload.WriteFailure(r.Context(), logger, "http.panic", "internal_error", workload.CauseOf(fmt.Errorf("panic: %v", recovered)),
						slog.String("operation", "http.request"),
						slog.String("event.kind", "panic"),
						slog.String("component", "http-api"),
						slog.String("request.id", requestID),
					)
					NoteRequestFailure(r, errors.New("the handler panicked"))
					WriteError(w, r, errors.New("internal error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// appendEdgeRequestID adds the verified edge request ID under edge.request.id.
// request.id remains this service's own request ID, which is also the public
// request-id response header; the edge ID joins this record to the Auth Check
// record of the same request.
func appendEdgeRequestID(ctx context.Context, attrs []any) []any {
	if edgeRequestID := auth.EdgeRequestIDFromContext(ctx); edgeRequestID != "" {
		attrs = append(attrs, slog.String("edge.request.id", edgeRequestID))
	}
	return attrs
}

// recordDecision accepts only the fixed Auth builder vocabulary and emits no
// path, workspace selector, subject, identity ID, issuer URL, or credential.
func (r authLogRecorder) recordDecision(ctx context.Context, event auth.AuditEvent) {
	if r.logger == nil {
		return
	}
	attrs := []any{slog.String("operation", "auth."+event.Stage), slog.String("component", "auth"), slog.String("auth.stage", event.Stage), slog.String("auth.result", event.Result), slog.String("request.id", RequestIDFromContext(ctx))}
	attrs = appendEdgeRequestID(ctx, attrs)
	if event.Operation != "" {
		attrs = append(attrs, slog.String("auth.operation", string(event.Operation)))
	}
	if event.IdentityKind != "" {
		attrs = append(attrs, slog.String("auth.identity.kind", event.IdentityKind))
	}
	for key, value := range map[string]int64{"auth.rule.revision": event.RuleRevision, "auth.identity.revision": event.IdentityRevision, "auth.grant.revision": event.GrantRevision} {
		if value > 0 {
			attrs = append(attrs, slog.Int64(key, value))
		}
	}
	if event.Result == "success" {
		if event.Stage == "grant" {
			r.logger.Info("auth.exchange.success", attrs...)
		} else {
			r.logger.Debug("auth."+event.Stage+".success", attrs...)
		}
		return
	}
	workload.WriteFailure(ctx, r.logger, "auth."+event.Stage+"."+event.Result, event.Code, workload.CauseOf(event.Failure()), attrs...)
}

// matchedRoute is the route pattern that served r, or "unmatched". A request
// that enters a mounted router and matches nothing there still carries the
// mount's pattern, so the root router is asked whether any route matches.
func matchedRoute(r *http.Request) string {
	routeContext := chi.RouteContext(r.Context())
	if routeContext == nil || routeContext.RoutePattern() == "" {
		return "unmatched"
	}
	path := r.URL.RawPath
	if path == "" {
		path = r.URL.Path
	}
	if routeContext.Routes != nil && routeContext.Routes.Find(chi.NewRouteContext(), r.Method, path) == "" {
		return "unmatched"
	}
	return routeContext.RoutePattern()
}
