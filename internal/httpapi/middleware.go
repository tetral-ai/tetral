package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/id"
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

// DefaultSlowRequestThreshold is the production boundary-log slow-request cutoff.
const DefaultSlowRequestThreshold = 2 * time.Second

type RequestMetricsRecorder interface {
	ObserveHTTPRequest(method string, statusCode int, duration time.Duration)
}

type requestLogOptions struct {
	metrics RequestMetricsRecorder
}

type RequestLogOption func(*requestLogOptions)

func WithRequestLogMetrics(metrics RequestMetricsRecorder) RequestLogOption {
	return func(opts *requestLogOptions) {
		opts.metrics = metrics
	}
}

// RequestLogMiddleware is the exported boundary request-logging constructor.
// It logs only the events that matter in production: server errors (>= 500) and
// slow requests (>= slowThreshold). Ordinary fast 2xx/3xx traffic stays quiet.
// Workloads that need the same boundary policy (e.g. event-stream) install this
// directly instead of reimplementing it; a high slowThreshold suppresses
// slow-request logging entirely.
func RequestLogMiddleware(logger *slog.Logger, slowThreshold time.Duration, options ...RequestLogOption) func(http.Handler) http.Handler {
	opts := requestLogOptions{}
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tracker := &statusTrackingWriter{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			ctx := auth.WithEdgeRequestIDRecorder(r.Context())
			ctx = auth.WithAuditRecorder(ctx, authLogRecorder{logger: logger})
			next.ServeHTTP(tracker, r.WithContext(ctx))
			duration := time.Since(start)
			if opts.metrics != nil {
				opts.metrics.ObserveHTTPRequest(r.Method, tracker.status, duration)
			}
			if tracker.status < http.StatusInternalServerError && duration < slowThreshold {
				return
			}
			kind := "server_error"
			if tracker.status < http.StatusInternalServerError {
				kind = "slow_request"
			}
			attrs := []any{
				slog.String("operation", "http.request"),
				slog.String("event.kind", kind),
				slog.String("component", "http-api"),
				slog.String("request.id", RequestIDFromContext(r.Context())),
				slog.String("http.request.method", r.Method),
				slog.String("url.path", safeRequestPath(r)),
				slog.Int("http.response.status_code", tracker.status),
				slog.Int64("duration.ms", duration.Milliseconds()),
			}
			attrs = appendEdgeRequestID(ctx, attrs)
			if tracker.status >= http.StatusInternalServerError {
				attrs = append(attrs,
					slog.String("error.class", "http_error"),
					slog.String("error.code", "server_error"),
					slog.String("error.message_safe", "HTTP request failed"),
				)
			}
			if tracker.status >= http.StatusInternalServerError {
				logger.Error("http.request", attrs...)
			} else {
				logger.Info("http.request", attrs...)
			}
		})
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
		slog.String("url.path", event.Path),
	}
	if event.ErrorType != "" {
		attrs = append(attrs,
			slog.String("error.class", "auth_error"),
			slog.String("error.code", event.ErrorType),
			slog.String("error.message_safe", "authentication backend error"),
		)
	} else {
		attrs = append(attrs,
			slog.String("error.class", "authentication_error"),
			slog.String("error.code", "authentication_error"),
			slog.String("error.message_safe", "authentication failed"),
		)
	}
	r.logger.Warn("http.auth", attrs...)
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
					logger.Error("http.panic",
						slog.String("operation", "http.request"),
						slog.String("event.kind", "panic"),
						slog.String("component", "http-api"),
						slog.String("request.id", requestID),
						slog.String("url.path", safeRequestPath(r)),
						slog.String("error.class", "panic"),
						slog.String("error.code", "internal_error"),
						slog.String("error.message_safe", "internal error"),
					)
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

func safeRequestPath(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, "/v1/api_keys/") {
		return "/v1/api_keys/{api_key_id}"
	}
	return r.URL.Path
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
	class := "authentication_error"
	if event.Result == "denied" {
		class = "permission_error"
	}
	if event.Result == "limited" {
		class = "rate_limit_error"
	}
	if event.Result == "unavailable" {
		class = "dependency_unavailable"
	}
	attrs = append(attrs, slog.String("error.class", class), slog.String("error.code", event.Code), slog.String("error.message_safe", "authentication decision failed"))
	r.logger.Warn("auth."+event.Stage+"."+event.Result, attrs...)
}
