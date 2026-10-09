package eventstream

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const defaultStreamBatchSize = internaleventstream.MaxStreamBatchSize

type Reader interface {
	CurrentStreamPosition(context.Context, workspace.ID, string) (int64, error)
	ListSessionEventChanges(context.Context, workspace.ID, string, int64, int) ([]StreamChange, error)
	CurrentThreadStreamPosition(context.Context, workspace.ID, string, string) (int64, error)
	ListThreadEventChanges(context.Context, workspace.ID, string, string, int64, int) ([]StreamChange, error)
	ReadPreviewRequest(context.Context, workspace.ID, string, string, string, string) (PreviewRequest, error)
	ListRequestFinalMessages(context.Context, ReadScope, string, int64, int) ([]RequestFinalMessage, error)
}

type Event = internaleventstream.Event
type StreamChange = internaleventstream.StreamChange
type ReadScope = internaleventstream.ReadScope
type PreviewRequest = internaleventstream.PreviewRequest
type RequestFinalMessage = internaleventstream.RequestFinalMessage

type Option func(*options)

type options struct {
	logger                *slog.Logger
	streamBatchSize       int
	requestMetrics        httpapi.RequestMetricsRecorder
	streamConfig          StreamConfig
	previewHub            *PreviewHub
	previewMetrics        *PreviewMetrics
	streamShutdownContext context.Context
	idleChecks            *IdleCoalescer
	completedCheckLimit   int
}

// WithIdleCoalescer supplies the process's shared idle checks. Every stream
// registers its Session with them and waits on them when idle; a router
// without them fails stream requests. The caller owns their lifetime.
func WithIdleCoalescer(idleChecks *IdleCoalescer) Option {
	return func(o *options) { o.idleChecks = idleChecks }
}

// WithStreamCompletedCheckLimit is a test hook for finite streams: a stream
// returns once limit completed shared checks found its Session unchanged
// while it waited with nothing to read. Production leaves it at zero, so an
// unchanged check wakes no viewer and the connection stays open until the
// client disconnects or session.deleted is emitted.
func WithStreamCompletedCheckLimit(limit int) Option {
	return func(o *options) {
		if limit > 0 {
			o.completedCheckLimit = limit
		}
	}
}

// WithStreamShutdownContext cancels long-lived SSE work when its process begins
// shutdown, before the shared finite HTTP drain. The caller owns the lifetime.
func WithStreamShutdownContext(ctx context.Context) Option {
	return func(o *options) { o.streamShutdownContext = ctx }
}
func WithPreviewHub(hub *PreviewHub) Option { return func(o *options) { o.previewHub = hub } }
func WithPreviewMetrics(metrics *PreviewMetrics) Option {
	return func(o *options) { o.previewMetrics = metrics }
}
func WithStreamConfig(config StreamConfig) Option {
	return func(o *options) { o.streamConfig = config }
}

func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

func WithRequestMetrics(metrics httpapi.RequestMetricsRecorder) Option {
	return func(o *options) { o.requestMetrics = metrics }
}

func NewRouter(reader Reader, verifier *auth.InternalPrincipalVerifier, opts ...Option) http.Handler {
	options := newOptions(opts...)

	handler := &handler{reader: reader, options: options}
	router := chi.NewRouter()
	router.Use(httpapi.RequestIDMiddleware)
	router.Use(httpapi.PublicRecoveryMiddleware(options.logger))
	router.Use(httpapi.RequestLogMiddleware(options.logger, httpapi.DefaultSlowRequestThreshold, httpapi.WithRequestLogMetrics(options.requestMetrics)))
	router.Route("/v1", func(r chi.Router) {
		r.Use(internalPrincipalMiddleware(verifier))
		r.Method(http.MethodGet, "/sessions/{session_id}/events/stream", httpapi.DeclarePublicOperation(http.MethodGet, "/v1/sessions/{session_id}/events/stream", handler.streamSessionEvents))
		r.Method(http.MethodGet, "/sessions/{session_id}/threads/{thread_id}/stream", httpapi.DeclarePublicOperation(http.MethodGet, "/v1/sessions/{session_id}/threads/{thread_id}/stream", handler.streamThreadEvents))
	})
	return router
}

func newOptions(opts ...Option) *options {
	options := &options{
		streamBatchSize: defaultStreamBatchSize,
		streamConfig:    DefaultStreamConfig(),
		previewMetrics:  NewPreviewMetrics(),
	}
	for _, option := range opts {
		option(options)
	}
	if options.logger == nil {
		options.logger = workload.ComponentLogger("event-stream")
	}
	return options
}

type handler struct {
	reader  Reader
	options *options
}

func internalPrincipalMiddleware(verifier *auth.InternalPrincipalVerifier) func(http.Handler) http.Handler {
	if verifier != nil {
		return auth.InternalPrincipalMiddleware(verifier, httpapi.WriteError, httpapi.RequestIDFromContext)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			httpapi.WriteError(w, r, &auth.AuthenticationError{Message: "authentication unavailable"})
		})
	}
}

func (h *handler) streamSessionEvents(w http.ResponseWriter, r *http.Request) {
	types, err := parseStreamQuery(r, true)
	if err != nil {
		httpapi.WriteError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		httpapi.WriteError(w, r, err)
		return
	}
	if h.reader == nil {
		httpapi.WriteError(w, r, errors.New("eventstream: reader is required"))
		return
	}
	sessionID := chi.URLParam(r, "session_id")
	h.streamEvents(w, r, ReadScope{WorkspaceID: ws, SessionID: sessionID}, types, func(ctx context.Context) (int64, error) {
		return h.reader.CurrentStreamPosition(ctx, ws, sessionID)
	}, func(ctx context.Context, after int64) ([]StreamChange, error) {
		return h.reader.ListSessionEventChanges(ctx, ws, sessionID, after, h.options.streamBatchSize)
	})
}

func (h *handler) streamThreadEvents(w http.ResponseWriter, r *http.Request) {
	_, err := parseStreamQuery(r, false)
	if err != nil {
		httpapi.WriteError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		httpapi.WriteError(w, r, err)
		return
	}
	if h.reader == nil {
		httpapi.WriteError(w, r, errors.New("eventstream: reader is required"))
		return
	}
	sessionID := chi.URLParam(r, "session_id")
	threadID := chi.URLParam(r, "thread_id")
	h.streamEvents(w, r, ReadScope{WorkspaceID: ws, SessionID: sessionID, ThreadID: threadID}, nil, func(ctx context.Context) (int64, error) {
		return h.reader.CurrentThreadStreamPosition(ctx, ws, sessionID, threadID)
	}, func(ctx context.Context, after int64) ([]StreamChange, error) {
		return h.reader.ListThreadEventChanges(ctx, ws, sessionID, threadID, after, h.options.streamBatchSize)
	})
}

func parseStreamQuery(r *http.Request, session bool) (map[string]bool, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, &httpapi.ValidationError{Message: "invalid query string"}
	}
	for key := range values {
		if key != "beta" && (key != "event_deltas[]" || !session) {
			return nil, &httpapi.ValidationError{Message: "unknown query parameter"}
		}
	}
	raw, ok, err := singleQueryValue(values, "beta")
	if err != nil {
		return nil, err
	}
	if !ok || raw != "true" {
		return nil, &httpapi.ValidationError{Message: "beta must be true"}
	}
	types := map[string]bool{}
	for _, value := range values["event_deltas[]"] {
		if value != "agent.message" && value != "agent.thinking" {
			return nil, &httpapi.ValidationError{Message: "unsupported event delta type"}
		}
		types[value] = true
	}
	return types, nil
}

func singleQueryValue(values map[string][]string, key string) (string, bool, error) {
	raw, ok := values[key]
	if !ok {
		return "", false, nil
	}
	if len(raw) != 1 || raw[0] == "" {
		return "", false, &httpapi.ValidationError{Message: "invalid query parameter"}
	}
	return raw[0], true, nil
}

func normalizeEvent(event Event) Event {
	return internaleventstream.NormalizeEvent(event)
}

func requestWorkspace(ctx context.Context) (workspace.ID, error) {
	return workspace.MustIDFromContext(ctx)
}
