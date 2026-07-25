package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	defaultStreamBatchSize = 100
	defaultStreamPollEvery = time.Second
)

type Reader interface {
	CurrentStreamPosition(context.Context, workspace.ID, string) (int64, error)
	ListSessionEventChanges(context.Context, workspace.ID, string, int64, int) ([]StreamChange, error)
	CurrentThreadStreamPosition(context.Context, workspace.ID, string, string) (int64, error)
	ListThreadEventChanges(context.Context, workspace.ID, string, string, int64, int) ([]StreamChange, error)
}

type Event = internaleventstream.Event
type StreamChange = internaleventstream.StreamChange

type Option func(*options)

type options struct {
	logger              *slog.Logger
	streamPollInterval  time.Duration
	streamBatchSize     int
	streamMaxEmptyPolls int
	requestMetrics      httpapi.RequestMetricsRecorder
}

func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

func WithRequestMetrics(metrics httpapi.RequestMetricsRecorder) Option {
	return func(o *options) { o.requestMetrics = metrics }
}

func WithStreamPollInterval(interval time.Duration) Option {
	return func(o *options) {
		if interval > 0 {
			o.streamPollInterval = interval
		}
	}
}

// WithStreamMaxEmptyPolls is a test hook. Production streams leave this at zero
// so the connection stays open until the client disconnects or session.deleted
// is emitted.
func WithStreamMaxEmptyPolls(max int) Option {
	return func(o *options) {
		if max > 0 {
			o.streamMaxEmptyPolls = max
		}
	}
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
		r.Get("/sessions/{session_id}/events/stream", handler.streamSessionEvents)
		r.Get("/sessions/{session_id}/threads/{thread_id}/stream", handler.streamThreadEvents)
	})
	return router
}

func newOptions(opts ...Option) *options {
	options := &options{
		streamPollInterval: defaultStreamPollEvery,
		streamBatchSize:    defaultStreamBatchSize,
	}
	for _, option := range opts {
		option(options)
	}
	if options.logger == nil {
		options.logger = defaultLogger(os.Stderr)
	}
	return options
}

func defaultLogger(writer io.Writer) *slog.Logger {
	return workload.NewLogger(writer, "event-stream", "", "")
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
	if err := requireBetaStreamQuery(r); err != nil {
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
	h.streamEvents(w, r, func() (int64, error) {
		return h.reader.CurrentStreamPosition(r.Context(), ws, sessionID)
	}, func(after int64) ([]StreamChange, error) {
		return h.reader.ListSessionEventChanges(r.Context(), ws, sessionID, after, h.options.streamBatchSize)
	})
}

func (h *handler) streamThreadEvents(w http.ResponseWriter, r *http.Request) {
	if err := requireBetaStreamQuery(r); err != nil {
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
	h.streamEvents(w, r, func() (int64, error) {
		return h.reader.CurrentThreadStreamPosition(r.Context(), ws, sessionID, threadID)
	}, func(after int64) ([]StreamChange, error) {
		return h.reader.ListThreadEventChanges(r.Context(), ws, sessionID, threadID, after, h.options.streamBatchSize)
	})
}

func (h *handler) streamEvents(w http.ResponseWriter, r *http.Request, currentPosition func() (int64, error), listChanges func(int64) ([]StreamChange, error)) {
	cursor, err := currentPosition()
	if err != nil {
		httpapi.WriteError(w, r, err)
		return
	}

	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	_ = controller.Flush()

	emptyPolls := 0
	for {
		changes, err := listChanges(cursor)
		if err != nil {
			return
		}
		if len(changes) == 0 {
			emptyPolls++
			if h.options.streamMaxEmptyPolls > 0 && emptyPolls >= h.options.streamMaxEmptyPolls {
				return
			}
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			_ = controller.Flush()
			if !sleepOrDone(r.Context(), h.options.streamPollInterval) {
				return
			}
			continue
		}
		emptyPolls = 0
		for _, change := range changes {
			cursor = change.StreamPosition
			event := normalizeEvent(change.Event)
			data, err := json.Marshal(event)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			_ = controller.Flush()
			if event.Type == "session.deleted" {
				return
			}
		}
	}
}

func requireBetaStreamQuery(r *http.Request) error {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &httpapi.ValidationError{Message: "invalid query string"}
	}
	for key := range values {
		if key != "beta" {
			return &httpapi.ValidationError{Message: "unknown query parameter"}
		}
	}
	raw, ok, err := singleQueryValue(values, "beta")
	if err != nil {
		return err
	}
	if !ok || raw != "true" {
		return &httpapi.ValidationError{Message: "beta must be true"}
	}
	return nil
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

func sleepOrDone(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func requestWorkspace(ctx context.Context) (workspace.ID, error) {
	return workspace.MustIDFromContext(ctx)
}
