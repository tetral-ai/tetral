package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

type Event struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	SessionID   string          `json:"session_id"`
	ThreadID    string          `json:"thread_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	ProcessedAt *string         `json:"processed_at"`
}

func (event Event) MarshalJSON() ([]byte, error) {
	return eventwire.MarshalPublicEvent(event.ID, event.Type, event.Payload, event.ProcessedAt)
}

type ListOptions struct {
	Limit        int
	Page         string
	Order        string
	Types        []string
	CreatedAtGT  string
	CreatedAtGTE string
	CreatedAtLT  string
	CreatedAtLTE string
}

type ListResult struct {
	Data     []Event `json:"data"`
	NextPage *string `json:"next_page"`
}

type StreamChange struct {
	StreamPosition int64
	Event          Event
}

type ListReader interface {
	ListSessionEvents(context.Context, workspace.ID, string, ListOptions) (ListResult, error)
	ListThreadEvents(context.Context, workspace.ID, string, string, ListOptions) (ListResult, error)
}

type ListOption func(*listOptions)

type listOptions struct {
	logger         *slog.Logger
	requestMetrics httpapi.RequestMetricsRecorder
}

func WithListLogger(logger *slog.Logger) ListOption {
	return func(options *listOptions) { options.logger = logger }
}

func WithListRequestMetrics(metrics httpapi.RequestMetricsRecorder) ListOption {
	return func(options *listOptions) { options.requestMetrics = metrics }
}

func NewListRouter(reader ListReader, verifier *auth.InternalPrincipalVerifier, opts ...ListOption) http.Handler {
	options := newListOptions(opts...)
	listHandler := NewListHandler(reader, opts...)
	router := chi.NewRouter()
	router.Use(httpapi.RequestIDMiddleware)
	router.Use(httpapi.PublicRecoveryMiddleware(options.logger))
	router.Use(httpapi.RequestLogMiddleware(options.logger, httpapi.DefaultSlowRequestThreshold, httpapi.WithRequestLogMetrics(options.requestMetrics)))
	router.Route("/v1", func(router chi.Router) {
		router.Use(internalPrincipalMiddleware(verifier))
		router.Get("/sessions/{session_id}/events", listHandler.ServeSessionEvents)
		router.Get("/sessions/{session_id}/threads/{thread_id}/events", listHandler.ServeThreadEvents)
	})
	return router
}

type ListHandler struct {
	reader ListReader
}

func NewListHandler(reader ListReader, _ ...ListOption) *ListHandler {
	return &ListHandler{reader: reader}
}

func (handler *ListHandler) ServeSessionEvents(writer http.ResponseWriter, request *http.Request) {
	ws, err := requestWorkspace(request.Context())
	if err != nil {
		httpapi.WriteError(writer, request, err)
		return
	}
	if handler.reader == nil {
		httpapi.WriteError(writer, request, errors.New("eventstream: reader is required"))
		return
	}
	options, err := decodeListOptions(request, true)
	if err != nil {
		httpapi.WriteError(writer, request, err)
		return
	}
	result, err := handler.reader.ListSessionEvents(request.Context(), ws, chi.URLParam(request, "session_id"), options)
	if err != nil {
		httpapi.WriteError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, normalizeListResult(result))
}

func (handler *ListHandler) ServeThreadEvents(writer http.ResponseWriter, request *http.Request) {
	ws, err := requestWorkspace(request.Context())
	if err != nil {
		httpapi.WriteError(writer, request, err)
		return
	}
	if handler.reader == nil {
		httpapi.WriteError(writer, request, errors.New("eventstream: reader is required"))
		return
	}
	options, err := decodeListOptions(request, true)
	if err != nil {
		httpapi.WriteError(writer, request, err)
		return
	}
	result, err := handler.reader.ListThreadEvents(request.Context(), ws, chi.URLParam(request, "session_id"), chi.URLParam(request, "thread_id"), options)
	if err != nil {
		httpapi.WriteError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, normalizeListResult(result))
}

func newListOptions(opts ...ListOption) *listOptions {
	options := &listOptions{}
	for _, option := range opts {
		option(options)
	}
	if options.logger == nil {
		options.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return options
}

func internalPrincipalMiddleware(verifier *auth.InternalPrincipalVerifier) func(http.Handler) http.Handler {
	if verifier != nil {
		return auth.InternalPrincipalMiddleware(verifier, httpapi.WriteError, httpapi.RequestIDFromContext)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			httpapi.WriteError(writer, request, &auth.AuthenticationError{Message: "authentication unavailable"})
		})
	}
}

func decodeListOptions(request *http.Request, allowFilters bool) (ListOptions, error) {
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return ListOptions{}, &httpapi.ValidationError{Message: "invalid query string"}
	}
	for key := range values {
		if allowFilters && isTypesQueryKey(key) {
			continue
		}
		switch key {
		case "limit", "page", "order", "beta":
		default:
			if allowFilters && isCreatedAtQueryKey(key) {
				continue
			}
			return ListOptions{}, &httpapi.ValidationError{Message: "unknown query parameter"}
		}
	}
	if raw, ok, err := singleQueryValue(values, "beta"); err != nil {
		return ListOptions{}, err
	} else if !ok || raw != "true" {
		return ListOptions{}, &httpapi.ValidationError{Message: "beta must be true"}
	}
	limit := defaultListLimit
	if raw, ok, err := singleQueryValue(values, "limit"); err != nil {
		return ListOptions{}, err
	} else if ok {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return ListOptions{}, &httpapi.ValidationError{Message: "limit must be a positive integer"}
		}
		if parsed > maxListLimit {
			return ListOptions{}, &httpapi.ValidationError{Message: "limit exceeds the maximum allowed value"}
		}
		limit = parsed
	}
	page, _, err := singleQueryValue(values, "page")
	if err != nil {
		return ListOptions{}, err
	}
	order := "asc"
	if raw, ok, err := singleQueryValue(values, "order"); err != nil {
		return ListOptions{}, err
	} else if ok {
		if raw != "asc" && raw != "desc" {
			return ListOptions{}, &httpapi.ValidationError{Message: "order must be asc or desc"}
		}
		order = raw
	}
	var types []string
	var createdAtGT string
	var createdAtGTE string
	var createdAtLT string
	var createdAtLTE string
	if allowFilters {
		types, err = listTypesQueryValues(values)
		if err != nil {
			return ListOptions{}, err
		}
		createdAtGT, err = createdAtQueryValue(values, "created_at[gt]")
		if err != nil {
			return ListOptions{}, err
		}
		createdAtGTE, err = createdAtQueryValue(values, "created_at[gte]")
		if err != nil {
			return ListOptions{}, err
		}
		createdAtLT, err = createdAtQueryValue(values, "created_at[lt]")
		if err != nil {
			return ListOptions{}, err
		}
		createdAtLTE, err = createdAtQueryValue(values, "created_at[lte]")
		if err != nil {
			return ListOptions{}, err
		}
	}
	return ListOptions{
		Limit: limit, Page: page, Order: order, Types: types,
		CreatedAtGT: createdAtGT, CreatedAtGTE: createdAtGTE,
		CreatedAtLT: createdAtLT, CreatedAtLTE: createdAtLTE,
	}, nil
}

func isCreatedAtQueryKey(key string) bool {
	switch key {
	case "created_at[gt]", "created_at[gte]", "created_at[lt]", "created_at[lte]":
		return true
	default:
		return false
	}
}

func isTypesQueryKey(key string) bool {
	if key == "types" || key == "types[]" {
		return true
	}
	if !strings.HasPrefix(key, "types[") || !strings.HasSuffix(key, "]") {
		return false
	}
	index := strings.TrimSuffix(strings.TrimPrefix(key, "types["), "]")
	if index == "" {
		return true
	}
	_, err := strconv.Atoi(index)
	return err == nil
}

func listTypesQueryValues(values map[string][]string) ([]string, error) {
	keys := make([]string, 0)
	for key := range values {
		if isTypesQueryKey(key) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(left int, right int) bool {
		return typesQueryKeyOrder(keys[left]) < typesQueryKeyOrder(keys[right])
	})
	types := make([]string, 0)
	for _, key := range keys {
		for _, raw := range values[key] {
			for _, value := range strings.Split(raw, ",") {
				value = strings.TrimSpace(value)
				if value == "" {
					return nil, &httpapi.ValidationError{Message: "invalid query parameter"}
				}
				types = append(types, value)
			}
		}
	}
	return types, nil
}

func typesQueryKeyOrder(key string) int {
	if key == "types" {
		return -2
	}
	if key == "types[]" {
		return -1
	}
	index := strings.TrimSuffix(strings.TrimPrefix(key, "types["), "]")
	parsed, err := strconv.Atoi(index)
	if err != nil {
		return 0
	}
	return parsed
}

func createdAtQueryValue(values map[string][]string, key string) (string, error) {
	raw, ok, err := singleQueryValue(values, key)
	if err != nil || !ok {
		return "", err
	}
	return normalizeCreatedAtFilter(raw)
}

const sessionEventCreatedAtFormat = "2006-01-02T15:04:05.000000000Z"

func normalizeCreatedAtFilter(raw string) (string, error) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return "", &httpapi.ValidationError{Message: "invalid created_at filter"}
	}
	return parsed.UTC().Format(sessionEventCreatedAtFormat), nil
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

func normalizeListResult(result ListResult) ListResult {
	if result.Data == nil {
		result.Data = []Event{}
	}
	for index := range result.Data {
		result.Data[index] = NormalizeEvent(result.Data[index])
	}
	return result
}

func NormalizeEvent(event Event) Event {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	if len(event.Payload) == 0 {
		event.Payload = json.RawMessage(`{}`)
	}
	return event
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func requestWorkspace(ctx context.Context) (workspace.ID, error) {
	return workspace.MustIDFromContext(ctx)
}
