package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const sessionEventBodyByteCap = 1 << 20
const sessionEventIdempotencyKeyByteCap = 255

type sessionEventService interface {
	AppendClientEvents(context.Context, workspace.ID, string, string, sessionevent.AppendRequest) (*sessionevent.AppendResult, error)
}

type SessionEventHandler struct {
	service     sessionEventService
	bodyByteCap int64
	limits      sessionevent.Limits
}

type SessionEventHandlerOption func(*SessionEventHandler)

func WithSessionEventBodyByteCap(byteCap int64) SessionEventHandlerOption {
	return func(h *SessionEventHandler) {
		if byteCap > 0 {
			h.bodyByteCap = byteCap
		}
	}
}

func WithSessionEventLimits(limits sessionevent.Limits) SessionEventHandlerOption {
	return func(h *SessionEventHandler) {
		h.limits = limits
	}
}

func NewSessionEventHandler(service sessionEventService, options ...SessionEventHandlerOption) *SessionEventHandler {
	handler := &SessionEventHandler{
		service:     service,
		bodyByteCap: sessionEventBodyByteCap,
		limits:      sessionevent.DefaultLimits(),
	}
	for _, option := range options {
		option(handler)
	}
	return handler
}

type publicSessionEventHTTPResponse struct {
	Data []publicSessionEventHTTPEvent `json:"data"`
}

type publicSessionEventHTTPEvent struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	SessionID   string          `json:"session_id"`
	ThreadID    string          `json:"thread_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	ProcessedAt *string         `json:"processed_at"`
}

func (event publicSessionEventHTTPEvent) MarshalJSON() ([]byte, error) {
	return eventwire.MarshalPublicEvent(event.ID, event.Type, event.Payload, event.ProcessedAt)
}

func (h *SessionEventHandler) appendClientEvents(w http.ResponseWriter, r *http.Request) {
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	workspaceID, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	idempotencyKey, err := sessionEventIdempotencyKey(r.Header.Values("Idempotency-Key"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := h.decodeStrictSessionEventBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.AppendClientEvents(r.Context(), workspaceID, chi.URLParam(r, "session_id"), idempotencyKey, request)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var appendedEvents []*sessionevent.Event
	if result != nil {
		appendedEvents = result.Data
	}
	responseEvents := publicSessionEventHTTPEvents(appendedEvents)
	writeJSON(w, http.StatusOK, publicSessionEventHTTPResponse{Data: responseEvents})
}

func (h *SessionEventHandler) decodeStrictSessionEventBody(w http.ResponseWriter, r *http.Request) (sessionevent.AppendRequest, error) {
	body, err := h.readStrictSessionEventBody(w, r)
	if err != nil {
		return sessionevent.AppendRequest{}, err
	}
	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.HasPrefix(contentType, "multipart/") {
		return sessionevent.AppendRequest{}, &ValidationError{Message: "session events endpoint accepts JSON only"}
	}
	return sessionevent.DecodeAppendRequestWithLimits(body, h.limits)
}

func sessionEventIdempotencyKey(rawValues []string) (string, error) {
	if len(rawValues) == 0 {
		return id.New("idem_"), nil
	}
	if len(rawValues) > 1 {
		return "", &sessionevent.ValidationError{Message: "Idempotency-Key header must appear at most once"}
	}
	key := strings.TrimSpace(rawValues[0])
	if key == "" {
		return "", &sessionevent.ValidationError{Message: "Idempotency-Key header must not be blank"}
	}
	if len([]byte(key)) > sessionEventIdempotencyKeyByteCap {
		return "", &sessionevent.ValidationError{Message: "Idempotency-Key header is too long"}
	}
	return key, nil
}

func (h *SessionEventHandler) readStrictSessionEventBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, h.bodyByteCap)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &SessionEventRequestTooLargeError{Message: "request body too large"}
		}
		return nil, err
	}
	return body, nil
}

func publicSessionEventHTTPEvents(appendedEvents []*sessionevent.Event) []publicSessionEventHTTPEvent {
	response := make([]publicSessionEventHTTPEvent, 0, len(appendedEvents))
	for _, event := range appendedEvents {
		if event == nil {
			continue
		}
		responseEvent := publicSessionEventHTTPEvent{
			ID:        event.ID,
			Type:      event.Type,
			SessionID: event.SessionID,
			ThreadID:  event.ThreadID,
			Payload:   append(json.RawMessage(nil), event.Payload...),
		}
		if len(responseEvent.Payload) == 0 {
			responseEvent.Payload = json.RawMessage(`{}`)
		}
		if event.ProcessedAt != nil {
			value := event.ProcessedAt.UTC().Format(time.RFC3339Nano)
			responseEvent.ProcessedAt = &value
		}
		response = append(response, responseEvent)
	}
	return response
}
