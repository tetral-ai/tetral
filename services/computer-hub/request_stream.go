package computerhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// requestScope is what one request stream subscribes to: a Workspace's
// requests of one Environment.
type requestScope struct {
	workspace   workspace.ID
	environment string
}

// RequestEvents fans the request notices of computer.RequestEventChannel out
// to this pod's GET /v1/instance_requests/stream subscribers. Delivery is best
// effort: a subscriber receives only the events noticed while it is
// connected.
type RequestEvents struct {
	database *dbconnect.Client
	logger   *slog.Logger
	// listening is closed once the first LISTEN is in place.
	listening     chan struct{}
	listeningOnce sync.Once

	mu          sync.Mutex
	closed      bool
	subscribers map[requestScope]map[*instanceSubscriber]struct{}
}

// NewRequestEvents returns the fan-out over database; Run receives the
// notices.
func NewRequestEvents(database *dbconnect.Client, logger *slog.Logger) *RequestEvents {
	if logger == nil {
		logger = workload.ComponentLogger(ServiceName)
	}
	return &RequestEvents{database: database, logger: logger, listening: make(chan struct{}), subscribers: map[requestScope]map[*instanceSubscriber]struct{}{}}
}

// Run listens on computer.RequestEventChannel, reconnecting after a failure,
// until ctx ends; then it closes every subscriber.
func (e *RequestEvents) Run(ctx context.Context) error {
	defer e.closeAll()
	listener := queue.PostgreSQLNotificationListener{Client: e.database, Operation: "computer_hub.request_events"}
	onReady := func() { e.listeningOnce.Do(func() { close(e.listening) }) }
	return queue.RunListener(ctx, listener, computer.RequestEventChannel, onReady, func(payload string) { e.deliver(ctx, payload) }, func(err error) {
		e.logger.WarnContext(ctx, "request_events.listener_disconnected",
			slog.String("operation", "computer_hub.request_events"),
			slog.String("error.class", "dependency_unavailable"),
			slog.String("error.code", "notification_listener_disconnected"),
			slog.String("error.message_safe", "request event listener disconnected"))
	})
}

// subscribe adds a subscriber to scope; it returns nil once Run has ended.
func (e *RequestEvents) subscribe(scope requestScope) (*instanceSubscriber, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, nil
	}
	subscriber := &instanceSubscriber{events: make(chan instanceEvent, subscriberBuffer)}
	if e.subscribers[scope] == nil {
		e.subscribers[scope] = map[*instanceSubscriber]struct{}{}
	}
	e.subscribers[scope][subscriber] = struct{}{}
	return subscriber, func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.dropLocked(scope, subscriber)
	}
}

// dropLocked removes subscriber and closes its channel, once.
func (e *RequestEvents) dropLocked(scope requestScope, subscriber *instanceSubscriber) {
	if _, ok := e.subscribers[scope][subscriber]; !ok {
		return
	}
	delete(e.subscribers[scope], subscriber)
	if len(e.subscribers[scope]) == 0 {
		delete(e.subscribers, scope)
	}
	close(subscriber.events)
}

func (e *RequestEvents) closeAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for scope, subscribers := range e.subscribers {
		for subscriber := range subscribers {
			e.dropLocked(scope, subscriber)
		}
	}
}

// listened reports whether any subscriber of ws is connected.
func (e *RequestEvents) listened(ws workspace.ID) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for scope := range e.subscribers {
		if scope.workspace == ws {
			return true
		}
	}
	return false
}

// deliver reads the request a notice names, in the notice's Workspace, and
// queues the event for each subscriber of that Workspace and the request's
// Environment. A malformed notice and a notice for a Workspace without
// subscribers are skipped.
func (e *RequestEvents) deliver(ctx context.Context, payload string) {
	var notice computer.RequestNotice
	if json.Unmarshal([]byte(payload), &notice) != nil || notice.WorkspaceID == "" {
		return
	}
	switch notice.Event {
	case computer.RequestEventCreated, computer.RequestEventClaimed, computer.RequestEventPending, computer.RequestEventExpired:
	default:
		return
	}
	ws := workspace.ID(notice.WorkspaceID)
	if !e.listened(ws) {
		return
	}
	var request computer.Request
	err := e.database.WithWorkspaceReadOnlyTx(ctx, string(ws), "computer_hub.request_events_read", func(tx *dbconnect.Tx) error {
		var err error
		request, err = computer.ReadRequest(ctx, tx, ws, notice.RequestID)
		return err
	})
	if err != nil {
		return
	}
	data, err := json.Marshal(request.Object())
	if err != nil {
		return
	}
	scope := requestScope{workspace: ws, environment: request.EnvironmentID}
	e.mu.Lock()
	defer e.mu.Unlock()
	for subscriber := range e.subscribers[scope] {
		select {
		case subscriber.events <- instanceEvent{name: notice.Event, data: data}:
		default:
			e.dropLocked(scope, subscriber)
		}
	}
}

// streamRequests answers GET /v1/instance_requests/stream: an SSE response
// whose events are the Workspace's request events of the Environment
// environment_id names, until the client leaves, the subscriber falls behind
// or the pod shuts down.
func (h *instanceHandler) streamRequests(w http.ResponseWriter, r *http.Request) {
	environmentID, err := requestEnvironment(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	subscriber, unsubscribe := h.Requests.subscribe(requestScope{workspace: mustPrincipal(r).Workspace.ID, environment: environmentID})
	if subscriber == nil {
		httpapi.WriteError(w, r, &httpapi.UnavailableError{Message: "the request stream is shutting down"})
		return
	}
	defer unsubscribe()
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-subscriber.events:
			if !ok {
				return
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.name, event.data); err != nil {
				return
			}
			if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return
			}
		}
	}
}
