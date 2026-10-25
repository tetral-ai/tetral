package computerhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// subscriberBuffer bounds the events waiting for one stream; a subscriber
// that falls this far behind is closed, so delivery never waits on a client.
const subscriberBuffer = 64

// InstanceEvents fans the instance event notices of EventChannel out to this
// pod's GET /v1/instances/stream subscribers. Delivery is best effort: a
// subscriber receives only the events noticed while it is connected.
type InstanceEvents struct {
	database *dbconnect.Client
	logger   *slog.Logger
	// listening is closed once the first LISTEN is in place.
	listening     chan struct{}
	listeningOnce sync.Once

	mu          sync.Mutex
	closed      bool
	subscribers map[workspace.ID]map[*instanceSubscriber]struct{}
}

type instanceSubscriber struct {
	events chan instanceEvent
}

type instanceEvent struct {
	name string
	data []byte
}

// NewInstanceEvents returns the fan-out over database; Run receives the
// notices.
func NewInstanceEvents(database *dbconnect.Client, logger *slog.Logger) *InstanceEvents {
	if logger == nil {
		logger = workload.ComponentLogger(ServiceName)
	}
	return &InstanceEvents{database: database, logger: logger, listening: make(chan struct{}), subscribers: map[workspace.ID]map[*instanceSubscriber]struct{}{}}
}

// Run listens on EventChannel, reconnecting after a failure, until ctx ends;
// then it closes every subscriber.
func (e *InstanceEvents) Run(ctx context.Context) error {
	defer e.closeAll()
	listener := queue.PostgreSQLNotificationListener{Client: e.database, Operation: "computer_hub.instance_events"}
	onReady := func() { e.listeningOnce.Do(func() { close(e.listening) }) }
	return queue.RunListener(ctx, listener, instance.EventChannel, onReady, func(payload string) { e.deliver(ctx, payload) }, func(err error) {
		e.logger.WarnContext(ctx, "instance_events.listener_disconnected",
			slog.String("operation", "computer_hub.instance_events"),
			slog.String("error.class", "dependency_unavailable"),
			slog.String("error.code", "notification_listener_disconnected"),
			slog.String("error.message_safe", "instance event listener disconnected"))
	})
}

// subscribe adds a subscriber in ws; it returns nil once Run has ended.
func (e *InstanceEvents) subscribe(ws workspace.ID) (*instanceSubscriber, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, nil
	}
	subscriber := &instanceSubscriber{events: make(chan instanceEvent, subscriberBuffer)}
	if e.subscribers[ws] == nil {
		e.subscribers[ws] = map[*instanceSubscriber]struct{}{}
	}
	e.subscribers[ws][subscriber] = struct{}{}
	return subscriber, func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.dropLocked(ws, subscriber)
	}
}

// dropLocked removes subscriber and closes its channel, once.
func (e *InstanceEvents) dropLocked(ws workspace.ID, subscriber *instanceSubscriber) {
	if _, ok := e.subscribers[ws][subscriber]; !ok {
		return
	}
	delete(e.subscribers[ws], subscriber)
	if len(e.subscribers[ws]) == 0 {
		delete(e.subscribers, ws)
	}
	close(subscriber.events)
}

func (e *InstanceEvents) closeAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for ws, subscribers := range e.subscribers {
		for subscriber := range subscribers {
			e.dropLocked(ws, subscriber)
		}
	}
}

// deliver reads the instance a notice names, in the notice's Workspace, and
// queues the event for each subscriber there. A malformed notice and a notice
// for a Workspace without subscribers are skipped.
func (e *InstanceEvents) deliver(ctx context.Context, payload string) {
	var notice instance.EventNotice
	if json.Unmarshal([]byte(payload), &notice) != nil || notice.WorkspaceID == "" || (notice.Event != instance.EventCredentialReused && notice.Event != instance.EventUnused) {
		return
	}
	ws := workspace.ID(notice.WorkspaceID)
	e.mu.Lock()
	listening := len(e.subscribers[ws]) > 0
	e.mu.Unlock()
	if !listening {
		return
	}
	var object instance.InstanceObject
	err := e.database.WithWorkspaceReadOnlyTx(ctx, string(ws), "computer_hub.instance_events_read", func(tx *dbconnect.Tx) error {
		row, err := instance.ReadInstance(ctx, tx, ws, notice.InstanceID)
		if err != nil {
			return err
		}
		now, err := instance.Now(ctx, tx)
		object = instance.ObjectOf(row, now)
		return err
	})
	if err != nil {
		return
	}
	data, err := json.Marshal(object)
	if err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for subscriber := range e.subscribers[ws] {
		select {
		case subscriber.events <- instanceEvent{name: notice.Event, data: data}:
		default:
			e.dropLocked(ws, subscriber)
		}
	}
}

// streamInstances answers GET /v1/instances/stream: an SSE response whose
// events are this Workspace's instance events, until the client leaves, the
// subscriber falls behind or the pod shuts down.
func (h *instanceHandler) streamInstances(w http.ResponseWriter, r *http.Request) {
	ws := mustPrincipal(r).Workspace.ID
	subscriber, unsubscribe := h.Events.subscribe(ws)
	if subscriber == nil {
		httpapi.WriteError(w, r, &httpapi.UnavailableError{Message: "the instance stream is shutting down"})
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
