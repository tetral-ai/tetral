package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/httpapi"
)

type previewEventState struct {
	eventType       string
	next            int64
	stopped, closed bool
}
type previewRequestState struct {
	startPosition     int64
	startID, threadID string
	observed, stopped bool
	lossLogged        bool
	counted           bool
	events            map[string]*previewEventState
}
type streamPreviewState struct {
	requests  map[string]*previewRequestState
	watermark int64
	epoch     uint64
	types     map[string]bool
}

func (h *handler) streamEvents(w http.ResponseWriter, r *http.Request, scope ReadScope, types map[string]bool, currentPosition func(context.Context) (int64, error), listChanges func(context.Context, int64) ([]StreamChange, error)) {
	if lifetime := h.options.streamShutdownContext; lifetime != nil {
		ctx, cancel := context.WithCancel(r.Context())
		joined := make(chan struct{})
		stop := context.AfterFunc(lifetime, func() { defer close(joined); cancel() })
		defer func() {
			if !stop() {
				<-joined
			}
			cancel()
		}()
		if lifetime.Err() != nil {
			cancel()
		}
		r = r.WithContext(ctx)
	}

	var viewer *PreviewViewer
	if len(types) > 0 && scope.ThreadID == "" && h.options.previewHub != nil {
		// Authorize before installing a broker subscription. The second high-water
		// read below is the opening mark, after the bounded subscription flush.
		if _, err := currentPosition(r.Context()); err != nil {
			httpapi.WriteError(w, r, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.options.streamConfig.PreviewSetupTimeout)
		var err error
		viewer, err = h.options.previewHub.Join(ctx, scope.WorkspaceID, scope.SessionID)
		cancel()
		if viewer != nil {
			defer viewer.Close()
		}
		if err != nil {
			h.logPreviewStop(scope, "", "setup_unavailable")
		}
	}
	cursor, err := currentPosition(r.Context())
	if err != nil {
		httpapi.WriteError(w, r, err)
		return
	}
	state := streamPreviewState{requests: map[string]*previewRequestState{}, watermark: cursor, types: types}
	defer h.releasePreviewRequests(&state)
	if viewer != nil {
		state.epoch, _ = viewer.Epoch()
	}
	controller := http.NewResponseController(w)
	writer := newSSEWriter(r.Context(), w, controller, h.options.streamConfig.WriteTimeout, h.options.previewMetrics)
	defer writer.close()
	if viewer != nil {
		writer.reservePreview = viewer.reserveEncoding
		writer.previewReceivedAt = viewer.receivedAt
	}
	h.options.previewMetrics.activeStreams.Add(1)
	defer h.options.previewMetrics.activeStreams.Add(-1)
	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := writer.flush(); err != nil {
		return
	}
	nextHeartbeat := time.Now()
	emptyPolls := 0
	for {
		if r.Context().Err() != nil {
			return
		}
		// Formal lifecycle/finals have priority on every bounded preview slice.
		selectedAt := time.Now()
		changes, err := listChanges(r.Context(), cursor)
		if err != nil {
			return
		}
		if len(changes) == 0 {
			emptyPolls++
			if h.options.streamMaxEmptyPolls > 0 && emptyPolls >= h.options.streamMaxEmptyPolls {
				return
			}
		} else {
			emptyPolls = 0
		}
		for len(changes) > 0 {
			// Zero each consumed element so the reader-returned batch no longer
			// references its payload once the row is handled.
			change := changes[0]
			changes[0] = StreamChange{}
			changes = changes[1:]
			if change.DeferredMessage {
				cursor = change.StreamPosition
				continue
			}
			if change.Event.Type == "span.model_request_end" && change.ModelRequestID != "" {
				// Drop all original payloads, including the unconsumed suffix, before the
				// first complete text read. Its cursor is still unacknowledged and queried
				// again after the End group. Never retain two generated text bodies.
				clear(changes)
				changes = nil
				request := state.requests[change.ModelRequestID]
				if request != nil {
					request.stopped = true
				}
				if err := h.writeEndGroup(r.Context(), writer, scope, change, &state); err != nil {
					return
				}
				cursor = change.StreamPosition
				break
			}
			data, err := json.Marshal(normalizeEvent(change.Event))
			if err != nil {
				return
			}
			if err := writer.event(change.Event.Type, data); err != nil {
				return
			}
			h.recordFormalLatency(selectedAt)
			h.options.previewMetrics.formalEvents.Add(1)
			h.closeFormalPreview(&state, change)
			cursor = change.StreamPosition
			if change.Event.Type == "session.deleted" {
				return
			}
		}
		if viewer != nil {
			epoch, reason := viewer.Epoch()
			if epoch != state.epoch {
				// Capture durable loss high-water once per subscription/viewer loss. New
				// requests after it can preview; already-started requests cannot replay.
				position, err := currentPosition(r.Context())
				var unreadable *httpapi.NotFoundError
				if errors.As(err, &unreadable) {
					// The session became unreadable after this feed opened. Stop
					// previews and release the subscription; formal delivery continues
					// until session.deleted is emitted or the feed's deletion gate
					// closes the stream.
					h.releasePreviewRequests(&state)
					viewer.Close()
					viewer = nil
					h.options.previewMetrics.stoppedRequests.Add(1)
					h.logPreviewStop(scope, "", "session_unreadable")
					continue
				}
				if err != nil {
					return
				}
				if position > state.watermark {
					state.watermark = position
				}
				for _, request := range state.requests {
					request.stopped = true
				}
				state.epoch = epoch
				h.options.previewMetrics.stoppedRequests.Add(1)
				h.logPreviewStop(scope, "", reason)
			}
			needPoll := false
			for i := 0; i < 64; i++ {
				frame, epoch, _, ok := viewer.Take()
				if !ok {
					break
				}
				if epoch != state.epoch {
					viewer.Release()
					needPoll = true
					break
				}
				if err := h.previewFrame(r.Context(), writer, scope, cursor, &state, frame); err != nil {
					viewer.Release()
					return
				}
				viewer.Release()
				if request := state.requests[frame.ModelRequestID]; request != nil && request.startPosition > cursor {
					needPoll = true
					break
				}
			}
			if needPoll {
				continue
			}
		}
		now := time.Now()
		if !now.Before(nextHeartbeat) {
			if err := writer.heartbeat(); err != nil {
				return
			}
			nextHeartbeat = time.Now().Add(h.options.streamConfig.HeartbeatInterval)
		}
		nextPoll := now.Add(h.options.streamConfig.PollInterval)
		delay := time.Until(nextPoll)
		if heartbeatDelay := time.Until(nextHeartbeat); heartbeatDelay < delay {
			delay = heartbeatDelay
		}
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		var wake <-chan struct{}
		if viewer != nil {
			wake = viewer.Wake()
		}
		select {
		case <-r.Context().Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-wake:
			timer.Stop()
		}
	}
}

func (h *handler) writeEndGroup(ctx context.Context, writer *sseWriter, scope ReadScope, end StreamChange, state *streamPreviewState) error {
	endSelectedAt := time.Now()
	after := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		selectedAt := time.Now()
		page, err := h.reader.ListRequestFinalMessages(ctx, scope, end.Event.ID, after, 1)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		if len(page) != 1 || page[0].Sequence <= after {
			return errors.New("invalid request final page")
		}
		message := page[0]
		// Defensive: drop the page's reference so only the current message holds
		// the body during the write. The production reader does not retain pages,
		// so this is not itself a residency guarantee.
		page[0] = RequestFinalMessage{}
		data, err := json.Marshal(normalizeEvent(message.Event))
		if err != nil {
			return err
		}
		if err := writer.event(message.Event.Type, data); err != nil {
			return err
		}
		h.recordFormalLatency(selectedAt)
		h.options.previewMetrics.formalEvents.Add(1)
		h.closeFormalPreview(state, StreamChange{Event: message.Event, ModelRequestID: end.ModelRequestID, RequestStartEventID: end.RequestStartEventID, RequestStartStreamPosition: end.RequestStartStreamPosition, ThreadRole: end.ThreadRole, RequestKind: end.RequestKind})
		after = message.Sequence
	}
	data, err := json.Marshal(normalizeEvent(end.Event))
	if err != nil {
		return err
	}
	if err := writer.event(end.Event.Type, data); err != nil {
		return err
	}
	h.recordFormalLatency(endSelectedAt)
	h.options.previewMetrics.formalEvents.Add(1)
	h.closeFormalPreview(state, end)
	return nil
}

func (h *handler) closeFormalPreview(state *streamPreviewState, change StreamChange) {
	if len(state.types) == 0 {
		return
	}
	if change.ModelRequestID == "" || change.ThreadRole != "main" || change.RequestKind != "agent_provider_request" {
		return
	}
	if change.Event.Type == "span.model_request_end" {
		if change.RequestStartStreamPosition > state.watermark {
			state.watermark = change.RequestStartStreamPosition
		}
		if request := state.requests[change.ModelRequestID]; request != nil && request.counted {
			h.options.previewMetrics.activeRequests.Add(-1)
			request.counted = false
		}
		delete(state.requests, change.ModelRequestID)
		return
	}
	request := state.requests[change.ModelRequestID]
	if request == nil {
		if change.RequestStartStreamPosition <= state.watermark || len(state.requests) >= h.options.streamConfig.ActiveRequests {
			return
		}
		request = &previewRequestState{startPosition: change.RequestStartStreamPosition, startID: change.RequestStartEventID, threadID: change.Event.ThreadID, events: map[string]*previewEventState{}}
		h.trackPreviewRequest(state, change.ModelRequestID, request)
	}
	if change.Event.Type != "agent.thinking" && change.Event.Type != "agent.message" {
		return
	}
	event := request.events[change.Event.ID]
	if event == nil && len(request.events) >= eventwire.MaxPreviewEventIdentities {
		request.stopped = true
		return
	}
	if event == nil {
		event = &previewEventState{}
		request.events[change.Event.ID] = event
	}
	event.closed = true
}

func (h *handler) previewFrame(ctx context.Context, writer *sseWriter, scope ReadScope, cursor int64, state *streamPreviewState, frame eventwire.PreviewFrame) error {
	request := state.requests[frame.ModelRequestID]
	if frame.Kind == "request_open" {
		if request != nil && (request.observed || request.stopped) {
			return nil
		}
		descriptor, err := h.reader.ReadPreviewRequest(ctx, scope.WorkspaceID, scope.SessionID, frame.ThreadID, frame.ModelRequestID, frame.ModelRequestStartEventID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		if descriptor.Ended || !descriptor.IsPrimaryThread || descriptor.ThreadRole != "main" || descriptor.ThreadVisibility != "public" || descriptor.RequestKind != "agent_provider_request" || descriptor.StartStreamPosition <= state.watermark {
			return nil
		}
		if request == nil {
			if len(state.requests) >= h.options.streamConfig.ActiveRequests {
				state.watermark = descriptor.StartStreamPosition
				h.options.previewMetrics.stoppedRequests.Add(1)
				h.logPreviewStop(scope, frame.ModelRequestID, "request_capacity")
				return nil
			}
			request = &previewRequestState{events: map[string]*previewEventState{}}
			h.trackPreviewRequest(state, frame.ModelRequestID, request)
		}
		request.observed = true
		request.startPosition = descriptor.StartStreamPosition
		request.startID = frame.ModelRequestStartEventID
		request.threadID = frame.ThreadID
		return nil
	}
	if request == nil || !request.observed || request.stopped || request.startPosition <= state.watermark || frame.ModelRequestStartEventID != request.startID || frame.ThreadID != request.threadID || !state.types[frame.EventType] {
		return nil
	}
	// The ordinary formal query always runs first, so a queued start waits until
	// its acknowledged Start is visible on this connection's durable cursor.
	if cursor < request.startPosition {
		return nil
	}
	event := request.events[frame.EventID]
	if frame.Kind == "event_start" {
		if event != nil {
			if !event.closed {
				event.stopped = true
				h.sequenceStop(scope, frame.ModelRequestID, request)
			}
			return nil
		}
		if len(request.events) >= eventwire.MaxPreviewEventIdentities {
			request.stopped = true
			h.options.previewMetrics.stoppedRequests.Add(1)
			h.logPreviewStop(scope, frame.ModelRequestID, "event_capacity")
			return nil
		}
		event = &previewEventState{eventType: frame.EventType, next: 1}
		request.events[frame.EventID] = event
	} else {
		if event == nil || event.closed || event.stopped {
			return nil
		}
		if frame.EventType != event.eventType || frame.PreviewSequence == nil || *frame.PreviewSequence != event.next {
			event.stopped = true
			h.sequenceStop(scope, frame.ModelRequestID, request)
			return nil
		}
		event.next++
	}
	startedAt := time.Now()
	if writer.previewReceivedAt != nil {
		if receivedAt := writer.previewReceivedAt(); !receivedAt.IsZero() {
			startedAt = receivedAt
		}
	}
	size, err := eventwire.PublicPreviewEncodedBytes(frame)
	if err != nil {
		return err
	}
	if writer.reservePreview != nil && !writer.reservePreview(size) {
		request.stopped = true
		return nil
	}
	data, err := eventwire.MarshalPreviewEvent(frame)
	if err != nil {
		return err
	}
	if err := writer.event(frame.Kind, data); err != nil {
		return err
	}
	h.options.previewMetrics.previewLatency.observe(time.Since(startedAt))
	h.options.previewMetrics.previewEvents.Add(1)
	return nil
}
func (h *handler) recordFormalLatency(selectedAt time.Time) {
	h.options.previewMetrics.formalLatency.observe(time.Since(selectedAt))
}
func (h *handler) trackPreviewRequest(state *streamPreviewState, id string, request *previewRequestState) {
	state.requests[id] = request
	request.counted = true
	h.options.previewMetrics.activeRequests.Add(1)
}
func (h *handler) releasePreviewRequests(state *streamPreviewState) {
	for _, request := range state.requests {
		if request.counted {
			h.options.previewMetrics.activeRequests.Add(-1)
			request.counted = false
		}
	}
	clear(state.requests)
}
func (h *handler) sequenceStop(scope ReadScope, requestID string, request *previewRequestState) {
	h.options.previewMetrics.sequenceStops.Add(1)
	if !request.lossLogged {
		request.lossLogged = true
		h.logPreviewStop(scope, requestID, "sequence_gap")
	}
}
func (h *handler) logPreviewStop(scope ReadScope, requestID, reason string) {
	h.options.logger.Warn("preview_stopped", "operation", "event_stream.preview", "workspace.id", string(scope.WorkspaceID), "session.id", scope.SessionID, "model_request.id", requestID, "reason", reason, "outcome", "formal_active")
}

type sseWriter struct {
	ctx               context.Context
	writer            http.ResponseWriter
	controller        *http.ResponseController
	timeout           time.Duration
	metrics           *PreviewMetrics
	reservePreview    func(int) bool
	previewReceivedAt func() time.Time
	mu                sync.Mutex
	stop              chan struct{}
	done              chan struct{}
}

func newSSEWriter(ctx context.Context, w http.ResponseWriter, controller *http.ResponseController, timeout time.Duration, metrics *PreviewMetrics) *sseWriter {
	s := &sseWriter{ctx: ctx, writer: w, controller: controller, timeout: timeout, metrics: metrics, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		select {
		case <-ctx.Done():
			s.mu.Lock()
			_ = controller.SetWriteDeadline(time.Now())
			s.mu.Unlock()
		case <-s.stop:
		}
	}()
	return s
}
func (s *sseWriter) close() { close(s.stop); <-s.done }
func (s *sseWriter) setDeadline() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	err := s.controller.SetWriteDeadline(time.Now().Add(s.timeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}
func (s *sseWriter) flush() error {
	if err := s.setDeadline(); err != nil {
		return err
	}
	return s.check(s.controller.Flush())
}
func (s *sseWriter) check(err error) error {
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		s.metrics.slowWriters.Add(1)
	}
	return err
}

// Write the reserved public encoding directly. Formatting the whole SSE frame
// would copy it into fmt's complete-frame buffer while a socket write is held.
func (s *sseWriter) event(name string, data []byte) error {
	if err := s.setDeadline(); err != nil {
		return err
	}
	// The small complete header preserves the event-prefix write barrier.
	if err := s.writeString("event: " + name + "\ndata: "); err != nil {
		return err
	}
	if err := s.writeBytes(data); err != nil {
		return err
	}
	if err := s.writeString("\n\n"); err != nil {
		return err
	}
	return s.check(s.controller.Flush())
}
func (s *sseWriter) writeBytes(data []byte) error {
	n, err := s.writer.Write(data)
	s.metrics.sentBytes.Add(uint64(n))
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return s.check(err)
}
func (s *sseWriter) writeString(data string) error {
	n, err := io.WriteString(s.writer, data)
	s.metrics.sentBytes.Add(uint64(n))
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return s.check(err)
}
func (s *sseWriter) heartbeat() error {
	if err := s.setDeadline(); err != nil {
		return err
	}
	if err := s.writeString(": heartbeat\n\n"); err != nil {
		return err
	}
	return s.check(s.controller.Flush())
}
