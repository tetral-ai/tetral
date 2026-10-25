package computerhub

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// HeartbeatInterval is the contract's spacing of heartbeats on a stream.
const HeartbeatInterval = 15 * time.Second

// StreamClock schedules a stream's heartbeats. Tests drive it by hand.
type StreamClock interface {
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Streams holds the client streams of this pod, one per connection, and
// receives the connection notices of the pod's channel (Run). A stream is
// the only writer of its response: notices reach it as connection rows.
type Streams struct {
	database *dbconnect.Client
	podName  string
	clock    StreamClock
	logger   *slog.Logger
	// listening is closed once the first LISTEN is in place.
	listening     chan struct{}
	listeningOnce sync.Once

	mu      sync.Mutex
	closed  bool
	streams map[string]*clientStream
}

// clientStream is one held stream. updates carries the latest connection row
// a notice read; a newer row replaces one the stream has not taken yet.
type clientStream struct {
	workspace  workspace.ID
	connection string
	updates    chan instance.Connection
	done       chan struct{}
	doneOnce   sync.Once
}

// NewStreams returns the streams of the pod named podName over database. A
// nil clock is the system clock.
func NewStreams(database *dbconnect.Client, podName string, clock StreamClock, logger *slog.Logger) *Streams {
	if clock == nil {
		clock = systemClock{}
	}
	if logger == nil {
		logger = workload.ComponentLogger(ServiceName)
	}
	return &Streams{database: database, podName: podName, clock: clock, logger: logger, listening: make(chan struct{}), streams: map[string]*clientStream{}}
}

// hold registers the stream of connectionID before its connection commits,
// so a notice sent by any later transaction finds it. It returns nil once
// Run has ended.
func (s *Streams) hold(ws workspace.ID, connectionID string) *clientStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	stream := &clientStream{workspace: ws, connection: connectionID, updates: make(chan instance.Connection, 1), done: make(chan struct{})}
	if previous := s.streams[connectionID]; previous != nil {
		previous.close()
	}
	s.streams[connectionID] = stream
	return stream
}

// release drops stream once its response ends.
func (s *Streams) release(stream *clientStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams[stream.connection] == stream {
		delete(s.streams, stream.connection)
	}
	stream.close()
}

func (s *Streams) lookup(connectionID string) *clientStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[connectionID]
}

func (s *Streams) held() []*clientStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	streams := make([]*clientStream, 0, len(s.streams))
	for _, stream := range s.streams {
		streams = append(streams, stream)
	}
	return streams
}

func (s *Streams) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, stream := range s.streams {
		stream.close()
	}
}

func (c *clientStream) close() { c.doneOnce.Do(func() { close(c.done) }) }

// offer hands the stream its connection's newest row; only the notice
// listener calls it, so the replacement below never races another offer.
func (c *clientStream) offer(connection instance.Connection) {
	for {
		select {
		case c.updates <- connection:
			return
		default:
			select {
			case <-c.updates:
			default:
			}
		}
	}
}

// serve writes the response of stream: the 200 headers and connected, then
// heartbeats once the connection's ready is accepted, until the client
// leaves, the pod shuts down or the connection ends. started is the
// connection as the stream's transaction committed it; on a resumed stream
// of an accepted connection the first heartbeat follows connected at once.
// The n-th heartbeat of the stream counted s carries
// s*instance.HeartbeatSeqStride+n.
func (s *Streams) serve(ctx context.Context, w http.ResponseWriter, stream *clientStream, started instance.Connection, resumed bool) {
	writer := sseWriter{w: w, controller: http.NewResponseController(w), connectionID: started.ID}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if writer.write(id.New("msg_"), protocol.ConnectedPayload{Resumed: resumed}) != nil {
		return
	}
	var (
		next     <-chan time.Time
		sent     int64
		accepted = started.ReadyAcceptedAt != nil
	)
	heartbeat := func() bool {
		sent++
		seq := started.StreamCount*instance.HeartbeatSeqStride + sent
		if writer.write(instance.HeartbeatMessageID(started.ID, seq), protocol.HeartbeatPayload{HeartbeatSeq: seq}) != nil {
			return false
		}
		next = s.clock.After(HeartbeatInterval)
		return true
	}
	if accepted {
		if resumed {
			if !heartbeat() {
				return
			}
		} else {
			next = s.clock.After(HeartbeatInterval)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-stream.done:
			return
		case connection := <-stream.updates:
			if connection.EndedAt != nil {
				if code, ok := disconnectCode(connection); ok {
					_ = writer.write(id.New("msg_"), protocol.DisconnectPayload{Scope: "connection", Retryable: protocol.Some(false), Code: protocol.Some(string(code))})
				}
				return
			}
			if !accepted && connection.ReadyAcceptedAt != nil {
				accepted = true
				next = s.clock.After(HeartbeatInterval)
			}
		case <-next:
			if !heartbeat() {
				return
			}
		}
	}
}

// disconnectCode is the code of the disconnect an ended connection's stream
// writes: credential_revoked after a revocation, instance_unavailable after
// a deregistration, none after the lease expired.
func disconnectCode(connection instance.Connection) (protocol.ErrorCode, bool) {
	switch *connection.EndCause {
	case instance.EndRevoked:
		return protocol.ErrorCredentialRevoked, true
	case instance.EndDeregistered:
		return protocol.ErrorInstanceUnavailable, true
	default:
		return "", false
	}
}

// sseWriter writes one envelope per event as a single data line followed by
// a blank line, and flushes it at once.
type sseWriter struct {
	w            http.ResponseWriter
	controller   *http.ResponseController
	connectionID string
}

func (s sseWriter) write(messageID string, payload protocol.DownstreamPayload) error {
	envelope := protocol.Encode(protocol.Downstream{ConnectionID: s.connectionID, MessageID: messageID, Payload: payload})
	event := make([]byte, 0, len(envelope)+8)
	event = append(append(append(event, "data: "...), envelope...), "\n\n"...)
	if _, err := s.w.Write(event); err != nil {
		return err
	}
	if err := s.controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}
