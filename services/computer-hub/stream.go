package computerhub

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// HeartbeatInterval is the time between two heartbeats a stream writes once
// its connection's ready is accepted.
const HeartbeatInterval = 15 * time.Second

// streamStallTimeout is how long a write may go without the stream's socket
// accepting a byte of it. A write has no total deadline, but a reader that
// frees no room in the socket's send buffer for that long loses its stream:
// one that stops, or one far below 1 MB per 10 seconds behind a backlog
// beyond the buffer, since the kernel frees room in batches of about a third
// of the buffer. The write is made in pieces of at most streamWritePiece
// bytes, each under its own stall deadline.
const (
	streamStallTimeout = 10 * time.Second
	streamWritePiece   = 16 << 10
)

// errStreamCut is a write cut because the stream's connection ended or
// moved, or the stream was closed (watch).
var errStreamCut = errors.New("computer-hub: the stream's write was cut")

// StreamClock schedules a stream's heartbeats and its age limit. Tests drive
// it by hand.
type StreamClock interface {
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Streams holds the client streams of this pod, one per connection, and
// receives the connection notices of the pod's channel (Run). A stream is
// the only writer of its response: notices only wake it, and it reads the
// connection and its instructions itself (dispatch.go).
type Streams struct {
	database *dbconnect.Client
	podName  string
	maxAge   time.Duration
	clock    StreamClock
	logger   *slog.Logger
	metrics  *hubMetrics
	// listening is closed once the first LISTEN is in place.
	listening     chan struct{}
	listeningOnce sync.Once

	mu sync.Mutex
	// draining refuses new streams and drains each one held from then on;
	// closed is set once Run has ended.
	draining bool
	closed   bool
	streams  map[string]*clientStream
}

// clientStream is one held stream: the connection and the stream count its
// transaction committed, and the release state notices leave for it.
type clientStream struct {
	workspace  workspace.ID
	connection string
	count      int64

	mu sync.Mutex
	// bound is the highest issue_seq a notice released; all asks the next
	// pass to release every instruction the connection row counts.
	bound int64
	all   bool
	wake  chan struct{}
	// notified is signalled by every notice, like wake, for watch.
	notified chan struct{}

	drain     chan struct{}
	drainOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once
	// closedBy is the end cause the stream's close gives, set before done
	// closes: endMoved when a later stream of the connection on this pod
	// replaced it, endDrain when Run ended.
	closedBy string
}

// NewStreams returns the streams of the pod named podName over database,
// each ended by a stream-only disconnect at maxAge plus up to a tenth of it.
// A nil clock is the system clock.
func NewStreams(database *dbconnect.Client, podName string, maxAge time.Duration, clock StreamClock, logger *slog.Logger, metrics *hubMetrics) *Streams {
	if clock == nil {
		clock = systemClock{}
	}
	if logger == nil {
		logger = workload.ComponentLogger(ServiceName)
	}
	return &Streams{database: database, podName: podName, maxAge: maxAge, clock: clock, logger: logger, metrics: metrics, listening: make(chan struct{}), streams: map[string]*clientStream{}}
}

// accepting reports whether the pod takes new streams.
func (s *Streams) accepting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.draining && !s.closed
}

// hold registers the stream of connectionID, whose transaction committed it
// with stream count count, and closes the pod's earlier stream of the
// connection. It is called after that commit; the stream's first pass reads
// whatever changed before it was registered. A stream held while the pod
// drains is drained at once; hold returns nil once Run has ended.
func (s *Streams) hold(ws workspace.ID, connectionID string, count int64) *clientStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	stream := &clientStream{workspace: ws, connection: connectionID, count: count, all: true,
		wake: make(chan struct{}, 1), notified: make(chan struct{}, 1), drain: make(chan struct{}), done: make(chan struct{})}
	if s.draining {
		stream.beginDrain()
	}
	if previous := s.streams[connectionID]; previous != nil {
		previous.close(endMoved)
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
	stream.close(endClientClosed)
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
		stream.close(endDrain)
	}
}

// Drain refuses new streams, then ends each held stream with a stream-only
// disconnect, the i-th of n at i/n of window, so their CLIs resume on other
// pods spread over the window. It returns once every disconnect is due, or
// when ctx ends.
func (s *Streams) Drain(ctx context.Context, window time.Duration) {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	streams := s.held()
	started := time.Now()
	for i, stream := range streams {
		due := started.Add(window * time.Duration(i) / time.Duration(len(streams)))
		timer := time.NewTimer(time.Until(due))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		stream.beginDrain()
	}
}

func (c *clientStream) close(cause string) {
	c.doneOnce.Do(func() {
		c.closedBy = cause
		close(c.done)
	})
}

func (c *clientStream) beginDrain() { c.drainOnce.Do(func() { close(c.drain) }) }

// release raises the stream's bound to issueSeq, or, with all, asks for
// every instruction the connection row counts, and wakes the stream.
func (c *clientStream) release(issueSeq int64, all bool) {
	c.mu.Lock()
	c.bound = max(c.bound, issueSeq)
	c.all = c.all || all
	c.mu.Unlock()
	for _, signal := range []chan struct{}{c.wake, c.notified} {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
}

// raise raises the stream's bound to issueSeq without waking it.
func (c *clientStream) raise(issueSeq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bound = max(c.bound, issueSeq)
}

// raiseAll asks the next pass again for every instruction the connection
// row counts, without waking the stream.
func (c *clientStream) raiseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.all = true
}

// take returns the bound and clears the all request.
func (c *clientStream) take() (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	all := c.all
	c.all = false
	return c.bound, all
}

// ageLimit is a stream's maximum age plus a uniform extra of 0 to a tenth
// of it.
func (s *Streams) ageLimit() time.Duration {
	return s.maxAge + time.Duration(rand.Int64N(int64(s.maxAge/10)+1)) //nolint:gosec // G404: spreads stream ends, not a secret.
}

// serve writes the response of stream: the 200 headers and connected, the
// stream's first heartbeat at once when it resumed an accepted connection,
// then the instructions and heartbeats until the client leaves, the pod
// drains or shuts down, the stream reaches its age limit, the connection
// moves to another stream or ends. started is the connection as the
// stream's transaction committed it. The n-th heartbeat of the stream counted
// s carries s*instance.HeartbeatSeqStride+n. When it returns it counts the
// stream's end under its first cause.
func (s *Streams) serve(ctx context.Context, w http.ResponseWriter, stream *clientStream, started instance.Connection, resumed bool) {
	writer := sseWriter{w: w, controller: http.NewResponseController(w), connectionID: started.ID, cutOff: new(atomic.Pointer[string])}
	state := &streamState{stream: stream, writer: writer, written: map[string]bool{}, unmarked: map[string]bool{}, accepted: started.ReadyAcceptedAt != nil, end: endClientClosed}
	defer func() { s.metrics.streamEndedBy(state.end) }()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	// The age limit counts from connected; its timer is set first so it is
	// pending once the client reads connected.
	age := s.clock.After(s.ageLimit())
	if err := writer.write(id.New("msg_"), protocol.ConnectedPayload{Resumed: resumed}); err != nil {
		state.end = writer.endCause(err)
		return
	}
	s.metrics.streamStarted(resumed)
	defer s.metrics.streamEnded()
	var (
		next <-chan time.Time
		sent int64
	)
	heartbeat := func() bool {
		sent++
		seq := started.StreamCount*instance.HeartbeatSeqStride + sent
		if err := writer.write(instance.HeartbeatMessageID(started.ID, seq), protocol.HeartbeatPayload{HeartbeatSeq: seq}); err != nil {
			state.end = writer.endCause(err)
			return false
		}
		next = s.clock.After(HeartbeatInterval)
		return true
	}
	state.beat = func() bool {
		select {
		case <-next:
			return heartbeat()
		default:
			return true
		}
	}
	if state.accepted && resumed && !heartbeat() {
		return
	}
	var retry <-chan time.Time
	for pass := true; ; {
		if pass {
			outcome := s.deliver(ctx, state)
			switch outcome {
			case passFailed:
				retry = s.clock.After(state.retryAfter(rand.Float64() * 0.2)) //nolint:gosec // G404: spreads retries, not a secret.
			case passEnded:
				state.end = endConnectionEnded
				return
			case passMoved:
				state.end = endMoved
				return
			case passBroken:
				return
			default:
				state.retryBase = 0
			}
			if state.accepted && next == nil {
				next = s.clock.After(HeartbeatInterval)
			}
			if pass = outcome == passWrote; pass {
				if !state.beat() {
					return
				}
				continue
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-stream.done:
			// Run closes every stream once the last drain is due, so a
			// stream whose drain began writes its disconnect first.
			state.end = stream.closedBy
			select {
			case <-stream.drain:
				state.end = endDrain
				_ = writer.write(id.New("msg_"), protocol.DisconnectPayload{Scope: "stream"})
			default:
			}
			return
		case <-stream.drain:
			state.end = endDrain
			_ = writer.write(id.New("msg_"), protocol.DisconnectPayload{Scope: "stream"})
			return
		case <-age:
			state.end = endAgeLimit
			_ = writer.write(id.New("msg_"), protocol.DisconnectPayload{Scope: "stream"})
			return
		case <-stream.wake:
			pass = true
		case <-retry:
			retry, pass = nil, true
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
// a blank line, flushed as it goes, under streamStallTimeout. cutOff, shared
// by its copies, holds the end cause of the first cut.
type sseWriter struct {
	w            http.ResponseWriter
	controller   *http.ResponseController
	connectionID string
	cutOff       *atomic.Pointer[string]
}

func (s sseWriter) write(messageID string, payload protocol.DownstreamPayload) error {
	return s.writeEnvelope(protocol.Encode(protocol.Downstream{ConnectionID: s.connectionID, MessageID: messageID, Payload: payload}))
}

func (s sseWriter) writeEnvelope(envelope []byte) error {
	event := make([]byte, 0, len(envelope)+8)
	event = append(append(append(event, "data: "...), envelope...), "\n\n"...)
	for len(event) > 0 {
		piece := event[:min(len(event), streamWritePiece)]
		if err := s.controller.SetWriteDeadline(time.Now().Add(streamStallTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		// Checked after the deadline is set, since cut sets its own after
		// the flag: a cut is never overwritten by a later piece's deadline.
		if s.cutOff.Load() != nil {
			return errStreamCut
		}
		if _, err := s.w.Write(piece); err != nil {
			return err
		}
		if err := s.controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		event = event[len(piece):]
	}
	if err := s.controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// cut ends the writer's write in progress and every later one, for cause.
func (s sseWriter) cut(cause string) {
	s.cutOff.CompareAndSwap(nil, &cause)
	_ = s.controller.SetWriteDeadline(time.Now())
}

// endCause is the end cause of the stream that err, a failed write, ends: the
// cut's cause, endStalled when the socket accepted no byte of a piece within
// streamStallTimeout, endClientClosed otherwise.
func (s sseWriter) endCause(err error) string {
	if cause := s.cutOff.Load(); cause != nil {
		return *cause
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return endStalled
	}
	return endClientClosed
}
