package eventstream

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	// idleCheckWorkers bounds shared checks in flight or queued for a worker.
	idleCheckWorkers = 4
	// idleCheckTimeout bounds one shared check statement.
	idleCheckTimeout = 2 * time.Second
)

// SessionSignal is the coarse idle signal of one Session.
type SessionSignal = internaleventstream.SessionSignal

// SessionSignalReader reads the idle signals of up to
// internaleventstream.MaxSessionSignalBatch Sessions of one workspace in one
// read-only statement, one signal per requested Session.
type SessionSignalReader interface {
	ReadSessionSignals(ctx context.Context, ws workspace.ID, sessionIDs []string) ([]SessionSignal, error)
}

var (
	errIdleChecksRequired = errors.New("eventstream: shared idle checks are required")
	errIdleChecksClosed   = errors.New("eventstream: shared idle checks are closed")
	errIdleCheckFailed    = errors.New("eventstream: shared idle check failed")
	errIdleSignalMissing  = errors.New("eventstream: shared idle check returned an incomplete batch")
)

// IdleCoalescer owns the process's shared idle checks: one scheduler
// goroutine and idleCheckWorkers database workers, never a goroutine or timer
// per watched feed. Every Session and Thread viewer of one Session shares one
// registration keyed by the trusted (workspace, Session); a registration's
// generation moves, and its channel closes, only when its Session signal
// changes or its check fails. Viewers keep their own scope, cursor and reads.
//
// Work is scheduled in workspace rounds. A round-robin deque holds the
// workspaces with registrations; a round snapshots the highest registration ID
// issued so far and visits that workspace's registrations up to it in
// ascending ID order, at most MaxSessionSignalBatch Sessions per statement,
// returning the workspace to the back of the deque after each chunk. A
// workspace has at most one check in flight, and the whole process at most
// idleCheckWorkers in flight or queued, so one large or slow workspace cannot
// occupy every worker. A completed round makes its workspace eligible again
// one poll interval after that round started, or at once when overdue.
type IdleCoalescer struct {
	reader   SessionSignalReader
	interval time.Duration
	logger   *slog.Logger

	ctx       context.Context
	cancel    context.CancelFunc
	work      chan idleChunk
	wake      chan struct{}
	joined    sync.WaitGroup
	closeOnce sync.Once

	// mu guards the registry and the schedule. It is never held over SQL or a
	// channel send.
	mu          sync.Mutex
	closed      bool
	lastID      uint64
	workspaces  map[workspace.ID]*idleWorkspace
	rounds      []*idleWorkspace
	outstanding int

	// Test hooks, set by in-package tests before start: the scheduler clock, a
	// callback after each chunk's result is published, and one after each
	// scheduler pass with the number of chunks it dispatched.
	now       func() time.Time
	published func(chunk idleChunk, err error)
	scheduled func(dispatched int)
}

type idleWorkspace struct {
	id       workspace.ID
	sessions map[string]*idleRegistration
	// order holds registrations by ascending ID, including detached ones until
	// the next round compacts it.
	order     []*idleRegistration
	cursor    uint64
	roundMax  uint64 // zero while no round is in progress
	nextRound time.Time
	inFlight  bool
	removed   bool
}

type idleRegistration struct {
	id        uint64
	workspace *idleWorkspace
	sessionID string
	refs      int
	// detached is set once the registry no longer maps this Session to this
	// registration; a later Join creates a new identity, and in-flight results
	// for this one are discarded.
	detached bool

	observed   bool
	signal     SessionSignal
	generation uint64
	changed    chan struct{}
	err        error

	// checked is closed and replaced after every completed check, only while a
	// test-only completed-check watcher holds this registration.
	checkWatchers int
	checked       chan struct{}
}

type idleChunk struct {
	workspace     *idleWorkspace
	registrations []*idleRegistration
}

// NewIdleCoalescer starts the shared idle checks. A non-positive interval
// selects the default poll interval. The caller owns its lifetime: Close it
// after the HTTP server drains and before closing the database pool.
func NewIdleCoalescer(reader SessionSignalReader, pollInterval time.Duration, logger *slog.Logger) *IdleCoalescer {
	c := newIdleCoalescer(reader, pollInterval, logger)
	c.start()
	return c
}

func newIdleCoalescer(reader SessionSignalReader, pollInterval time.Duration, logger *slog.Logger) *IdleCoalescer {
	if pollInterval <= 0 {
		pollInterval = DefaultStreamConfig().PollInterval
	}
	if logger == nil {
		logger = workload.ComponentLogger("event-stream")
	}
	// #nosec G118 -- the returned coalescer owns cancel; Close cancels and joins the scheduler and workers.
	ctx, cancel := context.WithCancel(context.Background())
	return &IdleCoalescer{
		reader:     reader,
		interval:   pollInterval,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		work:       make(chan idleChunk, idleCheckWorkers),
		wake:       make(chan struct{}, 1),
		workspaces: map[workspace.ID]*idleWorkspace{},
		now:        time.Now,
	}
}

func (c *IdleCoalescer) start() {
	c.joined.Add(1 + idleCheckWorkers)
	go c.schedule()
	for range idleCheckWorkers {
		go c.check()
	}
}

// Close stops admission, fails every registration so its viewers stop,
// cancels in-flight checks and joins the scheduler and every worker. No check
// runs after it returns.
func (c *IdleCoalescer) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		for _, entry := range c.workspaces {
			entry.removed = true
			for _, registration := range entry.sessions {
				registration.detached = true
				registration.fail(errIdleChecksClosed)
			}
		}
		c.workspaces = map[workspace.ID]*idleWorkspace{}
		c.rounds = nil
		c.mu.Unlock()
		c.cancel()
		c.joined.Wait()
	})
}

func (c *IdleCoalescer) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// join registers one viewer of a Session. countChecks enables the test-only
// completed-check channel for this viewer's registration.
func (c *IdleCoalescer) join(ws workspace.ID, sessionID string, countChecks bool) (*idleWatch, error) {
	if c == nil || c.reader == nil {
		return nil, errIdleChecksRequired
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errIdleChecksClosed
	}
	entry := c.workspaces[ws]
	added := entry == nil
	if added {
		entry = &idleWorkspace{id: ws, sessions: map[string]*idleRegistration{}}
		c.workspaces[ws] = entry
		c.rounds = append(c.rounds, entry)
	}
	registration := entry.sessions[sessionID]
	if registration == nil {
		c.lastID++
		registration = &idleRegistration{id: c.lastID, workspace: entry, sessionID: sessionID, changed: make(chan struct{})}
		entry.sessions[sessionID] = registration
		entry.order = append(entry.order, registration)
	}
	registration.refs++
	if countChecks {
		registration.checkWatchers++
		if registration.checked == nil {
			registration.checked = make(chan struct{})
		}
	}
	c.mu.Unlock()
	if added {
		c.signal()
	}
	return &idleWatch{coalescer: c, registration: registration, countChecks: countChecks}, nil
}

// detachLocked removes a registration from the registry, and its workspace
// once empty and not in flight.
func (c *IdleCoalescer) detachLocked(registration *idleRegistration) {
	registration.detached = true
	entry := registration.workspace
	if entry.sessions[registration.sessionID] == registration {
		delete(entry.sessions, registration.sessionID)
	}
	if len(entry.sessions) == 0 && !entry.inFlight && !entry.removed {
		entry.removed = true
		if c.workspaces[entry.id] == entry {
			delete(c.workspaces, entry.id)
		}
	}
}

func (r *idleRegistration) fail(err error) {
	if r.err != nil {
		return
	}
	r.err = err
	close(r.changed)
}

func (c *IdleCoalescer) schedule() {
	defer c.joined.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		c.mu.Lock()
		chunks, delay := c.dispatchLocked(c.now())
		c.mu.Unlock()
		for _, chunk := range chunks {
			// Never blocks: outstanding work never exceeds the buffer.
			c.work <- chunk
		}
		if c.scheduled != nil {
			c.scheduled(len(chunks))
		}
		if delay == 0 {
			continue
		}
		var due <-chan time.Time
		if delay > 0 {
			timer.Reset(delay)
			due = timer.C
		}
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-c.wake:
		case <-due:
		}
		timer.Stop()
	}
}

// dispatchLocked takes chunks from the deque front while workers are free. It
// returns the delay until the earliest pending round, zero to dispatch again
// at once, or a negative value when only a wake can make progress.
func (c *IdleCoalescer) dispatchLocked(now time.Time) ([]idleChunk, time.Duration) {
	var chunks []idleChunk
	delay := time.Duration(-1)
	for pending := len(c.rounds); pending > 0 && c.outstanding < idleCheckWorkers; pending-- {
		entry := c.rounds[0]
		c.rounds[0] = nil
		c.rounds = c.rounds[1:]
		if entry.removed {
			continue
		}
		if entry.roundMax == 0 {
			if wait := entry.nextRound.Sub(now); wait > 0 {
				c.rounds = append(c.rounds, entry)
				if delay < 0 || wait < delay {
					delay = wait
				}
				continue
			}
			entry.startRound(now, c.lastID, c.interval)
		}
		registrations := entry.nextChunk()
		if len(registrations) == 0 {
			// Every remaining registration of the round left before its turn.
			c.rounds = append(c.rounds, entry)
			if wait := entry.nextRound.Sub(now); delay < 0 || wait < delay {
				delay = max(wait, 0)
			}
			continue
		}
		entry.inFlight = true
		c.outstanding++
		chunks = append(chunks, idleChunk{workspace: entry, registrations: registrations})
	}
	return chunks, delay
}

func (w *idleWorkspace) startRound(now time.Time, maxID uint64, interval time.Duration) {
	live := w.order[:0]
	for _, registration := range w.order {
		if !registration.detached {
			live = append(live, registration)
		}
	}
	clear(w.order[len(live):])
	w.order = live
	w.cursor = 0
	w.roundMax = maxID
	w.nextRound = now.Add(interval)
}

// nextChunk returns the next live registrations of the round after the
// cursor, ending the round when none of its snapshot remains after them.
func (w *idleWorkspace) nextChunk() []*idleRegistration {
	i := sort.Search(len(w.order), func(i int) bool { return w.order[i].id > w.cursor })
	var chunk []*idleRegistration
	for ; i < len(w.order) && w.order[i].id <= w.roundMax; i++ {
		if w.order[i].detached {
			continue
		}
		if len(chunk) == internaleventstream.MaxSessionSignalBatch {
			break
		}
		chunk = append(chunk, w.order[i])
	}
	if len(chunk) > 0 {
		w.cursor = chunk[len(chunk)-1].id
	}
	for ; i < len(w.order) && w.order[i].id <= w.roundMax; i++ {
		if !w.order[i].detached {
			return chunk
		}
	}
	w.roundMax = 0
	return chunk
}

func (c *IdleCoalescer) check() {
	defer c.joined.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case chunk := <-c.work:
			c.run(chunk)
		}
	}
}

func (c *IdleCoalescer) run(chunk idleChunk) {
	ids := make([]string, len(chunk.registrations))
	for i, registration := range chunk.registrations {
		ids[i] = registration.sessionID
	}
	ctx, cancel := context.WithTimeout(c.ctx, idleCheckTimeout)
	signals, err := c.reader.ReadSessionSignals(ctx, chunk.workspace.id, ids)
	cancel()
	bySession := make(map[string]SessionSignal, len(signals))
	for _, signal := range signals {
		bySession[signal.SessionID] = signal
	}
	if err == nil && len(bySession) != len(ids) {
		err = errIdleSignalMissing
	}
	// The connection is released; publish without holding it.
	failed := c.publish(chunk, bySession, err)
	if failed > 0 && c.ctx.Err() == nil {
		reason := "query_failed"
		if errors.Is(err, context.DeadlineExceeded) {
			reason = "deadline_exceeded"
		}
		c.logger.Warn("event_stream.idle_check_failed", "operation", "event_stream.idle_check", "workspace.id", string(chunk.workspace.id), "failed.count", failed, "reason", reason, "outcome", "detached")
	}
	if c.published != nil {
		c.published(chunk, err)
	}
	c.signal()
}

// publish applies one chunk's result and returns the workspace to the deque.
// A changed signal advances the generation and closes its channel; a failure
// detaches every still-registered Session of the chunk with a sticky error.
// Registrations detached meanwhile, by their last viewer or a failure, are
// skipped, so a stale result never touches a replacement.
func (c *IdleCoalescer) publish(chunk idleChunk, signals map[string]SessionSignal, err error) (failed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := chunk.workspace
	entry.inFlight = false
	c.outstanding--
	for _, registration := range chunk.registrations {
		if registration.detached {
			continue
		}
		if err != nil {
			c.detachLocked(registration)
			registration.fail(errIdleCheckFailed)
			failed++
			continue
		}
		signal := signals[registration.sessionID]
		if !registration.observed || signal != registration.signal {
			registration.observed, registration.signal = true, signal
			registration.generation++
			close(registration.changed)
			registration.changed = make(chan struct{})
		}
		if registration.checked != nil {
			close(registration.checked)
			registration.checked = make(chan struct{})
		}
	}
	if entry.removed {
		return failed
	}
	if len(entry.sessions) == 0 {
		entry.removed = true
		if c.workspaces[entry.id] == entry {
			delete(c.workspaces, entry.id)
		}
		return failed
	}
	c.rounds = append(c.rounds, entry)
	return failed
}

// idleWatch is one viewer's reference to its Session registration.
type idleWatch struct {
	coalescer    *IdleCoalescer
	registration *idleRegistration
	countChecks  bool
}

// idleGeneration is a viewer's sample of its registration: the generation, the
// channel closed when it next changes, and the test-only completed-check
// channel (nil in production).
type idleGeneration struct {
	value   uint64
	changed <-chan struct{}
	checked <-chan struct{}
}

func (w *idleWatch) sample() (idleGeneration, error) {
	w.coalescer.mu.Lock()
	defer w.coalescer.mu.Unlock()
	registration := w.registration
	return idleGeneration{value: registration.generation, changed: registration.changed, checked: registration.checked}, registration.err
}

// changedSince reports whether the generation moved after sample, or the
// registration failed.
func (w *idleWatch) changedSince(sample idleGeneration) (bool, error) {
	w.coalescer.mu.Lock()
	defer w.coalescer.mu.Unlock()
	return w.registration.generation != sample.value, w.registration.err
}

// close releases this viewer's reference; the last one removes the Session
// registration and an emptied workspace entry.
func (w *idleWatch) close() {
	c := w.coalescer
	c.mu.Lock()
	defer c.mu.Unlock()
	registration := w.registration
	registration.refs--
	if w.countChecks {
		registration.checkWatchers--
		if registration.checkWatchers == 0 {
			registration.checked = nil
		}
	}
	if registration.refs == 0 && !registration.detached {
		c.detachLocked(registration)
	}
}
