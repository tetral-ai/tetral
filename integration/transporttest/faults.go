package transporttest

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// FaultMode applies only after the business owner independently observes the
// selected request/stream. TCP connection acceptance alone is not admission.
type FaultMode string

const (
	FaultReset     FaultMode = "reset"
	FaultBlackhole FaultMode = "blackhole"
)

type FaultEvent struct {
	State  string
	Offset time.Duration
}
type FaultSnapshot struct {
	ConnectionID                               uint64
	Mode                                       FaultMode
	Events                                     []FaultEvent
	DiscardedClientBytes, DiscardedServerBytes uint64
	ClientClosed, ServerClosed                 bool
}
type ForwardedConnection struct {
	ID                           uint64
	owner                        *FaultForwarder
	client, server               net.Conn
	mode                         atomic.Uint32
	discardClient, discardServer atomic.Uint64
	clientClosed, serverClosed   atomic.Bool
	closeOnce                    sync.Once
}

func (c *ForwardedConnection) close(reset bool) {
	c.closeOnce.Do(func() {
		if reset {
			if tcp, ok := c.client.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0)
			}
			if tcp, ok := c.server.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0)
			}
		}
		_ = c.client.Close()
		c.clientClosed.Store(true)
		_ = c.server.Close()
		c.serverClosed.Store(true)
	})
}

type operationFault struct {
	conn   *ForwardedConnection
	mode   FaultMode
	events []FaultEvent
}
type FaultForwarder struct {
	Address     string
	listener    net.Listener
	target      string
	started     time.Time
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	connections map[uint64]*ForwardedConnection
	operations  map[string]*operationFault
	accepted    chan *ForwardedConnection
	workers     sync.WaitGroup
	closeOnce   sync.Once
	joined      chan struct{}
	next        atomic.Uint64
	stopped     bool
}

func NewFaultForwarder(ctx context.Context, target string) (*FaultForwarder, error) {
	if _, _, err := net.SplitHostPort(target); err != nil {
		return nil, errors.New("forwarder requires an explicit TCP target")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	// #nosec G118 -- returned forwarder owns cancel; Close cancels and joins watcher, both byte pumps and accept loop.
	life, cancel := context.WithCancel(ctx)
	f := &FaultForwarder{Address: listener.Addr().String(), listener: listener, target: target, started: time.Now(), ctx: life, cancel: cancel, connections: map[uint64]*ForwardedConnection{}, operations: map[string]*operationFault{}, accepted: make(chan *ForwardedConnection, 16), joined: make(chan struct{})}
	f.workers.Add(2)
	go f.accept()
	go func() {
		defer f.workers.Done()
		<-life.Done()
		f.stop()
	}()
	return f, nil
}
func (f *FaultForwarder) accept() {
	defer f.workers.Done()
	for {
		client, err := f.listener.Accept()
		if err != nil {
			return
		}
		server, err := (&net.Dialer{Timeout: time.Second}).DialContext(f.ctx, "tcp", f.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		c := &ForwardedConnection{ID: f.next.Add(1), owner: f, client: client, server: server}
		f.mu.Lock()
		if f.stopped {
			f.mu.Unlock()
			c.close(false)
			return
		}
		f.connections[c.ID] = c
		f.workers.Add(2)
		f.mu.Unlock()
		go f.copy(c, c.client, c.server, &c.discardClient)
		go f.copy(c, c.server, c.client, &c.discardServer)
		select {
		case f.accepted <- c:
		case <-f.ctx.Done():
			return
		}
	}
}
func (f *FaultForwarder) copy(c *ForwardedConnection, from, to net.Conn, discarded *atomic.Uint64) {
	defer f.workers.Done()
	defer c.close(false)
	buffer := make([]byte, 32768)
	for {
		n, err := from.Read(buffer)
		if n > 0 {
			if c.mode.Load() == 2 {
				discarded.Add(uint64(n))
			} else {
				remaining := buffer[:n]
				for len(remaining) > 0 {
					written, e := to.Write(remaining)
					if e != nil || written == 0 {
						return
					}
					remaining = remaining[written:]
				}
			}
		}
		if err != nil {
			return
		}
	}
}
func (f *FaultForwarder) AwaitConnection(ctx context.Context) (*ForwardedConnection, error) {
	select {
	case c := <-f.accepted:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.ctx.Done():
		return nil, errors.New("forwarder is closed")
	}
}
func (f *FaultForwarder) Arm(operationID string, c *ForwardedConnection) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped || operationID == "" || c == nil || c.owner != f || f.connections[c.ID] != c || c.clientClosed.Load() || c.serverClosed.Load() {
		return errors.New("fault requires a live owned connection and operation identity")
	}
	if _, exists := f.operations[operationID]; exists {
		return errors.New("operation fault already exists")
	}
	for _, op := range f.operations {
		if op.conn == c {
			return errors.New("connection already belongs to another operation")
		}
	}
	f.operations[operationID] = &operationFault{conn: c, events: []FaultEvent{{"armed", time.Since(f.started)}}}
	return nil
}
func (f *FaultForwarder) Reached(operationID string) error {
	return f.transition(operationID, "armed", "reached")
}
func (f *FaultForwarder) Release(operationID string) error {
	return f.transition(operationID, "faulted", "released")
}
func (f *FaultForwarder) transition(id, from, to string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	op := f.operations[id]
	if f.stopped || op == nil || op.events[len(op.events)-1].State != from {
		return errors.New("fault barrier identity or state does not match")
	}
	op.events = append(op.events, FaultEvent{to, time.Since(f.started)})
	return nil
}
func (f *FaultForwarder) Fault(id string, mode FaultMode) error {
	f.mu.Lock()
	op := f.operations[id]
	if f.stopped || op == nil || op.events[len(op.events)-1].State != "reached" || (mode != FaultReset && mode != FaultBlackhole) {
		f.mu.Unlock()
		return errors.New("fault requires the reached selected operation")
	}
	op.mode = mode
	op.events = append(op.events, FaultEvent{"faulted", time.Since(f.started)})
	value := uint32(1)
	if mode == FaultBlackhole {
		value = 2
	}
	op.conn.mode.Store(value)
	f.mu.Unlock()
	if mode == FaultReset {
		op.conn.close(true)
	}
	return nil
}
func (f *FaultForwarder) Snapshot(id string) (FaultSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op := f.operations[id]
	if op == nil {
		return FaultSnapshot{}, errors.New("unknown fault operation")
	}
	c := op.conn
	return FaultSnapshot{c.ID, op.mode, append([]FaultEvent{}, op.events...), c.discardClient.Load(), c.discardServer.Load(), c.clientClosed.Load(), c.serverClosed.Load()}, nil
}

// stop initiates owned cancellation; it never waits for the calling worker.
func (f *FaultForwarder) stop() {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.stopped = true
		connections := make([]*ForwardedConnection, 0, len(f.connections))
		for _, c := range f.connections {
			connections = append(connections, c)
		}
		f.mu.Unlock()
		f.cancel()
		_ = f.listener.Close()
		for _, c := range connections {
			c.close(false)
		}
		go func() { f.workers.Wait(); close(f.joined) }()
	})
}
func (f *FaultForwarder) Close(ctx context.Context) error {
	f.stop()
	select {
	case <-f.joined:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
