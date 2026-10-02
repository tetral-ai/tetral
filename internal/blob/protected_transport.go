package blob

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

// protectedTransport owns only this store's HTTP pool generations. An admitted
// response body pins its immutable trust until EOF/Close or bounded retirement.
// New requests never return to a retired transport after an old body completes.
type protectedTransport struct {
	mu               sync.Mutex
	owner            *transportsecurity.Owner
	serverName       string
	active, retired  *httpGeneration
	pending, stopped bool
	done             chan struct{}
	workers          sync.WaitGroup
	once             sync.Once
}
type httpGeneration struct {
	transport   *http.Transport
	config      *tls.Config
	expires     time.Time
	users       int
	mu          sync.Mutex
	connections map[*trackedTLSConnection]struct{}
	closed      bool
	done        chan struct{}
}
type trackedTLSConnection struct {
	net.Conn
	generation *httpGeneration
	once       sync.Once
}

func (c *trackedTLSConnection) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.generation.mu.Lock()
		delete(c.generation.connections, c)
		c.generation.mu.Unlock()
	})
	return err
}
func newHTTPGeneration(config *tls.Config, expires time.Time) *httpGeneration {
	g := &httpGeneration{config: config, expires: expires, connections: map[*trackedTLSConnection]struct{}{}, done: make(chan struct{})}
	g.transport = http.DefaultTransport.(*http.Transport).Clone()
	// Store transport is direct HTTPS; CONNECT proxies are separate TLS owners.
	g.transport.Proxy = nil
	g.transport.ForceAttemptHTTP2 = false
	g.transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&tls.Dialer{Config: g.config}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		tracked := &trackedTLSConnection{Conn: conn, generation: g}
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			_ = conn.Close()
			return nil, errors.New("object-store transport generation is closed")
		}
		g.connections[tracked] = struct{}{}
		g.mu.Unlock()
		return tracked, nil
	}
	return g
}
func (g *httpGeneration) close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	close(g.done)
	connections := make([]*trackedTLSConnection, 0, len(g.connections))
	for conn := range g.connections {
		connections = append(connections, conn)
	}
	g.mu.Unlock()
	g.transport.CloseIdleConnections()
	for _, conn := range connections {
		_ = conn.Close()
	}
}
func newProtectedTransport(owner *transportsecurity.Owner, serverName string) (*protectedTransport, error) {
	config, expires, err := owner.ClientTLSConfigSnapshot(serverName, "")
	if err != nil {
		return nil, err
	}
	p := &protectedTransport{owner: owner, serverName: serverName, active: newHTTPGeneration(config, expires), done: make(chan struct{})}
	if err := owner.SetActivationObserver(p.activate); err != nil {
		p.active.close()
		return nil, err
	}
	return p, nil
}
func (p *protectedTransport) activate() { p.mu.Lock(); defer p.mu.Unlock(); p.activateLocked() }
func (p *protectedTransport) activateLocked() {
	if p.stopped {
		return
	}
	config, expires, err := p.owner.ClientTLSConfigSnapshot(p.serverName, "")
	if err != nil {
		return
	}
	if p.active.config.RootCAs == config.RootCAs {
		return
	}
	if p.retired != nil {
		p.pending = true
		return
	}
	old := p.active
	p.active = newHTTPGeneration(config, expires)
	p.retired = old
	old.transport.CloseIdleConnections()
	if old.users == 0 {
		p.finishLocked(old)
		return
	}
	p.workers.Add(1)
	go func() {
		defer p.workers.Done()
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			p.mu.Lock()
			if p.retired == old {
				p.finishLocked(old)
			}
			p.mu.Unlock()
		case <-p.done:
			return
		case <-old.done:
			return
		}
	}()
}
func (p *protectedTransport) finishLocked(old *httpGeneration) {
	old.close()
	if p.retired == old {
		p.retired = nil
	}
	if p.pending && !p.stopped {
		p.pending = false
		p.activateLocked()
	}
}
func (p *protectedTransport) release(g *httpGeneration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g.users--
	if g.users == 0 && p.retired == g {
		p.finishLocked(g)
	}
}
func (p *protectedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil, errors.New("object-store transport owner is closed")
	}
	// Expired trust cannot admit new operations even on an established socket.
	if _, err := p.owner.ClientTLSConfig(p.serverName, ""); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	g := p.active
	if !time.Now().Before(g.expires) {
		p.mu.Unlock()
		return nil, errors.New("object-store transport trust generation is expired")
	}
	g.users++
	p.mu.Unlock()
	response, err := g.transport.RoundTrip(request)
	if err != nil {
		p.release(g)
		return nil, err
	}
	if response.Body == nil || response.Body == http.NoBody {
		p.release(g)
	} else {
		response.Body = &ownedResponseBody{ReadCloser: response.Body, release: func() { p.release(g) }}
	}
	return response, nil
}

type ownedResponseBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (b *ownedResponseBody) Read(bytes []byte) (int, error) {
	n, err := b.ReadCloser.Read(bytes)
	if err != nil {
		b.once.Do(b.release)
	}
	return n, err
}
func (b *ownedResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}
func (p *protectedTransport) Close() error {
	p.once.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.pending = false
		close(p.done)
		active, retired := p.active, p.retired
		p.mu.Unlock()
		_ = p.owner.Close()
		active.close()
		if retired != nil {
			retired.close()
		}
		p.workers.Wait()
	})
	return nil
}
