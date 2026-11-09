package testinfra

import (
	"net"
	"sync"
)

// ListenLoopback opens the listener on a free loopback port that a
// reservation holds. A test process that owns a range of ports no other
// process is handed replaces it, so that no other process can take a port
// between its release and Docker's bind.
var ListenLoopback = func() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

// ReserveLoopbackPorts holds available host ports until release. Register
// release for cleanup, then call it immediately before Docker dispatch with
// explicit host bindings. A competing bind after release fails startup rather
// than silently changing the endpoint retained by fixture clients.
func ReserveLoopbackPorts(ports ...int) (map[int]int, func(), error) {
	bindings := make(map[int]int, len(ports))
	listeners := make([]net.Listener, 0, len(ports))
	var once sync.Once
	release := func() {
		once.Do(func() {
			for _, listener := range listeners {
				_ = listener.Close()
			}
		})
	}
	for _, port := range ports {
		listener, err := ListenLoopback()
		if err != nil {
			release()
			return nil, release, err
		}
		listeners = append(listeners, listener)
		bindings[port] = listener.Addr().(*net.TCPAddr).Port
	}
	return bindings, release, nil
}
