package transporttest

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestFaultForwarderSelectedConnection(t *testing.T) {
	for _, mode := range []FaultMode{FaultReset, FaultBlackhole} {
		t.Run(string(mode), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			accepted := make(chan net.Conn, 1)
			go func() {
				connection, err := listener.Accept()
				if err == nil {
					accepted <- connection
				}
			}()
			lifetime, cancelLifetime := context.WithCancel(t.Context())
			defer cancelLifetime()
			forwarder, err := NewFaultForwarder(lifetime, listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := forwarder.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			client, err := net.Dial("tcp", forwarder.Address)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			var server net.Conn
			select {
			case server = <-accepted:
			case <-time.After(time.Second):
				t.Fatal("backend socket not connected")
			}
			defer func() { _ = server.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			connection, err := forwarder.AwaitConnection(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Write([]byte("before")); err != nil {
				t.Fatal(err)
			}
			_ = server.SetReadDeadline(time.Now().Add(time.Second))
			buffer := make([]byte, 6)
			if _, err := io.ReadFull(server, buffer); err != nil || string(buffer) != "before" {
				t.Fatalf("healthy bytes=%q err=%v", buffer, err)
			}
			if err := forwarder.Arm("case-owned", connection); err != nil {
				t.Fatal(err)
			}
			if err := forwarder.Reached("another-case"); err == nil {
				t.Fatal("wrong operation released barrier")
			}
			if err := forwarder.Fault("case-owned", mode); err == nil {
				t.Fatal("unobserved operation faulted")
			}
			if err := forwarder.Reached("case-owned"); err != nil {
				t.Fatal(err)
			}
			if err := forwarder.Fault("case-owned", mode); err != nil {
				t.Fatal(err)
			}
			peers := map[string]net.Conn{"client": client, "server": server}
			if mode == FaultReset {
				// A reset aborts both connections: each peer observes a
				// connection reset, not an orderly end of stream.
				for name, peer := range peers {
					_ = peer.SetReadDeadline(time.Now().Add(time.Second))
					if _, err := peer.Read(buffer); !errors.Is(err, syscall.ECONNRESET) {
						t.Fatalf("reset %s read err=%v; want connection reset", name, err)
					}
				}
			}
			if mode == FaultBlackhole {
				if _, err := client.Write([]byte("client-drop")); err != nil {
					t.Fatal(err)
				}
				if _, err := server.Write([]byte("server-drop")); err != nil {
					t.Fatal(err)
				}
				if err := Await(ctx, func() bool {
					snapshot, _ := forwarder.Snapshot("case-owned")
					return snapshot.DiscardedClientBytes == 11 && snapshot.DiscardedServerBytes == 11
				}); err != nil {
					t.Fatal("bidirectional discard not observed", err)
				}
				_ = client.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
				if _, err := client.Read(buffer); err == nil {
					t.Fatal("blackhole forwarded response")
				}
				_ = server.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
				if _, err := server.Read(buffer); err == nil {
					t.Fatal("blackhole forwarded request")
				}
			}
			snapshot, err := forwarder.Snapshot("case-owned")
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.ConnectionID != connection.ID || snapshot.Mode != mode || len(snapshot.Events) != 3 {
				t.Fatalf("fault did not target owned connection: %+v", snapshot)
			}
			if mode == FaultReset {
				if !snapshot.ClientClosed || !snapshot.ServerClosed {
					t.Fatal("reset did not close both sockets")
				}
			} else if snapshot.ClientClosed || snapshot.ServerClosed {
				t.Fatal("blackhole closed socket")
			}
			if err := forwarder.Release("case-owned"); err != nil {
				t.Fatal(err)
			}
			if err := forwarder.Release("case-owned"); err == nil {
				t.Fatal("barrier released twice")
			}
			snapshot, _ = forwarder.Snapshot("case-owned")
			for i, state := range []string{"armed", "reached", "faulted", "released"} {
				if snapshot.Events[i].State != state || (i > 0 && snapshot.Events[i].Offset < snapshot.Events[i-1].Offset) {
					t.Fatal("unordered barrier events")
				}
			}
			cancelLifetime()
			select {
			case <-forwarder.joined:
			case <-time.After(time.Second):
				t.Fatal("parent cancellation did not join the watcher, accept loop and both byte pumps")
			}
			snapshot, _ = forwarder.Snapshot("case-owned")
			if !snapshot.ClientClosed || !snapshot.ServerClosed {
				t.Fatal("joined cancellation retained owned sockets")
			}
			if mode == FaultBlackhole {
				// Joined cancellation closes the retained sockets gracefully,
				// the control for the reset case above.
				for name, peer := range peers {
					_ = peer.SetReadDeadline(time.Now().Add(time.Second))
					if _, err := peer.Read(buffer); err != io.EOF {
						t.Fatalf("graceful close %s read err=%v; want end of stream", name, err)
					}
				}
			}
		})
	}
}
