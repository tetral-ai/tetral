package agentruntimebridge

import (
	"net"
	"testing"

	"google.golang.org/grpc"
)

func serveAttachmentCompositionBridge(t *testing.T, store BridgeAPIStore) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for attachment composition Bridge: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
