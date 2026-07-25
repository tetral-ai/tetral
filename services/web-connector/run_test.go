package webconnector

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
)

func TestRunOpensSeparateListenersAndStopsCleanlyOnCancellation(t *testing.T) {
	service, _, _ := testService(blob.NewFakeBlobStore(), &fakeBackend{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := make(chan struct{})
	var once sync.Once
	count := 0
	var mu sync.Mutex
	listen := func(network, address string) (net.Listener, error) {
		listener, err := net.Listen(network, "127.0.0.1:0")
		if err == nil {
			mu.Lock()
			count++
			if count == 2 {
				once.Do(func() { close(opened) })
			}
			mu.Unlock()
		}
		return listener, err
	}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{GRPCAddress: "127.0.0.1:0", MetricsAddress: "127.0.0.1:0"}, service, service.metrics, RuntimeConfig{Authenticator: fixedAuthenticator{identity: grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "runtime-pod"}}, Listen: listen})
	}()
	select {
	case <-opened:
	case <-time.After(2 * time.Second):
		t.Fatal("listeners did not open")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}
