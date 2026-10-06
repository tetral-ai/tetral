package webconnector

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/workload"
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
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	go func() {
		done <- Run(ctx, Config{GRPCAddress: "127.0.0.1:0", MetricsAddress: "127.0.0.1:0"}, service, service.metrics, RuntimeConfig{Authenticator: fixedAuthenticator{identity: grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "runtime-pod"}}, Listen: listen, Logger: logger})
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
	for _, field := range []string{
		`"msg":"workload.started"`,
		`"operation":"workload.lifecycle"`,
		`"event.kind":"started"`,
		`"component":"workload"`,
		`"listener.transport":"tcp"`,
		`"readiness.state":"not ready"`,
	} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("started log = %s; want %s", logs.String(), field)
		}
	}
}

func TestRunListenerStartupFailuresAreSafe(t *testing.T) {
	for _, name := range []string{"grpc", "metrics"} {
		t.Run(name, func(t *testing.T) {
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = occupied.Close() }()
			address := occupied.Addr().String()
			cfg := Config{GRPCAddress: "127.0.0.1:0", MetricsAddress: "127.0.0.1:0"}
			if name == "grpc" {
				cfg.GRPCAddress = address
			} else {
				cfg.MetricsAddress = address
			}
			service, _, _ := testService(blob.NewFakeBlobStore(), &fakeBackend{})
			var logs bytes.Buffer
			var opened string
			listen := func(network, address string) (net.Listener, error) {
				listener, err := net.Listen(network, address)
				if err == nil {
					opened = listener.Addr().String()
				}
				return listener, err
			}
			diagnostics := workload.NewProcessLogger(&logs, ServiceName, "test", "listener-test", workload.DefaultDiagnosticConfig())
			t.Cleanup(diagnostics.CloseWithBudget)
			err = Run(context.Background(), cfg, service, service.metrics, RuntimeConfig{Authenticator: fixedAuthenticator{}, Listen: listen, Logger: diagnostics.Logger})
			diagnostics.CloseWithBudget()
			if err == nil {
				t.Fatal("occupied listener was admitted")
			}
			for _, field := range []string{`"event.kind":"startup_failed"`, `"startup.cause":"listener"`, `"error.class":"startup_error"`, `"error.message_safe":"startup failed"`, `"startup.cause_category":"` + name + `"`} {
				if !strings.Contains(logs.String(), field) {
					t.Fatalf("missing safe startup field %s", field)
				}
			}
			if strings.Contains(logs.String(), address) || strings.Contains(logs.String(), err.Error()) {
				t.Fatal("raw bind failure leaked")
			}
			if opened != "" {
				listener, err := net.Listen("tcp", opened)
				if err != nil {
					t.Fatal("earlier listener was not closed on startup failure")
				}
				_ = listener.Close()
			}
		})
	}
}
