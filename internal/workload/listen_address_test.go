package workload

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestRunDoesNotLogListenerEndpoints(t *testing.T) {
	const endpoint = "listener.internal:43123"
	var output bytes.Buffer
	owner := NewProcessLogger(&output, "listener-test", "test", "unit", DefaultDiagnosticConfig())
	err := Run(context.Background(), Config{
		ServiceName:   "listener-test",
		ListenAddress: endpoint,
		Logger:        owner.Logger,
		Handler:       http.NotFoundHandler(),
		Listen: func(_, address string) (net.Listener, error) {
			if address != endpoint {
				t.Fatalf("listen address = %q; want fixture endpoint", address)
			}
			return nil, errors.New("bind failed")
		},
	})
	if err == nil {
		t.Fatal("Run returned nil; want bind failure")
	}
	owner.CloseWithBudget()
	logged := output.String()
	if !strings.Contains(logged, `"event":"workload.listener.failed"`) || !strings.Contains(logged, `"error.message":"bind failed"`) {
		t.Fatalf("listener failure record lacks the listen error: %s", logged)
	}
	if strings.Contains(logged, endpoint) || strings.Contains(logged, "listener.address") {
		t.Fatalf("listener endpoint leaked into logs: %s", logged)
	}
	if !strings.Contains(logged, `"listener.transport":"tcp"`) {
		t.Fatalf("listener transport missing from logs: %s", logged)
	}
}

func TestListenTCPRefusesAMalformedAddressWithoutQuotingIt(t *testing.T) {
	listenErr := errors.New("bind failed")
	for _, test := range []struct {
		address    string
		wellFormed bool
	}{
		{address: ":8080", wellFormed: true},
		{address: "0.0.0.0:9090", wellFormed: true},
		{address: "[::]:8080", wellFormed: true},
		{address: "localhost:0", wellFormed: true},
		{address: "queue.tetral-system.svc.cluster.local:9090", wellFormed: true},
		{address: "tetral_sk_abcdefghijklmnopqrstuvwxyz123456@db.internal:5432"},
		{address: "127.0.0.1:bad"},
		{address: "127.0.0.1:65536"},
		{address: "127.0.0.1:"},
		{address: "8080"},
	} {
		t.Run(test.address, func(t *testing.T) {
			listened := false
			_, err := ListenTCP(func(_, address string) (net.Listener, error) {
				listened = true
				return nil, listenErr
			}, test.address)
			if test.wellFormed {
				if !listened || !errors.Is(err, listenErr) {
					t.Fatalf("ListenTCP(%q) = %v, listened %v; want the listen error", test.address, err, listened)
				}
				return
			}
			if _, ok := AsConfigError(err); !ok || listened || strings.Contains(err.Error(), test.address) {
				t.Fatalf("ListenTCP(%q) = %v, listened %v; want a ConfigError that quotes nothing", test.address, err, listened)
			}
		})
	}
}
