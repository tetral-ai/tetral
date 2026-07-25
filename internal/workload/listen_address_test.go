package workload

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestRunDoesNotLogListenerEndpoints(t *testing.T) {
	const endpoint = "listener.internal:43123"
	var output bytes.Buffer
	err := Run(context.Background(), Config{
		ServiceName:   "listener-test",
		ListenAddress: endpoint,
		Logger:        slog.New(slog.NewJSONHandler(&output, nil)),
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
	logged := output.String()
	if strings.Contains(logged, endpoint) || strings.Contains(logged, "listener.address") {
		t.Fatalf("listener endpoint leaked into logs: %s", logged)
	}
	if !strings.Contains(logged, `"listener.transport":"tcp"`) {
		t.Fatalf("listener transport missing from logs: %s", logged)
	}
}
