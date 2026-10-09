package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestEventStreamNativeHTTPServiceLifecycle(t *testing.T) {
	transporttest.HTTPServiceLifecycle(t, func(ctx context.Context, transport transportsecurity.HTTPConfig, ready *workload.Readiness, run func(context.Context, workload.Config) error, public, metrics http.Handler) error {
		original := runWorkload
		runWorkload = run
		defer func() { runWorkload = original }()
		cfg := commandConfig{HTTPTransport: transport, ListenAddress: "127.0.0.1:0", MetricsAddress: "127.0.0.1:0"}
		return runPublicAndMetricsHTTP(ctx, cfg, ready, slog.New(slog.NewTextHandler(io.Discard, nil)), buildHTTPHandler(ready, public), metrics, workload.NewOperationMetrics("event-stream"))
	})
}
