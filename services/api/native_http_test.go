package tetralapi

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

func TestAPINativeHTTPServiceLifecycle(t *testing.T) {
	transporttest.HTTPServiceLifecycle(t, func(ctx context.Context, transport transportsecurity.HTTPConfig, ready *workload.Readiness, run func(context.Context, workload.Config) error, public, metrics http.Handler) error {
		cfg := Config{HTTPTransport: transport, ListenAddress: "127.0.0.1:0", MetricsAddress: "127.0.0.1:0"}
		return runPublicAndMetricsHTTP(ctx, run, cfg, ready, slog.New(slog.NewTextHandler(io.Discard, nil)), BuildHTTPHandler(ready, public), metrics, workload.NewOperationMetrics("api"))
	})
}
