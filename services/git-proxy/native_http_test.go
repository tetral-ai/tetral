package gitproxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestGitProxyNativeHTTPServiceLifecycle(t *testing.T) {
	transporttest.HTTPServiceLifecycle(t, func(ctx context.Context, transport transportsecurity.HTTPConfig, ready *workload.Readiness, run func(context.Context, workload.Config) error, public, metrics http.Handler) error {
		cfg := Config{HTTPTransport: transport, HTTPAddress: "127.0.0.1:0", MetricsAddress: "127.0.0.1:0", DrainGrace: time.Duration(DefaultDrainGraceSeconds) * time.Second}
		return runHTTPPair(ctx, run, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), ready, cfg, BuildHTTPHandler(ready, public), metrics, workload.NewOperationMetrics("git-proxy"))
	})
}
