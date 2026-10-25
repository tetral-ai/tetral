package computerhub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

// Run assembles the whole service from its Config and hands the public and
// the metrics listener to the workload runner, each answering readiness; the
// public one refuses a credential beyond the configured maximum lifetime and
// a ready below the configured minimum CLI version. Run returns once ctx
// ends.
func TestRunHandsBothListenersToTheWorkloadRunner(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cfg := Config{HTTPAddress: "127.0.0.1:18080", MetricsAddress: "127.0.0.1:18081", PrincipalVerifier: f.verifier, MaxCredentialLifetime: time.Hour, PageTokenKey: make([]byte, 32),
		MinCLIVersion: "1.0.0", RecommendedCLIVersion: "1.0.0", PodName: "computer-hub-run", StreamMaxAge: 30 * time.Minute}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	listeners := make(chan workload.Config, 2)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, Runtime{Database: f.client, RunHTTP: func(ctx context.Context, config workload.Config) error {
			listeners <- config
			<-ctx.Done()
			return nil
		}})
	}()
	var (
		addresses []string
		public    http.Handler
	)
	for range 2 {
		select {
		case config := <-listeners:
			response := httptest.NewRecorder()
			config.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if config.ServiceName != ServiceName || response.Code != http.StatusOK {
				t.Fatalf("listener %s of %q answers /ready %d", config.ListenAddress, config.ServiceName, response.Code)
			}
			addresses = append(addresses, config.ListenAddress)
			if config.ListenAddress == cfg.HTTPAddress {
				public = config.Handler
			}
		case err := <-done:
			t.Fatalf("Run returned before both listeners started: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("Run started fewer than two listeners")
		}
	}
	slices.Sort(addresses)
	if !slices.Equal(addresses, []string{cfg.HTTPAddress, cfg.MetricsAddress}) {
		t.Fatalf("listeners on %v; want %s and %s", addresses, cfg.HTTPAddress, cfg.MetricsAddress)
	}

	server := httptest.NewServer(public)
	defer server.Close()
	f.url = server.URL
	beyond := time.Now().UTC().Add(61 * time.Minute).Format(time.RFC3339)
	status, body := f.do(http.MethodPost, "/v1/instances", []string{"run-expiry-61"}, `{"name":"laptop","credential":{"expires_at":"`+beyond+`"}}`)
	requireAPIError(t, status, body, http.StatusBadRequest, "invalid_request_error", "expires_at")
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	response, err := cli.Ready(f.ctx, readyOf(stream.connectionID, "ready_old_cli", "0.9.0"))
	requireClientError(t, response, err, protocol.ErrorCLIUpgradeRequired)

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after ctx ended: %v", err)
	}
}
