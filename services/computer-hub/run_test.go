package computerhub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
)

// Run assembles the whole service and hands the public and the metrics
// listener to the workload runner, each answering readiness; it returns
// once ctx ends.
func TestRunHandsBothListenersToTheWorkloadRunner(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(storagetest.OpenWorkloadDB(t, admin, "computer_hub").OpenWorkload(t, "computer_hub", nil))
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{HTTPAddress: "127.0.0.1:18080", MetricsAddress: "127.0.0.1:18081", PrincipalVerifier: verifier, MaxCredentialLifetime: time.Hour, PageTokenKey: make([]byte, 32), MinCLIVersion: DefaultCLIVersion, RecommendedCLIVersion: DefaultCLIVersion, PodName: hubTestPod}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	listeners := make(chan workload.Config, 2)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, Runtime{Database: client, RunHTTP: func(ctx context.Context, config workload.Config) error {
			listeners <- config
			<-ctx.Done()
			return nil
		}})
	}()
	var addresses []string
	for range 2 {
		select {
		case config := <-listeners:
			response := httptest.NewRecorder()
			config.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if config.ServiceName != ServiceName || response.Code != http.StatusOK {
				t.Fatalf("listener %s of %q answers /ready %d", config.ListenAddress, config.ServiceName, response.Code)
			}
			addresses = append(addresses, config.ListenAddress)
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
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after ctx ended: %v", err)
	}
}
