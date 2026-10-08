package tetralauth

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// usageWritePause holds the usage worker's first UPDATE before it is sent,
// regardless of its context, until the test releases it.
type usageWritePause struct {
	claimed atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (p *usageWritePause) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "UPDATE public.api_keys") && p.claimed.CompareAndSwap(false, true) {
		close(p.entered)
		<-p.release
	}
	return ctx
}

func (*usageWritePause) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func usageDroppedClosed(t *testing.T, app *Application) float64 {
	t.Helper()
	metrics, err := app.UsageMetrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range metrics {
		if metric.Name == "tetral_auth_api_key_usage_submissions_dropped_total" && len(metric.Labels) == 1 && metric.Labels[0].Value == "closed" {
			return metric.Value
		}
	}
	t.Fatal("usage collector omitted closed submissions")
	return 0
}

func TestAuthApplicationCloseStopsUsageBeforePool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	if _, err := admin.ExecContext(ctx, `INSERT INTO workspaces (id, type, name, created_at) VALUES ('ws_auth_test', 'workspace', 'Auth Test', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	pause := &usageWritePause{entered: make(chan struct{}), release: make(chan struct{})}
	released := false
	release := func() {
		if !released {
			released = true
			close(pause.release)
		}
	}
	t.Cleanup(release)
	pool := storagetest.OpenWorkloadDB(t, admin, "auth").OpenWorkload(t, "auth", pause)
	cfg := Config{BootstrapAPIKey: testBootstrapAPIKey(), BootstrapWorkspaceID: "ws_auth_test", InternalPrincipalPrivateKeyB64: mustGenerateTestPrivateKey(t), InternalPrincipalTTL: DefaultInternalPrincipalTTL, JWKSCacheTTL: time.Minute}
	app, err := BuildApplication(ctx, cfg, func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{OpenResult: dbconnect.OpenResult{Client: dbconnect.NewClientForTesting(pool), RawDatabaseForExcludedStores: pool}, Client: &recordingSchemaStartupClient{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	admit := func() {
		t.Helper()
		response, err := app.ExternalAuthorization.Check(ctx, externalTestRequest("GET", "/v1/sessions", testBootstrapAPIKey(), nil))
		if err != nil || externalTestStatus(response) != 200 {
			t.Fatalf("production Check admission failed: %v", err)
		}
	}
	admit()
	// The production one-second tick starts writing the sample; hold it there.
	select {
	case <-pause.entered:
	case <-ctx.Done():
		t.Fatal("production usage worker never wrote the admitted sample")
	}
	closed := make(chan error, 1)
	go func() { closed <- app.Close() }()
	// Close marks submissions closed first and cannot close the pool until the
	// worker joins: admission still runs on the open pool, and its sample drops.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for usageDroppedClosed(t, app) == 0 {
		admit()
		select {
		case <-ctx.Done():
			t.Fatal("Close did not close usage submissions")
		case <-ticker.C:
		}
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before joining the in-flight usage worker: %v", err)
	default:
	}
	if err := pool.PingContext(ctx); err != nil {
		t.Fatal("Close closed the Auth pool before joining the usage worker")
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not join the cancelled usage worker")
	}
	if err := pool.PingContext(ctx); err == nil {
		t.Fatal("Close left the Auth pool open")
	}
	if err := app.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	var used sql.NullTime
	if err := admin.QueryRowContext(ctx, `SELECT last_used_at FROM api_keys WHERE key_kind='bootstrap'`).Scan(&used); err != nil || used.Valid {
		t.Fatal("cancelled or pending usage was written during shutdown")
	}
}
