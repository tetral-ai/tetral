package gitproxy

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

func TestConfigFromEnvLoadsGitProxyRuntimeSurface(t *testing.T) {
	cfg, err := ConfigFromEnv(envMap{
		EnvHTTPAddress:           "127.0.0.1:18080",
		EnvMetricsAddress:        "127.0.0.1:18081",
		EnvDatabaseURL:           "postgres://runtime:secret@postgres/tetral",
		EnvVaultKey:              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		EnvPublicBaseURL:         "https://git.tetral.example/",
		EnvDeploymentEnvironment: "test",
		EnvServiceVersion:        "unit",
		EnvDrainGraceSeconds:     "42",
		EnvLegacyPathCutover:     "true",
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.HTTPAddress != "127.0.0.1:18080" ||
		cfg.MetricsAddress != "127.0.0.1:18081" ||
		cfg.DatabaseURL == "" ||
		cfg.DeploymentEnvironment != "test" ||
		cfg.ServiceVersion != "unit" ||
		cfg.DrainGrace != 42*time.Second || !cfg.LegacyPathCutover {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.PublicBaseURL == nil || cfg.PublicBaseURL.String() != "https://git.tetral.example" {
		t.Fatalf("PublicBaseURL = %v; want trimmed https origin", cfg.PublicBaseURL)
	}
}

func TestConfigFromEnvRequiresSafeCredentialAndDatabaseShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  envMap
		want string
	}{
		{name: "database", env: envMap{EnvVaultKey: validVaultKey()}, want: EnvDatabaseURL},
		{name: "vault key", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral"}, want: EnvVaultKey},
		{name: "non hex vault key", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral", EnvVaultKey: strings.Repeat("z", 64)}, want: "hexadecimal"},
		{name: "public base", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral", EnvVaultKey: validVaultKey(), EnvPublicBaseURL: "http://git.tetral.example?token=secret"}, want: EnvPublicBaseURL},
		{name: "public base path", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral", EnvVaultKey: validVaultKey(), EnvPublicBaseURL: "https://git.tetral.example/base"}, want: EnvPublicBaseURL},
		{name: "drain", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral", EnvVaultKey: validVaultKey(), EnvDrainGraceSeconds: "0"}, want: EnvDrainGraceSeconds},
		{name: "legacy cutover", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral", EnvVaultKey: validVaultKey(), EnvLegacyPathCutover: "yes"}, want: EnvLegacyPathCutover},
		{name: "metrics same as public", env: envMap{EnvDatabaseURL: "postgres://runtime@postgres/tetral", EnvVaultKey: validVaultKey(), EnvHTTPAddress: "127.0.0.1:8080", EnvMetricsAddress: "127.0.0.1:8080"}, want: EnvMetricsAddress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ConfigFromEnv(tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ConfigFromEnv err = %v; want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "token=secret") {
				t.Fatalf("ConfigFromEnv error leaked secret-shaped input: %q", err.Error())
			}
		})
	}
}

func TestBuildHTTPHandlerKeepsProbesOutOfGitRelay(t *testing.T) {
	readiness := workload.NewReadiness()
	proxyCalls := 0
	handler := BuildHTTPHandler(readiness, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyCalls++
		w.WriteHeader(http.StatusNoContent)
	}))

	assertProbe(t, handler, "/health", 200, "ok\n")
	assertProbe(t, handler, "/ready", 503, "not ready\n")
	assertProbe(t, handler, "/metrics", 404, "404 page not found\n")
	assertProbe(t, handler, "/metrics/", 404, "404 page not found\n")
	readiness.MarkReady()
	assertProbe(t, handler, "/ready", 200, "ready\n")
	assertProbe(t, handler, "/ticket/github.com/tetral-ai/tetral/info/refs?service=git-upload-pack", 204, "")
	if proxyCalls != 1 {
		t.Fatalf("proxyCalls = %d; want only the git route to hit proxy", proxyCalls)
	}
}

func TestBuildMetricsHTTPHandlerServesMetricsOnInternalSurface(t *testing.T) {
	readiness := workload.NewReadiness()
	metrics := NewGitProxyMetrics()
	metrics.IncActive()
	handler := BuildMetricsHTTPHandler(readiness, metrics, fakeDBStatsProvider{stats: sql.DBStats{OpenConnections: 3}})

	assertProbe(t, handler, "/health", 200, "ok\n")
	assertProbe(t, handler, "/ready", 503, "not ready\n")
	assertProbeContains(t, handler, "/metrics", 200, "gitproxy_active_connections 1")
	assertProbeContains(t, handler, "/metrics", 200, "go_goroutines")
	assertProbeContains(t, handler, "/metrics", 200, `db_pool_open_connections{pool="runtime"} 3`)
	readiness.MarkReady()
	assertProbe(t, handler, "/ready", 200, "ready\n")
}

func TestRunHTTPPairStartsPublicAndMetricsListeners(t *testing.T) {
	calls := make(chan workload.Config, 2)
	err := runHTTPPair(
		context.Background(),
		func(_ context.Context, cfg workload.Config) error {
			calls <- cfg
			return nil
		},
		nil,
		nil,
		workload.NewReadiness(),
		Config{
			HTTPAddress:           "127.0.0.1:18080",
			MetricsAddress:        "127.0.0.1:18081",
			DeploymentEnvironment: "test",
			ServiceVersion:        "unit",
			DrainGrace:            time.Second,
		},
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	)
	if err != nil {
		t.Fatalf("runHTTPPair: %v", err)
	}
	byKey := map[string]workload.Config{}
	for range 2 {
		cfg := <-calls
		byKey[cfg.ListenConfigKey] = cfg
	}
	if byKey[EnvHTTPAddress].ListenAddress != "127.0.0.1:18080" {
		t.Fatalf("public listener config = %+v", byKey[EnvHTTPAddress])
	}
	if byKey[EnvMetricsAddress].ListenAddress != "127.0.0.1:18081" {
		t.Fatalf("metrics listener config = %+v", byKey[EnvMetricsAddress])
	}
}

func validVaultKey() string {
	return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

type envMap map[string]string

func (e envMap) Getenv(key string) string { return e[key] }

type fakeDBStatsProvider struct {
	stats sql.DBStats
}

func (p fakeDBStatsProvider) Stats() sql.DBStats { return p.stats }

func assertProbe(t *testing.T, handler http.Handler, path string, wantStatus int, wantBody string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != wantStatus || recorder.Body.String() != wantBody {
		t.Fatalf("%s response = %d %q; want %d %q", path, recorder.Code, recorder.Body.String(), wantStatus, wantBody)
	}
}

func assertProbeContains(t *testing.T, handler http.Handler, path string, wantStatus int, wantBodyPart string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != wantStatus || !strings.Contains(recorder.Body.String(), wantBodyPart) {
		t.Fatalf("%s response = %d %q; want %d containing %q", path, recorder.Code, recorder.Body.String(), wantStatus, wantBodyPart)
	}
}
