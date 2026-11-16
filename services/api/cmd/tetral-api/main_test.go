package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
	tetralapi "github.com/tetral-ai/tetral/services/api"
)

func TestTetralAPICommandHealthReadyProbes(t *testing.T) {
	readiness := workload.NewReadiness()
	handler := tetralapi.BuildHTTPHandler(readiness, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	assertCommandProbe(t, handler, "/health", http.StatusOK, "ok")
	assertCommandProbe(t, handler, "/ready", http.StatusServiceUnavailable, "not ready")
	readiness.MarkReady()
	assertCommandProbe(t, handler, "/ready", http.StatusOK, "ready")
	readiness.BeginShutdown()
	assertCommandProbe(t, handler, "/ready", http.StatusServiceUnavailable, "shutting down")
	assertCommandProbe(t, handler, "/metrics", http.StatusNotFound, "404 page not found")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/__fallback_probe__", nil))
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("api route status = %d; want fallback API handler", recorder.Code)
	}
}

func TestTetralAPICommandListenAddressConfig(t *testing.T) {
	valid := envMap{
		"ENGINE_VAULT_KEY":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"ENGINE_DATA_DIR":      secureCommandDataDir(t),
		"TETRAL_API_HTTP_ADDR": "127.0.0.1:4567",
	}
	cfg, err := tetralapi.ConfigFromEnv(valid)
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	if cfg.ListenAddress != "127.0.0.1:4567" || cfg.MetricsAddress != ":8081" || cfg.DeploymentEnvironment != "local" || cfg.ServiceVersion != "unknown" {
		t.Fatalf("config = %+v; want explicit listen address and identity defaults", cfg)
	}

	legacy := envMap{
		"ENGINE_VAULT_KEY": valid["ENGINE_VAULT_KEY"],
		"ENGINE_DATA_DIR":  secureCommandDataDir(t),
		"ENGINE_PORT":      "9090",
	}
	cfg, err = tetralapi.ConfigFromEnv(legacy)
	if err != nil {
		t.Fatalf("legacy configFromEnv: %v", err)
	}
	if cfg.ListenAddress != ":9090" {
		t.Fatalf("legacy listen address = %q; want :9090", cfg.ListenAddress)
	}
	explicitMetrics := envMap{
		"ENGINE_VAULT_KEY":        valid["ENGINE_VAULT_KEY"],
		"ENGINE_DATA_DIR":         secureCommandDataDir(t),
		"TETRAL_API_HTTP_ADDR":    "127.0.0.1:4567",
		"TETRAL_API_METRICS_ADDR": "127.0.0.1:4568",
	}
	cfg, err = tetralapi.ConfigFromEnv(explicitMetrics)
	if err != nil {
		t.Fatalf("explicit metrics configFromEnv: %v", err)
	}
	if cfg.MetricsAddress != "127.0.0.1:4568" {
		t.Fatalf("metrics listen address = %q; want 127.0.0.1:4568", cfg.MetricsAddress)
	}
	_, err = tetralapi.ConfigFromEnv(envMap{
		"ENGINE_VAULT_KEY":        valid["ENGINE_VAULT_KEY"],
		"ENGINE_DATA_DIR":         secureCommandDataDir(t),
		"TETRAL_API_HTTP_ADDR":    "127.0.0.1:4567",
		"TETRAL_API_METRICS_ADDR": "127.0.0.1:4567",
	})
	if err == nil {
		t.Fatal("configFromEnv accepted matching public and metrics addresses")
	}
}

func TestTetralAPICommandPassesPublicAPIConfigToApplicationBootstrap(t *testing.T) {
	env := envMap{
		"ENGINE_VAULT_KEY":                "vault-key-owned-by-tetralapi",
		"ENGINE_DATA_DIR":                 secureCommandDataDir(t),
		"TETRAL_DEPLOYMENT_ENVIRONMENT":   "test",
		"TETRAL_SERVICE_VERSION":          "unit",
		"TETRAL_API_HTTP_ADDR":            "127.0.0.1:bad",
		"UNRELATED_PUBLIC_API_CONFIG_KEY": "kept",
	}
	err := run(context.Background(), env, func(_ context.Context, config tetralapi.ProductionConfig) (*tetralapi.Application, error) {
		if config.VaultKey != env["ENGINE_VAULT_KEY"] || config.DataDir != env["ENGINE_DATA_DIR"] {
			t.Fatalf("application config = %+v; want api-owned env values", config)
		}
		if config.Env == nil {
			t.Fatal("application config missing env reader")
		}
		return &tetralapi.Application{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})}, nil
	})
	if err == nil {
		t.Fatal("run returned nil for malformed listen address")
	}
}

func assertCommandProbe(t *testing.T, handler http.Handler, path string, wantStatus int, wantBody string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != wantStatus || recorder.Body.String() != wantBody+"\n" {
		t.Fatalf("%s response = %d %q; want %d %q", path, recorder.Code, recorder.Body.String(), wantStatus, wantBody+"\n")
	}
}

func secureCommandDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // directory must be owner-only, not 0600
		t.Fatalf("chmod secure data dir: %v", err)
	}
	return dir
}

type envMap map[string]string

func (e envMap) Getenv(key string) string { return e[key] }
