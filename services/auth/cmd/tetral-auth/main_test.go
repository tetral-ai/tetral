package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralauth "github.com/tetral-ai/tetral/services/auth"
)

func TestTetralAuthCommandPublicHandlerDoesNotExposeMetrics(t *testing.T) {
	readiness := workload.NewReadiness()
	handler := buildHTTPHandler(readiness, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	assertProbe(t, handler, "/health", http.StatusOK, "ok")
	assertProbe(t, handler, "/ready", http.StatusServiceUnavailable, "not ready")
	readiness.MarkReady()
	assertProbe(t, handler, "/ready", http.StatusOK, "ready")
	assertProbe(t, handler, "/metrics", http.StatusNotFound, "404 page not found")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/__fallback_probe__", nil))
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("api route status = %d; want fallback auth handler", recorder.Code)
	}
}

func TestTetralAuthConfigAndRunnerExposeMetricsOnSeparateListener(t *testing.T) {
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	cfg, err := tetralauth.ConfigFromEnv(envMap{ //nolint:gosec // synthetic startup env values.
		tetralauth.EnvHTTPAddress:                    "127.0.0.1:18080",
		tetralauth.EnvMetricsAddress:                 "127.0.0.1:18081",
		tetralauth.EnvBootstrapWorkspaceID:           "default",
		tetralauth.EnvBootstrapAPIKey:                strings.Repeat("a", auth.MinBootstrapKeyBytes),
		tetralauth.EnvInternalPrincipalPrivateKeyB64: privateKey,
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.InternalPrincipalTTL != 60*time.Second || cfg.DeploymentEnvironment != "local" || cfg.ServiceVersion != "unknown" {
		t.Fatalf("boot defaults = %+v", cfg)
	}
	if cfg.HTTPAddress != "127.0.0.1:18080" || cfg.MetricsAddress != "127.0.0.1:18081" {
		t.Fatalf("listen config = %+v", cfg)
	}
	if _, err := tetralauth.ConfigFromEnv(envMap{ //nolint:gosec // synthetic startup env values.
		tetralauth.EnvHTTPAddress:                    "127.0.0.1:18080",
		tetralauth.EnvMetricsAddress:                 "127.0.0.1:18080",
		tetralauth.EnvBootstrapWorkspaceID:           "default",
		tetralauth.EnvBootstrapAPIKey:                strings.Repeat("a", auth.MinBootstrapKeyBytes),
		tetralauth.EnvInternalPrincipalPrivateKeyB64: privateKey,
	}); err == nil {
		t.Fatal("ConfigFromEnv accepted matching public and metrics addresses")
	}

	readiness := workload.NewReadiness()
	readiness.MarkReady()
	var (
		mu     sync.Mutex
		events []string
		tlsBy  = map[string]*tls.Config{}
	)
	previousRunWorkload := runWorkload
	runWorkload = func(_ context.Context, config workload.Config) error {
		mu.Lock()
		events = append(events, config.ListenConfigKey+":"+config.ListenAddress)
		tlsBy[config.ListenConfigKey] = config.TLSConfig
		mu.Unlock()
		if config.ListenConfigKey != tetralauth.EnvHTTPAddress && config.ListenConfigKey != tetralauth.EnvMetricsAddress {
			t.Fatalf("ListenConfigKey = %q; want public or metrics key", config.ListenConfigKey)
		}
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	// The public listener's TLS comes only from the bound native transport; the
	// metrics listener never serves it.
	publicTLS := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "auth-public-sentinel"}
	err = runPublicAndMetricsHTTP(
		context.Background(),
		cfg,
		readiness,
		workload.NewLogger(nil, "auth", cfg.DeploymentEnvironment, cfg.ServiceVersion),
		http.NotFoundHandler(),
		workload.HealthRouter(readiness),
		boundHTTPListeners{tls: publicTLS, operationMetrics: workload.NewOperationMetrics("auth")},
	)
	if err != nil {
		t.Fatalf("runPublicAndMetricsHTTP: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, want := range []string{
		tetralauth.EnvHTTPAddress + ":127.0.0.1:18080",
		tetralauth.EnvMetricsAddress + ":127.0.0.1:18081",
	} {
		if !stringSliceContains(events, want) {
			t.Fatalf("events = %v; missing %s", events, want)
		}
	}
	if tlsBy[tetralauth.EnvHTTPAddress] != publicTLS {
		t.Fatalf("public listener TLS = %p; want the bound transport config %p", tlsBy[tetralauth.EnvHTTPAddress], publicTLS)
	}
	if tlsBy[tetralauth.EnvMetricsAddress] != nil {
		t.Fatal("metrics listener received the public TLS config")
	}
}

func TestTetralAuthCommandStartupFailureLogUsesSharedFields(t *testing.T) {
	stderr, finish := captureStderr(t)
	err := run(context.Background(), envMap{})
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"auth"`,
		`"component":"auth"`,
		`"error.class":"config_error"`,
		tetralauth.EnvBootstrapWorkspaceID,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type envMap map[string]string

func (m envMap) Getenv(key string) string { return m[key] }

func captureStderr(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	previous := os.Stderr
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = writeEnd
	var buffer bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buffer.ReadFrom(readEnd)
		close(done)
	}()
	finish := func() {
		_ = writeEnd.Close()
		os.Stderr = previous
		<-done
		_ = readEnd.Close()
	}
	t.Cleanup(func() {
		if os.Stderr == writeEnd {
			finish()
		}
	})
	return &buffer, finish
}

func assertProbe(t *testing.T, handler http.Handler, path string, wantStatus int, wantBody string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != wantStatus || recorder.Body.String() != wantBody+"\n" {
		t.Fatalf("%s response = %d %q; want %d %q", path, recorder.Code, recorder.Body.String(), wantStatus, wantBody+"\n")
	}
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
