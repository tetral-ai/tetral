package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
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

func TestTetralAPICommandRunsWorkloadAfterApplicationBootstrap(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	var (
		eventsMu sync.Mutex
		events   []string
	)
	previousRunWorkload := runWorkload
	runWorkload = func(_ context.Context, config workload.Config) error {
		eventsMu.Lock()
		events = append(events, "listen:"+config.ListenConfigKey+":"+config.ListenAddress)
		eventsMu.Unlock()
		if config.ListenConfigKey != "TETRAL_API_HTTP_ADDR" && config.ListenConfigKey != "TETRAL_API_METRICS_ADDR" {
			t.Fatalf("ListenConfigKey = %q; want public or metrics key", config.ListenConfigKey)
		}
		if !config.Readiness.Ready() {
			t.Fatal("workload started before command readiness was marked ready")
		}
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	env := envMap{ //nolint:gosec // synthetic startup env values for command wiring test.
		"ENGINE_VAULT_KEY":                              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"ENGINE_DATA_DIR":                               secureCommandDataDir(t),
		"TETRAL_API_HTTP_ADDR":                          "127.0.0.1:4567",
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": commandInternalPrincipalPublicKey(t),
		"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF":       "artifact_default_test",
	}
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	err := run(context.Background(), env, func(ctx context.Context, config tetralapi.ProductionConfig) (*tetralapi.Application, error) {
		config.Open = func(context.Context) (tetralapi.StartupDatabase, error) {
			events = append(events, "open")
			client := &recordingCommandStartupClient{
				events:     &events,
				delegate:   dbconnect.NewClientForTesting(runtimeDB),
				rawDB:      adminDB,
				rawRuntime: runtimeDB,
			}
			return client.database(), nil
		}
		application, err := tetralapi.BuildProductionApplication(ctx, config)
		if err == nil {
			events = append(events, "bootstrap")
		}
		return application, err
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	for _, want := range []string{
		"open",
		"migrate",
		"role_verify",
		"bootstrap",
		"listen:TETRAL_API_HTTP_ADDR:127.0.0.1:4567",
		"listen:TETRAL_API_METRICS_ADDR::8081",
	} {
		if !stringSliceContains(events, want) {
			t.Fatalf("startup events = %v; missing %s", events, want)
		}
	}
	if len(events) != 6 {
		t.Fatalf("startup events = %v; want exactly 6 events", events)
	}
}

func TestTetralAPICommandStartupConfigLogsSafeFieldsAndStopsBeforeServing(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	previousRunWorkload := runWorkload
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("runWorkload must not be called after startup/config failure")
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	for _, test := range []struct {
		name      string
		env       envMap
		openCalls int
		events    string
		client    *recordingCommandStartupClient
	}{
		{name: "vault key", env: envMap{"ENGINE_VAULT_KEY": "short", "ENGINE_DATA_DIR": secureCommandDataDir(t)}},
		{name: "database", env: envMap{"ENGINE_VAULT_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "ENGINE_DATA_DIR": secureCommandDataDir(t)}, openCalls: 1},
		{name: "migrate", env: envMap{"ENGINE_VAULT_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "ENGINE_DATA_DIR": secureCommandDataDir(t)}, openCalls: 1, events: "open,migrate", client: &recordingCommandStartupClient{migrateErr: errors.New("schema migration failed for Secret/k8s-secret-startup-sentinel raw-mcp-auth-fragment")}},
		{name: "verify role", env: envMap{"ENGINE_VAULT_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "ENGINE_DATA_DIR": secureCommandDataDir(t)}, openCalls: 1, events: "open,migrate,role_verify", client: &recordingCommandStartupClient{verifyErr: errors.New("verify failed provider body bearer-startup-sentinel")}},
		{name: "runtime config", env: envMap{"ENGINE_VAULT_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "ENGINE_DATA_DIR": secureCommandDataDir(t), "TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": commandInternalPrincipalPublicKey(t), "TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST": "-1 credential-startup-sentinel"}, openCalls: 1, events: "open,migrate,role_verify"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stderr, finishCapture := captureCommandStderr(t)
			var events []string
			openCalls := 0
			err := run(context.Background(), test.env, func(ctx context.Context, config tetralapi.ProductionConfig) (*tetralapi.Application, error) {
				config.Open = func(context.Context) (tetralapi.StartupDatabase, error) {
					openCalls++
					events = append(events, "open")
					if test.name == "database" {
						return tetralapi.StartupDatabase{}, fmt.Errorf("TETRAL_DATABASE_URL is not set postgres://user:pass@db.internal/tetral")
					}
					runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
					client := test.client
					if client == nil {
						client = &recordingCommandStartupClient{}
					}
					client.events = &events
					client.delegate = dbconnect.NewClientForTesting(runtimeDB)
					client.rawDB = adminDB
					client.rawRuntime = runtimeDB
					return client.database(), nil
				}
				return tetralapi.BuildProductionApplication(ctx, config)
			})
			if err == nil {
				t.Fatal("run returned nil for invalid startup config")
			}
			finishCapture()
			if openCalls != test.openCalls {
				t.Fatalf("open calls = %d; want %d", openCalls, test.openCalls)
			}
			if test.events != "" && strings.Join(events, ",") != test.events {
				t.Fatalf("startup events = %v; want %s", events, test.events)
			}
			logOutput := stderr.String()
			for _, forbidden := range []string{"postgres://user:pass@db.internal/tetral", "k8s-secret-startup-sentinel", "raw-mcp-auth-fragment", "bearer-startup-sentinel", "credential-startup-sentinel"} {
				if strings.Contains(logOutput, forbidden) {
					t.Fatalf("startup log leaked %q: %s", forbidden, logOutput)
				}
			}
			if !strings.Contains(logOutput, `"msg":"startup.failed"`) ||
				!strings.Contains(logOutput, `"readiness.state":"not ready"`) ||
				!strings.Contains(logOutput, `"listener.state":"not started"`) {
				t.Fatalf("startup log missing safe fields for %s: %s", test.name, logOutput)
			}
		})
	}
}

// TestTetralAPICommandConfigStartupLogsConfigErrorWithSafeMessage proves a
// configuration-validation failure (a too-short vault key) is logged as
// error.class=config_error with the safe config key name in error.message_safe, while
// no key material leaks. Vault validation runs before the database opens, so this
// case needs no test database.
func TestTetralAPICommandConfigStartupLogsConfigErrorWithSafeMessage(t *testing.T) {
	previousRunWorkload := runWorkload
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("runWorkload must not be called after a config validation failure")
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	stderr, finishCapture := captureCommandStderr(t)
	vaultKeyMaterial := "tetral_sk_short_vault_key_sentinel"
	err := run(context.Background(), envMap{
		"ENGINE_VAULT_KEY": vaultKeyMaterial,
		"ENGINE_DATA_DIR":  secureCommandDataDir(t),
	}, tetralapi.BuildProductionApplication)
	if err == nil {
		t.Fatal("run returned nil for an invalid vault key")
	}
	finishCapture()
	logOutput := stderr.String()
	if !strings.Contains(logOutput, `"msg":"startup.failed"`) ||
		!strings.Contains(logOutput, `"error.class":"config_error"`) {
		t.Fatalf("startup log missing config_error classification: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"error.message_safe":`) || !strings.Contains(logOutput, "ENGINE_VAULT_KEY") {
		t.Fatalf("startup log missing safe ENGINE_VAULT_KEY message: %s", logOutput)
	}
	if strings.Contains(logOutput, `"error.message":`) {
		t.Fatalf("startup log still emitted legacy error.message: %s", logOutput)
	}
	if strings.Contains(logOutput, vaultKeyMaterial) {
		t.Fatalf("startup log leaked vault key material: %s", logOutput)
	}
}

// TestTetralAPICommandConfigStartupLogsNonHexVaultKeyWithoutLeak (encryptor
// no-leak lock) proves a 64-character-but-non-hex ENGINE_VAULT_KEY is logged as
// error.class=config_error naming ENGINE_VAULT_KEY, while neither the key material
// nor the raw hex.DecodeString error ("invalid hex key" / the offending byte) ever
// reaches the log. Without the up-front hex check in ValidateVaultKey, this key
// passed validation and failed later inside encryption.NewAES256GCMEncryptor,
// which echoes the offending byte.
func TestTetralAPICommandConfigStartupLogsNonHexVaultKeyWithoutLeak(t *testing.T) {
	previousRunWorkload := runWorkload
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("runWorkload must not be called after a config validation failure")
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	stderr, finishCapture := captureCommandStderr(t)
	// 64 characters, non-hex (contains 'z' and '_'), with a secret-shaped sentinel
	// in the value position.
	nonHexVaultKey := "tetralsk_zz_vault_sentinel_0000000000000000000000000000000000000"
	if len(nonHexVaultKey) != 64 {
		t.Fatalf("test setup: nonHexVaultKey length = %d; want 64", len(nonHexVaultKey))
	}
	err := run(context.Background(), envMap{
		"ENGINE_VAULT_KEY": nonHexVaultKey,
		"ENGINE_DATA_DIR":  secureCommandDataDir(t),
	}, tetralapi.BuildProductionApplication)
	if err == nil {
		t.Fatal("run returned nil for a 64-character non-hex vault key")
	}
	finishCapture()
	logOutput := stderr.String()
	if !strings.Contains(logOutput, `"msg":"startup.failed"`) ||
		!strings.Contains(logOutput, `"error.class":"config_error"`) {
		t.Fatalf("startup log missing config_error classification: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"error.message_safe":`) || !strings.Contains(logOutput, "ENGINE_VAULT_KEY") {
		t.Fatalf("startup log missing safe ENGINE_VAULT_KEY message: %s", logOutput)
	}
	if strings.Contains(logOutput, `"error.message":`) {
		t.Fatalf("startup log still emitted legacy error.message: %s", logOutput)
	}
	for _, forbidden := range []string{nonHexVaultKey, "tetralsk_zz_vault_sentinel", "invalid hex key"} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("startup log leaked vault key material or raw hex error fragment %q: %s", forbidden, logOutput)
		}
	}
}

// TestTetralAPICommandDataDirRejectionLogsNoPathLeak (P1 L1) drives the data-dir
// mode rejection through the real startup path with ENGINE_DATA_DIR pointing at a
// pre-existing group-accessible directory whose name carries a secret-shaped
// sentinel, and asserts the sentinel/raw path is absent from the startup.failed
// log error.message_safe AND the final stderr line, while the message still classifies
// config_error and names ENGINE_DATA_DIR + the mode (L2). The data-dir check runs
// inside BuildRouter after the database opens, so this case needs a test database.
func TestTetralAPICommandDataDirRejectionLogsNoPathLeak(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	previousRunWorkload := runWorkload
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("runWorkload must not be called after a config validation failure")
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	parent := t.TempDir()
	dataDir := filepath.Join(parent, "tetral_sk_data_dir_command_sentinel")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if err := os.Chmod(dataDir, 0o755); err != nil { //nolint:gosec // G302: deliberately group/other accessible to drive the rejection.
		t.Fatalf("chmod 0755: %v", err)
	}

	stderr, finishCapture := captureCommandStderr(t)
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	err := run(context.Background(), envMap{
		"ENGINE_VAULT_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"ENGINE_DATA_DIR":  dataDir,
	}, func(ctx context.Context, config tetralapi.ProductionConfig) (*tetralapi.Application, error) {
		config.Open = func(context.Context) (tetralapi.StartupDatabase, error) {
			return (&recordingCommandStartupClient{
				delegate:   dbconnect.NewClientForTesting(runtimeDB),
				rawDB:      adminDB,
				rawRuntime: runtimeDB,
				events:     &[]string{},
			}).database(), nil
		}
		return tetralapi.BuildProductionApplication(ctx, config)
	})
	if err == nil {
		t.Fatal("run returned nil for a group-accessible data directory")
	}
	finishCapture()
	logOutput := stderr.String()
	if !strings.Contains(logOutput, `"error.class":"config_error"`) ||
		!strings.Contains(logOutput, "ENGINE_DATA_DIR") || !strings.Contains(logOutput, "0755") {
		t.Fatalf("data-dir startup log missing config_error/ENGINE_DATA_DIR/mode: %s", logOutput)
	}
	for _, forbidden := range []string{dataDir, "tetral_sk_data_dir_command_sentinel", parent} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("data-dir startup log leaked the configured path fragment %q: %s", forbidden, logOutput)
		}
	}
}

func TestTetralAPICommandStartupFailureLogRedactsDependencyErrors(t *testing.T) {
	stderr, finishCapture := captureCommandStderr(t)
	sentinel := "postgres://user:secret@db.internal/tetral bearer_token=secret raw provider body" //nolint:gosec // G101: synthetic leak sentinel for startup redaction tests.
	env := envMap{
		"ENGINE_VAULT_KEY": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"ENGINE_DATA_DIR":  secureCommandDataDir(t),
	}
	err := run(context.Background(), env, func(context.Context, tetralapi.ProductionConfig) (*tetralapi.Application, error) {
		return nil, errors.New(sentinel)
	})
	if err == nil {
		t.Fatal("run returned nil for dependency failure")
	}
	finishCapture()
	logOutput := stderr.String()
	if strings.Contains(logOutput, sentinel) || strings.Contains(logOutput, "secret") || strings.Contains(logOutput, "postgres://") || strings.Contains(logOutput, "provider body") {
		t.Fatalf("startup log leaked sentinel material: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"msg":"startup.failed"`) || !strings.Contains(logOutput, `"error.class":"startup_error"`) {
		t.Fatalf("startup log missing safe classification: %s", logOutput)
	}
	if strings.Contains(logOutput, `"error.message"`) {
		t.Fatalf("dependency-failure startup log must not carry message fields: %s", logOutput)
	}
}

func TestTetralAPICommandListenerFailureLogRedactsMalformedAddress(t *testing.T) {
	stderr, finishCapture := captureCommandStderr(t)
	// A credential-bearing listen address (userinfo "@") is the shape precise
	// redaction must mask. The embedded tetral_sk_ sentinel must never reach the log.
	sentinelAddress := "tetral_sk_abcdefghijklmnopqrstuvwxyz123456@db.internal:5432"
	env := envMap{
		"ENGINE_VAULT_KEY":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"ENGINE_DATA_DIR":      secureCommandDataDir(t),
		"TETRAL_API_HTTP_ADDR": sentinelAddress,
	}
	err := run(context.Background(), env, func(context.Context, tetralapi.ProductionConfig) (*tetralapi.Application, error) {
		return &tetralapi.Application{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})}, nil
	})
	if err == nil {
		t.Fatal("run returned nil for malformed listen address")
	}
	finishCapture()
	logOutput := stderr.String()
	for _, forbidden := range []string{"tetral_sk_", sentinelAddress} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("listener failure log leaked %q: %s", forbidden, logOutput)
		}
	}
	if !strings.Contains(logOutput, `"msg":"workload.listener.failed"`) ||
		!strings.Contains(logOutput, `"listener.transport":"tcp"`) {
		t.Fatalf("listener failure log missing safe fields: %s", logOutput)
	}
	if strings.Contains(logOutput, `"listener.address"`) {
		t.Fatalf("listener failure log must not expose endpoint fields: %s", logOutput)
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

func commandInternalPrincipalPublicKey(t *testing.T) string {
	t.Helper()
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate test internal-principal key: %v", err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("build test internal-principal signer: %v", err)
	}
	return signer.PublicKeyBase64()
}

type envMap map[string]string

func (e envMap) Getenv(key string) string { return e[key] }

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type recordingCommandStartupClient struct {
	events     *[]string
	delegate   *dbconnect.Client
	rawDB      *sql.DB
	rawRuntime *sql.DB
	migrateErr error
	verifyErr  error
}

func (c *recordingCommandStartupClient) database() tetralapi.StartupDatabase {
	return tetralapi.StartupDatabase{
		OpenResult: dbconnect.OpenResult{
			Client:                       c.delegate,
			RawDatabaseForExcludedStores: c.rawDB,
		},
		Client: c,
	}
}

func (c *recordingCommandStartupClient) MigrateSchema(ctx context.Context) error {
	*c.events = append(*c.events, "migrate")
	if c.migrateErr != nil {
		return c.migrateErr
	}
	return nil
}

func (c *recordingCommandStartupClient) VerifyRuntimeRole(ctx context.Context) error {
	*c.events = append(*c.events, "role_verify")
	if c.verifyErr != nil {
		return c.verifyErr
	}
	return nil
}

func (c *recordingCommandStartupClient) Close() error {
	if c.rawRuntime != nil {
		return c.rawRuntime.Close()
	}
	return nil
}

func captureCommandStderr(t *testing.T) (*bytes.Buffer, func()) {
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
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		_ = writeEnd.Close()
		os.Stderr = previous
		<-done
		_ = readEnd.Close()
	}
	t.Cleanup(finish)
	return &buffer, finish
}
