package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestEventStreamCommandHealthReadyAndScopedRoutes(t *testing.T) {
	signer, verifier, _ := commandInternalPrincipalPair(t)
	readiness := workload.NewReadiness()
	handler := buildHTTPHandler(readiness, eventstream.NewRouter(
		&commandEventReader{},
		verifier,
		eventstream.WithStreamPollInterval(time.Millisecond),
		eventstream.WithStreamMaxEmptyPolls(1),
	))

	assertProbe(t, handler, "/health", http.StatusOK, "ok")
	assertProbe(t, handler, "/ready", http.StatusServiceUnavailable, "not ready")
	readiness.MarkReady()
	assertProbe(t, handler, "/ready", http.StatusOK, "ready")
	readiness.BeginShutdown()
	assertProbe(t, handler, "/ready", http.StatusServiceUnavailable, "shutting down")
	assertProbe(t, handler, "/metrics", http.StatusNotFound, "404 page not found")

	request := commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events/stream?beta=true")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("event stream route status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}

	threadEvents := commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/threads/thr_events/stream?beta=true")
	threadRecorder := httptest.NewRecorder()
	handler.ServeHTTP(threadRecorder, threadEvents)
	if threadRecorder.Code != http.StatusOK {
		t.Fatalf("thread event stream route status = %d; want 200 body=%s", threadRecorder.Code, threadRecorder.Body.String())
	}

	listEvents := commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events")
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listEvents)
	if listRecorder.Code != http.StatusNotFound {
		t.Fatalf("event list route status = %d; want 404 body=%s", listRecorder.Code, listRecorder.Body.String())
	}

	threadListEvents := commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/threads/thr_events/events")
	threadListRecorder := httptest.NewRecorder()
	handler.ServeHTTP(threadListRecorder, threadListEvents)
	if threadListRecorder.Code != http.StatusNotFound {
		t.Fatalf("thread event list route status = %d; want 404 body=%s", threadListRecorder.Code, threadListRecorder.Body.String())
	}

	postEvent := commandSignedRequest(t, signer, http.MethodPost, "/v1/sessions/sesn_events/events/stream")
	postRecorder := httptest.NewRecorder()
	handler.ServeHTTP(postRecorder, postEvent)
	if postRecorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST event stream route status = %d; want 405 body=%s", postRecorder.Code, postRecorder.Body.String())
	}

	sessions := commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions")
	sessionsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(sessionsRecorder, sessions)
	if sessionsRecorder.Code != http.StatusNotFound {
		t.Fatalf("mature sessions route status = %d; want 404 body=%s", sessionsRecorder.Code, sessionsRecorder.Body.String())
	}
}

func TestEventStreamCommandConfigDefaultsAndPrincipalVerifier(t *testing.T) {
	_, _, publicKey := commandInternalPrincipalPair(t)
	cfg, err := configFromEnv(envMap{envInternalPrincipalPublicKey: publicKey})
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	if cfg.ListenAddress != ":8080" || cfg.MetricsAddress != ":8081" || cfg.DeploymentEnvironment != "local" || cfg.ServiceVersion != "unknown" || cfg.PrincipalVerifier == nil {
		t.Fatalf("defaults = %+v; want :8080 local unknown with verifier", cfg)
	}
	explicit, err := configFromEnv(envMap{
		envHTTPAddress:                  "127.0.0.1:18082",
		envMetricsAddress:               "127.0.0.1:18083",
		"TETRAL_DEPLOYMENT_ENVIRONMENT": "test",
		"TETRAL_SERVICE_VERSION":        "unit",
		envInternalPrincipalPublicKey:   publicKey,
	})
	if err != nil {
		t.Fatalf("explicit configFromEnv: %v", err)
	}
	if explicit.ListenAddress != "127.0.0.1:18082" || explicit.MetricsAddress != "127.0.0.1:18083" || explicit.DeploymentEnvironment != "test" || explicit.ServiceVersion != "unit" || explicit.PrincipalVerifier == nil {
		t.Fatalf("explicit config = %+v", explicit)
	}
	if _, err := configFromEnv(envMap{
		envHTTPAddress:                "127.0.0.1:18082",
		envMetricsAddress:             "127.0.0.1:18082",
		envInternalPrincipalPublicKey: publicKey,
	}); err == nil {
		t.Fatal("configFromEnv accepted matching public and metrics addresses")
	}
}

func TestEventStreamCommandConfigRequiresInternalPrincipalPublicKey(t *testing.T) {
	if _, err := configFromEnv(envMap{}); err == nil || !strings.Contains(err.Error(), envInternalPrincipalPublicKey+" is required") {
		t.Fatalf("missing public key err = %v; want required config error", err)
	}
	if _, err := configFromEnv(envMap{envInternalPrincipalPublicKey: "not-base64-secret-value"}); err == nil || !strings.Contains(err.Error(), "internal principal public key must be base64") {
		t.Fatalf("invalid public key err = %v; want safe base64 config error", err)
	}
}

func TestEventStreamCommandConfigFailureLogsSharedFields(t *testing.T) {
	previousRunWorkload := runWorkload
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("runWorkload must not start after config failure")
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	stderr, finish := captureStderr(t)
	err := run(context.Background(), envMap{}, nil)
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"event-stream"`,
		`"component":"event-stream"`,
		`"error.class":"config_error"`,
		envInternalPrincipalPublicKey,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

func TestEventStreamCommandStartupDoesNotReferenceBootstrapOrRawAPIKeyAuth(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(source)
	for _, forbidden := range []string{"RefreshBootstrap", "UpsertBootstrap", "ValidateBootstrapKey", "NewAPIKeyStore", "StoreAuthenticator", "x-api-key", "InitializeSchema"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("services/event-stream/cmd/event-stream/main.go references forbidden startup token %q", forbidden)
		}
	}
	if strings.Contains(text, "OpenPlainDSNFromEnv") {
		t.Fatal("event-stream must use its service-specific read-only database DSN env")
	}
	if !strings.Contains(text, envEventStreamDatabaseURL) {
		t.Fatalf("main.go missing %s", envEventStreamDatabaseURL)
	}
}

func TestEventStreamCommandBootsReadyWithoutBootstrapKey(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	signer, _, publicKey := commandInternalPrincipalPair(t)
	var (
		eventsMu sync.Mutex
		events   []string
	)
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	previousRunWorkload := runWorkload
	runWorkload = func(_ context.Context, config workload.Config) error {
		eventsMu.Lock()
		events = append(events, "listen:"+config.ListenConfigKey+":"+config.ListenAddress)
		eventsMu.Unlock()
		if config.ServiceName != "event-stream" || (config.ListenConfigKey != envHTTPAddress && config.ListenConfigKey != envMetricsAddress) {
			t.Fatalf("workload config = %+v", config)
		}
		if !config.Readiness.Ready() {
			t.Fatal("workload started before readiness was marked ready")
		}
		request := commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_missing/events")
		recorder := httptest.NewRecorder()
		config.Handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("event list route status = %d; want 404 for missing session body=%s", recorder.Code, recorder.Body.String())
		}
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	// No ENGINE_API_KEY env: event-stream authenticates only the signed
	// principal produced by auth/Edge.
	err := run(context.Background(), envMap{
		"TETRAL_EVENT_STREAM_HTTP_ADDR": "127.0.0.1:18082",
		"TETRAL_DEPLOYMENT_ENVIRONMENT": "test",
		"TETRAL_SERVICE_VERSION":        "unit",
		envInternalPrincipalPublicKey:   publicKey,
	}, func(context.Context) (startupDatabase, error) {
		eventsMu.Lock()
		events = append(events, "open")
		eventsMu.Unlock()
		client := &recordingStartupClient{
			events:     &events,
			delegate:   dbconnect.NewClientForTesting(runtimeDB),
			rawDB:      adminDB,
			rawRuntime: runtimeDB,
		}
		return client.database(), nil
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	for _, want := range []string{
		"open",
		"schema_verify",
		"role_verify",
		"listen:" + envHTTPAddress + ":127.0.0.1:18082",
		"listen:" + envMetricsAddress + "::8081",
	} {
		if !stringSliceContains(events, want) {
			t.Fatalf("startup events = %v; missing %s", events, want)
		}
	}
	if len(events) != 5 {
		t.Fatalf("startup events = %v; want exactly 5 events", events)
	}
}

func TestEventStreamCommandStartupFailuresStopBeforeServingAndRedactLogs(t *testing.T) {
	_, _, publicKey := commandInternalPrincipalPair(t)
	previousRunWorkload := runWorkload
	runWorkload = func(context.Context, workload.Config) error {
		t.Fatal("runWorkload must not start after startup failure")
		return nil
	}
	t.Cleanup(func() { runWorkload = previousRunWorkload })

	for _, test := range []struct {
		name string
		env  envMap
		open openStartupFunc
	}{
		{
			name: "database open",
			env:  envMap{envInternalPrincipalPublicKey: publicKey},
			open: func(context.Context) (startupDatabase, error) {
				return startupDatabase{}, errors.New("TETRAL_DATABASE_URL postgres://user:secret@db.internal/tetral raw provider body")
			},
		},
		{
			name: "schema behind",
			env:  envMap{envInternalPrincipalPublicKey: publicKey},
			open: func(context.Context) (startupDatabase, error) {
				return (&recordingStartupClient{schemaErr: &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}}).database(), nil
			},
		},
		{
			name: "verify role",
			env:  envMap{envInternalPrincipalPublicKey: publicKey},
			open: func(context.Context) (startupDatabase, error) {
				return (&recordingStartupClient{verifyErr: errors.New("role verification failed Secret/raw-mcp-auth-fragment")}).database(), nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stderr, finish := captureStderr(t)
			err := run(context.Background(), test.env, test.open)
			if err == nil {
				t.Fatal("run returned nil for startup failure")
			}
			finish()
			output := stderr.String()
			for _, forbidden := range []string{"postgres://", "secret@db.internal", "raw provider body", "Secret/raw-mcp-auth-fragment"} {
				if strings.Contains(output, forbidden) {
					t.Fatalf("startup log leaked %q: %s", forbidden, output)
				}
			}
			if !strings.Contains(output, `"msg":"startup.failed"`) ||
				!strings.Contains(output, `"readiness.state":"not ready"`) ||
				!strings.Contains(output, `"listener.state":"not started"`) {
				t.Fatalf("startup log missing safe fields: %s", output)
			}
			if !strings.Contains(output, `"error.class":"startup_error"`) {
				t.Fatalf("startup log missing startup_error classification: %s", output)
			}
			if strings.Contains(output, `"error.message"`) {
				t.Fatalf("dependency startup log must not carry message fields: %s", output)
			}
		})
	}
}

type envMap map[string]string

func (m envMap) Getenv(key string) string { return m[key] }

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type commandEventReader struct{}

func (*commandEventReader) CurrentStreamPosition(context.Context, workspace.ID, string) (int64, error) {
	return 0, nil
}

func (*commandEventReader) CurrentThreadStreamPosition(context.Context, workspace.ID, string, string) (int64, error) {
	return 0, nil
}

func (*commandEventReader) ListSessionEventChanges(context.Context, workspace.ID, string, int64, int) ([]eventstream.StreamChange, error) {
	return nil, nil
}

func (*commandEventReader) ListThreadEventChanges(context.Context, workspace.ID, string, string, int64, int) ([]eventstream.StreamChange, error) {
	return nil, nil
}

func commandInternalPrincipalPair(t *testing.T) (*auth.InternalPrincipalSigner, *auth.InternalPrincipalVerifier, string) {
	t.Helper()
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate test internal-principal key: %v", err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("build test internal-principal signer: %v", err)
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatalf("build test internal-principal verifier: %v", err)
	}
	return signer, verifier, signer.PublicKeyBase64()
}

func commandSignedRequest(t *testing.T, signer *auth.InternalPrincipalSigner, method string, target string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, target, nil)
	token, err := signer.Mint(auth.Principal{ //nolint:gosec // Test principal token fixture.
		Workspace: workspace.Workspace{ID: workspace.DefaultID, Type: "workspace", Name: "Default"},
		APIKeyID:  "ak_event_stream_command_test", //nolint:gosec // Test principal id, not a secret.
	}, method, request.URL.Path, "req_event_stream_command_test", time.Minute)
	if err != nil {
		t.Fatalf("mint principal: %v", err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	return request
}

func assertProbe(t *testing.T, handler http.Handler, path string, wantStatus int, wantBody string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != wantStatus || recorder.Body.String() != wantBody+"\n" {
		t.Fatalf("%s response = %d %q; want %d %q", path, recorder.Code, recorder.Body.String(), wantStatus, wantBody+"\n")
	}
}

type recordingStartupClient struct {
	events     *[]string
	delegate   *dbconnect.Client
	rawDB      *sql.DB
	rawRuntime *sql.DB
	verifyErr  error
	schemaErr  error
}

func (c *recordingStartupClient) database() startupDatabase {
	return startupDatabase{runtimeClient: c.delegate, readinessClient: c}
}

func (c *recordingStartupClient) VerifySchema(context.Context) error {
	if c.events != nil {
		*c.events = append(*c.events, "schema_verify")
	}
	return c.schemaErr
}

func (c *recordingStartupClient) VerifyRuntimeRole(context.Context) error {
	if c.events != nil {
		*c.events = append(*c.events, "role_verify")
	}
	return c.verifyErr
}

func (c *recordingStartupClient) Close() error {
	if c.rawRuntime != nil {
		return c.rawRuntime.Close()
	}
	return nil
}

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
