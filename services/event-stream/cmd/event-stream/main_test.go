package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestEventStreamCommandHealthReadyAndScopedRoutes(t *testing.T) {
	signer, verifier, _ := commandInternalPrincipalPair(t)
	readiness := workload.NewReadiness()
	reader := &commandEventReader{}
	idle := eventstream.NewIdleCoalescer(reader, time.Millisecond, nil)
	t.Cleanup(idle.Close)
	handler := buildHTTPHandler(readiness, eventstream.NewRouter(
		reader,
		verifier,
		eventstream.WithIdleCoalescer(idle),
		eventstream.WithStreamCompletedCheckLimit(1),
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

func TestEventStreamCommandStartupDoesNotReferenceBootstrapOrRawAPIKeyAuth(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(source)
	for _, forbidden := range []string{"RefreshBootstrap", "UpsertBootstrap", "ValidateBootstrapKey", "NewAPIKeyStore", "AuthenticateKey", "x-api-key", "InitializeSchema"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("services/event-stream/cmd/event-stream/main.go references forbidden startup token %q", forbidden)
		}
	}
	if strings.Contains(text, "OpenProtectedDSNFromEnv") {
		t.Fatal("event-stream must use its service-specific read-only database DSN env")
	}
	if !strings.Contains(text, envEventStreamDatabaseURL) {
		t.Fatalf("main.go missing %s", envEventStreamDatabaseURL)
	}
}

func TestEventStreamPanickingRequestStillWritesItsCompletedRecord(t *testing.T) {
	signer, verifier, _ := commandInternalPrincipalPair(t)
	var logs bytes.Buffer
	handler := eventstream.NewRouter(panickingEventReader{&commandEventReader{}}, verifier, eventstream.WithLogger(workload.NewLogger(&logs, "event-stream", "test", "unit")))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, commandSignedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events/stream?beta=true"))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500", recorder.Code)
	}
	if got := strings.Count(logs.String(), `"msg":"http.request.completed"`); got != 1 || !strings.Contains(logs.String(), `"http.response.status_code":500`) {
		t.Fatalf("completed records = %d; want one with status 500: %s", got, logs.String())
	}
}

type panickingEventReader struct{ *commandEventReader }

func (panickingEventReader) CurrentStreamPosition(context.Context, workspace.ID, string) (int64, error) {
	panic("reader failed")
}

type envMap map[string]string

func (m envMap) Getenv(key string) string { return m[key] }

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

func (*commandEventReader) ReadSessionSignals(_ context.Context, _ workspace.ID, sessionIDs []string) ([]eventstream.SessionSignal, error) {
	signals := make([]eventstream.SessionSignal, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		signals = append(signals, eventstream.SessionSignal{SessionID: sessionID, Exists: true, LifecycleState: "active"})
	}
	return signals, nil
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
	token, err := signer.Mint(auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID, Type: "workspace", Name: "Default"}, "ak_event_stream_command_test"), method, request.URL.Path, "req_event_stream_command_test", time.Minute)
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

func (*commandEventReader) ReadPreviewRequest(context.Context, workspace.ID, string, string, string, string) (eventstream.PreviewRequest, error) {
	return eventstream.PreviewRequest{}, nil
}
func (*commandEventReader) ListRequestFinalMessages(context.Context, eventstream.ReadScope, string, int64, int) ([]eventstream.RequestFinalMessage, error) {
	return nil, nil
}
