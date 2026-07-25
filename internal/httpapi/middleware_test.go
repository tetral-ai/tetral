package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// newTestLogger builds a slog logger that captures JSON log lines into buffer,
// matching the production JSON handler so tests assert real emitted shape.
func newTestLogger(buffer *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buffer, nil)).With(
		slog.String("service.name", "api"),
		slog.String("service.version", "test"),
		slog.String("deployment.environment", "test"),
	)
}

func testAuthenticator(apiKey string) auth.Authenticator {
	return auth.AuthenticatorFunc(func(_ context.Context, rawKey string) (auth.Principal, error) {
		if rawKey != apiKey {
			return auth.Principal{}, &auth.AuthenticationError{Message: "invalid api key"}
		}
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID, Type: "workspace", Name: "Default"}, APIKeyID: "ak_test"}, nil
	})
}

func TestRequestIDMiddlewareInjectsUniquePrefixedIDs(t *testing.T) {
	var ids []string
	handler := RequestIDMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		ids = append(ids, RequestIDFromContext(r.Context()))
	}))

	first := httptest.NewRecorder()
	second := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/", nil))
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/", nil))

	if len(ids) != 2 || ids[0] == "" || ids[1] == "" || ids[0] == ids[1] {
		t.Fatalf("request ids = %#v; want two unique ids", ids)
	}
	for _, id := range ids {
		if !strings.HasPrefix(id, "req_") {
			t.Fatalf("request id %q missing req_ prefix", id)
		}
	}
	if got := first.Header().Get("request-id"); got != ids[0] {
		t.Fatalf("first response request-id = %q; want %q", got, ids[0])
	}
	if got := second.Header().Get("request-id"); got != ids[1] {
		t.Fatalf("second response request-id = %q; want %q", got, ids[1])
	}
}

func TestRequestLoggingSuppressesOrdinarySuccessByDefault(t *testing.T) {
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	metrics := &recordingRequestMetrics{}
	handler := RequestIDMiddleware(RequestLogMiddleware(logger, time.Hour, WithRequestLogMetrics(metrics))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/sessions", nil))

	if buffer.String() != "" {
		t.Fatalf("ordinary 2xx request logged by default: %s", buffer.String())
	}
	if metrics.calls != 1 || metrics.method != http.MethodGet || metrics.statusCode != http.StatusNoContent {
		t.Fatalf("request metrics = %+v; want one observed 204 GET", metrics)
	}
}

func TestRequestLoggingEmitsServerErrorAndSlowRequests(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		threshold time.Duration
		wantKind  string
		wantError bool
	}{
		{name: "server error", status: http.StatusInternalServerError, threshold: time.Hour, wantKind: "server_error", wantError: true},
		{name: "slow", status: http.StatusOK, threshold: 0, wantKind: "slow_request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			logger := newTestLogger(&buffer)
			handler := RequestIDMiddleware(RequestLogMiddleware(logger, test.threshold)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
			})))

			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/sessions", nil))

			fields := decodeHTTPLogLine(t, buffer.String())
			if fields["msg"] != "http.request" ||
				fields["operation"] != "http.request" ||
				fields["event.kind"] != test.wantKind ||
				fields["component"] != "http-api" ||
				fields["http.request.method"] != http.MethodPost {
				t.Fatalf("request log fields = %#v", fields)
			}
			if fields["http.response.status_code"].(float64) != float64(test.status) {
				t.Fatalf("status field = %#v; want %d", fields["http.response.status_code"], test.status)
			}
			if _, ok := fields["duration.ms"]; !ok {
				t.Fatalf("request log missing duration.ms: %#v", fields)
			}
			if _, ok := fields["event.duration_ms"]; ok {
				t.Fatalf("request log still emitted legacy event.duration_ms: %#v", fields)
			}
			if !strings.HasPrefix(fields["request.id"].(string), "req_") {
				t.Fatalf("request.id = %#v; want req_ prefix", fields["request.id"])
			}
			if test.wantError {
				if fields["error.class"] != "http_error" ||
					fields["error.code"] != "server_error" ||
					fields["error.message_safe"] != "HTTP request failed" {
					t.Fatalf("server-error request log missing shared error fields: %#v", fields)
				}
			} else {
				for _, field := range []string{"error.class", "error.code", "error.message_safe"} {
					if _, ok := fields[field]; ok {
						t.Fatalf("non-error request log contains %s: %#v", field, fields)
					}
				}
			}
		})
	}
}

func TestRequestLoggingRedactsSensitiveHTTPBoundaryInputs(t *testing.T) {
	databaseDSNSentinel := "postgres://user:pass@db.internal/tetral" //nolint:gosec // G101: synthetic DSN sentinel for boundary-log redaction proof.
	for _, test := range []struct {
		name        string
		body        string
		path        string
		headerName  string
		headerValue string
		err         string
		forbidden   []string
	}{
		{name: "prompt body", body: `{"prompt":"prompt-secret-sentinel","content":"memory content sentinel"}`, path: "/v1/sessions/sesn_secret/events", forbidden: []string{"prompt-secret-sentinel", "memory content sentinel"}},
		{name: "file content", body: "file-content-secret-sentinel", path: "/v1/files/file_secret/content", forbidden: []string{"file-content-secret-sentinel"}},
		{name: "provider body", body: "provider error raw body bearer-token-secret", path: "/v1/sessions", forbidden: []string{"provider error raw body", "bearer-token-secret"}},
		{name: "api key and bearer", body: `{"api_key":"tetral_sk_boundarysentinel1234567890"}`, path: "/v1/sessions?api_key=tetral_sk_querysentinel1234567890", headerName: "Authorization", headerValue: "Bearer bearer-boundary-sentinel", forbidden: []string{"tetral_sk_boundarysentinel", "tetral_sk_querysentinel", "bearer-boundary-sentinel"}},
		{name: "api key path", path: "/v1/api_keys/tetral_sk_pathsentinel1234567890", forbidden: []string{"tetral_sk_pathsentinel"}},
		{name: "credential and database dsn", body: `{"credential":"client_secret=credential-boundary-sentinel","dsn":"` + databaseDSNSentinel + `"}`, path: "/v1/vaults", forbidden: []string{"credential-boundary-sentinel", databaseDSNSentinel}},
		{name: "kubernetes secret and mcp auth", body: `{"kind":"Secret","data":{"token":"k8s-secret-boundary-sentinel"},"mcp_auth":"raw-mcp-auth-fragment"}`, path: "/v1/sessions", err: "provider returned raw-mcp-auth-fragment k8s-secret-boundary-sentinel", forbidden: []string{"k8s-secret-boundary-sentinel", "raw-mcp-auth-fragment"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			logger := newTestLogger(&buffer)
			handler := RequestIDMiddleware(RequestLogMiddleware(logger, time.Hour)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.err != "" {
					writeError(w, httptest.NewRequest(http.MethodPost, test.path, nil), errors.New(test.err))
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
			})))

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			if test.headerName != "" {
				request.Header.Set(test.headerName, test.headerValue)
			}
			handler.ServeHTTP(recorder, request)

			logOutput := buffer.String()
			for _, forbidden := range test.forbidden {
				if strings.Contains(logOutput, forbidden) || strings.Contains(recorder.Body.String(), forbidden) {
					t.Fatalf("request boundary leaked %q: log=%s body=%s", forbidden, logOutput, recorder.Body.String())
				}
			}
			fields := decodeHTTPLogLine(t, logOutput)
			if fields["msg"] != "http.request" || fields["http.response.status_code"].(float64) != 500 {
				t.Fatalf("request log fields = %#v", fields)
			}
			if fields["operation"] != "http.request" ||
				fields["component"] != "http-api" ||
				!strings.HasPrefix(fields["request.id"].(string), "req_") ||
				fields["http.request.method"] != http.MethodPost {
				t.Fatalf("request log missing safe request fields: %#v", fields)
			}
		})
	}
}

func TestAuthMiddlewareLogsFailuresWithoutSecretMaterial(t *testing.T) {
	rawToken := "tetral_sk_testrawtokenmaterial0000000000000000000000000" //nolint:gosec // G101: synthetic test key used to prove URL/header redaction.
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := RequestIDMiddleware(authMiddleware(testAuthenticator("secret-key-value"), logger)(inner))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/v1/api_keys/"+rawToken, nil)
	request.Header.Set("x-api-key", "attacker-guess-123")
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d; want 401 body=%s", recorder.Code, recorder.Body.String())
	}
	line := buffer.String()
	for _, forbidden := range []string{rawToken, "secret-key-value", "attacker-guess-123"} {
		if strings.Contains(line, forbidden) || strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("auth failure leaked %q; log=%s body=%s", forbidden, line, recorder.Body.String())
		}
	}
	fields := decodeHTTPLogLine(t, line)
	if fields["msg"] != "http.auth" || fields["auth.result"] != "failure" || fields["url.path"] != "/v1/api_keys/{api_key_id}" {
		t.Fatalf("auth log fields = %#v", fields)
	}
	if fields["operation"] != "http.auth" ||
		fields["event.kind"] != "auth_failure" ||
		fields["component"] != "http-api" ||
		fields["error.class"] != "authentication_error" ||
		fields["error.code"] != "authentication_error" ||
		fields["error.message_safe"] != "authentication failed" {
		t.Fatalf("auth failure log missing shared contract fields: %#v", fields)
	}
}

func TestAuthMiddlewareSuppressesSuccessLogsByDefault(t *testing.T) {
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	handler := RequestIDMiddleware(authMiddleware(testAuthenticator("secret-key-value"), logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	request := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	request.Header.Set("x-api-key", "secret-key-value")
	handler.ServeHTTP(httptest.NewRecorder(), request)

	if buffer.String() != "" {
		t.Fatalf("auth success logged by default: %s", buffer.String())
	}
}

func TestAuthMiddlewareInternalAuthenticatorErrorReturnsSafeAPIErrorAndLog(t *testing.T) {
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	authenticator := auth.AuthenticatorFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{}, errors.New("database timeout includes secret-key-value")
	})
	handler := RequestIDMiddleware(authMiddleware(authenticator, logger)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("handler must not be called when authenticator returns an internal error")
	})))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	request.Header.Set("x-api-key", "secret-key-value")
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500 body=%s", recorder.Code, recorder.Body.String())
	}
	for _, forbidden := range []string{"secret-key-value", "database timeout"} {
		if strings.Contains(buffer.String(), forbidden) || strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("auth error leaked %q; log=%s body=%s", forbidden, buffer.String(), recorder.Body.String())
		}
	}
	fields := decodeHTTPLogLine(t, buffer.String())
	if fields["auth.result"] != "error" ||
		fields["event.kind"] != "auth_error" ||
		fields["component"] != "http-api" ||
		fields["error.class"] != "auth_error" ||
		fields["error.code"] == "" ||
		fields["error.message_safe"] != "authentication backend error" {
		t.Fatalf("auth error log fields = %#v", fields)
	}
	if _, ok := fields["error.type"]; ok {
		t.Fatalf("auth error log still emitted legacy error.type: %#v", fields)
	}
}

func TestRecoveryMiddlewareReturnsStandardErrorAndLogsSafePanic(t *testing.T) {
	const panicSecret = "panic-secret-should-not-leak"
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	handler := RequestIDMiddleware(recoveryMiddleware(logger)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic(panicSecret)
	})))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500 body=%s", recorder.Code, recorder.Body.String())
	}
	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "api_error" || response.Error.Message != "internal error" || response.RequestID == "" {
		t.Fatalf("panic response = %+v; want safe api_error with request_id", response)
	}
	if strings.Contains(buffer.String(), panicSecret) || strings.Contains(recorder.Body.String(), panicSecret) {
		t.Fatalf("panic recovery leaked recovered value; log=%s body=%s", buffer.String(), recorder.Body.String())
	}
	fields := decodeHTTPLogLine(t, buffer.String())
	if fields["msg"] != "http.panic" || fields["request.id"] != response.RequestID {
		t.Fatalf("panic log fields = %#v response=%+v", fields, response)
	}
	if fields["operation"] != "http.request" ||
		fields["event.kind"] != "panic" ||
		fields["component"] != "http-api" ||
		fields["error.class"] != "panic" ||
		fields["error.code"] != "internal_error" ||
		fields["error.message_safe"] != "internal error" {
		t.Fatalf("panic log missing shared contract fields: %#v", fields)
	}
}

func TestStatusTrackingWriterForwardsFlushThroughResponseController(t *testing.T) {
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	flushErr := errors.New("flush not reached")
	handler := RequestIDMiddleware(RequestLogMiddleware(logger, time.Hour)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("partial"))
		// http.ResponseController walks the Unwrap chain to reach Flush; without
		// statusTrackingWriter.Unwrap this returns http.ErrNotSupported.
		flushErr = http.NewResponseController(w).Flush()
	})))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/sessions", nil))

	if flushErr != nil {
		t.Fatalf("ResponseController.Flush through wrapper = %v; want nil (Unwrap must forward)", flushErr)
	}
	if !recorder.Flushed {
		t.Fatal("underlying recorder was not flushed through the wrapper")
	}
	if recorder.Body.String() != "partial" {
		t.Fatalf("body = %q; want partial", recorder.Body.String())
	}
	// Status tracking is unaffected by the Unwrap forwarding: the boundary log
	// still records the 500 the handler wrote.
	fields := decodeHTTPLogLine(t, buffer.String())
	if fields["msg"] != "http.request" || fields["http.response.status_code"].(float64) != 500 {
		t.Fatalf("status tracking drifted after Unwrap: %#v", fields)
	}
}

func decodeHTTPLogLine(t *testing.T, line string) map[string]any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		t.Fatalf("decode log JSON %q: %v", line, err)
	}
	return fields
}

type recordingRequestMetrics struct {
	calls      int
	method     string
	statusCode int
	duration   time.Duration
}

func (m *recordingRequestMetrics) ObserveHTTPRequest(method string, statusCode int, duration time.Duration) {
	m.calls++
	m.method = method
	m.statusCode = statusCode
	m.duration = duration
}
