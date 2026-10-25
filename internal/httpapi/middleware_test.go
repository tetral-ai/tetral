package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
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
		return auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID, Type: "workspace", Name: "Default"}, "ak_test"), nil
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

// newRecordedRouter mounts handlers on a chi router behind the request
// boundary, as the public routers do, and records spans synchronously.
func newRecordedRouter(t *testing.T, buffer *bytes.Buffer, metrics RequestMetricsRecorder, options ...RequestLogOption) (*chi.Mux, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "api", SyncExporter: exporter, SampleRatio: 1})))
	router := chi.NewRouter()
	router.Use(RequestIDMiddleware)
	router.Use(RequestLogMiddleware(workload.NewLogger(buffer, "api", "test", "unit"), append([]RequestLogOption{WithRequestLogMetrics(metrics)}, options...)...))
	return router, exporter
}

func decodeHTTPLogLines(t *testing.T, output string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line != "" {
			records = append(records, decodeHTTPLogLine(t, line))
		}
	}
	return records
}

func TestRequestBoundaryWritesOneCompletedRecordPerRequestWithItsRoutePattern(t *testing.T) {
	for _, test := range []struct {
		name, method, path, wantRoute, wantMethod string
		status                                    int
	}{
		{name: "ok", method: http.MethodGet, path: "/v1/sessions/sesn_1", wantRoute: "/v1/sessions/{session_id}", wantMethod: "GET", status: http.StatusOK},
		{name: "not found", method: http.MethodGet, path: "/v1/sessions/sesn_missing", wantRoute: "/v1/sessions/{session_id}", wantMethod: "GET", status: http.StatusNotFound},
		{name: "server error", method: http.MethodPost, path: "/v1/sessions/sesn_1", wantRoute: "/v1/sessions/{session_id}", wantMethod: "POST", status: http.StatusInternalServerError},
		{name: "unmatched inside a mounted router", method: http.MethodGet, path: "/v1/nowhere/sesn_1", wantRoute: "unmatched", wantMethod: "GET", status: http.StatusNotFound},
		{name: "unmatched outside every mount", method: http.MethodGet, path: "/nowhere", wantRoute: "unmatched", wantMethod: "GET", status: http.StatusNotFound},
		{name: "nonstandard method", method: "PROPFIND", path: "/v1/sessions/sesn_1", wantRoute: "unmatched", wantMethod: "_OTHER", status: http.StatusMethodNotAllowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			metrics := &recordingRequestMetrics{}
			router, _ := newRecordedRouter(t, &buffer, metrics)
			router.Route("/v1", func(r chi.Router) {
				r.HandleFunc("/sessions/{session_id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(test.status) })
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, nil))
			records := decodeHTTPLogLines(t, buffer.String())
			wantRecords := 1
			if test.status >= 500 {
				wantRecords = 2
			}
			if len(records) != wantRecords {
				t.Fatalf("records = %v; want %d", records, wantRecords)
			}
			completed := records[0]
			if completed["msg"] != "http.request.completed" || completed["level"] != "INFO" || completed["http.route"] != test.wantRoute || completed["http.request.method"] != test.wantMethod || completed["http.response.status_code"] != float64(test.status) {
				t.Fatalf("completed record = %v", completed)
			}
			if _, ok := completed["duration_ms"]; !ok {
				t.Fatalf("completed record has no duration_ms: %v", completed)
			}
			if completed["request.id"] != recorder.Header().Get("request-id") || !strings.HasPrefix(recorder.Header().Get("request-id"), "req_") {
				t.Fatalf("request.id %v differs from the response request-id %q", completed["request.id"], recorder.Header().Get("request-id"))
			}
			if strings.Contains(buffer.String(), test.path) {
				t.Fatalf("a record carries the request path: %s", buffer.String())
			}
			if test.status >= 500 {
				failed := records[1]
				if failed["msg"] != "http.request.failed" || failed["level"] != "ERROR" || failed["error.class"] != "http_error" || failed["error.code"] != "server_error" || failed["error.message_safe"] != "HTTP request failed" {
					t.Fatalf("failure record = %v", failed)
				}
			}
			for _, field := range []string{"error.class", "error.code", "error.message_safe"} {
				if _, ok := completed[field]; ok {
					t.Fatalf("completed record carries %s: %v", field, completed)
				}
			}
			if metrics.calls != 1 || metrics.route != test.wantRoute || metrics.method != test.method || metrics.statusCode != test.status || metrics.long {
				t.Fatalf("observation = %+v", metrics)
			}
		})
	}
}

func TestRequestBoundaryLeavesHealthChecksAndMetricsUnrecordedAndUntraced(t *testing.T) {
	for _, path := range []string{"/health", "/ready", "/metrics"} {
		var buffer bytes.Buffer
		metrics := &recordingRequestMetrics{}
		router, exporter := newRecordedRouter(t, &buffer, metrics)
		router.Get(path, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		if buffer.Len() != 0 || metrics.calls != 0 || len(exporter.GetSpans()) != 0 {
			t.Fatalf("%s recorded: records=%q observations=%d spans=%d", path, buffer.String(), metrics.calls, len(exporter.GetSpans()))
		}
	}
}

func TestRequestBoundaryTurnsAnIncomingTraceparentIntoALinkOfANewTrace(t *testing.T) {
	var buffer bytes.Buffer
	router, exporter := newRecordedRouter(t, &buffer, &recordingRequestMetrics{})
	router.Post("/v1/sessions/{session_id}/events", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	const sent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_1/events", nil)
	request.Header.Set("traceparent", sent)
	router.ServeHTTP(httptest.NewRecorder(), request)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d; want 1", len(spans))
	}
	span := spans[0]
	if span.SpanContext.TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736" || span.Parent.IsValid() {
		t.Fatalf("the sent traceparent became the request's parent: trace %s parent %v", span.SpanContext.TraceID(), span.Parent)
	}
	if len(span.Links) != 1 || span.Links[0].SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || span.Links[0].SpanContext.SpanID().String() != "00f067aa0ba902b7" {
		t.Fatalf("links = %+v; want the sent traceparent as the only link", span.Links)
	}
	record := decodeHTTPLogLines(t, buffer.String())[0]
	if record["trace_id"] != span.SpanContext.TraceID().String() {
		t.Fatalf("record trace_id %v differs from the request's trace %s", record["trace_id"], span.SpanContext.TraceID())
	}
}

func TestRequestBoundarySpanCarriesTheRouteAndNeverThePathOrQuery(t *testing.T) {
	var buffer bytes.Buffer
	router, exporter := newRecordedRouter(t, &buffer, &recordingRequestMetrics{})
	router.Get("/v1/drives/{drive_id}/files/*", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/drives/drv_1/files/private/notes.txt?version=ver_1", nil))
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d; want 1", len(spans))
	}
	attributes := map[string]string{}
	for _, attribute := range spans[0].Attributes {
		attributes[string(attribute.Key)] = attribute.Value.String()
	}
	if attributes["http.route"] != "/v1/drives/{drive_id}/files/*" || attributes["http.request.method"] != "GET" || attributes["http.response.status_code"] != "200" {
		t.Fatalf("span attributes = %v", attributes)
	}
	for _, forbidden := range []string{"url.path", "url.query", "client.address", "user_agent.original"} {
		if _, ok := attributes[forbidden]; ok {
			t.Fatalf("span carries %s: %v", forbidden, attributes)
		}
	}
	for _, value := range attributes {
		if strings.Contains(value, "notes.txt") || strings.Contains(value, "ver_1") {
			t.Fatalf("span attribute carries the path or query: %v", attributes)
		}
	}
}

func TestRequestBoundaryObservesALongRouteInTheStreamHistogramOnly(t *testing.T) {
	var buffer bytes.Buffer
	metrics := workload.NewHTTPMetrics("api")
	router, _ := newRecordedRouter(t, &buffer, metrics, WithLongRoutes("GET /v1/sessions/{session_id}/stream"))
	router.Get("/v1/sessions/{session_id}/stream", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_1/stream", nil))
	samples, _ := metrics.Collector()(context.Background())
	var stream, request int
	for _, sample := range samples {
		switch sample.Name {
		case "http_server_stream_duration_seconds_count":
			stream += int(sample.Value)
		case "http_server_request_duration_seconds_count":
			request += int(sample.Value)
		}
	}
	if stream != 1 || request != 0 {
		t.Fatalf("stream samples = %d, request samples = %d; want 1 and 0", stream, request)
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
			handler := RequestIDMiddleware(RequestLogMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
			fields := decodeHTTPLogLines(t, logOutput)[0]
			if fields["msg"] != "http.request.completed" || fields["http.response.status_code"].(float64) != 500 {
				t.Fatalf("request log fields = %#v", fields)
			}
			if fields["http.route"] != "unmatched" ||
				!strings.HasPrefix(fields["request.id"].(string), "req_") ||
				fields["http.request.method"] != http.MethodPost {
				t.Fatalf("request log missing safe request fields: %#v", fields)
			}
		})
	}
}

// Backend records keep the service's own req_ request.id and add the verified
// principal's signed request_id as edge.request.id, so they join the Auth Check
// record of the same edge request. A forged X-Request-Id header is never logged.
func TestRequestRecordsCarryVerifiedEdgeRequestID(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	principal := auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID}, "ak_edge")
	token, err := signer.Mint(principal, http.MethodPost, "/v1/sessions", "edge-request-signed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	logger := newTestLogger(&buffer)
	handler := RequestIDMiddleware(RequestLogMiddleware(logger)(internalPrincipalMiddleware(verifier)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.RecordDecision(r.Context(), "operation", &auth.PermissionError{}, auth.AuditFacts(principal))
		w.WriteHeader(http.StatusInternalServerError)
	}))))
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions", nil)
	request.Header.Set("X-Tetral-Internal-Principal", token)
	request.Header.Set("X-Request-Id", "edge-request-forged")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if strings.Contains(buffer.String(), "edge-request-forged") {
		t.Fatalf("forged request ID header reached logs: %s", buffer.String())
	}
	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("log lines = %d; want decision, completed and failure records: %s", len(lines), buffer.String())
	}
	for index, wantMessage := range []string{"auth.operation.denied", "http.request.completed", "http.request.failed"} {
		fields := decodeHTTPLogLine(t, lines[index])
		if fields["msg"] != wantMessage {
			t.Fatalf("record %d msg = %v; want %s", index, fields["msg"], wantMessage)
		}
		if fields["request.id"] != recorder.Header().Get("request-id") || !strings.HasPrefix(recorder.Header().Get("request-id"), "req_") {
			t.Fatalf("%s request.id = %v; want the response request-id %q", wantMessage, fields["request.id"], recorder.Header().Get("request-id"))
		}
		if fields["edge.request.id"] != "edge-request-signed" {
			t.Fatalf("%s edge.request.id = %v; want the signed principal request_id", wantMessage, fields["edge.request.id"])
		}
		if index > 0 && fields["workspace.id"] != string(workspace.DefaultID) {
			t.Fatalf("%s workspace.id = %v; want the verified principal's workspace", wantMessage, fields["workspace.id"])
		}
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
	if fields["msg"] != "http.auth" || fields["auth.result"] != "failure" || fields["url.path"] != nil {
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
	handler := RequestIDMiddleware(RequestLogMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	fields := decodeHTTPLogLines(t, buffer.String())[0]
	if fields["msg"] != "http.request.completed" || fields["http.response.status_code"].(float64) != 500 {
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
	route      string
	method     string
	statusCode int
	long       bool
}

func (m *recordingRequestMetrics) ObserveHTTPRoute(route, method string, statusCode int, _ time.Duration, long bool) {
	m.calls++
	m.route, m.method, m.statusCode, m.long = route, method, statusCode, long
}
