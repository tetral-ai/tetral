package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestWriteErrorNotImplemented(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &NotImplementedError{Message: "not yet"})

	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Type != "error" {
		t.Errorf("expected type error, got %s", response.Type)
	}
	if response.Error.Type != "not_implemented" {
		t.Errorf("expected error type not_implemented, got %s", response.Error.Type)
	}
	if response.Error.Message != "not yet" {
		t.Errorf("expected message 'not yet', got %s", response.Error.Message)
	}
}

func TestWriteErrorNotFound(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &NotFoundError{Message: "session sesn_abc not found"})

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "not_found_error" {
		t.Errorf("expected error type not_found_error, got %s", response.Error.Type)
	}
	if response.Error.Message != "session sesn_abc not found" {
		t.Errorf("expected message 'session sesn_abc not found', got %s", response.Error.Message)
	}
}

func TestWriteErrorValidation(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &ValidationError{Message: "missing field"})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "invalid_request_error" {
		t.Errorf("expected error type invalid_request_error, got %s", response.Error.Type)
	}
}

func TestWriteErrorVaultReferenceConflictUsesInvalidRequestType(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/", nil)

	writeError(recorder, request, &vault.ConflictError{
		Message:        "credential is still referenced by sessions",
		InvalidRequest: true,
	})

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d; want 409", recorder.Code)
	}
	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "invalid_request_error" {
		t.Fatalf("error type = %q; want invalid_request_error", response.Error.Type)
	}
	if response.Error.Message != "credential is still referenced by sessions" {
		t.Fatalf("error message = %q", response.Error.Message)
	}
}

func TestWriteErrorNonMemoryEnvelopeByteShape(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &ValidationError{Message: "missing field"})

	want := "{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"missing field\"},\"request_id\":\"\"}\n"
	if got := recorder.Body.String(); got != want {
		t.Fatalf("non-memory error envelope changed:\ngot  %q\nwant %q", got, want)
	}
}

func TestWriteErrorMemoryPathConflictUsesSDKErrorType(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &memory.PathConflictError{
		Message:             "memory path already exists",
		ConflictingMemoryID: "mem_conflict",
		ConflictingPath:     "/notes/a.md",
	})

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d; want 409", recorder.Code)
	}
	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "invalid_request_error" {
		t.Fatalf("error type = %q; want invalid_request_error", response.Error.Type)
	}
	if strings.Contains(recorder.Body.String(), "conflicting_memory_id") || strings.Contains(recorder.Body.String(), "conflicting_path") {
		t.Fatalf("memory conflict envelope leaked non-SDK fields: %s", recorder.Body.String())
	}
}

func TestWriteErrorUnknown(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, errors.New("secret database connection string"))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "api_error" {
		t.Errorf("expected error type api_error, got %s", response.Error.Type)
	}
	if response.Error.Message != "internal error" {
		t.Errorf("expected message 'internal error', got %s", response.Error.Message)
	}
	if response.Error.Message == "secret database connection string" {
		t.Error("internal error message leaked to response")
	}
}

func TestWriteErrorSandboxFailuresUseSafePublicEnvelope(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantMsg    string
	}{
		{
			name:       "provider unconfigured",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorProviderUnconfigured, Message: "sandbox provider is not configured", Cause: errors.New("cloudflare token missing")},
			wantStatus: http.StatusInternalServerError,
			wantMsg:    "session sandbox setup failed",
		},
		{
			name:       "provider config invalid",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorProviderConfigInvalid, Message: "sandbox startup failed", Cause: errors.New("modal endpoint https://provider.example.invalid failed")},
			wantStatus: http.StatusInternalServerError,
			wantMsg:    "session sandbox setup failed",
		},
		{
			name:       "create failed",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorCreateFailed, Message: "sandbox startup failed", Cause: errors.New("provider sandbox psbox_123 raw body access_token=secret")},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "session sandbox setup failed",
		},
		{
			name:       "base template failed",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorBaseTemplateFailed, Message: "sandbox startup failed", Cause: errors.New("stderr: helper token leaked from /tmp/provider")},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "session sandbox setup failed",
		},
		{
			name:       "network failed",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorNetworkFailed, Message: "sandbox startup failed", Cause: errors.New("e2b network policy failed")},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "session sandbox setup failed",
		},
		{
			name:       "mount failed",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorMountFailed, Message: "sandbox startup failed", Cause: errors.New("command output: mounted private url https://token@example.invalid")},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "session sandbox setup failed",
		},
		{
			name:       "release failed",
			err:        &sandbox.SandboxError{Code: sandbox.SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: errors.New("cloudflare release psbox_456 failed")},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "session sandbox release failed",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request = request.WithContext(context.WithValue(request.Context(), requestIDKey, "req_sandbox_safe"))

			writeError(recorder, request, test.err)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d; want %d body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			var response errorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if response.Type != "error" || response.Error.Type != "api_error" || response.Error.Message != test.wantMsg || response.RequestID != "req_sandbox_safe" {
				t.Fatalf("response = %+v; want api_error/%q with request id", response, test.wantMsg)
			}
			for _, forbidden := range []string{
				"provider_unconfigured",
				"provider_config_invalid",
				"create_failed",
				"base_template_failed",
				"network_failed",
				"mount_failed",
				"release_failed",
				"cloudflare",
				"e2b",
				"modal",
				"psbox_",
				"access_token",
				"secret",
				"token",
				"https://",
				"raw body",
				"command output",
				"/tmp/provider",
				"stderr",
			} {
				if strings.Contains(recorder.Body.String(), forbidden) {
					t.Fatalf("sandbox error response leaked %q in body %q", forbidden, recorder.Body.String())
				}
			}
		})
	}
}

func TestWriteErrorDiagnosticErrorStaysInternal(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	privateDSNSentinel := strings.Join([]string{"postgres://user", "sentinel-private-dsn", "private-db.internal/tetral"}, ":")

	writeError(recorder, request, &dbconnect.DiagnosticError{
		Provider:  dbconnect.ProviderPlainDSN,
		Phase:     dbconnect.PhaseRuntimeQuery,
		Kind:      dbconnect.KindEndpointUnreachable,
		Operation: "memory.transaction",
		Descriptor: dbconnect.Descriptor{
			Host:     "private-db.internal",
			Database: "tenant_database",
		},
		Message: privateDSNSentinel,
		Cause:   errors.New("bind token credential leaked from driver"),
	})

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", recorder.Code)
	}
	body := recorder.Body.String()
	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "api_error" || response.Error.Message != "internal error" {
		t.Fatalf("diagnostic response = %+v; want api_error/internal error", response.Error)
	}
	for _, forbidden := range []string{
		"plain_dsn",
		"runtime_query",
		"endpoint_unreachable",
		"memory.transaction",
		"private-db.internal",
		"tenant_database",
		"sentinel-private-dsn",
		"bind token credential",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("diagnostic response leaked %q in body %q", forbidden, body)
		}
	}
}

func TestWriteErrorWrappedDiagnosticErrorsStayInternal(t *testing.T) {
	privateDSNSentinel := strings.Join([]string{"postgres://user", "sentinel-private-dsn", "private-db.internal/tetral"}, ":")
	diagnostic := &dbconnect.DiagnosticError{
		Provider:  dbconnect.ProviderPlainDSN,
		Phase:     dbconnect.PhaseRuntimeQuery,
		Kind:      dbconnect.KindEndpointUnreachable,
		Operation: "memory.transaction",
		Descriptor: dbconnect.Descriptor{
			Host:     "private-db.internal",
			Database: "tenant_database",
		},
		Message: privateDSNSentinel,
		Cause:   errors.New("bind token credential leaked from driver"),
	}
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "memory",
			err:  &memory.InternalError{Message: "memory operation failed", Cause: diagnostic},
		},
		{
			name: "skill",
			err:  &skill.InternalError{Message: "skill operation failed", Cause: diagnostic},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/", nil)

			writeError(recorder, request, tc.err)

			if recorder.Code != http.StatusInternalServerError {
				t.Fatalf("expected 500, got %d", recorder.Code)
			}
			body := recorder.Body.String()
			var response errorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if response.Error.Type != "api_error" || response.Error.Message != "internal error" {
				t.Fatalf("diagnostic response = %+v; want api_error/internal error", response.Error)
			}
			for _, forbidden := range []string{
				"plain_dsn",
				"runtime_query",
				"endpoint_unreachable",
				"memory.transaction",
				"private-db.internal",
				"tenant_database",
				"sentinel-private-dsn",
				"bind token credential",
				"operation failed",
			} {
				if strings.Contains(body, forbidden) {
					t.Fatalf("wrapped diagnostic response leaked %q in body %q", forbidden, body)
				}
			}
		})
	}
}

func TestWriteErrorIncludesRequestID(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(request.Context(), requestIDKey, "req_test123")
	request = request.WithContext(ctx)

	writeError(recorder, request, &NotImplementedError{Message: "stub"})

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.RequestID != "req_test123" {
		t.Errorf("expected request_id req_test123, got %s", response.RequestID)
	}
}

// TestWriteErrorRequestTooLarge proves the centralized envelope maps the
// typed RequestTooLargeError to HTTP 413 with error.type =
// "request_too_large". It must go through writeError / classifyError so
// request_id is preserved and the JSON envelope shape stays stable.
func TestWriteErrorRequestTooLarge(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &RequestTooLargeError{Message: "request body too large"})

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "request_too_large" {
		t.Errorf("expected error type request_too_large, got %s", response.Error.Type)
	}
	if response.Error.Message != "request body too large" {
		t.Errorf("expected message 'request body too large', got %s", response.Error.Message)
	}
}

func TestWriteErrorSessionEventRequestTooLarge(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &SessionEventRequestTooLargeError{Message: "request body too large"})

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "invalid_request_error" {
		t.Errorf("expected error type invalid_request_error, got %s", response.Error.Type)
	}
}

func TestWriteErrorMemoryRetainedPayloadRequestTooLarge(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &memory.RequestTooLargeError{Message: "memory retained payload quota exceeded"})

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "request_too_large" {
		t.Errorf("expected error type request_too_large, got %s", response.Error.Type)
	}
	if response.Error.Message != "memory retained payload quota exceeded" {
		t.Errorf("expected retained payload message, got %s", response.Error.Message)
	}
}

func TestWriteErrorSkillQuotaErrorsMapByKind(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantType    string
		wantMessage string
	}{
		{
			name:        "count quota",
			err:         &skill.QuotaError{Kind: skill.QuotaKindCount, Message: "skill count quota exceeded"},
			wantStatus:  http.StatusBadRequest,
			wantType:    "invalid_request_error",
			wantMessage: "skill count quota exceeded",
		},
		{
			name:        "retained byte quota",
			err:         &skill.QuotaError{Kind: skill.QuotaKindRetainedBytes, Message: "skill retained bytes quota exceeded"},
			wantStatus:  http.StatusRequestEntityTooLarge,
			wantType:    "request_too_large",
			wantMessage: "skill retained bytes quota exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request = request.WithContext(context.WithValue(request.Context(), requestIDKey, "req_skill_quota"))

			writeError(recorder, request, tt.err)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d; want %d (body=%q)", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			var response errorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if response.Type != "error" {
				t.Errorf("response type = %q; want error", response.Type)
			}
			if response.Error.Type != tt.wantType {
				t.Errorf("error type = %q; want %q", response.Error.Type, tt.wantType)
			}
			if response.Error.Message != tt.wantMessage {
				t.Errorf("error message = %q; want %q", response.Error.Message, tt.wantMessage)
			}
			if response.RequestID != "req_skill_quota" {
				t.Errorf("request_id = %q; want req_skill_quota", response.RequestID)
			}
		})
	}
}

func TestWriteErrorAuthentication(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &AuthenticationError{Message: "invalid api key"})

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", recorder.Code)
	}

	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "authentication_error" {
		t.Errorf("expected error type 'authentication_error', got %s", response.Error.Type)
	}
	if response.Error.Message != "invalid api key" {
		t.Errorf("expected message 'invalid api key', got %s", response.Error.Message)
	}
}

func TestWriteErrorNoWorkspaceContext(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, workspace.ErrNoWorkspaceInContext)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", recorder.Code)
	}
	var response errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if response.Error.Type != "authentication_error" {
		t.Errorf("expected error type authentication_error, got %s", response.Error.Type)
	}
	if response.Error.Message != "missing workspace context" {
		t.Errorf("expected sanitized missing-workspace message, got %q", response.Error.Message)
	}
}

func TestWriteErrorContentType(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	writeError(recorder, request, &NotImplementedError{Message: "stub"})

	contentType := recorder.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}
}
