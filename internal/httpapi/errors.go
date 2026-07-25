// Package httpapi provides HTTP handlers, middleware, and error handling for the Engine API.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	agentErrors "github.com/tetral-ai/tetral/internal/agent"
	authErrors "github.com/tetral-ai/tetral/internal/auth"
	environmentErrors "github.com/tetral-ai/tetral/internal/environment"
	fileErrors "github.com/tetral-ai/tetral/internal/files"
	memoryErrors "github.com/tetral-ai/tetral/internal/memory"
	sandboxErrors "github.com/tetral-ai/tetral/internal/sandbox"
	sessionErrors "github.com/tetral-ai/tetral/internal/session"
	sessionEventErrors "github.com/tetral-ai/tetral/internal/sessionevent"
	skillErrors "github.com/tetral-ai/tetral/internal/skill"
	vaultErrors "github.com/tetral-ai/tetral/internal/vault"
	workspaceErrors "github.com/tetral-ai/tetral/internal/workspace"
)

// NotImplementedError indicates a route that is registered but not yet implemented.
type NotImplementedError struct {
	Message string
}

func (e *NotImplementedError) Error() string { return e.Message }

// NotFoundError indicates that the requested resource does not exist.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// ValidationError indicates that the request body or parameters are invalid.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// ConflictError indicates a state conflict (e.g., sending a message while session is running).
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

// AuthenticationError indicates missing or invalid API key authentication.
type AuthenticationError struct {
	Message string
}

func (e *AuthenticationError) Error() string { return e.Message }

// RequestTooLargeError indicates the HTTP request body exceeded the
// per-route size cap. The centralized writeError envelope keeps
// request_id and the public `request_too_large` error type stable.
type RequestTooLargeError struct {
	Message string
}

func (e *RequestTooLargeError) Error() string { return e.Message }

// SessionEventRequestTooLargeError is the Events API-specific 413
// classification. The shared SDK envelope for session events uses
// invalid_request_error even when HTTP status is 413.
type SessionEventRequestTooLargeError struct {
	Message string
}

func (e *SessionEventRequestTooLargeError) Error() string { return e.Message }

// errorResponse is the standard error JSON envelope, matching the Anthropic error format.
type errorResponse struct {
	Type      string      `json:"type"`
	Error     errorDetail `json:"error"`
	RequestID string      `json:"request_id"`
}

type errorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// writeError maps a typed error to an HTTP status and writes the standard JSON error response.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	WriteError(w, r, err)
}

// WriteError maps a typed error to an HTTP status and writes the standard JSON error response.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	classification := classifyError(err)

	requestID, _ := r.Context().Value(requestIDKey).(string)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(classification.status)

	response := errorResponse{
		Type:      "error",
		Error:     classification.detail,
		RequestID: requestID,
	}

	_ = json.NewEncoder(w).Encode(response)
}

type errorClassification struct {
	status int
	detail errorDetail
}

func classified(status int, errorType string, message string) errorClassification {
	return errorClassification{status: status, detail: errorDetail{Type: errorType, Message: message}}
}

func classifyError(err error) errorClassification {
	var notImpl *NotImplementedError
	var notFound *NotFoundError
	var validation *ValidationError
	var conflict *ConflictError
	var auth *AuthenticationError
	var tooLarge *RequestTooLargeError
	var sessionEventTooLarge *SessionEventRequestTooLargeError

	// Session-layer error types (defined in internal/session/).
	var sessionNotFound *sessionErrors.NotFoundError
	var sessionValidation *sessionErrors.ValidationError
	var sessionConflict *sessionErrors.ConflictError
	var sessionPermission *sessionErrors.PermissionError

	// Session-event ingress errors.
	var sessionEventNotFound *sessionEventErrors.NotFoundError
	var sessionEventValidation *sessionEventErrors.ValidationError
	var sessionEventConflict *sessionEventErrors.ConflictError

	// Agent-layer error types.
	var agentNotFound *agentErrors.NotFoundError
	var agentValidation *agentErrors.ValidationError
	var agentConflict *agentErrors.ConflictError

	// Environment-layer error types.
	var envNotFound *environmentErrors.NotFoundError
	var envValidation *environmentErrors.ValidationError
	var envConflict *environmentErrors.ConflictError

	// Vault-layer error types.
	var vaultNotFound *vaultErrors.NotFoundError
	var vaultValidation *vaultErrors.ValidationError
	var vaultConflict *vaultErrors.ConflictError

	// Auth-layer error types. Public API-key management lives in
	// auth, but target route families still use auth errors
	// when signed principal or test authenticator checks fail.
	var authPkgAuthenticationError *authErrors.AuthenticationError
	var authPkgValidationError *authErrors.ValidationError
	var authPkgNotFoundError *authErrors.NotFoundError
	var authPkgWeakBootstrapError *authErrors.WeakBootstrapKeyError

	// Workspace-layer error types.
	var workspaceNotFound *workspaceErrors.NotFoundError

	// Skill-layer error types. The Skill domain distinguishes count quotas (400)
	// from retained-byte quotas (413); the QuotaError check below routes via
	// Kind.
	var skillNotFound *skillErrors.NotFoundError
	var skillValidation *skillErrors.ValidationError
	var skillConflict *skillErrors.ConflictError
	var skillTooLarge *skillErrors.RequestTooLargeError
	var skillQuota *skillErrors.QuotaError
	var skillInternal *skillErrors.InternalError

	// Files-layer error types.
	var fileNotFound *fileErrors.NotFoundError
	var fileValidation *fileErrors.ValidationError
	var fileConflict *fileErrors.ConflictError
	var filePermission *fileErrors.PermissionError
	var fileTooLarge *fileErrors.RequestTooLargeError
	var fileQuota *fileErrors.QuotaError

	// Memory-layer error types.
	var memoryNotFound *memoryErrors.NotFoundError
	var memoryValidation *memoryErrors.ValidationError
	var memoryPrecondition *memoryErrors.PreconditionFailedError
	var memoryConflict *memoryErrors.PathConflictError
	var memoryTooLarge *memoryErrors.RequestTooLargeError
	var memoryQuota *memoryErrors.QuotaError

	// Sandbox-layer error types are internal classifications only; HTTP
	// responses must use existing public Engine error types and fixed safe
	// messages so provider details never leak to callers.
	var sandboxError *sandboxErrors.SandboxError

	switch {
	case errors.As(err, &auth):
		return classified(http.StatusUnauthorized, "authentication_error", auth.Message)
	case errors.As(err, &authPkgAuthenticationError):
		return classified(http.StatusUnauthorized, "authentication_error", authPkgAuthenticationError.Message)
	case errors.Is(err, workspaceErrors.ErrNoWorkspaceInContext):
		return classified(http.StatusUnauthorized, "authentication_error", "missing workspace context")
	case errors.As(err, &tooLarge):
		return classified(http.StatusRequestEntityTooLarge, "request_too_large", tooLarge.Message)
	case errors.As(err, &sessionEventTooLarge):
		return classified(http.StatusRequestEntityTooLarge, "invalid_request_error", sessionEventTooLarge.Message)
	case errors.As(err, &notImpl):
		return classified(http.StatusNotImplemented, "not_implemented", notImpl.Message)
	case errors.As(err, &notFound):
		return classified(http.StatusNotFound, "not_found_error", notFound.Message)
	case errors.As(err, &sessionNotFound):
		return classified(http.StatusNotFound, "not_found_error", sessionNotFound.Message)
	case errors.As(err, &sessionEventNotFound):
		return classified(http.StatusNotFound, "not_found_error", sessionEventNotFound.Message)
	case errors.As(err, &authPkgNotFoundError):
		return classified(http.StatusNotFound, "not_found_error", authPkgNotFoundError.Message)
	case errors.As(err, &workspaceNotFound):
		return classified(http.StatusNotFound, "not_found_error", workspaceNotFound.Message)
	case errors.As(err, &sessionPermission):
		return classified(http.StatusForbidden, "permission_error", sessionPermission.Message)
	case errors.As(err, &validation):
		return classified(http.StatusBadRequest, "invalid_request_error", validation.Message)
	case errors.As(err, &sessionValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", sessionValidation.Message)
	case errors.As(err, &sessionEventValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", sessionEventValidation.Message)
	case errors.As(err, &authPkgValidationError):
		return classified(http.StatusBadRequest, "invalid_request_error", authPkgValidationError.Message)
	case errors.As(err, &authPkgWeakBootstrapError):
		// Surface the public message but not the raw env value.
		return classified(http.StatusInternalServerError, "api_error", "internal error")
	case errors.As(err, &conflict):
		return classified(http.StatusConflict, "conflict_error", conflict.Message)
	case errors.As(err, &sessionConflict):
		if sessionConflict.InvalidRequest {
			return classified(http.StatusConflict, "invalid_request_error", sessionConflict.Message)
		}
		return classified(http.StatusConflict, "conflict_error", sessionConflict.Message)
	case errors.As(err, &sessionEventConflict):
		return classified(http.StatusConflict, "invalid_request_error", sessionEventConflict.Message)
	case errors.As(err, &agentNotFound):
		return classified(http.StatusNotFound, "not_found_error", agentNotFound.Message)
	case errors.As(err, &agentValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", agentValidation.Message)
	case errors.As(err, &agentConflict):
		return classified(http.StatusConflict, "conflict_error", agentConflict.Message)
	case errors.As(err, &envNotFound):
		return classified(http.StatusNotFound, "not_found_error", envNotFound.Message)
	case errors.As(err, &envValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", envValidation.Message)
	case errors.As(err, &envConflict):
		return classified(http.StatusConflict, "conflict_error", envConflict.Message)
	case errors.As(err, &vaultNotFound):
		return classified(http.StatusNotFound, "not_found_error", vaultNotFound.Message)
	case errors.As(err, &vaultValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", vaultValidation.Message)
	case errors.As(err, &vaultConflict):
		if vaultConflict.InvalidRequest {
			return classified(http.StatusConflict, "invalid_request_error", vaultConflict.Message)
		}
		return classified(http.StatusConflict, "conflict_error", vaultConflict.Message)
	case errors.As(err, &skillTooLarge):
		return classified(http.StatusRequestEntityTooLarge, "request_too_large", skillTooLarge.Message)
	case errors.As(err, &skillNotFound):
		return classified(http.StatusNotFound, "not_found_error", skillNotFound.Message)
	case errors.As(err, &skillConflict):
		return classified(http.StatusConflict, "conflict_error", skillConflict.Message)
	case errors.As(err, &skillQuota):
		// Count overflow is a 400 invalid_request_error; retained-byte
		// overflow is a 413 request_too_large. QuotaError carries the kind.
		if skillQuota.Kind == skillErrors.QuotaKindRetainedBytes {
			return classified(http.StatusRequestEntityTooLarge, "request_too_large", skillQuota.Message)
		}
		return classified(http.StatusBadRequest, "invalid_request_error", skillQuota.Message)
	case errors.As(err, &skillValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", skillValidation.Message)
	case errors.As(err, &skillInternal):
		return classified(http.StatusInternalServerError, "api_error", "internal error")
	case errors.As(err, &fileTooLarge):
		return classified(http.StatusRequestEntityTooLarge, "request_too_large", fileTooLarge.Message)
	case errors.As(err, &fileNotFound):
		return classified(http.StatusNotFound, "not_found_error", fileNotFound.Message)
	case errors.As(err, &filePermission):
		return classified(http.StatusForbidden, "permission_error", filePermission.Message)
	case errors.As(err, &fileConflict):
		return classified(http.StatusConflict, "conflict_error", fileConflict.Message)
	case errors.As(err, &fileQuota):
		if fileQuota.Kind == fileErrors.QuotaKindRetainedBytes {
			return classified(http.StatusRequestEntityTooLarge, "request_too_large", fileQuota.Message)
		}
		return classified(http.StatusBadRequest, "invalid_request_error", fileQuota.Message)
	case errors.As(err, &fileValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", fileValidation.Message)
	case errors.As(err, &memoryTooLarge):
		return classified(http.StatusRequestEntityTooLarge, "request_too_large", memoryTooLarge.Message)
	case errors.As(err, &memoryNotFound):
		return classified(http.StatusNotFound, "not_found_error", memoryNotFound.Message)
	case errors.As(err, &memoryPrecondition):
		return classified(http.StatusConflict, "invalid_request_error", memoryPrecondition.Message)
	case errors.As(err, &memoryConflict):
		return classified(http.StatusConflict, "invalid_request_error", memoryConflict.Message)
	case errors.As(err, &memoryQuota):
		return classified(http.StatusBadRequest, "invalid_request_error", memoryQuota.Message)
	case errors.As(err, &memoryValidation):
		return classified(http.StatusBadRequest, "invalid_request_error", memoryValidation.Message)
	case errors.As(err, &sandboxError):
		return classifySandboxError(sandboxError)
	default:
		return classified(http.StatusInternalServerError, "api_error", "internal error")
	}
}

func classifySandboxError(err *sandboxErrors.SandboxError) errorClassification {
	switch err.Code {
	case sandboxErrors.SandboxErrorProviderUnconfigured, sandboxErrors.SandboxErrorProviderConfigInvalid:
		return classified(http.StatusInternalServerError, "api_error", "session sandbox setup failed")
	case sandboxErrors.SandboxErrorReleaseFailed:
		return classified(http.StatusBadGateway, "api_error", "session sandbox release failed")
	case sandboxErrors.SandboxErrorCreateFailed,
		sandboxErrors.SandboxErrorBaseTemplateFailed,
		sandboxErrors.SandboxErrorNetworkFailed,
		sandboxErrors.SandboxErrorMountFailed:
		return classified(http.StatusBadGateway, "api_error", "session sandbox setup failed")
	default:
		return classified(http.StatusInternalServerError, "api_error", "internal error")
	}
}
