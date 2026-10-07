package tetralauth

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type requestTooLargeError struct {
	message string
}

func (e requestTooLargeError) Error() string { return e.message }

type errorResponse struct {
	Type      string      `json:"type"`
	Error     errorDetail `json:"error"`
	RequestID string      `json:"request_id"`
}

type errorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	status, errorType, message := classifyAuthError(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{
		Type:      "error",
		Error:     errorDetail{Type: errorType, Message: message},
		RequestID: authRequestID(r),
	})
}

// authRequestID returns the ID that RequestIDMiddleware generated. Auth never
// echoes a client-supplied X-Request-Id into its error envelope.
func authRequestID(r *http.Request) string {
	return httpapi.RequestIDFromContext(r.Context())
}

func writeAuthJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func classifyAuthError(err error) (int, string, string) {
	var authErr *auth.AuthenticationError
	var validationErr *auth.ValidationError
	var notFoundErr *auth.NotFoundError
	var weakBootstrapErr *auth.WeakBootstrapKeyError
	var tooLarge requestTooLargeError
	var unavailable *auth.UnavailableError
	var permission *auth.PermissionError
	var rate exchangeRateError
	switch {
	case errors.As(err, &unavailable):
		return http.StatusServiceUnavailable, "api_error", "authentication unavailable"
	case errors.As(err, &permission):
		return http.StatusForbidden, "permission_error", "permission denied"
	case errors.As(err, &rate):
		return http.StatusTooManyRequests, "rate_limit_error", "token exchange rate exceeded"
	case errors.As(err, &authErr):
		return http.StatusUnauthorized, "authentication_error", authErr.Message
	case errors.Is(err, workspace.ErrNoWorkspaceInContext):
		return http.StatusUnauthorized, "authentication_error", "missing workspace context"
	case errors.As(err, &tooLarge):
		return http.StatusRequestEntityTooLarge, "invalid_request_error", tooLarge.message
	case errors.As(err, &validationErr):
		return http.StatusBadRequest, "invalid_request_error", validationErr.Message
	case errors.As(err, &notFoundErr):
		return http.StatusNotFound, "not_found_error", notFoundErr.Message
	case errors.As(err, &weakBootstrapErr):
		return http.StatusInternalServerError, "api_error", "internal error"
	default:
		return http.StatusInternalServerError, "api_error", "internal error"
	}
}
