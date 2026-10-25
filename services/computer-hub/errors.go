package computerhub

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
)

// clientError is the body of every /v1/client/* failure: a code of the client
// error table and a displayable message that never holds a credential.
// Management routes answer the API envelope through httpapi.WriteError.
type clientError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeClientError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(clientError{Code: code, Message: message})
}

func writeCredentialInvalid(w http.ResponseWriter) {
	writeClientError(w, http.StatusUnauthorized, "credential_invalid", "the connection key does not authenticate this request")
}

// writeClientPrincipalError answers a failed internal principal check on a
// client route. A credential failure is 401 credential_invalid; any other
// failure keeps the API envelope, since the client protocol defines no body
// for a 5xx answer.
func writeClientPrincipalError(w http.ResponseWriter, r *http.Request, err error) {
	var invalid *auth.AuthenticationError
	if errors.As(err, &invalid) {
		writeCredentialInvalid(w)
		return
	}
	httpapi.WriteError(w, r, err)
}

func writeClientNotFound(w http.ResponseWriter, _ *http.Request) {
	writeClientError(w, http.StatusNotFound, "not_found", "no client endpoint has this method and path")
}

func writeManagementNotFound(w http.ResponseWriter, r *http.Request) {
	httpapi.WriteError(w, r, &httpapi.NotFoundError{Message: "not found"})
}
