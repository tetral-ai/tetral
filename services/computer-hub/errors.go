package computerhub

import (
	"errors"
	"net/http"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// Every /v1/client/* failure answers the client error body: a code of the
// client error table and a displayable message that never holds a
// credential (errorAnswer). Management routes answer the API envelope
// through httpapi.WriteError.

func writeCredentialInvalid(w http.ResponseWriter) {
	writeClientAnswer(w, errorAnswer(protocol.ErrorCredentialInvalid, "the connection key does not authenticate this request"))
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
	writeClientAnswer(w, errorAnswer(protocol.ErrorNotFound, "no client endpoint has this method and path"))
}

func writeManagementNotFound(w http.ResponseWriter, r *http.Request) {
	httpapi.WriteError(w, r, &httpapi.NotFoundError{Message: "not found"})
}
