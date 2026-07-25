package httpapi

import "net/http"

// stubHandler returns 501 Not Implemented via the standard error format.
func stubHandler(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, &NotImplementedError{Message: "this endpoint is not yet implemented"})
}
