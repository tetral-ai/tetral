package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// WriteJSON writes v as a JSON response with the given status. It encodes v
// before it writes anything, so a value that cannot be encoded is answered
// through WriteError as a 500 that names the encoder's error as its cause,
// never as the status with an empty body.
func WriteJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		WriteError(w, r, fmt.Errorf("the response cannot be encoded: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
