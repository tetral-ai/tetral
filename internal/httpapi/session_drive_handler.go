package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/session"
)

// sessionDriveHTTPEntry is one drive binding of a request body. A missing
// field decodes as "", which the service refuses.
type sessionDriveHTTPEntry struct {
	DriveID strictSessionJSONString `json:"drive_id"`
	Access  strictSessionJSONString `json:"access"`
}

type sessionDriveAccessHTTPRequest struct {
	Access strictSessionJSONString `json:"access"`
}

// parseSessionDrives decodes the drives of a creation body.
func parseSessionDrives(raw json.RawMessage) ([]session.DriveBinding, error) {
	var entries []sessionDriveHTTPEntry
	if err := decodeStrictJSON(raw, &entries); err != nil {
		return nil, &ValidationError{Message: "drives must be a list of {drive_id, access} entries"}
	}
	bindings := make([]session.DriveBinding, len(entries))
	for i, entry := range entries {
		bindings[i] = session.DriveBinding{DriveID: string(entry.DriveID), Access: string(entry.Access)}
	}
	return bindings, nil
}

// addDrive serves POST /v1/sessions/{session_id}/drives: 201 with the new
// binding.
func (h *SessionHandler) addDrive(w http.ResponseWriter, r *http.Request) {
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readSessionBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var entry sessionDriveHTTPEntry
	if err := decodeStrictSessionJSON(body, &entry); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	binding, err := h.service.AddDrive(r.Context(), ws, chi.URLParam(r, "session_id"),
		session.DriveBinding{DriveID: string(entry.DriveID), Access: string(entry.Access)})
	if err != nil {
		writeError(w, r, err)
		return
	}
	WriteJSON(w, r, http.StatusCreated, binding)
}

// updateDrive serves POST /v1/sessions/{session_id}/drives/{drive_id}: 200
// with the binding.
func (h *SessionHandler) updateDrive(w http.ResponseWriter, r *http.Request) {
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readSessionBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var request sessionDriveAccessHTTPRequest
	if err := decodeStrictSessionJSON(body, &request); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	binding, err := h.service.UpdateDrive(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "drive_id"), string(request.Access))
	if err != nil {
		writeError(w, r, err)
		return
	}
	WriteJSON(w, r, http.StatusOK, binding)
}

// removeDrive serves DELETE /v1/sessions/{session_id}/drives/{drive_id}: 204.
func (h *SessionHandler) removeDrive(w http.ResponseWriter, r *http.Request) {
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.service.RemoveDrive(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "drive_id")); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
