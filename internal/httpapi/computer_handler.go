package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/session"
)

// computerHTTPEntry is one computer entry of Session creation and of the add
// route. A null field counts as absent.
type computerHTTPEntry struct {
	EnvironmentID    *string                 `json:"environment_id"`
	InstanceID       *string                 `json:"instance_id"`
	WorkingDirectory *string                 `json:"working_directory"`
	Networking       *computerHTTPNetworking `json:"networking"`
	Repositories     []repositoryHTTPRequest `json:"repositories"`
	DriveExclude     []computer.DriveExclude `json:"drive_exclude"`
}

type computerHTTPNetworking struct {
	Type string `json:"type"`
}

type repositoryHTTPRequest struct {
	URL                strictSessionJSONString `json:"url"`
	AuthorizationToken strictSessionJSONString `json:"authorization_token"`
	Checkout           json.RawMessage         `json:"checkout"`
	MountPath          *string                 `json:"mount_path"`
	GitIdentity        json.RawMessage         `json:"git_identity"`
}

func (e computerHTTPEntry) toServiceRequest() (session.ComputerRequest, error) {
	request := session.ComputerRequest{Entry: computer.Entry{
		EnvironmentID:    e.EnvironmentID,
		InstanceID:       e.InstanceID,
		WorkingDirectory: e.WorkingDirectory,
		DriveExclude:     e.DriveExclude,
	}}
	if e.Networking != nil {
		request.Entry.Networking = &e.Networking.Type
	}
	for _, repository := range e.Repositories {
		item := session.ResourceRequest{
			Type:               string(session.ResourceTypeGitHubRepository),
			GitHubURL:          string(repository.URL),
			AuthorizationToken: string(repository.AuthorizationToken),
			MountPath:          repository.MountPath,
		}
		if present(repository.Checkout) {
			checkout, err := decodeSessionGitHubCheckout(repository.Checkout)
			if err != nil {
				return session.ComputerRequest{}, &ValidationError{Message: err.Error()}
			}
			item.Checkout = checkout
		}
		if present(repository.GitIdentity) {
			identity, err := decodeSessionGitIdentity(repository.GitIdentity)
			if err != nil {
				return session.ComputerRequest{}, &ValidationError{Message: err.Error()}
			}
			item.GitIdentity = identity
		}
		request.Repositories = append(request.Repositories, item)
	}
	return request, nil
}

func present(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// parseComputerEntries decodes the computers of a creation body.
func parseComputerEntries(raw json.RawMessage) ([]session.ComputerRequest, error) {
	var entries []computerHTTPEntry
	if err := decodeStrictJSON(raw, &entries); err != nil {
		return nil, &ValidationError{Message: "computers must be a list of computer entries"}
	}
	requests := make([]session.ComputerRequest, 0, len(entries))
	for _, entry := range entries {
		request, err := entry.toServiceRequest()
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("unexpected trailing data")
	}
	return nil
}

// computerIdempotency reads the Idempotency-Key of a creation or addition:
// sent exactly once and holding, trimmed, 1 to 255 bytes. The scope is the
// principal's credential, the method and concrete path, and the key.
func computerIdempotency(r *http.Request, body []byte) (*session.Idempotency, error) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) == 0 {
		return nil, nil
	}
	key := ""
	if len(values) == 1 {
		key = strings.TrimSpace(values[0])
	}
	if key == "" || len(key) > 255 {
		return nil, &ValidationError{Message: "Idempotency-Key must be sent once and hold 1 to 255 bytes"}
	}
	principal, ok := auth.PrincipalFromContext(r.Context())
	if !ok {
		return nil, &AuthenticationError{Message: "authentication required"}
	}
	fingerprint, canonical := computer.Fingerprint(body)
	return &session.Idempotency{
		Scope:       instance.IdempotencyScope{PrincipalCredentialID: principal.Credential.ID, Route: r.Method + " " + r.URL.Path, Key: key},
		Fingerprint: fingerprint,
		Canonical:   canonical,
	}, nil
}

// addComputer serves POST /v1/sessions/{session_id}/computers.
func (h *SessionHandler) addComputer(w http.ResponseWriter, r *http.Request) {
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readSessionBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	idempotency, err := computerIdempotency(r, body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if idempotency == nil {
		writeError(w, r, &ValidationError{Message: "Idempotency-Key is required"})
		return
	}
	var entry computerHTTPEntry
	if err := decodeStrictJSON(body, &entry); err != nil {
		writeError(w, r, &ValidationError{Message: "request body must be one computer entry"})
		return
	}
	request, err := entry.toServiceRequest()
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := rejectComputerText(body, []session.ComputerRequest{request}); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	object, err := h.service.AddComputer(r.Context(), ws, chi.URLParam(r, "session_id"), request, *idempotency)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, object)
}

// getComputer serves GET /v1/sessions/{session_id}/computers/{computer_id}.
func (h *SessionHandler) getComputer(w http.ResponseWriter, r *http.Request) {
	if err := validateSessionBetaRoute(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	object, err := h.service.GetComputer(r.Context(), ws, chi.URLParam(r, "session_id"), chi.URLParam(r, "computer_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, object)
}

// rejectComputerText answers a working_directory holding U+0000 by its field,
// then any other U+0000 in the body as every Session route does.
func rejectComputerText(body []byte, requests []session.ComputerRequest) error {
	for _, request := range requests {
		if request.Entry.WorkingDirectory != nil && strings.ContainsRune(*request.Entry.WorkingDirectory, 0) {
			return &ValidationError{Message: "working_directory must be an absolute path without U+0000"}
		}
	}
	return rejectUnstorableJSONText(body)
}

// readSessionBody reads a Session route body up to its 1 MiB cap.
func readSessionBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, sessionBodyByteCap))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &RequestTooLargeError{Message: "request body exceeds the 1 MiB limit"}
		}
		return nil, err
	}
	return body, nil
}
