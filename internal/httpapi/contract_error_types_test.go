package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/tetral-ai/tetral/api"
	computerErrors "github.com/tetral-ai/tetral/internal/computer"
	driveErrors "github.com/tetral-ai/tetral/internal/drive"
	instanceErrors "github.com/tetral-ai/tetral/internal/instance"
	sessionErrors "github.com/tetral-ai/tetral/internal/session"
)

// Each error type of the contract is one the API answers: for every value of
// ErrorDetail.type in the OpenAPI file, the table names an error that is
// answered with it. An answer with a type outside the contract is refused by
// the fake developer client in every test that reaches one.
func TestEachContractErrorTypeIsAnswered(t *testing.T) {
	file, err := api.Files.ReadFile(api.File)
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Components struct {
			Schemas struct {
				ErrorDetail struct {
					Properties struct {
						Type struct {
							Enum []string `json:"enum"`
						} `json:"type"`
					} `json:"properties"`
				} `json:"ErrorDetail"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(file, &contract); err != nil {
		t.Fatal(err)
	}
	types := contract.Components.Schemas.ErrorDetail.Properties.Type.Enum
	if len(types) == 0 {
		t.Fatal("the OpenAPI file states no error type")
	}
	answeredWith := map[string]error{
		"invalid_request_error":           &ValidationError{Message: "invalid"},
		"authentication_error":            &AuthenticationError{Message: "unauthenticated"},
		"permission_error":                &sessionErrors.PermissionError{Message: "denied"},
		"not_found_error":                 &NotFoundError{Message: "absent"},
		"conflict_error":                  &ConflictError{Message: "conflict"},
		"request_too_large":               &RequestTooLargeError{Message: "large"},
		"rate_limit_error":                &driveErrors.UploadRefusedError{RetryAfterSeconds: 1},
		"api_error":                       errors.New("unclassified"),
		"not_implemented":                 &NotImplementedError{Message: "absent"},
		"idempotency_conflict_error":      &instanceErrors.IdempotencyConflictError{},
		"credential_limit_error":          &instanceErrors.CredentialLimitError{},
		"instance_offline_error":          &instanceErrors.OfflineError{},
		"instance_unavailable_error":      &instanceErrors.UnavailableError{},
		"instance_not_attachable_error":   &computerErrors.NotAttachableError{},
		"computer_already_attached_error": &computerErrors.AlreadyAttachedError{},
		"request_already_claimed_error":   &computerErrors.RequestAlreadyClaimedError{},
		"request_not_pending_error":       &computerErrors.RequestNotPendingError{},
		"request_not_claimed_error":       &computerErrors.RequestNotClaimedError{},
		"drive_cursor_expired_error":      driveErrors.ErrCursorExpired,
	}
	for _, errorType := range types {
		cause, listed := answeredWith[errorType]
		if !listed {
			t.Errorf("the contract states %s; the table names no error answered with it", errorType)
			continue
		}
		recorder := httptest.NewRecorder()
		WriteError(recorder, httptest.NewRequest(http.MethodGet, "/v1/sessions", nil), cause)
		var answer struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil || answer.Error.Type != errorType {
			t.Errorf("%T is answered %s (%v); want %s", cause, recorder.Body.String(), err, errorType)
		}
	}
	for errorType := range answeredWith {
		if !slices.Contains(types, errorType) {
			t.Errorf("the table names %s, which the contract does not state", errorType)
		}
	}
}
