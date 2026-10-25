package protocol

import (
	"fmt"
	"net/http"
)

// ErrorCode is a code of the client error table, the codes every
// /v1/client/* error body carries.
type ErrorCode string

const (
	ErrorInvalidMessage           ErrorCode = "invalid_message"
	ErrorContentHashMismatch      ErrorCode = "content_hash_mismatch"
	ErrorCredentialInvalid        ErrorCode = "credential_invalid" //nolint:gosec // G101: error code, not a credential value
	ErrorCredentialExpired        ErrorCode = "credential_expired" //nolint:gosec // G101: error code, not a credential value
	ErrorCredentialRevoked        ErrorCode = "credential_revoked" //nolint:gosec // G101: error code, not a credential value
	ErrorInstanceUnavailable      ErrorCode = "instance_unavailable"
	ErrorCLIUpgradeRequired       ErrorCode = "cli_upgrade_required"
	ErrorDriveAccessDenied        ErrorCode = "drive_access_denied"
	ErrorNotFound                 ErrorCode = "not_found"
	ErrorContentNotFound          ErrorCode = "content_not_found"
	ErrorConnectionConflict       ErrorCode = "connection_conflict"
	ErrorPreviousConnectionActive ErrorCode = "previous_connection_active"
	ErrorStaleConnection          ErrorCode = "stale_connection"
	ErrorStaleBinding             ErrorCode = "stale_binding"
	ErrorContentMissing           ErrorCode = "content_missing"
	ErrorMessageConflict          ErrorCode = "message_conflict"
	ErrorUnknownInstruction       ErrorCode = "unknown_instruction"
	ErrorDriveCursorExpired       ErrorCode = "drive_cursor_expired"
	ErrorRequestTooLarge          ErrorCode = "request_too_large"
	ErrorFileTooLarge             ErrorCode = "file_too_large"
	ErrorRangeNotSatisfiable      ErrorCode = "range_not_satisfiable"
	ErrorRateLimited              ErrorCode = "rate_limited"
)

// DecodeError is a decoding failure: the JSON pointer of the first value that
// fails and the answer computer-hub sends for it.
type DecodeError struct {
	Pointer string
	Status  int
	Code    ErrorCode
	Reason  string
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("protocol: %s at %q", e.Reason, e.Pointer)
}

// invalid is the answer of every decoding failure, 400 invalid_message, also
// for a member over its own limit inside a body within its limit, but a
// request body over its complete limit.
func invalid(ptr, reason string) *DecodeError {
	return &DecodeError{Pointer: ptr, Status: http.StatusBadRequest, Code: ErrorInvalidMessage, Reason: reason}
}

// tooLarge answers a request body over its complete limit with 413
// request_too_large.
func tooLarge(ptr, reason string) *DecodeError {
	return &DecodeError{Pointer: ptr, Status: http.StatusRequestEntityTooLarge, Code: ErrorRequestTooLarge, Reason: reason}
}
