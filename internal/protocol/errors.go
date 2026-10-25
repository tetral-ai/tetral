package protocol

import (
	"fmt"
	"net/http"
	"slices"
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

// ClientErrors maps each code of the client error table to the HTTP status
// it is sent with.
var ClientErrors = map[ErrorCode]int{
	ErrorInvalidMessage:           http.StatusBadRequest,
	ErrorContentHashMismatch:      http.StatusBadRequest,
	ErrorCredentialInvalid:        http.StatusUnauthorized,
	ErrorCredentialExpired:        http.StatusUnauthorized,
	ErrorCredentialRevoked:        http.StatusUnauthorized,
	ErrorInstanceUnavailable:      http.StatusForbidden,
	ErrorCLIUpgradeRequired:       http.StatusForbidden,
	ErrorDriveAccessDenied:        http.StatusForbidden,
	ErrorNotFound:                 http.StatusNotFound,
	ErrorContentNotFound:          http.StatusNotFound,
	ErrorConnectionConflict:       http.StatusConflict,
	ErrorPreviousConnectionActive: http.StatusConflict,
	ErrorStaleConnection:          http.StatusConflict,
	ErrorStaleBinding:             http.StatusConflict,
	ErrorContentMissing:           http.StatusConflict,
	ErrorMessageConflict:          http.StatusConflict,
	ErrorUnknownInstruction:       http.StatusConflict,
	ErrorDriveCursorExpired:       http.StatusGone,
	ErrorRequestTooLarge:          http.StatusRequestEntityTooLarge,
	ErrorFileTooLarge:             http.StatusRequestEntityTooLarge,
	ErrorRangeNotSatisfiable:      http.StatusRequestedRangeNotSatisfiable,
	ErrorRateLimited:              http.StatusTooManyRequests,
}

func errorCodeNames() []string {
	names := make([]string, 0, len(ClientErrors))
	for code := range ClientErrors {
		names = append(names, string(code))
	}
	slices.Sort(names)
	return names
}

// ClientError is the status and code of one error answer.
type ClientError struct {
	Status int
	Code   ErrorCode
}

// DriveTransferEndpoints lists the error answers of each drive transfer
// endpoint, as the drive transfer schema's endpoint table states them.
var DriveTransferEndpoints = map[DriveTransferEndpoint][]ClientError{
	EndpointListFiles: transferErrors(),
	EndpointListChanges: append(transferErrors(),
		ClientError{http.StatusGone, ErrorDriveCursorExpired},
	),
	EndpointUploadContent: append(transferErrors(),
		ClientError{http.StatusBadRequest, ErrorContentHashMismatch},
		ClientError{http.StatusRequestEntityTooLarge, ErrorFileTooLarge},
		ClientError{http.StatusTooManyRequests, ErrorRateLimited},
	),
	EndpointCommit: append(transferErrors(),
		ClientError{http.StatusConflict, ErrorContentMissing},
		ClientError{http.StatusRequestEntityTooLarge, ErrorRequestTooLarge},
	),
	EndpointDownloadContent: append(transferErrors(),
		ClientError{http.StatusNotFound, ErrorContentNotFound},
		ClientError{http.StatusRequestedRangeNotSatisfiable, ErrorRangeNotSatisfiable},
	),
}

// transferErrors are the answers every drive transfer endpoint lists.
func transferErrors() []ClientError {
	return []ClientError{
		{http.StatusBadRequest, ErrorInvalidMessage},
		{http.StatusUnauthorized, ErrorCredentialExpired},
		{http.StatusUnauthorized, ErrorCredentialInvalid},
		{http.StatusUnauthorized, ErrorCredentialRevoked},
		{http.StatusForbidden, ErrorDriveAccessDenied},
		{http.StatusForbidden, ErrorInstanceUnavailable},
		{http.StatusConflict, ErrorStaleBinding},
		{http.StatusConflict, ErrorStaleConnection},
	}
}

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
// request body over its complete limit, an upload Content-Length above
// 500,000,000, a download Range naming more than one range and a commit of
// more than 10,000 changes.
func invalid(ptr, reason string) *DecodeError {
	return &DecodeError{Pointer: ptr, Status: http.StatusBadRequest, Code: ErrorInvalidMessage, Reason: reason}
}

// tooLarge answers a request body over its complete limit, and a commit of
// more than 10,000 changes, with 413 request_too_large.
func tooLarge(ptr, reason string) *DecodeError {
	return &DecodeError{Pointer: ptr, Status: http.StatusRequestEntityTooLarge, Code: ErrorRequestTooLarge, Reason: reason}
}
