package wire

import (
	"slices"
	"strings"
)

// ErrorCode is a code of the client error table. Each code has one HTTP
// status, which Status gives.
type ErrorCode string

// errorStatus is the client error table. A 5xx answer carries no defined
// body and is never decoded.
var errorStatus = map[ErrorCode]int{
	"invalid_message":            400,
	"content_hash_mismatch":      400,
	"credential_invalid":         401,
	"credential_expired":         401,
	"credential_revoked":         401,
	"instance_unavailable":       403,
	"cli_upgrade_required":       403,
	"drive_access_denied":        403,
	"not_found":                  404,
	"content_not_found":          404,
	"connection_conflict":        409,
	"previous_connection_active": 409,
	"stale_connection":           409,
	"stale_binding":              409,
	"content_missing":            409,
	"message_conflict":           409,
	"unknown_instruction":        409,
	"drive_cursor_expired":       410,
	"request_too_large":          413,
	"file_too_large":             413,
	"range_not_satisfiable":      416,
	"rate_limited":               429,
}

// Status is the HTTP status the code is sent with.
func (code ErrorCode) Status() int { return errorStatus[code] }

func (code *ErrorCode) check() error {
	_, known := errorStatus[*code]
	return want(known, "a code of the client error table")
}

// MissingContentMax bounds the hashes one content_missing error lists: the
// changes of one commit.
const MissingContentMax = CommitMaxChanges

// ErrorBody is the body of every /v1/client/* error response.
// RetryAfterMs comes exactly with previous_connection_active and
// MissingSha256 exactly with content_missing.
type ErrorBody struct {
	Code ErrorCode `json:"code"`
	// Message is displayable text that never holds a connection key.
	Message       string        `json:"message"`
	RetryAfterMs  Opt[int64]    `json:"retry_after_ms,omitzero" wire:"min=0,max=45000"`
	MissingSha256 Opt[[]Sha256] `json:"missing_sha256,omitzero" wire:"min=1,max=10000,unique"`
}

func (ErrorBody) MaxBytes() int { return MessageMaxBytes }

func (body *ErrorBody) check() error {
	if err := want(body.RetryAfterMs.Set == (body.Code == "previous_connection_active"),
		"retry_after_ms exactly with previous_connection_active"); err != nil {
		return err
	}
	if err := want(body.MissingSha256.Set == (body.Code == "content_missing"),
		"missing_sha256 exactly with content_missing"); err != nil {
		return err
	}
	return rule(!strings.Contains(body.Message, ConnectionKeyPrefix), "error message holds a connection key")
}

// DecodeClientError reads the error body of a connection endpoint's answer
// with status.
func DecodeClientError(status int, input []byte) (ErrorBody, error) {
	body, err := Decode[ErrorBody](input)
	if err != nil {
		return body, err
	}
	if err := rule(body.Code.Status() == status, "error code not sent with this status"); err != nil {
		return body, classify(err)
	}
	return body, nil
}

// driveTransferErrorBody is an error body on a drive transfer endpoint. The
// protocol gives it no size of its own, so the largest drive transfer body
// bounds it.
type driveTransferErrorBody struct{ ErrorBody }

func (driveTransferErrorBody) MaxBytes() int { return CommitResponseMaxJSONBytes }

// DecodeDriveTransferError reads the error body of endpoint's answer with
// status.
func DecodeDriveTransferError(endpoint DriveTransferEndpoint, status int, input []byte) (ErrorBody, error) {
	body, err := Decode[driveTransferErrorBody](input)
	if err != nil {
		return body.ErrorBody, err
	}
	sent := body.Code.Status() == status && slices.Contains(endpoint.Spec().Errors, body.Code)
	if err := rule(sent, "error code not sent by this endpoint with this status"); err != nil {
		return body.ErrorBody, classify(err)
	}
	return body.ErrorBody, nil
}

// DriveTransferEndpoint names one drive transfer endpoint, as the members of
// DriveTransferEndpoints do.
type DriveTransferEndpoint string

const (
	ListFiles       DriveTransferEndpoint = "list_files"
	ListChanges     DriveTransferEndpoint = "list_changes"
	UploadContent   DriveTransferEndpoint = "upload_content"
	Commit          DriveTransferEndpoint = "commit"
	DownloadContent DriveTransferEndpoint = "download_content"
)

// DriveTransferEndpoints lists every drive transfer endpoint.
var DriveTransferEndpoints = []DriveTransferEndpoint{ListFiles, ListChanges, UploadContent, Commit, DownloadContent}

// EndpointSpec is one row of DriveTransferEndpoints.
type EndpointSpec struct {
	Method string
	// Path has `{drive_id}` and `{sha256}` for its parameters.
	Path string
	// Errors are the codes the endpoint answers with, each with its own
	// status.
	Errors []ErrorCode
}

var accessErrors = []ErrorCode{"invalid_message", "credential_expired", "credential_invalid", "credential_revoked",
	"drive_access_denied", "instance_unavailable", "stale_binding", "stale_connection"}

func (endpoint DriveTransferEndpoint) Spec() EndpointSpec {
	with := func(codes ...ErrorCode) []ErrorCode { return append(slices.Clone(accessErrors), codes...) }
	switch endpoint {
	case ListFiles:
		return EndpointSpec{"GET", "/v1/client/drives/{drive_id}/files", with()}
	case ListChanges:
		return EndpointSpec{"GET", "/v1/client/drives/{drive_id}/changes", with("drive_cursor_expired")}
	case UploadContent:
		return EndpointSpec{"PUT", "/v1/client/drives/{drive_id}/content/{sha256}",
			with("content_hash_mismatch", "file_too_large", "rate_limited")}
	case Commit:
		return EndpointSpec{"POST", "/v1/client/drives/{drive_id}/commits", with("content_missing", "request_too_large")}
	case DownloadContent:
		return EndpointSpec{"GET", "/v1/client/drives/{drive_id}/content/{sha256}",
			with("content_not_found", "range_not_satisfiable")}
	}
	return EndpointSpec{}
}
