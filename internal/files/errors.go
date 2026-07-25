package files

// ValidationError indicates a malformed Files request or invalid
// Files-domain input. Public Message text must not echo raw uploaded
// bytes, local paths, BlobStore keys, bucket names, or provider errors.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// RequestTooLargeError indicates an upload exceeded a documented size
// cap. HTTP maps this to request_too_large.
type RequestTooLargeError struct {
	Message string
}

func (e *RequestTooLargeError) Error() string { return e.Message }

// NotFoundError indicates a file identity does not exist, is deleted,
// or is not visible in the caller's workspace.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// PermissionError indicates the file exists but cannot be accessed
// through the requested public boundary.
type PermissionError struct {
	Message string
}

func (e *PermissionError) Error() string { return e.Message }

// ConflictError indicates a create-only storage collision or other
// state conflict.
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

// QuotaError indicates a workspace exceeded a fixed Files quota.
type QuotaError struct {
	Kind    QuotaKind
	Message string
}

func (e *QuotaError) Error() string { return e.Message }

// QuotaKind is the failure class for QuotaError.
type QuotaKind int

const (
	// QuotaKindCount marks file identity count exhaustion.
	QuotaKindCount QuotaKind = iota + 1
	// QuotaKindRetainedBytes marks retained file-object byte exhaustion.
	QuotaKindRetainedBytes
)
