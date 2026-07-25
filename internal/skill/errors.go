package skill

// Error types returned by the Skill domain. The HTTP error layer
// (engine/internal/httpapi/errors.go) maps these to the standard
// `invalid_request_error` (400), `not_found_error` (404),
// `request_too_large` (413), and `conflict_error` (409) envelopes
// via `errors.As`, so callers see the centralized response shape
// without the domain package importing httpapi.
//
// Every public Error() string is safe to surface in HTTP responses and
// ordinary logs. Validators must not embed raw package paths from client
// input, raw frontmatter content, upload bytes, object storage credentials,
// or DSNs into these messages. Constructors here accept only operator-safe
// Message strings; raw input is filtered at the origin call site.

// ValidationError indicates a malformed Skill upload, malformed Skill
// reference, frontmatter rule violation, package-shape rule
// violation, or other 400 invalid_request_error condition. Public
// Message text describes the failed rule by name, never echoes
// untrusted client bytes.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// RequestTooLargeError indicates the upload exceeded a documented
// size cap (compressed body, uncompressed total, per-file, or
// retained-byte quota). HTTP layer maps this to 413 request_too_large.
type RequestTooLargeError struct {
	Message string
}

func (e *RequestTooLargeError) Error() string { return e.Message }

// NotFoundError indicates a Skill or Skill version targeted by a
// read or delete does not exist (or is soft-deleted under default
// public read semantics). HTTP layer maps this to 404 not_found_error.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// ConflictError indicates the request collided with the active
// registry state — e.g. duplicate active (workspace, runtime, name)
// at registration time, or a version-id collision the store could
// not recover from. HTTP layer maps this to 409 conflict_error.
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

// InternalError indicates an unexpected backend failure that has been
// normalized to a public-safe service error. HTTP maps this through the
// centralized fallback path without exposing storage or blob details.
type InternalError struct {
	Message string
	Cause   error
}

func (e *InternalError) Error() string { return e.Message }

func (e *InternalError) Unwrap() error { return e.Cause }

// QuotaError indicates a workspace exceeded a fixed Skill registry
// quota (active+deleted Skill count, per-Skill version count, or
// retained compressed bytes). The contract distinguishes count-limit
// failures (400 invalid_request_error) from retained-byte failures
// (413 request_too_large); the HTTP layer routes via Kind.
type QuotaError struct {
	Kind    QuotaKind
	Message string
}

// QuotaKind is the failure kind for QuotaError. Count exhaustion
// surfaces as 400; retained-byte exhaustion surfaces as 413.
type QuotaKind int

const (
	// QuotaKindCount marks a Skill-count or version-count overflow.
	QuotaKindCount QuotaKind = iota + 1
	// QuotaKindRetainedBytes marks the workspace-retained-bytes overflow.
	QuotaKindRetainedBytes
)

func (e *QuotaError) Error() string { return e.Message }
