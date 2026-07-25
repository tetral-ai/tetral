// Package auth implements Engine's API-key authentication: token
// generation, deterministic digest storage, bootstrap key management,
// workspace resolution, and the HTTP middleware that pulls
// `x-api-key` from a request and attaches the resolved workspace to
// the request context. This package owns the workspace/auth execution
// boundary.
//
// The package does not import httpapi: it exposes typed errors that
// httpapi maps centrally through writeError.
package auth

// AuthenticationError indicates the request did not produce a valid
// authenticated workspace. The middleware emits this for every
// failure mode (missing header, invalid digest, revoked key) and the
// public message is intentionally identical across them so an
// attacker cannot distinguish the failure cases through error text.
// Internal log lines record outcome only ("auth=failure"); raw key
// material never appears in either the response or the logs.
type AuthenticationError struct {
	Message string
}

func (e *AuthenticationError) Error() string { return e.Message }

// ValidationError indicates a request body or parameter rejected by
// the API key management endpoints (bad name, unknown field,
// oversized body that already passed the byte cap). Maps to HTTP
// 400 invalid_request_error.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// NotFoundError indicates a workspace-scoped lookup did not return a
// row (DELETE/GET against a missing or cross-workspace api_key).
// Maps to HTTP 404 not_found_error.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// WeakBootstrapKeyError indicates the configured ENGINE_API_KEY does
// not satisfy the bootstrap strength floor: non-empty after trim and
// at least 32 bytes of secret material. Surfaced by Engine startup before
// serving requests so the failure is loud and immediate.
//
// Reason names the specific rule that was violated; raw key material
// is never embedded in Error() text.
type WeakBootstrapKeyError struct {
	Reason string
}

func (e *WeakBootstrapKeyError) Error() string {
	return "auth: ENGINE_API_KEY is too weak to act as a bootstrap secret: " + e.Reason
}

// GeneratorError indicates that the random byte source backing
// API-key generation failed. Surfaced by tests through the injected
// reader path; production callers see this only in the unlikely case
// that crypto/rand cannot supply 32 bytes.
type GeneratorError struct {
	Reason string
}

func (e *GeneratorError) Error() string {
	return "auth: API key generator failed: " + e.Reason
}
