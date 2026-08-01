// Package protocol defines the payload, result-envelope, and capture wire
// types the sandbox helper exchanges with its callers.
//
// This is the one type set that crosses process boundaries: the helper
// produces these values, and Sandbox Service consumes tool, capture, and health
// envelopes while executing provider-owned work. Everything under
// internal/sandbox/helper/internal is private to
// the helper tree; only this package is importable from outside it.
//
// OWNS:
//   - Payload: the per-tool request the helper reads from disk
//     (schema_version, tool, tool_use_event_id, workspace_root, roots, limits,
//     input).
//   - Envelope: the single JSON result written to stdout (status, truncated,
//     error, result, metrics) and its size and error-field bounds.
//   - CaptureEnvelope / CaptureResult / CaptureError: the idle-output capture
//     wire shape, including additive successful skip markers and bounded
//     directory enumeration fields.
//   - ErrorKind and AllErrorKinds: the shared error taxonomy.
//   - SchemaVersion: pinned at 1 for every payload, envelope, and capture
//     document.
//
// STATE MACHINE: none; the package holds no runtime state and drives no
// lifecycle. It is data types plus the envelope size/error trimming helpers.
//
// INVARIANTS:
//   - Every payload, envelope, and capture document carries schema_version 1;
//     a mismatch is rejected before the value is used.
//   - A result envelope never carries payload paths, task-directory paths,
//     supervisor pids, provider identifiers, or helper-internal file names, in
//     any status. Only the raw continuation facts (next_offset, task_id,
//     totals) appear; the model-facing hint built from them is the caller's
//     work, not the helper's.
//   - Envelope emission is byte-bounded (see envelope.go); emitting sites never
//     return an oversize document except through the explicit oversize-allowed
//     path.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/protocol/payload.go
//   - internal/sandbox/helper/protocol/envelope.go
//   - internal/sandbox/helper/protocol/capture.go
//   - internal/sandbox/helper/internal/cli/cli.go
package protocol
