// Package imagestore holds every image and document that a client posts or a
// tool returns, as attachments that callers keep only by reference. An
// attachment's bytes reach a model request only through Resolve.
//
// OWNS:
//   - Table session_attachments: one row per stored attachment of a Session
//     thread, with its media type, size and the SHA-256 of its bytes. A row is
//     live while created_at, the database clock of the storing transaction,
//     plus the lifetime lies in the future; no read consumes it.
//   - The blob attachments/<workspace_id>/<content_sha256>: the bytes, stored
//     once per Workspace and hash and kept while a row of that Workspace names
//     the hash.
//   - The advisory lock of (workspace_id, content_sha256), which every writer
//     of the blob or of the rows naming it holds in its transaction, so a blob
//     is never deleted while a row that names it commits.
//
// Writers:
//   - Store, in the caller's transaction, so the row commits with the caller's
//     event or result row.
//   - CollectExpired, Bridge's collection of rows past their lifetime across
//     Workspaces, through the SELECT-only attachment_gc policy that the
//     transaction setting tetral.attachment_gc enables.
//   - DeleteSession, Job Runner's Session deletion cleanup.
//
// Readers: Resolve, Bridge's ResolveAttachment, the only path from stored
// bytes to a model request.
//
// Media types: image/jpeg, image/png, image/gif, image/webp, application/pdf
// and text/plain. Callers check type and size before Store; storing any other
// type is a defect.
package imagestore
