// Package imagestore holds every image and document that a client posts or a
// tool returns, as attachments that callers keep only by reference. An
// attachment's bytes reach a model request only through Resolve.
//
// OWNS:
//   - Table session_attachments: one row per stored attachment of a Session
//     thread, with its media type, size and the SHA-256 of its bytes. A row is
//     live while created_at, the database clock of the storing transaction,
//     plus the lifetime lies in the future; no read consumes it.
//   - Table session_attachment_uploads: one entry per upload whose caller has
//     not committed its row, with the hash and check_after, one hour after
//     the upload's database clock.
//   - The blob attachments/<workspace_id>/<content_sha256>: the bytes, stored
//     once per Workspace and hash and kept while a row or an upload entry of
//     that Workspace names the hash.
//   - The advisory lock of (workspace_id, content_sha256), which Upload holds
//     while it records its entry and every deleter of the blob holds while it
//     checks the rows and entries and deletes it. Store takes none: its row
//     and the deletion of its entry commit together, so a deleter sees one or
//     the other.
//
// Writers:
//   - Upload, before the caller's transaction: the entry, in its own
//     transaction, then the bytes.
//   - Store, in the caller's transaction, so the row commits with the caller's
//     event or result row; it deletes the upload's entry.
//   - CollectExpired, Bridge's collection of rows past their lifetime and of
//     upload entries past check_after across Workspaces, through the
//     SELECT-only attachment_gc policies that the transaction setting
//     tetral.attachment_gc enables.
//   - DeleteSession, Job Runner's Session deletion cleanup.
//
// Readers: Resolve, Bridge's ResolveAttachment, the only path from stored
// bytes to a model request.
//
// Media types: image/jpeg, image/png, image/gif, image/webp, application/pdf
// and text/plain. Callers check type and size before Upload; uploading any
// other type is a defect.
//
// UPDATE-WITH:
//
//	internal/storage/postgresql_schema.go (session_attachments with its media
//	  type constraint, session_attachment_uploads and their attachment_gc
//	  policies), internal/schemaidentity,
//	  database/postgresql.json and database/roles.json;
//	services/bridge/bridge_api_attachments.go (ResolveAttachment and the
//	  collection task) and services/job-runner/runtime_session_cleanup.go
//	  (Session deletion).
package imagestore
