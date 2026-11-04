// Package drive owns drives: Workspace objects holding files at paths relative
// to their root, every change kept as a version, committed in numbered
// commits. It is the only package that reads or writes drive state, and Commit
// is the only way a version comes to exist. Each Service method runs its own
// Workspace-scoped transaction, and the binding set functions run in their
// caller's; Commit, Redact, Restore, PublishSkill, DeleteSkill and Delete hold
// the drive row lock (lockDrive), which serializes them per drive whichever
// service calls.
//
// Terms: a current file is a path whose current version is not a deletion; an
// old version is any other version of the path. A path's fold key is the path
// under full Unicode case folding (FoldKey); two current files never share one.
//
// OWNS:
//   - Table drives: one row per drive with its settings (skills_path,
//     memory_path, memory_instructions, the caps max_bytes and max_files,
//     metadata), its usage (usage_bytes, usage_files), stored_bytes as the
//     daily cleanup last recorded it, last_seq and last_commit_at of its latest
//     commit, removed_change_seq and removed_change_index of the newest change
//     record the cleanup deleted, and deleted_at.
//   - Table drive_workspace_usage: the bytes and files of every current file
//     over the Workspace's drives.
//   - Table drive_files: one row per current file, with its fold_key and
//     current version.
//   - Table drive_versions: every stored version, a deletion included, with
//     its writer, superseded_at (null while current), seq and redaction.
//   - Table drive_changes: one change record per version a commit created,
//     keyed (seq, change_index), copying the version's fields and writer.
//   - Table drive_contents: content the drive holds, once per hash, at its
//     blob_key in the blob store.
//   - Table drive_blob_cleanup: blobs to delete once delete_after has passed.
//   - Table drive_upload_budgets: each Workspace's upload budget, tokens as of
//     refilled_at; a Workspace without a row has a full budget
//     (upload_budget.go).
//   - Table drive_binding_sets: the drives each session.prepare or
//     session.drives sent to a computer binding, under that instruction's
//     issue_seq as send_seq (binding_sets.go). A binding's drive set, which
//     authorizes its drive transfer, is the union of its remaining rows, a
//     drive read_write when any row gives read_write; an applied report
//     deletes the rows of every earlier send.
//   - The commit notice on NoticeChannel, a Notice of the drive and its new
//     seq.
//
// STATES:
//
//	drives:          active, deleted (deleted_at set; files, versions and
//	                 change records gone, content and row left to the cleanup).
//	drive_versions:  current (superseded_at null), old, removed (aged out or
//	                 past the count cap; absent from every read), redacted
//	                 (redacted_at set; names no content).
//
// WRITERS (callers):
//   - Create, Update, Delete, Redact, Restore, PublishSkill, DeleteSkill
//     (api).
//   - PutContent, Commit, UploadBudget.Take (api, and computer-hub for a
//     binding's drive transfer, whose commits are machine commits).
//   - RecordBindingDrives (api at a binding change; api and computer-hub at
//     every session.prepare; computer-hub at a prepared report) and
//     ApplyBindingDrivesReport (computer-hub at an applied report), each in
//     its caller's transaction, which writes the instruction or stores the
//     report.
//   - Cleaner.CleanDrive and Cleaner.DeleteDueBlobs, the daily cleanup
//     (cleanup); cleanup.go.
//
// READERS: computer-hub's drive transfer reads its pages and content through
// TransferFiles, TransferChanges, FindContent and OpenRange, and the
// drive.changed it sends at a stream start through LastSeqs (transfer.go).
//
// INVARIANTS:
//   - Every read applies retention by age against the database clock
//     (retention.go), whether or not the cleanup has deleted the rows yet.
//   - A commit decides deletions before writes, each change by the first
//     matching rule of b.1 to b.5 (commit.go), and all its records share one
//     seq; created_at never decreases as seq grows.
//   - After every commit, a drive's usage equals the size sum and count of its
//     current files, the Workspace row equals the sum over its drives, and each
//     changed path keeps at most 10 old versions.
//   - A blob is recorded in drive_blob_cleanup before it is written, so no
//     crash leaves a blob the cleanup cannot find (content.go).
//   - Path rules 1 to 6 are lexical (CheckPath, paths.go); 7 to 9 compare fold
//     keys with drive_files as the commit has changed it so far.
//   - The fixed limits and retention periods of the API are in limits.go.
//   - Delete refuses a drive that a Session not deleted binds
//     (session_drive_bindings, owned by internal/session), after the drive
//     row lock.
//
// UPDATE-WITH: internal/storage/postgresql_drive_schema.go,
// database/postgresql.json, database/roles.json.
package drive
