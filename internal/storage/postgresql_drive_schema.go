package storage

// A drive is a Workspace object holding files at paths relative to its root;
// internal/drive owns these tables and their transitions. Paths and fold keys
// use the C collation, so ordering and prefix scans follow UTF-8 bytes.
// deleted_at marks a deleted drive whose content and row the daily cleanup
// removes later.
const createPostgreSQLDrivesTable = `CREATE TABLE drives (
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 drive_id TEXT NOT NULL,
 skills_path TEXT COLLATE "C" NOT NULL DEFAULT 'skills/',
 memory_path TEXT COLLATE "C" NOT NULL DEFAULT 'memory/',
 memory_instructions TEXT CHECK(char_length(memory_instructions)<=4096),
 max_bytes BIGINT CHECK(max_bytes BETWEEN 1 AND 500000000000),
 max_files BIGINT CHECK(max_files BETWEEN 1 AND 100000),
 metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(metadata)='object'),
 usage_bytes BIGINT NOT NULL DEFAULT 0 CHECK(usage_bytes>=0),
 usage_files BIGINT NOT NULL DEFAULT 0 CHECK(usage_files>=0),
 stored_bytes BIGINT CHECK(stored_bytes>=0),
 last_seq BIGINT NOT NULL DEFAULT 0 CHECK(last_seq>=0),
 last_commit_at TIMESTAMPTZ,
 removed_change_seq BIGINT,
 removed_change_index INTEGER,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 deleted_at TIMESTAMPTZ,
 PRIMARY KEY(workspace_id,drive_id),
 CHECK((last_seq=0)=(last_commit_at IS NULL)),
 CHECK((removed_change_seq IS NULL)=(removed_change_index IS NULL))
)`

const createPostgreSQLDriveWorkspaceUsageTable = `CREATE TABLE drive_workspace_usage (
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 bytes BIGINT NOT NULL DEFAULT 0 CHECK(bytes>=0),
 files BIGINT NOT NULL DEFAULT 0 CHECK(files>=0),
 PRIMARY KEY(workspace_id)
)`

// One row per current file: a path whose current version is not a deletion.
// fold_key is the path under full Unicode case folding; its unique index keeps
// two paths that differ only in case out of one drive.
const createPostgreSQLDriveFilesTable = `CREATE TABLE drive_files (
 workspace_id TEXT NOT NULL,
 drive_id TEXT NOT NULL,
 path TEXT COLLATE "C" NOT NULL,
 fold_key TEXT COLLATE "C" NOT NULL,
 version_id TEXT NOT NULL,
 content_sha256 TEXT NOT NULL CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 size_bytes BIGINT NOT NULL CHECK(size_bytes>=0),
 executable BOOLEAN NOT NULL,
 PRIMARY KEY(workspace_id,drive_id,path),
 FOREIGN KEY(workspace_id,drive_id) REFERENCES drives(workspace_id,drive_id)
)`

// A writer is stored as its kind plus its ID; a session_actor also names the
// computer. A deletion holds no content (size 0, not executable).
const createPostgreSQLDriveVersionsTable = `CREATE TABLE drive_versions (
 workspace_id TEXT NOT NULL,
 drive_id TEXT NOT NULL,
 version_id TEXT NOT NULL,
 path TEXT COLLATE "C" NOT NULL,
 operation TEXT NOT NULL CHECK(operation IN ('created','modified','deleted')),
 content_sha256 TEXT CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 size_bytes BIGINT NOT NULL CHECK(size_bytes>=0),
 executable BOOLEAN NOT NULL,
 writer_kind TEXT NOT NULL CHECK(writer_kind IN ('api_actor','user_actor','service_account_actor','session_actor')),
 writer_id TEXT NOT NULL CHECK(char_length(writer_id)>=1),
 writer_computer_id TEXT,
 created_at TIMESTAMPTZ NOT NULL,
 superseded_at TIMESTAMPTZ,
 seq BIGINT NOT NULL CHECK(seq>=1),
 redacted_at TIMESTAMPTZ,
 redacted_by_kind TEXT CHECK(redacted_by_kind IN ('api_actor','user_actor','service_account_actor','session_actor')),
 redacted_by_id TEXT,
 redacted_by_computer_id TEXT,
 PRIMARY KEY(workspace_id,drive_id,version_id),
 FOREIGN KEY(workspace_id,drive_id) REFERENCES drives(workspace_id,drive_id),
 CHECK((operation='deleted')=(content_sha256 IS NULL)),
 CHECK(operation<>'deleted' OR (size_bytes=0 AND NOT executable)),
 CHECK((writer_kind='session_actor')=(writer_computer_id IS NOT NULL)),
 CHECK((redacted_at IS NULL)=(redacted_by_kind IS NULL) AND (redacted_at IS NULL)=(redacted_by_id IS NULL)),
 CHECK((redacted_by_kind IS NOT DISTINCT FROM 'session_actor')=(redacted_by_computer_id IS NOT NULL))
)`

// A change record copies its version's content fields and writer, so it stays
// listable after the version is removed.
const createPostgreSQLDriveChangesTable = `CREATE TABLE drive_changes (
 workspace_id TEXT NOT NULL,
 drive_id TEXT NOT NULL,
 seq BIGINT NOT NULL CHECK(seq>=1),
 change_index INTEGER NOT NULL CHECK(change_index>=0),
 path TEXT COLLATE "C" NOT NULL,
 operation TEXT NOT NULL CHECK(operation IN ('created','modified','deleted')),
 version_id TEXT NOT NULL,
 content_sha256 TEXT CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 size_bytes BIGINT NOT NULL CHECK(size_bytes>=0),
 executable BOOLEAN NOT NULL,
 writer_kind TEXT NOT NULL CHECK(writer_kind IN ('api_actor','user_actor','service_account_actor','session_actor')),
 writer_id TEXT NOT NULL CHECK(char_length(writer_id)>=1),
 writer_computer_id TEXT,
 created_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(workspace_id,drive_id,seq,change_index),
 FOREIGN KEY(workspace_id,drive_id) REFERENCES drives(workspace_id,drive_id),
 CHECK((operation='deleted')=(content_sha256 IS NULL)),
 CHECK((writer_kind='session_actor')=(writer_computer_id IS NOT NULL))
)`

// Content a drive holds, stored once per drive and hash at blob_key.
const createPostgreSQLDriveContentsTable = `CREATE TABLE drive_contents (
 workspace_id TEXT NOT NULL,
 drive_id TEXT NOT NULL,
 content_sha256 TEXT NOT NULL CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 size_bytes BIGINT NOT NULL CHECK(size_bytes>=0),
 blob_key TEXT NOT NULL UNIQUE,
 uploaded_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(workspace_id,drive_id,content_sha256),
 FOREIGN KEY(workspace_id,drive_id) REFERENCES drives(workspace_id,drive_id)
)`

// Blobs to delete once delete_after has passed: every upload records its key
// here before it writes, so a crash leaves nothing the cleanup cannot find.
const createPostgreSQLDriveBlobCleanupTable = `CREATE TABLE drive_blob_cleanup (
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 blob_key TEXT NOT NULL,
 delete_after TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(workspace_id,blob_key)
)`

// One row per Workspace that has taken from its upload budget; a Workspace
// without one has a full budget.
const createPostgreSQLDriveUploadBudgetsTable = `CREATE TABLE drive_upload_budgets (
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 tokens NUMERIC NOT NULL CHECK(tokens>=0),
 refilled_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(workspace_id)
)`

func postgresqlDriveTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_drives", createPostgreSQLDrivesTable},
		{"create_drive_workspace_usage", createPostgreSQLDriveWorkspaceUsageTable},
		{"create_drive_files", createPostgreSQLDriveFilesTable},
		{"create_drive_versions", createPostgreSQLDriveVersionsTable},
		{"create_drive_changes", createPostgreSQLDriveChangesTable},
		{"create_drive_contents", createPostgreSQLDriveContentsTable},
		{"create_drive_blob_cleanup", createPostgreSQLDriveBlobCleanupTable},
		{"create_drive_upload_budgets", createPostgreSQLDriveUploadBudgetsTable},
		{"index_drives_workspace_created", `CREATE INDEX idx_drives_workspace_created ON drives(workspace_id,created_at,drive_id)`},
		{"index_drive_files_fold_key", `CREATE UNIQUE INDEX idx_drive_files_fold_key ON drive_files(workspace_id,drive_id,fold_key text_pattern_ops)`},
		{"index_drive_versions_path_created", `CREATE INDEX idx_drive_versions_path_created ON drive_versions(workspace_id,drive_id,path,created_at)`},
		{"index_drive_versions_current", `CREATE UNIQUE INDEX idx_drive_versions_current ON drive_versions(workspace_id,drive_id,path) WHERE superseded_at IS NULL`},
		{"index_drive_changes_created", `CREATE INDEX idx_drive_changes_created ON drive_changes(workspace_id,drive_id,created_at)`},
		{"index_drive_blob_cleanup_due", `CREATE INDEX idx_drive_blob_cleanup_due ON drive_blob_cleanup(delete_after)`},
	}
}
