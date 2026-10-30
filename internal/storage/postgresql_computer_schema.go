package storage

// A computer is one machine of a Session: a cloud computer defined by its
// Environment or a user computer defined by its instance. internal/computer
// owns these tables. storage_sequence orders a Session's computers in the
// order they were added; a removed computer stays as a detached row as long
// as its Session.
const createPostgreSQLComputersTable = `CREATE TABLE computers (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('cloud','user')),
 environment_id TEXT,
 instance_id TEXT,
 current_instance_id TEXT,
 working_directory TEXT NOT NULL CHECK(working_directory LIKE '/%'),
 cwd TEXT,
 networking JSONB NOT NULL CHECK(jsonb_typeof(networking)='object'),
 drive_exclude JSONB NOT NULL CHECK(jsonb_typeof(drive_exclude)='array'),
 association_state TEXT NOT NULL CHECK(association_state IN ('attached','detached')),
 execution_state TEXT NOT NULL CHECK(execution_state IN ('waiting','preparing','ready','failed','closing','closed','unknown')),
 connection_id TEXT,
 binding_id TEXT,
 error_code TEXT,
 error_message TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 detached_at TIMESTAMPTZ,
 PRIMARY KEY(workspace_id,id),
 UNIQUE(workspace_id,session_id,id),
 FOREIGN KEY(workspace_id,session_id) REFERENCES sessions(workspace_id,id) ON DELETE CASCADE,
 FOREIGN KEY(workspace_id,instance_id) REFERENCES instances(workspace_id,id),
 FOREIGN KEY(workspace_id,current_instance_id) REFERENCES instances(workspace_id,id),
 CHECK((kind='cloud')=(environment_id IS NOT NULL)),
 CHECK((kind='user')=(instance_id IS NOT NULL)),
 CHECK(kind='cloud' OR current_instance_id=instance_id),
 CHECK((association_state='detached')=(detached_at IS NOT NULL)),
 CHECK((error_code IS NULL)=(error_message IS NULL))
)`

// One row per Idempotency-Key of Session creation and of a computer
// addition, scoped as instance_idempotency_records are: computer_id is NULL
// for a creation, whose result is the Session. Records live as long as their
// Session.
const createPostgreSQLComputerIdempotencyRecordsTable = `CREATE TABLE computer_idempotency_records (
 workspace_id TEXT NOT NULL,
 principal_credential_id TEXT NOT NULL CHECK(char_length(principal_credential_id) BETWEEN 1 AND 256),
 route TEXT NOT NULL CHECK(char_length(route) BETWEEN 1 AND 512),
 idempotency_key BYTEA NOT NULL CHECK(octet_length(idempotency_key) BETWEEN 1 AND 255),
 fingerprint BYTEA NOT NULL CHECK(octet_length(fingerprint)=32),
 session_id TEXT NOT NULL,
 computer_id TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(workspace_id,principal_credential_id,route,idempotency_key),
 FOREIGN KEY(workspace_id,session_id) REFERENCES sessions(workspace_id,id) ON DELETE CASCADE,
 FOREIGN KEY(workspace_id,session_id,computer_id) REFERENCES computers(workspace_id,session_id,id) ON DELETE CASCADE
)`

func postgresqlComputerTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_computers", createPostgreSQLComputersTable},
		{"create_computer_idempotency_records", createPostgreSQLComputerIdempotencyRecordsTable},
		{"foreign_key_session_github_repository_resources_computer", `ALTER TABLE session_github_repository_resources ADD FOREIGN KEY(workspace_id,session_id,computer_id) REFERENCES computers(workspace_id,session_id,id)`},
		{"index_computers_session_seq", `CREATE INDEX idx_computers_session_seq ON computers(workspace_id,session_id,storage_sequence)`},
		{"index_computers_attached_instance", `CREATE INDEX idx_computers_attached_instance ON computers(workspace_id,current_instance_id) WHERE association_state='attached'`},
		{"create_session_machine_messages", createPostgreSQLSessionMachineMessagesTable},
	}
}
