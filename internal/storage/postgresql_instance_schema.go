package storage

// An instance is one registration of a CLI with a Workspace; internal/instance
// owns these tables and their transitions. A user instance carries its display
// name, a cloud instance has none. Deregistration keeps the row as a tombstone,
// so an instance ID is never reused.
const createPostgreSQLInstancesTable = `CREATE TABLE instances (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL REFERENCES workspaces(id),
 id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('user','cloud')),
 name TEXT CHECK(char_length(name) BETWEEN 1 AND 256),
 registration_state TEXT NOT NULL CHECK(registration_state IN ('active','deregistered')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 deregistered_at TIMESTAMPTZ,
 cli_version TEXT CHECK(char_length(cli_version) BETWEEN 1 AND 256),
 PRIMARY KEY(id),
 UNIQUE(workspace_id,id),
 CHECK((kind='user')=(name IS NOT NULL)),
 CHECK((registration_state='deregistered')=(deregistered_at IS NOT NULL))
)`

// A connection credential stores only the SHA-256 digest of its key. The
// stored state is pending, active or revoked; expiry is read from expires_at
// against the database clock, never stored.
const createPostgreSQLInstanceCredentialsTable = `CREATE TABLE instance_credentials (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','active','revoked')),
 key_digest BYTEA NOT NULL UNIQUE CHECK(octet_length(key_digest)=32),
 issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 expires_at TIMESTAMPTZ NOT NULL,
 revoked_at TIMESTAMPTZ,
 revocation_cause TEXT CHECK(revocation_cause IN ('delete','deregistration')),
 PRIMARY KEY(id),
 UNIQUE(workspace_id,id),
 FOREIGN KEY(workspace_id,instance_id) REFERENCES instances(workspace_id,id),
 CHECK(expires_at>issued_at),
 CHECK((state='revoked')=(revoked_at IS NOT NULL)),
 CHECK((revoked_at IS NULL)=(revocation_cause IS NULL))
)`

// One row per issuing request's Idempotency-Key, scoped to the principal's
// credential and the route (method and concrete path). The fingerprint is the
// SHA-256 of the canonical request body; the result identity is the instance
// and credential the request created. The key is stored as bytes because a
// header value need not be UTF-8.
const createPostgreSQLInstanceIdempotencyRecordsTable = `CREATE TABLE instance_idempotency_records (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 principal_credential_id TEXT NOT NULL CHECK(char_length(principal_credential_id) BETWEEN 1 AND 256),
 route TEXT NOT NULL CHECK(char_length(route) BETWEEN 1 AND 512),
 idempotency_key BYTEA NOT NULL CHECK(octet_length(idempotency_key) BETWEEN 1 AND 255),
 fingerprint BYTEA NOT NULL CHECK(octet_length(fingerprint)=32),
 instance_id TEXT NOT NULL,
 credential_id TEXT NOT NULL,
 status INTEGER NOT NULL CHECK(status=201),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(workspace_id,principal_credential_id,route,idempotency_key),
 FOREIGN KEY(workspace_id,instance_id) REFERENCES instances(workspace_id,id),
 FOREIGN KEY(workspace_id,credential_id) REFERENCES instance_credentials(workspace_id,id)
)`

// One row per connection of an instance; the latest one stays after it ends,
// for the instance read. hub_pod names the pod that holds the stream and gets
// its notices. A ready is accepted (ready_accepted_at and the ready fields)
// or refused (ready_refused_code and message), never both; ready_message_id
// and its fingerprint are its replay record, as last_heartbeat_message_id and
// its fingerprint are the latest accepted heartbeat's. ended_at and end_cause
// are written once, by the end step. At most one connection of an instance
// has not ended. last_issue_seq is the issue sequence of the connection's
// latest Session instruction, and first_session_report_at the time its first
// Session report was stored.
const createPostgreSQLInstanceConnectionsTable = `CREATE TABLE instance_connections (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 id TEXT NOT NULL CHECK(id ~ '^conn_[0-9a-f]{32}$'),
 credential_id TEXT NOT NULL,
 hub_pod TEXT NOT NULL CHECK(char_length(hub_pod) BETWEEN 1 AND 253),
 connected_at TIMESTAMPTZ NOT NULL,
 lease_expires_at TIMESTAMPTZ NOT NULL,
 stream_count INTEGER NOT NULL CHECK(stream_count>=1),
 ready_message_id TEXT,
 ready_fingerprint BYTEA CHECK(octet_length(ready_fingerprint)=32),
 ready_accepted_at TIMESTAMPTZ,
 ready_valid_for_ms INTEGER CHECK(ready_valid_for_ms BETWEEN 1 AND 45000),
 os TEXT CHECK(os IN ('linux','macos')),
 arch TEXT CHECK(arch IN ('x86_64','aarch64')),
 cli_version TEXT CHECK(char_length(cli_version) BETWEEN 1 AND 256),
 capabilities JSONB CHECK(jsonb_typeof(capabilities)='object'),
 ready_refused_code TEXT CHECK(ready_refused_code IN ('cli_upgrade_required')),
 ready_refused_message TEXT,
 last_heartbeat_message_id TEXT,
 last_heartbeat_fingerprint BYTEA CHECK(octet_length(last_heartbeat_fingerprint)=32),
 last_heartbeat_seq BIGINT CHECK(last_heartbeat_seq>0),
 last_heartbeat_at TIMESTAMPTZ,
 last_heartbeat_valid_for_ms INTEGER CHECK(last_heartbeat_valid_for_ms BETWEEN 1 AND 45000),
 ended_at TIMESTAMPTZ,
 end_cause TEXT CHECK(end_cause IN ('lease_expired','revoked','deregistered')),
 last_issue_seq BIGINT NOT NULL DEFAULT 0 CHECK(last_issue_seq>=0),
 first_session_report_at TIMESTAMPTZ,
 PRIMARY KEY(id),
 UNIQUE(workspace_id,id),
 FOREIGN KEY(workspace_id,instance_id) REFERENCES instances(workspace_id,id),
 FOREIGN KEY(workspace_id,credential_id) REFERENCES instance_credentials(workspace_id,id),
 CHECK((ready_message_id IS NULL)=(ready_fingerprint IS NULL)),
 CHECK((ready_accepted_at IS NULL)=(ready_valid_for_ms IS NULL)),
 CHECK((ready_accepted_at IS NULL)=(os IS NULL) AND (os IS NULL)=(arch IS NULL) AND (arch IS NULL)=(cli_version IS NULL) AND (cli_version IS NULL)=(capabilities IS NULL)),
 CHECK((ready_refused_code IS NULL)=(ready_refused_message IS NULL)),
 CHECK(ready_accepted_at IS NULL OR ready_refused_code IS NULL),
 CHECK((ready_message_id IS NULL)=(ready_accepted_at IS NULL AND ready_refused_code IS NULL)),
 CHECK((last_heartbeat_message_id IS NULL)=(last_heartbeat_fingerprint IS NULL) AND (last_heartbeat_fingerprint IS NULL)=(last_heartbeat_seq IS NULL)
   AND (last_heartbeat_seq IS NULL)=(last_heartbeat_at IS NULL) AND (last_heartbeat_at IS NULL)=(last_heartbeat_valid_for_ms IS NULL)),
 CHECK(last_heartbeat_seq IS NULL OR ready_accepted_at IS NOT NULL),
 CHECK((ended_at IS NULL)=(end_cause IS NULL))
)`

// The replay ledger of a connection's accepted upstream messages other than
// ready and heartbeat, whose records the connection row holds. A row is keyed
// by the SHA-256 of its message_id, since a message_id has no length limit of
// its own, and holds the route, the canonical body fingerprint and the exact
// answer. The end step deletes a connection's rows.
const createPostgreSQLConnectionUpstreamMessagesTable = `CREATE TABLE connection_upstream_messages (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 connection_id TEXT NOT NULL,
 message_digest BYTEA NOT NULL CHECK(octet_length(message_digest)=32),
 message_id TEXT NOT NULL CHECK(char_length(message_id)>=1),
 route TEXT NOT NULL CHECK(char_length(route) BETWEEN 1 AND 128),
 fingerprint BYTEA NOT NULL CHECK(octet_length(fingerprint)=32),
 status INTEGER NOT NULL CHECK(status BETWEEN 200 AND 599),
 body BYTEA NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(workspace_id,connection_id,message_digest),
 FOREIGN KEY(workspace_id,connection_id) REFERENCES instance_connections(workspace_id,id)
)`

// One row per Session instruction issued on a connection, unique by
// message_id over all connections. issue_seq numbers a connection's
// instructions from 1 in the commit order of their issuing transactions.
// envelope is the exact SSE envelope every write sends, trace_context the
// traceparent of the work that issued it; first_written_at is set by the first
// write to any stream and reported_at once the instruction's final report is
// stored.
const createPostgreSQLConnectionInstructionsTable = `CREATE TABLE connection_instructions (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 connection_id TEXT NOT NULL,
 message_id TEXT NOT NULL CHECK(char_length(message_id)>=1),
 issue_seq BIGINT NOT NULL CHECK(issue_seq>0),
 type TEXT NOT NULL CHECK(type IN ('session.prepare','session.close','session.drives','tool.execute','tool.stdin','tool.cancel','mcp.call')),
 envelope BYTEA NOT NULL,
 trace_context TEXT CHECK(trace_context ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 first_written_at TIMESTAMPTZ,
 reported_at TIMESTAMPTZ,
 PRIMARY KEY(message_id),
 UNIQUE(workspace_id,connection_id,issue_seq),
 FOREIGN KEY(workspace_id,connection_id) REFERENCES instance_connections(workspace_id,id)
)`

// One row per accepted protocol-error report. request_message_id is stored as
// the CLI gave it, whether or not it names a message Engine sent.
const createPostgreSQLConnectionProtocolErrorsTable = `CREATE TABLE connection_protocol_errors (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 connection_id TEXT NOT NULL,
 message_id TEXT NOT NULL CHECK(char_length(message_id)>=1),
 code TEXT NOT NULL CHECK(code IN ('message_conflict','invalid_message')),
 message TEXT NOT NULL,
 request_message_id TEXT CHECK(char_length(request_message_id)>=1),
 created_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(storage_sequence),
 FOREIGN KEY(workspace_id,connection_id) REFERENCES instance_connections(workspace_id,id)
)`

// Auth resolves a connection key before it knows the Workspace. The function
// returns the digest's holder whatever the credential's state: Auth decides
// only the credential class and identity, and computer-hub checks the state on
// every request. Its auth_lookup policy is SELECT-only.
const createPostgreSQLAuthLookupConnectionKey = `CREATE FUNCTION public.tetral_auth_lookup_connection_key(bytea)
RETURNS TABLE(workspace_id text,instance_id text,credential_id text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.auth_lookup','true',true);
 RETURN QUERY SELECT c.workspace_id,c.instance_id,c.id FROM public.instance_credentials c WHERE c.key_digest=$1;
END $$`

// The hub's instruction pressure gauges count the unreported instructions of
// every connection that has not ended, across Workspaces: in_progress those
// written to a stream, queued the others, and the age at $1 of the oldest
// queued one. The function sets tetral.instruction_pressure, and the
// SELECT-only instruction_pressure policies admit a row only when that flag is
// set and current_user is the table owner.
const createPostgreSQLComputerHubInstructionPressure = `CREATE FUNCTION public.tetral_computer_hub_instruction_pressure(timestamptz)
RETURNS TABLE(in_progress bigint,queued bigint,oldest_queued_seconds double precision)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.instruction_pressure','true',true);
 RETURN QUERY SELECT count(*) FILTER (WHERE i.first_written_at IS NOT NULL),
   count(*) FILTER (WHERE i.first_written_at IS NULL),
   COALESCE(GREATEST(0,EXTRACT(EPOCH FROM $1-min(i.created_at) FILTER (WHERE i.first_written_at IS NULL)))::double precision,0)
  FROM public.instance_connections c JOIN public.connection_instructions i ON i.workspace_id=c.workspace_id AND i.connection_id=c.id
  WHERE c.ended_at IS NULL AND i.reported_at IS NULL;
END $$`

func instructionPressurePolicy(table string) string {
	return `CREATE POLICY instruction_pressure ON ` + table + ` FOR SELECT USING (current_setting('tetral.instruction_pressure', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.` + table + `'::regclass))`
}

const postgresqlInstanceCredentialsAuthLookupPredicate = `current_setting('tetral.auth_lookup', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.instance_credentials'::regclass)`

func postgresqlInstanceTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_instances", createPostgreSQLInstancesTable},
		{"create_instance_credentials", createPostgreSQLInstanceCredentialsTable},
		{"create_instance_idempotency_records", createPostgreSQLInstanceIdempotencyRecordsTable},
		{"create_instance_connections", createPostgreSQLInstanceConnectionsTable},
		{"create_connection_upstream_messages", createPostgreSQLConnectionUpstreamMessagesTable},
		{"create_connection_protocol_errors", createPostgreSQLConnectionProtocolErrorsTable},
		{"create_connection_instructions", createPostgreSQLConnectionInstructionsTable},
		{"index_instances_workspace_seq", `CREATE INDEX idx_instances_workspace_seq ON instances(workspace_id,storage_sequence)`},
		{"index_instance_credentials_instance_seq", `CREATE INDEX idx_instance_credentials_instance_seq ON instance_credentials(workspace_id,instance_id,storage_sequence)`},
		{"index_instance_connections_instance_seq", `CREATE INDEX idx_instance_connections_instance_seq ON instance_connections(workspace_id,instance_id,storage_sequence)`},
		{"index_instance_connections_unended", `CREATE UNIQUE INDEX idx_instance_connections_unended ON instance_connections(workspace_id,instance_id) WHERE ended_at IS NULL`},
		{"index_instance_connections_lease", `CREATE INDEX idx_instance_connections_lease ON instance_connections(lease_expires_at) WHERE ended_at IS NULL`},
		{"index_connection_protocol_errors_connection", `CREATE INDEX idx_connection_protocol_errors_connection ON connection_protocol_errors(workspace_id,connection_id,storage_sequence)`},
		{"index_connection_instructions_unreported", `CREATE INDEX idx_connection_instructions_unreported ON connection_instructions(workspace_id,connection_id,issue_seq) WHERE reported_at IS NULL`},
	}
}

func postgresqlInstanceFunctionSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"rls_policy_auth_lookup_instance_credentials", `CREATE POLICY auth_lookup ON instance_credentials FOR SELECT USING (` + postgresqlInstanceCredentialsAuthLookupPredicate + `)`},
		{"create_auth_lookup_connection_key", createPostgreSQLAuthLookupConnectionKey},
		{"revoke_auth_lookup_connection_key_public", `REVOKE ALL ON FUNCTION public.tetral_auth_lookup_connection_key(bytea) FROM PUBLIC`},
		{"rls_policy_instruction_pressure_instance_connections", instructionPressurePolicy("instance_connections")},
		{"rls_policy_instruction_pressure_connection_instructions", instructionPressurePolicy("connection_instructions")},
		{"create_computer_hub_instruction_pressure", createPostgreSQLComputerHubInstructionPressure},
		{"revoke_computer_hub_instruction_pressure_public", `REVOKE ALL ON FUNCTION public.tetral_computer_hub_instruction_pressure(timestamptz) FROM PUBLIC`},
	}
}
