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

// A request is what a cloud computer without a usable instance needs from a
// Controller: new (no current instance) or wake (an offline current instance
// instance_id). instance_id of a new request is the instance its latest claim
// created; worker_id, claimed_at and claim_credential_id keep the latest
// claim's values. At most one request of a computer is open (pending or
// claimed).
const createPostgreSQLComputerRequestsTable = `CREATE TABLE computer_requests (
 storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 workspace_id TEXT NOT NULL,
 id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 computer_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('new','wake')),
 environment_id TEXT NOT NULL,
 instance_id TEXT,
 state TEXT NOT NULL CHECK(state IN ('pending','claimed','fulfilled','failed','expired')),
 worker_id TEXT CHECK(char_length(worker_id)>=1),
 claim_credential_id TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 claimed_at TIMESTAMPTZ,
 pending_since TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(workspace_id,id),
 FOREIGN KEY(workspace_id,session_id,computer_id) REFERENCES computers(workspace_id,session_id,id) ON DELETE CASCADE,
 FOREIGN KEY(workspace_id,instance_id) REFERENCES instances(workspace_id,id),
 FOREIGN KEY(workspace_id,claim_credential_id) REFERENCES instance_credentials(workspace_id,id),
 CHECK(kind='new' OR instance_id IS NOT NULL),
 CHECK((worker_id IS NULL)=(claimed_at IS NULL)),
 CHECK((worker_id IS NULL)=(claim_credential_id IS NULL)),
 CHECK(state<>'claimed' OR (worker_id IS NOT NULL AND instance_id IS NOT NULL))
)`

// The hub's request pressure gauges count, across Workspaces, the pending
// requests, the claimed ones and the age of the oldest pending one by the
// database clock. The function sets tetral.request_pressure, and the
// SELECT-only request_pressure policy admits a row only when that flag is set
// and current_user is the table owner.
const createPostgreSQLComputerHubRequestPressure = `CREATE FUNCTION public.tetral_computer_hub_request_pressure()
RETURNS TABLE(queued bigint,in_progress bigint,oldest_queued_seconds double precision)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.request_pressure','true',true);
 RETURN QUERY SELECT count(*) FILTER (WHERE r.state='pending'),
   count(*) FILTER (WHERE r.state='claimed'),
   COALESCE(GREATEST(0,EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()-min(r.created_at) FILTER (WHERE r.state='pending')))::double precision,0)
  FROM public.computer_requests r WHERE r.state IN ('pending','claimed');
END $$`

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
		{"create_computer_requests", createPostgreSQLComputerRequestsTable},
		{"index_computer_requests_open", `CREATE UNIQUE INDEX idx_computer_requests_open ON computer_requests(workspace_id,computer_id) WHERE state IN ('pending','claimed')`},
		{"index_computer_requests_pending", `CREATE INDEX idx_computer_requests_pending ON computer_requests(workspace_id,environment_id,storage_sequence) WHERE state='pending'`},
	}
}

func postgresqlComputerFunctionSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"rls_policy_request_pressure_computer_requests", `CREATE POLICY request_pressure ON computer_requests FOR SELECT USING (current_setting('tetral.request_pressure', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.computer_requests'::regclass))`},
		{"create_computer_hub_request_pressure", createPostgreSQLComputerHubRequestPressure},
		{"revoke_computer_hub_request_pressure_public", `REVOKE ALL ON FUNCTION public.tetral_computer_hub_request_pressure() FROM PUBLIC`},
	}
}
