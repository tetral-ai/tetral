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

const postgresqlInstanceCredentialsAuthLookupPredicate = `current_setting('tetral.auth_lookup', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.instance_credentials'::regclass)`

func postgresqlInstanceTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_instances", createPostgreSQLInstancesTable},
		{"create_instance_credentials", createPostgreSQLInstanceCredentialsTable},
		{"create_instance_idempotency_records", createPostgreSQLInstanceIdempotencyRecordsTable},
		{"index_instances_workspace_seq", `CREATE INDEX idx_instances_workspace_seq ON instances(workspace_id,storage_sequence)`},
		{"index_instance_credentials_instance_seq", `CREATE INDEX idx_instance_credentials_instance_seq ON instance_credentials(workspace_id,instance_id,storage_sequence)`},
	}
}

func postgresqlInstanceFunctionSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"rls_policy_auth_lookup_instance_credentials", `CREATE POLICY auth_lookup ON instance_credentials FOR SELECT USING (` + postgresqlInstanceCredentialsAuthLookupPredicate + `)`},
		{"create_auth_lookup_connection_key", createPostgreSQLAuthLookupConnectionKey},
		{"revoke_auth_lookup_connection_key_public", `REVOKE ALL ON FUNCTION public.tetral_auth_lookup_connection_key(bytea) FROM PUBLIC`},
	}
}
