package storage

// Cleanup scheduling keeps one global maintenance row: the elected scheduler's
// owner generation and its durable position in the current discovery cycle. It
// carries no Workspace data, so it has no Workspace RLS; database/roles.json
// grants Cleanup SELECT/UPDATE only. Either all progress fields are NULL (no
// cycle), or a cycle cutoff exists with no position yet, or the cutoff has a
// complete (cleanup_after, session_id) position.
const createPostgreSQLCleanupScheduleCursorTable = `CREATE TABLE cleanup_schedule_cursor (
 singleton BOOLEAN PRIMARY KEY CHECK (singleton),
 owner_generation BIGINT NOT NULL DEFAULT 0 CHECK (owner_generation >= 0),
 cycle_cutoff TIMESTAMPTZ NULL,
 after_cleanup_at TIMESTAMPTZ NULL,
 after_session_id TEXT NULL,
 CONSTRAINT cleanup_schedule_cursor_progress_shape CHECK (
  (cycle_cutoff IS NULL AND after_cleanup_at IS NULL AND after_session_id IS NULL)
  OR (cycle_cutoff IS NOT NULL AND after_cleanup_at IS NULL AND after_session_id IS NULL)
  OR (cycle_cutoff IS NOT NULL AND after_cleanup_at IS NOT NULL AND after_session_id IS NOT NULL AND after_session_id <> '')
 )
)`

// Serving roles cannot insert the row, so a missing singleton stays an
// invariant failure instead of being recreated by a scheduler.
const seedPostgreSQLCleanupScheduleCursor = `INSERT INTO cleanup_schedule_cursor (singleton, owner_generation, cycle_cutoff, after_cleanup_at, after_session_id) VALUES (true, 0, NULL, NULL, NULL)`

// The global due index serves Cleanup discovery in (cleanup_after, session_id)
// order across every Workspace. Its predicate repeats the due predicate term for
// term; Session IDs are globally unique, so the pair is a total order.
const createPostgreSQLRuntimeStatusCleanupGlobalDueIndex = `CREATE INDEX idx_session_runtime_status_cleanup_global_due ON session_runtime_status(cleanup_after, session_id) WHERE status = 'idle' AND cleanup_job_id IS NULL AND binding_id IS NOT NULL`

// Only the discovery function, executing as the actual table owner with its
// purpose flag set, reads across Workspaces. A serving role that sets the flag
// itself still fails the owner predicate.
const createPostgreSQLCleanupDiscoveryPolicy = `CREATE POLICY cleanup_discovery ON session_runtime_status
 FOR SELECT
 USING (current_setting('tetral.cleanup_discovery', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.session_runtime_status'::regclass))`

// cleanupDueSessionsSelect, cleanupDueSessionsFirstPage and
// cleanupDueSessionsNextPage are the two discovery statements of
// tetral_cleanup_due_sessions: $1 is the cycle cutoff, $2/$3 the exclusive
// (cleanup_after, session_id) position and $4 the page limit. Separate
// statements keep the position an index condition under a generic plan.
const cleanupDueSessionsSelect = `SELECT s.workspace_id, s.session_id, s.cleanup_after
   FROM public.session_runtime_status s
  WHERE s.status = 'idle' AND s.cleanup_job_id IS NULL AND s.binding_id IS NOT NULL
    AND s.cleanup_after <= $1`

const cleanupDueSessionsFirstPage = cleanupDueSessionsSelect + `
  ORDER BY s.cleanup_after, s.session_id
  LIMIT $4`

const cleanupDueSessionsNextPage = cleanupDueSessionsSelect + `
    AND (s.cleanup_after, s.session_id) > ($2, $3)
  ORDER BY s.cleanup_after, s.session_id
  LIMIT $4`

// The function returns only identifiers and the due time, never Session
// content. It rejects a cutoff later than the database clock, a partly NULL or
// empty position and a limit outside 1..100 with invalid_parameter_value.
const createPostgreSQLCleanupDueSessionsFunction = `CREATE FUNCTION public.tetral_cleanup_due_sessions(timestamptz, timestamptz, text, integer)
RETURNS TABLE(workspace_id text, session_id text, cleanup_after timestamptz)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 IF $1 IS NULL OR $1 > pg_catalog.clock_timestamp() THEN
  RAISE EXCEPTION 'invalid cleanup cycle cutoff' USING ERRCODE='22023';
 END IF;
 IF ($2 IS NULL) <> ($3 IS NULL) OR $3 = '' THEN
  RAISE EXCEPTION 'invalid cleanup discovery position' USING ERRCODE='22023';
 END IF;
 IF $4 IS NULL OR $4 < 1 OR $4 > 100 THEN
  RAISE EXCEPTION 'invalid cleanup discovery limit' USING ERRCODE='22023';
 END IF;
 PERFORM pg_catalog.set_config('tetral.cleanup_discovery','true',true);
 IF $2 IS NULL THEN
  RETURN QUERY ` + cleanupDueSessionsFirstPage + `;
 ELSE
  RETURN QUERY ` + cleanupDueSessionsNextPage + `;
 END IF;
END $$`

func postgresqlCleanupScheduleTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_cleanup_schedule_cursor", createPostgreSQLCleanupScheduleCursorTable},
		{"seed_cleanup_schedule_cursor", seedPostgreSQLCleanupScheduleCursor},
	}
}

func postgresqlCleanupDiscoverySteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"rls_policy_cleanup_discovery_drop", "DROP POLICY IF EXISTS cleanup_discovery ON session_runtime_status"},
		{"rls_policy_cleanup_discovery", createPostgreSQLCleanupDiscoveryPolicy},
		{"create_cleanup_due_sessions", createPostgreSQLCleanupDueSessionsFunction},
		{"revoke_cleanup_due_sessions_public", `REVOKE ALL ON FUNCTION public.tetral_cleanup_due_sessions(timestamptz, timestamptz, text, integer) FROM PUBLIC`},
	}
}
