package storage

import (
	"fmt"
	"strings"
)

// Feed retention metadata records, per Session feed, the highest stream
// position whose change row retention pruning has deleted. feed_key is
// 'session' for the Session feed and 'thread:<id>' for one Thread feed, so the
// key never depends on a NULL column. Rows live until a future parent purge;
// change-row expiry never removes them. Event Stream reads them; only the
// Cleanup change-retention function writes them.
const createPostgreSQLSessionEventFeedRetentionTable = `CREATE TABLE session_event_feed_retention (
 workspace_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 feed_key TEXT NOT NULL,
 session_thread_id TEXT NULL,
 pruned_through BIGINT NOT NULL CHECK (pruned_through > 0),
 PRIMARY KEY (workspace_id, session_id, feed_key),
 FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
 FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
 CONSTRAINT session_event_feed_retention_scope_shape CHECK (
  (session_thread_id IS NULL AND feed_key = 'session')
  OR (session_thread_id IS NOT NULL AND session_thread_id <> '' AND feed_key = 'thread:' || session_thread_id)
 )
)`

// The age indexes serve retention pruning in (age, primary key) order. The
// head indexes bound each feed's newest-first seek to public changes; Session
// feed reads additionally require session_visible. Neither replaces a
// uniqueness constraint.
const (
	createPostgreSQLSessionEventIdempotencyAgeIndex   = `CREATE INDEX idx_session_event_idempotency_keys_age ON session_event_idempotency_keys(created_at, workspace_id, session_id, idempotency_key_digest)`
	createPostgreSQLSessionEventStreamChangesAgeIndex = `CREATE INDEX idx_session_event_stream_changes_age ON session_event_stream_changes(changed_at, workspace_id, session_id, stream_position)`
	createPostgreSQLSessionEventStreamThreadHeadIndex = `CREATE INDEX idx_session_event_stream_changes_thread_head ON session_event_stream_changes(workspace_id, session_id, session_thread_id, stream_position DESC) WHERE visibility = 'public'`
	createPostgreSQLSessionEventStreamSessionHead     = `CREATE INDEX idx_session_event_stream_changes_session_head ON session_event_stream_changes(workspace_id, session_id, stream_position DESC) WHERE visibility = 'public' AND session_visible`
)

// jobRunnerKindSQLList is the exact five-kind Job Runner allowlist. Runner
// terminal retention repeats it in its indexes, candidate selection and DELETE,
// so it never reaches Sandbox or Environment kinds.
const jobRunnerKindSQLList = `'runtime_input', 'runtime_recovery', 'runtime_config_update', 'cleanup_session', 'session_delete_cleanup'`

func jobRunnerTerminalRetentionIndex(status, column string) string {
	return fmt.Sprintf(`CREATE INDEX idx_queue_jobs_job_runner_%s_retention ON queue_jobs(%s, id) WHERE status = '%s' AND kind IN (%s)`, status, column, status, jobRunnerKindSQLList)
}

// retentionMaintenancePredicate admits a row only inside a retention function:
// the transaction-local purpose flag must be set AND current_user must be the
// table's actual owner. A serving role that sets the flag itself gains nothing.
func retentionMaintenancePredicate(table string) string {
	return fmt.Sprintf("current_setting('tetral.retention_maintenance', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.%s'::regclass)", table)
}

// retentionMaintenanceCommands lists, per table, exactly the commands the
// owner-only retention path needs under FORCE ROW LEVEL SECURITY: receipts,
// changes and Runner terminal jobs are locked (UPDATE) and deleted; feed
// metadata is read by GREATEST, inserted and updated; permanent event and
// Thread headers are only read for eligibility.
var retentionMaintenanceCommands = []struct {
	table    string
	commands []string
}{
	{"session_event_idempotency_keys", []string{"SELECT", "UPDATE", "DELETE"}},
	{"session_event_stream_changes", []string{"SELECT", "UPDATE", "DELETE"}},
	{"session_event_feed_retention", []string{"SELECT", "INSERT", "UPDATE"}},
	{"session_events", []string{"SELECT"}},
	{"session_threads", []string{"SELECT"}},
	{"queue_jobs", []string{"SELECT", "UPDATE", "DELETE"}},
}

// Receipt pruning: $1 cutoff, $2..$5 the exclusive (created_at, workspace_id,
// session_id, idempotency_key_digest) continuation, $6 the page limit. The
// candidate page is locked in age/key order, skipping rows another transaction
// holds; the DELETE repeats the age condition on the locked current tuple.
// more_remaining is one indexed LIMIT 1 probe, with the page's own eligibility
// predicate, for another eligible row after the last examined key (after the
// caller's continuation when the page examined nothing).
const pruneEventIdempotencyPage = `WITH candidates AS MATERIALIZED (
   SELECT k.created_at, k.workspace_id, k.session_id, k.idempotency_key_digest
     FROM public.session_event_idempotency_keys k
    WHERE k.created_at <= cutoff%s
    ORDER BY k.created_at, k.workspace_id, k.session_id, k.idempotency_key_digest
    LIMIT page_limit
    FOR UPDATE OF k SKIP LOCKED
  ), deleted AS (
   DELETE FROM public.session_event_idempotency_keys k USING candidates c
    WHERE k.workspace_id = c.workspace_id AND k.session_id = c.session_id
      AND k.idempotency_key_digest = c.idempotency_key_digest AND k.created_at <= cutoff
   RETURNING k.workspace_id
  )
  SELECT (SELECT count(*) FROM deleted)::integer, (SELECT count(*) FROM candidates)::integer,
         tail.created_at, tail.workspace_id, tail.session_id, tail.idempotency_key_digest
    INTO deleted_count, examined_count, last_created_at, last_workspace_id, last_session_id, last_idempotency_key_digest
    FROM (SELECT 1) AS one
    LEFT JOIN LATERAL (
     SELECT c.created_at, c.workspace_id, c.session_id, c.idempotency_key_digest FROM candidates c
      ORDER BY c.created_at DESC, c.workspace_id DESC, c.session_id DESC, c.idempotency_key_digest DESC LIMIT 1
    ) AS tail ON true`

var createPostgreSQLPruneEventIdempotencyFunction = `CREATE FUNCTION public.tetral_prune_event_idempotency(timestamptz, timestamptz, text, text, bytea, integer)
RETURNS TABLE(deleted_count integer, examined_count integer, last_created_at timestamptz, last_workspace_id text, last_session_id text, last_idempotency_key_digest bytea, more_remaining boolean)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ DECLARE cutoff timestamptz; page_limit integer;
 from_at timestamptz; from_workspace text; from_session text; from_digest bytea; BEGIN
 IF $1 IS NULL THEN
  RAISE EXCEPTION 'invalid retention cutoff' USING ERRCODE='22023';
 END IF;
 IF NOT COALESCE(($2 IS NULL AND $3 IS NULL AND $4 IS NULL AND $5 IS NULL)
   OR ($2 IS NOT NULL AND $3 <> '' AND $4 <> '' AND octet_length($5) > 0), false) THEN
  RAISE EXCEPTION 'invalid retention continuation' USING ERRCODE='22023';
 END IF;
 IF $6 IS NULL THEN
  RAISE EXCEPTION 'invalid retention limit' USING ERRCODE='22023';
 END IF;
 cutoff := LEAST($1, pg_catalog.clock_timestamp() - interval '24 hours');
 page_limit := LEAST(GREATEST($6, 0), 256);
 PERFORM pg_catalog.set_config('tetral.retention_maintenance','true',true);
 IF $2 IS NULL THEN
  ` + pruneEventIdempotencyFirstPage + `;
 ELSE
  ` + pruneEventIdempotencyNextPage + `;
 END IF;
 IF last_created_at IS NOT NULL THEN
  from_at := last_created_at; from_workspace := last_workspace_id; from_session := last_session_id; from_digest := last_idempotency_key_digest;
 ELSE
  from_at := $2; from_workspace := $3; from_session := $4; from_digest := $5;
 END IF;
 IF from_at IS NULL THEN
  PERFORM 1 FROM public.session_event_idempotency_keys k
   WHERE k.created_at <= cutoff
   ORDER BY k.created_at, k.workspace_id, k.session_id, k.idempotency_key_digest
   LIMIT 1;
 ELSE
  PERFORM 1 FROM public.session_event_idempotency_keys k
   WHERE k.created_at <= cutoff
     AND (k.created_at, k.workspace_id, k.session_id, k.idempotency_key_digest) > (from_at, from_workspace, from_session, from_digest)
   ORDER BY k.created_at, k.workspace_id, k.session_id, k.idempotency_key_digest
   LIMIT 1;
 END IF;
 more_remaining := FOUND;
 RETURN NEXT;
END $$`

var (
	pruneEventIdempotencyFirstPage = fmt.Sprintf(pruneEventIdempotencyPage, "")
	pruneEventIdempotencyNextPage  = fmt.Sprintf(pruneEventIdempotencyPage, `
      AND (k.created_at, k.workspace_id, k.session_id, k.idempotency_key_digest) > ($2, $3, $4, $5)`)
)

// Change pruning: $1 cutoff, $2..$5 the exclusive (changed_at, workspace_id,
// session_id, stream_position) continuation, $6 the page limit. Only rows this
// statement actually deleted advance a feed watermark, and only for a scope
// whose reader would have returned them: both the change and its permanent
// event public; the Session feed additionally requires session_visible and no
// Thread or a public non-reviewer Thread; a Thread feed requires that exact
// public non-reviewer Thread. One row can advance both scopes. Watermarks are
// upserted in a fixed key order and never move backward. Deletion and every
// watermark change commit or roll back together. more_remaining is the same
// indexed LIMIT 1 probe as for receipts.
const pruneEventChangesPage = `WITH candidates AS MATERIALIZED (
   SELECT c.changed_at, c.workspace_id, c.session_id, c.stream_position
     FROM public.session_event_stream_changes c
    WHERE c.changed_at <= cutoff%s
    ORDER BY c.changed_at, c.workspace_id, c.session_id, c.stream_position
    LIMIT page_limit
    FOR UPDATE OF c SKIP LOCKED
  ), deleted AS (
   DELETE FROM public.session_event_stream_changes c USING candidates k
    WHERE c.workspace_id = k.workspace_id AND c.session_id = k.session_id
      AND c.stream_position = k.stream_position AND c.changed_at <= cutoff
   RETURNING c.workspace_id, c.session_id, c.stream_position, c.event_id, c.session_thread_id, c.visibility, c.session_visible
  ), advanced AS (
   SELECT d.workspace_id, d.session_id, 'session'::text AS feed_key, NULL::text AS session_thread_id, d.stream_position
     FROM deleted d
     JOIN public.session_events e ON e.workspace_id = d.workspace_id AND e.session_id = d.session_id
      AND e.session_thread_id IS NOT DISTINCT FROM d.session_thread_id AND e.event_id = d.event_id AND e.visibility = 'public'
     LEFT JOIN public.session_threads t ON t.workspace_id = d.workspace_id AND t.session_id = d.session_id AND t.id = d.session_thread_id
    WHERE d.visibility = 'public' AND d.session_visible
      AND (d.session_thread_id IS NULL OR (t.visibility = 'public' AND t.role <> 'approval_reviewer'))
   UNION ALL
   SELECT d.workspace_id, d.session_id, 'thread:' || d.session_thread_id, d.session_thread_id, d.stream_position
     FROM deleted d
     JOIN public.session_events e ON e.workspace_id = d.workspace_id AND e.session_id = d.session_id
      AND e.session_thread_id = d.session_thread_id AND e.event_id = d.event_id AND e.visibility = 'public'
     JOIN public.session_threads t ON t.workspace_id = d.workspace_id AND t.session_id = d.session_id AND t.id = d.session_thread_id
      AND t.visibility = 'public' AND t.role <> 'approval_reviewer'
    WHERE d.visibility = 'public'
  ), upserted AS (
   INSERT INTO public.session_event_feed_retention AS r (workspace_id, session_id, feed_key, session_thread_id, pruned_through)
   SELECT a.workspace_id, a.session_id, a.feed_key, a.session_thread_id, max(a.stream_position)
     FROM advanced a
    GROUP BY a.workspace_id, a.session_id, a.feed_key, a.session_thread_id
    ORDER BY a.workspace_id, a.session_id, a.feed_key COLLATE pg_catalog."C"
   ON CONFLICT (workspace_id, session_id, feed_key)
   DO UPDATE SET pruned_through = GREATEST(r.pruned_through, EXCLUDED.pruned_through)
   RETURNING r.feed_key
  )
  SELECT (SELECT count(*) FROM deleted)::integer, (SELECT count(*) FROM candidates)::integer,
         tail.changed_at, tail.workspace_id, tail.session_id, tail.stream_position
    INTO deleted_count, examined_count, last_changed_at, last_workspace_id, last_session_id, last_stream_position
    FROM (SELECT count(*) FROM upserted) AS watermarks
    LEFT JOIN LATERAL (
     SELECT c.changed_at, c.workspace_id, c.session_id, c.stream_position FROM candidates c
      ORDER BY c.changed_at DESC, c.workspace_id DESC, c.session_id DESC, c.stream_position DESC LIMIT 1
    ) AS tail ON true`

var createPostgreSQLPruneEventChangesFunction = `CREATE FUNCTION public.tetral_prune_event_changes(timestamptz, timestamptz, text, text, bigint, integer)
RETURNS TABLE(deleted_count integer, examined_count integer, last_changed_at timestamptz, last_workspace_id text, last_session_id text, last_stream_position bigint, more_remaining boolean)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ DECLARE cutoff timestamptz; page_limit integer;
 from_at timestamptz; from_workspace text; from_session text; from_position bigint; BEGIN
 IF $1 IS NULL THEN
  RAISE EXCEPTION 'invalid retention cutoff' USING ERRCODE='22023';
 END IF;
 IF NOT COALESCE(($2 IS NULL AND $3 IS NULL AND $4 IS NULL AND $5 IS NULL)
   OR ($2 IS NOT NULL AND $3 <> '' AND $4 <> '' AND $5 > 0), false) THEN
  RAISE EXCEPTION 'invalid retention continuation' USING ERRCODE='22023';
 END IF;
 IF $6 IS NULL THEN
  RAISE EXCEPTION 'invalid retention limit' USING ERRCODE='22023';
 END IF;
 cutoff := LEAST($1, pg_catalog.clock_timestamp() - interval '24 hours');
 page_limit := LEAST(GREATEST($6, 0), 256);
 PERFORM pg_catalog.set_config('tetral.retention_maintenance','true',true);
 IF $2 IS NULL THEN
  ` + pruneEventChangesFirstPage + `;
 ELSE
  ` + pruneEventChangesNextPage + `;
 END IF;
 IF last_changed_at IS NOT NULL THEN
  from_at := last_changed_at; from_workspace := last_workspace_id; from_session := last_session_id; from_position := last_stream_position;
 ELSE
  from_at := $2; from_workspace := $3; from_session := $4; from_position := $5;
 END IF;
 IF from_at IS NULL THEN
  PERFORM 1 FROM public.session_event_stream_changes c
   WHERE c.changed_at <= cutoff
   ORDER BY c.changed_at, c.workspace_id, c.session_id, c.stream_position
   LIMIT 1;
 ELSE
  PERFORM 1 FROM public.session_event_stream_changes c
   WHERE c.changed_at <= cutoff
     AND (c.changed_at, c.workspace_id, c.session_id, c.stream_position) > (from_at, from_workspace, from_session, from_position)
   ORDER BY c.changed_at, c.workspace_id, c.session_id, c.stream_position
   LIMIT 1;
 END IF;
 more_remaining := FOUND;
 RETURN NEXT;
END $$`

var (
	pruneEventChangesFirstPage = fmt.Sprintf(pruneEventChangesPage, "")
	pruneEventChangesNextPage  = fmt.Sprintf(pruneEventChangesPage, `
      AND (c.changed_at, c.workspace_id, c.session_id, c.stream_position) > ($2, $3, $4, $5)`)
)

// Runner terminal retention for one terminal state: $1 the state, $2 the
// cutoff, $3 the page limit. Acknowledged and cancelled rows expire 24 hours
// after their terminal timestamp; dead-lettered rows after 168 hours (7 days,
// spelled in hours so the session time zone cannot change it). Selection and
// DELETE repeat the status, the five-kind allowlist and the age under the row
// lock. A NULL terminal timestamp is never replaced by another column: such
// rows are kept and reported as a count bounded at 256. more_remaining is one
// indexed LIMIT 1 probe, with the page's eligibility predicate, for another
// eligible row of the state after the page.
func pruneJobRunnerTerminalState(status, column, retention string) string {
	return fmt.Sprintf(`IF $1 = '%[1]s' THEN
  cutoff := LEAST($2, pg_catalog.clock_timestamp() - interval '%[3]s');
  WITH candidates AS MATERIALIZED (
   SELECT j.id FROM public.queue_jobs j
    WHERE j.status = '%[1]s' AND j.kind IN (%[4]s) AND j.%[2]s <= cutoff
    ORDER BY j.%[2]s, j.id
    LIMIT page_limit
    FOR UPDATE OF j SKIP LOCKED
  ), deleted AS (
   DELETE FROM public.queue_jobs j USING candidates c
    WHERE j.id = c.id AND j.status = '%[1]s' AND j.kind IN (%[4]s) AND j.%[2]s <= cutoff
   RETURNING j.id
  ) SELECT count(*)::integer INTO deleted_count FROM deleted;
  PERFORM 1 FROM public.queue_jobs j
   WHERE j.status = '%[1]s' AND j.kind IN (%[4]s) AND j.%[2]s <= cutoff
   ORDER BY j.%[2]s, j.id
   LIMIT 1;
  more_remaining := FOUND;
  SELECT count(*)::integer INTO malformed_count FROM (
   SELECT 1 FROM public.queue_jobs j
    WHERE j.status = '%[1]s' AND j.kind IN (%[4]s) AND j.%[2]s IS NULL
    LIMIT 256
  ) AS malformed;
 END IF;`, status, column, retention, jobRunnerKindSQLList)
}

var createPostgreSQLPruneJobRunnerJobsFunction = `CREATE FUNCTION public.tetral_prune_job_runner_jobs(text, timestamptz, integer)
RETURNS TABLE(deleted_count integer, malformed_count integer, more_remaining boolean)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ DECLARE cutoff timestamptz; page_limit integer; BEGIN
 IF $1 IS NULL OR $1 NOT IN ('acknowledged', 'cancelled', 'dead_lettered') THEN
  RAISE EXCEPTION 'unknown terminal state' USING ERRCODE='22023';
 END IF;
 IF $2 IS NULL THEN
  RAISE EXCEPTION 'invalid retention cutoff' USING ERRCODE='22023';
 END IF;
 IF $3 IS NULL THEN
  RAISE EXCEPTION 'invalid retention limit' USING ERRCODE='22023';
 END IF;
 page_limit := LEAST(GREATEST($3, 0), 256);
 PERFORM pg_catalog.set_config('tetral.retention_maintenance','true',true);
 ` + pruneJobRunnerTerminalState("acknowledged", "acknowledged_at", "24 hours") + `
 ` + pruneJobRunnerTerminalState("cancelled", "cancelled_at", "24 hours") + `
 ` + pruneJobRunnerTerminalState("dead_lettered", "dead_lettered_at", "168 hours") + `
 RETURN NEXT;
END $$`

func postgresqlRetentionIndexSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"index_session_event_idempotency_keys_age", createPostgreSQLSessionEventIdempotencyAgeIndex},
		{"index_session_event_stream_changes_age", createPostgreSQLSessionEventStreamChangesAgeIndex},
		{"index_session_event_stream_changes_thread_head", createPostgreSQLSessionEventStreamThreadHeadIndex},
		{"index_session_event_stream_changes_session_head", createPostgreSQLSessionEventStreamSessionHead},
		{"index_queue_jobs_job_runner_acknowledged_retention", jobRunnerTerminalRetentionIndex("acknowledged", "acknowledged_at")},
		{"index_queue_jobs_job_runner_cancelled_retention", jobRunnerTerminalRetentionIndex("cancelled", "cancelled_at")},
		{"index_queue_jobs_job_runner_dead_lettered_retention", jobRunnerTerminalRetentionIndex("dead_lettered", "dead_lettered_at")},
	}
}

func postgresqlRetentionMaintenanceSteps() []postgresqlSchemaStep {
	var steps []postgresqlSchemaStep
	for _, entry := range retentionMaintenanceCommands {
		predicate := retentionMaintenancePredicate(entry.table)
		for _, command := range entry.commands {
			name := "retention_maintenance_" + strings.ToLower(command)
			var clauses string
			switch command {
			case "INSERT":
				clauses = "WITH CHECK (" + predicate + ")"
			case "UPDATE":
				clauses = "USING (" + predicate + ") WITH CHECK (" + predicate + ")"
			default:
				clauses = "USING (" + predicate + ")"
			}
			steps = append(steps,
				postgresqlSchemaStep{name: "rls_policy_" + name + "_drop_" + entry.table, ddl: "DROP POLICY IF EXISTS " + name + " ON " + entry.table},
				postgresqlSchemaStep{name: "rls_policy_" + name + "_" + entry.table, ddl: "CREATE POLICY " + name + " ON " + entry.table + " FOR " + command + " " + clauses},
			)
		}
	}
	return append(steps,
		postgresqlSchemaStep{"create_prune_event_idempotency", createPostgreSQLPruneEventIdempotencyFunction},
		postgresqlSchemaStep{"create_prune_event_changes", createPostgreSQLPruneEventChangesFunction},
		postgresqlSchemaStep{"create_prune_job_runner_jobs", createPostgreSQLPruneJobRunnerJobsFunction},
		postgresqlSchemaStep{"revoke_retention_maintenance_public", `REVOKE ALL ON FUNCTION public.tetral_prune_event_idempotency(timestamptz, timestamptz, text, text, bytea, integer), public.tetral_prune_event_changes(timestamptz, timestamptz, text, text, bigint, integer), public.tetral_prune_job_runner_jobs(text, timestamptz, integer) FROM PUBLIC`},
	)
}
