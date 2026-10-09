package storage

import "fmt"

// Job Runner repair discovery reads raw Runtime binding membership across
// Workspaces in binding primary-key order. Only the two security-definer
// functions below, executing as the actual table owner with the
// tetral.runner_discovery purpose flag set, pass these SELECT-only policies; a
// serving role that sets the flag itself still sees only its own Workspace.
// runtime_processes is a global table without Workspace RLS.
var jobRunnerDiscoveryPolicyTables = []string{"session_runtime_bindings", "session_runtime_status", "sessions", "session_runtime_inbox"}

func jobRunnerDiscoveryPolicy(table string) string {
	return fmt.Sprintf(`CREATE POLICY runner_discovery ON %s
 FOR SELECT
 USING (current_setting('tetral.runner_discovery', true) = 'true' AND current_user = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.%s'::regclass))`, table, table)
}

// The accepted-Inbox predicate of the binding page probes this exact identity.
const createPostgreSQLSessionRuntimeInboxAcceptedBindingIndex = `CREATE INDEX idx_session_runtime_inbox_accepted_binding ON session_runtime_inbox(workspace_id, session_id, binding_id, binding_generation, target_pod_uid) WHERE status = 'accepted'`

// The upper bound of a discovery cycle is one reverse primary-key seek.
const createPostgreSQLJobRunnerBindingUpperFunction = `CREATE FUNCTION public.tetral_job_runner_binding_upper()
RETURNS TABLE(workspace_id text, session_id text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 PERFORM pg_catalog.set_config('tetral.runner_discovery','true',true);
 RETURN QUERY SELECT b.workspace_id, b.session_id
   FROM public.session_runtime_bindings b
  ORDER BY b.workspace_id DESC, b.session_id DESC
  LIMIT 1;
END $$`

// jobRunnerBindingPage selects at most $5 raw binding rows of the key range
// (after, upper] before any eligibility join; the MATERIALIZED CTE keeps that
// LIMIT ahead of the per-row status, Session, Inbox and process lookups.
// Separate first and later statements keep the position an index condition.
func jobRunnerBindingPage(afterBound bool) string {
	lower := ""
	if afterBound {
		lower = `
      AND (b.workspace_id, b.session_id) > ($1, $2)`
	}
	return `WITH raw AS MATERIALIZED (
    SELECT b.workspace_id, b.session_id, b.binding_id, b.binding_generation,
           b.agent_runtime_namespace, b.agent_runtime_pod_name, b.agent_runtime_pod_uid,
           b.agent_runtime_pod_ip, b.runtime_process_id
      FROM public.session_runtime_bindings b
     WHERE (b.workspace_id, b.session_id) <= ($3, $4)` + lower + `
     ORDER BY b.workspace_id, b.session_id
     LIMIT $5
  )
  SELECT raw.workspace_id, raw.session_id, raw.binding_id, raw.binding_generation,
         raw.agent_runtime_namespace, raw.agent_runtime_pod_name, raw.agent_runtime_pod_uid,
         raw.agent_runtime_pod_ip, raw.runtime_process_id,
         COALESCE((
           SELECT runtime.status = 'running' OR session.status = 'rescheduling' OR EXISTS (
                    SELECT 1 FROM public.session_runtime_inbox inbox
                     WHERE inbox.workspace_id = raw.workspace_id AND inbox.session_id = raw.session_id
                       AND inbox.binding_id = raw.binding_id AND inbox.binding_generation = raw.binding_generation
                       AND inbox.target_pod_uid = raw.agent_runtime_pod_uid AND inbox.status = 'accepted')
             FROM public.session_runtime_status runtime
             JOIN public.sessions session
               ON session.workspace_id = runtime.workspace_id AND session.id = runtime.session_id
            WHERE runtime.workspace_id = raw.workspace_id AND runtime.session_id = raw.session_id
              AND runtime.binding_id = raw.binding_id AND runtime.binding_generation = raw.binding_generation
         ), false),
         process.runtime_process_id IS NOT NULL,
         COALESCE(process.is_current, false),
         COALESCE(process.phase, ''),
         process.retired_at
    FROM raw
    LEFT JOIN public.runtime_processes process
      ON process.namespace = raw.agent_runtime_namespace
     AND process.pod_uid = raw.agent_runtime_pod_uid
     AND process.runtime_process_id = raw.runtime_process_id
   ORDER BY raw.workspace_id, raw.session_id`
}

// The page returns identifiers, binding target, the active flag and process
// facts only; never a receipt, payload or conversation content. The active flag
// is true only when runtime status matches the exact binding identity and its
// Session exists, and then the binding is running, its Session is
// rescheduling, or an accepted Inbox row targets the exact binding and Pod. A
// missing process is process_registered=false, never an omitted row. A partly
// NULL or empty position, a missing upper bound and a limit outside 1..128 are
// rejected with invalid_parameter_value.
var createPostgreSQLJobRunnerBindingPageFunction = `CREATE FUNCTION public.tetral_job_runner_binding_page(text, text, text, text, integer)
RETURNS TABLE(workspace_id text, session_id text, binding_id text, binding_generation bigint,
 agent_runtime_namespace text, agent_runtime_pod_name text, agent_runtime_pod_uid text,
 agent_runtime_pod_ip text, runtime_process_id text, active boolean, process_registered boolean,
 is_current boolean, phase text, retired_at timestamptz)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog
AS $$ BEGIN
 IF ($1 IS NULL) <> ($2 IS NULL) OR $1 = '' OR $2 = '' THEN
  RAISE EXCEPTION 'invalid runner discovery position' USING ERRCODE='22023';
 END IF;
 IF $3 IS NULL OR $4 IS NULL OR $3 = '' OR $4 = '' THEN
  RAISE EXCEPTION 'invalid runner discovery upper bound' USING ERRCODE='22023';
 END IF;
 IF $5 IS NULL OR $5 < 1 OR $5 > 128 THEN
  RAISE EXCEPTION 'invalid runner discovery limit' USING ERRCODE='22023';
 END IF;
 PERFORM pg_catalog.set_config('tetral.runner_discovery','true',true);
 IF $1 IS NULL THEN
  RETURN QUERY ` + jobRunnerBindingPage(false) + `;
 ELSE
  RETURN QUERY ` + jobRunnerBindingPage(true) + `;
 END IF;
END $$`

func postgresqlJobRunnerDiscoverySteps() []postgresqlSchemaStep {
	steps := make([]postgresqlSchemaStep, 0, 2*len(jobRunnerDiscoveryPolicyTables)+4)
	for _, table := range jobRunnerDiscoveryPolicyTables {
		steps = append(steps,
			postgresqlSchemaStep{name: "rls_policy_runner_discovery_drop_" + table, ddl: "DROP POLICY IF EXISTS runner_discovery ON " + table},
			postgresqlSchemaStep{name: "rls_policy_runner_discovery_" + table, ddl: jobRunnerDiscoveryPolicy(table)},
		)
	}
	return append(steps,
		postgresqlSchemaStep{name: "create_job_runner_binding_upper", ddl: createPostgreSQLJobRunnerBindingUpperFunction},
		postgresqlSchemaStep{name: "create_job_runner_binding_page", ddl: createPostgreSQLJobRunnerBindingPageFunction},
		postgresqlSchemaStep{name: "revoke_job_runner_binding_discovery_public", ddl: `REVOKE ALL ON FUNCTION public.tetral_job_runner_binding_upper(), public.tetral_job_runner_binding_page(text, text, text, text, integer) FROM PUBLIC`},
	)
}
