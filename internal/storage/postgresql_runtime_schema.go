package storage

// Process custody is global workload state. These tables carry no Workspace
// data; serving capabilities are declared separately in database/roles.json.
const createPostgreSQLRuntimeProcessPodsTable = `CREATE TABLE runtime_process_pods (
 namespace TEXT NOT NULL CHECK (namespace <> ''),
 pod_uid TEXT NOT NULL CHECK (pod_uid <> ''),
 next_registration_order BIGINT NOT NULL DEFAULT 0 CHECK (next_registration_order >= 0),
 last_promoted_order BIGINT NOT NULL DEFAULT 0 CHECK (last_promoted_order >= 0 AND last_promoted_order <= next_registration_order),
 PRIMARY KEY (namespace, pod_uid)
)`

const createPostgreSQLRuntimeProcessesTable = `CREATE TABLE runtime_processes (
 namespace TEXT NOT NULL,
 pod_uid TEXT NOT NULL,
 runtime_process_id TEXT NOT NULL CHECK (runtime_process_id <> ''),
 registration_order BIGINT NOT NULL CHECK (registration_order > 0),
 registration_receipt TEXT NOT NULL CHECK (registration_receipt <> ''),
 phase TEXT NOT NULL DEFAULT 'starting' CHECK (phase IN ('starting', 'accepting', 'draining')),
 is_current BOOLEAN NOT NULL DEFAULT FALSE,
 registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 reported_at TIMESTAMPTZ,
 retired_at TIMESTAMPTZ,
 PRIMARY KEY (namespace, pod_uid, runtime_process_id),
 UNIQUE (namespace, pod_uid, registration_order),
 FOREIGN KEY (namespace, pod_uid) REFERENCES runtime_process_pods(namespace, pod_uid),
 CHECK (NOT is_current OR (phase IN ('accepting', 'draining') AND reported_at IS NOT NULL AND retired_at IS NULL))
)`

const createPostgreSQLRuntimeProcessesCurrentIndex = `CREATE UNIQUE INDEX idx_runtime_processes_current ON runtime_processes(namespace, pod_uid) WHERE is_current`

const createPostgreSQLSessionRuntimeHandoffsTable = `CREATE TABLE session_runtime_handoffs (
 workspace_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 handoff_id TEXT NOT NULL CHECK (handoff_id <> ''),
 operation_id TEXT NOT NULL CHECK (operation_id <> ''),
 operation_digest TEXT NOT NULL CHECK (length(operation_digest) = 64),
 binding_id TEXT NOT NULL CHECK (binding_id <> ''),
 binding_generation BIGINT NOT NULL CHECK (binding_generation > 0),
 runtime_namespace TEXT NOT NULL,
 runtime_pod_uid TEXT NOT NULL,
 runtime_process_id TEXT NOT NULL,
 committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (workspace_id, session_id, handoff_id),
 UNIQUE (workspace_id, session_id, operation_id),
 UNIQUE (workspace_id, session_id, binding_id, binding_generation),
 FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
 FOREIGN KEY (runtime_namespace, runtime_pod_uid, runtime_process_id) REFERENCES runtime_processes(namespace, pod_uid, runtime_process_id)
)`

const createPostgreSQLSessionRuntimeHandoffThreadsTable = `CREATE TABLE session_runtime_handoff_threads (
 workspace_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 handoff_id TEXT NOT NULL,
 session_thread_id TEXT NOT NULL,
 disposition TEXT NOT NULL CHECK (disposition IN ('idle', 'recover')),
 queue_job_id TEXT,
 PRIMARY KEY (workspace_id, session_id, handoff_id, session_thread_id),
 FOREIGN KEY (workspace_id, session_id, handoff_id) REFERENCES session_runtime_handoffs(workspace_id, session_id, handoff_id) ON DELETE CASCADE,
 FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
 CHECK ((disposition = 'idle' AND queue_job_id IS NULL) OR (disposition = 'recover' AND queue_job_id IS NOT NULL AND queue_job_id <> ''))
)`

// The Runner can read lifecycle facts but cannot mutate them. PostgreSQL row
// locks require UPDATE privilege; this lock-only definer grants no mutation.
// Its owner is the migration role, and every object reference is qualified.
const createPostgreSQLRuntimeProcessLockFunction = `CREATE FUNCTION public.tetral_lock_runtime_process(text, text, text)
RETURNS SETOF public.runtime_processes
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT process.* FROM public.runtime_processes process
      WHERE process.namespace = $1 AND process.pod_uid = $2 AND process.runtime_process_id = $3
      FOR SHARE OF process $$`

const revokePostgreSQLRuntimeProcessLockPublic = `REVOKE ALL ON FUNCTION public.tetral_lock_runtime_process(text, text, text) FROM PUBLIC`
