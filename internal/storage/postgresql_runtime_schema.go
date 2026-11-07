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
 retired_at TIMESTAMPTZ,
 PRIMARY KEY (namespace, pod_uid, runtime_process_id),
 UNIQUE (namespace, pod_uid, registration_order),
 FOREIGN KEY (namespace, pod_uid) REFERENCES runtime_process_pods(namespace, pod_uid),
 CHECK (NOT is_current OR (phase IN ('accepting', 'draining') AND retired_at IS NULL))
)`

const createPostgreSQLRuntimeProcessesCurrentIndex = `CREATE UNIQUE INDEX idx_runtime_processes_current ON runtime_processes(namespace, pod_uid) WHERE is_current`

// Liveness is separate from lifecycle so an unchanged report updates only this
// row and never waits for the process row shared locks held by Session
// mutations. Registration creates the row with a NULL report; every successful
// report records database time. A retired process keeps its last report.
const createPostgreSQLRuntimeProcessLivenessTable = `CREATE TABLE public.runtime_process_liveness (
 namespace TEXT NOT NULL,
 pod_uid TEXT NOT NULL,
 runtime_process_id TEXT NOT NULL,
 reported_at TIMESTAMPTZ,
 PRIMARY KEY (namespace, pod_uid, runtime_process_id),
 FOREIGN KEY (namespace, pod_uid, runtime_process_id)
   REFERENCES public.runtime_processes(namespace, pod_uid, runtime_process_id)
   ON DELETE CASCADE
)`

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

// Job Runner's final loss classification holds the liveness row FOR SHARE, so a
// concurrent report either finishes before the decision or waits for it. The
// same lock-only definer pattern grants the Runner no liveness mutation.
const createPostgreSQLRuntimeProcessLivenessLockFunction = `CREATE FUNCTION public.tetral_lock_runtime_process_liveness(text, text, text)
RETURNS SETOF public.runtime_process_liveness
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT live.* FROM public.runtime_process_liveness live
      WHERE live.namespace = $1 AND live.pod_uid = $2 AND live.runtime_process_id = $3
      FOR SHARE OF live $$`

const revokePostgreSQLRuntimeProcessLivenessLockPublic = `REVOKE ALL ON FUNCTION public.tetral_lock_runtime_process_liveness(text, text, text) FROM PUBLIC`

// One row per downstream tool instruction of a machine-bound call:
// tool.execute for an admitted call, tool.stdin for a write_stdin, tool.cancel
// for a turn-end cancel. internal/runtimecontrol owns it. message_id is the
// Engine ID under which the instruction is issued on its connection.
// payload_json is the instruction payload; a tool.execute keeps binding_id
// empty until release binds and freezes it. last_write_seq numbers the
// nonempty writes to a tool.execute's task. The request and the connection
// are named without foreign keys: their writers lock those rows before the
// computer, and a key-share lock taken from here would reverse that order.
const createPostgreSQLSessionToolDispatchesTable = `CREATE TABLE session_tool_dispatches (
 workspace_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 message_id TEXT NOT NULL CHECK(message_id ~ '^msg_[0-9a-f]{16}$'),
 type TEXT NOT NULL CHECK(type IN ('tool.execute','tool.stdin','tool.cancel')),
 session_thread_id TEXT NOT NULL,
 tool_use_event_id TEXT NOT NULL CHECK(char_length(tool_use_event_id)>=1),
 task_id TEXT NOT NULL CHECK(task_id ~ '^task_[0-9a-f]{16}$'),
 computer_id TEXT NOT NULL,
 instance_id TEXT,
 connection_id TEXT,
 binding_id TEXT,
 request_id TEXT,
 payload_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('waiting_instance','waiting_preparation','ready','reported','ended')),
 last_write_seq BIGINT CHECK(last_write_seq>=0),
 trace_context TEXT CHECK(trace_context ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(message_id),
 FOREIGN KEY(workspace_id,session_id,session_thread_id) REFERENCES session_threads(workspace_id,session_id,id) ON DELETE CASCADE,
 FOREIGN KEY(workspace_id,session_id,computer_id) REFERENCES computers(workspace_id,session_id,id) ON DELETE CASCADE,
 CHECK((type='tool.execute')=(last_write_seq IS NOT NULL)),
 CHECK(state<>'waiting_instance' OR (type='tool.execute' AND request_id IS NOT NULL)),
 CHECK(state<>'waiting_preparation' OR (type='tool.execute' AND instance_id IS NOT NULL AND connection_id IS NOT NULL)),
 CHECK(state NOT IN ('ready','reported') OR (instance_id IS NOT NULL AND connection_id IS NOT NULL AND binding_id IS NOT NULL))
)`

// session_tool_reports holds each tool report a CLI connection sent, under
// the identity it sent it with, as stored: an image's bytes are replaced by
// their image store reference. Its instruction's record owns it.
const createPostgreSQLSessionToolReportsTable = `CREATE TABLE session_tool_reports (
 workspace_id TEXT NOT NULL,
 connection_id TEXT NOT NULL,
 message_id TEXT NOT NULL CHECK(char_length(message_id)>=1),
 request_message_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 binding_id TEXT NOT NULL,
 tool_use_event_id TEXT NOT NULL,
 task_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('execution','control')),
 phase TEXT CHECK(phase IN ('running','terminal','rejected')),
 report_seq BIGINT CHECK(report_seq>0),
 body_json TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(workspace_id,connection_id,message_id),
 FOREIGN KEY(workspace_id,connection_id) REFERENCES instance_connections(workspace_id,id),
 FOREIGN KEY(request_message_id) REFERENCES session_tool_dispatches(message_id) ON DELETE CASCADE,
 CHECK((kind='execution')=(phase IS NOT NULL)),
 CHECK(COALESCE(phase IN ('running','terminal'),FALSE)=(report_seq IS NOT NULL))
)`

func postgresqlToolDispatchTableSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"create_session_tool_dispatches", createPostgreSQLSessionToolDispatchesTable},
		{"index_session_tool_dispatches_connection_state", `CREATE INDEX idx_session_tool_dispatches_connection_state ON session_tool_dispatches(workspace_id,connection_id,state)`},
		{"index_session_tool_dispatches_request", `CREATE INDEX idx_session_tool_dispatches_request ON session_tool_dispatches(workspace_id,request_id)`},
		{"index_session_tool_dispatches_task", `CREATE INDEX idx_session_tool_dispatches_task ON session_tool_dispatches(workspace_id,session_id,task_id)`},
		{"create_session_tool_reports", createPostgreSQLSessionToolReportsTable},
		{"index_session_tool_reports_request", `CREATE INDEX idx_session_tool_reports_request ON session_tool_reports(request_message_id)`},
	}
}
