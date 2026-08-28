package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tetral-ai/tetral/database"
)

// PostgreSQL DDL constants for the workspace/auth foundation: the `workspaces`
// system table, the `api_keys` table, and PostgreSQL row-level security
// policies that enforce workspace
// isolation at the database level even when application SQL forgets
// its explicit workspace_id predicate.
//
// Each statement is idempotent: tables use IF NOT EXISTS, indexes use
// IF NOT EXISTS, and RLS policies are recreated through DROP POLICY IF EXISTS
// + CREATE POLICY so a second call against an already-initialized database is
// a no-op.
//
// The DDL only relies on ordinary table/index DDL plus the standard
// row-level security feature, so it stays portable across self-managed
// PostgreSQL and managed providers (RDS, Cloud SQL, Azure Database for
// PostgreSQL).
const (
	// sessions.status is a projected surface, not a single stored axis.
	//
	//   value          meaning                    source / writer                            precedence
	//   running/idle   live residency status      projected at READ time from                yields to a direct value
	//                                              session_runtime_status.status
	//   rescheduling   request-turn retry pending  written directly here by Bridge            wins over the projection
	//   terminated     terminal runtime closeout   written directly here by Bridge            wins over the projection
	//
	// Read projection (internal/session sessionSelectSQL): a stored terminated
	// or rescheduling is returned as-is; every other read returns
	// COALESCE(session_runtime_status.status, sessions.status), so the live
	// running/idle value comes from session_runtime_status, not from this row.
	//
	// lifecycle_state {admitted, active, archiving, archived, deleted} is a
	// separate durable admission/archive/tombstone axis, not the public status.
	// The admission/archive/delete handlers and the Bridge cleanup scheduler do
	// branch on the active-vs-archiving distinction: UpdateResource admits an
	// active session but rejects archiving/archived ("session is archived"),
	// ArchiveSession runs the active->archiving UPDATE only from active,
	// DeleteSession rejects an in-progress archiving, and the cleanup claim
	// proceeds only while lifecycle_state = 'active'.
	//
	// UPDATE-WITH: internal/session sessionSelectSQL (read projection);
	// services/bridge bridge_api_events.go (running/idle/
	// rescheduling/terminated status writes); internal/session service.go and
	// postgresql_store.go plus services/bridge
	// runtime_session_cleanup.go (lifecycle_state branches).
	createPostgreSQLSessionsTable = `CREATE TABLE IF NOT EXISTS sessions (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		main_thread_id TEXT,
		type TEXT NOT NULL,
		title TEXT,
		metadata_json TEXT NOT NULL DEFAULT '{}',
		status TEXT NOT NULL,
		lifecycle_state TEXT NOT NULL DEFAULT 'admitted',
		config_generation BIGINT NOT NULL DEFAULT 1,
		sandbox_resource_revision BIGINT NOT NULL DEFAULT 1,
		approval_mode TEXT NOT NULL DEFAULT 'ask_for_approval',
		usage_json TEXT NOT NULL DEFAULT '{}',
		archived_at TIMESTAMPTZ,
		agent_id TEXT NOT NULL,
		agent_version_id TEXT NOT NULL,
		agent_version INTEGER NOT NULL,
		agent_config_hash TEXT NOT NULL DEFAULT '',
		environment_id TEXT NOT NULL,
		vault_ids_json TEXT NOT NULL DEFAULT '[]',
		provider_access_json TEXT NOT NULL DEFAULT '{}',
		installed_tools_json TEXT NOT NULL DEFAULT '[]',
		delete_cleanup_id TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, id),
		UNIQUE (id),
		CONSTRAINT sessions_workspace_agent_fk
			FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, id) ON DELETE RESTRICT,
		CONSTRAINT sessions_workspace_agent_version_fk
			FOREIGN KEY (workspace_id, agent_id, agent_version) REFERENCES agent_versions(workspace_id, agent_id, version) ON DELETE RESTRICT,
		CONSTRAINT sessions_workspace_agent_version_id_fk
			FOREIGN KEY (workspace_id, agent_id, agent_version, agent_version_id) REFERENCES agent_versions(workspace_id, agent_id, version, id) ON DELETE RESTRICT,
		CONSTRAINT sessions_workspace_environment_fk
			FOREIGN KEY (workspace_id, environment_id) REFERENCES environments(workspace_id, id) ON DELETE RESTRICT,
		CONSTRAINT sessions_status_shape CHECK (status IN ('idle', 'running', 'rescheduling', 'terminated')),
		CONSTRAINT sessions_lifecycle_state_shape CHECK (lifecycle_state IN ('admitted', 'active', 'archiving', 'archived', 'deleted')),
		CONSTRAINT sessions_config_generation_shape CHECK (config_generation > 0),
		CONSTRAINT sessions_sandbox_resource_revision_shape CHECK (sandbox_resource_revision > 0),
		CONSTRAINT sessions_approval_mode_shape CHECK (approval_mode IN ('full_access', 'ask_for_approval', 'approve_for_me'))
	)`

	// session_threads is the runnable conversation lane. Its durable status and
	// visibility carry couplings the CHECK/UNIQUE DDL cannot state.
	//
	// Status ownership (durable value -> public projection; every write is Bridge):
	//   durable status       written by                                    public thread status / event
	//   running              child-thread status write                     running / thread_status_running
	//   idle                 child idle / finalization                     idle / thread_status_idle
	//   requires_action      pending external-wait projection              idle + stop-reason / requires-action metadata
	//   closed_for_runtime   CloseChildControl (close_agent)               idle; resumable by resume_agent
	//   rescheduling         child retry settlement                        rescheduling / thread_status_rescheduled
	//   terminated           terminal closeout (CommitRuntimeTermination)  terminated / thread_status_terminated
	//   failed               terminal closeout (CommitRuntimeTermination)  terminated / thread_status_terminated
	//
	// failed and terminated both project the single public "terminated" (the
	// public SDK has no thread "failed"): failed is written when the terminal
	// trigger is the thread's own unrecoverable failure, terminated when the
	// thread is closed out for another proven-non-resumable cause; a closeout
	// writes exactly one of the two.
	//
	// archived_at is a separate public archive lifecycle, not closed_for_runtime:
	// ordinary close_agent never sets archived_at and never emits
	// thread_status_terminated.
	//
	// Visibility (reader rule): public APIs never return visibility='internal'
	// rows and never expose main_thread_id or role='main' (the primary thread's
	// internal names). visibility='internal' rows (such as approval_reviewer)
	// reach only internal service paths that ask for them.
	//
	// Partial-unique intent (indexes below): idx_session_threads_main_unique
	// pins one role='main' row per session; idx_session_threads_subagent_task_unique
	// pins one subagent task_name per parent; idx_session_threads_reviewer_trunk_unique
	// pins one approval_reviewer trunk (is_trunk) per parent, leaving ephemeral
	// reviews (is_trunk=false) unconstrained.
	//
	// UPDATE-WITH: services/bridge child-thread and
	// runtime_termination status writers; the createPostgreSQLSessionThreads*Index
	// constants below; internal/session and internal/eventstream reader
	// projections.
	createPostgreSQLSessionThreadsTable = `CREATE TABLE IF NOT EXISTS session_threads (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		parent_thread_id TEXT,
		role TEXT NOT NULL DEFAULT 'main',
		visibility TEXT NOT NULL DEFAULT 'public',
		status TEXT NOT NULL,
		agent_type TEXT NOT NULL DEFAULT 'default',
		title TEXT,
		task_name TEXT,
		is_trunk BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMPTZ NOT NULL,
		last_active_at TIMESTAMPTZ NOT NULL,
		closed_at TIMESTAMPTZ,
		archived_at TIMESTAMPTZ,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, id),
		UNIQUE (workspace_id, id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, session_id, parent_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_threads_role_shape CHECK (role IN ('main', 'subagent', 'approval_reviewer')),
		CONSTRAINT session_threads_visibility_shape CHECK (visibility IN ('public', 'internal')),
		CONSTRAINT session_threads_status_shape CHECK (status IN ('running', 'idle', 'requires_action', 'closed_for_runtime', 'rescheduling', 'terminated', 'failed')),
		CONSTRAINT session_threads_subagent_task_name_shape CHECK (role <> 'subagent' OR (task_name IS NOT NULL AND task_name <> '')),
		CONSTRAINT session_threads_main_parent_shape CHECK (
			(role = 'main' AND parent_thread_id IS NULL)
			OR (role IN ('subagent', 'approval_reviewer') AND parent_thread_id IS NOT NULL)
		)
	)`

	createPostgreSQLSessionSandboxBindingsTable = `CREATE TABLE IF NOT EXISTS session_sandbox_bindings (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		logical_sandbox_id TEXT NOT NULL,
		environment_id TEXT NOT NULL,
		environment_generation BIGINT NOT NULL,
		provider TEXT NOT NULL,
		provider_resource_id TEXT,
		binding_revision BIGINT NOT NULL,
		release_requested_at TIMESTAMPTZ,
		materialized_resource_revision BIGINT NOT NULL DEFAULT 0,
		resource_credential_expires_at TIMESTAMPTZ,
		resource_roots_json TEXT NOT NULL DEFAULT '[]',
		helper_verified_at TIMESTAMPTZ,
		provider_metadata_json TEXT NOT NULL DEFAULT '{}',
		release_reason TEXT,
		released_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id),
		UNIQUE (workspace_id, logical_sandbox_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, environment_id) REFERENCES environments(workspace_id, id) ON DELETE RESTRICT,
		CONSTRAINT session_sandbox_bindings_provider_shape CHECK (provider = 'daytona'),
		CONSTRAINT session_sandbox_bindings_generation_shape CHECK (environment_generation > 0),
		CONSTRAINT session_sandbox_bindings_revision_shape CHECK (binding_revision > 0),
		CONSTRAINT session_sandbox_bindings_resource_revision_shape CHECK (materialized_resource_revision >= 0),
		CONSTRAINT session_sandbox_bindings_roots_shape CHECK (jsonb_typeof(resource_roots_json::jsonb) = 'array'),
		CONSTRAINT session_sandbox_bindings_metadata_shape CHECK (jsonb_typeof(provider_metadata_json::jsonb) = 'object'),
		CONSTRAINT session_sandbox_bindings_release_shape CHECK (
			(release_requested_at IS NULL AND release_reason IS NULL)
			OR (release_requested_at IS NOT NULL AND release_reason = 'session_delete')
		)
	)`

	createPostgreSQLSandboxLifecycleOperationsTable = `CREATE TABLE IF NOT EXISTS sandbox_lifecycle_operations (
		workspace_id TEXT NOT NULL,
		operation_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		logical_sandbox_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		state TEXT NOT NULL,
		observed_binding_revision BIGINT,
		target_environment_generation BIGINT,
		target_resource_revision BIGINT,
		target_provider_resource_id TEXT,
		materialization_resources_json TEXT,
		waiting_activation_operation_id TEXT,
		provider_create_name TEXT,
		provider_request_labels_json TEXT,
		adopted_provider_resource_id TEXT,
		release_reason TEXT,
		superseded_by_operation_id TEXT,
		outcome_effect_boundary TEXT,
		outcome_disposition TEXT,
		error_kind TEXT,
		safe_message TEXT,
		queue_job_id TEXT,
		queue_kind TEXT,
		queue_partition_key TEXT,
		queue_dedupe_key TEXT,
		lease_owner TEXT,
		lease_token TEXT,
		lease_expires_at TIMESTAMPTZ,
		attempt_count INTEGER NOT NULL DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		completed_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, operation_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, session_id) REFERENCES session_sandbox_bindings(workspace_id, session_id) ON DELETE CASCADE,
		CONSTRAINT sandbox_lifecycle_operations_kind_shape CHECK (kind IN ('create', 'start', 'replace', 'materialize', 'release')),
		CONSTRAINT sandbox_lifecycle_operations_state_shape CHECK (state IN ('pending', 'waiting_artifact', 'waiting_activation', 'running', 'completed', 'failed', 'skipped_cold', 'abandoned')),
		CONSTRAINT sandbox_lifecycle_operations_kind_state_shape CHECK (
			(kind IN ('create', 'start', 'replace') AND state IN ('pending', 'waiting_artifact', 'running', 'completed', 'failed', 'abandoned'))
			OR (kind = 'materialize' AND state IN ('pending', 'waiting_activation', 'running', 'completed', 'failed', 'skipped_cold', 'abandoned'))
			OR (kind = 'release' AND state IN ('pending', 'running', 'completed', 'failed'))
		),
		CONSTRAINT sandbox_lifecycle_operations_attempt_shape CHECK (attempt_count >= 0),
		CONSTRAINT sandbox_lifecycle_operations_queue_shape CHECK (
			(queue_job_id IS NULL AND queue_kind IS NULL AND queue_partition_key IS NULL AND queue_dedupe_key IS NULL)
			OR (
				queue_job_id IS NOT NULL AND queue_job_id <> ''
				AND queue_partition_key IS NOT NULL AND queue_partition_key <> ''
				AND queue_dedupe_key IS NOT NULL AND queue_dedupe_key <> ''
				AND (
				(kind IN ('create', 'start', 'replace') AND queue_kind = 'sandbox_activate')
				OR (kind = 'materialize' AND queue_kind = 'sandbox_materialize')
				OR (kind = 'release' AND queue_kind = 'sandbox_release')
				)
			)
		),
		CONSTRAINT sandbox_lifecycle_operations_target_shape CHECK (
			(kind IN ('create', 'replace')
				AND observed_binding_revision > 0
				AND target_environment_generation > 0
				AND provider_create_name IS NOT NULL AND provider_create_name <> ''
				AND provider_request_labels_json IS NOT NULL
				AND jsonb_typeof(provider_request_labels_json::jsonb) = 'object'
				AND materialization_resources_json IS NULL
				AND target_resource_revision IS NULL
				AND release_reason IS NULL)
			OR (kind = 'start'
				AND observed_binding_revision > 0
				AND target_provider_resource_id IS NOT NULL AND target_provider_resource_id <> ''
				AND target_environment_generation IS NULL
				AND target_resource_revision IS NULL
				AND materialization_resources_json IS NULL
				AND release_reason IS NULL)
			OR (kind = 'materialize'
				AND observed_binding_revision > 0
				AND target_provider_resource_id IS NOT NULL AND target_provider_resource_id <> ''
				AND target_environment_generation > 0
				AND target_resource_revision >= 0
				AND materialization_resources_json IS NOT NULL
				AND jsonb_typeof(materialization_resources_json::jsonb) = 'object'
				AND release_reason IS NULL)
			OR (kind = 'release'
				AND target_provider_resource_id IS NOT NULL AND target_provider_resource_id <> ''
				AND release_reason IN ('session_delete', 'replaced_handle')
				AND target_environment_generation IS NULL
				AND target_resource_revision IS NULL
				AND materialization_resources_json IS NULL)
		),
		CONSTRAINT sandbox_lifecycle_operations_outcome_shape CHECK (
			(outcome_effect_boundary IS NULL AND outcome_disposition IS NULL)
			OR (
				outcome_effect_boundary IN ('proved_not_started', 'submitted', 'outcome_unknown')
				AND outcome_disposition IN ('retryable', 'terminal')
			)
		),
		CONSTRAINT sandbox_lifecycle_operations_lease_shape CHECK (
			(lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
			OR (lease_owner IS NOT NULL AND lease_owner <> '' AND lease_token IS NOT NULL AND lease_token <> '' AND lease_expires_at IS NOT NULL)
		)
	)`

	// session_events column semantics the DDL does not carry:
	//
	// processed_at is set only when Runtime has actually processed that input
	// (the Bridge delivery/commit path), NOT at admission. A NULL processed_at
	// is an admitted-but-unprocessed public input; idx_session_events_pending_client
	// indexes exactly those rows.
	//
	// insert_stream_position is the immutable session-global insert order: the
	// stream_position of the event's revision-1 change in
	// session_event_stream_changes, copied into this row once at insert (set
	// once from 0, never updated) and used as the session-level list cursor.
	// Thread-level list paging uses the per-thread sequence column instead.
	//
	// UPDATE-WITH: services/bridge and internal/session event
	// append/delivery paths; internal/eventstream list/cursor reader.
	createPostgreSQLSessionEventsTable = `CREATE TABLE IF NOT EXISTS session_events (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT,
		event_id TEXT NOT NULL,
		sequence BIGINT NOT NULL,
		revision BIGINT NOT NULL DEFAULT 1,
		type TEXT NOT NULL,
		payload_json TEXT NOT NULL,
		visibility TEXT NOT NULL DEFAULT 'public',
		session_visible BOOLEAN NOT NULL DEFAULT TRUE,
		latest_stream_position BIGINT NOT NULL DEFAULT 0,
		insert_stream_position BIGINT NOT NULL DEFAULT 0,
		runtime_write_id TEXT,
		model_request_id TEXT,
		projection_json TEXT NOT NULL DEFAULT '{}',
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		processed_at TIMESTAMPTZ,
		PRIMARY KEY (event_id),
		UNIQUE (workspace_id, event_id),
		CONSTRAINT session_events_thread_sequence_key UNIQUE NULLS NOT DISTINCT (workspace_id, session_id, session_thread_id, sequence),
		CONSTRAINT session_events_attachment_scope_key UNIQUE (workspace_id, session_id, session_thread_id, event_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_events_revision_shape CHECK (revision > 0),
		CONSTRAINT session_events_visibility_shape CHECK (visibility IN ('public', 'internal')),
		CONSTRAINT session_events_latest_stream_position_shape CHECK (latest_stream_position >= 0),
		CONSTRAINT session_events_insert_stream_position_shape CHECK (insert_stream_position >= 0)
	)`

	createPostgreSQLSessionEventStreamChangesTable = `CREATE TABLE IF NOT EXISTS session_event_stream_changes (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		stream_position BIGINT GENERATED ALWAYS AS IDENTITY,
		event_id TEXT NOT NULL,
		session_thread_id TEXT,
		revision BIGINT NOT NULL,
		visibility TEXT NOT NULL DEFAULT 'public',
		session_visible BOOLEAN NOT NULL,
		changed_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, stream_position),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, event_id) REFERENCES session_events(workspace_id, event_id) ON DELETE CASCADE,
		CONSTRAINT session_event_stream_changes_revision_shape CHECK (revision > 0),
		CONSTRAINT session_event_stream_changes_visibility_shape CHECK (visibility IN ('public', 'internal'))
	)`

	// session_messages is the durable context-window projection LoadContext
	// replays on cold start. The kind CHECK lists the values; their projection
	// meaning:
	//
	//   kind                   projects
	//   user                   an accepted user-input context message
	//   assistant              assistant output and tool state (agent.tool_use /
	//                          agent.tool_result merge into the assistant row)
	//   runtime_notification   an internal, model-visible runtime notice (e.g.
	//                          background command completion); NOT a public
	//                          user.message and NOT a second tool result
	//   compaction             the active context-window boundary; LoadContext
	//                          starts at the latest compaction row and loads
	//                          later rows by sequence
	//
	// Usage, status, request/transport metadata, billing data, and raw
	// attachment bytes never enter this table.
	//
	// UPDATE-WITH: services/bridge projection, compaction, and
	// fork writers and LoadContext.
	createPostgreSQLSessionMessagesTable = `CREATE TABLE IF NOT EXISTS session_messages (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		message_id TEXT NOT NULL,
		sequence BIGINT NOT NULL,
		kind TEXT NOT NULL,
		data_json TEXT NOT NULL,
		source_event_id TEXT,
		repair_key TEXT,
		model_request_id TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, message_id),
		UNIQUE (workspace_id, session_id, session_thread_id, sequence),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_messages_sequence_shape CHECK (sequence > 0),
		CONSTRAINT session_messages_kind_shape CHECK (kind IN ('user', 'assistant', 'runtime_notification', 'compaction')),
		CONSTRAINT session_messages_model_request_id_shape CHECK (
			model_request_id IS NULL OR (kind = 'assistant' AND model_request_id <> '')
		)
	)`

	createPostgreSQLSessionFileAttachmentConsumptionsTable = `CREATE TABLE IF NOT EXISTS session_file_attachment_consumptions (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		request_start_event_id TEXT NOT NULL,
		source_event_id TEXT NOT NULL,
		file_id TEXT NOT NULL,
		CONSTRAINT session_file_attachment_consumptions_source_file_key
			UNIQUE (workspace_id, source_event_id, file_id),
		FOREIGN KEY (workspace_id, session_id)
			REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, session_id, session_thread_id)
			REFERENCES session_threads(workspace_id, session_id, id),
		FOREIGN KEY (
			workspace_id, session_id, session_thread_id, request_start_event_id
		) REFERENCES session_events(
			workspace_id, session_id, session_thread_id, event_id
		),
		FOREIGN KEY (
			workspace_id, session_id, session_thread_id, source_event_id
		) REFERENCES session_events(
			workspace_id, session_id, session_thread_id, event_id
		)
	)`

	// A child thread's parent transcript is durable context material, not a
	// child message. It remains outside child message sequencing until a
	// successful compaction checkpoint atomically consumes it.
	createPostgreSQLSessionThreadContextPrefixesTable = `CREATE TABLE IF NOT EXISTS session_thread_context_prefixes (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		child_thread_id TEXT NOT NULL,
		parent_thread_id TEXT NOT NULL,
		parent_boundary_event_id TEXT NOT NULL,
		entries_json TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		consumed_by_checkpoint_message_id TEXT,
		PRIMARY KEY (workspace_id, session_id, child_thread_id),
		FOREIGN KEY (workspace_id, session_id, child_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, session_id, parent_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, parent_boundary_event_id) REFERENCES session_events(workspace_id, event_id) ON DELETE RESTRICT,
		FOREIGN KEY (workspace_id, consumed_by_checkpoint_message_id) REFERENCES session_messages(workspace_id, message_id) ON DELETE RESTRICT,
		CONSTRAINT session_thread_context_prefixes_identity_shape CHECK (
			child_thread_id <> '' AND parent_thread_id <> '' AND parent_boundary_event_id <> ''
		)
	)`

	// session_pending_tool_uses is the durable route for every public Tool Call
	// until its terminal Tool Result commits. It closes the crash gap between
	// the Tool Use declaration and executor admission; approval is one route
	// state, not the table's ownership boundary.
	// The row is a cold-resume and stale-reply routing record, NOT a context
	// message: LoadContext installs unresolved rows as thread-local ToolJob
	// state, and a rehydrated decision is applied, never re-evaluated under
	// current policy.
	//
	//   status      meaning                             entered on                                 legal next
	//   pending     approval opened, undecided          agent.tool_use(ask) projection             resolving, cancelled
	//   resolving   execution authorized or denied,     agent.tool_use(allow/deny) projection or   resolved, cancelled
	//               awaiting the terminal Tool Result   user.tool_confirmation processing
	//   resolved    terminal agent.tool_result          markPendingToolResultResolved              (terminal)
	//               committed (sets result_event_id)
	//   cancelled   route closed without Tool Result    interrupt or request closeout              (terminal)
	//
	// UPDATE-WITH: services/bridge pending-tool projection, input-commit,
	// terminal settlement, and LoadContext paths.
	createPostgreSQLSessionPendingToolUsesTable = `CREATE TABLE IF NOT EXISTS session_pending_tool_uses (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		tool_use_event_id TEXT NOT NULL,
		model_tool_call_id TEXT NOT NULL,
		tool_name TEXT NOT NULL,
		input_json TEXT NOT NULL,
		decision TEXT,
		deny_message TEXT,
		status TEXT NOT NULL,
		result_event_id TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		resolved_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, session_thread_id, tool_use_event_id),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_pending_tool_uses_decision_shape CHECK (decision IS NULL OR decision IN ('allow', 'deny')),
		CONSTRAINT session_pending_tool_uses_status_shape CHECK (status IN ('pending', 'resolving', 'resolved', 'cancelled'))
	)`

	// session_background_tasks is the durable recovery record for background
	// sandbox commands that already returned a model-visible running result but
	// may finish after the owning Runtime Pod hot state is gone. Recovery
	// identity the DDL cannot describe:
	//
	//   - provider_command_metadata_json is driver-written and never
	//     model-visible; it is a JSON object bounded to 4096 bytes (4 KiB) of
	//     UTF-8. The provider_metadata_shape CHECK enforces both the object
	//     shape and the byte cap, and Sandbox Service validates the same before write.
	//   - The active driver's recovery identity lives in provider_session_id and
	//     provider_command_id: for the Daytona helper driver these hold the
	//     provider sandbox id and the helper task_id, and that helper task
	//     directory keyed by task_id is the detached-process source of truth.
	//   - The model-visible task_id alone cannot recover the provider process,
	//     and Daytona's own launch-command ids recover nothing for a detached
	//     child and are not persisted.
	//   - None of these identifiers appear in model-visible tool output, public
	//     events, or provider requests.
	//
	// UPDATE-WITH: services/sandbox background-task settlement and recovery;
	// the provider_metadata_shape CHECK below.
	createPostgreSQLSessionBackgroundTasksTable = `CREATE TABLE IF NOT EXISTS session_background_tasks (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		task_id TEXT NOT NULL,
		source_tool_use_event_id TEXT NOT NULL,
		binding_id TEXT,
		sandbox_id TEXT NOT NULL,
		provider TEXT NOT NULL DEFAULT 'daytona',
		binding_revision BIGINT NOT NULL DEFAULT 1,
		provider_session_id TEXT NOT NULL,
		provider_command_id TEXT NOT NULL,
		provider_command_metadata_json TEXT NOT NULL DEFAULT '{}',
		resource_roots_json TEXT NOT NULL DEFAULT '[]',
		stdin_write_sequence BIGINT NOT NULL DEFAULT 0,
		status TEXT NOT NULL,
		terminal_result_json TEXT,
		terminal_result_digest TEXT,
		terminal_event_id TEXT,
		reconcile_generation BIGINT NOT NULL DEFAULT 1,
		next_poll_at TIMESTAMPTZ,
		release_operation_id TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		terminal_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, task_id),
		UNIQUE (workspace_id, session_id, source_tool_use_event_id),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_background_tasks_status_shape CHECK (status IN ('running', 'completed', 'failed', 'cancelled', 'expired', 'unknown_outcome')),
		CONSTRAINT session_background_tasks_stdin_write_sequence_shape CHECK (stdin_write_sequence >= 0),
		CONSTRAINT session_background_tasks_binding_revision_shape CHECK (binding_revision > 0),
		CONSTRAINT session_background_tasks_reconcile_generation_shape CHECK (reconcile_generation > 0),
		CONSTRAINT session_background_tasks_resource_roots_shape CHECK (jsonb_typeof(resource_roots_json::jsonb) = 'array'),
		CONSTRAINT session_background_tasks_provider_metadata_shape CHECK (
			jsonb_typeof(provider_command_metadata_json::jsonb) = 'object'
			AND octet_length(convert_to(provider_command_metadata_json, 'UTF8')) <= 4096
		)
	)`

	createPostgreSQLSessionRuntimeInboxTable = `CREATE TABLE IF NOT EXISTS session_runtime_inbox (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		runtime_input_id TEXT NOT NULL,
		input_kind TEXT NOT NULL,
		rejection_reason_code TEXT,
		event_ids_json TEXT NOT NULL DEFAULT '[]',
		sequence_from BIGINT,
		sequence_to BIGINT,
		status TEXT NOT NULL,
		binding_id TEXT,
		binding_generation BIGINT,
		target_pod_uid TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		committed_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, runtime_input_id),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_runtime_inbox_kind_shape CHECK (input_kind IN ('messages', 'interrupt_control', 'tool_confirmation', 'task_notification', 'agent_mail', 'approval_review', 'rejection')),
		CONSTRAINT session_runtime_inbox_status_shape CHECK (status IN ('queued', 'delivering', 'accepted', 'parked', 'committed', 'cancelled', 'dead_lettered')),
		CONSTRAINT session_runtime_inbox_sequence_shape CHECK (
			(sequence_from IS NULL AND sequence_to IS NULL)
			OR (sequence_from IS NOT NULL AND sequence_to IS NOT NULL AND sequence_from <= sequence_to)
		),
		CONSTRAINT session_runtime_inbox_binding_shape CHECK (
			(status = 'queued' AND binding_id IS NULL AND binding_generation IS NULL AND target_pod_uid IS NULL)
			OR (status IN ('delivering', 'accepted', 'parked', 'committed')
				AND binding_id IS NOT NULL AND binding_id <> ''
				AND binding_generation IS NOT NULL AND binding_generation > 0
				AND target_pod_uid IS NOT NULL AND target_pod_uid <> '')
			OR (status IN ('cancelled', 'dead_lettered') AND (
				(binding_id IS NULL AND binding_generation IS NULL AND target_pod_uid IS NULL)
				OR (binding_id IS NOT NULL AND binding_id <> ''
					AND binding_generation IS NOT NULL AND binding_generation > 0
					AND target_pod_uid IS NOT NULL AND target_pod_uid <> '')
			))
		)
	)`

	// session_event_idempotency_keys stores hashed idempotency state for accepted
	// event-send responses. It deliberately stores only digests/hashes and
	// response echo JSON, never raw Idempotency-Key values, auth headers, bearer
	// tokens, provider credentials, or raw request bodies.
	createPostgreSQLSessionEventIdempotencyKeysTable = `CREATE TABLE IF NOT EXISTS session_event_idempotency_keys (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		idempotency_key_digest BYTEA NOT NULL,
		canonical_request_hash BYTEA NOT NULL,
		response_events_json TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		UNIQUE (workspace_id, session_id, idempotency_key_digest),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE
	)`

	// session_runtime_bindings stores current routing identity for Runtime Pod
	// targets only. Bridge owns this binding row and allocates
	// binding_generation from the database-backed monotonic sequence below;
	// Runtime Pod does not own durable idle or expiry lifecycle state.
	createPostgreSQLSessionRuntimeBindingsTable = `CREATE TABLE IF NOT EXISTS session_runtime_bindings (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		binding_id TEXT NOT NULL,
		binding_generation BIGINT NOT NULL,
		agent_runtime_namespace TEXT NOT NULL,
		agent_runtime_pod_name TEXT NOT NULL,
		agent_runtime_pod_uid TEXT NOT NULL,
		agent_runtime_pod_ip TEXT NOT NULL,
		bound_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_runtime_bindings_binding_id_shape CHECK (binding_id <> ''),
		CONSTRAINT session_runtime_bindings_generation_shape CHECK (
			binding_generation > 0 AND binding_generation <= 4294967295
		),
		CONSTRAINT session_runtime_bindings_identity_shape CHECK (
			agent_runtime_namespace <> ''
			AND agent_runtime_pod_name <> ''
			AND agent_runtime_pod_uid <> ''
			AND agent_runtime_pod_ip <> ''
		)
	)`

	createPostgreSQLSessionRuntimeBindingGenerationSequence = `CREATE SEQUENCE IF NOT EXISTS session_runtime_binding_generation_seq
		AS BIGINT
		MINVALUE 1
		MAXVALUE 4294967295
		START WITH 1
		NO CYCLE`

	createPostgreSQLSessionMCPManifestsTable = `CREATE TABLE IF NOT EXISTS session_mcp_manifests (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		mcp_server_name TEXT NOT NULL,
		tools_json TEXT,
		manifest_etag TEXT,
		manifest_generation BIGINT NOT NULL,
		readiness TEXT NOT NULL DEFAULT 'ready',
		diagnostic TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, mcp_server_name),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_mcp_manifests_identity_shape CHECK (
			mcp_server_name <> '' AND (manifest_etag IS NULL OR manifest_etag <> '')
		),
		CONSTRAINT session_mcp_manifests_tools_json_shape CHECK (
			tools_json IS NULL OR (
				octet_length(tools_json) <= 262144
				AND jsonb_typeof(tools_json::jsonb) = 'array'
			)
		),
		CONSTRAINT session_mcp_manifests_generation_shape CHECK (manifest_generation > 0),
		CONSTRAINT session_mcp_manifests_readiness_shape CHECK (
			(
				readiness = 'ready'
				AND diagnostic IS NULL
				AND tools_json IS NOT NULL
				AND manifest_etag IS NOT NULL
			) OR (
				readiness = 'unready'
				AND diagnostic IS NOT NULL
				AND diagnostic <> ''
				AND (
					(tools_json IS NULL AND manifest_etag IS NULL)
					OR (tools_json IS NOT NULL AND manifest_etag IS NOT NULL)
				)
			)
		)
	)`

	createPostgreSQLSessionRuntimeStatusTable = `CREATE TABLE IF NOT EXISTS session_runtime_status (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		status TEXT NOT NULL,
		status_event_id TEXT,
		idle_since TIMESTAMPTZ,
		running_since TIMESTAMPTZ,
		active_seconds_total DOUBLE PRECISION NOT NULL DEFAULT 0,
		cleanup_after TIMESTAMPTZ,
		cleanup_enqueued_at TIMESTAMPTZ,
		cleanup_claimed_at TIMESTAMPTZ,
		cleanup_job_id TEXT,
		binding_id TEXT,
		binding_generation BIGINT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_runtime_status_status_shape CHECK (status IN ('running', 'idle')),
		CONSTRAINT session_runtime_status_binding_generation_shape CHECK (binding_generation IS NULL OR binding_generation > 0),
		CONSTRAINT session_runtime_status_active_seconds_shape CHECK (active_seconds_total >= 0)
	)`

	createPostgreSQLSessionTurnRetriesTable = `CREATE TABLE IF NOT EXISTS session_turn_retries (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		provider_attempts BIGINT NOT NULL DEFAULT 0,
		compaction_attempts BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, session_thread_id),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_turn_retries_provider_attempts_shape CHECK (provider_attempts >= 0),
		CONSTRAINT session_turn_retries_compaction_attempts_shape CHECK (compaction_attempts >= 0)
	)`

	createPostgreSQLSessionBridgeOperationsTable = `CREATE TABLE IF NOT EXISTS session_bridge_operations (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		operation TEXT NOT NULL,
		idempotency_key TEXT NOT NULL,
		source_kind TEXT NOT NULL,
		request_hash TEXT NOT NULL,
		declaration_digest TEXT,
		receipt_json TEXT,
		ack_status TEXT NOT NULL,
		runtime_input_id TEXT,
		runtime_write_id TEXT,
		error_code TEXT,
		result_json TEXT NOT NULL DEFAULT '{}',
		stdin_write_seq BIGINT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_bridge_operations_operation_shape CHECK (operation IN (
			'commit_inputs',
			'commit_task_notification_result',
			'write_event',
			'settle_tool_result',
			'write_request_end',
			'finish_idle',
			'create_child_thread',
			'deliver_inter_agent_mail',
			'resolve_child_thread',
			'list_child_threads',
			'close_child_control',
			'close_approval_reviewer',
			'mark_child_thread_active',
			'read_command_result',
			'send_command_input',
			'cancel_command',
			'run_memory',
			'create_transient_attachment',
			'mcp_manifest_changed',
			'commit_mcp_tool_result',
			'relinquish_mcp_tool_result',
			'commit_internal_tool_repair',
			'commit_runtime_termination'
		)),
		CONSTRAINT session_bridge_operations_ack_shape CHECK (ack_status IN ('committed', 'rejected')),
		CONSTRAINT session_bridge_operations_key_shape CHECK (idempotency_key <> '' AND request_hash <> ''),
		CONSTRAINT session_bridge_operations_stdin_write_seq_shape CHECK (stdin_write_seq IS NULL OR stdin_write_seq > 0)
	)`

	createPostgreSQLSessionRuntimeToolResultsTable = `CREATE TABLE IF NOT EXISTS session_runtime_tool_results (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		tool_use_event_id TEXT NOT NULL,
		tool_kind TEXT NOT NULL,
		normalized_input_hash TEXT NOT NULL,
		tool_name TEXT NOT NULL,
		input_json TEXT NOT NULL,
		ack_status TEXT NOT NULL,
		result_json TEXT,
		model_tool_call_id TEXT,
		execution_state TEXT,
		execution_attempt_generation BIGINT,
		waiting_activation_operation_id TEXT,
		waiting_materialization_operation_id TEXT,
		authorized_binding_revision BIGINT,
		authorized_provider_resource_id TEXT,
		preparation_deadline TIMESTAMPTZ,
		result_digest TEXT,
		provider_command_reference_json TEXT,
		cancel_requested_at TIMESTAMPTZ,
		cancel_state TEXT,
		cancel_submitted_at TIMESTAMPTZ,
		consumed_by_terminal_event_id TEXT,
		consumption_reason TEXT,
		helper_recovery_count INTEGER NOT NULL DEFAULT 0,
		background_task_started BOOLEAN NOT NULL DEFAULT FALSE,
		task_id TEXT,
		background_operation_kind TEXT,
		background_operation_state TEXT,
		background_request_id TEXT,
		background_task_id TEXT,
		background_max_output_tokens INTEGER,
		background_write_sequence BIGINT,
		memory_projection_state TEXT,
		mcp_claim_status TEXT,
		mcp_claim_id TEXT,
		mcp_claim_lease_expires_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, session_thread_id, tool_use_event_id),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, consumed_by_terminal_event_id) REFERENCES session_events(workspace_id, event_id),
		CONSTRAINT session_runtime_tool_results_kind_shape CHECK (tool_kind IN ('sandbox_tool', 'sandbox_background', 'memory', 'mcp')),
		CONSTRAINT session_runtime_tool_results_ack_shape CHECK (ack_status IN ('committed', 'rejected')),
		CONSTRAINT session_runtime_tool_results_execution_state_shape CHECK (
			execution_state IS NULL OR execution_state IN (
				'pending', 'preparing', 'running', 'waiting_activation',
				'waiting_materialization', 'terminal_unconsumed', 'consumed'
			)
		),
		CONSTRAINT session_runtime_tool_results_execution_generation_shape CHECK (
			execution_attempt_generation IS NULL OR execution_attempt_generation > 0
		),
		CONSTRAINT session_runtime_tool_results_sandbox_execution_shape CHECK (
			(tool_kind = 'sandbox_tool'
				AND model_tool_call_id IS NOT NULL AND model_tool_call_id <> ''
				AND execution_state IS NOT NULL
				AND execution_attempt_generation IS NOT NULL)
			OR (tool_kind = 'sandbox_background'
				AND model_tool_call_id IS NULL
				AND execution_state IS NULL
				AND execution_attempt_generation IS NULL
				AND waiting_activation_operation_id IS NULL
				AND waiting_materialization_operation_id IS NULL
				AND authorized_binding_revision IS NULL
				AND authorized_provider_resource_id IS NULL
				AND preparation_deadline IS NULL
				AND provider_command_reference_json IS NULL
				AND helper_recovery_count = 0
				AND background_task_started = FALSE
				AND task_id IS NULL)
			OR (tool_kind IN ('memory', 'mcp')
				AND result_json IS NOT NULL
				AND model_tool_call_id IS NULL
				AND execution_state IS NULL
				AND execution_attempt_generation IS NULL
				AND waiting_activation_operation_id IS NULL
				AND waiting_materialization_operation_id IS NULL
				AND authorized_binding_revision IS NULL
				AND authorized_provider_resource_id IS NULL
				AND preparation_deadline IS NULL
				AND result_digest IS NULL
				AND provider_command_reference_json IS NULL
				AND cancel_requested_at IS NULL
				AND cancel_state IS NULL
				AND cancel_submitted_at IS NULL
				AND consumed_by_terminal_event_id IS NULL
				AND consumption_reason IS NULL
				AND helper_recovery_count = 0
				AND background_task_started = FALSE
				AND task_id IS NULL)
		),
		CONSTRAINT session_runtime_tool_results_background_operation_shape CHECK (
			(tool_kind = 'sandbox_background'
				AND background_operation_kind IN ('poll', 'stdin', 'cancel')
				AND background_operation_state IN ('pending', 'submitted', 'terminal')
				AND background_request_id IS NOT NULL AND background_request_id <> ''
				AND background_task_id IS NOT NULL AND background_task_id <> ''
				AND background_max_output_tokens IS NOT NULL AND background_max_output_tokens >= 0
				AND (background_write_sequence IS NULL OR background_write_sequence > 0))
			OR (tool_kind <> 'sandbox_background'
				AND background_operation_kind IS NULL
				AND background_operation_state IS NULL
				AND background_request_id IS NULL
				AND background_task_id IS NULL
				AND background_max_output_tokens IS NULL
				AND background_write_sequence IS NULL)
		),
		CONSTRAINT session_runtime_tool_results_cancellation_shape CHECK (
			(cancel_requested_at IS NULL AND cancel_state IS NULL AND cancel_submitted_at IS NULL)
			OR (tool_kind = 'sandbox_tool' AND cancel_requested_at IS NOT NULL
				AND cancel_state = 'pending' AND cancel_submitted_at IS NULL)
			OR (tool_kind = 'sandbox_tool' AND cancel_requested_at IS NOT NULL
				AND cancel_state = 'submitted' AND cancel_submitted_at IS NOT NULL)
		),
		CONSTRAINT session_runtime_tool_results_consumption_shape CHECK (
			(tool_kind NOT IN ('sandbox_tool', 'sandbox_background')) OR (
			tool_kind = 'sandbox_tool' AND (
				(execution_state IN ('pending', 'preparing', 'running', 'waiting_activation', 'waiting_materialization')
					AND result_json IS NULL AND result_digest IS NULL
					AND consumed_by_terminal_event_id IS NULL AND consumption_reason IS NULL)
				OR (execution_state = 'terminal_unconsumed'
					AND result_json IS NOT NULL AND result_digest IS NOT NULL AND result_digest <> ''
					AND consumed_by_terminal_event_id IS NULL AND consumption_reason IS NULL)
				OR (execution_state = 'consumed'
					AND result_json IS NULL AND result_digest IS NOT NULL AND result_digest <> ''
					AND consumed_by_terminal_event_id IS NOT NULL
					AND consumption_reason IN (
						'conversation_tool_result', 'pod_lost', 'runtime_terminated', 'cleanup_wait_expired'
					))
			)) OR (tool_kind = 'sandbox_background' AND (
				(background_operation_state IN ('pending', 'submitted')
					AND result_json IS NULL AND result_digest IS NULL
					AND consumed_by_terminal_event_id IS NULL AND consumption_reason IS NULL)
				OR (background_operation_state = 'terminal' AND result_json IS NOT NULL
					AND result_digest IS NOT NULL
					AND consumed_by_terminal_event_id IS NULL AND consumption_reason IS NULL)
				OR (background_operation_state = 'terminal' AND result_json IS NULL
					AND result_digest IS NOT NULL
					AND consumed_by_terminal_event_id IS NOT NULL
					AND consumption_reason IN (
						'conversation_tool_result', 'pod_lost', 'runtime_terminated', 'cleanup_wait_expired'
					))
			))
		),
		CONSTRAINT session_runtime_tool_results_helper_recovery_shape CHECK (helper_recovery_count >= 0 AND helper_recovery_count <= 1),
		CONSTRAINT session_runtime_tool_results_memory_projection_state_shape CHECK (
			(tool_kind = 'memory' AND (
				memory_projection_state IS NULL
				OR memory_projection_state IN ('pending', 'refreshed', 'skipped_cold', 'failed')
			))
			OR (tool_kind <> 'memory' AND memory_projection_state IS NULL)
		),
		CONSTRAINT session_runtime_tool_results_mcp_claim_status_shape CHECK (
			mcp_claim_status IS NULL OR mcp_claim_status IN ('stored', 'in_flight', 'consumed')
		),
		CONSTRAINT session_runtime_tool_results_mcp_claim_shape CHECK (
			(tool_kind <> 'mcp' AND mcp_claim_status IS NULL AND mcp_claim_id IS NULL AND mcp_claim_lease_expires_at IS NULL)
			OR (tool_kind = 'mcp' AND mcp_claim_status IS NOT NULL AND (
				(mcp_claim_status = 'stored' AND mcp_claim_id IS NULL AND mcp_claim_lease_expires_at IS NULL)
				OR (mcp_claim_status = 'consumed' AND mcp_claim_id IS NULL AND mcp_claim_lease_expires_at IS NULL)
				OR (mcp_claim_status = 'in_flight' AND mcp_claim_id IS NOT NULL AND mcp_claim_lease_expires_at IS NOT NULL)
			))
		),
		CONSTRAINT session_runtime_tool_results_input_shape CHECK (normalized_input_hash <> '' AND tool_name <> '' AND input_json <> '')
	)`

	createPostgreSQLSessionOutputCapturesTable = `CREATE TABLE IF NOT EXISTS session_output_captures (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		source_path TEXT NOT NULL,
		last_file_id TEXT,
		last_size_bytes BIGINT,
		last_sha256 TEXT,
		last_captured_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, source_path),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, last_file_id) REFERENCES files(workspace_id, file_id)
	)`

	createPostgreSQLSandboxOutputCaptureOperationsTable = `CREATE TABLE IF NOT EXISTS sandbox_output_capture_operations (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		finish_idle_write_id TEXT NOT NULL,
		capture_generation BIGINT NOT NULL,
		state TEXT NOT NULL,
		binding_id TEXT NOT NULL,
		binding_generation BIGINT NOT NULL,
		logical_sandbox_id TEXT,
		provider TEXT,
		provider_resource_id TEXT,
		sandbox_binding_revision BIGINT,
		manifest_json TEXT NOT NULL DEFAULT '[]',
		skipped_json TEXT NOT NULL DEFAULT '[]',
		scan_records_json TEXT NOT NULL DEFAULT '[]',
		failure_kind TEXT,
		failure_detail TEXT,
		outcome_state TEXT,
		outcome_digest TEXT,
		retain_until TIMESTAMPTZ NOT NULL,
		cleanup_generation BIGINT NOT NULL DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		staged_at TIMESTAMPTZ,
		adopted_at TIMESTAMPTZ,
		cleaned_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, finish_idle_write_id, capture_generation),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT sandbox_output_capture_generation_shape CHECK (capture_generation > 0 AND binding_generation > 0 AND cleanup_generation >= 0 AND (sandbox_binding_revision IS NULL OR sandbox_binding_revision > 0)),
		CONSTRAINT sandbox_output_capture_state_shape CHECK (state IN ('pending', 'running', 'staged', 'skipped_unavailable', 'failed', 'cleanup_pending', 'cleaned', 'adopted')),
		CONSTRAINT sandbox_output_capture_outcome_shape CHECK (
			(outcome_state IS NULL AND outcome_digest IS NULL)
			OR (outcome_state IN ('staged', 'skipped_unavailable', 'failed') AND outcome_digest IS NOT NULL AND outcome_digest <> '')
		),
		CONSTRAINT sandbox_output_capture_provider_shape CHECK ((provider IS NULL) = (provider_resource_id IS NULL) AND (provider IS NULL) = (logical_sandbox_id IS NULL) AND (provider IS NULL) = (sandbox_binding_revision IS NULL))
	)`

	createPostgreSQLSandboxOutputCaptureBlobsTable = `CREATE TABLE IF NOT EXISTS sandbox_output_capture_blobs (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		finish_idle_write_id TEXT NOT NULL,
		capture_generation BIGINT NOT NULL,
		source_path TEXT NOT NULL,
		blob_pointer TEXT NOT NULL,
		size_bytes BIGINT NOT NULL,
		sha256 TEXT NOT NULL,
		state TEXT NOT NULL,
		file_id TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		uploaded_at TIMESTAMPTZ,
		adopted_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, finish_idle_write_id, capture_generation, source_path),
		UNIQUE (blob_pointer),
		FOREIGN KEY (workspace_id, session_id, finish_idle_write_id, capture_generation)
			REFERENCES sandbox_output_capture_operations(workspace_id, session_id, finish_idle_write_id, capture_generation) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, file_id) REFERENCES files(workspace_id, file_id),
		CONSTRAINT sandbox_output_capture_blob_size_shape CHECK (size_bytes >= 0),
		CONSTRAINT sandbox_output_capture_blob_state_shape CHECK (state IN ('pending', 'uploaded', 'adopted'))
	)`

	createPostgreSQLSessionTransientAttachmentsTable = `CREATE TABLE IF NOT EXISTS session_transient_attachments (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		attachment_ref TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		source_tool_use_event_id TEXT NOT NULL,
		blob_pointer TEXT NOT NULL,
		mime TEXT NOT NULL,
		metadata_json TEXT NOT NULL DEFAULT '{}',
		status TEXT NOT NULL,
		expires_at TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, attachment_ref),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT session_transient_attachments_status_shape CHECK (status IN ('uploading', 'staged', 'active', 'consumed', 'expired', 'deleting', 'deleted', 'failed')),
		CONSTRAINT session_transient_attachments_mime_shape CHECK (mime IN ('application/pdf', 'image/png', 'image/jpeg', 'image/gif', 'image/webp'))
	)`

	createPostgreSQLRequestUsageDetailsTable = `CREATE TABLE IF NOT EXISTS request_usage_details (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		session_thread_id TEXT NOT NULL,
		model_request_id TEXT NOT NULL,
		runtime_write_id TEXT NOT NULL,
		request_kind TEXT NOT NULL,
		input_total_tokens BIGINT NOT NULL DEFAULT 0,
		input_uncached_tokens BIGINT NOT NULL DEFAULT 0,
		input_cache_read_tokens BIGINT,
		input_cache_write_tokens BIGINT,
		output_total_tokens BIGINT NOT NULL DEFAULT 0,
		output_reasoning_tokens BIGINT,
		total_tokens BIGINT,
		provider_usage_json TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id, model_request_id, runtime_write_id),
		FOREIGN KEY (workspace_id, session_id, session_thread_id) REFERENCES session_threads(workspace_id, session_id, id) ON DELETE CASCADE,
		CONSTRAINT request_usage_details_input_total_shape CHECK (input_total_tokens >= 0),
		CONSTRAINT request_usage_details_input_uncached_shape CHECK (input_uncached_tokens >= 0),
		CONSTRAINT request_usage_details_cache_read_shape CHECK (input_cache_read_tokens IS NULL OR input_cache_read_tokens >= 0),
		CONSTRAINT request_usage_details_cache_write_shape CHECK (input_cache_write_tokens IS NULL OR input_cache_write_tokens >= 0),
		CONSTRAINT request_usage_details_output_total_shape CHECK (output_total_tokens >= 0),
		CONSTRAINT request_usage_details_reasoning_shape CHECK (output_reasoning_tokens IS NULL OR output_reasoning_tokens >= 0),
		CONSTRAINT request_usage_details_total_shape CHECK (total_tokens IS NULL OR total_tokens >= 0),
		CONSTRAINT request_usage_details_kind_shape CHECK (request_kind IN ('agent_provider_request', 'compaction_summary', 'approval_reviewer'))
	)`

	createPostgreSQLSessionProviderAuthTable = `CREATE TABLE IF NOT EXISTS session_provider_auth (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		provider_id TEXT NOT NULL,
		vault_id TEXT NOT NULL,
		credential_id TEXT NOT NULL,
		access_mode TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		deleted_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, provider_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_provider_auth_workspace_vault_credential_fkey
			FOREIGN KEY (workspace_id, vault_id, credential_id) REFERENCES credentials(workspace_id, vault_id, id) ON DELETE RESTRICT,
		CONSTRAINT session_provider_auth_provider_shape CHECK (provider_id <> ''),
		CONSTRAINT session_provider_auth_access_mode_shape CHECK (access_mode <> '')
	)`

	createPostgreSQLPlatformProviderKeysTable = `CREATE TABLE IF NOT EXISTS platform_provider_keys (
		key_id TEXT PRIMARY KEY,
		provider_id TEXT NOT NULL,
		encrypted_key BYTEA NOT NULL,
		weight INT NOT NULL DEFAULT 1,
		priority INT NOT NULL DEFAULT 0,
		cache_scope TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		disabled_reason TEXT,
		updated_at TIMESTAMPTZ NOT NULL,
		CONSTRAINT platform_provider_keys_id_shape CHECK (key_id LIKE 'pfk\_%' ESCAPE '\'),
		CONSTRAINT platform_provider_keys_provider_shape CHECK (provider_id IN ('anthropic', 'openai', 'deepseek')),
		CONSTRAINT platform_provider_keys_encrypted_key_shape CHECK (octet_length(encrypted_key) > 0),
		CONSTRAINT platform_provider_keys_weight_shape CHECK (weight >= 0),
		CONSTRAINT platform_provider_keys_priority_shape CHECK (priority >= 0),
		CONSTRAINT platform_provider_keys_cache_scope_shape CHECK (cache_scope <> ''),
		CONSTRAINT platform_provider_keys_status_shape CHECK (status IN ('active', 'disabled'))
	)`

	createPostgreSQLQueueJobsTable = `CREATE TABLE IF NOT EXISTS queue_jobs (
		id TEXT NOT NULL,
		workspace_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		partition_key TEXT NOT NULL,
		queue_partition_sequence BIGINT NOT NULL,
		causal_session_id TEXT,
		delivery_scope TEXT NOT NULL DEFAULT 'partition',
		delivery_thread_id TEXT,
		control_class TEXT NOT NULL DEFAULT 'ordinary',
		dedupe_key TEXT,
		payload_version INTEGER NOT NULL DEFAULT 1,
		status TEXT NOT NULL,
		payload_json TEXT NOT NULL,
		priority INTEGER NOT NULL DEFAULT 0,
		lease_token TEXT,
		leased_by TEXT,
		leased_at TIMESTAMPTZ,
		leased_until TIMESTAMPTZ,
		attempt_count INTEGER NOT NULL DEFAULT 0,
		defer_count INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 10,
		available_at TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		acknowledged_at TIMESTAMPTZ,
		cancelled_at TIMESTAMPTZ,
		dead_lettered_at TIMESTAMPTZ,
		last_error_kind TEXT,
		last_error_message TEXT,
		PRIMARY KEY (id),
		UNIQUE (workspace_id, id),
		CONSTRAINT queue_jobs_kind_shape CHECK (kind IN ('runtime_input', 'runtime_recovery', 'runtime_config_update', 'cleanup_session', 'session_delete_cleanup', 'environment_build', 'environment_ready_fanout', 'sandbox_tool_execute', 'sandbox_activate', 'sandbox_materialize', 'sandbox_release', 'sandbox_tool_cancel', 'sandbox_output_capture', 'sandbox_output_capture_cleanup', 'sandbox_memory_projection', 'sandbox_background_command', 'sandbox_background_reconcile')),
		CONSTRAINT queue_jobs_status_shape CHECK (status IN ('pending', 'leased', 'acknowledged', 'cancelled', 'dead_lettered')),
		CONSTRAINT queue_jobs_payload_version_shape CHECK (payload_version > 0),
		CONSTRAINT queue_jobs_partition_sequence_shape CHECK (queue_partition_sequence > 0),
		CONSTRAINT queue_jobs_delivery_scope_shape CHECK (delivery_scope IN ('partition', 'thread', 'session')),
		CONSTRAINT queue_jobs_control_class_shape CHECK (control_class IN ('ordinary', 'interrupt', 'agent_mail')),
		CONSTRAINT queue_jobs_delivery_authority_shape CHECK (
			(delivery_scope = 'partition' AND causal_session_id IS NULL AND delivery_thread_id IS NULL AND control_class = 'ordinary')
			OR (delivery_scope = 'thread' AND causal_session_id IS NOT NULL AND delivery_thread_id IS NOT NULL)
			OR (delivery_scope = 'session' AND causal_session_id IS NOT NULL AND delivery_thread_id IS NULL AND control_class = 'ordinary')
		),
		CONSTRAINT queue_jobs_attempt_count_shape CHECK (attempt_count >= 0),
		CONSTRAINT queue_jobs_defer_count_shape CHECK (defer_count >= 0),
		CONSTRAINT queue_jobs_max_attempts_shape CHECK (max_attempts >= 0),
		CONSTRAINT queue_jobs_lease_shape CHECK (
			(status = 'leased' AND lease_token IS NOT NULL AND leased_by IS NOT NULL AND leased_at IS NOT NULL AND leased_until IS NOT NULL)
			OR (status <> 'leased')
		)
	)`

	createPostgreSQLQueuePartitionCountersTable = `CREATE TABLE IF NOT EXISTS queue_partition_counters (
		workspace_id TEXT NOT NULL,
		partition_key TEXT NOT NULL,
		last_sequence BIGINT NOT NULL DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, partition_key),
		CONSTRAINT queue_partition_counters_partition_shape CHECK (partition_key <> ''),
		CONSTRAINT queue_partition_counters_sequence_shape CHECK (last_sequence >= 0)
	)`

	createPostgreSQLSessionResourcesTable = `CREATE TABLE IF NOT EXISTS session_resources (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		resource_id TEXT NOT NULL,
		type TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		delete_requested_at TIMESTAMPTZ,
		detached_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, resource_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_resources_type_shape CHECK (type IN ('file', 'memory_store', 'github_repository'))
	)`

	createPostgreSQLSessionResourcePrefixGCTable = `CREATE TABLE IF NOT EXISTS session_resource_prefix_gc (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		prefix TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		attempt_count INTEGER NOT NULL DEFAULT 0,
		last_attempt_at TIMESTAMPTZ,
		next_attempt_at TIMESTAMPTZ,
		completed_at TIMESTAMPTZ,
		last_error_kind TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, session_id),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_resource_prefix_gc_prefix_shape CHECK (prefix <> ''),
		CONSTRAINT session_resource_prefix_gc_status_shape CHECK (status IN ('pending', 'retryable_failed', 'deleted')),
		CONSTRAINT session_resource_prefix_gc_attempt_count_nonnegative CHECK (attempt_count >= 0)
	)`

	createPostgreSQLSessionFileResourcesTable = `CREATE TABLE IF NOT EXISTS session_file_resources (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		resource_id TEXT NOT NULL,
		source_file_id TEXT NOT NULL,
		file_id TEXT NOT NULL,
		mount_path TEXT NOT NULL,
		PRIMARY KEY (workspace_id, session_id, resource_id),
		FOREIGN KEY (workspace_id, session_id, resource_id) REFERENCES session_resources(workspace_id, session_id, resource_id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, source_file_id) REFERENCES files(workspace_id, file_id) ON DELETE RESTRICT,
		FOREIGN KEY (workspace_id, file_id) REFERENCES files(workspace_id, file_id) ON DELETE RESTRICT
	)`

	createPostgreSQLSessionMemoryStoreResourcesTable = `CREATE TABLE IF NOT EXISTS session_memory_store_resources (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		resource_id TEXT NOT NULL,
		memory_store_id TEXT NOT NULL,
		access TEXT NOT NULL,
		instructions TEXT NOT NULL DEFAULT '',
		name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		mount_path TEXT NOT NULL,
		PRIMARY KEY (workspace_id, session_id, resource_id),
		FOREIGN KEY (workspace_id, session_id, resource_id) REFERENCES session_resources(workspace_id, session_id, resource_id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, memory_store_id) REFERENCES memory_stores(workspace_id, memory_store_id) ON DELETE RESTRICT,
		CONSTRAINT session_memory_store_access_shape CHECK (access IN ('read_write', 'read_only'))
	)`

	createPostgreSQLSessionGitHubRepositoryResourcesTable = `CREATE TABLE IF NOT EXISTS session_github_repository_resources (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		resource_id TEXT NOT NULL,
		url TEXT NOT NULL,
		mount_path TEXT,
		checkout_type TEXT,
		checkout_ref TEXT,
		authorization_token_encrypted BYTEA NOT NULL,
		PRIMARY KEY (workspace_id, session_id, resource_id),
		FOREIGN KEY (workspace_id, session_id, resource_id) REFERENCES session_resources(workspace_id, session_id, resource_id) ON DELETE CASCADE,
		CONSTRAINT session_github_repository_checkout_shape CHECK (
			(checkout_type IS NULL AND checkout_ref IS NULL)
			OR (checkout_type IS NOT NULL AND checkout_type IN ('branch', 'commit') AND checkout_ref IS NOT NULL AND checkout_ref <> '')
		),
		CONSTRAINT session_github_repository_authorization_token_required CHECK (
			authorization_token_encrypted IS NOT NULL
		)
	)`

	createPostgreSQLSessionGitTicketsTable = `CREATE TABLE IF NOT EXISTS session_git_tickets (
		workspace_id TEXT NOT NULL,
		session_id TEXT NOT NULL,
		ticket_id TEXT NOT NULL,
		token_hash BYTEA NOT NULL,
		status TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		rotated_at TIMESTAMPTZ,
		PRIMARY KEY (workspace_id, session_id, ticket_id),
		UNIQUE (token_hash),
		FOREIGN KEY (workspace_id, session_id) REFERENCES sessions(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT session_git_tickets_token_hash_shape CHECK (octet_length(token_hash) = 32),
		CONSTRAINT session_git_tickets_status_shape CHECK (status IN ('pending', 'live', 'rotated')),
		CONSTRAINT session_git_tickets_rotation_shape CHECK (
			(status IN ('pending', 'live') AND rotated_at IS NULL)
			OR (status = 'rotated' AND rotated_at IS NOT NULL)
		)
	)`

	createPostgreSQLAgentsTable = `CREATE TABLE IF NOT EXISTS agents (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		name TEXT NOT NULL,
		description TEXT,
		version INTEGER NOT NULL DEFAULT 1,
		archived_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (id),
		UNIQUE (workspace_id, id)
	)`

	createPostgreSQLAgentVersionsTable = `CREATE TABLE IF NOT EXISTS agent_versions (
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		agent_id TEXT NOT NULL,
		version INTEGER NOT NULL,
		config_json TEXT NOT NULL,
		config_hash TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (id),
		UNIQUE (agent_id, version),
		UNIQUE (workspace_id, agent_id, version),
		CONSTRAINT agent_versions_workspace_agent_version_id_key UNIQUE (workspace_id, agent_id, version, id),
		FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, id)
	)`

	createPostgreSQLEnvironmentsTable = `CREATE TABLE IF NOT EXISTS environments (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		config_json TEXT NOT NULL,
		current_generation BIGINT NOT NULL DEFAULT 1,
		metadata_json TEXT NOT NULL DEFAULT '{}',
		archived_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (id),
		UNIQUE (workspace_id, id),
		UNIQUE (workspace_id, name),
		CONSTRAINT environments_current_generation_shape CHECK (current_generation > 0)
	)`

	createPostgreSQLEnvironmentArtifactsTable = `CREATE TABLE IF NOT EXISTS environment_artifacts (
		workspace_id TEXT NOT NULL,
		environment_id TEXT NOT NULL,
		generation BIGINT NOT NULL,
		status TEXT NOT NULL,
		provider TEXT NOT NULL,
		provider_artifact_ref TEXT,
		provider_create_submitted_at TIMESTAMPTZ,
		lease_job_id TEXT,
		lease_token TEXT,
		lease_attempt_count BIGINT,
		normalized_config_hash TEXT NOT NULL,
		artifact_input_hash TEXT NOT NULL,
		runtime_network_policy_json TEXT NOT NULL,
		packages_json TEXT NOT NULL,
		failure_stage TEXT,
		last_error_kind TEXT,
		failure_reason TEXT,
		retryable BOOLEAN,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, environment_id, generation),
		FOREIGN KEY (workspace_id, environment_id) REFERENCES environments(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT environment_artifacts_generation_shape CHECK (generation > 0),
		CONSTRAINT environment_artifacts_status_shape CHECK (status IN ('pending', 'building', 'ready', 'failed')),
		CONSTRAINT environment_artifacts_lease_shape CHECK (
			(status = 'building' AND lease_job_id IS NOT NULL AND lease_token IS NOT NULL AND lease_attempt_count > 0)
			OR (status <> 'building' AND lease_job_id IS NULL AND lease_token IS NULL AND lease_attempt_count IS NULL)
		),
		CONSTRAINT environment_artifacts_provider_shape CHECK (provider = 'daytona')
	)`

	//nolint:gosec // Schema credential identity constraint, not a secret value.
	//nolint:gosec // Schema credential auth type constraint, not a secret value.
	//nolint:gosec // Schema credential provider constraint, not a secret value.
	createPostgreSQLSessionsAgentVersionIDFunction = `CREATE OR REPLACE FUNCTION tetral_sessions_fill_agent_version_id()
RETURNS trigger AS $$
BEGIN
	IF NEW.agent_version_id IS NULL OR NEW.agent_version_id = '' THEN
		SELECT av.id
		  INTO NEW.agent_version_id
		  FROM agent_versions av
		 WHERE av.workspace_id = NEW.workspace_id
		   AND av.agent_id = NEW.agent_id
		   AND av.version = NEW.agent_version
		 LIMIT 1;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql`
	createPostgreSQLSessionsAgentVersionIDTrigger = `DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1
		  FROM pg_trigger
		 WHERE tgname = 'sessions_fill_agent_version_id'
		   AND tgrelid = 'sessions'::regclass
	) THEN
		CREATE TRIGGER sessions_fill_agent_version_id
		BEFORE INSERT OR UPDATE OF agent_id, agent_version, agent_version_id
		ON sessions
		FOR EACH ROW
		EXECUTE FUNCTION tetral_sessions_fill_agent_version_id();
	END IF;
END $$`
	createPostgreSQLVaultsTable = `CREATE TABLE IF NOT EXISTS vaults (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		display_name TEXT NOT NULL,
		metadata_json TEXT NOT NULL DEFAULT '{}',
		archived_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (id),
		UNIQUE (workspace_id, id)
	)`

	createPostgreSQLCredentialsTable = `CREATE TABLE IF NOT EXISTS credentials (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		id TEXT NOT NULL,
		vault_id TEXT NOT NULL,
		display_name TEXT NOT NULL DEFAULT '',
		metadata_json TEXT NOT NULL DEFAULT '{}',
		auth_type TEXT NOT NULL,
		auth_public_json TEXT NOT NULL DEFAULT '{}',
		provider_id TEXT,
		access_mode TEXT,
		mcp_server_url TEXT NOT NULL DEFAULT '',
		expires_at TEXT NOT NULL DEFAULT '',
		encrypted_auth BYTEA,
		archived_at TIMESTAMPTZ,
		revoked_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (workspace_id, vault_id, id),
		FOREIGN KEY (workspace_id, vault_id) REFERENCES vaults(workspace_id, id) ON DELETE CASCADE,
		CONSTRAINT credentials_auth_type_shape CHECK (auth_type IN ('mcp_oauth', 'static_bearer', 'provider_api_key', 'provider_oauth')),
		CONSTRAINT credentials_provider_shape CHECK (
			(auth_type IN ('provider_api_key', 'provider_oauth') AND provider_id IS NOT NULL AND access_mode IS NOT NULL)
			OR (auth_type IN ('mcp_oauth', 'static_bearer'))
		)
	)`

	createPostgreSQLSkillsTable = `CREATE TABLE IF NOT EXISTS skills (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		skill_id TEXT NOT NULL,
		display_title TEXT,
		latest_version TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		deleted_at TIMESTAMPTZ,
		PRIMARY KEY (skill_id),
		UNIQUE (workspace_id, skill_id)
	)`

	createPostgreSQLSkillVersionsTable = `CREATE TABLE IF NOT EXISTS skill_versions (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		skill_id TEXT NOT NULL,
		skill_version_id TEXT NOT NULL,
		version TEXT NOT NULL,
		name TEXT NOT NULL,
		description TEXT NOT NULL,
		directory TEXT NOT NULL,
		blob_key TEXT NOT NULL,
		size_bytes BIGINT NOT NULL,
		sha256 TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		deleted_at TIMESTAMPTZ,
		PRIMARY KEY (skill_version_id),
		UNIQUE (workspace_id, skill_id, version),
		UNIQUE (workspace_id, skill_version_id),
		UNIQUE (blob_key),
		FOREIGN KEY (workspace_id, skill_id) REFERENCES skills(workspace_id, skill_id)
	)`

	// file_objects stores the unique committed bytes that back one or
	// more public Files identities. blob_key is server-generated and
	// globally unique so object storage writes cannot collide across
	// workspaces. The workspace/object composite uniqueness supports a
	// workspace-aligned FK from files below.
	createPostgreSQLFileObjectsTable = `CREATE TABLE IF NOT EXISTS file_objects (
		object_id TEXT NOT NULL,
		workspace_id TEXT NOT NULL,
		blob_key TEXT NOT NULL,
		size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
		sha256 TEXT NOT NULL,
		pdf_page_count BIGINT,
		created_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (object_id),
		UNIQUE (workspace_id, object_id),
		UNIQUE (blob_key),
		CONSTRAINT file_objects_pdf_page_count_shape CHECK (
			pdf_page_count IS NULL OR pdf_page_count >= -1
		)
	)`

	// files is the API-visible Files identity. Multiple files rows may
	// point at the same file_objects row so future session-scoped
	// logical copies can share the same stored bytes. The composite FK
	// stops a row in one workspace from referencing bytes owned by
	// another workspace even if application code forgets its explicit
	// workspace predicate.
	createPostgreSQLFilesTable = `CREATE TABLE IF NOT EXISTS files (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		file_id TEXT NOT NULL,
		workspace_id TEXT NOT NULL,
		object_id TEXT NOT NULL,
		filename TEXT NOT NULL,
		mime_type TEXT NOT NULL,
		downloadable BOOLEAN NOT NULL,
		scope_type TEXT,
		scope_id TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		deleted_at TIMESTAMPTZ,
		PRIMARY KEY (file_id),
		UNIQUE (workspace_id, file_id),
		FOREIGN KEY (workspace_id, object_id) REFERENCES file_objects(workspace_id, object_id),
		CONSTRAINT files_scope_shape CHECK (
			(scope_type IS NULL AND scope_id IS NULL)
			OR (scope_type = 'session' AND scope_id IS NOT NULL)
		)
	)`

	createPostgreSQLMemoryStoresTable = `CREATE TABLE IF NOT EXISTS memory_stores (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		memory_store_id TEXT NOT NULL,
		name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		metadata_json TEXT NOT NULL DEFAULT '{}',
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		archived_at TIMESTAMPTZ,
		PRIMARY KEY (memory_store_id),
		UNIQUE (workspace_id, memory_store_id)
	)`

	createPostgreSQLMemoriesTable = `CREATE TABLE IF NOT EXISTS memories (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		memory_store_id TEXT NOT NULL,
		memory_id TEXT NOT NULL,
		current_version_id TEXT NOT NULL,
		path TEXT NOT NULL,
		content_sha256 TEXT,
		content_size_bytes BIGINT,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		deleted_at TIMESTAMPTZ,
		PRIMARY KEY (memory_id),
		UNIQUE (workspace_id, memory_store_id, memory_id),
		FOREIGN KEY (workspace_id, memory_store_id) REFERENCES memory_stores(workspace_id, memory_store_id) ON DELETE CASCADE,
		CONSTRAINT memories_active_cache_shape CHECK (
			(
				deleted_at IS NULL
				AND content_sha256 IS NOT NULL
				AND content_size_bytes IS NOT NULL
				AND content_size_bytes >= 0
			)
			OR (
				deleted_at IS NOT NULL
				AND content_sha256 IS NULL
				AND content_size_bytes IS NULL
			)
		)
	)`

	createPostgreSQLMemoryVersionsTable = `CREATE TABLE IF NOT EXISTS memory_versions (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		workspace_id TEXT NOT NULL,
		memory_store_id TEXT NOT NULL,
		memory_id TEXT NOT NULL,
		memory_version_id TEXT NOT NULL,
		operation TEXT NOT NULL,
		path TEXT,
		content TEXT,
		content_sha256 TEXT,
		content_size_bytes BIGINT,
		created_at TIMESTAMPTZ NOT NULL,
		created_actor_type TEXT NOT NULL,
		created_api_key_id TEXT,
		created_session_id TEXT,
		created_user_id TEXT,
		redacted_at TIMESTAMPTZ,
		redacted_actor_type TEXT,
		redacted_api_key_id TEXT,
		redacted_session_id TEXT,
		redacted_user_id TEXT,
		PRIMARY KEY (memory_version_id),
		UNIQUE (workspace_id, memory_store_id, memory_id, memory_version_id),
		FOREIGN KEY (workspace_id, memory_store_id, memory_id) REFERENCES memories(workspace_id, memory_store_id, memory_id) ON DELETE CASCADE,
		FOREIGN KEY (workspace_id, created_api_key_id) REFERENCES api_keys(workspace_id, id),
		FOREIGN KEY (workspace_id, redacted_api_key_id) REFERENCES api_keys(workspace_id, id),
		CONSTRAINT memory_versions_operation_shape CHECK (operation IN ('created', 'modified', 'deleted')),
		CONSTRAINT memory_versions_created_actor_shape CHECK (
			(created_actor_type = 'api_actor' AND created_api_key_id IS NOT NULL AND created_session_id IS NULL AND created_user_id IS NULL)
			OR (created_actor_type = 'session_actor' AND created_api_key_id IS NULL AND created_session_id IS NOT NULL AND created_user_id IS NULL)
			OR (created_actor_type = 'user_actor' AND created_api_key_id IS NULL AND created_session_id IS NULL AND created_user_id IS NOT NULL)
		),
		CONSTRAINT memory_versions_redacted_actor_shape CHECK (
			(redacted_at IS NULL AND redacted_actor_type IS NULL AND redacted_api_key_id IS NULL AND redacted_session_id IS NULL AND redacted_user_id IS NULL)
			OR (redacted_at IS NOT NULL AND redacted_actor_type = 'api_actor' AND redacted_api_key_id IS NOT NULL AND redacted_session_id IS NULL AND redacted_user_id IS NULL)
			OR (redacted_at IS NOT NULL AND redacted_actor_type = 'session_actor' AND redacted_api_key_id IS NULL AND redacted_session_id IS NOT NULL AND redacted_user_id IS NULL)
			OR (redacted_at IS NOT NULL AND redacted_actor_type = 'user_actor' AND redacted_api_key_id IS NULL AND redacted_session_id IS NULL AND redacted_user_id IS NOT NULL)
		),
		CONSTRAINT memory_versions_payload_shape CHECK (
			(
				redacted_at IS NOT NULL
				AND path IS NULL
				AND content IS NULL
				AND content_sha256 IS NULL
				AND content_size_bytes IS NULL
			)
			OR (
				redacted_at IS NULL
				AND path IS NOT NULL
				AND (
					(operation = 'deleted' AND content IS NULL AND content_sha256 IS NULL AND content_size_bytes IS NULL)
					OR (
						operation IN ('created', 'modified')
						AND content IS NOT NULL
						AND content_sha256 IS NOT NULL
						AND content_size_bytes IS NOT NULL
						AND content_size_bytes >= 0
					)
				)
			)
		)
	)`

	// workspaces is the system/catalog table that lists every isolation
	// scope. This stage exposes no public workspace lifecycle API; rows
	// are supplied by deployment/bootstrap data and never chosen by the
	// schema initializer.
	createPostgreSQLWorkspacesTable = `CREATE TABLE IF NOT EXISTS workspaces (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL DEFAULT 'workspace',
		name TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL
	)`

	// api_keys stores workspace-scoped bearer credentials. The full
	// raw key is never persisted; only a deterministic non-recoverable
	// digest plus a non-secret prefix and metadata are stored.
	//
	// `key_kind` distinguishes the single bootstrap row per workspace
	// (refreshed by startup from ENGINE_API_KEY) from the standard rows
	// created by POST /v1/api_keys. A partial unique index on
	// (workspace_id) WHERE key_kind = 'bootstrap' enforces the
	// "exactly one bootstrap key per workspace" invariant without
	// blocking multiple standard keys.
	createPostgreSQLApiKeysTable = `CREATE TABLE IF NOT EXISTS api_keys (
		storage_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
		id TEXT NOT NULL,
		type TEXT NOT NULL DEFAULT 'api_key',
		workspace_id TEXT NOT NULL REFERENCES workspaces(id),
		name TEXT NOT NULL,
		key_prefix TEXT NOT NULL,
		key_digest BYTEA NOT NULL UNIQUE,
		key_kind TEXT NOT NULL CHECK (key_kind IN ('bootstrap','standard')),
		created_at TIMESTAMPTZ NOT NULL,
		last_used_at TIMESTAMPTZ,
		revoked_at TIMESTAMPTZ,
		PRIMARY KEY (id),
		UNIQUE (workspace_id, id)
	)`

	// Phase 2 query-path indexes. Each one supports the
	// rowid-replacement list path or the cascade lookup. CREATE INDEX
	// IF NOT EXISTS keeps idempotence; the index set is intentionally
	// minimal and behavior-aligned, not speculative.
	createPostgreSQLSessionsWorkspaceSeqIndex               = `CREATE INDEX IF NOT EXISTS idx_sessions_workspace_seq ON sessions(workspace_id, storage_sequence)`
	createPostgreSQLSessionsWorkspaceCreatedIndex           = `CREATE INDEX IF NOT EXISTS idx_sessions_workspace_created_id ON sessions(workspace_id, created_at, id)`
	createPostgreSQLSessionsWorkspaceAgentIndex             = `CREATE INDEX IF NOT EXISTS idx_sessions_workspace_agent ON sessions(workspace_id, agent_id, agent_version)`
	createPostgreSQLSessionsWorkspaceEnvIndex               = `CREATE INDEX IF NOT EXISTS idx_sessions_workspace_environment ON sessions(workspace_id, environment_id)`
	createPostgreSQLSessionThreadsPrimaryIndex              = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_threads_main_unique ON session_threads(workspace_id, session_id) WHERE role = 'main'`
	createPostgreSQLSessionThreadsParentIndex               = `CREATE INDEX IF NOT EXISTS idx_session_threads_parent ON session_threads(workspace_id, session_id, parent_thread_id)`
	createPostgreSQLSessionThreadsSubAgentTaskIndex         = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_threads_subagent_task_unique ON session_threads(workspace_id, session_id, parent_thread_id, task_name) WHERE role = 'subagent'`
	createPostgreSQLSessionThreadsReviewerTrunkIndex        = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_threads_reviewer_trunk_unique ON session_threads(workspace_id, session_id, parent_thread_id) WHERE role = 'approval_reviewer' AND is_trunk`
	createPostgreSQLSandboxBindingsProviderResourceIndex    = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_sandbox_bindings_provider_resource_unique ON session_sandbox_bindings(provider, provider_resource_id) WHERE provider_resource_id IS NOT NULL`
	createPostgreSQLSandboxActivationUnfinishedIndex        = `CREATE UNIQUE INDEX IF NOT EXISTS idx_sandbox_lifecycle_activation_unfinished ON sandbox_lifecycle_operations(workspace_id, logical_sandbox_id) WHERE kind IN ('create', 'start', 'replace') AND state IN ('pending', 'waiting_artifact', 'running')`
	createPostgreSQLSandboxMaterializationUnfinishedIndex   = `CREATE UNIQUE INDEX IF NOT EXISTS idx_sandbox_lifecycle_materialization_unfinished ON sandbox_lifecycle_operations(workspace_id, logical_sandbox_id) WHERE kind = 'materialize' AND state IN ('pending', 'waiting_activation', 'running')`
	createPostgreSQLSessionEventsSessionSequenceIndex       = `CREATE INDEX IF NOT EXISTS idx_session_events_session_sequence ON session_events(workspace_id, session_id, sequence)`
	createPostgreSQLSessionEventsInsertStreamPositionIndex  = `CREATE INDEX IF NOT EXISTS idx_session_events_insert_stream_position ON session_events(workspace_id, session_id, insert_stream_position)`
	createPostgreSQLSessionEventsPendingClientIndex         = `CREATE INDEX IF NOT EXISTS idx_session_events_pending_client ON session_events(workspace_id, session_id, sequence) WHERE processed_at IS NULL`
	createPostgreSQLSessionEventsThreadSequenceIndex        = `CREATE INDEX IF NOT EXISTS idx_session_events_thread_sequence ON session_events(workspace_id, session_id, session_thread_id, sequence)`
	createPostgreSQLSessionEventsThreadTypeSequenceIndex    = `CREATE INDEX IF NOT EXISTS idx_session_events_thread_type_sequence ON session_events(workspace_id, session_id, session_thread_id, type, sequence)`
	createPostgreSQLSessionEventsThreadRequestTypeIndex     = `CREATE INDEX IF NOT EXISTS idx_session_events_thread_request_type ON session_events(workspace_id, session_id, session_thread_id, model_request_id, type, sequence) WHERE model_request_id IS NOT NULL`
	createPostgreSQLSessionEventsThreadRunningIndex         = `CREATE INDEX IF NOT EXISTS idx_session_events_thread_running_sequence ON session_events(workspace_id, session_id, session_thread_id, sequence) WHERE type IN ('session.status_running', 'session.thread_status_running')`
	createPostgreSQLSessionEventsThreadCloseIndex           = `CREATE INDEX IF NOT EXISTS idx_session_events_thread_close_sequence ON session_events(workspace_id, session_id, session_thread_id, sequence) WHERE type IN ('session.status_idle', 'session.thread_status_idle', 'session.status_terminated', 'session.thread_status_terminated')`
	createPostgreSQLSessionEventStreamChangesIndex          = `CREATE INDEX IF NOT EXISTS idx_session_event_stream_changes_session ON session_event_stream_changes(workspace_id, session_id, stream_position)`
	createPostgreSQLSessionMessagesKindSeqIndex             = `CREATE INDEX IF NOT EXISTS idx_session_messages_kind_seq ON session_messages(workspace_id, session_id, session_thread_id, kind, sequence)`
	createPostgreSQLSessionMessagesSeqIndex                 = `CREATE INDEX IF NOT EXISTS idx_session_messages_seq ON session_messages(workspace_id, session_id, session_thread_id, sequence)`
	createPostgreSQLSessionMessagesSourceEventIndex         = `CREATE INDEX IF NOT EXISTS idx_session_messages_source_event ON session_messages(workspace_id, source_event_id)`
	createPostgreSQLSessionMessagesSourceEventUniqueIndex   = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_messages_source_event_unique ON session_messages(workspace_id, session_id, session_thread_id, source_event_id) WHERE source_event_id IS NOT NULL`
	createPostgreSQLSessionMessagesRepairKeyIndex           = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_messages_repair_key_unique ON session_messages(workspace_id, session_id, session_thread_id, repair_key) WHERE repair_key IS NOT NULL`
	createPostgreSQLSessionMessagesModelRequestIndex        = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_messages_model_request_unique ON session_messages(workspace_id, session_id, session_thread_id, model_request_id) WHERE model_request_id IS NOT NULL`
	createPostgreSQLSessionEventsPendingMediaIndex          = `CREATE INDEX IF NOT EXISTS session_events_pending_media_lookup ON session_events(workspace_id, session_id, session_thread_id, sequence, event_id) WHERE type = 'user.message' AND payload_json::jsonb @? '$.content[*] ? (@.type == "image" || @.type == "document")'`
	createPostgreSQLSessionFileAttachmentPendingIndex       = `CREATE INDEX IF NOT EXISTS session_file_attachment_consumptions_pending_lookup ON session_file_attachment_consumptions(workspace_id, session_id, session_thread_id, source_event_id, file_id)`
	createPostgreSQLSessionRuntimeInboxAttachmentIndex      = `CREATE INDEX IF NOT EXISTS session_runtime_inbox_attachment_authority_lookup ON session_runtime_inbox(workspace_id, session_id, session_thread_id, runtime_input_id) INCLUDE (input_kind, status)`
	createPostgreSQLSessionEventsAgentMailDeliveryIndex     = `CREATE INDEX IF NOT EXISTS idx_session_events_agent_mail_delivery ON session_events(workspace_id, session_id, ((payload_json::jsonb ->> 'delivery_id'))) WHERE type IN ('agent.thread_message_sent', 'agent.thread_message_received')`
	createPostgreSQLPendingToolUsesStatusIndex              = `CREATE INDEX IF NOT EXISTS idx_session_pending_tool_uses_status ON session_pending_tool_uses(workspace_id, session_id, session_thread_id, status)`
	createPostgreSQLBackgroundTasksStatusIndex              = `CREATE INDEX IF NOT EXISTS idx_session_background_tasks_status ON session_background_tasks(workspace_id, session_id, status, updated_at)`
	createPostgreSQLSessionMCPManifestsGenerationIndex      = `CREATE INDEX IF NOT EXISTS idx_session_mcp_manifests_session_generation ON session_mcp_manifests(workspace_id, session_id, manifest_generation)`
	createPostgreSQLRuntimeStatusCleanupDueIndex            = `CREATE INDEX IF NOT EXISTS idx_session_runtime_status_cleanup_due ON session_runtime_status(workspace_id, cleanup_after, cleanup_job_id) WHERE status = 'idle' AND binding_id IS NOT NULL`
	createPostgreSQLBridgeOperationsRuntimeWriteIndex       = `CREATE INDEX IF NOT EXISTS idx_session_bridge_operations_runtime_write ON session_bridge_operations(workspace_id, session_id, runtime_write_id) WHERE runtime_write_id IS NOT NULL`
	createPostgreSQLRuntimeToolResultsKindIndex             = `CREATE INDEX IF NOT EXISTS idx_session_runtime_tool_results_kind ON session_runtime_tool_results(workspace_id, session_id, tool_kind, updated_at)`
	createPostgreSQLSessionResourcePrefixGCDueIndex         = `CREATE INDEX IF NOT EXISTS idx_session_resource_prefix_gc_due ON session_resource_prefix_gc(workspace_id, next_attempt_at, created_at) WHERE status IN ('pending', 'retryable_failed')`
	createPostgreSQLSessionOutputCapturesIndex              = `CREATE INDEX IF NOT EXISTS idx_session_output_captures_session ON session_output_captures(workspace_id, session_id, updated_at)`
	createPostgreSQLSandboxOutputCaptureOpenIndex           = `CREATE UNIQUE INDEX IF NOT EXISTS idx_sandbox_output_capture_open ON sandbox_output_capture_operations(workspace_id, session_id, finish_idle_write_id) WHERE state IN ('pending', 'running', 'staged', 'skipped_unavailable')`
	createPostgreSQLSandboxOutputCaptureExpiryIndex         = `CREATE INDEX IF NOT EXISTS idx_sandbox_output_capture_expiry ON sandbox_output_capture_operations(workspace_id, retain_until) WHERE state IN ('staged', 'skipped_unavailable', 'failed')`
	createPostgreSQLRequestUsageDetailsThreadIndex          = `CREATE INDEX IF NOT EXISTS idx_request_usage_details_thread ON request_usage_details(workspace_id, session_id, session_thread_id, created_at)`
	createPostgreSQLSessionProviderAuthCredentialIndex      = `CREATE INDEX IF NOT EXISTS idx_session_provider_auth_credential ON session_provider_auth(workspace_id, vault_id, credential_id) WHERE deleted_at IS NULL` //nolint:gosec // Credential id index name, not a secret value.
	createPostgreSQLSessionProviderAuthActiveSessionIndex   = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_provider_auth_active_session ON session_provider_auth(workspace_id, session_id) WHERE deleted_at IS NULL`
	createPostgreSQLPlatformProviderKeysProviderStatusIndex = `CREATE INDEX IF NOT EXISTS idx_platform_provider_keys_provider_status ON platform_provider_keys(provider_id, status, priority, key_id)` //nolint:gosec // Provider key-pool index name, not a secret value.
	createPostgreSQLQueueJobsActiveDedupeIndex              = `CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_jobs_active_dedupe ON queue_jobs(workspace_id, dedupe_key) WHERE status IN ('pending', 'leased')`
	createPostgreSQLQueueJobsLeasedPartitionIndex           = `CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_jobs_leased_partition ON queue_jobs(workspace_id, partition_key) WHERE status = 'leased' AND delivery_scope = 'partition'`
	createPostgreSQLQueueJobsLeasedThreadIndex              = `CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_jobs_leased_thread ON queue_jobs(workspace_id, causal_session_id, delivery_thread_id) WHERE status = 'leased' AND delivery_scope = 'thread'`
	createPostgreSQLQueueJobsLeasedSessionIndex             = `CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_jobs_leased_session ON queue_jobs(workspace_id, causal_session_id) WHERE status = 'leased' AND delivery_scope = 'session'`
	createPostgreSQLQueueJobsPartitionSequenceIndex         = `CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_jobs_partition_sequence ON queue_jobs(workspace_id, partition_key, queue_partition_sequence)`
	createPostgreSQLQueueJobsAvailableIndex                 = `CREATE INDEX IF NOT EXISTS idx_queue_jobs_available ON queue_jobs(workspace_id, kind, status, causal_session_id, delivery_scope, delivery_thread_id, control_class, priority DESC, available_at, queue_partition_sequence) WHERE status = 'pending'`
	createPostgreSQLQueueJobsSandboxTerminalRetentionIndex  = `CREATE INDEX IF NOT EXISTS idx_queue_jobs_sandbox_terminal_retention ON queue_jobs(COALESCE(acknowledged_at, cancelled_at, dead_lettered_at), id) WHERE kind IN ('sandbox_tool_execute', 'sandbox_activate', 'sandbox_materialize', 'sandbox_release', 'sandbox_tool_cancel', 'sandbox_output_capture', 'sandbox_output_capture_cleanup', 'sandbox_memory_projection', 'sandbox_background_command', 'sandbox_background_reconcile') AND status IN ('acknowledged', 'cancelled', 'dead_lettered')`
	createPostgreSQLQueueJobsSandboxSessionCleanupIndex     = `CREATE INDEX IF NOT EXISTS idx_queue_jobs_sandbox_session_cleanup ON queue_jobs(workspace_id, (payload_json::jsonb ->> 'session_id'), status) WHERE kind IN ('sandbox_tool_execute', 'sandbox_activate', 'sandbox_materialize', 'sandbox_release', 'sandbox_tool_cancel', 'sandbox_output_capture', 'sandbox_output_capture_cleanup', 'sandbox_memory_projection', 'sandbox_background_command', 'sandbox_background_reconcile')`
	createPostgreSQLSessionResourcesSessionSeqIndex         = `CREATE INDEX IF NOT EXISTS idx_session_resources_session_seq ON session_resources(workspace_id, session_id, storage_sequence)`
	createPostgreSQLSessionResourcesTypeIndex               = `CREATE INDEX IF NOT EXISTS idx_session_resources_type ON session_resources(workspace_id, session_id, type, detached_at)`
	createPostgreSQLSessionGitTicketsLiveIndex              = `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_git_tickets_live ON session_git_tickets(workspace_id, session_id) WHERE status = 'live'`
	createPostgreSQLSessionMemoryStoreFilterIndex           = `CREATE INDEX IF NOT EXISTS idx_session_memory_store_resources_store ON session_memory_store_resources(workspace_id, memory_store_id, session_id)`
	createPostgreSQLAgentsWorkspaceSeqIndex                 = `CREATE INDEX IF NOT EXISTS idx_agents_workspace_seq ON agents(workspace_id, storage_sequence)`
	createPostgreSQLEnvironmentsWorkspaceSeqIndex           = `CREATE INDEX IF NOT EXISTS idx_environments_workspace_seq ON environments(workspace_id, storage_sequence)`
	createPostgreSQLEnvironmentArtifactsStatusIndex         = `CREATE INDEX IF NOT EXISTS idx_environment_artifacts_status ON environment_artifacts(workspace_id, status, updated_at)`
	createPostgreSQLVaultsWorkspaceSeqIndex                 = `CREATE INDEX IF NOT EXISTS idx_vaults_workspace_seq ON vaults(workspace_id, storage_sequence)`
	createPostgreSQLCredentialsVaultSeqIndex                = `CREATE INDEX IF NOT EXISTS idx_credentials_vault_seq ON credentials(workspace_id, vault_id, storage_sequence)` //nolint:gosec // G101: schema DDL, not a secret
	createPostgreSQLApiKeysWorkspaceSeqIndex                = `CREATE INDEX IF NOT EXISTS idx_api_keys_workspace_seq ON api_keys(workspace_id, storage_sequence)`             //nolint:gosec // G101: schema DDL, not a secret
	createPostgreSQLCredentialsActiveMCPURLIndex            = `CREATE UNIQUE INDEX IF NOT EXISTS idx_credentials_active_mcp_url_unique ON credentials(workspace_id, vault_id, mcp_server_url) WHERE archived_at IS NULL AND mcp_server_url <> '' AND auth_type IN ('mcp_oauth', 'static_bearer')`

	// Partial unique index: at most one bootstrap row per workspace.
	// Standard rows are unconstrained, so a workspace can have many
	// active API keys created through POST /v1/api_keys.
	createPostgreSQLApiKeysBootstrapUniqueIndex = `CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_bootstrap_unique ON api_keys(workspace_id) WHERE key_kind = 'bootstrap'` //nolint:gosec // G101: schema DDL, not a secret

	//nolint:gosec // G101: schema DDL for api_keys table, not a secret
	deferredPostgreSQLMemoriesCurrentVersionFK = `DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_constraint
		 WHERE conname = 'memories_current_version_fk'
		   AND conrelid = 'memories'::regclass
	) THEN
		ALTER TABLE memories
			ADD CONSTRAINT memories_current_version_fk
			FOREIGN KEY (workspace_id, memory_store_id, memory_id, current_version_id)
			REFERENCES memory_versions(workspace_id, memory_store_id, memory_id, memory_version_id)
			DEFERRABLE INITIALLY DEFERRED;
	END IF;
END $$`

	// Skill list pagination: ordered by storage_sequence within a
	// workspace mirrors the agents/sessions/api_keys list path.
	createPostgreSQLSkillsWorkspaceSeqIndex      = `CREATE INDEX IF NOT EXISTS idx_skills_workspace_seq ON skills(workspace_id, storage_sequence)`
	createPostgreSQLSkillVersionsSkillSeqIndex   = `CREATE INDEX IF NOT EXISTS idx_skill_versions_skill_seq ON skill_versions(workspace_id, skill_id, storage_sequence)`
	createPostgreSQLFilesWorkspaceSeqIndex       = `CREATE INDEX IF NOT EXISTS idx_files_workspace_seq ON files(workspace_id, storage_sequence)`
	createPostgreSQLFilesWorkspaceScopeSeqIndex  = `CREATE INDEX IF NOT EXISTS idx_files_workspace_scope_seq ON files(workspace_id, scope_id, storage_sequence)`
	createPostgreSQLMemoryStoresCreatedIndex     = `CREATE INDEX IF NOT EXISTS idx_memory_stores_workspace_created_id ON memory_stores(workspace_id, created_at, memory_store_id)`
	createPostgreSQLMemoryStoresSeqIndex         = `CREATE INDEX IF NOT EXISTS idx_memory_stores_workspace_seq ON memory_stores(workspace_id, storage_sequence)`
	createPostgreSQLMemoriesActivePathIndex      = `CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_active_path ON memories(workspace_id, memory_store_id, path) WHERE deleted_at IS NULL`
	createPostgreSQLMemoriesCreatedIndex         = `CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(workspace_id, memory_store_id, created_at, memory_id)`
	createPostgreSQLMemoriesUpdatedIndex         = `CREATE INDEX IF NOT EXISTS idx_memories_updated ON memories(workspace_id, memory_store_id, updated_at, memory_id)`
	createPostgreSQLMemoryVersionsCreatedIndex   = `CREATE INDEX IF NOT EXISTS idx_memory_versions_created ON memory_versions(workspace_id, memory_store_id, created_at, memory_version_id)`
	createPostgreSQLMemoryVersionsMemoryIndex    = `CREATE INDEX IF NOT EXISTS idx_memory_versions_memory ON memory_versions(workspace_id, memory_store_id, memory_id, created_at, memory_version_id)`
	createPostgreSQLMemoryVersionsOperationIndex = `CREATE INDEX IF NOT EXISTS idx_memory_versions_operation ON memory_versions(workspace_id, memory_store_id, operation, created_at, memory_version_id)`
	createPostgreSQLMemoryVersionsAPIKeyIndex    = `CREATE INDEX IF NOT EXISTS idx_memory_versions_api_key ON memory_versions(workspace_id, memory_store_id, created_api_key_id, created_at, memory_version_id)` //nolint:gosec // G101: schema DDL for api-key actor index, not a secret

)

// postgresqlContract enumerates the workspace-owned tables on which Engine's
// runtime traffic must be subject to RLS. Each table is enabled with
// FORCE ROW LEVEL SECURITY so even the table owner is subject to RLS
// in normal connections; superuser/BYPASSRLS roles still bypass it,
// which is why VerifyRuntimeRole rejects those roles before serving.
var postgresqlContract = mustLoadPostgreSQLContract()

func mustLoadPostgreSQLContract() database.PostgreSQL {
	contract, err := database.LoadPostgreSQL()
	if err != nil {
		panic(err)
	}
	return contract
}

// PostgreSQLSchemaError is the storage-local origin error for the
// PostgreSQL schema initialization path. Error() text is public-safe:
// it names the failed step but does not embed pgx error text, raw SQL,
// or connection details. Storage-local conventions keep raw driver
// state out of test/loggable strings.
type PostgreSQLSchemaError struct {
	Stage string
	cause error
}

func (e *PostgreSQLSchemaError) Error() string {
	return fmt.Sprintf("postgresql: schema initialization step %q failed", e.Stage)
}

type postgresqlSchemaStep struct {
	name string
	ddl  string
}

type postgresqlSchemaExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func executePostgreSQLSchemaSteps(ctx context.Context, executor postgresqlSchemaExecutor, steps []postgresqlSchemaStep) error {
	for _, step := range steps {
		if _, err := executor.ExecContext(ctx, step.ddl); err != nil {
			return &PostgreSQLSchemaError{Stage: step.name, cause: err}
		}
	}
	return nil
}

// postgresqlBaselineSteps is the single ordered payload owned by migration
// version 1. Before the first release,
// schema edits replace this clean baseline and its checksum together. After
// that release, later schema changes belong in new migration versions.
func postgresqlBaselineSteps() []postgresqlSchemaStep {
	steps := []postgresqlSchemaStep{
		// Tables. Order follows foreign-key ownership: workspaces before
		// api_keys, agents before agent_versions, environments and agent_versions
		// before sessions, files/memory stores before Session resource details,
		// and vaults before credentials.
		{"create_workspaces", createPostgreSQLWorkspacesTable},
		{"create_environments", createPostgreSQLEnvironmentsTable},
		{"create_environment_artifacts", createPostgreSQLEnvironmentArtifactsTable},
		{"create_agents", createPostgreSQLAgentsTable},
		{"create_agent_versions", createPostgreSQLAgentVersionsTable},
		{"create_vaults", createPostgreSQLVaultsTable},
		{"create_credentials", createPostgreSQLCredentialsTable},
		{"create_api_keys", createPostgreSQLApiKeysTable},
		{"create_skills", createPostgreSQLSkillsTable},
		{"create_skill_versions", createPostgreSQLSkillVersionsTable},
		{"create_file_objects", createPostgreSQLFileObjectsTable},
		{"create_files", createPostgreSQLFilesTable},
		{"create_memory_stores", createPostgreSQLMemoryStoresTable},
		{"create_sessions", createPostgreSQLSessionsTable},
		{"create_session_threads", createPostgreSQLSessionThreadsTable},
		{"create_session_sandbox_bindings", createPostgreSQLSessionSandboxBindingsTable},
		{"create_sandbox_lifecycle_operations", createPostgreSQLSandboxLifecycleOperationsTable},
		{"create_session_events", createPostgreSQLSessionEventsTable},
		{"create_session_event_stream_changes", createPostgreSQLSessionEventStreamChangesTable},
		{"create_session_event_idempotency_keys", createPostgreSQLSessionEventIdempotencyKeysTable},
		{"create_session_messages", createPostgreSQLSessionMessagesTable},
		{"create_session_file_attachment_consumptions", createPostgreSQLSessionFileAttachmentConsumptionsTable},
		{"create_session_thread_context_prefixes", createPostgreSQLSessionThreadContextPrefixesTable},
		{"create_session_turn_retries", createPostgreSQLSessionTurnRetriesTable},
		{"create_session_pending_tool_uses", createPostgreSQLSessionPendingToolUsesTable},
		{"create_session_background_tasks", createPostgreSQLSessionBackgroundTasksTable},
		{"create_session_runtime_inbox", createPostgreSQLSessionRuntimeInboxTable},
		{"create_session_runtime_binding_generation_sequence", createPostgreSQLSessionRuntimeBindingGenerationSequence},
		{"create_session_runtime_bindings", createPostgreSQLSessionRuntimeBindingsTable},
		{"create_session_mcp_manifests", createPostgreSQLSessionMCPManifestsTable},
		{"create_session_runtime_status", createPostgreSQLSessionRuntimeStatusTable},
		{"create_session_bridge_operations", createPostgreSQLSessionBridgeOperationsTable},
		{"create_session_runtime_tool_results", createPostgreSQLSessionRuntimeToolResultsTable},
		{"create_session_output_captures", createPostgreSQLSessionOutputCapturesTable},
		{"create_sandbox_output_capture_operations", createPostgreSQLSandboxOutputCaptureOperationsTable},
		{"create_sandbox_output_capture_blobs", createPostgreSQLSandboxOutputCaptureBlobsTable},
		{"create_session_transient_attachments", createPostgreSQLSessionTransientAttachmentsTable},
		{"create_session_resources", createPostgreSQLSessionResourcesTable},
		{"create_session_resource_prefix_gc", createPostgreSQLSessionResourcePrefixGCTable},
		{"create_session_file_resources", createPostgreSQLSessionFileResourcesTable},
		{"create_session_memory_store_resources", createPostgreSQLSessionMemoryStoreResourcesTable},
		{"create_session_github_repository_resources", createPostgreSQLSessionGitHubRepositoryResourcesTable},
		{"create_session_git_tickets", createPostgreSQLSessionGitTicketsTable},
		{"create_memories", createPostgreSQLMemoriesTable},
		{"create_memory_versions", createPostgreSQLMemoryVersionsTable},
		{"constraint_memories_current_version_fk", deferredPostgreSQLMemoriesCurrentVersionFK},
		{"create_request_usage_details", createPostgreSQLRequestUsageDetailsTable},
		{"create_session_provider_auth", createPostgreSQLSessionProviderAuthTable},
		{"create_platform_provider_keys", createPostgreSQLPlatformProviderKeysTable},
		{"create_queue_partition_counters", createPostgreSQLQueuePartitionCountersTable},
		{"create_queue_jobs", createPostgreSQLQueueJobsTable},

		// Current-state trigger that fills the immutable Agent version reference
		// for Session rows created through the public API.
		{"create_sessions_agent_version_id_function", createPostgreSQLSessionsAgentVersionIDFunction},
		{"create_sessions_agent_version_id_trigger", createPostgreSQLSessionsAgentVersionIDTrigger},

		// Indexes.
		{"index_sessions_workspace_seq", createPostgreSQLSessionsWorkspaceSeqIndex},
		{"index_sessions_workspace_created_id", createPostgreSQLSessionsWorkspaceCreatedIndex},
		{"index_sessions_workspace_agent", createPostgreSQLSessionsWorkspaceAgentIndex},
		{"index_sessions_workspace_environment", createPostgreSQLSessionsWorkspaceEnvIndex},
		{"index_session_threads_primary_unique", createPostgreSQLSessionThreadsPrimaryIndex},
		{"index_session_threads_parent", createPostgreSQLSessionThreadsParentIndex},
		{"index_session_threads_subagent_task_unique", createPostgreSQLSessionThreadsSubAgentTaskIndex},
		{"index_session_threads_reviewer_trunk_unique", createPostgreSQLSessionThreadsReviewerTrunkIndex},
		{"index_session_sandbox_bindings_provider_resource_unique", createPostgreSQLSandboxBindingsProviderResourceIndex},
		{"index_sandbox_lifecycle_activation_unfinished", createPostgreSQLSandboxActivationUnfinishedIndex},
		{"index_sandbox_lifecycle_materialization_unfinished", createPostgreSQLSandboxMaterializationUnfinishedIndex},
		{"index_session_events_session_sequence", createPostgreSQLSessionEventsSessionSequenceIndex},
		{"index_session_events_insert_stream_position", createPostgreSQLSessionEventsInsertStreamPositionIndex},
		{"index_session_events_pending_client", createPostgreSQLSessionEventsPendingClientIndex},
		{"index_session_events_thread_sequence", createPostgreSQLSessionEventsThreadSequenceIndex},
		{"index_session_events_thread_type_sequence", createPostgreSQLSessionEventsThreadTypeSequenceIndex},
		{"index_session_events_thread_request_type", createPostgreSQLSessionEventsThreadRequestTypeIndex},
		{"index_session_events_thread_running_sequence", createPostgreSQLSessionEventsThreadRunningIndex},
		{"index_session_events_thread_close_sequence", createPostgreSQLSessionEventsThreadCloseIndex},
		{"index_session_event_stream_changes_session", createPostgreSQLSessionEventStreamChangesIndex},
		{"index_session_messages_kind_seq", createPostgreSQLSessionMessagesKindSeqIndex},
		{"index_session_messages_seq", createPostgreSQLSessionMessagesSeqIndex},
		{"index_session_messages_source_event", createPostgreSQLSessionMessagesSourceEventIndex},
		{"index_session_messages_source_event_unique", createPostgreSQLSessionMessagesSourceEventUniqueIndex},
		{"index_session_messages_repair_key", createPostgreSQLSessionMessagesRepairKeyIndex},
		{"index_session_messages_model_request_unique", createPostgreSQLSessionMessagesModelRequestIndex},
		{"index_session_events_pending_media", createPostgreSQLSessionEventsPendingMediaIndex},
		{"index_session_file_attachment_consumptions_pending", createPostgreSQLSessionFileAttachmentPendingIndex},
		{"index_session_runtime_inbox_attachment_authority", createPostgreSQLSessionRuntimeInboxAttachmentIndex},
		{"index_session_events_agent_mail_delivery", createPostgreSQLSessionEventsAgentMailDeliveryIndex},
		{"index_session_pending_tool_uses_status", createPostgreSQLPendingToolUsesStatusIndex},
		{"index_session_background_tasks_status", createPostgreSQLBackgroundTasksStatusIndex},
		{"index_session_mcp_manifests_generation", createPostgreSQLSessionMCPManifestsGenerationIndex},
		{"index_session_runtime_status_cleanup_due", createPostgreSQLRuntimeStatusCleanupDueIndex},
		{"index_session_bridge_operations_runtime_write", createPostgreSQLBridgeOperationsRuntimeWriteIndex},
		{"index_session_runtime_tool_results_kind", createPostgreSQLRuntimeToolResultsKindIndex},
		{"index_session_output_captures_session", createPostgreSQLSessionOutputCapturesIndex},
		{"index_sandbox_output_capture_open", createPostgreSQLSandboxOutputCaptureOpenIndex},
		{"index_sandbox_output_capture_expiry", createPostgreSQLSandboxOutputCaptureExpiryIndex},
		{"index_request_usage_details_thread", createPostgreSQLRequestUsageDetailsThreadIndex},
		{"index_session_provider_auth_credential", createPostgreSQLSessionProviderAuthCredentialIndex},
		{"index_session_provider_auth_active_session", createPostgreSQLSessionProviderAuthActiveSessionIndex},
		{"index_platform_provider_keys_provider_status", createPostgreSQLPlatformProviderKeysProviderStatusIndex},
		{"index_queue_jobs_active_dedupe", createPostgreSQLQueueJobsActiveDedupeIndex},
		{"index_queue_jobs_leased_partition", createPostgreSQLQueueJobsLeasedPartitionIndex},
		{"index_queue_jobs_leased_thread", createPostgreSQLQueueJobsLeasedThreadIndex},
		{"index_queue_jobs_leased_session", createPostgreSQLQueueJobsLeasedSessionIndex},
		{"index_queue_jobs_partition_sequence", createPostgreSQLQueueJobsPartitionSequenceIndex},
		{"index_queue_jobs_available", createPostgreSQLQueueJobsAvailableIndex},
		{"index_queue_jobs_sandbox_terminal_retention", createPostgreSQLQueueJobsSandboxTerminalRetentionIndex},
		{"index_queue_jobs_sandbox_session_cleanup", createPostgreSQLQueueJobsSandboxSessionCleanupIndex},
		{"index_session_resources_session_seq", createPostgreSQLSessionResourcesSessionSeqIndex},
		{"index_session_resources_type", createPostgreSQLSessionResourcesTypeIndex},
		{"index_session_resource_prefix_gc_due", createPostgreSQLSessionResourcePrefixGCDueIndex},
		{"index_session_git_tickets_live", createPostgreSQLSessionGitTicketsLiveIndex},
		{"index_session_memory_store_resources_store", createPostgreSQLSessionMemoryStoreFilterIndex},
		{"index_agents_workspace_seq", createPostgreSQLAgentsWorkspaceSeqIndex},
		{"index_environments_workspace_seq", createPostgreSQLEnvironmentsWorkspaceSeqIndex},
		{"index_environment_artifacts_status", createPostgreSQLEnvironmentArtifactsStatusIndex},
		{"index_vaults_workspace_seq", createPostgreSQLVaultsWorkspaceSeqIndex},
		{"index_credentials_vault_seq", createPostgreSQLCredentialsVaultSeqIndex},
		{"index_credentials_active_mcp_url_unique", createPostgreSQLCredentialsActiveMCPURLIndex},
		{"index_api_keys_workspace_seq", createPostgreSQLApiKeysWorkspaceSeqIndex},
		{"index_api_keys_bootstrap_unique", createPostgreSQLApiKeysBootstrapUniqueIndex},
		{"index_skills_workspace_seq", createPostgreSQLSkillsWorkspaceSeqIndex},
		{"index_skill_versions_skill_seq", createPostgreSQLSkillVersionsSkillSeqIndex},
		{"index_files_workspace_seq", createPostgreSQLFilesWorkspaceSeqIndex},
		{"index_files_workspace_scope_seq", createPostgreSQLFilesWorkspaceScopeSeqIndex},
		{"index_memory_stores_workspace_created_id", createPostgreSQLMemoryStoresCreatedIndex},
		{"index_memory_stores_workspace_seq", createPostgreSQLMemoryStoresSeqIndex},
		{"index_memories_active_path", createPostgreSQLMemoriesActivePathIndex},
		{"index_memories_created", createPostgreSQLMemoriesCreatedIndex},
		{"index_memories_updated", createPostgreSQLMemoriesUpdatedIndex},
		{"index_memory_versions_created", createPostgreSQLMemoryVersionsCreatedIndex},
		{"index_memory_versions_memory", createPostgreSQLMemoryVersionsMemoryIndex},
		{"index_memory_versions_operation", createPostgreSQLMemoryVersionsOperationIndex},
		{"index_memory_versions_api_key", createPostgreSQLMemoryVersionsAPIKeyIndex},
	}

	// RLS: enable + force on every workspace-owned table, then
	// (re)create the workspace_isolation policy. Each ALTER TABLE is
	// idempotent. Policy refresh uses DROP IF EXISTS + CREATE because
	// CREATE POLICY does not have IF NOT EXISTS in PostgreSQL 18.
	for _, table := range postgresqlContract.WorkspaceTables {
		steps = append(steps,
			postgresqlSchemaStep{
				name: "rls_enable_" + table,
				ddl:  fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentifier(table)),
			},
			postgresqlSchemaStep{
				name: "rls_force_" + table,
				ddl:  fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", quoteIdentifier(table)),
			},
			postgresqlSchemaStep{
				name: "rls_policy_workspace_isolation_" + table,
				ddl:  fmt.Sprintf("DROP POLICY IF EXISTS workspace_isolation ON %s", quoteIdentifier(table)),
			},
			postgresqlSchemaStep{
				name: "rls_policy_workspace_isolation_" + table,
				ddl: fmt.Sprintf(`CREATE POLICY workspace_isolation ON %s
				USING (workspace_id = current_setting('tetral.workspace_id', true))
				WITH CHECK (workspace_id = current_setting('tetral.workspace_id', true))`,
					quoteIdentifier(table)),
			},
		)
	}

	// Attachment-consumption rows are append-only accounting: Runtime may
	// read and insert rows in its workspace, but no serving path may update or
	// delete them.
	steps = append(steps,
		postgresqlSchemaStep{name: "rls_enable_session_file_attachment_consumptions", ddl: `ALTER TABLE session_file_attachment_consumptions ENABLE ROW LEVEL SECURITY`},
		postgresqlSchemaStep{name: "rls_force_session_file_attachment_consumptions", ddl: `ALTER TABLE session_file_attachment_consumptions FORCE ROW LEVEL SECURITY`},
		postgresqlSchemaStep{name: "rls_policy_workspace_select_session_file_attachment_consumptions_drop", ddl: `DROP POLICY IF EXISTS workspace_select ON session_file_attachment_consumptions`},
		postgresqlSchemaStep{name: "rls_policy_workspace_insert_session_file_attachment_consumptions_drop", ddl: `DROP POLICY IF EXISTS workspace_insert ON session_file_attachment_consumptions`},
		postgresqlSchemaStep{name: "rls_policy_workspace_select_session_file_attachment_consumptions", ddl: `CREATE POLICY workspace_select ON session_file_attachment_consumptions
		FOR SELECT
		USING (workspace_id = current_setting('tetral.workspace_id', true))`},
		postgresqlSchemaStep{name: "rls_policy_workspace_insert_session_file_attachment_consumptions", ddl: `CREATE POLICY workspace_insert ON session_file_attachment_consumptions
		FOR INSERT
		WITH CHECK (workspace_id = current_setting('tetral.workspace_id', true))`},
	)

	// Narrow auth-lookup policy on api_keys: when
	// `tetral.auth_lookup` is set to 'true' for the current
	// transaction, SELECTs see every workspace's api_keys row. Used
	// solely by the pre-workspace authentication lookup that resolves
	// x-api-key → workspace_id. Every other access path leaves the
	// setting unset and falls through to workspace_isolation. The
	// policy is FOR SELECT only, so an auth-lookup transaction cannot
	// insert/update/delete api_keys rows even by accident.
	steps = append(steps,
		postgresqlSchemaStep{
			name: "rls_policy_auth_lookup_drop",
			ddl:  "DROP POLICY IF EXISTS auth_lookup ON api_keys",
		},
		postgresqlSchemaStep{
			name: "rls_policy_auth_lookup",
			ddl: `CREATE POLICY auth_lookup ON api_keys
			FOR SELECT
			USING (current_setting('tetral.auth_lookup', true) = 'true')`,
		},
	)

	// Narrow git-ticket lookup policy on session_git_tickets: the git
	// proxy validates a capability ticket before it knows the workspace.
	// This SELECT-only policy is enabled only inside that lookup
	// transaction, which then yields the workspace/session bound to the
	// ticket row.
	steps = append(steps,
		postgresqlSchemaStep{
			name: "rls_policy_git_ticket_lookup_drop",
			ddl:  "DROP POLICY IF EXISTS git_ticket_lookup ON session_git_tickets",
		},
		postgresqlSchemaStep{
			name: "rls_policy_git_ticket_lookup",
			ddl: `CREATE POLICY git_ticket_lookup ON session_git_tickets
			FOR SELECT
			USING (current_setting('tetral.git_ticket_lookup', true) = 'true')`,
		},
		postgresqlSchemaStep{
			name: "rls_policy_queue_maintenance_drop",
			ddl:  "DROP POLICY IF EXISTS queue_maintenance ON queue_jobs",
		},
		postgresqlSchemaStep{
			name: "rls_policy_queue_maintenance",
			ddl: `CREATE POLICY queue_maintenance ON queue_jobs
			USING (current_setting('tetral.queue_maintenance', true) = 'true')
			WITH CHECK (current_setting('tetral.queue_maintenance', true) = 'true')`,
		},
		postgresqlSchemaStep{
			name: "rls_policy_queue_counter_maintenance_drop",
			ddl:  "DROP POLICY IF EXISTS queue_maintenance ON queue_partition_counters",
		},
		postgresqlSchemaStep{
			name: "rls_policy_queue_counter_maintenance",
			ddl: `CREATE POLICY queue_maintenance ON queue_partition_counters
			USING (current_setting('tetral.queue_maintenance', true) = 'true')
			WITH CHECK (current_setting('tetral.queue_maintenance', true) = 'true')`,
		},
		// Narrow transient-attachment sweep policy: the blob garbage
		// collector reclaims expired and consumed attachments across every
		// workspace, so it cannot carry a workspace predicate. The policy is
		// enabled only inside that sweep transaction.
		postgresqlSchemaStep{
			name: "rls_policy_transient_attachment_gc_drop",
			ddl:  "DROP POLICY IF EXISTS transient_attachment_gc ON session_transient_attachments; DROP POLICY IF EXISTS transient_attachment_gc_select ON session_transient_attachments; DROP POLICY IF EXISTS transient_attachment_gc_update ON session_transient_attachments",
		},
		// SELECT and UPDATE only: the sweep reads candidates and marks them, and
		// nothing else. A FOR ALL policy would also let a bug inside the sweep
		// transaction insert or delete rows in any workspace.
		postgresqlSchemaStep{
			name: "rls_policy_transient_attachment_gc_select",
			ddl: `CREATE POLICY transient_attachment_gc_select ON session_transient_attachments
			FOR SELECT
			USING (current_setting('tetral.transient_attachment_gc', true) = 'true')`,
		},
		postgresqlSchemaStep{
			name: "rls_policy_transient_attachment_gc_update",
			ddl: `CREATE POLICY transient_attachment_gc_update ON session_transient_attachments
			FOR UPDATE
			USING (current_setting('tetral.transient_attachment_gc', true) = 'true')
			WITH CHECK (current_setting('tetral.transient_attachment_gc', true) = 'true')`,
		},
		postgresqlSchemaStep{
			name: "rls_policy_transient_attachment_gc_execution_drop",
			ddl:  "DROP POLICY IF EXISTS transient_attachment_gc_select ON session_runtime_tool_results",
		},
		postgresqlSchemaStep{
			name: "rls_policy_transient_attachment_gc_execution_select",
			ddl: `CREATE POLICY transient_attachment_gc_select ON session_runtime_tool_results
			FOR SELECT
			USING (current_setting('tetral.transient_attachment_gc', true) = 'true')`,
		},
	)

	return steps
}

// quoteIdentifier double-quotes a PostgreSQL identifier. The rls
// targets are hardcoded constants and never user input, so the helper
// is a lightweight safety wrapper for embedding identifiers in
// fmt.Sprintf — not a general-purpose escape.
func quoteIdentifier(name string) string {
	return `"` + name + `"`
}

// WithWorkspaceTx opens a PostgreSQL transaction, sets the
// transaction-local `tetral.workspace_id` GUC to workspaceID, invokes
// fn against the transaction, and commits if fn returns nil. On any
// error the transaction is rolled back.
//
// Workspace-owned business writes/reads must run through this helper
// so PostgreSQL row-level security policies see a configured
// workspace_id. A SELECT/INSERT/UPDATE/DELETE that runs without the
// setting hits the workspace_isolation policy with NULL = workspace_id
// (which is false), so RLS fails closed when workspace context is missing.
//
// Connection-pool safety: the helper uses set_config with the
// transaction-local flag (third argument true), equivalent to
// `SET LOCAL`. The setting reverts at COMMIT/ROLLBACK so a
// connection returned to the pool does not leak workspace context to
// the next caller.
func WithWorkspaceTx(ctx context.Context, db *sql.DB, workspaceID string, fn func(*sql.Tx) error) error {
	if workspaceID == "" {
		return errors.New("storage: WithWorkspaceTx requires a non-empty workspace_id")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	// set_config with the third argument true is the transaction-local
	// equivalent of SET LOCAL. Unlike SET LOCAL the value can flow
	// through a bind parameter, which avoids any string concatenation
	// of the workspace identifier into SQL.
	if _, err := tx.ExecContext(ctx,
		"SELECT set_config('tetral.workspace_id', $1, true)",
		workspaceID,
	); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// skillRegistryAdvisoryLockCategory partitions Engine's transaction-
// scoped advisory lock keyspace. PostgreSQL's two-int4 advisory lock
// form (`pg_advisory_xact_lock(int4, int4)`) gives us a stable
// (category, resource_hash) pair: the category int identifies which
// resource family is locked, and the resource_hash is derived from
// the workspace identifier. Two different lock helpers using
// distinct categories cannot collide even when their resource_hash
// values coincide. The literal value is a stable arbitrary int32
// scoped to Skill-registry serialization.
const skillRegistryAdvisoryLockCategory int32 = 0x736B_696C // "skil"

// filesAdvisoryLockCategory partitions the Files quota/mutation
// advisory-lock keyspace away from Skills. The literal is stable and
// arbitrary; only category distinctness and workspace-derived resource
// hashing matter.
const filesAdvisoryLockCategory int32 = 0x6669_6C65 // "file"

// memoryStoreCreateAdvisoryLockCategory serializes Memory Store count
// quota checks for store creation. It is intentionally separate from
// per-store mutation locking because a new store does not have an id
// until the create transaction allocates one.
const memoryStoreCreateAdvisoryLockCategory int32 = 0x6D65_6D63 // "memc"

// memoryStoreMutationAdvisoryLockCategory serializes mutations within
// one Memory Store. The resource hash includes both workspace id and
// memory_store_id, so different stores in one workspace can mutate
// concurrently.
const memoryStoreMutationAdvisoryLockCategory int32 = 0x6D65_6D73 // "mems"

// SessionRuntimeMutationAdvisoryLockCategory serializes runtime
// session resource materialization across Engine instances. It is
// session-scoped, not workspace-scoped, so two sessions in the same
// workspace can still add/delete resources concurrently.
const SessionRuntimeMutationAdvisoryLockCategory int32 = 0x7365_7372 // "sesr"

type transactionExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type sessionFileTransactionExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (interface {
		RowsAffected() (int64, error)
	}, error)
}

// AcquireWorkspaceSkillRegistryLock takes a transaction-scoped
// PostgreSQL advisory lock keyed deterministically by workspaceID.
// The lock serializes Skill registry mutations (new Skill upload, new
// version upload, soft-delete) plus Agent create/update transactions
// that reference non-empty skills[] for the same workspace, so quota
// checks, active name uniqueness checks, and version generation see
// a consistent registry view per workspace.
//
// Lock release is automatic on COMMIT or ROLLBACK because the call
// uses pg_advisory_xact_lock; callers do not need an explicit
// unlock path. Different workspace ids hash to distinct lock slots
// so unrelated workspaces never serialize against each other.
func AcquireWorkspaceSkillRegistryLock(ctx context.Context, tx transactionExecutor, workspaceID string) error {
	if workspaceID == "" {
		return errors.New("storage: workspace skill registry lock requires a non-empty workspace_id")
	}
	resource := workspaceSkillRegistryLockResource(workspaceID)
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		skillRegistryAdvisoryLockCategory, resource,
	); err != nil {
		return err
	}
	return nil
}

// AcquireWorkspaceFilesLock takes a transaction-scoped PostgreSQL
// advisory lock keyed deterministically by workspaceID. The lock
// serializes Files quota reads and quota-counted mutations within one
// workspace while using a distinct category from Skills, so unrelated
// resource families do not block each other.
func AcquireWorkspaceFilesLock(ctx context.Context, tx transactionExecutor, workspaceID string) error {
	if workspaceID == "" {
		return errors.New("storage: workspace files lock requires a non-empty workspace_id")
	}
	resource := workspaceAdvisoryLockResource(workspaceID)
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		filesAdvisoryLockCategory, resource,
	); err != nil {
		return err
	}
	return nil
}

func AcquireWorkspaceFilesLockForSession(ctx context.Context, tx sessionFileTransactionExecutor, workspaceID string) error {
	if workspaceID == "" {
		return errors.New("storage: workspace files lock requires a non-empty workspace_id")
	}
	resource := workspaceAdvisoryLockResource(workspaceID)
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		filesAdvisoryLockCategory, resource,
	); err != nil {
		return err
	}
	return nil
}

// AcquireWorkspaceMemoryStoreCreateLock takes a transaction-scoped
// PostgreSQL advisory lock keyed by workspaceID. Memory Store create
// uses this lock for race-safe workspace store-count quota checks
// before a concrete memory_store_id exists.
func AcquireWorkspaceMemoryStoreCreateLock(ctx context.Context, tx transactionExecutor, workspaceID string) error {
	if workspaceID == "" {
		return errors.New("storage: workspace memory store create lock requires a non-empty workspace_id")
	}
	resource := workspaceAdvisoryLockResource(workspaceID)
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		memoryStoreCreateAdvisoryLockCategory, resource,
	); err != nil {
		return err
	}
	return nil
}

// AcquireMemoryStoreMutationLock takes a transaction-scoped
// PostgreSQL advisory lock keyed by workspaceID and memoryStoreID.
// It serializes store update/archive/delete, memory mutations, and
// version redaction for exactly one Memory Store.
func AcquireMemoryStoreMutationLock(ctx context.Context, tx transactionExecutor, workspaceID string, memoryStoreID string) error {
	category, resource, err := MemoryStoreMutationAdvisoryLockKey(workspaceID, memoryStoreID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		category, resource,
	); err != nil {
		return err
	}
	return nil
}

// MemoryStoreMutationAdvisoryLockKey returns the stable PostgreSQL advisory
// lock key shared by transaction-scoped memory mutations and the connection-
// scoped live projection push lock.
func MemoryStoreMutationAdvisoryLockKey(workspaceID string, memoryStoreID string) (int32, int32, error) {
	if workspaceID == "" {
		return 0, 0, errors.New("storage: memory store mutation lock requires a non-empty workspace_id")
	}
	if memoryStoreID == "" {
		return 0, 0, errors.New("storage: memory store mutation lock requires a non-empty memory_store_id")
	}
	return memoryStoreMutationAdvisoryLockCategory, memoryStoreAdvisoryLockResource(workspaceID, memoryStoreID), nil
}

// workspaceSkillRegistryLockResource maps a workspace identifier to
// the stable int32 resource id used as the second argument of the
// two-int4 advisory lock form. SHA-256 keeps the mapping
// distribution-free and avoids accidental collisions for workspace
// ids drawn from the application namespace.
func workspaceSkillRegistryLockResource(workspaceID string) int32 {
	return workspaceAdvisoryLockResource(workspaceID)
}

func workspaceAdvisoryLockResource(workspaceID string) int32 {
	sum := sha256.Sum256([]byte(workspaceID))
	return int32(binary.BigEndian.Uint32(sum[:4]))
}

func memoryStoreAdvisoryLockResource(workspaceID string, memoryStoreID string) int32 {
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + memoryStoreID))
	return int32(binary.BigEndian.Uint32(sum[:4]))
}

// SessionRuntimeMutationAdvisoryLockResource maps a workspace/session pair to
// the stable resource id used by the two-int4 PostgreSQL advisory lock form.
func SessionRuntimeMutationAdvisoryLockResource(workspaceID string, sessionID string) (int32, error) {
	if workspaceID == "" {
		return 0, errors.New("storage: session runtime mutation lock requires a non-empty workspace_id")
	}
	if sessionID == "" {
		return 0, errors.New("storage: session runtime mutation lock requires a non-empty session_id")
	}
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + sessionID))
	return int32(binary.BigEndian.Uint32(sum[:4])), nil
}

// AcquireSessionRuntimeMutationLock joins a transaction to the short,
// database-wide Session arbitration boundary shared by Queue lease admission
// and Session infrastructure mutations. Callers must take this lock before
// locking pre-existing Queue, Inbox, Thread, or operation rows.
func AcquireSessionRuntimeMutationLock(ctx context.Context, tx transactionExecutor, workspaceID string, sessionID string) error {
	resource, err := SessionRuntimeMutationAdvisoryLockResource(workspaceID, sessionID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1, $2)",
		SessionRuntimeMutationAdvisoryLockCategory,
		resource,
	)
	return err
}

// RuntimeRoleError indicates that the active PostgreSQL connection
// role bypasses RLS. Engine startup must reject such a role before
// serving traffic because workspace isolation cannot be enforced
// against superusers or roles with the BYPASSRLS attribute.
type RuntimeRoleError struct {
	Role        string
	IsSuperuser bool
	BypassesRLS bool
}

func (e *RuntimeRoleError) Error() string {
	return fmt.Sprintf(
		"postgresql: runtime role %q is unsafe for workspace RLS (superuser=%v, bypassrls=%v); use a non-superuser NOBYPASSRLS role for Engine connections",
		e.Role, e.IsSuperuser, e.BypassesRLS,
	)
}

// VerifyRuntimeRole checks that the active connection role is not a
// PostgreSQL superuser and does not have the BYPASSRLS attribute.
// Engine startup runs this against the runtime *sql.DB before serving
// requests so a misconfigured DSN cannot silently disable RLS.
func VerifyRuntimeRole(ctx context.Context, db *sql.DB) error {
	var role string
	var isSuperuser, bypassRLS bool
	err := db.QueryRowContext(ctx,
		`SELECT current_user::text, rolsuper, rolbypassrls
		   FROM pg_roles WHERE rolname = current_user`,
	).Scan(&role, &isSuperuser, &bypassRLS)
	if err != nil {
		return err
	}
	if isSuperuser || bypassRLS {
		return &RuntimeRoleError{
			Role:        role,
			IsSuperuser: isSuperuser,
			BypassesRLS: bypassRLS,
		}
	}
	return nil
}
