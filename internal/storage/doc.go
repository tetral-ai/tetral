// Package storage owns the Engine control-plane's PostgreSQL durable schema:
// the DDL for the session/runtime/workspace tables, schema versioning and
// migration, connection opening, and the workspace-scoped transaction and
// advisory-lock helpers that other packages build their stores on.
//
// OWNS:
//   - The durable table/index/policy DDL and its ordered application
//     through the migration registry, per-version checksums, and the
//     cluster-wide migration advisory lock (MigrateSchema,
//     VerifySchema, PostgreSQLSchemaAdvisoryLockID).
//   - PostgreSQL connection opening from the runtime DSN
//     (OpenPostgreSQLDatabase, OpenPostgreSQLDatabaseFromEnv).
//   - Workspace-scoped transaction entry and the row-family advisory locks
//     (WithWorkspaceTx, the AcquireWorkspace*/AcquireMemoryStore* helpers,
//     MemoryStoreMutationAdvisoryLockKey, SessionRuntimeMutationAdvisoryLockResource).
//   - The row-level-security workspace-isolation policies applied with the
//     table DDL.
//
// STATE MACHINE (schema readiness, owned here):
//
//	state                      meaning                                       outcome
//	absent                     no history table and no namespace objects     -> current (MigrateSchema); VerifySchema: SchemaErrorMissing
//	current                    history equals the version-one baseline       MigrateSchema verifies only; VerifySchema accepts
//	occupied, no history       namespace objects without a history table     MigrateSchema: SchemaErrorUnexpectedState;
//	                                                                         VerifySchema: SchemaErrorMissing
//	behind                     history is a proper prefix of the registry    rejected by both: SchemaErrorBehind
//	ahead / gap / duplicate    history newer, non-contiguous or repeated     rejected by both
//	checksum drift             an applied checksum != its pinned identity    rejected by both: SchemaErrorChecksumDrift
//	RLS drift                  isolation policies differ from the contract   rejected by both: SchemaErrorRLSDrift
//
// There is no upgrade path: MigrateSchema initializes an empty namespace or
// verifies the exact current identity, and rejects every other state.
// Production initialization is invoked only by cmd/tetral-db-prepare; serving
// processes call VerifySchema without schema repair.
//
// Only one connection migrates at a time: every migrator holds
// PostgreSQLSchemaAdvisoryLockID while inspecting and applying history.
//
// INVARIANTS:
//   - Durable rows are the source of truth. Runtime Pod hot state is
//     residency and execution state only; it is recoverable from the durable
//     state Bridge loads, so losing pod hot state loses no committed fact.
//   - MigrateSchema is the only public schema writer. Initialization commits
//     the whole baseline atomically with its history entry; a rerun on the
//     exact identity applies no DDL.
//   - The single version-one baseline is the fresh-install identity.
//     MigrateSchema never upgrades a predecessor schema.
//   - The DDL is ordinary table/index DDL with row-level security, plus the
//     sessions agent-version trigger, the Auth key-lineage, key
//     usage-generation and terminal-grant triggers, and SECURITY DEFINER
//     functions with a fixed search_path owned by the migration role: the
//     lock-only runtime process function and the Auth credential lookup,
//     authority lock and token prune functions. It stays portable across
//     self-managed PostgreSQL and managed providers.
//     The complete database preparation command additionally requires a
//     PostgreSQL superuser for its current role installer; managed-provider
//     customer administrators without that privilege cannot run it.
//
// UPDATE-WITH:
//   - postgresql_schema.go (version-one table/index/policy/trigger DDL)
//   - postgresql_runtime_schema.go (runtime process, liveness and handoff tables, lock-only functions)
//   - postgresql_auth_schema.go (Auth policy and token tables, lookup/lock/prune functions, key and grant triggers)
//   - postgresql_migrator.go (version checksums, baseline steps, MigrateSchema/VerifySchema)
//   - postgresql_migration_logging.go (safe transaction diagnostics)
//   - postgresql_database.go (connection open)
//
// # Durable row-family ownership
//
// The DDL in this package defines the columns and constraints of each table
// but not which service may write it or read it. Owner (writer boundary) and
// reader boundary for the session, runtime and Auth durable row families
// follow the actual code paths; database/roles.json grants are the enforced
// upper bound (a grant may exist only for row locks or Session-delete
// cascades):
//
//	row family                                             owner / writer boundary                                          readers
//	sessions                                               api Session admission (row + pinned config);              Bridge LoadContext, Gateway provider lookup,
//	                                                        Bridge writes the usage_json projection                          Event Stream
//	session_threads                                        Bridge (runtime status + child-thread writes);                   Runtime LoadContext, Event Stream,
//	                                                        api (public archive admission)                            public Threads API
//	session_events                                         api (public input admission); Bridge (runtime             api list reads, Event Stream SSE,
//	                                                        agent/status/span events)                                        Bridge reconcile, Runtime repair
//	session_event_stream_changes                           the same transaction that inserts/updates a public event         Event Stream cursor / SSE
//	                                                        (api admission, Bridge event/projection writes)
//	session_event_idempotency_keys                         api event admission                                       api replay/conflict lookup
//	session_messages                                       Runtime declarations persisted by Bridge                       Bridge LoadContext, Runtime cold repair
//	session_pending_tool_uses                              Bridge (declaration insert, settlement, interrupt); Job Runner   Bridge LoadContext cold-resume,
//	                                                        settlement through internal/runtimecontrol; api approval         Runtime pending ToolJob, api approval
//	                                                        decision (pending -> resolving)                                  lookup
//	session_runtime_status                                 Bridge, Job Runner (delivery, pod-loss repair, Session           cleanup scheduler, Job Runner, repair
//	                                                        cleanup), the cleanup scheduler, and session-create seeding
//	session_runtime_bindings                               Job Runner (placement, delivery, pod-loss repair, Session        Job Runner delivery/reconcile/repair,
//	                                                        cleanup); Bridge and Job Runner termination closeout and         Bridge receipt fences, Sandbox Service
//	                                                        Bridge handoff release through internal/runtimecontrol           notification custody and prefix GC
//	runtime_process_pods / runtime_processes               Bridge process registration, report and promotion                Bridge receipt fences; Job Runner
//	                                                                                                                         placement and delivery fences (reads
//	                                                                                                                         and the lock-only function)
//	runtime_process_liveness                               Bridge registration (NULL row) and every successful report       Job Runner final loss classification
//	                                                                                                                         (liveness lock-only function)
//	session_runtime_handoffs / _threads                    Bridge handoff release                                           Bridge replay; Bridge and Job Runner
//	                                                                                                                         recovery source checks
//	session_runtime_inbox (runtime delivery commits)       Bridge delivery; Sandbox task notifications                    Bridge lifecycle and exact pod-loss custody
//	session_sandbox_bindings                               Sandbox Service; api/Bridge through the provider-neutral         Sandbox Service tool/lifecycle workers
//	                                                        Session-delete release boundary
//	sandbox_lifecycle_operations                           Sandbox Service; api/Bridge through the provider-neutral         Sandbox Service lifecycle workers
//	                                                        Session-delete release boundary
//	session_output_captures                                Bridge FinishIdle adoption                                      Sandbox Service capture dedup
//	sandbox_output_capture_operations / blobs              Sandbox Service                                                  Bridge FinishIdle adoption, cleanup workers
//	session_resources / session_github_repository_resources api Session admission (+ token rotation)                 Sandbox Service clone, git-proxy
//	                                                                                                                         allowlist, public Resources API
//	session_git_tickets                                    Sandbox Service GitHub materialization (mint/rotate)             git-proxy per-request validation
//	session_mcp_manifests                                  Bridge (connector refresh acceptance); Job Runner (initial       Bridge LoadContext, Job Runner manifest
//	                                                        discovery capture, delivery exhaustion)                          patch delivery
//	session_background_tasks                               Sandbox Service (execution/result); Bridge (conversation commit) Runtime task read/send/cancel, cleanup
//	request_usage_details                                  Bridge (WriteRequestEnd); Job Runner (synthetic terminal End)     session usage projection, audit/billing
//	session_runtime_tool_results                           Bridge (accept/consume); Sandbox Service (execute/result)         Runtime result wait, Bridge replay, MCP lifecycle
//	session_transient_attachments                          Sandbox Service (stage); Bridge (activate/consume/GC)             Gateway ResolveTransientAttachment,
//	                                                                                                                         Bridge LoadContext
//	session_provider_auth                                  api upserts (rotate + soft-delete siblings,               Gateway provider credential resolution
//	                                                       not insert-only); Vault hard-deletes on credential delete
//	queue_jobs                                             owning services admit atomically; Queue Service transitions      Sandbox Service, Job Runner,
//	                                                                                                                         cleanup scheduler
//	queue_partition_counters                               Queue admission and bounded Queue maintenance                    Queue admission and maintenance
//	platform_provider_keys                                 operator ops CLI (platform credential domain)                    Gateway platform key pool (read-only, cached)
//	auth_federation_rules / auth_identities /              tetral-auth-policy over the administrative connection            Auth exchange rule and identity reads;
//	auth_workspace_grants                                   (import, removal, grant revocation)                             grant lookup and authority lock
//	                                                                                                                        functions for exchange, admission and
//	                                                                                                                        key issuance
//	auth_access_tokens                                     Auth exchange issuance; pruning                                  Auth admission (lookup function) and
//	                                                        through the prune function; tetral-auth-policy revocation       derived-key issuance
//
// UPDATE-WITH: the table DDL in postgresql_schema.go,
// postgresql_runtime_schema.go and postgresql_auth_schema.go; the writer/reader
// services under services/bridge, services/job-runner, services/api,
// services/sandbox, services/queue, services/cleanup, services/event-stream,
// services/git-proxy, services/gateway and services/auth; the shared writers in
// internal/runtimecontrol, internal/sessionevent, internal/mcpmanifest,
// internal/session and internal/auth; and the enforced grants in
// database/roles.json.
package storage
