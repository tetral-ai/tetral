// Package storage owns the Engine control-plane's PostgreSQL durable schema:
// the DDL for the session/runtime/workspace tables, schema versioning and
// migration, connection opening, and the workspace-scoped transaction and
// advisory-lock helpers that other packages build their stores on.
//
// OWNS:
//   - The durable table/index/policy DDL and its ordered application
//     (InitializePostgreSQLSchema), plus the migration registry, per-version
//     checksums, and the cluster-wide migration advisory lock (MigrateSchema,
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
//	state            meaning                              writer          legal next
//	absent           no migration registry                -               -> current (MigrateSchema)
//	current          registry present, versions applied   MigrateSchema   -> ahead (append a new migration)
//	behind           registry older than this binary      -               -> current (MigrateSchema)
//	ahead            registry newer than this binary      -               (rejected: SchemaErrorAhead)
//	checksum drift   an applied checksum != its pinned     -               (rejected: SchemaErrorChecksumDrift)
//
// Only one connection migrates at a time: every migrator holds
// PostgreSQLSchemaAdvisoryLockID while inspecting and applying history.
//
// INVARIANTS:
//   - Durable rows are the source of truth. Runtime Pod hot state is
//     residency and execution state only; it is recoverable from the durable
//     state Bridge loads, so losing pod hot state loses no committed fact.
//   - Every DDL statement is idempotent (IF NOT EXISTS, or DROP + CREATE for
//     policies), so InitializePostgreSQLSchema is a no-op against an
//     already-initialized database.
//   - Before the first public release, the version-one baseline is the clean
//     bootstrap schema and its checksum is regenerated when pre-release
//     shapes are folded into their final definitions. After release, schema
//     changes append migrations without rewriting an applied version.
//   - The DDL uses only ordinary table/index DDL plus row-level security, so
//     it stays portable across self-managed PostgreSQL and managed providers.
//
// UPDATE-WITH:
//   - postgresql_schema.go (table/index/policy DDL and InitializePostgreSQLSchema)
//   - postgresql_migrator.go (version checksums, baseline steps, MigrateSchema/VerifySchema)
//   - postgresql_database.go (connection open)
//
// # Durable row-family ownership
//
// The DDL in this package defines the columns and constraints of each table
// but not which service may write it or read it. Owner (writer boundary) and
// reader boundary for the session and runtime durable row families:
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
//	session_pending_tool_uses                              Bridge only                                                      Bridge LoadContext cold-resume,
//	                                                                                                                         Runtime pending ToolJob
//	session_runtime_status                                 Bridge, the cleanup scheduler, and session-create seeding        cleanup scheduler, Bridge Job Runner, repair
//	session_runtime_bindings                               Bridge only                                                      Bridge command delivery/reconcile, repair
//	session_runtime_inbox (runtime delivery commits)       api/Bridge admission; Sandbox task notifications               Bridge repair, Runtime cold-load guard
//	session_sandbox_bindings                               Sandbox Service; api/Bridge through the provider-neutral         Sandbox Service tool/lifecycle workers
//	                                                        Session-delete release boundary
//	sandbox_lifecycle_operations                           Sandbox Service; api/Bridge through the provider-neutral         Sandbox Service lifecycle workers
//	                                                        Session-delete release boundary
//	session_output_captures                                Bridge FinishIdle adoption                                      Sandbox Service capture dedup
//	sandbox_output_capture_operations / blobs              Sandbox Service                                                  Bridge FinishIdle adoption, cleanup workers
//	session_resources / session_github_repository_resources api Session admission (+ token rotation)                 Sandbox Service clone, git-proxy
//	                                                                                                                         allowlist, public Resources API
//	session_git_tickets                                    Sandbox Service GitHub materialization (mint/rotate)             git-proxy per-request validation
//	session_mcp_manifests                                  Bridge only                                                      Bridge LoadContext, manifest patch delivery
//	session_background_tasks                               Sandbox Service (execution/result); Bridge (conversation commit) Runtime task read/send/cancel, cleanup
//	request_usage_details                                  Bridge only (WriteRequestEnd)                                    session usage projection, audit/billing
//	session_runtime_tool_results                           Bridge (accept/consume); Sandbox Service (execute/result)         Runtime result wait, Bridge replay, MCP lifecycle
//	session_transient_attachments                          Sandbox Service (stage); Bridge (activate/consume/GC)             Gateway ResolveTransientAttachment,
//	                                                                                                                         Bridge LoadContext
//	session_provider_auth                                  api upserts (rotate + soft-delete siblings,               Gateway provider credential resolution
//	                                                       not insert-only); Vault hard-deletes on credential delete
//	queue_jobs                                             owning services admit atomically; Queue Service transitions      Sandbox Service, Bridge Job Runner,
//	                                                                                                                         cleanup scheduler
//	queue_partition_counters                               Queue admission and bounded Queue maintenance                    Queue admission and maintenance
//	platform_provider_keys                                 operator ops CLI (platform credential domain)                    Gateway platform key pool (read-only, cached)
//
// UPDATE-WITH: the table DDL in postgresql_schema.go; the writer/reader
// services under services/bridge, services/api,
// services/sandbox, services/queue, services/event-stream,
// services/git-proxy, services/gateway, and internal/session.
package storage
