// Package environment owns Environment template CRUD and the environment
// artifact provisioning lifecycle for cloud sandbox images.
//
// OWNS:
//   - Table `environments`: template create/get/list/update/archive/delete
//     (PostgreSQLEnvironmentStore), each write inside a per-workspace tx.
//   - Table `environment_artifacts`: one row per
//     (workspace_id, environment_id, generation) carrying status, provider
//     'daytona', normalized_config_hash, artifact_input_hash,
//     runtime_network_policy_json, packages_json, provider_artifact_ref, and
//     the summarized failure fields. This package writes the admission rows;
//     the sandbox build and fanout runners flip them.
//   - Admission-time enqueue of the environment_build queue job
//     (queue.KindEnvironmentBuild) when packages need a fresh build.
//
// ADMISSION (createEnvironmentArtifactAdmission), by config input:
//   - packages empty: ready generation, provider_artifact_ref = the store's
//     default artifact ref; no build job.
//   - packages non-empty with a reusable ready artifact in the same lineage:
//     ready generation reusing that provider_artifact_ref; no build job.
//   - packages non-empty with a matching in-flight (pending|building) build in
//     the same lineage: pending generation that rides it; no new build job.
//   - packages non-empty with none of the above: pending generation and one
//     environment_build enqueued in the same transaction.
//   - networking changed only: artifact_input_hash is unchanged, so networking
//     is never itself a build trigger; admission still resolves through the same
//     packages-based branches above on that unchanged hash — admitted ready via
//     the default or a reusable ready artifact, ridden as pending against a
//     matching in-flight build, or (when the only same-hash prior generation is
//     failed with no reusable ready artifact) a fresh environment_build — and
//     records the updated runtime_network_policy_json.
//
// STATE MACHINE (environment_artifacts.status):
//
//	state     meaning                                    writer
//	--------  -----------------------------------------  ------------------------------------------------
//	pending   admitted, build not yet completed          this package (admission); build runner (retry)
//	building  build runner holds the row, build running  build runner
//	ready     provider_artifact_ref usable               this package (default/reuse); build runner (ok)
//	failed    terminal build failure, summarized fields  build runner (terminal failure)
//
//	legal transitions:
//	  (new) -> pending | ready   [admission, this package]
//	  pending  -> building       [environment_build lease, build runner]
//	  building -> ready          [build success, build runner]
//	  building -> pending        [retryable build failure, build runner]
//	  building -> failed         [terminal build failure, build runner]
//	  pending  -> ready          [shared build success flips riding rows by
//	                              artifact_input_hash, build runner]
//	  pending  -> failed         [shared terminal failure flips riding rows by
//	                              artifact_input_hash, build runner]
//	(a generation admitted pending that rides an in-flight build is never
//	individually leased to building; when the shared build settles, the ready and
//	terminal-failure flips update every pending|building row of that
//	artifact_input_hash, so riding pending rows go straight to ready or failed.)
//
//	readers: session-create, event re-drive, and claim-path preparation writers
//	read the current row FOR SHARE; environment_ready_fanout reads `ready` rows
//	to advance waiting session preparations; environment_failed_fanout settles
//	gated inputs after a `failed` flip.
//
// INVARIANTS:
//   - artifact_input_hash is the SHA-256 of normalized packages only;
//     networking feeds runtime_network_policy_json and never triggers a
//     rebuild. Ready-artifact reuse is limited to the same
//     (workspace_id, environment_id) lineage with an earlier `ready` generation
//     and a matching artifact_input_hash.
//   - environment_build never calls the sandbox provider while holding a
//     PostgreSQL transaction: it marks `building` in one tx, builds, then
//     persists `ready`/`failed` in a second tx.
//   - Every transaction that writes a session_preparations row with
//     status = waiting_environment (session-create admission, failed-attempt
//     re-drive, and the claim-path writer) first resolves the environment's
//     current generation and locks that single environment_artifacts row
//     FOR SHARE — a direct single-table read that cannot ride an outer join —
//     before writing the waiting row. The terminal build tx takes an exclusive
//     lock on every artifact row it flips. Admission and the failed flip
//     therefore serialize on the artifact row: if the waiting writer commits
//     first, the flip's scan sees the row and environment_failed_fanout settles
//     it; if the flip commits first, the writer's shared read observes `failed`
//     and rejects the session create.
//   - No transaction takes an artifact lock before a sessions-row lock, so the
//     FOR SHARE read adds no lock-order cycle. Plain-append admission onto an
//     already-existing waiting attempt takes no artifact lock; its
//     serialization point is the preparation active-row lock it already holds.
//   - On terminal build failure the build runner, in one transaction, flips the
//     artifact to `failed`, transitions that generation's waiting_environment
//     session_preparations to `failed`, and enqueues exactly one
//     environment_failed_fanout — no artifact-before-sessions lock edge.
//
// UPDATE-WITH:
//
//	internal/environment/postgresql_store.go (createEnvironmentArtifactAdmission,
//	  upsertEnvironmentArtifact, reusableEnvironmentArtifactRef,
//	  reusableInFlightEnvironmentArtifact, enqueueEnvironmentBuild,
//	  environmentArtifactSnapshot)
//	internal/environment/input.go (NormalizeEnvironmentConfig)
//	internal/queue (KindEnvironmentBuild, KindEnvironmentReadyFanout,
//	  KindEnvironmentFailedFanout)
//	internal/session/postgresql_store.go (loadCurrentEnvironmentArtifactForPreparation)
//	internal/sessionevent/postgresql_store.go (failed-attempt re-drive FOR SHARE)
//	internal/sandbox/postgresql_store.go (claim-path waiting_environment FOR SHARE)
//	services/sandbox/environment_build_runner.go
//	services/sandbox/environment_artifact_store.go
//	services/sandbox/environment_ready_fanout_runner.go
//	services/sandbox/environment_failed_fanout_runner.go
package environment
