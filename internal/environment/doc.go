// Package environment owns Environment template CRUD and durable provider
// artifact admission.
//
// The environments table holds the public template and current generation.
// environment_artifacts holds one provider artifact state per Environment
// generation. Package changes may enqueue environment_build in the same
// transaction as admission; networking remains runtime policy and does not by
// itself trigger an artifact build.
//
// Artifact states are pending, building, ready, and failed. A building row
// records the current Queue job, lease token, and attempt number; a successor
// claim replaces that identity and fences stale provider outcomes. Sandbox
// Service performs provider builds outside database transactions, then persists
// the terminal result. A ready transition enqueues environment_ready_fanout, which
// releases Sandbox lifecycle operations waiting for that exact generation. A
// terminal failure settles those operations with a normalized Sandbox error.
// Session creation never waits for an artifact and never creates a Sandbox.
//
// Artifact reuse is limited to the same Environment lineage and normalized
// package-input hash. Provider SDK types and credentials never enter this
// package's public types or Queue payloads.
//
// UPDATE-WITH: internal/environment/postgresql_store.go,
// services/sandbox/environment_artifact_store.go,
// services/sandbox/environment_build_runner.go, and
// services/sandbox/environment_ready_fanout_runner.go.
package environment
