// Package sandbox defines provider-neutral Sandbox identities, durable resource
// declarations, normalized provider outcomes, and shared materialization data.
//
// Provider lifecycle and command execution are owned by the Sandbox Service.
// This package supplies the stable vocabulary used by the Session store and the
// Daytona adapter without exposing provider SDK types to Bridge, Runtime, Queue,
// or public APIs.
//
// Durable lifecycle state lives in session_sandbox_bindings and
// sandbox_lifecycle_operations. Session creation does not allocate a provider
// resource. An approved Sandbox Tool Use drives lazy activation and
// materialization through Queue jobs; release is produced only by Session
// deletion or displacement of a recorded provider handle.
//
// UPDATE-WITH: services/sandbox/provider_adapter.go,
// services/sandbox/lifecycle_store.go, services/sandbox/execution_store.go, and
// internal/session/materialization_snapshot.go.
package sandbox
