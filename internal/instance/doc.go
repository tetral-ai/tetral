// Package instance owns CLI registrations (instances) and their connection
// credentials.
//
// OWNS:
//   - Table instances: one row per registration of a CLI with a Workspace.
//     kind is user (name is its display label) or cloud (name is NULL). The
//     row stays after deregistration as a tombstone, so an instance ID is
//     never issued again.
//   - Table instance_credentials: one row per connection credential of an
//     instance. A row stores only key_digest, the SHA-256 digest of its key,
//     unique across every Workspace.
//   - The connection key format: KeyPrefix followed by the unpadded base64url
//     encoding of 32 bytes from crypto/rand, KeyLength characters in all.
//
// STATES:
//
//	instances.registration_state: active, deregistered (deregistered_at set).
//	instance_credentials.state:   pending, active, revoked (revoked_at and
//	                              revocation_cause set). Expiry is never stored:
//	                              a credential whose expires_at is not later
//	                              than the database clock reads as expired.
//
//	Transitions (writer; callers):
//	  (none) -> active instance          InsertInstance    test fixtures
//	  (none) -> pending/active credential InsertCredential test fixtures
//
//	Readers: ReadInstance, ReadCredential.
//
// INVARIANTS:
//   - The plaintext of a key is never stored, logged or returned except in
//     the one response that may hold it; ConnectionKey prints a placeholder
//     under fmt and slog, and InsertCredential writes only its digest.
//   - Auth resolves a key only through the security-definer function
//     tetral_auth_lookup_connection_key, which returns the digest's holder
//     whatever the credential's state. Auth decides the credential class and
//     identity; computer-hub checks the credential's state, the registration
//     and the connection on every request.
//   - Every row function runs in a transaction scoped to the row's Workspace.
//
// UPDATE-WITH:
//
//	internal/storage/postgresql_instance_schema.go (tables, CHECK constraints,
//	  the lookup function and its auth_lookup policy);
//	database/postgresql.json and database/roles.json (Workspace tables, the
//	  computer_hub role, Auth's execute grant);
//	internal/auth/authority_resolver.go (AuthenticateConnectionKey);
//	services/auth/ext_authz.go (the credential class rules at the edge).
package instance
