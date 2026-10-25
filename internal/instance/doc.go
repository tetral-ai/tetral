// Package instance owns CLI registrations (instances), their connection
// credentials, the idempotency records of the issuing routes, and the CLI
// connections with their replay ledger and protocol-error reports.
//
// OWNS:
//   - Table instances: one row per registration of a CLI with a Workspace.
//     kind is user (name is its display label) or cloud (name is NULL). The
//     row stays after deregistration as a tombstone, so an instance ID is
//     never issued again.
//   - Table instance_credentials: one row per connection credential of an
//     instance. A row stores only key_digest, the SHA-256 digest of its key,
//     unique across every Workspace.
//   - Table instance_idempotency_records: one row per (principal credential,
//     route, Idempotency-Key) of an issuing request, holding the canonical
//     request fingerprint and the instance and credential it created.
//   - Table instance_connections: one row per connection of an instance,
//     kept after it ends; the latest one is the instance read's connection.
//     It holds the opening credential, hub_pod (the pod holding the stream),
//     the lease, stream_count, the accepted ready (its replay record, grant,
//     os, arch, cli_version, capabilities) or the refused ready (its replay
//     record and error), the latest accepted heartbeat (its replay record,
//     sequence, time and grant), and the end. instances.cli_version is the
//     latest ready's.
//   - Table connection_upstream_messages: the replay ledger of a
//     connection's accepted upstream messages other than ready and heartbeat,
//     keyed by the SHA-256 of message_id, with route, fingerprint and the
//     exact answer.
//   - Table connection_protocol_errors: one row per accepted protocol-error
//     report.
//   - The connection key format: KeyPrefix followed by the unpadded base64url
//     encoding of 32 bytes from crypto/rand, KeyLength characters in all.
//   - The instance event notice on EventChannel, and the connection notice on
//     each hub pod's ConnectionNoticeChannel.
//
// STATES:
//
//	instances.registration_state: active, deregistered (deregistered_at set).
//	instance_credentials.state:   pending, active, revoked (revoked_at and
//	                              revocation_cause delete or deregistration
//	                              set). Expiry is never stored: a credential
//	                              whose expires_at is not later than the
//	                              database clock reads as expired
//	                              (CredentialState).
//	instance_connections:         live (not ended, lease_expires_at later
//	                              than the database clock), lease passed,
//	                              ended (ended_at, end_cause lease_expired,
//	                              revoked or deregistered). The instance read
//	                              shows registering, online or offline
//	                              (ConnectionState).
//
//	Transitions (writer; callers):
//	  (none) -> active user instance      CreateUserInstance   computer-hub
//	  (none) -> active instance           InsertInstance       test fixtures
//	  (none) -> active credential         IssueCredential      computer-hub
//	  (none) -> pending/active credential InsertCredential     test fixtures
//	  pending/active -> revoked (delete)  RevokeCredential     computer-hub
//	  active -> deregistered instance,
//	    unrevoked -> revoked (deregistration) Deregister       computer-hub
//	  (none) -> idempotency record        SaveIdempotencyRecord computer-hub
//	  stale record -> (none)              FindIdempotencyRecord computer-hub
//	  (none) -> live connection           CreateConnection     computer-hub
//	  ready unanswered -> accepted        AcceptReady          hubflow.AcceptReady
//	  ready unanswered -> refused         RefuseReady          computer-hub
//	  latest heartbeat -> newer one       AcceptHeartbeat      computer-hub
//	  not ended -> ended                  EndConnection        hubflow.EndConnection
//	  (none) -> ledger row                SaveUpstreamMessage  computer-hub
//	  ledger rows -> (none)               EndConnection        hubflow.EndConnection
//	  (none) -> protocol error            RecordProtocolError  computer-hub
//
//	Readers: ReadInstance, LockInstance, ListInstances, ReadObjects,
//	ReadCredential, ReadInstanceCredential, ListCredentials,
//	LiveCredentialID, FindIdempotencyRecord, LockUnendedConnection,
//	LockLiveConnection, LockLiveConnectionOf, ReadConnection,
//	FindUpstreamMessage.
//
// INVARIANTS:
//   - The plaintext of a key is never stored, logged or returned except in
//     the one response that may hold it; ConnectionKey prints a placeholder
//     under fmt and slog, InsertCredential writes only its digest, and
//     idempotency records and audit records hold no key.
//   - An instance holds at most CredentialLimit credentials that are pending
//     or active and expire after the database clock: IssueCredential locks
//     the instance row before it counts, so concurrent issuances on one
//     instance run one at a time. No credential is revoked to make room.
//   - An idempotency record is written in the transaction of its effect, so a
//     request that fails writes none. Its primary key lets one of two
//     concurrent identical requests commit; the other's transaction fails
//     with IsIdempotencyRace and finds the record when it runs again. A record
//     is honoured while its instance is registered and for
//     IdempotencyRetention after deregistered_at.
//   - Times are the database clock (Now); expiries are stored to the
//     microsecond (ResolveExpiry).
//   - Auth resolves a key only through the security-definer function
//     tetral_auth_lookup_connection_key, which returns the digest's holder
//     whatever the credential's state. Auth decides the credential class and
//     identity; computer-hub checks the credential's state, the registration
//     and the connection on every request.
//   - Every row function runs in a transaction scoped to the row's Workspace.
//   - AuditCredentialOperation writes one record per issued or revoked
//     credential after the commit; a revocation that changes nothing writes
//     none.
//   - An instance has at most one connection that has not ended (a partial
//     unique index; IsConnectionRace reports the loser of two concurrent
//     creations). Every lease and grant is the database clock plus at most
//     LeaseGrant, capped by the connection credential's expires_at (Grant).
//   - A replay record is written in the transaction that accepts the
//     message; a message refused by an earlier check records nothing. The
//     connection row keeps the ready's record and the latest heartbeat's
//     only; EndConnection deletes the ledger rows.
//   - EndConnection and hubflow.AcceptReady notify the pod named by hub_pod
//     in their transaction; the notice carries only the Workspace and the
//     connection, and the pod reads the row.
//
// UPDATE-WITH:
//
//	internal/storage/postgresql_instance_schema.go (tables, CHECK constraints,
//	  the lookup function and its auth_lookup policy);
//	database/postgresql.json and database/roles.json (Workspace tables, the
//	  computer_hub role, Auth's execute grant);
//	internal/httpapi/errors.go (the API error types of this package's errors);
//	services/computer-hub/instance_routes.go and instance_stream.go (the
//	  routes over these writers and the EventChannel listener);
//	services/computer-hub/client_*.go, stream.go and notice.go (the client
//	  routes, the streams and the connection notice listener);
//	internal/hubflow (the steps that accept a ready and end a connection);
//	services/computer-hub/observability.json (AuditEvent);
//	internal/auth/authority_resolver.go (AuthenticateConnectionKey);
//	services/auth/ext_authz.go (the credential class rules at the edge).
package instance
