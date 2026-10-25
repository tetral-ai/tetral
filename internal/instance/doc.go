// Package instance owns CLI registrations (instances), their connection
// credentials, the idempotency records of the issuing routes, and the CLI
// connections with their replay ledger, protocol-error reports and Session
// instructions.
//
// OWNS:
//   - Table instances: one row per registration of a CLI with a Workspace.
//     kind is user (name is its display label) or cloud (name is NULL). The
//     row stays after deregistration as a tombstone, so an instance ID is
//     never issued again. last_credential_reuse_* is the latest use of a
//     credential that an activation revoked.
//   - Table instance_credentials: one row per connection credential of an
//     instance. A row stores only key_digest, the SHA-256 digest of its key,
//     unique across every Workspace. delivery is response (the key is in the
//     issuing response) or stream (a live renewal of renewal_connection_id,
//     replacing replaces_credential_id, whose key_digest stays NULL until
//     its one fetch at fetched_at); used_at is a pending credential's first
//     authenticated use, which activated it.
//   - Table instance_idempotency_records: one row per (principal credential,
//     route, Idempotency-Key) of an issuing request, holding the canonical
//     request fingerprint and the instance and credential it created.
//   - Table instance_connections: one row per connection of an instance,
//     kept after it ends; the latest one is the instance read's connection.
//     It holds credential_id (the credential that opened it, or the renewal
//     a resume activated), hub_pod (the pod holding the stream),
//     the lease, stream_count, the accepted ready (its replay record, grant,
//     os, arch, cli_version, capabilities) or the refused ready (its replay
//     record and error), the latest accepted heartbeat (its replay record,
//     sequence, time and grant), the end, last_issue_seq (the issue sequence
//     of its latest Session instruction) and first_session_report_at.
//     instances.cli_version is the latest ready's.
//   - Table connection_upstream_messages: the replay ledger of a
//     connection's accepted upstream messages other than ready and heartbeat,
//     keyed by the SHA-256 of message_id, with route, fingerprint and the
//     answer a replay returns; a credential fetch stores status 200 with no
//     body, never the key, so its retry is checked again.
//   - Table connection_protocol_errors: one row per accepted protocol-error
//     report.
//   - Table connection_instructions: one row per Session instruction issued
//     on a connection, unique by message_id over all connections, with its
//     issue_seq, type, exact envelope, trace_context, first_written_at and
//     reported_at. The security-definer function
//     tetral_computer_hub_instruction_pressure counts them across Workspaces
//     (ReadInstructionPressure).
//   - The connection key format: KeyPrefix followed by the unpadded base64url
//     encoding of 32 bytes from crypto/rand, KeyLength characters in all.
//   - The instance event notice on EventChannel, and the connection notice on
//     each hub pod's ConnectionNoticeChannel.
//
// STATES:
//
//	instances.registration_state: active, deregistered (deregistered_at set).
//	instance_credentials.state:   pending, active, revoked (revoked_at and
//	                              revocation_cause delete, deregistration or
//	                              pending_activation set). Expiry is never
//	                              stored: a credential
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
//	  (none) -> active credential         IssueCredential      computer-hub
//	  (none) -> pending stream renewal    IssueRenewal         computer-hub
//	  renewal unfetched -> fetched        FetchCredential      computer-hub
//	  pending -> active, the replaced
//	    credential, unless expired,
//	    -> revoked (pending_activation)   ActivateCredential   computer-hub
//	  latest reuse -> newer one           RecordCredentialReuse computer-hub
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
//	  lease moved to another stream       ResumeConnection     computer-hub
//	  (none) -> issued instruction        IssueInstruction     no service yet
//	  unwritten -> written                MarkWritten          computer-hub
//	  unreported -> reported              MarkReported         no service yet
//	  no Session report -> first stored   MarkFirstSessionReport no service yet
//
//	Readers: ReadInstance, LockInstance, ShareInstance,
//	LockInstanceNoKeyUpdate, ListInstances, ReadObjects,
//	ReadCredential, ReadInstanceCredential, ShareCredential, LockCredential,
//	ListCredentials, LiveCredentialID, PendingRenewals, FindIdempotencyRecord,
//	LockUnendedConnection, LockLiveConnection, LockLiveConnectionOf,
//	ShareOnlineConnection, ReadConnection, FindUpstreamMessage,
//	UnreportedInstructions, ReadInstructionPressure.
//
// INVARIANTS:
//   - The plaintext of a key is never stored, logged or returned except in
//     the one response that may hold it; ConnectionKey prints a placeholder
//     under fmt and slog, IssueCredential writes only its digest, and
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
//     tetral_auth_lookup_connection_key (auth.AuthorityResolver
//     AuthenticateConnectionKey); computer-hub checks the credential's state,
//     the registration and the connection on every request.
//   - Every row function runs in a transaction scoped to the row's Workspace.
//   - AuditCredentialOperation writes one record per issued, fetched,
//     activated or revoked credential after the commit; a revocation that
//     changes nothing writes none.
//   - A stream renewal's key exists only from FetchCredential's one
//     successful call; a pending credential never authenticates anything but
//     the request that activates it, and only a use of a credential revoked
//     with CausePendingActivation is recorded as a reuse. A renewal whose
//     connection ends stays pending until it expires or is revoked.
//   - An instance has at most one connection that has not ended (a partial
//     unique index; IsConnectionRace reports the loser of two concurrent
//     creations). Every lease and grant is the database clock plus at most
//     LeaseGrant, capped by the connection credential's expires_at (Grant).
//   - Every transaction that locks more than one of an instance's rows
//     locks them in the order instance, credentials, connection, each at its
//     first lock in the mode the transaction's writes to it need (FOR UPDATE
//     or FOR NO KEY UPDATE), otherwise FOR SHARE, and never raises a share
//     lock; an UPDATE that finds its row itself takes that row's lock in the
//     same order. So no two of them wait on each other in a cycle.
//   - A replay record is written in the transaction that accepts the
//     message; a message refused by an earlier check records nothing. The
//     connection row keeps the ready's record and the latest heartbeat's
//     only; EndConnection deletes the ledger rows.
//   - Every writer that changes what a connection's stream must write
//     (IssueInstruction, IssueRenewal, MarkFirstSessionReport, EndConnection,
//     hubflow.AcceptReady) notifies the pod named by hub_pod in its
//     transaction, and ResumeConnection the pod that held the previous
//     stream. The notice carries the Workspace, the connection and an
//     issue_seq, the last_issue_seq the transaction read; the pod reads the
//     rows itself and releases the instructions up to that issue_seq.
//   - An instruction's issue_seq is the connection's last_issue_seq plus 1,
//     taken under the connection row's lock in the issuing transaction, so a
//     connection's sequences have no gaps and follow commit order. An
//     instruction is never stored on an ended connection, and its envelope
//     never changes after it is stored.
//
// UPDATE-WITH:
//
//	internal/storage/postgresql_instance_schema.go (tables, CHECK constraints,
//	  the lookup function and its auth_lookup policy, the pressure function
//	  and its instruction_pressure policies);
//	database/postgresql.json and database/roles.json (Workspace tables, the
//	  computer_hub role, Auth's and Computer Hub's execute grants);
//	internal/httpapi/errors.go (the API error types of this package's errors);
//	services/computer-hub/instance_routes.go and instance_stream.go (the
//	  routes over these writers and the EventChannel listener);
//	services/computer-hub/client_*.go, stream.go, dispatch.go and notice.go
//	  (the client routes, the streams, their instruction writes and the
//	  connection notice listener);
//	internal/hubflow (the steps that accept a ready and end a connection);
//	services/computer-hub/observability.json (AuditEvent);
//	internal/auth/authority_resolver.go (AuthenticateConnectionKey);
//	services/auth/ext_authz.go (the credential class rules at the edge).
package instance
