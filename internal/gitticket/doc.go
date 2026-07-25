// Package gitticket owns opaque Git proxy capability tokens and their durable lifecycle.
//
// OWNS:
//   - Table session_git_tickets: every read and write to it flows through
//     PostgreSQLGitTicketStore; no other package reaches the table directly.
//   - The Ticket durable domain type and the status vocabulary StatusPending,
//     StatusLive, StatusRotated, which is the CHECK-constrained status column.
//   - The capability-token format: a 256-bit token (TokenBytes=32) encoded
//     base64url without padding, always TokenChars=43 characters; carried in the
//     HeaderName request header and persisted only as its 32-byte SHA-256
//     token_hash. The raw token is never stored.
//   - The read-path row-level-security handshake: FindByTokenHash runs a
//     read-only transaction that sets the tetral.git_ticket_lookup GUC so the
//     SELECT-only policy on the table admits a hash lookup made before the
//     caller knows which workspace owns the ticket.
//
// STATE MACHINE (session_git_tickets.status; a row is keyed by (workspace_id, session_id, ticket_id)):
//
//	States:
//	  pending  hash stored, token not yet proven installed in the sandbox; the proxy rejects it.
//	  live     token installed and authoritative; the proxy accepts it.
//	  rotated  superseded by a newer live token; the proxy accepts it only within RotationGrace of rotated_at.
//
//	Transitions (writer store method; every writer is called from internal/sandbox):
//	  (none) -> pending   CreatePending     INSERT, before the sandbox git-config install of that token.
//	  pending -> live     ActivatePending   after the install of that token succeeds; clears rotated_at.
//	  live -> rotated     ActivatePending   same transaction as the sibling pending->live promotion; sets rotated_at=now.
//
//	ActivatePending is idempotent (an already-live row is returned unchanged) and rejects a row that is neither pending nor live.
//
//	Readers:
//	  FindByTokenHash         global token_hash lookup — the git proxy validator (services/git-proxy).
//	  FindBySessionTokenHash  session-scoped token_hash lookup — sandbox install recovery.
//
// INVARIANTS:
//   - A row advances pending -> live -> rotated only; it never moves backward and
//     never skips the live state.
//   - At most one row per (workspace_id, session_id) is live at any instant
//     (partial unique index on status = 'live'); ActivatePending rotates every
//     live row of the session before promoting the pending one, so the promotion
//     never collides with that index.
//   - The live->rotated demotion of the old token and the pending->live promotion
//     of its replacement commit together in one workspace transaction, so the two
//     tokens are never both live and never both non-live.
//   - ActivatePending runs only after the sandbox git-config install of the pending
//     token succeeds; the previously installed token therefore stays live until its
//     replacement is proven installed.
//   - rotated_at is NULL for pending and live rows and holds the promotion instant
//     for rotated rows (CHECK-enforced).
//   - The proxy honors a token only while its row is live, or rotated within
//     RotationGrace of rotated_at; a pending row is never honored.
//
// UPDATE-WITH:
//
//	internal/gitticket/store.go (CreatePending, ActivatePending, FindByTokenHash, FindBySessionTokenHash);
//	internal/gitticket/token.go (status vocabulary, token/hash format and bounds);
//	internal/storage/postgresql_schema.go (session_git_tickets table, the live partial
//	  unique index, the status/rotation CHECK constraints, the git_ticket_lookup RLS policy);
//	internal/sandbox/github_preparation.go (prepareGitHubRepositories, recoverInstalledGitTicket);
//	services/git-proxy/ticket.go (TicketValidator.Validate).
package gitticket
