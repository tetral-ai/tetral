# mcpmanifest

Bridge owns hot `McpManifestChanged` orchestration. Job Runner owns initial and
restoration discovery before input execution. This package owns their common
bounded manifest validation/canonicalization, collision filtering, readiness,
generation acceptance and durable config-carrier enqueue rules.

Connector listing is outside PostgreSQL transactions. Acceptance helpers receive
the owner's existing transaction and acquire the same Session/server arbitration
lock, preserving one manifest generation and Queue carrier on same-etag races.
A repeated etag can restore unready state; an etag is not generation authority.
No helper opens a transaction, reads process environment or imports either
service's business package. ConnectorLister is the actual outbound gRPC client
used by both process constructors; credentials are supplied by the caller.

Installed discovery follows the Session snapshot's configured MCP server names,
including multiple supported adapters. The installed server's endpoint selects
the adapter at the Connector boundary; a configured name is not a provider ID.
Enabled unsupported or missing server selections fail explicitly before external
traffic rather than disappearing from discovery.
