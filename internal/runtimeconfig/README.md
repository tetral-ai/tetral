# runtimeconfig

This package interprets durable Runtime configuration for independently owned
Bridge cold loading and Job Runner command preparation. The system comes from
the Session-pinned `agent_versions.config_json`; tool family, MCP configuration
and permission policy come from current `sessions.installed_tools_json`.
Attached memory resources are read under the caller's workspace transaction.
Original agent tool settings never override the installed snapshot.

Callers keep one transaction across the Session's pinned version, installed
configuration and attached resources. This package owns no environment parsing,
database connection pool, RPC server or worker lifecycle. Literal configuration
vectors and cross-owner PostgreSQL compositions verify policy and payload facts.
