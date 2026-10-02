# Bun PostgreSQL configuration

This package owns the common process-pool policy used by Provider Gateway and
MCP Connector. It reads no ambient environment, opens no connection, and imports
no service. Commands pass validated bounds to Bun SQL. Go's pool policy remains
owned independently by `internal/dbconnect`.

| Environment key | Field | Default | Unit |
| --- | --- | --- | --- |
| `TETRAL_DATABASE_POOL_MAX` | max | 10 | connections |
| `TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS` | idleTimeout | 30 | seconds |
| `TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS` | maxLifetime | 1800 | seconds |
| `TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS` | connectionTimeout | 30 | seconds |
| `TETRAL_DATABASE_STATEMENT_TIMEOUT_MS` | statementTimeoutMs | 30000 | milliseconds |

All supplied values must be canonical positive decimal safe integers. Zero,
negative, fractions, whitespace and unsafe integers fail startup; omitted keys
use defaults. The caller explicitly supplies the established empty-value policy:
Provider Gateway treats an empty string as omitted; MCP Connector rejects it.
Settings are parsed once during boot and require a process restart to change.
Service configuration tests assert literal defaults, override propagation and
invalid values through those caller boundaries.
