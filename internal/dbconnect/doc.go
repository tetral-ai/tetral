// Package dbconnect opens Engine's PostgreSQL connections and wraps them in
// Client, Tx and SessionLockConn, through which every service runs named
// statements.
//
// Design rules:
//   - Every statement and lock acquisition carries an operation name fixed in
//     code (operation.go validates it); the name, never the SQL text or its
//     arguments, is what diagnostics and metrics see.
//   - observeStatement (client.go) records each named Exec, Query and
//     QueryRow, inside a transaction or not, and each lock acquisition in
//     db_client_operation_duration_seconds, writes db.query.slow above the
//     process's slow-query threshold, and starts a span only when the trace
//     is sampled. Listen, a transaction's whole scope and the time a lock is
//     held are not observed.
//   - Query outcome, elapsed time and span finish together when Rows exhausts,
//     fails or closes, or when Row.Scan completes. Immediate query failures
//     finish immediately. Elapsed time includes caller read intervals, not
//     just server execution. Callers must consume/close Rows and Scan Row.
//     A Rows.Scan conversion error leaves the stream open; Row.Scan's final
//     error is the client-operation outcome. sql.ErrNoRows remains successful.
//   - Connection failures become a DiagnosticError (diagnostic.go) that names
//     provider, phase and kind without the DSN.
//
// Entry points: OpenPlainDSN, OpenProtectedDSN and OpenProtectedDSNFromEnv
// (plain_dsn.go) open a Client; PoolConfigFromEnv (pool_config.go) reads the
// pool bounds.
package dbconnect
