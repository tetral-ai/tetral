// Package schemaidentity owns PostgreSQL version identities shared by migration,
// readiness and release code. It has no database, driver or DDL dependencies.
package schemaidentity

const (
	PostgreSQLSchemaVersionOneChecksum = "693b9bab2fd21c94698ca73959c9c83cdcc87460dbcde29b1ee77477922b91ae"
)

// Identity binds a schema version to the checksum of its ordered migration DDL.
type Identity struct {
	Version  int64
	Checksum string
}

// History returns a detached, ordered copy of the immutable schema identities.
// Storage binds each identity to its DDL and verifies the checksum before use.
func History() []Identity {
	return []Identity{{Version: 1, Checksum: PostgreSQLSchemaVersionOneChecksum}}
}
