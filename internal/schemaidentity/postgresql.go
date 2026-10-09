// Package schemaidentity owns PostgreSQL version identities shared by migration,
// readiness and release code. It has no database, driver or DDL dependencies.
package schemaidentity

const (
	PostgreSQLSchemaVersionOneChecksum = "7bd02f81776abbe8e1f8747703448301aa885feb81bdaab7d9abb6ff53ea3488"
)

// Identity binds a schema version to the checksum of its ordered migration DDL.
type Identity struct {
	Version  int64
	Checksum string
}

// History returns a detached, ordered copy of the registered schema identities,
// currently the single fresh-install baseline. Storage pins its registry to
// PostgreSQLSchemaVersionOneChecksum and verifies its DDL checksum; release
// metadata and the Gateway schema mirror check consume this list.
func History() []Identity {
	return []Identity{{Version: 1, Checksum: PostgreSQLSchemaVersionOneChecksum}}
}
