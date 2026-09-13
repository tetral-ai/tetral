// Package schemaidentity owns PostgreSQL version identities shared by migration,
// readiness and release code. It has no database, driver or DDL dependencies.
package schemaidentity

const (
	PostgreSQLSchemaVersionOneChecksum   = "d42f4f8936525f02525b621e943d9ad98a91c6d8a76ca11a309c62dee496ade6"
	PostgreSQLSchemaVersionTwoChecksum   = "36b50e4c53b62e8a7b38b8d91b3128400ff06394bf71dcd3e1d992df32b55458"
	PostgreSQLSchemaVersionThreeChecksum = "be73f97aa7ebc41ec39ad270aed25a2b9d5228eb8ab49e814032283cb9dbd90f"
)

// Identity binds a schema version to the checksum of its ordered migration DDL.
type Identity struct {
	Version  int64
	Checksum string
}

// History returns a detached, ordered copy of the immutable schema identities.
// Storage binds each identity to its DDL and verifies the checksum before use.
func History() []Identity {
	return []Identity{
		{Version: 1, Checksum: PostgreSQLSchemaVersionOneChecksum},
		{Version: 2, Checksum: PostgreSQLSchemaVersionTwoChecksum},
		{Version: 3, Checksum: PostgreSQLSchemaVersionThreeChecksum},
	}
}
