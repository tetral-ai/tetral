package storage

import "testing"

func TestCurrentSchemaIdentity(t *testing.T) {
	actual := checksumPostgreSQLSchemaSteps(postgresqlBaselineSteps())
	if actual != PostgreSQLSchemaVersionOneChecksum {
		t.Fatalf("current schema checksum: %s", actual)
	}
}
