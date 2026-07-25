package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestPostgreSQLSchemaVersionOneChecksumMatchesExactOrderedPayload(t *testing.T) {
	hash := sha256.New()
	var length [8]byte
	for _, step := range postgresqlBaselineSteps() {
		binary.BigEndian.PutUint64(length[:], uint64(len(step.ddl)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(step.ddl))
	}
	got := hex.EncodeToString(hash.Sum(nil))
	if got != PostgreSQLSchemaVersionOneChecksum {
		t.Fatalf("exact ordered baseline checksum = %q, declared = %q", got, PostgreSQLSchemaVersionOneChecksum)
	}
}

func TestValidatePostgreSQLMigrationRegistryRejectsMalformedLocalRegistry(t *testing.T) {
	validStep := postgresqlSchemaStep{name: "one", ddl: "SELECT 1"}
	validChecksum := checksumPostgreSQLSchemaSteps([]postgresqlSchemaStep{validStep})
	tests := []struct {
		name       string
		migrations []postgresqlMigration
		kind       SchemaErrorKind
	}{
		{name: "empty", kind: SchemaErrorMalformed},
		{name: "starts after one", migrations: []postgresqlMigration{{version: 2, checksum: validChecksum, steps: []postgresqlSchemaStep{validStep}}}, kind: SchemaErrorGap},
		{name: "gap", migrations: []postgresqlMigration{{version: 1, checksum: validChecksum, steps: []postgresqlSchemaStep{validStep}}, {version: 3, checksum: validChecksum, steps: []postgresqlSchemaStep{validStep}}}, kind: SchemaErrorGap},
		{name: "duplicate", migrations: []postgresqlMigration{{version: 1, checksum: validChecksum, steps: []postgresqlSchemaStep{validStep}}, {version: 1, checksum: validChecksum, steps: []postgresqlSchemaStep{validStep}}}, kind: SchemaErrorDuplicate},
		{name: "payload drift", migrations: []postgresqlMigration{{version: 1, checksum: strings.Repeat("0", 64), steps: []postgresqlSchemaStep{validStep}}}, kind: SchemaErrorChecksumDrift},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostgreSQLMigrationRegistry(test.migrations)
			if err == nil || err.Kind != test.kind {
				t.Fatalf("validatePostgreSQLMigrationRegistry() = %#v, want kind %q", err, test.kind)
			}
		})
	}
}

func TestSchemaMigrationErrorDoesNotExposeDriverCause(t *testing.T) {
	const secret = "postgres://schema-user:do-not-leak@db.internal/tetral"
	err := newSchemaMigrationError(SchemaErrorApply, 1, errors.New(secret))
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("schema error exposed driver cause: %q", err.Error())
	}
	if errors.Is(err, err.cause) {
		t.Fatal("schema error must not unwrap to its private driver cause")
	}
}
