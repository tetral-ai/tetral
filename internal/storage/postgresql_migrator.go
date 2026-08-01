package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"
)

const (
	// PostgreSQLSchemaAdvisoryLockID is the cluster-wide session advisory lock
	// held by the sole migration owner while inspecting and applying schema
	// history. It is exported so deployment/integration tests can prove the
	// cross-connection serialization contract.
	PostgreSQLSchemaAdvisoryLockID int64 = 0x7465_7472_616c_7363 // "tetralsc"

	// PostgreSQLSchemaVersionOneChecksum pins the canonical byte stream of the
	// exact ordered baseline statements returned by postgresqlBaselineSteps.
	// Before the first release baseline is declared, schema-file edits replace
	// that payload and digest together. After declaration, changes append a new
	// migration and leave this digest immutable.
	PostgreSQLSchemaVersionOneChecksum = "33e4d34dad77d1aedd0d7e09dafdb020c19b10ec1128235732ac95441c580ede"

	createPostgreSQLSchemaMigrationsTable = `CREATE TABLE tetral_schema_migrations (
		version BIGINT PRIMARY KEY,
		checksum TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT tetral_schema_migrations_checksum_shape CHECK (length(checksum) = 64)
	)`
)

// SchemaErrorKind is a stable, machine-inspectable schema readiness failure.
type SchemaErrorKind string

const (
	SchemaErrorMissing       SchemaErrorKind = "schema_missing"
	SchemaErrorBehind        SchemaErrorKind = "schema_behind"
	SchemaErrorAhead         SchemaErrorKind = "schema_ahead"
	SchemaErrorMalformed     SchemaErrorKind = "schema_history_malformed"
	SchemaErrorGap           SchemaErrorKind = "schema_history_gap"
	SchemaErrorDuplicate     SchemaErrorKind = "schema_history_duplicate"
	SchemaErrorChecksumDrift SchemaErrorKind = "schema_checksum_drift"
	SchemaErrorLock          SchemaErrorKind = "schema_lock_failed"
	SchemaErrorApply         SchemaErrorKind = "schema_apply_failed"
	SchemaErrorCanceled      SchemaErrorKind = "schema_operation_canceled"
)

// SchemaMigrationError is safe to return through startup and logging
// boundaries. It deliberately retains the raw driver cause only in an
// unexported field and does not unwrap it, so DSNs, SQL, and server details do
// not escape through generic error formatting.
type SchemaMigrationError struct {
	Kind    SchemaErrorKind
	Version int64
	cause   error
}

func (e *SchemaMigrationError) Error() string {
	switch e.Kind {
	case SchemaErrorMissing:
		return "postgresql schema registry is missing"
	case SchemaErrorBehind:
		return "postgresql schema is behind this binary"
	case SchemaErrorAhead:
		return "postgresql schema is ahead of this binary"
	case SchemaErrorMalformed, SchemaErrorGap, SchemaErrorDuplicate:
		return "postgresql schema history is malformed"
	case SchemaErrorChecksumDrift:
		return "postgresql schema checksum does not match this binary"
	case SchemaErrorLock:
		return "postgresql schema migration lock failed"
	case SchemaErrorApply:
		return "postgresql schema migration failed"
	case SchemaErrorCanceled:
		return "postgresql schema operation was canceled"
	default:
		return "postgresql schema operation failed"
	}
}

type postgresqlMigration struct {
	version  int64
	checksum string
	steps    []postgresqlSchemaStep
}

type appliedPostgreSQLMigration struct {
	version  int64
	checksum string
}

type postgresqlMigrationQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func postgresqlMigrationRegistry() []postgresqlMigration {
	return []postgresqlMigration{
		{
			version:  1,
			checksum: PostgreSQLSchemaVersionOneChecksum,
			steps:    postgresqlBaselineSteps(),
		},
	}
}

// MigrateSchema serializes migration owners on one pinned PostgreSQL
// connection, rejects invalid history before mutation, and applies each
// pending migration and its stamp in one transaction on that connection.
func MigrateSchema(ctx context.Context, db *sql.DB) error {
	registry := postgresqlMigrationRegistry()
	if err := validatePostgreSQLMigrationRegistry(registry); err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return newSchemaMigrationError(SchemaErrorLock, 0, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, PostgreSQLSchemaAdvisoryLockID); err != nil {
		return newSchemaMigrationError(SchemaErrorLock, 0, err)
	}

	locked := true
	defer func() {
		if !locked {
			return
		}
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, PostgreSQLSchemaAdvisoryLockID)
	}()

	exists, history, historyErr := readPostgreSQLMigrationHistory(ctx, conn)
	if historyErr != nil {
		return historyErr
	}
	if exists {
		if historyErr := validateAppliedPostgreSQLMigrations(history, registry); historyErr != nil {
			return historyErr
		}
	}

	for index := len(history); index < len(registry); index++ {
		migration := registry[index]
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return newSchemaMigrationError(SchemaErrorApply, migration.version, err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
		if !exists {
			if _, err := tx.ExecContext(ctx, createPostgreSQLSchemaMigrationsTable); err != nil {
				_ = tx.Rollback()
				return newSchemaMigrationError(SchemaErrorApply, migration.version, err)
			}
			exists = true
		}
		if err := executePostgreSQLSchemaSteps(ctx, tx, migration.steps); err != nil {
			_ = tx.Rollback()
			return newSchemaMigrationError(SchemaErrorApply, migration.version, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO tetral_schema_migrations (version, checksum) VALUES ($1, $2)`,
			migration.version,
			migration.checksum,
		); err != nil {
			_ = tx.Rollback()
			return newSchemaMigrationError(SchemaErrorApply, migration.version, err)
		}
		if err := tx.Commit(); err != nil {
			return newSchemaMigrationError(SchemaErrorApply, migration.version, err)
		}
		committed = true
	}

	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var unlocked bool
	if err := conn.QueryRowContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, PostgreSQLSchemaAdvisoryLockID).Scan(&unlocked); err != nil || !unlocked {
		return newSchemaMigrationError(SchemaErrorLock, 0, err)
	}
	locked = false
	return nil
}

// VerifySchema checks the complete local registry through an explicitly
// read-only transaction and never creates, stamps, or repairs schema state.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	registry := postgresqlMigrationRegistry()
	if err := validatePostgreSQLMigrationRegistry(registry); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return newSchemaMigrationError(SchemaErrorMalformed, 0, err)
	}
	defer func() { _ = tx.Rollback() }()
	exists, history, historyErr := readPostgreSQLMigrationHistory(ctx, tx)
	if historyErr != nil {
		return historyErr
	}
	if !exists {
		return newSchemaMigrationError(SchemaErrorMissing, 0, nil)
	}
	if err := validateAppliedPostgreSQLMigrations(history, registry); err != nil {
		return err
	}
	if len(history) < len(registry) {
		return newSchemaMigrationError(SchemaErrorBehind, registry[len(history)].version, nil)
	}
	if err := tx.Commit(); err != nil {
		return newSchemaMigrationError(SchemaErrorMalformed, 0, err)
	}
	return nil
}

func validatePostgreSQLMigrationRegistry(registry []postgresqlMigration) *SchemaMigrationError {
	if len(registry) == 0 {
		return newSchemaMigrationError(SchemaErrorMalformed, 0, nil)
	}
	seen := make(map[int64]struct{}, len(registry))
	for index, migration := range registry {
		if _, duplicate := seen[migration.version]; duplicate {
			return newSchemaMigrationError(SchemaErrorDuplicate, migration.version, nil)
		}
		seen[migration.version] = struct{}{}
		expected := int64(index + 1)
		if migration.version != expected {
			return newSchemaMigrationError(SchemaErrorGap, migration.version, nil)
		}
		if migration.checksum != checksumPostgreSQLSchemaSteps(migration.steps) {
			return newSchemaMigrationError(SchemaErrorChecksumDrift, migration.version, nil)
		}
	}
	return nil
}

func readPostgreSQLMigrationHistory(ctx context.Context, queryer postgresqlMigrationQueryer) (bool, []appliedPostgreSQLMigration, *SchemaMigrationError) {
	var exists bool
	if err := queryer.QueryRowContext(ctx, `SELECT to_regclass('tetral_schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return false, nil, newSchemaMigrationError(SchemaErrorMalformed, 0, err)
	}
	if !exists {
		return false, nil, nil
	}
	rows, err := queryer.QueryContext(ctx, `SELECT version, checksum FROM tetral_schema_migrations ORDER BY version`)
	if err != nil {
		return true, nil, newSchemaMigrationError(SchemaErrorMalformed, 0, err)
	}
	defer func() { _ = rows.Close() }()
	var history []appliedPostgreSQLMigration
	for rows.Next() {
		var migration appliedPostgreSQLMigration
		if err := rows.Scan(&migration.version, &migration.checksum); err != nil {
			return true, nil, newSchemaMigrationError(SchemaErrorMalformed, 0, err)
		}
		history = append(history, migration)
	}
	if err := rows.Err(); err != nil {
		return true, nil, newSchemaMigrationError(SchemaErrorMalformed, 0, err)
	}
	return true, history, nil
}

func validateAppliedPostgreSQLMigrations(history []appliedPostgreSQLMigration, registry []postgresqlMigration) *SchemaMigrationError {
	seen := make(map[int64]struct{}, len(history))
	for index, applied := range history {
		if _, duplicate := seen[applied.version]; duplicate {
			return newSchemaMigrationError(SchemaErrorDuplicate, applied.version, nil)
		}
		seen[applied.version] = struct{}{}
		expected := int64(index + 1)
		if applied.version != expected {
			return newSchemaMigrationError(SchemaErrorGap, applied.version, nil)
		}
		if index >= len(registry) {
			return newSchemaMigrationError(SchemaErrorAhead, applied.version, nil)
		}
		if applied.checksum != registry[index].checksum {
			return newSchemaMigrationError(SchemaErrorChecksumDrift, applied.version, nil)
		}
	}
	return nil
}

// checksumPostgreSQLSchemaSteps serializes the exact DDL bytes with an
// unambiguous big-endian length prefix per statement. Step labels are excluded:
// the migration identity is the ordered SQL payload itself.
func checksumPostgreSQLSchemaSteps(steps []postgresqlSchemaStep) string {
	hash := sha256.New()
	var length [8]byte
	for _, step := range steps {
		binary.BigEndian.PutUint64(length[:], uint64(len(step.ddl)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(step.ddl))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func newSchemaMigrationError(kind SchemaErrorKind, version int64, cause error) *SchemaMigrationError {
	cancellationCause := cause
	if initializationError, ok := cause.(*PostgreSQLSchemaError); ok {
		cancellationCause = initializationError.cause
	}
	if errors.Is(cancellationCause, context.Canceled) || errors.Is(cancellationCause, context.DeadlineExceeded) {
		kind = SchemaErrorCanceled
	}
	return &SchemaMigrationError{Kind: kind, Version: version, cause: cause}
}
