package storage

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type migrationLoggerKey struct{}

// WithMigrationLogger routes migration records through the caller's logger,
// at the transaction owner before returning the safe error. It does not change
// the process-wide default logger or expose private driver errors.
func WithMigrationLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}
	return context.WithValue(ctx, migrationLoggerKey{}, logger)
}

type migrationDiagnostics struct {
	logger  *slog.Logger
	started time.Time
	version int64
	step    string
	outcome string
}

func newMigrationDiagnostics(ctx context.Context) *migrationDiagnostics {
	logger, _ := ctx.Value(migrationLoggerKey{}).(*slog.Logger)
	if logger == nil {
		logger = slog.Default()
	}
	return &migrationDiagnostics{logger: logger, started: time.Now(), step: "validate_registry", outcome: "not_started"}
}

func (d *migrationDiagnostics) attrs() []any {
	attrs := []any{
		slog.String("operation", "database.migrate"),
		slog.String("schema.step", d.step),
		slog.String("transaction.outcome", d.outcome),
		slog.Int64("duration_ms", time.Since(d.started).Milliseconds()),
	}
	if d.version > 0 {
		attrs = append(attrs, slog.Int64("schema.version", d.version))
	}
	return attrs
}

func (d *migrationDiagnostics) start(ctx context.Context, version int64) {
	d.version, d.started, d.step, d.outcome = version, time.Now(), "begin_transaction", "not_started"
	d.logger.InfoContext(ctx, "schema.migration.started", d.attrs()...)
}

func (d *migrationDiagnostics) completed(ctx context.Context) {
	d.logger.InfoContext(ctx, "schema.migration.completed", d.attrs()...)
}

func (d *migrationDiagnostics) failed(ctx context.Context, err error) {
	var migrationError *SchemaMigrationError
	if !errors.As(err, &migrationError) {
		migrationError = newSchemaMigrationError(SchemaErrorApply, d.version, nil)
	}
	if migrationError.Version > 0 {
		d.version = migrationError.Version
	}
	attrs := append(d.attrs(),
		slog.String("error.class", string(migrationError.Kind)),
		slog.String("error.code", string(migrationError.Kind)),
		slog.String("error.message_safe", migrationError.Error()),
	)
	// Unwrap only inside storage. Never serialize Message, Detail, Hint, SQL,
	// identifiers, or connection data from the PostgreSQL driver error.
	cause := migrationError.cause
	if stepError, ok := cause.(*PostgreSQLSchemaError); ok {
		cause = stepError.cause
	}
	var pgError *pgconn.PgError
	if errors.As(cause, &pgError) && validMigrationSQLState(pgError.Code) {
		attrs = append(attrs, slog.String("db.sqlstate", pgError.Code))
	}
	d.logger.ErrorContext(ctx, "schema.migration.failed", attrs...)
}

func validMigrationSQLState(code string) bool {
	if len(code) != 5 {
		return false
	}
	for _, c := range code {
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}
