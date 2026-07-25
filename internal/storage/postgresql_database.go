package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// envProductionDatabaseURL names the production/runtime PostgreSQL DSN.
// Service-local workload commands require it during startup.
const envProductionDatabaseURL = "TETRAL_DATABASE_URL"

// PostgreSQLConnectionError is the storage-local origin error for the
// PostgreSQL connection path. Error() text is public-safe: it names the
// configured env var and a sanitized host/database summary derived
// through redactPostgreSQLDSN. Raw driver errors and the original DSN
// are not exposed here — callers that want them must pass internal logs
// through their own redactor first.
type PostgreSQLConnectionError struct {
	EnvVarName string
	Stage      string
	HostHint   string
	DBHint     string
}

func (e *PostgreSQLConnectionError) Error() string {
	switch e.Stage {
	case stageMissingDSN:
		return fmt.Sprintf("postgresql: %s is not set; provide a real PostgreSQL DSN", e.EnvVarName)
	case stageParse:
		return fmt.Sprintf("postgresql: parse %s failed (host=%s database=%s)", e.EnvVarName, e.HostHint, e.DBHint)
	case stageOpen:
		return fmt.Sprintf("postgresql: open %s failed (host=%s database=%s)", e.EnvVarName, e.HostHint, e.DBHint)
	case stagePing:
		return fmt.Sprintf("postgresql: ping %s failed (host=%s database=%s)", e.EnvVarName, e.HostHint, e.DBHint)
	case stageMigrate:
		return fmt.Sprintf("postgresql: migrate %s failed (host=%s database=%s)", e.EnvVarName, e.HostHint, e.DBHint)
	default:
		return fmt.Sprintf("postgresql: %s failed for %s (host=%s database=%s)", e.Stage, e.EnvVarName, e.HostHint, e.DBHint)
	}
}

const (
	stageMissingDSN = "missing_dsn"
	stageParse      = "parse"
	stageOpen       = "open"
	stagePing       = "ping"
	stageMigrate    = "migrate"
)

// hostHintUnknown / dbHintUnknown are the placeholders used when DSN
// parsing failed, so the public error message degrades to
// `host=<unknown> database=<unknown>` instead of leaking the raw DSN.
const (
	hostHintUnknown = "<unknown>"
	dbHintUnknown   = "<unknown>"
)

// OpenPostgreSQLDatabaseFromEnv reads TETRAL_DATABASE_URL and opens a
// PostgreSQL connection through pgx's database/sql adapter. It does not
// fall back to the test variable.
func OpenPostgreSQLDatabaseFromEnv(ctx context.Context) (*sql.DB, error) {
	return OpenPostgreSQLDatabase(ctx, envProductionDatabaseURL, os.Getenv(envProductionDatabaseURL))
}

// OpenPostgreSQLDatabase opens a PostgreSQL connection from the given
// DSN, verifying connectivity with PingContext before returning. The
// caller's TLS settings are honored as parsed by pgx; this function
// never overrides sslmode and never derives a database location from
// ENGINE_DATA_DIR.
func OpenPostgreSQLDatabase(ctx context.Context, envVarName string, dsn string) (*sql.DB, error) {
	if dsn == "" {
		return nil, &PostgreSQLConnectionError{
			EnvVarName: envVarName,
			Stage:      stageMissingDSN,
		}
	}

	cfg, err := configurePostgreSQLConnection(dsn)
	if err != nil {
		return nil, &PostgreSQLConnectionError{
			EnvVarName: envVarName,
			Stage:      stageParse,
			HostHint:   hostHintUnknown,
			DBHint:     dbHintUnknown,
		}
	}

	connector := stdlib.GetConnector(*cfg)
	db := sql.OpenDB(connector)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		host, database := safeHostAndDatabase(cfg)
		return nil, &PostgreSQLConnectionError{
			EnvVarName: envVarName,
			Stage:      stagePing,
			HostHint:   host,
			DBHint:     database,
		}
	}

	return db, nil
}

// configurePostgreSQLConnection parses dsn through pgx and returns the
// resolved connection config. Exposed for tests so the proof can verify
// that secure sslmode settings (require, verify-full, verify-ca) survive
// without being downgraded.
func configurePostgreSQLConnection(dsn string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("parse failed")
	}
	return cfg, nil
}

// safeHostAndDatabase returns the host and database fields from a parsed
// pgx config. Both fields come from pgx's parsed structure and are safe
// to surface in public error text — passwords/userinfo/query-string
// secrets are stripped during parsing.
func safeHostAndDatabase(cfg *pgx.ConnConfig) (host string, database string) {
	host = cfg.Host
	if host == "" {
		host = hostHintUnknown
	}
	database = cfg.Database
	if database == "" {
		database = dbHintUnknown
	}
	return host, database
}
