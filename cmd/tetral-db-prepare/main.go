package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workload"
)

const adminDatabaseURLEnv = "TETRAL_DATABASE_ADMIN_URL"

func main() {
	if err := run(context.Background(), os.Getenv, os.Stdin, os.Stderr); err != nil {
		os.Exit(1)
	}
}

type preparationConnection struct {
	config  *pgx.ConnConfig
	refresh func(context.Context, *pgx.ConnConfig) error
	close   func() error
}

func protectedPreparationConnection(ctx context.Context, getenv func(string) string) (preparationConnection, error) {
	name := getenv("TETRAL_DATABASE_TLS_SERVER_NAME")
	config, owner, err := dbconnect.OpenProtectedConfig(ctx, getenv(adminDatabaseURLEnv), getenv("TETRAL_DATABASE_TLS_CA_PATH"), name)
	if err != nil {
		return preparationConnection{}, prepareFailure("PostgreSQL protected connection requires valid CA trust and server name", err)
	}
	return preparationConnection{config: config, close: owner.Close, refresh: func(_ context.Context, next *pgx.ConnConfig) error {
		var err error
		next.TLSConfig, err = owner.ClientTLSConfig(name, "")
		next.Fallbacks = nil
		return err
	}}, nil
}

func run(ctx context.Context, getenv func(string) string, input io.Reader, stderr io.Writer) error {
	return runWithConnection(ctx, getenv, input, stderr, protectedPreparationConnection)
}

func runWithConnection(ctx context.Context, getenv func(string) string, input io.Reader, stderr io.Writer, open func(context.Context, func(string) string) (preparationConnection, error)) (result error) {
	diagnostics, diagnosticErr := workload.DiagnosticConfigFromEnv(getenv)
	diagnosticOwner := workload.NewProcessLogger(stderr, "db-prepare", getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), getenv("TETRAL_SERVICE_VERSION"), diagnostics)
	defer diagnosticOwner.CloseWithBudget()
	logger := diagnosticOwner.Logger
	defer workload.InstallDefaultLogger(logger)()
	if diagnosticErr != nil {
		return workload.LogStartupFailure(logger, "db-prepare", diagnosticErr)
	}
	ctx = storage.WithMigrationLogger(ctx, logger)
	step := "validate_input"
	logger.Info("database.prepare.started")
	defer func() {
		if result != nil {
			workload.WriteFailure(ctx, logger, "database.prepare.failed", "prepare_failed", workload.CauseOf(result), "step", step, "prepare.problem", safeFailureMessage(result))
		} else {
			logger.Info("database.prepare.completed")
		}
	}()
	dsn := getenv(adminDatabaseURLEnv)
	if dsn == "" {
		return prepareError(adminDatabaseURLEnv + " is required")
	}
	var declarations database.RoleDeclarations
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&declarations); err != nil {
		return prepareError("PostgreSQL role declaration must be valid JSON with only supported fields")
	}
	if err := requireJSONEnd(decoder); err != nil {
		return err
	}
	if err := declarations.Validate(); err != nil {
		return err
	}
	_, err := pgx.ParseConfig(dsn)
	if err != nil {
		return prepareError("PostgreSQL administrative connection string is invalid")
	}
	step = "configure_transport"
	connectionOwner, err := open(ctx, getenv)
	if err != nil {
		return err
	}
	defer func() { _ = connectionOwner.close() }()
	config := connectionOwner.config
	step = "verify_admin"
	migrationDB := sql.OpenDB(stdlib.GetConnector(*config, stdlib.OptionBeforeConnect(connectionOwner.refresh)))
	defer func() { _ = migrationDB.Close() }()
	var superuser bool
	if err := migrationDB.QueryRowContext(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&superuser); err != nil {
		return prepareFailure("Could not verify PostgreSQL administrative privileges; check database connectivity and authentication", err)
	}
	if !superuser {
		return prepareError("PostgreSQL database preparation requires a superuser connection")
	}
	step = "migrate_schema"
	if err := storage.MigrateSchema(ctx, migrationDB); err != nil {
		return prepareError("PostgreSQL schema migration failed; see schema.migration.failed for details")
	}
	step = "close_migration_connection"
	if err := migrationDB.Close(); err != nil {
		return prepareFailure("Could not close PostgreSQL schema connection", err)
	}
	step = "connect_roles"
	if err := connectionOwner.refresh(ctx, config); err != nil {
		return prepareFailure("PostgreSQL trust refresh failed before role installation", err)
	}
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return prepareFailure("Could not connect PostgreSQL role installer; check database connectivity and authentication", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	step = "apply_roles"
	if err := database.ApplyRoleContract(ctx, connection, declarations); err != nil {
		var contractError *database.RoleContractError
		if errors.As(err, &contractError) {
			return contractError
		}
		return prepareError("PostgreSQL role installation failed; schema migrations may already be committed")
	}
	return nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	}
	return prepareError("PostgreSQL role declaration must contain one JSON value")
}

// Only explicitly safe error types may supply log messages. The role
// declaration, the connection string and the role statements hold passwords,
// so an error of their parsing or execution is never recorded.
type prepareError string

func (e prepareError) Error() string { return string(e) }

// connectionFailure is a failed step that sends no password in a statement:
// its record carries the driver's error as the cause.
type connectionFailure struct {
	prepareError
	cause error
}

func prepareFailure(message string, cause error) error {
	return connectionFailure{prepareError: prepareError(message), cause: cause}
}

func (e connectionFailure) Unwrap() error { return e.prepareError }

func (e connectionFailure) RecordedCause() error { return e.cause }

func safeFailureMessage(err error) string {
	var preparationError prepareError
	if errors.As(err, &preparationError) {
		return preparationError.Error()
	}
	var contractError *database.RoleContractError
	if errors.As(err, &contractError) {
		return contractError.Error()
	}
	return "PostgreSQL database preparation failed"
}
