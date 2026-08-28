package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage"
)

const adminDatabaseURLEnv = "TETRAL_DATABASE_ADMIN_URL"

func main() {
	if err := run(context.Background(), os.Getenv, os.Stdin); err != nil {
		slog.Error("postgresql_role_contract_failed")
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, input io.Reader) error {
	dsn := getenv(adminDatabaseURLEnv)
	if dsn == "" {
		return fmt.Errorf("%s is required", adminDatabaseURLEnv)
	}
	var declarations database.RoleDeclarations
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&declarations); err != nil {
		return err
	}
	if err := requireJSONEnd(decoder); err != nil {
		return err
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse PostgreSQL administrative connection: %w", err)
	}
	migrationDB := sql.OpenDB(stdlib.GetConnector(*config))
	if err := storage.MigrateSchema(ctx, migrationDB); err != nil {
		_ = migrationDB.Close()
		return fmt.Errorf("construct PostgreSQL schema: %w", err)
	}
	if err := migrationDB.Close(); err != nil {
		return fmt.Errorf("close PostgreSQL schema connection: %w", err)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL role installer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	return database.ApplyRoleContract(ctx, connection, declarations)
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("PostgreSQL role declaration must contain one JSON value")
}
