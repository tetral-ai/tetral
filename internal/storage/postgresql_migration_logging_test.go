package storage_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestMigrationLogsObserveRollbackAndRetryWithoutDriverDetails(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	_, err := db.Exec(`ALTER TABLE session_github_repository_resources
DROP CONSTRAINT session_github_repository_git_identity_shape,
DROP COLUMN git_identity_name, DROP COLUMN git_identity_email;
DELETE FROM tetral_schema_migrations WHERE version = 2;
CREATE FUNCTION reject_migration() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'private-migration-message' USING ERRCODE = '42501', DETAIL = 'private-migration-detail'; END $$;
CREATE EVENT TRIGGER reject_migration ON ddl_command_start WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION reject_migration()`)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	ctx := storage.WithMigrationLogger(context.Background(), slog.New(slog.NewJSONHandler(&logs, nil)))
	if err := storage.MigrateSchema(ctx, db); err == nil {
		t.Fatal("expected rejected DDL")
	}
	if strings.Contains(logs.String(), "private-migration") {
		t.Fatal("driver message or detail escaped into logs")
	}
	records := migrationLogRecords(t, &logs)
	if len(records) != 2 {
		t.Fatalf("records = %d; want start and failure", len(records))
	}
	failed := records[1]
	if failed["msg"] != "schema.migration.failed" || failed["schema.version"] != float64(2) || failed["schema.step"] != "add_git_identity_columns" || failed["db.sqlstate"] != "42501" || failed["transaction.outcome"] != "rolled_back" {
		t.Fatalf("failure record = %#v", failed)
	}
	var stamps, columns int
	if err := db.QueryRow(`SELECT count(*) FROM tetral_schema_migrations`).Scan(&stamps); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.columns WHERE table_name='session_github_repository_resources' AND column_name LIKE 'git_identity_%'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if stamps != 1 || columns != 0 {
		t.Fatalf("rollback left stamps=%d columns=%d", stamps, columns)
	}
	if _, err := db.Exec(`DROP EVENT TRIGGER reject_migration; DROP FUNCTION reject_migration()`); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	records = migrationLogRecords(t, &logs)
	if len(records) != 2 || records[1]["msg"] != "schema.migration.completed" || records[1]["transaction.outcome"] != "committed" || records[1]["schema.version"] != float64(2) {
		t.Fatalf("retry records = %#v", records)
	}
	if err := storage.VerifySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Fatal("up-to-date database logged another migration")
	}
}

// The database really commits; only the driver acknowledgement is replaced.
// This distinguishes an unknown outcome from falsely reporting a rollback.
func TestMigrationCommitAcknowledgementLossLogsUnknown(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	config, err := pgx.ParseConfig(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(lostCommitConnector{stdlib.GetConnector(*config)})
	defer func() { _ = db.Close() }()
	var logs bytes.Buffer
	ctx := storage.WithMigrationLogger(context.Background(), slog.New(slog.NewJSONHandler(&logs, nil)))
	if err := storage.MigrateSchema(ctx, db); err == nil {
		t.Fatal("expected lost acknowledgement")
	}
	if strings.Contains(logs.String(), "private-commit-response") {
		t.Fatal("driver error escaped into logs")
	}
	records := migrationLogRecords(t, &logs)
	if len(records) != 2 || records[1]["schema.step"] != "commit" || records[1]["transaction.outcome"] != "unknown" {
		t.Fatalf("records = %#v", records)
	}
	var stamps int
	if err := admin.QueryRow(`SELECT count(*) FROM tetral_schema_migrations WHERE version = 1`).Scan(&stamps); err != nil {
		t.Fatal(err)
	}
	if stamps != 1 {
		t.Fatal("fixture did not commit V1")
	}
	if err := storage.MigrateSchema(ctx, admin); err != nil {
		t.Fatalf("retry after reconnect: %v", err)
	}
	if err := storage.VerifySchema(ctx, admin); err != nil {
		t.Fatal(err)
	}
}

func migrationLogRecords(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	decoder := json.NewDecoder(logs)
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record["operation"] != "database.migrate" || record["duration_ms"].(float64) < 0 {
			t.Fatalf("invalid migration record: %#v", record)
		}
		records = append(records, record)
	}
	return records
}

type lostCommitConnector struct{ driver.Connector }

func (c lostCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return lostCommitConn{conn}, nil
}

type lostCommitConn struct{ driver.Conn }

func (c lostCommitConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c lostCommitConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c lostCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return lostCommitTx{tx}, nil
}

type lostCommitTx struct{ driver.Tx }

func (tx lostCommitTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	return errors.New("private-commit-response")
}
