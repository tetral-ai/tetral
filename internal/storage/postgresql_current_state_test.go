package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgreSQLVersionOneCurrentStateConvergesOnSecondApplication(t *testing.T) {
	ctx := context.Background()
	config, err := pgx.ParseConfig(os.Getenv("TETRAL_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("TETRAL_TEST_DATABASE_URL must be a PostgreSQL administrative DSN")
	}
	control := stdlib.OpenDB(*config)
	defer func() { _ = control.Close() }()
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "tetral_current_state_" + hex.EncodeToString(suffix)
	if _, err := control.ExecContext(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = control.ExecContext(context.Background(), `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, name)
		_, _ = control.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize())
	}()
	config.Database = name
	db := stdlib.OpenDB(*config)
	defer func() { _ = db.Close() }()
	for attempt := 0; attempt < 2; attempt++ {
		if err := executePostgreSQLSchemaSteps(ctx, db, postgresqlBaselineSteps()); err != nil {
			t.Fatalf("apply current Version 1 state attempt %d: %v", attempt+1, err)
		}
	}
}
