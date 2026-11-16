package tetralapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestAPIStartupRejectsUnpreparedDatabaseWithoutChangingSchema(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	dsn := storagetest.AdminDatabaseURL(t, admin)
	var logs bytes.Buffer
	err := Run(context.Background(), startupDatabaseEnv{dataDir: t.TempDir()}, &logs,
		func(ctx context.Context, cfg ProductionConfig) (*Application, error) {
			cfg.Open = func(ctx context.Context) (StartupDatabase, error) {
				runtimeDB, err := dbconnect.OpenPlainDSN(ctx, "TETRAL_DATABASE_URL", dsn)
				if err != nil {
					return StartupDatabase{}, err
				}
				return StartupDatabase{OpenResult: runtimeDB, RuntimeClient: runtimeDB.Client}, nil
			}
			return BuildProductionApplication(ctx, cfg)
		}, func(context.Context, workload.Config) error {
			t.Fatal("serving started with an unprepared database")
			return nil
		})
	var diagnostic *dbconnect.DiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic.Phase != dbconnect.PhaseVerifySchema {
		t.Fatalf("expected schema verification failure, got %v", err)
	}
	if strings.Contains(logs.String(), dsn) {
		t.Fatal("private database detail escaped")
	}
	var tables int
	if err := admin.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema='public'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("API created %d tables in an unprepared database", tables)
	}
	var startup []map[string]any
	decoder := json.NewDecoder(&logs)
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] == "startup.failed" {
			startup = append(startup, record)
		}
	}
	const cause = "schema verification: postgresql runtime client: provider=plain_dsn phase=verify_schema kind=schema_verification_failed: postgresql schema registry is missing"
	if len(startup) != 1 || startup[0]["error.message"] != cause {
		t.Fatalf("startup.failed records = %v; want one with the verification failure as its cause", startup)
	}
}

type startupDatabaseEnv struct{ dataDir string }

func (e startupDatabaseEnv) Getenv(key string) string {
	switch key {
	case "ENGINE_VAULT_KEY":
		return strings.Repeat("a", 64)
	case "ENGINE_DATA_DIR":
		return e.dataDir
	default:
		return ""
	}
}
