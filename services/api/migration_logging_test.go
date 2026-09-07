package tetralapi

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestAPIStartupRetainsMigrationDiagnosticsBeforeAdapterRedaction(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	if _, err := admin.Exec(`CREATE FUNCTION reject_migration() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'private-api-migration-message' USING ERRCODE = '42501', DETAIL = 'private-api-migration-detail'; END $$;
CREATE EVENT TRIGGER reject_migration ON ddl_command_start WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION reject_migration()`); err != nil {
		t.Fatal(err)
	}
	dsn := storagetest.AdminDatabaseURL(t, admin)
	var logs bytes.Buffer
	err := Run(context.Background(), migrationLogEnv{dataDir: t.TempDir()}, &logs,
		func(ctx context.Context, cfg ProductionConfig) (*Application, error) {
			cfg.Open = func(ctx context.Context) (StartupDatabase, error) {
				runtimeDB, err := dbconnect.OpenPlainDSN(ctx, "TETRAL_DATABASE_URL", dsn)
				if err != nil {
					return StartupDatabase{}, err
				}
				migrationDB, err := dbconnect.OpenPlainDSN(ctx, "TETRAL_MIGRATION_DATABASE_URL", dsn)
				if err != nil {
					_ = runtimeDB.Client.Close()
					return StartupDatabase{}, err
				}
				return StartupDatabase{OpenResult: runtimeDB, RuntimeClient: runtimeDB.Client, MigrationClient: migrationDB.Client}, nil
			}
			return BuildProductionApplication(ctx, cfg)
		}, func(context.Context, workload.Config) error {
			t.Fatal("serving started after migration failure")
			return nil
		})
	if err == nil {
		t.Fatal("expected migration failure")
	}
	if strings.Contains(logs.String(), "private-api-migration") || strings.Contains(logs.String(), dsn) {
		t.Fatal("private database detail escaped")
	}
	var failure, startup bool
	decoder := json.NewDecoder(&logs)
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] == "schema.migration.failed" {
			failure = record["service.name"] == "api" && record["schema.version"] == float64(1) && record["schema.step"] == "create_history" && record["db.sqlstate"] == "42501" && record["transaction.outcome"] == "rolled_back"
		}
		startup = startup || record["msg"] == "startup.failed"
	}
	if !failure || !startup {
		t.Fatalf("migration diagnostic=%t startup failure=%t", failure, startup)
	}
}

type migrationLogEnv struct{ dataDir string }

func (e migrationLogEnv) Getenv(key string) string {
	switch key {
	case "ENGINE_VAULT_KEY":
		return strings.Repeat("a", 64)
	case "ENGINE_DATA_DIR":
		return e.dataDir
	default:
		return ""
	}
}
