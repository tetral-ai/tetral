package tetralapi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

func TestPrepareStartupDatabaseMigratesAndVerifiesRuntimeRole(t *testing.T) {
	events := []string{}
	runtimeClient := &recordingRuntimeStartupClient{events: &events}
	migrationClient := &recordingMigrationStartupClient{events: &events}
	_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{RuntimeClient: runtimeClient, MigrationClient: migrationClient}, nil
	})
	if err != nil {
		t.Fatalf("PrepareStartupDatabase: %v", err)
	}
	if strings.Join(events, ",") != "migrate,migration_close,schema_verify,role_verify" {
		t.Fatalf("startup events = %v", events)
	}
}

func TestPrepareStartupDatabaseFailuresStopBeforeNextStep(t *testing.T) {
	migrateErr := errors.New("migrate failed")
	verifyErr := errors.New("verify failed")
	closeErr := errors.New("close failed")
	for _, test := range []struct {
		name            string
		migrationError  error
		migrationClose  error
		schemaVerifyErr error
		roleVerifyErr   error
		wantEvents      string
	}{
		{name: "migrate", migrationError: migrateErr, wantEvents: "migrate,migration_close,runtime_close"},
		{name: "migration close", migrationClose: closeErr, wantEvents: "migrate,migration_close,runtime_close"},
		{name: "verify schema", schemaVerifyErr: verifyErr, wantEvents: "migrate,migration_close,schema_verify,runtime_close"},
		{name: "verify role", roleVerifyErr: verifyErr, wantEvents: "migrate,migration_close,schema_verify,role_verify,runtime_close"},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
				return StartupDatabase{
					RuntimeClient:   &recordingRuntimeStartupClient{events: &events, schemaVerifyErr: test.schemaVerifyErr, roleVerifyErr: test.roleVerifyErr},
					MigrationClient: &recordingMigrationStartupClient{events: &events, migrateErr: test.migrationError, closeErr: test.migrationClose},
				}, nil
			})
			if err == nil {
				t.Fatal("PrepareStartupDatabase returned nil error")
			}
			if strings.Join(events, ",") != test.wantEvents {
				t.Fatalf("startup events = %v; want %s", events, test.wantEvents)
			}
		})
	}
}

func TestPrepareStartupDatabaseClosesTheSurvivingClientWhenThePairIsIncomplete(t *testing.T) {
	for _, test := range []struct {
		name       string
		database   func(*[]string) StartupDatabase
		wantEvents string
	}{
		{
			name: "runtime missing",
			database: func(events *[]string) StartupDatabase {
				return StartupDatabase{MigrationClient: &recordingMigrationStartupClient{events: events}}
			},
			wantEvents: "migration_close",
		},
		{
			name: "migration missing",
			database: func(events *[]string) StartupDatabase {
				return StartupDatabase{RuntimeClient: &recordingRuntimeStartupClient{events: events}}
			},
			wantEvents: "runtime_close",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			if _, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
				return test.database(&events), nil
			}); err == nil {
				t.Fatal("PrepareStartupDatabase accepted an incomplete client pair")
			}
			if strings.Join(events, ",") != test.wantEvents {
				t.Fatalf("startup events = %v; want %s", events, test.wantEvents)
			}
		})
	}
}

func TestPrepareStartupDatabaseRejectsNilReadinessClient(t *testing.T) {
	_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{}, nil
	})
	if err == nil {
		t.Fatal("PrepareStartupDatabase returned nil error for nil readiness client")
	}
	if !strings.Contains(err.Error(), "runtime database client is required") {
		t.Fatalf("PrepareStartupDatabase error = %q; want nil-client message", err.Error())
	}
}

func TestBuildProductionApplicationValidatesPublicAPIConfigBeforeOpeningDatabase(t *testing.T) {
	openCalled := false
	_, err := BuildProductionApplication(context.Background(), ProductionConfig{
		VaultKey: "short",
		DataDir:  t.TempDir(),
		Open: func(context.Context) (StartupDatabase, error) {
			openCalled = true
			return StartupDatabase{}, nil
		},
	})
	if err == nil {
		t.Fatal("BuildProductionApplication returned nil error for invalid public API config")
	}
	if openCalled {
		t.Fatal("BuildProductionApplication opened the database before validating public API config")
	}
}

func TestBuildProductionApplicationRunsEnvironmentNetworkingPreflightBeforeRouter(t *testing.T) {
	preflightErr := errors.New("environment networking preflight failed")
	events := []string{}
	preflightCalled := false
	_, err := BuildProductionApplication(context.Background(), ProductionConfig{
		VaultKey: strings.Repeat("01", 32),
		DataDir:  t.TempDir(),
		Open: func(context.Context) (StartupDatabase, error) {
			return StartupDatabase{RuntimeClient: &recordingRuntimeStartupClient{events: &events}, MigrationClient: &recordingMigrationStartupClient{events: &events}}, nil
		},
		EnvironmentNetworkPreflight: func(context.Context, *dbconnect.Client) error {
			preflightCalled = true
			return preflightErr
		},
	})
	if !errors.Is(err, preflightErr) {
		t.Fatalf("BuildProductionApplication error = %v; want preflight error", err)
	}
	if !preflightCalled {
		t.Fatal("BuildProductionApplication did not run environment networking preflight")
	}
	if strings.Join(events, ",") != "migrate,migration_close,schema_verify,role_verify" {
		t.Fatalf("startup events = %v; want migration and runtime verification before preflight", events)
	}
}

type recordingMigrationStartupClient struct {
	events     *[]string
	migrateErr error
	closeErr   error
}

func (c *recordingMigrationStartupClient) MigrateSchema(context.Context) error {
	*c.events = append(*c.events, "migrate")
	return c.migrateErr
}

func (c *recordingMigrationStartupClient) Close() error {
	*c.events = append(*c.events, "migration_close")
	return c.closeErr
}

type recordingRuntimeStartupClient struct {
	events          *[]string
	schemaVerifyErr error
	roleVerifyErr   error
}

func (c *recordingRuntimeStartupClient) VerifySchema(context.Context) error {
	*c.events = append(*c.events, "schema_verify")
	return c.schemaVerifyErr
}

func (c *recordingRuntimeStartupClient) VerifyRuntimeRole(context.Context) error {
	*c.events = append(*c.events, "role_verify")
	return c.roleVerifyErr
}

func (c *recordingRuntimeStartupClient) Close() error {
	*c.events = append(*c.events, "runtime_close")
	return nil
}
