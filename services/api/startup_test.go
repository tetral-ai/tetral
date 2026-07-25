package tetralapi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

func TestPrepareStartupDatabaseMigratesAndVerifiesRuntimeRole(t *testing.T) {
	client := &recordingStartupClient{}
	_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{Client: client}, nil
	})
	if err != nil {
		t.Fatalf("PrepareStartupDatabase: %v", err)
	}
	if strings.Join(client.events, ",") != "migrate,role_verify" {
		t.Fatalf("startup events = %v; want migrate,role_verify", client.events)
	}
}

func TestPrepareStartupDatabaseFailuresStopBeforeNextStep(t *testing.T) {
	migrateErr := errors.New("migrate failed")
	verifyErr := errors.New("verify failed")
	for _, test := range []struct {
		name       string
		client     *recordingStartupClient
		wantEvents string
	}{
		{name: "migrate", client: &recordingStartupClient{migrateErr: migrateErr}, wantEvents: "migrate"},
		{name: "verify role", client: &recordingStartupClient{verifyErr: verifyErr}, wantEvents: "migrate,role_verify"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
				return StartupDatabase{Client: test.client}, nil
			})
			if err == nil {
				t.Fatal("PrepareStartupDatabase returned nil error")
			}
			if strings.Join(test.client.events, ",") != test.wantEvents {
				t.Fatalf("startup events = %v; want %s", test.client.events, test.wantEvents)
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
	client := &recordingStartupClient{}
	preflightCalled := false
	_, err := BuildProductionApplication(context.Background(), ProductionConfig{
		VaultKey: strings.Repeat("01", 32),
		DataDir:  t.TempDir(),
		Open: func(context.Context) (StartupDatabase, error) {
			return StartupDatabase{Client: client}, nil
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
	if strings.Join(client.events, ",") != "migrate,role_verify" {
		t.Fatalf("startup events = %v; want migrate,role_verify before preflight", client.events)
	}
}

type recordingStartupClient struct {
	events     []string
	migrateErr error
	verifyErr  error
}

func (c *recordingStartupClient) MigrateSchema(context.Context) error {
	c.events = append(c.events, "migrate")
	return c.migrateErr
}

func (c *recordingStartupClient) VerifyRuntimeRole(context.Context) error {
	c.events = append(c.events, "role_verify")
	return c.verifyErr
}
