package tetralapi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestPrepareStartupDatabaseVerifiesSchemaAndRuntimeRole(t *testing.T) {
	events := []string{}
	runtimeClient := &recordingRuntimeStartupClient{events: &events}
	_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{RuntimeClient: runtimeClient}, nil
	})
	if err != nil {
		t.Fatalf("PrepareStartupDatabase: %v", err)
	}
	if strings.Join(events, ",") != "schema_verify,role_verify" {
		t.Fatalf("startup events = %v", events)
	}
}

func TestPrepareStartupDatabaseFailuresStopBeforeNextStep(t *testing.T) {
	verifyErr := errors.New("verify failed")
	for _, test := range []struct {
		name            string
		schemaVerifyErr error
		roleVerifyErr   error
		wantEvents      string
	}{
		{name: "verify schema", schemaVerifyErr: verifyErr, wantEvents: "schema_verify,runtime_close"},
		{name: "verify role", roleVerifyErr: verifyErr, wantEvents: "schema_verify,role_verify,runtime_close"},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			_, err := PrepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
				return StartupDatabase{
					RuntimeClient: &recordingRuntimeStartupClient{events: &events, schemaVerifyErr: test.schemaVerifyErr, roleVerifyErr: test.roleVerifyErr},
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

// The preflight runs after the runtime checks and before the router, and its
// failure names what failed: rows this version cannot read, or the query.
func TestBuildProductionApplicationRunsEnvironmentNetworkingPreflightBeforeRouter(t *testing.T) {
	for _, row := range []struct {
		err   error
		cause string
	}{
		{err: &environment.LegacyNetworkingConfigError{}, cause: "schema"},
		{err: errors.New("connection reset while reading environments"), cause: "dependency_readiness"},
	} {
		t.Run(row.cause, func(t *testing.T) {
			events := []string{}
			_, err := BuildProductionApplication(context.Background(), ProductionConfig{
				VaultKey: strings.Repeat("01", 32),
				DataDir:  t.TempDir(),
				Open: func(context.Context) (StartupDatabase, error) {
					return StartupDatabase{RuntimeClient: &recordingRuntimeStartupClient{events: &events}}, nil
				},
				EnvironmentNetworkPreflight: func(context.Context, *dbconnect.Client) error {
					events = append(events, "preflight")
					return row.err
				},
			})
			if !errors.Is(err, row.err) {
				t.Fatalf("BuildProductionApplication error = %v; want the preflight error", err)
			}
			if strings.Join(events, ",") != "schema_verify,role_verify,preflight" {
				t.Fatalf("startup events = %v; want runtime verification, then the preflight", events)
			}
			var record bytes.Buffer
			_ = workload.LogStartupFailure(slog.New(slog.NewJSONHandler(&record, nil)), "api", err)
			if want := `"startup.cause":"` + row.cause + `"`; !strings.Contains(record.String(), want) {
				t.Fatalf("startup record = %s; want %s", record.String(), want)
			}
		})
	}
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
