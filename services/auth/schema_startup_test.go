package tetralauth

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
)

func TestPrepareStartupDatabaseVerifiesSchemaBeforeRuntimeRole(t *testing.T) {
	client := &recordingSchemaStartupClient{}
	_, err := prepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{
			OpenResult: dbconnect.OpenResult{RawDatabaseForExcludedStores: new(sql.DB)},
			Client:     client,
		}, nil
	})
	if err != nil {
		t.Fatalf("prepareStartupDatabase: %v", err)
	}
	if got := strings.Join(client.events, ","); got != "schema_verify,role_verify" {
		t.Fatalf("events = %q, want schema_verify,role_verify", got)
	}
}

func TestPrepareStartupDatabaseSchemaBehindStopsBeforeRuntimeRoleAndBootstrap(t *testing.T) {
	behind := &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	client := &recordingSchemaStartupClient{schemaErr: behind}
	_, err := prepareStartupDatabase(context.Background(), func(context.Context) (StartupDatabase, error) {
		return StartupDatabase{
			OpenResult: dbconnect.OpenResult{RawDatabaseForExcludedStores: new(sql.DB)},
			Client:     client,
		}, nil
	})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("error = %v, want schema-behind", err)
	}
	if got := strings.Join(client.events, ","); got != "schema_verify" {
		t.Fatalf("events = %q, want schema_verify only", got)
	}
}

type recordingSchemaStartupClient struct {
	events    []string
	schemaErr error
	roleErr   error
}

func (c *recordingSchemaStartupClient) VerifySchema(context.Context) error {
	c.events = append(c.events, "schema_verify")
	return c.schemaErr
}

func (c *recordingSchemaStartupClient) VerifyRuntimeRole(context.Context) error {
	c.events = append(c.events, "role_verify")
	return c.roleErr
}
