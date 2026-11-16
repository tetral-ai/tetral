package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestVerifySchemaRejectsLiveRLSCatalogDrift(t *testing.T) {
	for _, drift := range storagetest.RLSCatalogDrifts(t) {
		t.Run(drift.Name, func(t *testing.T) {
			runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
			if err := storage.VerifySchema(context.Background(), runtimeDB); err != nil {
				t.Fatalf("baseline VerifySchema: %v", err)
			}
			drift.Apply(t, runtimeDB, adminDB)
			err := storage.VerifySchema(context.Background(), runtimeDB)
			var schemaErr *storage.SchemaMigrationError
			if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorRLSDrift {
				t.Fatalf("VerifySchema error = %T %v; want schema_rls_drift", err, err)
			}
		})
	}
}

func TestVerifySchemaRejectsWorkspaceTableOutsideContract(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	if _, err := db.Exec(`CREATE TABLE accidental_tenant_data (workspace_id text NOT NULL, id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err := storage.VerifySchema(context.Background(), db)
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorRLSDrift {
		t.Fatalf("VerifySchema error = %T %v; want schema_rls_drift", err, err)
	}
}
