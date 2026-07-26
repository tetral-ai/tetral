package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestBootstrapWorkspaceIDValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing", args: nil},
		{name: "empty", args: []string{"--workspace-id", ""}},
		{name: "whitespace only", args: []string{"--workspace-id", " \t"}},
		{name: "too many bytes", args: []string{"--workspace-id", strings.Repeat("a", workspace.MaxWorkspaceIDBytes+1)}},
		{name: "slash", args: []string{"--workspace-id", "team/one"}},
		{name: "space", args: []string{"--workspace-id", "team one"}},
		{name: "newline", args: []string{"--workspace-id", "team\none"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseBootstrapConfig(tc.args); err == nil {
				t.Fatalf("parseBootstrapConfig(%q) unexpectedly succeeded", tc.args)
			}
		})
	}
}

func TestBootstrapWorkspaceRequiresMigratedSchema(t *testing.T) {
	adminDB := storagetest.NewEmptyPostgreSQLAdminDB(t)
	var output bytes.Buffer

	err := seedBootstrapWorkspace(context.Background(), adminDB, bootstrapConfig{
		workspaceID: workspace.ID("schema-first"),
		name:        "Schema First",
	}, &output)

	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorMissing {
		t.Fatalf("seedBootstrapWorkspace error = %T %v; want missing-schema error", err, err)
	}
	if output.Len() != 0 {
		t.Fatalf("seedBootstrapWorkspace output = %q; want no success output", output.String())
	}
}

func TestBootstrapWorkspaceIsIdempotentAndWarnsForLongIDs(t *testing.T) {
	ctx := context.Background()
	adminDB := storagetest.NewEmptyPostgreSQLAdminDB(t)
	if err := storage.MigrateSchema(ctx, adminDB); err != nil {
		t.Fatalf("migrate schema: %v", err)
	}
	id := workspace.ID("workspace-id-over-20x")
	config := bootstrapConfig{workspaceID: id, name: string(id)}
	var first bytes.Buffer
	if err := seedBootstrapWorkspace(ctx, adminDB, config, &first); err != nil {
		t.Fatalf("first seed: %v", err)
	}
	if got := first.String(); !strings.Contains(got, "warning:") || !strings.Contains(got, "snapshot-name budget") || !strings.Contains(got, "created") {
		t.Fatalf("first seed output = %q; want snapshot warning and created status", got)
	}

	var second bytes.Buffer
	if err := seedBootstrapWorkspace(ctx, adminDB, config, &second); err != nil {
		t.Fatalf("second seed: %v", err)
	}
	if got := second.String(); !strings.Contains(got, "warning:") || !strings.Contains(got, "already present") {
		t.Fatalf("second seed output = %q; want snapshot warning and existing status", got)
	}

	var count int
	var name string
	if err := adminDB.QueryRowContext(ctx,
		`SELECT count(*), min(name) FROM workspaces WHERE id = $1`,
		id,
	).Scan(&count, &name); err != nil {
		t.Fatalf("read seeded workspace: %v", err)
	}
	if count != 1 || name != string(id) {
		t.Fatalf("workspace row count/name = %d/%q; want 1/%q", count, name, id)
	}
}
