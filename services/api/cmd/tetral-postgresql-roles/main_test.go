package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestRunConstructsFreshSchemaBeforeInstallingRoles(t *testing.T) {
	admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	declarations := database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}
	for index, workload := range append(contract.WorkloadNames(), contract.MigrationOwner) {
		declarations.Roles[workload] = database.RoleCredential{
			Name:     fmt.Sprintf("tetral_installer_%d_%d", time.Now().UnixNano(), index),
			Password: "installer-test-password-" + workload,
		}
	}
	defer cleanupInstalledRoles(t, admin, declarations)
	payload, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	getenv := func(name string) string {
		if name != adminDatabaseURLEnv {
			return ""
		}
		return storagetest.AdminDatabaseURL(t, admin)
	}
	if err := run(context.Background(), getenv, bytes.NewReader(payload)); err != nil {
		t.Fatalf("install roles on empty database: %v", err)
	}

	var tables, stamps int
	if err := admin.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema='public'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM tetral_schema_migrations`).Scan(&stamps); err != nil {
		t.Fatal(err)
	}
	if tables == 0 || stamps != 1 {
		t.Fatalf("installed catalog tables=%d stamps=%d; want nonempty catalog and one stamp", tables, stamps)
	}
}

func cleanupInstalledRoles(t *testing.T, admin queryExecer, declarations database.RoleDeclarations) {
	t.Helper()
	var owner string
	if err := admin.QueryRow(`SELECT current_user`).Scan(&owner); err != nil {
		t.Errorf("read test database owner: %v", err)
		return
	}
	migration := declarations.Roles["migration"].Name
	if _, err := admin.Exec("REASSIGN OWNED BY " + pgx.Identifier{migration}.Sanitize() + " TO " + pgx.Identifier{owner}.Sanitize()); err != nil {
		t.Errorf("restore schema ownership: %v", err)
		return
	}
	for _, declaration := range declarations.Roles {
		if _, err := admin.Exec("DROP OWNED BY " + pgx.Identifier{declaration.Name}.Sanitize()); err != nil {
			t.Errorf("remove managed role grants: %v", err)
			return
		}
	}
	control, err := pgx.Connect(context.Background(), os.Getenv(storagetest.EnvTestDatabaseURL))
	if err != nil {
		t.Errorf("connect test control database: %v", err)
		return
	}
	defer func() { _ = control.Close(context.Background()) }()
	for _, declaration := range declarations.Roles {
		if _, err := control.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{declaration.Name}.Sanitize()); err != nil {
			t.Errorf("drop managed role: %v", err)
			return
		}
	}
}

type queryExecer interface {
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

func TestRunRejectsTrailingRoleDeclaration(t *testing.T) {
	err := run(context.Background(), func(string) string { return "unused" }, bytes.NewBufferString(`{"roles":{}} {}`))
	if err == nil {
		t.Fatal("accepted more than one role declaration")
	}
}
