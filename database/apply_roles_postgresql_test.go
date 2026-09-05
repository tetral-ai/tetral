package database_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLRoleContractIsIdempotentAndLeastPrivilege(t *testing.T) {
	t.Run("database", func(t *testing.T) {
		_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		var databaseName, cloneOwner, cloneRuntimeRole string
		if err := admin.QueryRow(`SELECT current_database(), current_user`).Scan(&databaseName, &cloneOwner); err != nil {
			t.Fatal(err)
		}
		if err := admin.QueryRow(`SELECT rolname FROM pg_roles WHERE shobj_description(oid, 'pg_authid') LIKE 'tetral-test-role:%:' || current_database()`).Scan(&cloneRuntimeRole); err != nil {
			t.Fatal(err)
		}
		roleContract, err := database.LoadRoleContract()
		if err != nil {
			t.Fatal(err)
		}
		declarations := database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}
		names := append(roleContract.WorkloadNames(), roleContract.MigrationOwner)
		for index, workload := range names {
			name := fmt.Sprintf("tetral_contract_%d_%d", time.Now().UnixNano(), index)
			declarations.Roles[workload] = database.RoleCredential{Name: name, Password: "contract-password-" + workload}
		}
		for attempt := 0; attempt < 2; attempt++ {
			if err := withPGXConnection(t, admin, func(connection *pgx.Conn) error {
				return database.ApplyRoleContract(context.Background(), connection, declarations)
			}); err != nil {
				t.Fatalf("apply role contract attempt %d: %v", attempt+1, err)
			}
			if attempt == 0 {
				defer restoreCloneRoleBoundary(t, admin, cloneOwner, cloneRuntimeRole, declarations)
				assertExactWorkloadPrivileges(t, admin, roleContract, declarations)
				for _, statement := range []string{
					"REVOKE UPDATE ON environments FROM " + pgx.Identifier{declarations.Roles["sandbox"].Name}.Sanitize(),
					"GRANT SELECT ON queue_jobs TO " + pgx.Identifier{declarations.Roles["auth"].Name}.Sanitize(),
					"GRANT UPDATE ON SEQUENCE session_runtime_binding_generation_seq TO " + pgx.Identifier{declarations.Roles["auth"].Name}.Sanitize(),
				} {
					if _, err := admin.Exec(statement); err != nil {
						t.Fatal(err)
					}
				}
				drift := workloadPrivilegeMismatches(t, admin, roleContract, declarations)
				for _, want := range []string{"sandbox environments UPDATE", "auth queue_jobs SELECT", "auth session_runtime_binding_generation_seq UPDATE"} {
					if !contains(drift, want) {
						t.Fatalf("catalog checker missed injected drift %s: %v", want, drift)
					}
				}
			}
		}

		for workload, declaration := range declarations.Roles {
			connection := openManagedRole(t, databaseName, declaration)
			var canLogin, superuser, bypassRLS, createDB, createRole, replication, inherit bool
			var memberships int
			if err := connection.QueryRow(context.Background(), `
				SELECT rolcanlogin, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole, rolreplication, rolinherit,
				       (SELECT count(*) FROM pg_auth_members WHERE member=pg_roles.oid)
				FROM pg_roles WHERE rolname=current_user`).Scan(
				&canLogin, &superuser, &bypassRLS, &createDB, &createRole, &replication, &inherit, &memberships,
			); err != nil {
				t.Fatal(err)
			}
			if !canLogin || superuser || bypassRLS || createDB || createRole || replication || inherit || memberships != 0 {
				t.Fatalf("workload %s has unsafe role attributes", workload)
			}
			_ = connection.Close(context.Background())
		}
		assertExactWorkloadPrivileges(t, admin, roleContract, declarations)
		assertGoServingReadinessAcceptsEveryWorkloadRole(t, databaseName, roleContract, declarations)
		assertBunReadinessAcceptsServingRole(t, databaseName, declarations.Roles["gateway"])
		assertGatewayCommandsAcceptOrdinaryRole(t, databaseName, declarations.Roles["gateway"])
		if _, err := admin.Exec("ALTER ROLE " + pgx.Identifier{declarations.Roles["gateway"].Name}.Sanitize() + " BYPASSRLS"); err != nil {
			t.Fatal(err)
		}
		assertGatewayCommandsRejectPrivilegedRole(t, databaseName, declarations.Roles["gateway"])

		assertWorkloadDenied(t, databaseName, declarations.Roles["auth"], `SELECT 1 FROM queue_jobs LIMIT 1`)
		assertWorkloadDenied(t, databaseName, declarations.Roles["gateway"], `SELECT 1 FROM session_events LIMIT 1`)
		assertWorkloadDenied(t, databaseName, declarations.Roles["queue"], `TRUNCATE queue_jobs`)
		assertWorkloadDenied(t, databaseName, declarations.Roles["bridge"], `CREATE TABLE forbidden_bridge_ddl (id integer)`)
		assertWorkloadDenied(t, databaseName, declarations.Roles["auth"], `BEGIN; SELECT set_config('tetral.queue_maintenance','true',true); SELECT 1 FROM queue_jobs LIMIT 1; COMMIT`)

		seedRLSRows(t, admin)
		assertAPIAndBridgeDurableOperations(t, databaseName, admin, declarations)
		auth := openManagedRole(t, databaseName, declarations.Roles["auth"])
		defer func() { _ = auth.Close(context.Background()) }()
		tx, err := auth.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `SELECT set_config('tetral.workspace_id','ws_contract_a',true)`); err != nil {
			t.Fatal(err)
		}
		var visible int
		if err := tx.QueryRow(context.Background(), `SELECT count(*) FROM api_keys WHERE id IN ('ak_contract_a','ak_contract_b')`).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		_ = tx.Rollback(context.Background())
		if visible != 1 {
			t.Fatalf("auth cross-workspace visibility = %d; want 1", visible)
		}
	})
}

func assertGoServingReadinessAcceptsEveryWorkloadRole(t *testing.T, databaseName string, contract database.RoleContract, declarations database.RoleDeclarations) {
	t.Helper()
	for _, workload := range contract.WorkloadNames() {
		db := openManagedRoleSQL(t, databaseName, declarations.Roles[workload])
		client := dbconnect.NewClientForTesting(db)
		if err := client.VerifySchema(context.Background()); err != nil {
			t.Fatalf("workload %s schema readiness failed: %v", workload, err)
		}
		if err := client.VerifyRuntimeRole(context.Background()); err != nil {
			t.Fatalf("workload %s runtime-role readiness failed: %v", workload, err)
		}
		_ = db.Close()
	}
}

func TestPostgreSQLRoleContractRefusesUnmanagedRoleCollision(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	declarations := database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}
	names := append(contract.WorkloadNames(), contract.MigrationOwner)
	for index, workload := range names {
		declarations.Roles[workload] = database.RoleCredential{
			Name:     fmt.Sprintf("tetral_collision_%d_%d", time.Now().UnixNano(), index),
			Password: "contract-password-" + workload,
		}
	}
	collision := declarations.Roles[names[0]].Name
	if _, err := admin.Exec("CREATE ROLE " + pgx.Identifier{collision}.Sanitize() + " LOGIN"); err != nil {
		t.Fatal(err)
	}
	defer dropDeclaredRoles(t, declarations)

	err = withPGXConnection(t, admin, func(connection *pgx.Conn) error {
		return database.ApplyRoleContract(context.Background(), connection, declarations)
	})
	if err == nil || !strings.Contains(err.Error(), "conflicts with an existing role") {
		t.Fatalf("ApplyRoleContract collision error = %v; want managed-role conflict", err)
	}
	var existing int
	if err := admin.QueryRow(`SELECT count(*) FROM pg_roles WHERE rolname = ANY($1)`, declarationRoleNames(declarations)).Scan(&existing); err != nil {
		t.Fatal(err)
	}
	if existing != 1 {
		t.Fatalf("role contract collision left %d roles; want only the pre-existing role", existing)
	}
}

func assertBunReadinessAcceptsServingRole(t *testing.T, databaseName string, credential database.RoleCredential) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(file))
	dsn := managedRoleURL(t, databaseName, credential)
	command := exec.Command("bun", "packages/schema/test/fixtures/live-readiness.ts")
	command.Dir = filepath.Join(root, "services", "gateway")
	command.Env = append(os.Environ(), "TETRAL_TEST_RUNTIME_DATABASE_URL="+dsn)
	if output, err := command.CombinedOutput(); err != nil {
		if strings.Contains(string(output), dsn) || strings.Contains(string(output), credential.Name) {
			t.Fatal("Bun readiness failure disclosed database identity")
		}
		t.Fatalf("Bun readiness rejected gateway serving role: %v: %s", err, output)
	}
}

func assertGatewayCommandsAcceptOrdinaryRole(t *testing.T, databaseName string, credential database.RoleCredential) {
	t.Helper()
	runGatewayCommandRoleFixture(t, managedRoleURL(t, databaseName, credential), "accepted")
}

func assertGatewayCommandsRejectPrivilegedRole(t *testing.T, databaseName string, credential database.RoleCredential) {
	t.Helper()
	runGatewayCommandRoleFixture(t, managedRoleURL(t, databaseName, credential), "rejected")
}

func runGatewayCommandRoleFixture(t *testing.T, dsn, expected string) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(file))
	command := exec.Command("bun", "packages/schema/test/fixtures/serving-command-role-readiness.ts")
	command.Dir = filepath.Join(root, "services", "gateway")
	command.Env = append(os.Environ(), "TETRAL_TEST_RUNTIME_DATABASE_URL="+dsn, "TETRAL_TEST_ROLE_EXPECTATION="+expected)
	if output, err := command.CombinedOutput(); err != nil {
		if strings.Contains(string(output), dsn) || strings.Contains(string(output), "postgres://") {
			t.Fatal("Gateway command readiness failure disclosed database identity")
		}
		t.Fatalf("Gateway command role readiness failed: %v: %s", err, output)
	}
}

func assertExactWorkloadPrivileges(t *testing.T, admin *sql.DB, contract database.RoleContract, declarations database.RoleDeclarations) {
	t.Helper()
	if mismatches := workloadPrivilegeMismatches(t, admin, contract, declarations); len(mismatches) != 0 {
		t.Fatalf("workload privilege drift: %v", mismatches)
	}
}

// Inspect every live public relation, including tables absent from a workload's
// declaration. Iterating only declared tables cannot detect accidental grants.
func workloadPrivilegeMismatches(t *testing.T, admin *sql.DB, contract database.RoleContract, declarations database.RoleDeclarations) []string {
	t.Helper()
	var mismatches []string
	for workload, role := range contract.Workloads {
		credential := declarations.Roles[workload]
		rows, err := admin.Query(`SELECT c.relname, p.privilege, has_table_privilege($1, c.oid, p.privilege)
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN']) AS p(privilege)
   WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f')`, credential.Name)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var table, privilege string
			var granted bool
			if err := rows.Scan(&table, &privilege, &granted); err != nil {
				t.Fatal(err)
			}
			if granted != contains(role.Tables[table], privilege) {
				mismatches = append(mismatches, fmt.Sprintf("%s %s %s", workload, table, privilege))
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		rows, err = admin.Query(`SELECT seq.relname, COALESCE(tbl.relname,''), p.privilege, has_sequence_privilege($1,seq.oid,p.privilege)
   FROM pg_class seq JOIN pg_namespace n ON n.oid=seq.relnamespace
   LEFT JOIN pg_depend dep ON dep.classid='pg_class'::regclass AND dep.objid=seq.oid AND dep.deptype IN ('a','i')
   LEFT JOIN pg_class tbl ON tbl.oid=dep.refobjid
   CROSS JOIN unnest(ARRAY['USAGE','SELECT','UPDATE']) AS p(privilege)
   WHERE n.nspname='public' AND seq.relkind='S'`, credential.Name)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var sequence, table, privilege string
			var granted bool
			if err := rows.Scan(&sequence, &table, &privilege, &granted); err != nil {
				t.Fatal(err)
			}
			want := privilege == "USAGE" && (contains(role.Sequences, sequence) || contains(role.Tables[table], "INSERT"))
			if granted != want {
				mismatches = append(mismatches, fmt.Sprintf("%s %s %s", workload, sequence, privilege))
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
	}
	return mismatches
}

func assertAPIAndBridgeDurableOperations(t *testing.T, databaseName string, admin *sql.DB, declarations database.RoleDeclarations) {
	t.Helper()

	api := openManagedRole(t, databaseName, declarations.Roles["api"])
	defer func() { _ = api.Close(context.Background()) }()
	tx, err := api.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), `SELECT set_config('tetral.workspace_id','ws_contract_a',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `INSERT INTO sessions (
		workspace_id,id,type,status,lifecycle_state,agent_id,agent_version,environment_id,created_at,updated_at
	) VALUES ('ws_contract_a','ses_contract','session','idle','active','agt_contract_a',1,'env_contract_a',clock_timestamp(),clock_timestamp())`); err != nil {
		t.Fatalf("representative api session creation failed: %v", err)
	}
	var versionID string
	if err := tx.QueryRow(context.Background(), `SELECT agent_version_id FROM sessions WHERE workspace_id='ws_contract_a' AND id='ses_contract'`).Scan(&versionID); err != nil {
		t.Fatal(err)
	}
	if versionID != "agv_contract_a" {
		t.Fatalf("session trigger resolved agent_version_id %q; want agv_contract_a", versionID)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertAPIEnvironmentBuildAdmission(t, databaseName, admin, declarations.Roles["api"])
	assertBridgeRuntimeRecoveryAdmission(t, databaseName, admin, declarations.Roles["bridge"])
	assertSameWorkspaceCrossSessionAccess(t, databaseName, declarations)
}

func assertAPIEnvironmentBuildAdmission(t *testing.T, databaseName string, admin *sql.DB, credential database.RoleCredential) {
	t.Helper()
	api := openManagedRoleSQL(t, databaseName, credential)
	defer func() { _ = api.Close() }()
	store := environment.NewPostgreSQLEnvironmentStore(dbconnect.NewClientForTesting(api))
	created, err := store.Create(context.Background(), workspace.ID("ws_contract_a"), environment.CreateEnvironmentRequest{
		Name: "role-contract-packages",
		Config: environment.EnvironmentConfig{
			Packages: environment.PackageMap{"pip": {"pandas==2.2.0"}},
		},
	})
	if err != nil {
		t.Fatalf("API role package environment creation failed: %v", err)
	}
	var artifacts, jobs, counters int
	if err := admin.QueryRow(`SELECT count(*) FROM environment_artifacts WHERE workspace_id='ws_contract_a' AND environment_id=$1 AND status='pending'`, created.ID).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='ws_contract_a' AND kind='environment_build'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_partition_counters WHERE workspace_id='ws_contract_a'`).Scan(&counters); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 || jobs != 1 || counters != 1 {
		t.Fatalf("API role package environment admission = artifacts:%d jobs:%d counters:%d; want 1/1/1", artifacts, jobs, counters)
	}
}

func assertBridgeRuntimeRecoveryAdmission(t *testing.T, databaseName string, admin *sql.DB, credential database.RoleCredential) {
	t.Helper()
	bridge := openManagedRoleSQL(t, databaseName, credential)
	defer func() { _ = bridge.Close() }()
	client := dbconnect.NewClientForTesting(bridge)
	ws := workspace.ID("ws_contract_a")
	request, err := queue.NewRuntimeRecoveryEnqueueRequest(ws, "ses_seed_a_0", "sth_contract", "evt_contract_recovery", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WithWorkspaceTx(context.Background(), string(ws), "role_contract.bridge_recovery", func(tx *dbconnect.Tx) error {
		_, enqueueErr := queue.EnqueueTx(context.Background(), tx, request)
		return enqueueErr
	}); err != nil {
		t.Fatalf("Bridge role Runtime recovery admission failed: %v", err)
	}
	var jobs, counters int
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id=$1 AND kind=$2 AND dedupe_key=$3`, string(ws), request.Kind, request.DedupeKey).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_partition_counters WHERE workspace_id=$1 AND partition_key=$2`, string(ws), request.PartitionKey).Scan(&counters); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || counters != 1 {
		t.Fatalf("Bridge role Runtime recovery admission = jobs:%d counters:%d; want 1/1", jobs, counters)
	}
}

func assertSameWorkspaceCrossSessionAccess(t *testing.T, databaseName string, declarations database.RoleDeclarations) {
	t.Helper()
	for workload, table := range map[string]string{
		"bridge":  "sessions",
		"queue":   "sessions",
		"cleanup": "session_runtime_status",
	} {
		connection := openManagedRole(t, databaseName, declarations.Roles[workload])
		tx, err := connection.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `SELECT set_config('tetral.workspace_id','ws_contract_a',true)`); err != nil {
			t.Fatal(err)
		}
		var workspaceA int
		if err := tx.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&workspaceA); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `SELECT set_config('tetral.workspace_id','ws_contract_b',true)`); err != nil {
			t.Fatal(err)
		}
		var workspaceB int
		if err := tx.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&workspaceB); err != nil {
			t.Fatal(err)
		}
		_ = tx.Rollback(context.Background())
		_ = connection.Close(context.Background())
		if workspaceA < 2 || workspaceB != 1 {
			t.Fatalf("%s same-workspace cross-session counts = %d/%d; want at least 2/1", workload, workspaceA, workspaceB)
		}
	}
}

func withPGXConnection(t *testing.T, db *sql.DB, fn func(*pgx.Conn) error) error {
	t.Helper()
	connection, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	return connection.Raw(func(raw any) error {
		stdlibConnection, ok := raw.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("unexpected PostgreSQL driver connection")
		}
		return fn(stdlibConnection.Conn())
	})
}

func openManagedRole(t *testing.T, databaseName string, credential database.RoleCredential) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(os.Getenv(storagetest.EnvTestDatabaseURL))
	if err != nil {
		t.Fatal(err)
	}
	config.Database = databaseName
	config.User = credential.Name
	config.Password = credential.Password
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("connect workload role: %v", err)
	}
	return connection
}

func openManagedRoleSQL(t *testing.T, databaseName string, credential database.RoleCredential) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(managedRoleURL(t, databaseName, credential))
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*config)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("connect workload role: %v", err)
	}
	return db
}

func managedRoleURL(t *testing.T, databaseName string, credential database.RoleCredential) string {
	t.Helper()
	parsed, err := url.Parse(os.Getenv(storagetest.EnvTestDatabaseURL))
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(credential.Name, credential.Password)
	parsed.Path = "/" + databaseName
	return parsed.String()
}

func assertWorkloadDenied(t *testing.T, databaseName string, credential database.RoleCredential, statement string) {
	t.Helper()
	connection := openManagedRole(t, databaseName, credential)
	defer func() { _ = connection.Close(context.Background()) }()
	if _, err := connection.Exec(context.Background(), statement); err == nil {
		t.Fatalf("unauthorized statement succeeded: %s", statement)
	}
}

func seedRLSRows(t *testing.T, admin *sql.DB) {
	t.Helper()
	for _, workspaceID := range []string{"ws_contract_a", "ws_contract_b"} {
		if _, err := admin.Exec(`INSERT INTO workspaces (id, name, created_at) VALUES ($1,$1,clock_timestamp())`, workspaceID); err != nil {
			t.Fatal(err)
		}
	}
	for index, workspaceID := range []string{"ws_contract_a", "ws_contract_b"} {
		if _, err := admin.Exec(`INSERT INTO api_keys (workspace_id,id,name,key_prefix,key_digest,key_kind,created_at) VALUES ($1,$2,'contract','tk_test',$3,'standard',clock_timestamp())`, workspaceID, fmt.Sprintf("ak_contract_%c", 'a'+index), []byte{byte(index + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, suffix := range []string{"a", "b"} {
		workspaceID := "ws_contract_" + suffix
		agentID := "agt_contract_" + suffix
		versionID := "agv_contract_" + suffix
		environmentID := "env_contract_" + suffix
		if _, err := admin.Exec(`INSERT INTO agents (workspace_id,id,name,created_at,updated_at) VALUES ($1,$2,'contract',clock_timestamp(),clock_timestamp())`, workspaceID, agentID); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(`INSERT INTO agent_versions (workspace_id,id,agent_id,version,config_json,created_at) VALUES ($1,$2,$3,1,'{}',clock_timestamp())`, workspaceID, versionID, agentID); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(`INSERT INTO environments (workspace_id,id,name,config_json,created_at,updated_at) VALUES ($1,$2,$3,'{}',clock_timestamp(),clock_timestamp())`, workspaceID, environmentID, "contract-"+suffix); err != nil {
			t.Fatal(err)
		}
		sessionCount := 1
		if suffix == "a" {
			sessionCount = 2
		}
		for index := 0; index < sessionCount; index++ {
			sessionID := fmt.Sprintf("ses_seed_%s_%d", suffix, index)
			if _, err := admin.Exec(`INSERT INTO sessions (workspace_id,id,type,status,lifecycle_state,agent_id,agent_version_id,agent_version,environment_id,created_at,updated_at) VALUES ($1,$2,'session','idle','active',$3,$4,1,$5,clock_timestamp(),clock_timestamp())`, workspaceID, sessionID, agentID, versionID, environmentID); err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(`INSERT INTO session_runtime_status (workspace_id,session_id,status,idle_since,created_at,updated_at) VALUES ($1,$2,'idle',clock_timestamp(),clock_timestamp(),clock_timestamp())`, workspaceID, sessionID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func restoreCloneRoleBoundary(t *testing.T, admin *sql.DB, cloneOwner, cloneRuntimeRole string, declarations database.RoleDeclarations) {
	t.Helper()
	migration := declarations.Roles["migration"].Name
	if _, err := admin.Exec("REASSIGN OWNED BY " + pgx.Identifier{migration}.Sanitize() + " TO " + pgx.Identifier{cloneOwner}.Sanitize()); err != nil {
		t.Fatalf("restore clone ownership: %v", err)
	}
	for _, declaration := range declarations.Roles {
		if _, err := admin.Exec("DROP OWNED BY " + pgx.Identifier{declaration.Name}.Sanitize()); err != nil {
			t.Fatalf("revoke managed role grants: %v", err)
		}
	}
	dropDeclaredRoles(t, declarations)
	control, err := pgx.Connect(context.Background(), os.Getenv(storagetest.EnvTestDatabaseURL))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = control.Close(context.Background()) }()
	if _, err := control.Exec(context.Background(), "GRANT CONNECT ON DATABASE "+pgx.Identifier{currentDatabase(t, admin)}.Sanitize()+" TO "+pgx.Identifier{cloneRuntimeRole}.Sanitize()); err != nil {
		t.Fatalf("restore clone connect grant: %v", err)
	}
}

func dropDeclaredRoles(t *testing.T, declarations database.RoleDeclarations) {
	t.Helper()
	control, err := pgx.Connect(context.Background(), os.Getenv(storagetest.EnvTestDatabaseURL))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = control.Close(context.Background()) }()
	for _, declaration := range declarations.Roles {
		if _, err := control.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{declaration.Name}.Sanitize()); err != nil {
			t.Fatalf("drop managed test role: %v", err)
		}
	}
}

func declarationRoleNames(declarations database.RoleDeclarations) []string {
	names := make([]string, 0, len(declarations.Roles))
	for _, declaration := range declarations.Roles {
		names = append(names, declaration.Name)
	}
	return names
}

func currentDatabase(t *testing.T, admin *sql.DB) string {
	t.Helper()
	var name string
	if err := admin.QueryRow(`SELECT current_database()`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	return name
}
