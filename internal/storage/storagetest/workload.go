package storagetest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
)

// WorkloadDB authenticates as an installer-configured serving role in a private
// test clone. Admin remains exclusively for fixtures, fault injection and
// assertions; production operations must use DB.
type WorkloadDB struct {
	DB           *sql.DB
	admin        *sql.DB
	workload     string
	contract     database.RoleContract
	declarations database.RoleDeclarations
}

// OpenWorkloadDB applies the production contract to an existing test clone.
// Unlike the ordinary test role, this connection has only the workload's
// declared privileges. Use once per clone; all declared roles are test-owned.
func OpenWorkloadDB(t testing.TB, admin *sql.DB, workload string) *WorkloadDB {
	t.Helper()
	handleMu.RLock()
	handle := handles[admin]
	handleMu.RUnlock()
	if handle == nil {
		t.Fatal("storagetest: workload database requires a live clone handle")
		return nil
	}
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := contract.Workloads[workload]; !ok {
		t.Fatalf("unknown workload %q", workload)
	}
	identity, err := randomHex(12)
	if err != nil {
		t.Fatal(err)
	}
	w := &WorkloadDB{admin: admin, workload: workload, contract: contract,
		declarations: database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}}
	for i, name := range append(contract.WorkloadNames(), contract.MigrationOwner) {
		password, err := randomHex(24)
		if err != nil {
			t.Fatal(err)
		}
		w.declarations.Roles[name] = database.RoleCredential{Name: fmt.Sprintf("tetral_workload_%s_%d", identity, i), Password: password}
	}
	// Reserve the cluster roles and their OIDs in the control registry atomically,
	// before the installer can commit CONNECT grants or schema ownership.
	if err := reserveWorkloadRoles(context.Background(), handle, w.declarations); err != nil {
		t.Fatalf("reserve workload roles: %v", err)
	}
	t.Cleanup(func() {
		if w.DB != nil {
			unregisterHandle(w.DB)
			_ = w.DB.Close()
		}
	})
	w.apply(t)
	credential := w.declarations.Roles[workload]
	config := handle.adminConfig.Copy()
	config.User, config.Password = credential.Name, credential.Password
	w.DB = openPool(config)
	if err := w.DB.PingContext(context.Background()); err != nil {
		t.Fatal("storagetest: connect workload database")
	}
	workloadHandle := *handle
	workloadHandle.runtimeRole, workloadHandle.runtimeConfig = credential.Name, config
	workloadHandle.runtimeURL, err = connectionURLWithIdentity(handle.adminURL, handle.database, credential.Name, credential.Password)
	if err != nil {
		t.Fatal("storagetest: construct workload connection identity")
	}
	registerHandle(w.DB, &workloadHandle)
	return w
}

// RequirePrivilege removes exactly one declared grant and calls a production
// operation, which must fail with insufficient_privilege. The installer restores
// the whole contract even on failure. The caller must then verify successful
// execution and its durable outcomes. Do not run mutations concurrently.
func (w *WorkloadDB) RequirePrivilege(t testing.TB, table, privilege string, operation func() error) {
	t.Helper()
	if !slices.Contains(w.contract.Workloads[w.workload].Tables[table], privilege) {
		t.Fatalf("%s does not declare %s on %s", w.workload, privilege, table)
	}
	if _, err := w.admin.Exec("REVOKE " + privilege + " ON TABLE " + pgx.Identifier{"public", table}.Sanitize() + " FROM " + pgx.Identifier{w.declarations.Roles[w.workload].Name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer w.apply(t)
	err := operation()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("%s without %s on %s: want SQLSTATE 42501, got %v", w.workload, privilege, table, err)
	}
	t.Logf("%s requires %s on %s: SQLSTATE 42501", w.workload, privilege, table)
}

func (w *WorkloadDB) apply(t testing.TB) {
	t.Helper()
	conn, err := w.admin.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Raw(func(raw any) error {
		return database.ApplyRoleContract(context.Background(), raw.(*stdlib.Conn).Conn(), w.declarations)
	}); err != nil {
		t.Fatalf("apply workload role contract: %v", err)
	}
}

// Role OIDs fence reuse of a name after interruption. The installer owns the
// standard role comments; the clone registry owns lifetime and deletion rights.
type registeredWorkloadRole struct {
	Name    string `json:"name"`
	OID     uint32 `json:"oid"`
	Comment string `json:"comment"`
	exists  bool
}

func reserveWorkloadRoles(ctx context.Context, handle *cloneHandle, declarations database.RoleDeclarations) error {
	control := openPool(handle.controlConfig)
	defer func() { _ = control.Close() }()
	tx, err := control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var available bool
	if err := tx.QueryRowContext(ctx, "SELECT workload_roles='[]'::jsonb FROM "+cloneRegistryTable+" WHERE database_name=$1 AND role_name=$2 AND phase=$3 FOR UPDATE", handle.database, handle.runtimeRole, clonePhaseReady).Scan(&available); err != nil {
		return err
	}
	if !available {
		return errors.New("clone already has workload roles")
	}
	roles := make([]registeredWorkloadRole, 0, len(declarations.Roles))
	for workload, credential := range declarations.Roles {
		role := registeredWorkloadRole{Name: credential.Name, Comment: "tetral-database-role:" + workload + ":v1"}
		// NOLOGIN until ApplyRoleContract sets the actual credential and attributes.
		if _, err := tx.ExecContext(ctx, "CREATE ROLE "+pgx.Identifier{role.Name}.Sanitize()+" NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "COMMENT ON ROLE "+pgx.Identifier{role.Name}.Sanitize()+" IS "+quoteLiteral(role.Comment)); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT oid FROM pg_roles WHERE rolname=$1", role.Name).Scan(&role.OID); err != nil {
			return err
		}
		roles = append(roles, role)
	}
	raw, err := json.Marshal(roles)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+cloneRegistryTable+" SET workload_roles=$2::jsonb WHERE database_name=$1", handle.database, string(raw)); err != nil {
		return err
	}
	return tx.Commit()
}

func registeredWorkloadRoles(ctx context.Context, db cleanupExecutor, registration cloneRegistration) ([]registeredWorkloadRole, error) {
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT workload_roles::text FROM "+cloneRegistryTable+" WHERE database_name=$1 AND run_id=$2 AND role_name=$3", registration.database, registration.runID, registration.role).Scan(&raw); err != nil {
		return nil, err
	}
	var roles []registeredWorkloadRole
	if err := json.Unmarshal([]byte(raw), &roles); err != nil {
		return nil, err
	}
	for i := range roles {
		role := &roles[i]
		if !safeGeneratedName(role.Name, "tetral_workload_") || role.OID == 0 {
			return nil, errors.New("invalid registered workload role")
		}
		var owned bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1 OR oid=$2),
   COALESCE((SELECT oid=$2 AND shobj_description(oid,'pg_authid')=$3
    AND NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolinherit
    AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid OR roleid=pg_roles.oid)
    FROM pg_roles WHERE rolname=$1),false)`, role.Name, role.OID, role.Comment).Scan(&role.exists, &owned); err != nil {
			return nil, err
		}
		if role.exists && !owned {
			return nil, errors.New("registered workload role authority changed")
		}
	}
	return roles, nil
}
