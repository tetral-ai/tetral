package database

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

const roleContractAdvisoryLock int64 = 0x7465_7472_616c_524c

type RoleCredential struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type RoleDeclarations struct {
	Roles map[string]RoleCredential `json:"roles"`
}

// ApplyRoleContract installs the exact serving-role boundary after the schema
// owner has constructed the current Version 1 catalog. Names and credentials
// come from the operator; the repository owns only workload capabilities.
func ApplyRoleContract(ctx context.Context, connection *pgx.Conn, declarations RoleDeclarations) error {
	if connection == nil {
		return fmt.Errorf("PostgreSQL role contract requires an administrative connection")
	}
	contract, err := LoadRoleContract()
	if err != nil {
		return err
	}
	required := append(contract.WorkloadNames(), contract.MigrationOwner)
	if err := validateRoleDeclarations(required, declarations); err != nil {
		return err
	}
	tx, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL role contract: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", roleContractAdvisoryLock); err != nil {
		return fmt.Errorf("lock PostgreSQL role contract: %w", err)
	}
	var databaseName string
	if err := tx.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		return fmt.Errorf("inspect PostgreSQL role contract database: %w", err)
	}
	for _, workload := range required {
		if err := ensureManagedRole(ctx, tx, workload, declarations.Roles[workload]); err != nil {
			return err
		}
	}
	migrationRole := identifier(declarations.Roles[contract.MigrationOwner].Name)
	if _, err := tx.Exec(ctx, "REVOKE CONNECT, TEMPORARY ON DATABASE "+identifier(databaseName)+" FROM PUBLIC"); err != nil {
		return fmt.Errorf("revoke public database privileges: %w", err)
	}
	if _, err := tx.Exec(ctx, "REVOKE ALL ON SCHEMA public FROM PUBLIC"); err != nil {
		return fmt.Errorf("revoke public schema privileges: %w", err)
	}
	for _, object := range []string{"TABLES", "SEQUENCES", "FUNCTIONS"} {
		if _, err := tx.Exec(ctx, "REVOKE ALL ON ALL "+object+" IN SCHEMA public FROM PUBLIC"); err != nil {
			return fmt.Errorf("revoke public %s privileges: %w", strings.ToLower(object), err)
		}
	}
	if _, err := tx.Exec(ctx, "GRANT CONNECT ON DATABASE "+identifier(databaseName)+" TO "+migrationRole); err != nil {
		return fmt.Errorf("grant migration database access: %w", err)
	}
	if _, err := tx.Exec(ctx, "GRANT USAGE, CREATE ON SCHEMA public TO "+migrationRole); err != nil {
		return fmt.Errorf("grant migration schema access: %w", err)
	}
	if err := ownCurrentSchemaObjects(ctx, tx, migrationRole); err != nil {
		return err
	}
	for _, workload := range contract.WorkloadNames() {
		declaration := declarations.Roles[workload]
		role := identifier(declaration.Name)
		if _, err := tx.Exec(ctx, "REVOKE ALL ON DATABASE "+identifier(databaseName)+" FROM "+role); err != nil {
			return fmt.Errorf("reset workload database grants: %w", err)
		}
		for _, object := range []string{"TABLES", "SEQUENCES", "FUNCTIONS"} {
			if _, err := tx.Exec(ctx, "REVOKE ALL ON ALL "+object+" IN SCHEMA public FROM "+role); err != nil {
				return fmt.Errorf("reset workload %s grants: %w", strings.ToLower(object), err)
			}
		}
		if _, err := tx.Exec(ctx, "REVOKE ALL ON SCHEMA public FROM "+role); err != nil {
			return fmt.Errorf("reset workload schema grants: %w", err)
		}
		if _, err := tx.Exec(ctx, "GRANT CONNECT ON DATABASE "+identifier(databaseName)+" TO "+role); err != nil {
			return fmt.Errorf("grant workload database access: %w", err)
		}
		if _, err := tx.Exec(ctx, "GRANT USAGE ON SCHEMA public TO "+role); err != nil {
			return fmt.Errorf("grant workload schema access: %w", err)
		}
		if err := grantWorkloadTables(ctx, tx, role, contract.Workloads[workload]); err != nil {
			return fmt.Errorf("grant workload %q: %w", workload, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL role contract: %w", err)
	}
	return nil
}

func validateRoleDeclarations(required []string, declarations RoleDeclarations) error {
	if len(declarations.Roles) != len(required) {
		return fmt.Errorf("PostgreSQL role declarations must exactly match the contract")
	}
	seenNames := map[string]bool{}
	for _, workload := range required {
		declaration, ok := declarations.Roles[workload]
		if !ok || declaration.Name == "" || declaration.Password == "" || seenNames[declaration.Name] {
			return fmt.Errorf("invalid PostgreSQL role declaration for workload %q", workload)
		}
		seenNames[declaration.Name] = true
	}
	return nil
}

func ensureManagedRole(ctx context.Context, tx pgx.Tx, workload string, declaration RoleCredential) error {
	comment := "tetral-database-role:" + workload + ":v1"
	var exists, superuser, bypassRLS, createDB, createRole, replication, inherit bool
	var memberships int
	var existingComment string
	err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1),
		       COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname=$1), false),
		       COALESCE((SELECT rolbypassrls FROM pg_roles WHERE rolname=$1), false),
		       COALESCE((SELECT rolcreatedb FROM pg_roles WHERE rolname=$1), false),
		       COALESCE((SELECT rolcreaterole FROM pg_roles WHERE rolname=$1), false),
		       COALESCE((SELECT rolreplication FROM pg_roles WHERE rolname=$1), false),
		       COALESCE((SELECT rolinherit FROM pg_roles WHERE rolname=$1), false),
		       COALESCE((SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=$1)), 0),
		       COALESCE((SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname=$1), '')`, declaration.Name).Scan(
		&exists, &superuser, &bypassRLS, &createDB, &createRole, &replication, &inherit, &memberships, &existingComment,
	)
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL role declaration: %w", err)
	}
	if superuser || bypassRLS || createDB || createRole || replication || inherit || memberships != 0 || (exists && existingComment != comment) {
		return fmt.Errorf("PostgreSQL role declaration conflicts with an existing role")
	}
	role := identifier(declaration.Name)
	if !exists {
		if _, err := tx.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD "+literal(declaration.Password)); err != nil {
			return fmt.Errorf("create PostgreSQL workload role: %w", err)
		}
	} else if _, err := tx.Exec(ctx, "ALTER ROLE "+role+" LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD "+literal(declaration.Password)); err != nil {
		return fmt.Errorf("update PostgreSQL workload role: %w", err)
	}
	if _, err := tx.Exec(ctx, "COMMENT ON ROLE "+role+" IS "+literal(comment)); err != nil {
		return fmt.Errorf("record PostgreSQL workload role authority: %w", err)
	}
	return nil
}

func ownCurrentSchemaObjects(ctx context.Context, tx pgx.Tx, migrationRole string) error {
	type ownedObject struct {
		name      string
		arguments string
	}
	queries := []struct {
		query  string
		format func(ownedObject) string
	}{
		{`SELECT c.relname, '' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p') ORDER BY c.relname`, func(object ownedObject) string {
			return "ALTER TABLE " + identifier(object.name) + " OWNER TO " + migrationRole
		}},
		{`SELECT c.relname, '' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='S' ORDER BY c.relname`, func(object ownedObject) string {
			return "ALTER SEQUENCE " + identifier(object.name) + " OWNER TO " + migrationRole
		}},
		{`SELECT p.proname, pg_get_function_identity_arguments(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)`, func(object ownedObject) string {
			return "ALTER FUNCTION " + identifier(object.name) + "(" + object.arguments + ") OWNER TO " + migrationRole
		}},
	}
	for _, item := range queries {
		rows, err := tx.Query(ctx, item.query)
		if err != nil {
			return fmt.Errorf("inspect migration-owned schema objects: %w", err)
		}
		var objects []ownedObject
		for rows.Next() {
			var object ownedObject
			if err := rows.Scan(&object.name, &object.arguments); err != nil {
				rows.Close()
				return fmt.Errorf("read migration-owned schema object: %w", err)
			}
			objects = append(objects, object)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("read migration-owned schema objects: %w", err)
		}
		rows.Close()
		for _, object := range objects {
			if _, err := tx.Exec(ctx, item.format(object)); err != nil {
				return fmt.Errorf("assign migration-owned schema object: %w", err)
			}
		}
	}
	return nil
}

func grantWorkloadTables(ctx context.Context, tx pgx.Tx, role string, workload WorkloadRole) error {
	tables := make([]string, 0, len(workload.Tables))
	for table := range workload.Tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		privileges := append([]string(nil), workload.Tables[table]...)
		sort.Strings(privileges)
		if _, err := tx.Exec(ctx, "GRANT "+strings.Join(privileges, ", ")+" ON TABLE "+identifier(table)+" TO "+role); err != nil {
			return err
		}
		if containsPrivilege(privileges, "INSERT") {
			rows, err := tx.Query(ctx, `SELECT seq.relname
				FROM pg_class seq
				JOIN pg_depend dep ON dep.objid=seq.oid AND dep.deptype IN ('a','i')
				JOIN pg_class tbl ON tbl.oid=dep.refobjid
				JOIN pg_namespace n ON n.oid=tbl.relnamespace
				WHERE n.nspname='public' AND seq.relkind='S' AND tbl.relname=$1`, table)
			if err != nil {
				return err
			}
			var sequences []string
			for rows.Next() {
				var sequence string
				if err := rows.Scan(&sequence); err != nil {
					rows.Close()
					return err
				}
				sequences = append(sequences, sequence)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
			for _, sequence := range sequences {
				if _, err := tx.Exec(ctx, "GRANT USAGE ON SEQUENCE "+identifier(sequence)+" TO "+role); err != nil {
					return err
				}
			}
		}
	}
	for _, sequence := range workload.Sequences {
		if _, err := tx.Exec(ctx, "GRANT USAGE ON SEQUENCE "+identifier(sequence)+" TO "+role); err != nil {
			return err
		}
	}
	return nil
}

func identifier(value string) string { return pgx.Identifier{value}.Sanitize() }

func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func containsPrivilege(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
