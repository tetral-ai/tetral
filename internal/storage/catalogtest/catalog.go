// Package catalogtest captures the stable PostgreSQL catalog facts owned by
// the Version 1 schema constructor. It is test infrastructure, not a runtime
// schema reader.
package catalogtest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
)

type querySnapshot struct {
	Name string     `json:"name"`
	Rows [][]string `json:"rows"`
}

// Snapshot returns catalog facts without catalog OIDs or other allocation-
// order identities. Every query has an explicit semantic order.
func Snapshot(ctx context.Context, db *sql.DB) ([]byte, error) {
	return snapshot(ctx, db, nil)
}

func snapshot(ctx context.Context, db *sql.DB, replacements map[string]string) ([]byte, error) {
	queries := []struct {
		name string
		sql  string
	}{
		{"schema", `SELECT pg_get_userbyid(n.nspowner), COALESCE(n.nspacl::text,'') FROM pg_namespace n WHERE n.nspname=current_schema()`},
		{"relations", `SELECT c.relname, c.relkind::text, pg_get_userbyid(c.relowner), c.relrowsecurity::text, c.relforcerowsecurity::text, COALESCE(c.relacl::text,'') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','S') ORDER BY c.relkind,c.relname`},
		{"columns", `SELECT c.relname, a.attnum::text, a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull::text, COALESCE(pg_get_expr(d.adbin,d.adrelid),''), a.attidentity::text, a.attgenerated::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum`},
		{"constraints", `SELECT c.relname, con.conname, con.contype::text, pg_get_constraintdef(con.oid,true), con.condeferrable::text, con.condeferred::text FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() ORDER BY c.relname,con.conname`},
		{"indexes", `SELECT t.relname, i.relname, pg_get_indexdef(i.oid) FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class t ON t.oid=x.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname=current_schema() ORDER BY t.relname,i.relname`},
		{"sequences", `SELECT c.relname, format_type(s.seqtypid,NULL), s.seqstart::text, s.seqincrement::text, s.seqmax::text, s.seqmin::text, s.seqcache::text, s.seqcycle::text FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() ORDER BY c.relname`},
		{"functions", `SELECT p.proname, pg_get_function_identity_arguments(p.oid), pg_get_function_result(p.oid), pg_get_functiondef(p.oid), pg_get_userbyid(p.proowner), COALESCE(p.proacl::text,'') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() ORDER BY p.proname,pg_get_function_identity_arguments(p.oid)`},
		{"triggers", `SELECT c.relname, t.tgname, t.tgenabled::text, pg_get_triggerdef(t.oid,true) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND NOT t.tgisinternal ORDER BY c.relname,t.tgname`},
		{"policies", `SELECT c.relname, p.polname, p.polpermissive::text, p.polcmd::text, ARRAY(SELECT pg_get_userbyid(role_oid) FROM unnest(p.polroles) role_oid ORDER BY 1)::text, COALESCE(pg_get_expr(p.polqual,p.polrelid),''), COALESCE(pg_get_expr(p.polwithcheck,p.polrelid),'') FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() ORDER BY c.relname,p.polname`},
		{"dependencies", `SELECT source.type, CASE WHEN source.schema=current_schema() THEN '<schema>' ELSE COALESCE(source.schema,'') END, CASE WHEN source.type='schema' THEN '<schema>' WHEN source.type='toast table' THEN '<toast>' ELSE COALESCE(source.name,'') END, CASE WHEN source.type='schema' THEN '<schema>' WHEN source.type='toast table' THEN '<toast>' ELSE regexp_replace(replace(source.identity,current_schema()||'.','<schema>.'),'"RI_ConstraintTrigger_[^"]+"','"<constraint-trigger>"','g') END, target.type, CASE WHEN target.schema=current_schema() THEN '<schema>' ELSE COALESCE(target.schema,'') END, CASE WHEN target.type='schema' THEN '<schema>' WHEN target.type='toast table' THEN '<toast>' ELSE COALESCE(target.name,'') END, CASE WHEN target.type='schema' THEN '<schema>' WHEN target.type='toast table' THEN '<toast>' ELSE regexp_replace(replace(target.identity,current_schema()||'.','<schema>.'),'"RI_ConstraintTrigger_[^"]+"','"<constraint-trigger>"','g') END, d.deptype::text FROM pg_depend d CROSS JOIN LATERAL pg_identify_object(d.classid,d.objid,d.objsubid) source CROSS JOIN LATERAL pg_identify_object(d.refclassid,d.refobjid,d.refobjsubid) target WHERE (source.schema=current_schema() OR target.schema=current_schema()) AND d.deptype <> 'e' ORDER BY source.type,source.schema,source.name,source.identity,target.type,target.schema,target.name,target.identity,d.deptype`},
		{"migrations", `SELECT version::text, checksum FROM tetral_schema_migrations ORDER BY version`},
	}
	result := make([]querySnapshot, 0, len(queries))
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query.sql)
		if err != nil {
			return nil, fmt.Errorf("capture %s catalog: %w", query.name, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		item := querySnapshot{Name: query.name}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for index := range values {
				pointers[index] = &values[index]
			}
			if err := rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				return nil, err
			}
			line := make([]string, len(values))
			for index, value := range values {
				switch typed := value.(type) {
				case nil:
					line[index] = "<null>"
				case []byte:
					line[index] = string(typed)
				default:
					line[index] = fmt.Sprint(typed)
				}
			}
			item.Rows = append(item.Rows, line)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(replacements))
	for source := range replacements {
		keys = append(keys, source)
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, source := range keys {
		body = bytes.ReplaceAll(body, []byte(source), []byte(replacements[source]))
	}
	return body, nil
}

// HelperSnapshot captures the logical seed and privileges installed by the
// ordinary storage-test bootstrap while normalizing generated role and schema
// identities. Database-level clone isolation is proved by storagetest itself.
func HelperSnapshot(ctx context.Context, runtimeDB, adminDB *sql.DB) ([]byte, error) {
	var runtimeRole, schemaName, schemaOwner, objectOwner string
	if err := runtimeDB.QueryRowContext(ctx, `SELECT current_user`).Scan(&runtimeRole); err != nil {
		return nil, err
	}
	if err := adminDB.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schemaName); err != nil {
		return nil, err
	}
	if err := adminDB.QueryRowContext(ctx, `SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=current_schema()`).Scan(&schemaOwner); err != nil {
		return nil, err
	}
	if err := adminDB.QueryRowContext(ctx, `SELECT current_user`).Scan(&objectOwner); err != nil {
		return nil, err
	}
	catalog, err := snapshot(ctx, adminDB, map[string]string{
		runtimeRole:      "<runtime-role>",
		schemaName + ".": "<schema>.",
		schemaOwner:      "<owner>",
		objectOwner:      "<owner>",
	})
	if err != nil {
		return nil, err
	}
	var workspaceType, workspaceName string
	if err := adminDB.QueryRowContext(ctx, `SELECT type, name FROM workspaces WHERE id='default'`).Scan(&workspaceType, &workspaceName); err != nil {
		return nil, err
	}
	var login, superuser, bypassRLS bool
	if err := adminDB.QueryRowContext(ctx, `SELECT rolcanlogin, rolsuper, rolbypassrls FROM pg_roles WHERE rolname=$1`, runtimeRole).Scan(&login, &superuser, &bypassRLS); err != nil {
		return nil, err
	}
	privileges, err := helperPrivileges(ctx, adminDB, runtimeRole)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(struct {
		Catalog     json.RawMessage `json:"catalog"`
		Workspace   [2]string       `json:"workspace"`
		RuntimeRole [3]bool         `json:"runtime_role"`
		Privileges  [][]string      `json:"privileges"`
	}{catalog, [2]string{workspaceType, workspaceName}, [3]bool{login, superuser, bypassRLS}, privileges}, "", "  ")
}

func helperPrivileges(ctx context.Context, db *sql.DB, role string) ([][]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT c.relname, privilege
		FROM pg_class c
		JOIN pg_namespace n ON n.oid=c.relnamespace
		CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) privilege
		WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND has_table_privilege($1,c.oid,privilege)
		UNION ALL
		SELECT c.relname, privilege
		FROM pg_class c
		JOIN pg_namespace n ON n.oid=c.relnamespace
		CROSS JOIN unnest(ARRAY['SELECT','UPDATE','USAGE']) privilege
		WHERE n.nspname=current_schema() AND c.relkind='S' AND has_sequence_privilege($1,c.oid,privilege)
		ORDER BY 1,2`, role)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result [][]string
	for rows.Next() {
		var relation, privilege string
		if err := rows.Scan(&relation, &privilege); err != nil {
			return nil, err
		}
		result = append(result, []string{relation, privilege})
	}
	return result, rows.Err()
}
