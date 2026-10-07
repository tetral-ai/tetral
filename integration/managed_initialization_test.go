package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The actual initialization commands run against a private empty target. A
// rejected preparation or import exits nonzero without mutating the target;
// Auth cannot start before bootstrap; repeating preparation, bootstrap and
// import preserves workspace, grant and key identities; a failed import leaves
// the prior authority rows unchanged, and the repaired sequence admits again.
func TestPostgreSQLManagedInitialization(t *testing.T) {
	oidcIsolatedTLSCaseWithMarker(t, "managed_initialization_assertion=commands_order_repeat_failure_preserved", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		admin := storagetest.NewEmptyPostgreSQLAdminDB(t)
		var target, schema string
		var objects int
		if err := admin.QueryRowContext(ctx, `SELECT current_database(),current_schema(),(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace)`).Scan(&target, &schema, &objects); err != nil {
			t.Fatal(err)
		}
		connection, err := url.Parse(storagetest.AdminDatabaseURL(t, admin))
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimPrefix(connection.Path, "/") != target || schema != "public" || objects != 0 {
			t.Fatal("initialization did not begin on the declared empty private target")
		}
		contract, err := database.LoadRoleContract()
		if err != nil {
			t.Fatal(err)
		}
		declarations := database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}
		for i, name := range append(contract.WorkloadNames(), contract.MigrationOwner) {
			declarations.Roles[name] = database.RoleCredential{Name: fmt.Sprintf("tetral_managed_%d_%d", time.Now().UnixNano(), i), Password: "managed-private-password-" + name}
		}
		t.Cleanup(func() { managedInitializationDropRoles(t, admin, declarations) })
		roleInput := managedInitializationJSON(t, declarations)
		binaries := map[string]string{}
		for _, entry := range []struct{ name, path string }{{"prepare", "./cmd/tetral-db-prepare"}, {"bootstrap", "./cmd/tetral-bootstrap"}, {"policy", "./services/auth/cmd/tetral-auth-policy"}, {"auth", "./services/auth/cmd/tetral-auth"}} {
			binary := filepath.Join(t.TempDir(), entry.name)
			buildCtx, stop := context.WithTimeout(ctx, time.Minute)
			command := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binary, entry.path) //nolint:gosec // Fixed production entrypoints, private output path.
			command.Dir = ".."
			output, buildErr := command.CombinedOutput()
			stop()
			if buildErr != nil {
				t.Fatalf("build initialization command %s: %v: %s", entry.name, buildErr, output)
			}
			binaries[entry.name] = binary
		}
		base := map[string]string{"TETRAL_DATABASE_ADMIN_URL": connection.String(), "TETRAL_DATABASE_TLS_CA_PATH": filepath.Join(os.Getenv("TETRAL_OIDC_PROCESS_PG_CERTS"), "ca.pem"), "TETRAL_DATABASE_TLS_SERVER_NAME": "postgres.transport.test"}
		roleURL := func(name string) string {
			u := *connection
			role := declarations.Roles[name]
			u.User = url.UserPassword(role.Name, role.Password)
			return u.String()
		}
		bootstrapEnv := managedInitializationClone(base)
		delete(bootstrapEnv, "TETRAL_DATABASE_ADMIN_URL")
		bootstrapEnv["TETRAL_DATABASE_URL"] = roleURL(contract.MigrationOwner)
		privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
		if err != nil {
			t.Fatal(err)
		}
		// Allocate addresses for the negative pre-bootstrap Auth start; each
		// serving start reserves anew.
		processEnv := managedInitializationClone(base)
		delete(processEnv, "TETRAL_DATABASE_ADMIN_URL")
		for key, value := range map[string]string{"TETRAL_DATABASE_URL": roleURL("auth"), "TETRAL_AUTH_GRPC_TRANSPORT": "plaintext", "TETRAL_HTTP_TRANSPORT": "plaintext", "TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64": privateKey, "ENGINE_API_KEY": strings.Repeat("m", auth.MinBootstrapKeyBytes), "ENGINE_BOOTSTRAP_WORKSPACE_ID": "managed_workspace"} {
			processEnv[key] = value
		}
		releaseInitial := managedInitializationReserveListeners(t, processEnv)
		releaseInitial()
		document := auth.PolicyDocument{FederationRules: []auth.FederationRule{{ID: "managed_rule", OrganizationID: "managed_org", Issuer: "https://issuer.managed.test", Audience: "tetral-engine", JWKSURL: "https://issuer.managed.test/keys", Algorithm: "RS256", Enabled: true}}, Identities: []auth.IdentityBinding{{ID: "managed_identity", OrganizationID: "managed_org", Issuer: "https://issuer.managed.test", Subject: "managed_subject", Kind: auth.IdentityHuman, Enabled: true}}, WorkspaceGrants: []auth.WorkspaceGrant{{ID: "managed_grant", IdentityID: "managed_identity", WorkspaceID: workspace.ID("managed_workspace"), Role: auth.WorkspaceFullAccess, Enabled: true}}}
		policyInput := managedInitializationJSON(t, document)
		run := func(name string, environment map[string]string, input []byte, wantOK bool, args ...string) []byte {
			return managedInitializationCommand(ctx, t, binaries[name], environment, input, wantOK, args...)
		}
		run("prepare", base, []byte(`{"roles":{}}`), false)
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace`).Scan(&objects); err != nil || objects != 0 {
			t.Fatal("failed initialization mutated the empty target")
		}
		// An unexpected nonempty target is rejected by the actual initializer;
		// the test removes only its own fault fixture before continuing fresh.
		if _, err := admin.ExecContext(ctx, `CREATE TABLE managed_existing(value text); INSERT INTO managed_existing VALUES ('preserve-existing')`); err != nil {
			t.Fatal(err)
		}
		run("prepare", base, roleInput, false)
		var preserved string
		var initialized bool
		if err := admin.QueryRowContext(ctx, `SELECT value,to_regclass('tetral_schema_migrations') IS NOT NULL FROM managed_existing`).Scan(&preserved, &initialized); err != nil || preserved != "preserve-existing" || initialized {
			t.Fatal("rejected nonempty target was overwritten or initialized")
		}
		if _, err := admin.ExecContext(ctx, `DROP TABLE managed_existing`); err != nil {
			t.Fatal(err)
		}
		run("prepare", base, roleInput, true)
		if err := storage.VerifySchema(ctx, admin); err != nil {
			t.Fatal("actual preparation did not install canonical schema/RLS")
		}
		var checksum string
		if err := admin.QueryRowContext(ctx, `SELECT checksum FROM tetral_schema_migrations WHERE version=1`).Scan(&checksum); err != nil || checksum != storage.PostgreSQLSchemaVersionOneChecksum {
			t.Fatal("actual preparation lost canonical schema identity")
		}
		var count int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM workspaces`).Scan(&count); err != nil || count != 0 {
			t.Fatal("schema-only preparation silently bootstrapped a workspace")
		}
		// Both real grant import and real Auth startup require explicit bootstrap.
		run("policy", base, policyInput, false)
		run("auth", processEnv, nil, false)
		if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM workspaces)+(SELECT count(*) FROM auth_federation_rules)+(SELECT count(*) FROM auth_identities)+(SELECT count(*) FROM auth_workspace_grants)+(SELECT count(*) FROM api_keys)`).Scan(&count); err != nil || count != 0 {
			t.Fatal("failed pre-bootstrap import/startup created hidden workspace, policy or keys")
		}
		run("bootstrap", bootstrapEnv, nil, true, "--workspace-id", "managed_workspace", "--name", "Managed Installation")
		changes := run("policy", base, policyInput, true)
		managedInitializationChanges(t, changes, 3)
		for _, role := range declarations.Roles {
			var safe bool
			if err := admin.QueryRowContext(ctx, `SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolinherit FROM pg_roles WHERE rolname=$1`, role.Name).Scan(&safe); err != nil || !safe {
				t.Fatal("preparation installed a privileged serving/migration role")
			}
		}
		process := managedInitializationStartAuth(ctx, t, binaries["auth"], processEnv)
		managedInitializationProbes(ctx, t, process, processEnv["ENGINE_API_KEY"])
		process.stop(t)
		before := managedInitializationRows(ctx, t, admin)
		run("prepare", base, roleInput, true)
		run("bootstrap", bootstrapEnv, nil, true, "--workspace-id", "managed_workspace", "--name", "Must Not Overwrite")
		managedInitializationChanges(t, run("policy", base, policyInput, true), 0)
		if after := managedInitializationRows(ctx, t, admin); after != before {
			t.Fatal("repeat initialization changed workspace/grant/key identities or existing data")
		}
		process = managedInitializationStartAuth(ctx, t, binaries["auth"], processEnv)
		// Restart's matching bootstrap refresh is a no-op, before any touch probe.
		if after := managedInitializationRows(ctx, t, admin); after != before {
			t.Fatal("repeat Auth startup changed existing bootstrap identity or data")
		}
		managedInitializationProbes(ctx, t, process, processEnv["ENGINE_API_KEY"])
		process.stop(t)
		before = managedInitializationRows(ctx, t, admin)
		run("prepare", base, []byte(`{"roles":{}}`), false)
		failed := document
		failed.FederationRules = append(append([]auth.FederationRule{}, document.FederationRules...), auth.FederationRule{ID: "failed_rule", OrganizationID: "managed_org", Issuer: "https://issuer.failure.test", Audience: "tetral-engine", JWKSURL: "https://issuer.failure.test/keys", Algorithm: "RS256", Enabled: true})
		failed.WorkspaceGrants = []auth.WorkspaceGrant{{ID: "failed_grant", IdentityID: "managed_identity", WorkspaceID: workspace.ID("missing_workspace"), Role: auth.WorkspaceFullAccess, Enabled: true}}
		run("policy", base, managedInitializationJSON(t, failed), false)
		if after := managedInitializationRows(ctx, t, admin); after != before {
			t.Fatal("failed preparation/import destroyed or partially changed seeded data")
		}
		// Repair the failed release gate explicitly; the old policy remains valid.
		run("prepare", base, roleInput, true)
		managedInitializationChanges(t, run("policy", base, policyInput, true), 0)
		process = managedInitializationStartAuth(ctx, t, binaries["auth"], processEnv)
		managedInitializationProbes(ctx, t, process, processEnv["ENGINE_API_KEY"])
		process.stop(t)
		t.Log("managed_initialization_assertion=commands_order_repeat_failure_preserved")
	})
}

func managedInitializationJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func managedInitializationClone(values map[string]string) map[string]string {
	copy := map[string]string{}
	for k, v := range values {
		copy[k] = v
	}
	return copy
}
func managedInitializationEnvironment(values map[string]string) []string {
	environment := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "TETRAL_DATABASE_") || strings.HasPrefix(key, "TETRAL_AUTH_") || strings.HasPrefix(key, "TETRAL_HTTP_") || strings.HasPrefix(key, "ENGINE_") {
			continue
		}
		if _, set := values[key]; !set {
			environment = append(environment, entry)
		}
	}
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	return environment
}
func managedInitializationCommand(ctx context.Context, t *testing.T, binary string, values map[string]string, input []byte, wantOK bool, args ...string) []byte {
	t.Helper()
	commandCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	command := exec.CommandContext(commandCtx, binary, args...) //nolint:gosec // Built production command and owning fixed arguments.
	command.Env = managedInitializationEnvironment(values)
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if commandCtx.Err() != nil || (err == nil) != wantOK {
		t.Fatalf("actual initialization command %s success=%t want=%t", filepath.Base(binary), err == nil, wantOK)
	}
	if !wantOK {
		var exited *exec.ExitError
		if !errors.As(err, &exited) || exited.ExitCode() != 1 {
			t.Fatal("initialization prerequisite did not produce the actual command's failure exit")
		}
	}
	for _, secret := range []string{values["TETRAL_DATABASE_ADMIN_URL"], values["TETRAL_DATABASE_URL"], values["TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64"]} {
		if secret != "" && bytes.Contains(output, []byte(secret)) {
			t.Fatal("initialization command emitted protected material")
		}
	}
	t.Logf("managed_initialization_command=%s success=%t", filepath.Base(binary), wantOK)
	return output
}
func managedInitializationChanges(t *testing.T, output []byte, want int) {
	t.Helper()
	var result struct {
		Changes []auth.PolicyChange `json:"changes"`
	}
	if err := json.Unmarshal(output, &result); err != nil || len(result.Changes) != want {
		t.Fatal("actual policy command lost exact change/no-op result")
	}
}
func managedInitializationRows(ctx context.Context, t *testing.T, admin *sql.DB) string {
	t.Helper()
	var rows string
	if err := admin.QueryRowContext(ctx, `SELECT jsonb_build_object('schema',(SELECT jsonb_agg(to_jsonb(s) ORDER BY version) FROM tetral_schema_migrations s),'workspaces',(SELECT jsonb_agg(to_jsonb(w) ORDER BY id) FROM workspaces w),'rules',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM auth_federation_rules r),'identities',(SELECT jsonb_agg(to_jsonb(i) ORDER BY id) FROM auth_identities i),'grants',(SELECT jsonb_agg(to_jsonb(g) ORDER BY id) FROM auth_workspace_grants g),'keys',(SELECT jsonb_agg(to_jsonb(k) ORDER BY id) FROM api_keys k))::text`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}
func managedInitializationReserveListeners(t *testing.T, values map[string]string) func() {
	t.Helper()
	addresses, release := publicReserveAddresses(t, 3)
	values["TETRAL_AUTH_HTTP_ADDR"] = addresses[0]
	values["TETRAL_AUTH_METRICS_ADDR"] = addresses[1]
	values["TETRAL_AUTH_GRPC_ADDR"] = addresses[2]
	return release
}

func managedInitializationStartAuth(ctx context.Context, t *testing.T, binary string, values map[string]string) *oidcAuthProcess {
	t.Helper()
	release := managedInitializationReserveListeners(t, values)
	process := &oidcAuthProcess{joined: make(chan error, 1), URL: "http://" + values["TETRAL_AUTH_HTTP_ADDR"], MetricsURL: "http://" + values["TETRAL_AUTH_METRICS_ADDR"], GRPCAddress: values["TETRAL_AUTH_GRPC_ADDR"]}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(values["TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64"])
	if err != nil {
		t.Fatal(err)
	}
	process.verifier, err = auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatal(err)
	}
	process.command = exec.Command(binary) //nolint:gosec // Actual built Auth command, explicitly declared serving role.
	process.command.Env = managedInitializationEnvironment(values)
	process.command.Stdout, process.command.Stderr = &process.output, &process.output
	release()
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.joined <- process.command.Wait() }()
	t.Cleanup(func() { process.stop(t) })
	ready, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	for {
		request, err := http.NewRequestWithContext(ready, "GET", process.URL+"/ready", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			if err := response.Body.Close(); err != nil {
				t.Fatal("initialization readiness response close failed")
			}
			if response.StatusCode == 200 {
				return process
			}
		}
		select {
		case err := <-process.joined:
			process.joined <- err
			t.Fatalf("actual initialized Auth exited before readiness: %v; %s", err, publicStartupDiagnostic(process.output.String()))
		case <-ready.Done():
			t.Fatalf("actual initialized Auth did not become ready: %v; %s", ready.Err(), publicStartupDiagnostic(process.output.String()))
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func managedInitializationProbes(ctx context.Context, t *testing.T, process *oidcAuthProcess, key string) {
	t.Helper()
	for _, test := range []struct {
		credential string
		status     int
	}{{key, 200}, {"invalid-managed-key", 401}} {
		response, err := directEdgeCheck(ctx, process.GRPCAddress, "GET", "/v1/sessions?limit=1", "managed_probe", http.Header{"X-Api-Key": []string{test.credential}})
		if err != nil {
			t.Fatal("initialized Auth Check transport failed")
		}
		status, signed := directEdgeCheckStatus(response)
		if status != test.status {
			t.Fatal("initialized Auth lost allowed/denied probe")
		}
		if status == 200 {
			principal, claims, err := process.verifier.Verify(signed, "GET", "/v1/sessions")
			if err != nil || principal.Workspace.ID != "managed_workspace" || principal.APIKeyID == "" || claims.RequestID != "managed_probe" {
				t.Fatal("initialized Auth lacks bound declared-workspace principal")
			}
		} else if signed != "" {
			t.Fatal("denied initialization probe minted principal")
		}
	}
}
func managedInitializationDropRoles(t *testing.T, admin *sql.DB, declarations database.RoleDeclarations) {
	t.Helper()
	var owner string
	if err := admin.QueryRow(`SELECT current_user`).Scan(&owner); err != nil {
		t.Error("managed initialization cleanup cannot inspect owner")
		return
	}
	for _, role := range declarations.Roles {
		var exists bool
		if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role.Name).Scan(&exists); err != nil {
			t.Error(err)
			return
		}
		if !exists {
			continue
		}
		for _, statement := range []string{"REASSIGN OWNED BY " + pgx.Identifier{role.Name}.Sanitize() + " TO " + pgx.Identifier{owner}.Sanitize(), "DROP OWNED BY " + pgx.Identifier{role.Name}.Sanitize(), "DROP ROLE " + pgx.Identifier{role.Name}.Sanitize()} {
			if _, err := admin.Exec(statement); err != nil {
				t.Error("managed initialization role cleanup failed")
				return
			}
		}
	}
}
