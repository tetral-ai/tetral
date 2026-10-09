package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLAuthPolicyCommand(t *testing.T) {
	if os.Getenv("TETRAL_POLICY_COMMAND_TEST_CHILD") != "yes" {
		postgres := transporttest.NewPostgreSQL(t)
		connection, err := url.Parse(postgres.URL)
		if err != nil {
			t.Fatal(err)
		}
		query := connection.Query()
		query.Set("sslmode", "require")
		connection.RawQuery = query.Encode()
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^TestPostgreSQLAuthPolicyCommand$", "-test.v") //nolint:gosec // Fixed owning root in current test executable.
		command.Env = append(os.Environ(), "TETRAL_POLICY_COMMAND_TEST_CHILD=yes", storagetest.EnvTestDatabaseURL+"="+connection.String(), storagetest.EnvTestRunID+"=", "TETRAL_POLICY_COMMAND_TEST_CA="+filepath.Join(postgres.Directory, "ca.pem"))
		output, err := command.CombinedOutput()
		// Child emits only safe counts, never protected connection or policy contents.
		t.Logf("protected policy command assertions:\n%s", output)
		if err != nil || !bytes.Contains(output, []byte("policy_command_assertion=protected_admin_noop_serving_denied")) {
			t.Fatal("protected administrative command proof did not complete")
		}
		return
	}
	admin := storagetest.NewPostgreSQLAdminDB(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "auth")
	document := auth.PolicyDocument{
		FederationRules: []auth.FederationRule{{ID: "rule_command", OrganizationID: "organization_command", Issuer: "https://issuer.command.test", Audience: "tetral-engine", JWKSURL: "https://issuer.command.test/keys", Algorithm: "RS256", Enabled: true}},
		Identities:      []auth.IdentityBinding{{ID: "identity_command", OrganizationID: "organization_command", Issuer: "https://issuer.command.test", Subject: "subject_command", Kind: auth.IdentityHuman, Enabled: true}},
		WorkspaceGrants: []auth.WorkspaceGrant{{ID: "grant_command", IdentityID: "identity_command", WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true}},
	}
	input, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	administrativeURL := storagetest.AdminDatabaseURL(t, admin)
	getenv := func(name string) string {
		switch name {
		case "TETRAL_DATABASE_ADMIN_URL":
			return administrativeURL
		case "TETRAL_DATABASE_TLS_CA_PATH":
			return os.Getenv("TETRAL_POLICY_COMMAND_TEST_CA")
		case "TETRAL_DATABASE_TLS_SERVER_NAME":
			return "postgres.transport.test"
		default:
			return ""
		}
	}
	for _, want := range []int{3, 0} {
		var output bytes.Buffer
		if err := run(context.Background(), getenv, bytes.NewReader(input), &output); err != nil {
			t.Fatalf("protected policy import failed: %T", err)
		}
		var result struct {
			Changes []auth.PolicyChange `json:"changes"`
		}
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Changes) != want {
			t.Fatalf("policy changes=%d want=%d", len(result.Changes), want)
		}
		if strings.Contains(output.String(), "https:") || strings.Contains(output.String(), "subject_command") || strings.Contains(output.String(), "postgres:") {
			t.Fatal("command emitted policy or connection material")
		}
	}
	administrativeURL = storagetest.RuntimeDatabaseURL(t, workload.DB)
	var output bytes.Buffer
	err = run(context.Background(), getenv, bytes.NewReader(input), &output)
	var denied *auth.PermissionError
	if !errors.As(err, &denied) {
		t.Fatalf("serving policy command did not reach permission denial: %T", err)
	}
	if output.Len() != 0 {
		t.Fatal("denied administrative import emitted changes")
	}
	var count int
	if err := admin.QueryRow(`SELECT count(*) FROM auth_workspace_grants WHERE id='grant_command' AND revision=1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("serving-role denial changed existing policy")
	}
	t.Log("policy_command_assertion=protected_admin_noop_serving_denied")
}
