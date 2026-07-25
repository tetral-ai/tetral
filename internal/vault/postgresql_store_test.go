package vault_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testEncryptionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func staticBearerAuth(token string) vault.CredentialAuth {
	hostToken := strings.NewReplacer("_", "-", ".", "-", ":", "-").Replace(strings.ToLower(token))
	return vault.CredentialAuth{
		Type:         "static_bearer",
		MCPServerURL: "https://" + hostToken + ".example.com/mcp",
		Token:        token,
	}
}

func newPGVaultEnv(t *testing.T) (runtime *sql.DB, admin *sql.DB, encryptor *vault.Encryptor) {
	t.Helper()
	r, a := storagetest.NewPostgreSQLDBWithAdmin(t)
	enc, err := vault.NewEncryptor(testEncryptionKey)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return r, a, enc
}

func seedVaultWorkspace(t *testing.T, admin *sql.DB, ws workspace.ID, name string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`, string(ws), name); err != nil {
		t.Fatalf("seed workspace %s: %v", ws, err)
	}
}

func seedVaultSession(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string) {
	t.Helper()
	ctx := context.Background()
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $3, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(ws), agentID, "agent-"+sessionID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')`,
		string(ws), agentVersionID, agentID, "hash-"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(ws), environmentID, "environment-"+sessionID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO sessions (
			workspace_id, id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at
		) VALUES ($1, $2, 'session', 'idle', 'active', $3, 1, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(ws), sessionID, agentID, environmentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func TestPostgreSQLVaultStoreCreateGetDelete(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := store.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{
		DisplayName: "primary",
		Metadata:    vault.StringMap{"team": "runtime"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if v.Metadata["team"] != "runtime" {
		t.Fatalf("created metadata = %v; want team runtime", v.Metadata)
	}
	if v.CreatedAt.IsZero() || v.UpdatedAt.IsZero() {
		t.Fatalf("created timestamps must be set: created=%v updated=%v", v.CreatedAt, v.UpdatedAt)
	}
	got, err := store.Get(ctx, workspace.DefaultID, v.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DisplayName != "primary" {
		t.Errorf("DisplayName = %q; want primary", got.DisplayName)
	}
	if got.ID != v.ID || got.Type != "vault" {
		t.Errorf("got identity = id %q type %q; want %q vault", got.ID, got.Type, v.ID)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() || got.ArchivedAt != nil {
		t.Errorf("got timestamps/archive = created %v updated %v archived %v", got.CreatedAt, got.UpdatedAt, got.ArchivedAt)
	}
	if got.Metadata["team"] != "runtime" {
		t.Errorf("got metadata = %v; want team runtime", got.Metadata)
	}
	credential, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "child",
		Auth:        staticBearerAuth("secret"),
	})
	if err != nil {
		t.Fatalf("Create credential: %v", err)
	}
	deleted, err := store.Delete(ctx, workspace.DefaultID, v.ID)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.ID != v.ID || deleted.Type != "vault_deleted" {
		t.Fatalf("delete result = %+v", deleted)
	}
	if _, err := store.Get(ctx, workspace.DefaultID, v.ID); err == nil {
		t.Error("Get after Delete must fail")
	}
	if _, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, credential.ID); err == nil {
		t.Fatal("GetMetadata after parent Delete must fail")
	} else {
		var notFound *vault.NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("GetMetadata after parent Delete error = %T %v; want NotFoundError", err, err)
		}
	}
	var credentialRows int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM credentials WHERE vault_id = $1`,
		v.ID,
	).Scan(&credentialRows); err != nil {
		t.Fatalf("count deleted vault credentials: %v", err)
	}
	if credentialRows != 0 {
		t.Fatalf("credential rows after parent Delete = %d; want 0", credentialRows)
	}
}

func TestPostgreSQLVaultStoreCreateEnforcesDomainLimits(t *testing.T) {
	runtime, admin, _ := newPGVaultEnv(t)
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()

	tooManyMetadataPairs := vault.StringMap{}
	for i := 0; i < 17; i++ {
		tooManyMetadataPairs[fmt.Sprintf("key-%02d", i)] = "value"
	}

	cases := []struct {
		name    string
		request vault.CreateVaultRequest
	}{
		{
			name:    "empty display name",
			request: vault.CreateVaultRequest{DisplayName: ""},
		},
		{
			name:    "over display name limit",
			request: vault.CreateVaultRequest{DisplayName: repeatedRunes("名", 256)},
		},
		{
			name: "too many metadata pairs",
			request: vault.CreateVaultRequest{
				DisplayName: "metadata-pairs",
				Metadata:    tooManyMetadataPairs,
			},
		},
		{
			name: "metadata key over limit",
			request: vault.CreateVaultRequest{
				DisplayName: "metadata-key",
				Metadata:    vault.StringMap{repeatedRunes("鍵", 65): "value"},
			},
		},
		{
			name: "metadata value over limit",
			request: vault.CreateVaultRequest{
				DisplayName: "metadata-value",
				Metadata:    vault.StringMap{"key": repeatedRunes("値", 513)},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.Create(ctx, workspace.DefaultID, tc.request); err == nil {
				t.Fatal("Create must reject invalid direct-store request")
			} else {
				var validationErr *vault.ValidationError
				if !errors.As(err, &validationErr) {
					t.Fatalf("Create error = %T %v; want ValidationError", err, err)
				}
			}
			assertVaultRowCount(t, admin, 0)
		})
	}
}

func TestPostgreSQLVaultStoreCreateNormalizesNilMetadata(t *testing.T) {
	runtime, admin, _ := newPGVaultEnv(t)
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "nil-metadata"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Metadata == nil || len(created.Metadata) != 0 {
		t.Fatalf("created metadata = %#v; want empty map", created.Metadata)
	}

	got, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Metadata == nil || len(got.Metadata) != 0 {
		t.Fatalf("stored metadata = %#v; want empty map", got.Metadata)
	}

	var metadataJSON string
	if err := admin.QueryRowContext(ctx, `SELECT metadata_json FROM vaults WHERE id = $1`, created.ID).Scan(&metadataJSON); err != nil {
		t.Fatalf("read metadata_json: %v", err)
	}
	if metadataJSON != "{}" {
		t.Fatalf("metadata_json = %q; want {}", metadataJSON)
	}
}

func TestPostgreSQLVaultStoreUpdateRejectsArchived(t *testing.T) {
	runtime, _, _ := newPGVaultEnv(t)
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{
		DisplayName: "before",
		Metadata:    vault.StringMap{"keep": "old", "drop": "yes"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	patch, err := vault.DecodeUpdateVaultRequest([]byte(`{"display_name":"after","metadata":{"keep":"new","drop":null,"added":"yes"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateVaultRequest: %v", err)
	}
	updated, err := store.Update(ctx, workspace.DefaultID, created.ID, patch)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.DisplayName != "after" || updated.Metadata["keep"] != "new" || updated.Metadata["added"] != "yes" {
		t.Fatalf("updated vault = %+v", updated)
	}
	if _, ok := updated.Metadata["drop"]; ok {
		t.Fatalf("metadata drop key still present: %v", updated.Metadata)
	}
	if updated.UpdatedAt.IsZero() {
		t.Fatal("updated_at must be set after update")
	}

	archived, err := store.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if archived.ArchivedAt == nil {
		t.Fatal("Archive must set archived_at")
	}
	if _, err := store.Update(ctx, workspace.DefaultID, created.ID, patch); err == nil {
		t.Fatal("Update must reject archived vault")
	}
}

func TestPostgreSQLVaultStoreArchiveCascadesCredentialsAndIsIdempotent(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	created, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "archive"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	credential, err := creds.Create(ctx, workspace.DefaultID, created.ID, vault.CreateCredentialRequest{
		DisplayName: "key",
		Auth:        staticBearerAuth("secret"),
	})
	if err != nil {
		t.Fatalf("Create credential: %v", err)
	}
	archived, err := vaults.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if archived.ArchivedAt == nil {
		t.Fatal("Archive must set vault archived_at")
	}
	var credentialArchivedAt sql.NullString
	var encryptedAuth []byte
	if err := admin.QueryRowContext(ctx,
		`SELECT archived_at, encrypted_auth FROM credentials WHERE id = $1`,
		credential.ID,
	).Scan(&credentialArchivedAt, &encryptedAuth); err != nil {
		t.Fatalf("read archived credential: %v", err)
	}
	if !credentialArchivedAt.Valid {
		t.Fatal("Archive must set credential archived_at")
	}
	if encryptedAuth != nil {
		t.Fatal("Archive must purge credential encrypted_auth")
	}

	again, err := vaults.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive again: %v", err)
	}
	if again.ArchivedAt == nil || !again.ArchivedAt.Equal(*archived.ArchivedAt) {
		t.Fatalf("Archive must be idempotent: first=%v second=%v", archived.ArchivedAt, again.ArchivedAt)
	}
	if _, err := creds.Create(ctx, workspace.DefaultID, created.ID, vault.CreateCredentialRequest{
		DisplayName: "blocked",
		Auth:        staticBearerAuth("secret2"),
	}); err == nil {
		t.Fatal("Create credential must reject archived parent vault")
	}
}

func TestPostgreSQLCredentialCreateWaitsForConcurrentVaultArchive(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	const vaultID = "vlt_race"
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at) VALUES ('default', $1, 'race', $2, $2)`,
		vaultID, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed vault: %v", err)
	}

	lockTx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	defer func() { _ = lockTx.Rollback() }()
	if _, err := lockTx.ExecContext(ctx,
		`UPDATE vaults SET archived_at = $1, updated_at = $1 WHERE id = $2`,
		"2026-01-02T00:00:00Z", vaultID,
	); err != nil {
		t.Fatalf("archive vault without commit: %v", err)
	}
	var lockerPID int
	if err := lockTx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&lockerPID); err != nil {
		t.Fatalf("read locker pid: %v", err)
	}

	createErrCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, createErr := creds.Create(ctx, workspace.DefaultID, vaultID, vault.CreateCredentialRequest{
			DisplayName: "late",
			Auth:        staticBearerAuth("late-secret"),
		})
		createErrCh <- createErr
	}()

	waitForLockWaiters(t, admin, lockerPID, 1)
	if err := lockTx.Commit(); err != nil {
		t.Fatalf("commit archive: %v", err)
	}
	wg.Wait()
	createErr := <-createErrCh
	if createErr == nil {
		t.Fatal("credential create must reject after concurrent archive wins")
	}
	var validationErr *vault.ValidationError
	if !errors.As(createErr, &validationErr) {
		t.Fatalf("create error = %T %v; want ValidationError", createErr, createErr)
	}

	var activeSecrets int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM credentials WHERE vault_id = $1 AND archived_at IS NULL AND encrypted_auth IS NOT NULL`,
		vaultID,
	).Scan(&activeSecrets); err != nil {
		t.Fatalf("count active secrets: %v", err)
	}
	if activeSecrets != 0 {
		t.Fatalf("active secret-bearing credentials under archived vault = %d; want 0", activeSecrets)
	}
}

func waitForLockWaiters(t *testing.T, admin *sql.DB, lockerPID int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*)
			   FROM pg_locks waiting_lock
			   JOIN pg_locks granted_lock
			     ON waiting_lock.locktype = granted_lock.locktype
			    AND waiting_lock.relation IS NOT DISTINCT FROM granted_lock.relation
			    AND waiting_lock.page IS NOT DISTINCT FROM granted_lock.page
			    AND waiting_lock.tuple IS NOT DISTINCT FROM granted_lock.tuple
			    AND waiting_lock.transactionid IS NOT DISTINCT FROM granted_lock.transactionid
			    AND waiting_lock.classid IS NOT DISTINCT FROM granted_lock.classid
			    AND waiting_lock.objid IS NOT DISTINCT FROM granted_lock.objid
			    AND waiting_lock.objsubid IS NOT DISTINCT FROM granted_lock.objsubid
			  WHERE NOT waiting_lock.granted
			    AND granted_lock.granted
			    AND granted_lock.pid = $1`,
			lockerPID,
		).Scan(&waiters); err != nil {
			t.Fatalf("check lock waiters: %v", err)
		}
		if waiters >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("got fewer than %d waiter(s) on the locked vault row", want)
}

func TestPostgreSQLVaultStoreListPaginationAndArchiveFilter(t *testing.T) {
	runtime, _, _ := newPGVaultEnv(t)
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()
	var created []*vault.Vault
	for i := 0; i < 3; i++ {
		item, err := store.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: fmt.Sprintf("vault-%d", i)})
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		created = append(created, item)
	}
	if _, err := store.Archive(ctx, workspace.DefaultID, created[1].ID); err != nil {
		t.Fatalf("Archive middle: %v", err)
	}
	first, err := store.List(ctx, workspace.DefaultID, vault.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List first: %v", err)
	}
	if len(first.Data) != 1 || first.Data[0].ID != created[0].ID || first.NextPage == nil {
		t.Fatalf("first page = %+v", first)
	}
	second, err := store.List(ctx, workspace.DefaultID, vault.ListOptions{Limit: 1, Page: *first.NextPage})
	if err != nil {
		t.Fatalf("List second: %v", err)
	}
	if len(second.Data) != 1 || second.Data[0].ID != created[2].ID || second.NextPage != nil {
		t.Fatalf("second page = %+v", second)
	}
	withArchived, err := store.List(ctx, workspace.DefaultID, vault.ListOptions{Limit: 10, IncludeArchived: true})
	if err != nil {
		t.Fatalf("List include archived: %v", err)
	}
	if len(withArchived.Data) != 3 || withArchived.Data[1].ArchivedAt == nil {
		t.Fatalf("include archived page = %+v", withArchived)
	}
	if _, err := store.List(ctx, workspace.DefaultID, vault.ListOptions{Limit: 1, Page: *first.NextPage, IncludeArchived: true}); err == nil {
		t.Fatal("page token must reject changed include_archived filter")
	}
}

func TestPostgreSQLVaultStoreListRejectsCrossWorkspacePage(t *testing.T) {
	runtime, admin, _ := newPGVaultEnv(t)
	seedVaultWorkspace(t, admin, "workspace_b", "B")
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := store.Create(ctx, "workspace_b", vault.CreateVaultRequest{DisplayName: fmt.Sprintf("b-%d", i)}); err != nil {
			t.Fatalf("Create workspace_b vault %d: %v", i, err)
		}
	}
	page, err := store.List(ctx, "workspace_b", vault.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List workspace_b: %v", err)
	}
	if page.NextPage == nil {
		t.Fatal("workspace_b first page must return next_page")
	}
	if _, err := store.List(ctx, workspace.DefaultID, vault.ListOptions{Limit: 1, Page: *page.NextPage}); err == nil {
		t.Fatal("default workspace must reject workspace_b vault page token")
	}
}

func TestPostgreSQLCredentialStoreListPaginationAndArchiveFilter(t *testing.T) {
	runtime, _, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	parentVault, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "credential-pages"})
	if err != nil {
		t.Fatalf("Create parent vault: %v", err)
	}
	otherVault, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "other-pages"})
	if err != nil {
		t.Fatalf("Create other vault: %v", err)
	}
	created := make([]*vault.CredentialMetadata, 0, 3)
	for i := 0; i < 3; i++ {
		credential, err := creds.Create(ctx, workspace.DefaultID, parentVault.ID, vault.CreateCredentialRequest{
			DisplayName: fmt.Sprintf("credential-%d", i),
			Auth:        staticBearerAuth(fmt.Sprintf("secret-%d", i)),
		})
		if err != nil {
			t.Fatalf("Create credential %d: %v", i, err)
		}
		created = append(created, credential)
	}
	if _, err := creds.Archive(ctx, workspace.DefaultID, parentVault.ID, created[1].ID); err != nil {
		t.Fatalf("Archive middle credential: %v", err)
	}

	first, err := creds.List(ctx, workspace.DefaultID, parentVault.ID, vault.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List first page: %v", err)
	}
	if len(first.Data) != 1 || first.Data[0].ID != created[0].ID || first.NextPage == nil {
		t.Fatalf("first page = %+v", first)
	}
	second, err := creds.List(ctx, workspace.DefaultID, parentVault.ID, vault.ListOptions{Limit: 1, Page: *first.NextPage})
	if err != nil {
		t.Fatalf("List second page: %v", err)
	}
	if len(second.Data) != 1 || second.Data[0].ID != created[2].ID || second.NextPage != nil {
		t.Fatalf("second page = %+v", second)
	}
	withArchived, err := creds.List(ctx, workspace.DefaultID, parentVault.ID, vault.ListOptions{Limit: 10, IncludeArchived: true})
	if err != nil {
		t.Fatalf("List include archived: %v", err)
	}
	if len(withArchived.Data) != 3 ||
		withArchived.Data[0].ID != created[0].ID ||
		withArchived.Data[1].ID != created[1].ID ||
		withArchived.Data[1].ArchivedAt == nil ||
		withArchived.Data[2].ID != created[2].ID {
		t.Fatalf("include archived page = %+v", withArchived)
	}
	if _, err := creds.List(ctx, workspace.DefaultID, otherVault.ID, vault.ListOptions{Limit: 1, Page: *first.NextPage}); err == nil {
		t.Fatal("page token must reject changed parent vault")
	}
	if _, err := creds.List(ctx, workspace.DefaultID, parentVault.ID, vault.ListOptions{Limit: 1, Page: *first.NextPage, IncludeArchived: true}); err == nil {
		t.Fatal("page token must reject changed include_archived filter")
	}
}

func TestPostgreSQLVaultStoreCrossWorkspaceIsolation(t *testing.T) {
	runtime, admin, _ := newPGVaultEnv(t)
	seedVaultWorkspace(t, admin, "workspace_b", "B")
	store := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()
	bVault, err := store.Create(ctx, "workspace_b", vault.CreateVaultRequest{DisplayName: "b"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := store.Get(ctx, workspace.DefaultID, bVault.ID); err == nil {
		t.Error("default must not Get workspace_b vault")
	}
	_, err = store.Delete(ctx, workspace.DefaultID, bVault.ID)
	if err == nil {
		t.Error("default must not Delete workspace_b vault")
	}
	var notFound *vault.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Delete cross-workspace error = %T %v; want NotFoundError", err, err)
	}
}

func TestPostgreSQLCredentialStoreEncryptionAtRestAndScoping(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "v1"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	const token = "tetral_secret_value_xxxxxxxxxxxxxxxxxx" //nolint:gosec // G101: synthetic test token, not a real secret
	created, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "primary-key",
		Auth:        staticBearerAuth(token),
	})
	if err != nil {
		t.Fatalf("Create credential: %v", err)
	}

	// Encrypted at rest: read the raw encrypted_auth bytes through
	// the admin (BYPASSRLS) connection so the runtime role's RLS
	// requirement does not gate this assertion. The bytes must NOT
	// contain the raw token in any form.
	var raw []byte
	if err := admin.QueryRowContext(ctx,
		`SELECT encrypted_auth FROM credentials WHERE id = $1`,
		created.ID,
	).Scan(&raw); err != nil {
		t.Fatalf("admin read encrypted_auth: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("encrypted_auth is empty; row missing")
	}
	if bytes.Contains(raw, []byte(token)) {
		t.Errorf("credentials.encrypted_auth contains raw token bytes; encryption-at-rest broken")
	}
	if string(raw) == token {
		t.Errorf("credentials.encrypted_auth equals raw token; encryption-at-rest broken")
	}

	// DecryptCredential through the workspace-scoped path returns
	// the secret.
	dec, err := creds.DecryptCredential(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("DecryptCredential: %v", err)
	}
	if dec.Token != token {
		t.Errorf("decrypted token = %q; want %q", dec.Token, token)
	}

	// GetMetadata never returns secrets — explicit zero-value check
	// on every secret-bearing field of the public auth view.
	meta, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, created.ID)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if meta.Auth.Type != "static_bearer" || meta.Auth.MCPServerURL == "" {
		t.Errorf("public auth = %+v; want static_bearer metadata", meta.Auth)
	}
	// Defense-in-depth: the JSON serialization of the metadata must
	// not contain the raw secret token even as a stray byte sequence.
	encoded, jsonErr := json.Marshal(meta)
	if jsonErr != nil {
		t.Fatalf("marshal metadata: %v", jsonErr)
	}
	if bytes.Contains(encoded, []byte(token)) {
		t.Errorf("CredentialMetadata JSON serialization leaks raw token: %s", encoded)
	}
}

func TestPostgreSQLCredentialStoreAuthVariantsPublicRedactionAndSecrets(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "variants"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}

	staticBearer, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "static",
		Auth: vault.CredentialAuth{
			Type:         "static_bearer",
			MCPServerURL: "https://MCP.Example.com/path",
			Token:        "static-secret-token",
		},
	})
	if err != nil {
		t.Fatalf("Create static_bearer: %v", err)
	}
	if staticBearer.Auth.MCPServerURL != "https://mcp.example.com/path" {
		t.Fatalf("static bearer URL = %q; want canonical host", staticBearer.Auth.MCPServerURL)
	}
	assertCredentialJSONOmits(t, staticBearer, "static-secret-token")
	assertEncryptedAuthOmits(t, admin, staticBearer.ID, "static-secret-token")
	assertProviderCredentialColumnsAbsent(t, admin, staticBearer.ID)

	minimalProviderOAuth, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "minimal provider oauth",
		Auth: vault.CredentialAuth{ //nolint:gosec // G101: synthetic test secret sentinel, not a real credential
			Type:        "provider_oauth",
			ProviderID:  "deepseek",
			AccessMode:  "oauth",
			AccessToken: "provider-oauth-minimal-access-secret",
		},
	})
	if err != nil {
		t.Fatalf("Create minimal provider_oauth: %v", err)
	}
	if minimalProviderOAuth.Auth.Type != "provider_oauth" ||
		minimalProviderOAuth.Auth.ProviderID != "deepseek" ||
		minimalProviderOAuth.Auth.AccessMode != "oauth" ||
		minimalProviderOAuth.Auth.ExpiresAt != "" ||
		minimalProviderOAuth.Auth.AccountID != "" {
		t.Fatalf("minimal provider_oauth public auth = %+v", minimalProviderOAuth.Auth)
	}
	assertCredentialJSONOmits(t, minimalProviderOAuth, "provider-oauth-minimal-access-secret")
	assertEncryptedAuthOmits(t, admin, minimalProviderOAuth.ID, "provider-oauth-minimal-access-secret")
	assertProviderCredentialColumns(t, admin, minimalProviderOAuth.ID, "deepseek", "oauth", "provider-oauth-minimal-access-secret")
	decryptedMinimalProviderOAuth, err := creds.DecryptCredential(ctx, workspace.DefaultID, minimalProviderOAuth.ID)
	if err != nil {
		t.Fatalf("Decrypt minimal provider_oauth: %v", err)
	}
	if decryptedMinimalProviderOAuth.AccessToken != "provider-oauth-minimal-access-secret" || decryptedMinimalProviderOAuth.RefreshToken != "" {
		t.Fatalf("minimal provider_oauth secret auth = %+v", decryptedMinimalProviderOAuth)
	}

	oauth, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "oauth",
		Auth: vault.CredentialAuth{ //nolint:gosec // G101: synthetic test secret sentinels, not real credentials
			Type:         "mcp_oauth",
			MCPServerURL: "https://OAuth.Example.com/mcp",
			AccessToken:  "oauth-access-secret",
			ExpiresAt:    "2026-05-04T05:00:00Z",
			Refresh: &vault.CredentialOAuthRefresh{ //nolint:gosec // G101: synthetic test secret sentinels, not real credentials
				RefreshToken:  "oauth-refresh-secret",
				ClientID:      "client-123",
				TokenEndpoint: "https://Auth.Example.com/token",
				Scope:         "read write",
				Resource:      "https://resource.example.com",
				TokenEndpointAuth: &vault.CredentialTokenEndpointAuth{ //nolint:gosec // G101: synthetic test secret sentinel, not a real credential
					Type:         "client_secret_basic",
					ClientSecret: "oauth-client-secret",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Create mcp_oauth: %v", err)
	}
	if oauth.Auth.MCPServerURL != "https://oauth.example.com/mcp" ||
		oauth.Auth.Refresh == nil ||
		oauth.Auth.Refresh.TokenEndpoint != "https://auth.example.com/token" ||
		oauth.Auth.Refresh.TokenEndpointAuth.Type != "client_secret_basic" {
		t.Fatalf("oauth public auth = %+v", oauth.Auth)
	}
	assertCredentialJSONOmits(t, oauth, "oauth-access-secret", "oauth-refresh-secret", "oauth-client-secret")
	assertEncryptedAuthOmits(t, admin, oauth.ID, "oauth-access-secret", "oauth-refresh-secret", "oauth-client-secret")
	assertProviderCredentialColumnsAbsent(t, admin, oauth.ID)

	var publicJSON string
	if err := admin.QueryRowContext(ctx,
		`SELECT auth_public_json FROM credentials WHERE id = $1`,
		oauth.ID,
	).Scan(&publicJSON); err != nil {
		t.Fatalf("read auth_public_json: %v", err)
	}
	assertStringOmits(t, publicJSON, "oauth-access-secret", "oauth-refresh-secret", "oauth-client-secret")
}

func TestPostgreSQLCredentialStoreProviderCredentialsRedactPersistAndRotate(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "provider-variants"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}

	apiKey, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "anthropic",
		Auth: vault.CredentialAuth{ //nolint:gosec // G101: synthetic test secret sentinel, not a real credential
			Type:       "provider_api_key",
			ProviderID: "anthropic",
			AccessMode: "user_api_key",
			Token:      "provider-api-key-old",
		},
	})
	if err != nil {
		t.Fatalf("Create provider_api_key: %v", err)
	}
	if apiKey.Auth.Type != "provider_api_key" || apiKey.Auth.ProviderID != "anthropic" || apiKey.Auth.AccessMode != "user_api_key" {
		t.Fatalf("provider_api_key public auth = %+v", apiKey.Auth)
	}
	assertCredentialJSONOmits(t, apiKey, "provider-api-key-old")
	assertEncryptedAuthOmits(t, admin, apiKey.ID, "provider-api-key-old")
	decryptedAPIKey, err := creds.DecryptCredential(ctx, workspace.DefaultID, apiKey.ID)
	if err != nil {
		t.Fatalf("Decrypt provider_api_key: %v", err)
	}
	if decryptedAPIKey.ProviderID != "anthropic" || decryptedAPIKey.AccessMode != "user_api_key" || decryptedAPIKey.Token != "provider-api-key-old" {
		t.Fatalf("provider_api_key secret auth = %+v", decryptedAPIKey)
	}
	assertProviderCredentialColumns(t, admin, apiKey.ID, "anthropic", "user_api_key", "provider-api-key-old")

	updatedAPIKey, err := creds.Update(ctx, workspace.DefaultID, v.ID, apiKey.ID, mustDecodeCredentialPatch(t, `{
		"auth":{"type":"provider_api_key","provider_id":"anthropic","access_mode":"user_api_key","token":"provider-api-key-new"}
	}`))
	if err != nil {
		t.Fatalf("Update provider_api_key: %v", err)
	}
	if updatedAPIKey.Auth.ProviderID != "anthropic" || updatedAPIKey.Auth.AccessMode != "user_api_key" {
		t.Fatalf("updated provider_api_key public auth = %+v", updatedAPIKey.Auth)
	}
	decryptedAPIKey, err = creds.DecryptCredential(ctx, workspace.DefaultID, apiKey.ID)
	if err != nil {
		t.Fatalf("Decrypt updated provider_api_key: %v", err)
	}
	if decryptedAPIKey.Token != "provider-api-key-new" {
		t.Fatalf("provider_api_key token = %q; want provider-api-key-new", decryptedAPIKey.Token)
	}
	if _, err := creds.Update(ctx, workspace.DefaultID, v.ID, apiKey.ID, mustDecodeCredentialPatch(t, `{"auth":{"token":"provider-api-key-partial"}}`)); err != nil {
		t.Fatalf("Update provider_api_key without auth.type: %v", err)
	}
	decryptedAPIKey, err = creds.DecryptCredential(ctx, workspace.DefaultID, apiKey.ID)
	if err != nil {
		t.Fatalf("Decrypt partial provider_api_key: %v", err)
	}
	if decryptedAPIKey.Token != "provider-api-key-partial" {
		t.Fatalf("provider_api_key token = %q; want provider-api-key-partial", decryptedAPIKey.Token)
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{"provider id", `{"auth":{"type":"provider_api_key","provider_id":"openai","token":"provider-api-key-immutable-secret"}}`},
		{"access mode", `{"auth":{"type":"provider_api_key","access_mode":"oauth","token":"provider-api-key-mode-secret"}}`},
		{"type", `{"auth":{"type":"provider_oauth","provider_id":"anthropic","access_mode":"user_api_key","access_token":"provider-api-key-type-secret"}}`},
		{"wrong variant", `{"auth":{"access_token":"provider-api-key-wrong-secret"}}`},
	} {
		err := updateCredentialRaw(ctx, creds, v.ID, apiKey.ID, tc.body)
		if err == nil {
			t.Fatalf("provider_api_key %s update must reject", tc.name)
		}
		assertStringOmits(t, err.Error(), "provider-api-key-immutable-secret", "provider-api-key-mode-secret", "provider-api-key-type-secret", "provider-api-key-wrong-secret")
	}

	providerOAuth, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "openai",
		Auth: vault.CredentialAuth{ //nolint:gosec // G101: synthetic test secret sentinels, not real credentials
			Type:         "provider_oauth",
			ProviderID:   "openai",
			AccessMode:   "oauth",
			AccessToken:  "provider-oauth-access-old",
			RefreshToken: "provider-oauth-refresh-old",
			ExpiresAt:    "2026-05-04T05:00:00Z",
			AccountID:    "acct_old",
		},
	})
	if err != nil {
		t.Fatalf("Create provider_oauth: %v", err)
	}
	if providerOAuth.Auth.Type != "provider_oauth" ||
		providerOAuth.Auth.ProviderID != "openai" ||
		providerOAuth.Auth.AccessMode != "oauth" ||
		providerOAuth.Auth.ExpiresAt != "2026-05-04T05:00:00Z" ||
		providerOAuth.Auth.AccountID != "acct_old" {
		t.Fatalf("provider_oauth public auth = %+v", providerOAuth.Auth)
	}
	assertCredentialJSONOmits(t, providerOAuth, "provider-oauth-access-old", "provider-oauth-refresh-old")
	assertEncryptedAuthOmits(t, admin, providerOAuth.ID, "provider-oauth-access-old", "provider-oauth-refresh-old")
	decryptedProviderOAuth, err := creds.DecryptCredential(ctx, workspace.DefaultID, providerOAuth.ID)
	if err != nil {
		t.Fatalf("Decrypt provider_oauth: %v", err)
	}
	if decryptedProviderOAuth.AccessToken != "provider-oauth-access-old" ||
		decryptedProviderOAuth.RefreshToken != "provider-oauth-refresh-old" {
		t.Fatalf("provider_oauth secret auth = %+v", decryptedProviderOAuth)
	}
	assertProviderCredentialColumns(t, admin, providerOAuth.ID, "openai", "oauth", "provider-oauth-access-old", "provider-oauth-refresh-old")

	updatedProviderOAuth, err := creds.Update(ctx, workspace.DefaultID, v.ID, providerOAuth.ID, mustDecodeCredentialPatch(t, `{
		"auth":{
			"type":"provider_oauth",
			"provider_id":"openai",
			"access_mode":"oauth",
			"access_token":"provider-oauth-access-new",
			"refresh_token":"provider-oauth-refresh-new",
			"expires_at":"2026-05-04T06:00:00Z",
			"account_id":"acct_new"
		}
	}`))
	if err != nil {
		t.Fatalf("Update provider_oauth: %v", err)
	}
	if updatedProviderOAuth.Auth.ExpiresAt != "2026-05-04T06:00:00Z" || updatedProviderOAuth.Auth.AccountID != "acct_new" {
		t.Fatalf("updated provider_oauth public auth = %+v", updatedProviderOAuth.Auth)
	}
	decryptedProviderOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, providerOAuth.ID)
	if err != nil {
		t.Fatalf("Decrypt updated provider_oauth: %v", err)
	}
	if decryptedProviderOAuth.AccessToken != "provider-oauth-access-new" ||
		decryptedProviderOAuth.RefreshToken != "provider-oauth-refresh-new" ||
		decryptedProviderOAuth.AccountID != "acct_new" {
		t.Fatalf("updated provider_oauth secret auth = %+v", decryptedProviderOAuth)
	}
	if _, err := creds.Update(ctx, workspace.DefaultID, v.ID, providerOAuth.ID, mustDecodeCredentialPatch(t, `{
		"auth":{"access_token":"provider-oauth-access-partial","refresh_token":"provider-oauth-refresh-partial"}
	}`)); err != nil {
		t.Fatalf("Update provider_oauth without auth.type: %v", err)
	}
	decryptedProviderOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, providerOAuth.ID)
	if err != nil {
		t.Fatalf("Decrypt partial provider_oauth: %v", err)
	}
	if decryptedProviderOAuth.AccessToken != "provider-oauth-access-partial" ||
		decryptedProviderOAuth.RefreshToken != "provider-oauth-refresh-partial" {
		t.Fatalf("partial provider_oauth secret auth = %+v", decryptedProviderOAuth)
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty refresh token", `{"auth":{"refresh_token":""}}`},
		{"provider id", `{"auth":{"type":"provider_oauth","provider_id":"anthropic","access_token":"provider-oauth-immutable-secret"}}`},
		{"access mode", `{"auth":{"type":"provider_oauth","access_mode":"user_api_key","access_token":"provider-oauth-mode-secret"}}`},
		{"type", `{"auth":{"type":"provider_api_key","provider_id":"openai","access_mode":"oauth","token":"provider-oauth-type-secret"}}`},
		{"wrong variant", `{"auth":{"token":"provider-oauth-wrong-secret"}}`},
	} {
		err := updateCredentialRaw(ctx, creds, v.ID, providerOAuth.ID, tc.body)
		if err == nil {
			t.Fatalf("provider_oauth %s update must reject", tc.name)
		}
		assertStringOmits(t, err.Error(), "provider-oauth-immutable-secret", "provider-oauth-mode-secret", "provider-oauth-type-secret", "provider-oauth-wrong-secret")
	}

	providerOAuthAfterRejects, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, providerOAuth.ID)
	if err != nil {
		t.Fatalf("Get provider_oauth after rejects: %v", err)
	}
	decryptedProviderOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, providerOAuth.ID)
	if err != nil {
		t.Fatalf("Decrypt provider_oauth after rejects: %v", err)
	}
	if providerOAuthAfterRejects.Auth.ProviderID != "openai" ||
		providerOAuthAfterRejects.Auth.AccessMode != "oauth" ||
		providerOAuthAfterRejects.Auth.AccountID != "acct_new" ||
		decryptedProviderOAuth.AccessToken != "provider-oauth-access-partial" ||
		decryptedProviderOAuth.RefreshToken != "provider-oauth-refresh-partial" {
		t.Fatalf("provider_oauth changed after rejects: public=%+v secret=%+v", providerOAuthAfterRejects.Auth, decryptedProviderOAuth)
	}
}

func TestPostgreSQLCredentialStoreUpdateRotateSecretsAndRejectImmutableAuth(t *testing.T) {
	runtime, _, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "updates"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	staticBearer, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "runtime",
		Auth:        staticBearerAuth("old-key"),
	})
	if err != nil {
		t.Fatalf("Create static_bearer: %v", err)
	}
	updatedStaticBearer, err := creds.Update(ctx, workspace.DefaultID, v.ID, staticBearer.ID, mustDecodeCredentialPatch(t, `{
		"display_name":"runtime updated",
		"metadata":{"rotated":"yes"},
		"auth":{"type":"static_bearer","token":"new-key"}
	}`))
	if err != nil {
		t.Fatalf("Update static_bearer: %v", err)
	}
	if updatedStaticBearer.DisplayName != "runtime updated" || updatedStaticBearer.Metadata["rotated"] != "yes" {
		t.Fatalf("updated static bearer = %+v", updatedStaticBearer)
	}
	decryptedStaticBearer, err := creds.DecryptCredential(ctx, workspace.DefaultID, staticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt updated static_bearer: %v", err)
	}
	if decryptedStaticBearer.Token != "new-key" {
		t.Fatalf("decrypted static bearer token = %q; want new-key", decryptedStaticBearer.Token)
	}
	updatedStaticBearer, err = creds.Update(ctx, workspace.DefaultID, v.ID, staticBearer.ID, mustDecodeCredentialPatch(t, `{"auth":{"token":"partial-key"}}`))
	if err != nil {
		t.Fatalf("Update static_bearer without auth.type: %v", err)
	}
	if updatedStaticBearer.Auth.Type != "static_bearer" {
		t.Fatalf("static_bearer public auth after partial update = %+v", updatedStaticBearer.Auth)
	}
	decryptedStaticBearer, err = creds.DecryptCredential(ctx, workspace.DefaultID, staticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt partial static_bearer: %v", err)
	}
	if decryptedStaticBearer.Token != "partial-key" {
		t.Fatalf("decrypted static bearer token = %q; want partial-key", decryptedStaticBearer.Token)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, staticBearer.ID, `{"auth":{"token":""}}`)
	if err == nil {
		t.Fatal("static_bearer empty token rotation must reject")
	}
	var validationErr *vault.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("static_bearer empty token error = %T %v; want ValidationError", err, err)
	}
	decryptedStaticBearer, err = creds.DecryptCredential(ctx, workspace.DefaultID, staticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt static_bearer after empty token reject: %v", err)
	}
	if decryptedStaticBearer.Token != "partial-key" {
		t.Fatalf("static_bearer changed after empty token reject: %+v", decryptedStaticBearer)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, staticBearer.ID, `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://type-change.example.com/mcp","access_token":"type-change-secret"}}`)
	if err == nil {
		t.Fatal("auth.type change from static_bearer must reject")
	}
	assertStringOmits(t, err.Error(), "type-change.example.com", "type-change-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, staticBearer.ID, `{"auth":{"access_token":"wrong-token-secret"}}`)
	if err == nil {
		t.Fatal("static_bearer wrong-variant access_token must reject")
	}
	assertStringOmits(t, err.Error(), "wrong-token-secret")
	staticBearerAfterRejects, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, staticBearer.ID)
	if err != nil {
		t.Fatalf("Get static_bearer after immutable rejects: %v", err)
	}
	decryptedStaticBearerAfterRejects, err := creds.DecryptCredential(ctx, workspace.DefaultID, staticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt static_bearer after immutable rejects: %v", err)
	}
	if staticBearerAfterRejects.Auth.Type != "static_bearer" ||
		decryptedStaticBearerAfterRejects.Token != "partial-key" {
		t.Fatalf("static_bearer changed after immutable reject: public=%+v secret=%+v", staticBearerAfterRejects.Auth, decryptedStaticBearerAfterRejects)
	}

	oauth, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "oauth",
		Auth: vault.CredentialAuth{
			Type:         "mcp_oauth",
			MCPServerURL: "https://oauth.example.com/mcp",
			AccessToken:  "old-access",
			ExpiresAt:    "2026-05-04T05:00:00Z",
			Refresh: &vault.CredentialOAuthRefresh{ //nolint:gosec // G101: synthetic test secret sentinels, not real credentials
				RefreshToken:      "old-refresh",
				ClientID:          "client-123",
				TokenEndpoint:     "https://auth.example.com/token",
				Resource:          "https://resource.example.com",
				TokenEndpointAuth: &vault.CredentialTokenEndpointAuth{Type: "client_secret_basic", ClientSecret: "old-client-secret"}, //nolint:gosec // G101: synthetic test secret sentinel, not a real credential
			},
		},
	})
	if err != nil {
		t.Fatalf("Create oauth: %v", err)
	}
	updatedOAuth, err := creds.Update(ctx, workspace.DefaultID, v.ID, oauth.ID, mustDecodeCredentialPatch(t, `{
		"auth":{
			"type":"mcp_oauth",
			"mcp_server_url":"https://OAUTH.example.com/mcp",
			"access_token":"new-access",
			"expires_at":"2026-05-04T06:00:00Z",
			"refresh":{
				"refresh_token":"new-refresh",
				"client_id":"client-123",
				"token_endpoint":"https://AUTH.example.com/token",
				"resource":"https://resource.example.com",
				"scope":"read",
				"token_endpoint_auth":{"type":"client_secret_post","client_secret":"new-client-secret"}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Update oauth mutable fields: %v", err)
	}
	if updatedOAuth.Auth.ExpiresAt != "2026-05-04T06:00:00Z" ||
		updatedOAuth.Auth.Refresh == nil ||
		updatedOAuth.Auth.Refresh.Scope != "read" ||
		updatedOAuth.Auth.Refresh.TokenEndpointAuth.Type != "client_secret_post" {
		t.Fatalf("updated oauth public auth = %+v", updatedOAuth.Auth)
	}
	decryptedOAuth, err := creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt updated oauth: %v", err)
	}
	if decryptedOAuth.AccessToken != "new-access" ||
		decryptedOAuth.Refresh.RefreshToken != "new-refresh" ||
		decryptedOAuth.Refresh.TokenEndpointAuth.ClientSecret != "new-client-secret" {
		t.Fatalf("updated oauth secret auth = %+v", decryptedOAuth)
	}
	updatedOAuth, err = creds.Update(ctx, workspace.DefaultID, v.ID, oauth.ID, mustDecodeCredentialPatch(t, `{
		"auth":{"access_token":"partial-access"}
	}`))
	if err != nil {
		t.Fatalf("Update oauth without auth.type: %v", err)
	}
	if updatedOAuth.Auth.Type != "mcp_oauth" || updatedOAuth.Auth.MCPServerURL != "https://oauth.example.com/mcp" {
		t.Fatalf("oauth public auth after partial update = %+v", updatedOAuth.Auth)
	}
	decryptedOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt partial oauth: %v", err)
	}
	if decryptedOAuth.AccessToken != "partial-access" || decryptedOAuth.Refresh.RefreshToken != "new-refresh" {
		t.Fatalf("partial oauth secret auth = %+v", decryptedOAuth)
	}
	updatedOAuth, err = creds.Update(ctx, workspace.DefaultID, v.ID, oauth.ID, mustDecodeCredentialPatch(t, `{
		"auth":{"expires_at":null,"refresh":{"scope":null,"resource":null}}
	}`))
	if err != nil {
		t.Fatalf("Clear nullable oauth fields: %v", err)
	}
	decryptedOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt oauth after nullable clear: %v", err)
	}
	if updatedOAuth.Auth.ExpiresAt != "" || updatedOAuth.Auth.Refresh == nil ||
		updatedOAuth.Auth.Refresh.Scope != "" || updatedOAuth.Auth.Refresh.Resource != "" ||
		decryptedOAuth.ExpiresAt != "" || decryptedOAuth.Refresh.Scope != "" || decryptedOAuth.Refresh.Resource != "" {
		t.Fatalf("nullable oauth fields were not cleared: public=%+v secret=%+v", updatedOAuth.Auth, decryptedOAuth)
	}
	updatedOAuth, err = creds.Update(ctx, workspace.DefaultID, v.ID, oauth.ID, mustDecodeCredentialPatch(t, `{"auth":{"refresh":null}}`))
	if err != nil {
		t.Fatalf("Clear oauth refresh: %v", err)
	}
	decryptedOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt oauth after refresh clear: %v", err)
	}
	if updatedOAuth.Auth.Refresh != nil || decryptedOAuth.Refresh != nil {
		t.Fatalf("oauth refresh was not cleared: public=%+v secret=%+v", updatedOAuth.Auth.Refresh, decryptedOAuth.Refresh)
	}
	_, err = creds.Update(ctx, workspace.DefaultID, v.ID, oauth.ID, mustDecodeCredentialPatch(t, `{
		"auth":{
			"expires_at":"2026-05-04T06:00:00Z",
			"refresh":{
				"refresh_token":"new-refresh",
				"client_id":"client-123",
				"token_endpoint":"https://auth.example.com/token",
				"resource":"https://resource.example.com",
				"scope":"read",
				"token_endpoint_auth":{"type":"client_secret_post","client_secret":"new-client-secret"}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("Restore oauth refresh after nullable clear proof: %v", err)
	}
	_, err = creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt restored oauth: %v", err)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"expires_at":""}}`)
	if err == nil {
		t.Fatal("oauth empty expires_at update must reject")
	}
	validationErr = nil
	if !errors.As(err, &validationErr) {
		t.Fatalf("oauth empty expires_at error = %T %v; want ValidationError", err, err)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"refresh":{"scope":""}}}`)
	if err == nil {
		t.Fatal("oauth empty refresh scope update must reject")
	}
	validationErr = nil
	if !errors.As(err, &validationErr) {
		t.Fatalf("oauth empty refresh scope error = %T %v; want ValidationError", err, err)
	}
	oauthAfterEmptyPublicRejects, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, oauth.ID)
	if err != nil {
		t.Fatalf("Get oauth after empty public field rejects: %v", err)
	}
	decryptedOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt oauth after empty public field rejects: %v", err)
	}
	if oauthAfterEmptyPublicRejects.Auth.ExpiresAt != "2026-05-04T06:00:00Z" ||
		oauthAfterEmptyPublicRejects.Auth.Refresh == nil ||
		oauthAfterEmptyPublicRejects.Auth.Refresh.Scope != "read" {
		t.Fatalf("oauth public auth changed after empty public field rejects: %+v", oauthAfterEmptyPublicRejects.Auth)
	}
	if decryptedOAuth.ExpiresAt != "2026-05-04T06:00:00Z" ||
		decryptedOAuth.AccessToken != "partial-access" ||
		decryptedOAuth.Refresh.RefreshToken != "new-refresh" ||
		decryptedOAuth.Refresh.Scope != "read" ||
		decryptedOAuth.Refresh.TokenEndpointAuth.ClientSecret != "new-client-secret" {
		t.Fatalf("oauth secret auth changed after empty public field rejects: %+v", decryptedOAuth)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"access_token":""}}`)
	if err == nil {
		t.Fatal("oauth empty access token rotation must reject")
	}
	validationErr = nil
	if !errors.As(err, &validationErr) {
		t.Fatalf("oauth empty access token error = %T %v; want ValidationError", err, err)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"refresh":{"refresh_token":""}}}`)
	if err == nil {
		t.Fatal("oauth empty refresh token rotation must reject")
	}
	validationErr = nil
	if !errors.As(err, &validationErr) {
		t.Fatalf("oauth empty refresh token error = %T %v; want ValidationError", err, err)
	}
	decryptedOAuth, err = creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt oauth after empty secret rejects: %v", err)
	}
	if decryptedOAuth.AccessToken != "partial-access" || decryptedOAuth.Refresh.RefreshToken != "new-refresh" {
		t.Fatalf("oauth changed after empty secret rejects: %+v", decryptedOAuth)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"token":"wrong-token-secret"}}`)
	if err == nil {
		t.Fatal("mcp_oauth wrong-variant token must reject")
	}
	assertStringOmits(t, err.Error(), "wrong-token-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://other.example.com/mcp","access_token":"do-not-echo"}}`)
	if err == nil {
		t.Fatal("mcp_server_url change must reject")
	}
	assertStringOmits(t, err.Error(), "other.example.com", "do-not-echo")
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"type":"mcp_oauth","access_token":"client-id-secret","refresh":{"client_id":"client-456"}}}`)
	if err == nil {
		t.Fatal("refresh.client_id change must reject")
	}
	assertStringOmits(t, err.Error(), "client-id-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"type":"mcp_oauth","access_token":"empty-client-id-secret","refresh":{"client_id":""}}}`)
	if err == nil {
		t.Fatal("refresh.client_id explicit empty string must reject")
	}
	assertStringOmits(t, err.Error(), "empty-client-id-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"type":"mcp_oauth","access_token":"endpoint-secret","refresh":{"token_endpoint":"https://other-auth.example.com/token"}}}`)
	if err == nil {
		t.Fatal("refresh.token_endpoint change must reject")
	}
	assertStringOmits(t, err.Error(), "other-auth.example.com", "endpoint-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"type":"mcp_oauth","access_token":"resource-secret","refresh":{"resource":"https://other-resource.example.com"}}}`)
	if err == nil {
		t.Fatal("refresh.resource change must reject")
	}
	assertStringOmits(t, err.Error(), "resource-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, oauth.ID, `{"auth":{"type":"mcp_oauth","access_token":"empty-resource-secret","refresh":{"resource":""}}}`)
	if err == nil {
		t.Fatal("refresh.resource explicit empty string must reject")
	}
	assertStringOmits(t, err.Error(), "empty-resource-secret")
	decryptedOAuthAfterRejects, err := creds.DecryptCredential(ctx, workspace.DefaultID, oauth.ID)
	if err != nil {
		t.Fatalf("Decrypt oauth after immutable rejects: %v", err)
	}
	if decryptedOAuthAfterRejects.AccessToken != "partial-access" ||
		decryptedOAuthAfterRejects.Refresh.ClientID != "client-123" ||
		decryptedOAuthAfterRejects.Refresh.TokenEndpoint != "https://auth.example.com/token" ||
		decryptedOAuthAfterRejects.Refresh.Resource != "https://resource.example.com" {
		t.Fatalf("oauth changed after immutable reject: %+v", decryptedOAuthAfterRejects)
	}

	secondStaticBearer, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "static",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://static.example.com/mcp", Token: "old-static-token"},
	})
	if err != nil {
		t.Fatalf("Create static bearer: %v", err)
	}
	if _, err := creds.Update(ctx, workspace.DefaultID, v.ID, secondStaticBearer.ID, mustDecodeCredentialPatch(t, `{
		"auth":{"type":"static_bearer","mcp_server_url":"https://STATIC.example.com/mcp","token":"new-static-token"}
	}`)); err != nil {
		t.Fatalf("Update static bearer token: %v", err)
	}
	decryptedStatic, err := creds.DecryptCredential(ctx, workspace.DefaultID, secondStaticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt updated static bearer: %v", err)
	}
	if decryptedStatic.Token != "new-static-token" {
		t.Fatalf("decrypted static token = %q; want new-static-token", decryptedStatic.Token)
	}
	if _, err := creds.Update(ctx, workspace.DefaultID, v.ID, secondStaticBearer.ID, mustDecodeCredentialPatch(t, `{
		"auth":{"token":"partial-static-token"}
	}`)); err != nil {
		t.Fatalf("Update static bearer without auth.type: %v", err)
	}
	decryptedStatic, err = creds.DecryptCredential(ctx, workspace.DefaultID, secondStaticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt partial static bearer: %v", err)
	}
	if decryptedStatic.Token != "partial-static-token" {
		t.Fatalf("decrypted static token = %q; want partial-static-token", decryptedStatic.Token)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, secondStaticBearer.ID, `{"auth":{"token":""}}`)
	if err == nil {
		t.Fatal("static_bearer empty token rotation must reject")
	}
	validationErr = nil
	if !errors.As(err, &validationErr) {
		t.Fatalf("static_bearer empty token error = %T %v; want ValidationError", err, err)
	}
	decryptedStatic, err = creds.DecryptCredential(ctx, workspace.DefaultID, secondStaticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt static bearer after empty token reject: %v", err)
	}
	if decryptedStatic.Token != "partial-static-token" {
		t.Fatalf("static bearer changed after empty token reject: %+v", decryptedStatic)
	}
	err = updateCredentialRaw(ctx, creds, v.ID, secondStaticBearer.ID, `{"auth":{"key":"wrong-key-secret"}}`)
	if err == nil {
		t.Fatal("static_bearer wrong-variant key must reject")
	}
	assertStringOmits(t, err.Error(), "wrong-key-secret")
	err = updateCredentialRaw(ctx, creds, v.ID, secondStaticBearer.ID, `{"auth":{"type":"static_bearer","mcp_server_url":"https://other-static.example.com/mcp","token":"static-do-not-echo"}}`)
	if err == nil {
		t.Fatal("static_bearer mcp_server_url change must reject")
	}
	assertStringOmits(t, err.Error(), "other-static.example.com", "static-do-not-echo")
	err = updateCredentialRaw(ctx, creds, v.ID, secondStaticBearer.ID, `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://static-type-change.example.com/mcp","access_token":"static-type-change-secret"}}`)
	if err == nil {
		t.Fatal("auth.type change from static_bearer must reject")
	}
	assertStringOmits(t, err.Error(), "static-type-change-secret")
	staticAfterRejects, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, secondStaticBearer.ID)
	if err != nil {
		t.Fatalf("Get static bearer after immutable rejects: %v", err)
	}
	decryptedStaticAfterRejects, err := creds.DecryptCredential(ctx, workspace.DefaultID, secondStaticBearer.ID)
	if err != nil {
		t.Fatalf("Decrypt static bearer after immutable rejects: %v", err)
	}
	if staticAfterRejects.Auth.Type != "static_bearer" ||
		staticAfterRejects.Auth.MCPServerURL != "https://static.example.com/mcp" ||
		decryptedStaticAfterRejects.Token != "partial-static-token" {
		t.Fatalf("static bearer changed after immutable reject: public=%+v secret=%+v", staticAfterRejects.Auth, decryptedStaticAfterRejects)
	}
}

func TestPostgreSQLCredentialStoreArchiveDeleteAndPurgedDecrypt(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "archive"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	credential, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "secret",
		Auth:        staticBearerAuth("archive-secret"),
	})
	if err != nil {
		t.Fatalf("Create credential: %v", err)
	}
	archived, err := creds.Archive(ctx, workspace.DefaultID, v.ID, credential.ID)
	if err != nil {
		t.Fatalf("Archive credential: %v", err)
	}
	if archived.ArchivedAt == nil || archived.Auth.Type != "static_bearer" {
		t.Fatalf("archived credential = %+v", archived)
	}
	var encryptedAuth []byte
	if err := admin.QueryRowContext(ctx,
		`SELECT encrypted_auth FROM credentials WHERE id = $1`,
		credential.ID,
	).Scan(&encryptedAuth); err != nil {
		t.Fatalf("read archived encrypted_auth: %v", err)
	}
	if encryptedAuth != nil {
		t.Fatal("Archive must purge encrypted_auth")
	}
	if _, err := creds.DecryptCredential(ctx, workspace.DefaultID, credential.ID); err == nil {
		t.Fatal("Decrypt archived credential must fail")
	} else {
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("Decrypt archived credential error = %T %v; want ValidationError", err, err)
		}
		assertStringOmits(t, err.Error(), "archive-secret")
	}
	if _, err := creds.Update(ctx, workspace.DefaultID, v.ID, credential.ID, mustDecodeCredentialPatch(t, `{"metadata":{"after":"archive"}}`)); err == nil {
		t.Fatal("Update archived credential must reject")
	}

	purged, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "purged",
		Auth:        staticBearerAuth("purged-active-secret"),
	})
	if err != nil {
		t.Fatalf("Create purged credential: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE credentials SET encrypted_auth = NULL WHERE id = $1`, purged.ID); err != nil {
		t.Fatalf("purge active credential: %v", err)
	}
	if _, err := creds.DecryptCredential(ctx, workspace.DefaultID, purged.ID); err == nil {
		t.Fatal("Decrypt active purged credential must fail")
	} else {
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("Decrypt active purged credential error = %T %v; want ValidationError", err, err)
		}
		assertStringOmits(t, err.Error(), "purged-active-secret")
	}

	deleted, err := creds.Delete(ctx, workspace.DefaultID, v.ID, credential.ID)
	if err != nil {
		t.Fatalf("Delete credential: %v", err)
	}
	if deleted.ID != credential.ID || deleted.Type != "vault_credential_deleted" {
		t.Fatalf("delete result = %+v", deleted)
	}
	if _, err := creds.GetMetadata(ctx, workspace.DefaultID, v.ID, credential.ID); err == nil {
		t.Fatal("GetMetadata after Delete must fail")
	}
}

func TestPostgreSQLCredentialStoreDeletePrunesHistoricalSelectorsAndRestrictsActiveSelectors(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	credentials := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "selector deletion"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	historicalCredential, err := credentials.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "historical selector",
		Auth:        staticBearerAuth("historical-selector"),
	})
	if err != nil {
		t.Fatalf("Create historical credential: %v", err)
	}
	activeCredential, err := credentials.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "active selector",
		Auth:        staticBearerAuth("active-selector"),
	})
	if err != nil {
		t.Fatalf("Create active credential: %v", err)
	}

	const sessionID = "sesn_credential_delete_selectors"
	seedVaultSession(t, admin, workspace.DefaultID, sessionID)
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_provider_auth (
			workspace_id, session_id, provider_id, vault_id, credential_id, access_mode, created_at, updated_at, deleted_at
		) VALUES
			($1, $2, 'historical-provider', $3, $4, 'user', $6, $6, $6),
			($1, $2, 'active-provider', $3, $5, 'user', $6, $6, NULL)`,
		string(workspace.DefaultID), sessionID, v.ID, historicalCredential.ID, activeCredential.ID, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed provider selectors: %v", err)
	}

	if _, err := credentials.Delete(ctx, workspace.DefaultID, v.ID, historicalCredential.ID); err != nil {
		t.Fatalf("Delete credential referenced only by historical selector: %v", err)
	}
	var historicalSelectorRows int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM session_provider_auth
		  WHERE workspace_id = $1 AND vault_id = $2 AND credential_id = $3`,
		string(workspace.DefaultID), v.ID, historicalCredential.ID,
	).Scan(&historicalSelectorRows); err != nil {
		t.Fatalf("count historical selectors: %v", err)
	}
	if historicalSelectorRows != 0 {
		t.Fatalf("historical selector rows after Delete = %d; want 0", historicalSelectorRows)
	}

	if _, err := credentials.Delete(ctx, workspace.DefaultID, v.ID, activeCredential.ID); err == nil {
		t.Fatal("Delete credential referenced by active selector must fail")
	} else {
		var conflict *vault.ConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("Delete referenced credential error = %T %v; want ConflictError", err, err)
		}
		if !conflict.InvalidRequest {
			t.Fatalf("Delete referenced credential conflict = %+v; want invalid-request classification", conflict)
		}
		if conflict.Message != "credential is still referenced by sessions" {
			t.Fatalf("Delete referenced credential message = %q", conflict.Message)
		}
	}
	var activeSelectorRows, activeCredentialRows int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM session_provider_auth
		  WHERE workspace_id = $1 AND vault_id = $2 AND credential_id = $3 AND deleted_at IS NULL`,
		string(workspace.DefaultID), v.ID, activeCredential.ID,
	).Scan(&activeSelectorRows); err != nil {
		t.Fatalf("count active selectors: %v", err)
	}
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM credentials WHERE workspace_id = $1 AND vault_id = $2 AND id = $3`,
		string(workspace.DefaultID), v.ID, activeCredential.ID,
	).Scan(&activeCredentialRows); err != nil {
		t.Fatalf("count active credential: %v", err)
	}
	if activeSelectorRows != 1 || activeCredentialRows != 1 {
		t.Fatalf("active selector rows = %d, credential rows = %d; want both 1", activeSelectorRows, activeCredentialRows)
	}
}

func TestPostgreSQLCredentialStoreRevokedSecretsFailClosed(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "revoked"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	credential, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "revoked secret",
		Auth:        staticBearerAuth("revoked-secret"),
	})
	if err != nil {
		t.Fatalf("Create credential: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE credentials
		    SET revoked_at = $1,
		        updated_at = $1
		  WHERE workspace_id = $2
		    AND vault_id = $3
		    AND id = $4`,
		time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		string(workspace.DefaultID), v.ID, credential.ID,
	); err != nil {
		t.Fatalf("revoke credential: %v", err)
	}

	if _, err := creds.DecryptCredential(ctx, workspace.DefaultID, credential.ID); err == nil {
		t.Fatal("Decrypt revoked credential must reject")
	} else {
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("Decrypt revoked credential error = %T %v; want ValidationError", err, err)
		}
		assertStringOmits(t, err.Error(), "revoked-secret")
	}
	if _, err := creds.Update(ctx, workspace.DefaultID, v.ID, credential.ID, mustDecodeCredentialPatch(t, `{"metadata":{"after":"revoke"}}`)); err == nil {
		t.Fatal("Update revoked credential must reject")
	}
	callbackCalled := false
	if _, err := creds.UpdateWithLockedCredential(ctx, workspace.DefaultID, v.ID, credential.ID, func(auth vault.CredentialAuth) (*vault.CredentialPatch, error) {
		callbackCalled = true
		return nil, nil
	}); err == nil {
		t.Fatal("Locked update revoked credential must reject")
	} else {
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("Locked update revoked credential error = %T %v; want ValidationError", err, err)
		}
		assertStringOmits(t, err.Error(), "revoked-secret")
	}
	if callbackCalled {
		t.Fatal("Locked update callback must not run for revoked credentials")
	}
}

func TestPostgreSQLCredentialStoreMCPUniquenessAndCap(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	seedVaultWorkspace(t, admin, "workspace_b", "B")
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "mcp"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	first, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "static one",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://Dup.Example.com/mcp", Token: "static-one"},
	})
	if err != nil {
		t.Fatalf("Create first static bearer: %v", err)
	}
	if _, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "static duplicate",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://dup.example.com/mcp", Token: "static-two"},
	}); err == nil {
		t.Fatal("duplicate active MCP URL must conflict")
	} else {
		var conflict *vault.ConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("duplicate error = %T %v; want ConflictError", err, err)
		}
		assertStringOmits(t, err.Error(), "static-two")
	}
	if _, err := creds.Archive(ctx, workspace.DefaultID, v.ID, first.ID); err != nil {
		t.Fatalf("Archive duplicate source: %v", err)
	}
	if _, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "static replacement",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://dup.example.com/mcp", Token: "static-three"},
	}); err != nil {
		t.Fatalf("Create replacement after archive: %v", err)
	}

	capVault, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "cap"})
	if err != nil {
		t.Fatalf("Create cap vault: %v", err)
	}
	capCredentialIDs := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		credential, err := creds.Create(ctx, workspace.DefaultID, capVault.ID, vault.CreateCredentialRequest{
			DisplayName: fmt.Sprintf("mcp-%02d", i),
			Auth: vault.CredentialAuth{
				Type:         "static_bearer",
				MCPServerURL: fmt.Sprintf("https://mcp-%02d.example.com/service", i),
				Token:        fmt.Sprintf("token-%02d", i),
			},
		})
		if err != nil {
			t.Fatalf("Create MCP credential %d: %v", i, err)
		}
		capCredentialIDs = append(capCredentialIDs, credential.ID)
	}
	if _, err := creds.Create(ctx, workspace.DefaultID, capVault.ID, vault.CreateCredentialRequest{
		DisplayName: "mcp-over-cap",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://mcp-over.example.com/service", Token: "over-cap-secret"},
	}); err == nil {
		t.Fatal("21st active MCP credential must reject")
	} else {
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("cap error = %T %v; want ValidationError", err, err)
		}
		assertStringOmits(t, err.Error(), "over-cap-secret")
	}
	workspaceBVault, err := vaults.Create(ctx, "workspace_b", vault.CreateVaultRequest{DisplayName: "workspace-b-cap"})
	if err != nil {
		t.Fatalf("Create workspace_b cap vault: %v", err)
	}
	if _, err := creds.Create(ctx, "workspace_b", workspaceBVault.ID, vault.CreateCredentialRequest{
		DisplayName: "workspace-b-mcp",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://workspace-b.example.com/service", Token: "workspace-b-token"}, //nolint:gosec // G101: synthetic test secret sentinel, not a real credential
	}); err != nil {
		t.Fatalf("workspace_b MCP credential must not consume default workspace cap: %v", err)
	}
	if _, err := creds.Archive(ctx, workspace.DefaultID, capVault.ID, capCredentialIDs[0]); err != nil {
		t.Fatalf("Archive one capped MCP credential: %v", err)
	}
	if _, err := creds.Create(ctx, workspace.DefaultID, capVault.ID, vault.CreateCredentialRequest{
		DisplayName: "mcp-after-archive",
		Auth:        vault.CredentialAuth{Type: "static_bearer", MCPServerURL: "https://mcp-after-archive.example.com/service", Token: "after-archive-token"},
	}); err != nil {
		t.Fatalf("Create MCP credential after archive frees cap: %v", err)
	}
}

func TestPostgreSQLCredentialStoreMCPCreateCapIsSerializedByParentVaultLock(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "serialized-cap"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	for i := 0; i < 19; i++ {
		if _, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
			DisplayName: fmt.Sprintf("existing-%02d", i),
			Auth: vault.CredentialAuth{
				Type:         "static_bearer",
				MCPServerURL: fmt.Sprintf("https://serialized-%02d.example.com/service", i),
				Token:        fmt.Sprintf("serialized-token-%02d", i),
			},
		}); err != nil {
			t.Fatalf("Create existing MCP credential %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, createErr := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
				DisplayName: fmt.Sprintf("concurrent-%d", i),
				Auth: vault.CredentialAuth{
					Type:         "static_bearer",
					MCPServerURL: fmt.Sprintf("https://concurrent-%d.example.com/service", i),
					Token:        fmt.Sprintf("concurrent-token-%d", i),
				},
			})
			errs <- createErr
		}()
	}
	wg.Wait()
	close(errs)

	successes := 0
	failures := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		failures++
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("concurrent create error = %T %v; want ValidationError", err, err)
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("concurrent cap result successes=%d failures=%d; want 1/1", successes, failures)
	}
	var activeMCP int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM credentials
		  WHERE workspace_id = 'default'
		    AND vault_id = $1
		    AND archived_at IS NULL
		    AND auth_type IN ('mcp_oauth', 'static_bearer')`,
		v.ID,
	).Scan(&activeMCP); err != nil {
		t.Fatalf("count active MCP credentials: %v", err)
	}
	if activeMCP != 20 {
		t.Fatalf("active MCP credentials = %d; want 20", activeMCP)
	}
	if _, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
		DisplayName: "provider-over-mcp-cap",
		Auth: vault.CredentialAuth{ //nolint:gosec // G101: synthetic test secret sentinel, not a real credential
			Type:       "provider_api_key",
			ProviderID: "anthropic",
			AccessMode: "user_api_key",
			Token:      "provider-over-mcp-cap-secret",
		},
	}); err != nil {
		t.Fatalf("provider credential must not count against MCP cap: %v", err)
	}
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM credentials
		  WHERE workspace_id = 'default'
		    AND vault_id = $1
		    AND archived_at IS NULL
		    AND auth_type IN ('mcp_oauth', 'static_bearer')`,
		v.ID,
	).Scan(&activeMCP); err != nil {
		t.Fatalf("count active MCP credentials after provider create: %v", err)
	}
	if activeMCP != 20 {
		t.Fatalf("active MCP credentials after provider create = %d; want 20", activeMCP)
	}
}

func TestPostgreSQLCredentialStoreRejectsUnsupportedAuthTypes(t *testing.T) {
	runtime, _, encryptor := newPGVaultEnv(t)
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	v, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "unsupported-auth"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	for _, authType := range []string{"password", "unsupported"} {
		_, err := creds.Create(ctx, workspace.DefaultID, v.ID, vault.CreateCredentialRequest{
			DisplayName: authType,
			Auth:        vault.CredentialAuth{Type: authType, Token: "unsupported-secret"},
		})
		if err == nil {
			t.Fatalf("Create with auth.type %q must reject", authType)
		}
		var validationErr *vault.ValidationError
		if !errors.As(err, &validationErr) {
			t.Fatalf("Create with auth.type %q error = %T %v; want ValidationError", authType, err, err)
		}
		if got, want := err.Error(), "auth.type must be mcp_oauth, static_bearer, provider_api_key, or provider_oauth"; got != want {
			t.Fatalf("Create with auth.type %q error = %q; want %q", authType, got, want)
		}
		assertStringOmits(t, err.Error(), "unsupported-secret")
	}
}

func TestPostgreSQLCredentialStoreCrossWorkspacePageRejected(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	seedVaultWorkspace(t, admin, "workspace_b", "B")
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	// A default-workspace List with workspace_b's page token must be rejected.
	bVault, err := vaults.Create(ctx, "workspace_b", vault.CreateVaultRequest{DisplayName: "b"})
	if err != nil {
		t.Fatalf("Create workspace_b vault: %v", err)
	}
	if _, err := creds.Create(ctx, "workspace_b", bVault.ID, vault.CreateCredentialRequest{
		DisplayName: "b-key",
		Auth:        staticBearerAuth("secret-b"),
	}); err != nil {
		t.Fatalf("Create workspace_b cred: %v", err)
	}
	if _, err := creds.Create(ctx, "workspace_b", bVault.ID, vault.CreateCredentialRequest{
		DisplayName: "b-key-two",
		Auth:        staticBearerAuth("secret-b-two"),
	}); err != nil {
		t.Fatalf("Create workspace_b second cred: %v", err)
	}
	bPage, err := creds.List(ctx, "workspace_b", bVault.ID, vault.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List workspace_b creds: %v", err)
	}
	if bPage.NextPage == nil {
		t.Fatal("workspace_b first page must return next page")
	}
	defaultVault, err := vaults.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "d"})
	if err != nil {
		t.Fatalf("Create default vault: %v", err)
	}
	_, err = creds.List(ctx, workspace.DefaultID, defaultVault.ID, vault.ListOptions{Limit: 100, Page: *bPage.NextPage})
	if err == nil {
		t.Fatal("default workspace must not list using workspace_b credential page")
	}
	var v *vault.ValidationError
	if !errors.As(err, &v) {
		t.Errorf("expected *vault.ValidationError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLCredentialStoreCrossWorkspaceDecryptIsNotFound(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	seedVaultWorkspace(t, admin, "workspace_b", "B")
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	bVault, err := vaults.Create(ctx, "workspace_b", vault.CreateVaultRequest{DisplayName: "b"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	bCred, err := creds.Create(ctx, "workspace_b", bVault.ID, vault.CreateCredentialRequest{
		DisplayName: "b-key",
		Auth:        staticBearerAuth("secret"),
	})
	if err != nil {
		t.Fatalf("Create cred: %v", err)
	}
	// Default tries to decrypt workspace_b's credential → NotFound.
	_, err = creds.DecryptCredential(ctx, workspace.DefaultID, bCred.ID)
	if err == nil {
		t.Fatal("default must not DecryptCredential of workspace_b row")
	}
	var nf *vault.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *vault.NotFoundError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLCredentialStoreCrossWorkspaceDeleteIsNotFound(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	seedVaultWorkspace(t, admin, "workspace_b", "B")
	vaults := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	creds := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	ctx := context.Background()
	bVault, err := vaults.Create(ctx, "workspace_b", vault.CreateVaultRequest{DisplayName: "b"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	bCred, err := creds.Create(ctx, "workspace_b", bVault.ID, vault.CreateCredentialRequest{
		DisplayName: "b-key",
		Auth:        staticBearerAuth("secret"),
	})
	if err != nil {
		t.Fatalf("Create cred: %v", err)
	}
	_, err = creds.Delete(ctx, workspace.DefaultID, bVault.ID, bCred.ID)
	if err == nil {
		t.Fatal("default must not Delete workspace_b credential")
	}
	var notFound *vault.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Delete cross-workspace error = %T %v; want NotFoundError", err, err)
	}
}

func mustDecodeCredentialPatch(t *testing.T, body string) vault.CredentialPatch {
	t.Helper()
	patch, err := vault.DecodeUpdateCredentialRequest([]byte(body))
	if err != nil {
		t.Fatalf("DecodeUpdateCredentialRequest: %v", err)
	}
	return patch
}

func updateCredentialRaw(ctx context.Context, creds *vault.PostgreSQLCredentialStore, vaultID string, credentialID string, body string) error {
	patch, err := vault.DecodeUpdateCredentialRequest([]byte(body))
	if err != nil {
		return err
	}
	_, err = creds.Update(ctx, workspace.DefaultID, vaultID, credentialID, patch)
	return err
}

func assertCredentialJSONOmits(t *testing.T, credential *vault.CredentialMetadata, values ...string) {
	t.Helper()
	encoded, err := json.Marshal(credential)
	if err != nil {
		t.Fatalf("marshal credential: %v", err)
	}
	assertStringOmits(t, string(encoded), values...)
}

func assertStringOmits(t *testing.T, got string, values ...string) {
	t.Helper()
	for _, value := range values {
		if value != "" && bytes.Contains([]byte(got), []byte(value)) {
			t.Fatalf("%q must not contain %q", got, value)
		}
	}
}

func repeatedRunes(value string, count int) string {
	return strings.Repeat(value, count)
}

func assertVaultRowCount(t *testing.T, admin *sql.DB, want int) {
	t.Helper()
	var got int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM vaults`).Scan(&got); err != nil {
		t.Fatalf("count vault rows: %v", err)
	}
	if got != want {
		t.Fatalf("vault rows = %d; want %d", got, want)
	}
}

func assertEncryptedAuthOmits(t *testing.T, admin *sql.DB, credentialID string, values ...string) {
	t.Helper()
	var encryptedAuth []byte
	if err := admin.QueryRowContext(context.Background(),
		`SELECT encrypted_auth FROM credentials WHERE id = $1`,
		credentialID,
	).Scan(&encryptedAuth); err != nil {
		t.Fatalf("read encrypted_auth: %v", err)
	}
	if len(encryptedAuth) == 0 {
		t.Fatal("encrypted_auth is empty")
	}
	for _, value := range values {
		if value != "" && bytes.Contains(encryptedAuth, []byte(value)) {
			t.Fatalf("encrypted_auth for %s contains plaintext %q", credentialID, value)
		}
	}
}

func assertProviderCredentialColumns(t *testing.T, admin *sql.DB, credentialID string, wantProviderID string, wantAccessMode string, forbiddenValues ...string) {
	t.Helper()
	var publicAuthJSON, providerID, accessMode string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT auth_public_json, provider_id, access_mode FROM credentials WHERE id = $1`,
		credentialID,
	).Scan(&publicAuthJSON, &providerID, &accessMode); err != nil {
		t.Fatalf("read provider credential row: %v", err)
	}
	if providerID != wantProviderID || accessMode != wantAccessMode {
		t.Fatalf("provider columns = (%q, %q); want (%q, %q)", providerID, accessMode, wantProviderID, wantAccessMode)
	}
	assertStringOmits(t, publicAuthJSON, forbiddenValues...)
}

func assertProviderCredentialColumnsAbsent(t *testing.T, admin *sql.DB, credentialID string) {
	t.Helper()
	var providerID, accessMode sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT provider_id, access_mode FROM credentials WHERE id = $1`,
		credentialID,
	).Scan(&providerID, &accessMode); err != nil {
		t.Fatalf("read credential provider columns: %v", err)
	}
	if providerID.Valid || accessMode.Valid {
		t.Fatalf("non-provider credential provider columns = (%v, %v); want SQL NULLs", providerID, accessMode)
	}
}
