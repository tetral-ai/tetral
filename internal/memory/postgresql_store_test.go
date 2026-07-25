package memory_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func newMemoryStoreTestEnv(t *testing.T) (*memory.Service, *sql.DB) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	return memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))), admin
}

func listMemoryVersionsByStorageSequence(t *testing.T, admin *sql.DB, storeID string, memoryID string) []*memory.MemoryVersion {
	t.Helper()
	rows, err := admin.QueryContext(context.Background(),
		`SELECT memory_version_id, memory_store_id, memory_id, operation, path, content, content_sha256, content_size_bytes, created_at,
		        created_actor_type, created_api_key_id, created_session_id, created_user_id,
		        redacted_at, redacted_actor_type, redacted_api_key_id, redacted_session_id, redacted_user_id
		   FROM memory_versions
		  WHERE workspace_id = 'default' AND memory_store_id = $1 AND memory_id = $2
		  ORDER BY storage_sequence ASC`,
		storeID, memoryID)
	if err != nil {
		t.Fatalf("list memory versions by storage sequence: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var versions []*memory.MemoryVersion
	for rows.Next() {
		version := &memory.MemoryVersion{Type: "memory_version"}
		var path sql.NullString
		var content sql.NullString
		var contentHash sql.NullString
		var contentSize sql.NullInt64
		var createdAPIKeyID sql.NullString
		var createdSessionID sql.NullString
		var createdUserID sql.NullString
		var redactedAt sql.NullString
		var redactedActorType sql.NullString
		var redactedAPIKeyID sql.NullString
		var redactedSessionID sql.NullString
		var redactedUserID sql.NullString
		if err := rows.Scan(
			&version.ID, &version.MemoryStoreID, &version.MemoryID, &version.Operation,
			&path, &content, &contentHash, &contentSize, &version.CreatedAt,
			&version.CreatedBy.Type, &createdAPIKeyID, &createdSessionID, &createdUserID,
			&redactedAt, &redactedActorType, &redactedAPIKeyID, &redactedSessionID, &redactedUserID,
		); err != nil {
			t.Fatalf("scan memory version: %v", err)
		}
		version.Path = stringPtrFromNull(path)
		version.Content = stringPtrFromNull(content)
		version.ContentSHA256 = stringPtrFromNull(contentHash)
		if contentSize.Valid {
			version.ContentSizeBytes = &contentSize.Int64
		}
		version.CreatedBy.APIKeyID = createdAPIKeyID.String
		version.CreatedBy.SessionID = createdSessionID.String
		version.CreatedBy.UserID = createdUserID.String
		version.RedactedAt = stringPtrFromNull(redactedAt)
		if redactedActorType.Valid {
			version.RedactedBy = &memory.Actor{
				Type:      redactedActorType.String,
				APIKeyID:  redactedAPIKeyID.String,
				SessionID: redactedSessionID.String,
				UserID:    redactedUserID.String,
			}
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read memory versions: %v", err)
	}
	return versions
}

func stringPtrFromNull(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func seedMemoryWorkspace(t *testing.T, admin *sql.DB, workspaceID workspace.ID) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), string(workspaceID)); err != nil {
		t.Fatalf("seed workspace %s: %v", workspaceID, err)
	}
}

func seedMemorySessionStoreReference(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, memoryStoreID string) {
	t.Helper()
	agentID := "agent_" + sessionID
	envID := "env_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $2, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		string(workspaceID), "agv_"+sessionID, agentID, "hash_"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $2, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), envID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (workspace_id, id, type, metadata_json, status, lifecycle_state, agent_id, agent_version, environment_id, vault_ids_json, created_at, updated_at)
		 VALUES ($1, $2, 'session', '{}', 'idle', 'active', $3, 1, $4, '[]', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, agentID, envID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	resourceID := "sesrsc_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'memory_store', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, resourceID); err != nil {
		t.Fatalf("seed session resource: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_memory_store_resources (workspace_id, session_id, resource_id, memory_store_id, access, instructions, name, description, mount_path)
		 VALUES ($1, $2, $3, $4, 'read_write', '', 'Referenced', '', '/mnt/memory/referenced')`,
		string(workspaceID), sessionID, resourceID, memoryStoreID); err != nil {
		t.Fatalf("seed memory resource: %v", err)
	}
}

type updateStoreResponseRaceResult struct {
	store *memory.Store
	err   error
}

type archiveStoreResponseRaceResult struct {
	store *memory.Store
	err   error
}

type createMemoryTimestampRaceResult struct {
	memory *memory.Memory
	err    error
}

func TestStoreLifecycleCreateUpdateArchiveDeleteAcrossWorkspaces(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	seedMemoryWorkspace(t, admin, "workspace_b")
	ctx := context.Background()

	first, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "Shared", Metadata: map[string]string{"team": "a"}})
	if err != nil {
		t.Fatalf("CreateStore first: %v", err)
	}
	second, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "Shared"})
	if err != nil {
		t.Fatalf("duplicate store names must succeed: %v", err)
	}
	if first.ID == second.ID {
		t.Fatal("duplicate store names must still produce distinct ids")
	}
	other, err := service.CreateStore(ctx, "workspace_b", memory.CreateStoreRequest{Name: "Other"})
	if err != nil {
		t.Fatalf("CreateStore workspace_b: %v", err)
	}
	if _, err := service.GetStore(ctx, workspace.DefaultID, other.ID); !isMemoryNotFound(err) {
		t.Fatalf("default workspace Get workspace_b store err = %T %v; want not found", err, err)
	}
	if err := service.DeleteStore(ctx, workspace.DefaultID, other.ID); !isMemoryNotFound(err) {
		t.Fatalf("default workspace Delete workspace_b store err = %T %v; want not found", err, err)
	}

	updated, err := service.UpdateStore(ctx, workspace.DefaultID, first.ID, memory.StorePatch{
		Name:            strPtr("Renamed"),
		Description:     strPtr(""),
		MetadataPatch:   map[string]*string{"team": nil},
		MetadataPresent: true,
		MutablePresent:  true,
	})
	if err != nil {
		t.Fatalf("UpdateStore: %v", err)
	}
	if updated.Name != "Renamed" || updated.Description != "" || len(updated.Metadata) != 0 {
		t.Fatalf("updated store = %+v", updated)
	}

	archived, err := service.ArchiveStore(ctx, workspace.DefaultID, first.ID)
	if err != nil {
		t.Fatalf("ArchiveStore: %v", err)
	}
	if archived.ArchivedAt == nil {
		t.Fatal("ArchivedAt is nil after archive")
	}
	again, err := service.ArchiveStore(ctx, workspace.DefaultID, first.ID)
	if err != nil {
		t.Fatalf("ArchiveStore again: %v", err)
	}
	if again.ArchivedAt == nil || *again.ArchivedAt != *archived.ArchivedAt {
		t.Fatalf("archive not idempotent: first=%v again=%v", archived.ArchivedAt, again.ArchivedAt)
	}
	_, err = service.UpdateStore(ctx, workspace.DefaultID, first.ID, memory.StorePatch{Name: strPtr("blocked"), MutablePresent: true})
	assertMemoryValidationError(t, err)

	_, err = service.UpdateStore(ctx, workspace.DefaultID, second.ID, memory.StorePatch{})
	assertMemoryValidationError(t, err)

	if err := service.DeleteStore(ctx, workspace.DefaultID, first.ID); err != nil {
		t.Fatalf("DeleteStore: %v", err)
	}
	if _, err := service.GetStore(ctx, workspace.DefaultID, first.ID); !isMemoryNotFound(err) {
		t.Fatalf("Get deleted err = %T %v; want not found", err, err)
	}
}

func TestDeleteStoreRejectsDurableSessionReferenceWithSafeError(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	store, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "Referenced"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	seedMemorySessionStoreReference(t, admin, workspace.DefaultID, "sesn_memory_reference", store.ID)

	err = service.DeleteStore(ctx, workspace.DefaultID, store.ID)
	var validation *memory.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("DeleteStore err = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "memory store has session references" {
		t.Fatalf("DeleteStore validation message = %q", validation.Message)
	}
	if _, getErr := service.GetStore(ctx, workspace.DefaultID, store.ID); getErr != nil {
		t.Fatalf("referenced store should remain after rejected delete: %v", getErr)
	}
}

func TestCreateMemoryTimestampReflectsSerializedMutationOrder(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	runtime.SetMaxOpenConns(1)
	runtime.SetMaxIdleConns(1)
	waitingService := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))
	committingService := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin)))
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_timestamp_order")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_timestamp_order"} //nolint:gosec // Public test API key id.
	store := createMemoryStore(t, committingService, "timestamp-order")

	heldConnection, err := runtime.Conn(ctx)
	if err != nil {
		t.Fatalf("hold runtime connection: %v", err)
	}
	defer func() { _ = heldConnection.Close() }()

	waitCountBefore := runtime.Stats().WaitCount
	done := make(chan createMemoryTimestampRaceResult, 1)
	go func() {
		created, createErr := waitingService.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
			Path:       "/waiter.md",
			Content:    "waiter",
			ContentSet: true,
		}, actor)
		done <- createMemoryTimestampRaceResult{memory: created, err: createErr}
	}()
	waitForRuntimeConnectionWait(t, runtime, waitCountBefore)
	waitForNextTimestampSecond(t)

	first, err := committingService.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
		Path:       "/first.md",
		Content:    "first",
		ContentSet: true,
	}, actor)
	if err != nil {
		t.Fatalf("CreateMemory first mutation: %v", err)
	}
	firstCreatedAt := mustParseMemoryTimestamp(t, first.CreatedAt)
	waitUntilAfterTimestamp(t, firstCreatedAt)

	if err := heldConnection.Close(); err != nil {
		t.Fatalf("release held runtime connection: %v", err)
	}
	var result createMemoryTimestampRaceResult
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked CreateMemory")
	}
	if result.err != nil {
		t.Fatalf("CreateMemory waiter: %v", result.err)
	}
	waiterCreatedAt := mustParseMemoryTimestamp(t, result.memory.CreatedAt)
	if waiterCreatedAt.Before(firstCreatedAt) {
		t.Fatalf("waiter created_at = %s; want at or after first committed mutation %s", result.memory.CreatedAt, first.CreatedAt)
	}

	list, err := committingService.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		OrderBy: memory.MemoryOrderByCreatedAt,
		Order:   memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories created_at order: %v", err)
	}
	assertListPaths(t, list.Data, []string{"/first.md", "/waiter.md"})

	versions, err := committingService.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{})
	if err != nil {
		t.Fatalf("ListMemoryVersions serialized order: %v", err)
	}
	assertVersionIDs(t, versions.Data, []string{result.memory.MemoryVersionID, first.MemoryVersionID})
}

func TestUpdateStoreReturnsMutationStateBeforePostLockWriter(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	store := createMemoryStore(t, service, "response-race-update")
	releaseGate := installPostCommitResponseRaceGate(t, admin, "memory_stores", "update_store_response")

	done := make(chan updateStoreResponseRaceResult, 1)
	go func() {
		updated, err := service.UpdateStore(ctx, workspace.DefaultID, store.ID, memory.StorePatch{
			Name:           strPtr("first response"),
			MutablePresent: true,
		})
		done <- updateStoreResponseRaceResult{store: updated, err: err}
	}()

	waitForPostCommitResponseRaceGate(t, admin, "update_store_response")
	var result updateStoreResponseRaceResult
	var returned bool
	select {
	case result = <-done:
		returned = true
	default:
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE memory_stores
		    SET name = 'second writer', updated_at = '2030-01-01T00:00:00Z'
		  WHERE workspace_id = $1 AND memory_store_id = $2`,
		string(workspace.DefaultID), store.ID); err != nil {
		t.Fatalf("second writer update: %v", err)
	}
	releaseGate()
	if !returned {
		select {
		case result = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for UpdateStore")
		}
	}
	if result.err != nil {
		t.Fatalf("UpdateStore: %v", result.err)
	}
	if result.store.Name != "first response" {
		t.Fatalf("UpdateStore returned name %q; want its own mutation state", result.store.Name)
	}
}

func TestArchiveStoreReturnsMutationStateBeforePostLockDelete(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	store := createMemoryStore(t, service, "response-race-archive")
	releaseGate := installPostCommitResponseRaceGate(t, admin, "memory_stores", "archive_store_response")

	done := make(chan archiveStoreResponseRaceResult, 1)
	go func() {
		archived, err := service.ArchiveStore(ctx, workspace.DefaultID, store.ID)
		done <- archiveStoreResponseRaceResult{store: archived, err: err}
	}()

	waitForPostCommitResponseRaceGate(t, admin, "archive_store_response")
	var result archiveStoreResponseRaceResult
	var returned bool
	select {
	case result = <-done:
		returned = true
	default:
	}
	if _, err := admin.ExecContext(ctx,
		`DELETE FROM memory_stores WHERE workspace_id = $1 AND memory_store_id = $2`,
		string(workspace.DefaultID), store.ID); err != nil {
		t.Fatalf("second writer delete: %v", err)
	}
	releaseGate()
	if !returned {
		select {
		case result = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for ArchiveStore")
		}
	}
	if result.err != nil {
		t.Fatalf("ArchiveStore: %v", result.err)
	}
	if result.store.ID != store.ID || result.store.ArchivedAt == nil {
		t.Fatalf("ArchiveStore returned %+v; want archived mutation state", result.store)
	}
}

func TestStoreListFiltersLimitsAndPageTokenBinding(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	seedMemoryWorkspace(t, admin, "workspace_b")
	ctx := context.Background()
	a, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "a"})
	if err != nil {
		t.Fatalf("Create a: %v", err)
	}
	b, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "b"})
	if err != nil {
		t.Fatalf("Create b: %v", err)
	}
	c, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "c"})
	if err != nil {
		t.Fatalf("Create c: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE memory_stores
		    SET created_at = CASE memory_store_id
		        WHEN $1 THEN '2026-01-01T00:00:00Z'
		        WHEN $2 THEN '2026-01-01T00:00:00Z'
		        WHEN $3 THEN '2026-01-02T00:00:00Z'
		    END::timestamptz,
		    updated_at = created_at
		  WHERE memory_store_id IN ($1, $2, $3)`,
		a.ID, b.ID, c.ID); err != nil {
		t.Fatalf("normalize created_at: %v", err)
	}
	if _, err := service.ArchiveStore(ctx, workspace.DefaultID, b.ID); err != nil {
		t.Fatalf("Archive b: %v", err)
	}

	defaultList, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{})
	if err != nil {
		t.Fatalf("ListStores default: %v", err)
	}
	if len(defaultList.Data) != 2 {
		t.Fatalf("default list len = %d; want 2 active stores", len(defaultList.Data))
	}
	if defaultList.Data[0].CreatedAt > defaultList.Data[1].CreatedAt ||
		(defaultList.Data[0].CreatedAt == defaultList.Data[1].CreatedAt && defaultList.Data[0].ID > defaultList.Data[1].ID) {
		t.Fatalf("default list not ordered by created_at,id: %+v", defaultList.Data)
	}

	withArchived, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{IncludeArchived: true})
	if err != nil {
		t.Fatalf("ListStores include_archived: %v", err)
	}
	if len(withArchived.Data) != 3 {
		t.Fatalf("include_archived list len = %d; want 3", len(withArchived.Data))
	}

	filtered, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{CreatedAtGTE: "2026-01-02T00:00:00Z"})
	if err != nil {
		t.Fatalf("ListStores created_at[gte]: %v", err)
	}
	if len(filtered.Data) != 1 || filtered.Data[0].ID != c.ID {
		t.Fatalf("created_at[gte] result = %+v; want c only", filtered.Data)
	}
	filtered, err = service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{CreatedAtLTE: "2026-01-01T00:00:00Z", IncludeArchived: true})
	if err != nil {
		t.Fatalf("ListStores created_at[lte]: %v", err)
	}
	if len(filtered.Data) != 2 {
		t.Fatalf("created_at[lte] result len = %d; want 2", len(filtered.Data))
	}

	pageOne, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{Limit: 1, IncludeArchived: true})
	if err != nil {
		t.Fatalf("ListStores page one: %v", err)
	}
	if pageOne.NextPage == nil {
		t.Fatal("page one token is nil")
	}
	pageTwo, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{Limit: 1, IncludeArchived: true, Page: *pageOne.NextPage})
	if err != nil {
		t.Fatalf("ListStores page two: %v", err)
	}
	if len(pageTwo.Data) != 1 || pageTwo.Data[0].ID == pageOne.Data[0].ID {
		t.Fatalf("page two did not advance: page1=%+v page2=%+v", pageOne.Data, pageTwo.Data)
	}
	for _, tc := range []memory.ListStoresOptions{
		{Limit: 1, IncludeArchived: false, Page: *pageOne.NextPage},
		{Limit: 1, IncludeArchived: true, CreatedAtGTE: "2026-01-02T00:00:00Z", Page: *pageOne.NextPage},
		{Limit: 1, IncludeArchived: true, CreatedAtLTE: "2026-01-01T00:00:00Z", Page: *pageOne.NextPage},
	} {
		if _, err := service.ListStores(ctx, workspace.DefaultID, tc); err == nil {
			t.Fatalf("ListStores accepted page token with changed filters: %+v", tc)
		}
	}
	if _, err := service.ListStores(ctx, "workspace_b", memory.ListStoresOptions{Limit: 1, IncludeArchived: true, Page: *pageOne.NextPage}); err == nil {
		t.Fatal("workspace_b accepted default workspace page token")
	}
}

func installPostCommitResponseRaceGate(t *testing.T, admin *sql.DB, tableName string, resourceName string) func() {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, `
		CREATE TABLE memory_response_race_gate (
			resource TEXT PRIMARY KEY
		);
		CREATE OR REPLACE FUNCTION memory_response_race_block(blocked_resource TEXT)
		RETURNS BOOLEAN
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF EXISTS (SELECT 1 FROM memory_response_race_gate WHERE resource = blocked_resource) THEN
				PERFORM pg_advisory_xact_lock(571003);
			END IF;
			RETURN TRUE;
		END;
		$$;
		CREATE OR REPLACE FUNCTION memory_response_race_mark()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			INSERT INTO memory_response_race_gate (resource)
			VALUES (TG_ARGV[0])
			ON CONFLICT (resource) DO NOTHING;
			RETURN NEW;
		END;
		$$;
		GRANT SELECT, INSERT ON memory_response_race_gate TO PUBLIC;
		GRANT EXECUTE ON FUNCTION memory_response_race_block(TEXT) TO PUBLIC;
		GRANT EXECUTE ON FUNCTION memory_response_race_mark() TO PUBLIC;
	`); err != nil {
		t.Fatalf("install response race gate functions: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		`DROP POLICY IF EXISTS response_race_gate_select ON %s;
		 CREATE POLICY response_race_gate_select ON %s
		 AS RESTRICTIVE
		 FOR SELECT
		 USING (memory_response_race_block('%s'));
		 DROP TRIGGER IF EXISTS response_race_gate_mark ON %s;
		 CREATE TRIGGER response_race_gate_mark
		 AFTER UPDATE ON %s
		 FOR EACH ROW
		 EXECUTE FUNCTION memory_response_race_mark('%s');`,
		tableName, tableName, resourceName, tableName, tableName, resourceName,
	)); err != nil {
		t.Fatalf("install response race gate policy on %s: %v", tableName, err)
	}
	lockConnection, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("open response race lock connection: %v", err)
	}
	if _, err := lockConnection.ExecContext(ctx, `SELECT pg_advisory_lock(571003)`); err != nil {
		_ = lockConnection.Close()
		t.Fatalf("hold response race advisory lock: %v", err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_, _ = lockConnection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(571003)`)
		_ = lockConnection.Close()
	}
	t.Cleanup(release)
	return release
}

func waitForPostCommitResponseRaceGate(t *testing.T, admin *sql.DB, resourceName string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var exists bool
		if err := admin.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM memory_response_race_gate WHERE resource = $1)`,
			resourceName).Scan(&exists); err != nil {
			t.Fatalf("query response race gate: %v", err)
		}
		if exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for response race gate %q", resourceName)
}

func waitForRuntimeConnectionWait(t *testing.T, db *sql.DB, waitCountBefore int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.Stats().WaitCount > waitCountBefore {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for blocked runtime connection")
}

func waitForNextTimestampSecond(t *testing.T) {
	t.Helper()
	start := time.Now().UTC().Format(time.RFC3339)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if time.Now().UTC().Format(time.RFC3339) != start {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for next timestamp second")
}

func waitUntilAfterTimestamp(t *testing.T, timestamp time.Time) {
	t.Helper()
	wantAfter := timestamp.UTC().Format(time.RFC3339)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if time.Now().UTC().Format(time.RFC3339) > wantAfter {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for clock to pass %s", timestamp.Format(time.RFC3339))
}

func mustParseMemoryTimestamp(t *testing.T, timestamp string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		t.Fatalf("parse timestamp %q: %v", timestamp, err)
	}
	return parsed
}

func TestStoreListLimitValidation(t *testing.T) {
	service, _ := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	for i := 0; i < 21; i++ {
		if _, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: string(rune('a' + i))}); err != nil {
			t.Fatalf("CreateStore %d: %v", i, err)
		}
	}
	defaultList, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{})
	if err != nil {
		t.Fatalf("ListStores default: %v", err)
	}
	if len(defaultList.Data) != 20 {
		t.Fatalf("default limit returned %d; want 20", len(defaultList.Data))
	}
	if _, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{Limit: 100}); err != nil {
		t.Fatalf("limit 100 must succeed: %v", err)
	}
	capped, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{Limit: 101, LimitSet: true})
	if err != nil {
		t.Fatalf("over-cap limit must cap, got error: %v", err)
	}
	if len(capped.Data) != 21 {
		t.Fatalf("over-cap limit returned %d stores; want all 21 under cap 100", len(capped.Data))
	}
	for _, limit := range []int{-1, 0} {
		if _, err := service.ListStores(ctx, workspace.DefaultID, memory.ListStoresOptions{Limit: limit, LimitSet: true}); err == nil {
			t.Fatalf("limit %d succeeded; want validation error", limit)
		}
	}
}

func isMemoryNotFound(err error) bool {
	var notFound *memory.NotFoundError
	return errors.As(err, &notFound)
}

func strPtr(value string) *string { return &value }
