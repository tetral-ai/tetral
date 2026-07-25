package memory_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMemoryCreateRetrieveAndActor(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_memory")
	store := createMemoryStore(t, service, "docs")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_memory"}

	created, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
		Path:       "/notes/a.md",
		Content:    "hello",
		ContentSet: true,
		View:       memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}
	if created.Type != "memory" || created.MemoryStoreID != store.ID || created.ID == "" || created.MemoryVersionID == "" {
		t.Fatalf("created memory identity = %+v", created)
	}
	if created.Content == nil || *created.Content != "hello" {
		t.Fatalf("created content = %v; want hello", created.Content)
	}
	wantHash := sha256Hex("hello")
	if created.ContentSHA256 != wantHash || created.ContentSizeBytes != 5 {
		t.Fatalf("hash/size = %s/%d; want %s/5", created.ContentSHA256, created.ContentSizeBytes, wantHash)
	}
	version, err := service.GetMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemoryVersion: %v", err)
	}
	if version.Operation != memory.OperationCreated || version.CreatedBy != actor {
		t.Fatalf("version audit = operation %q actor %+v", version.Operation, version.CreatedBy)
	}
	if version.Content == nil || *version.Content != "hello" ||
		version.ContentSHA256 == nil || *version.ContentSHA256 != wantHash ||
		version.ContentSizeBytes == nil || *version.ContentSizeBytes != 5 {
		t.Fatalf("version payload = %+v", version)
	}

	full, err := service.GetMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemory full: %v", err)
	}
	if full.Content == nil || *full.Content != "hello" {
		t.Fatalf("full content = %v", full.Content)
	}
	defaultView, err := service.GetMemory(ctx, workspace.DefaultID, store.ID, created.ID, "")
	if err != nil {
		t.Fatalf("GetMemory default view: %v", err)
	}
	if defaultView.Content == nil || *defaultView.Content != "hello" {
		t.Fatalf("default view content = %v; want hello", defaultView.Content)
	}
	basic, err := service.GetMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.ViewBasic)
	if err != nil {
		t.Fatalf("GetMemory basic: %v", err)
	}
	if basic.Content != nil {
		t.Fatalf("basic content = %q; want nil", *basic.Content)
	}
	if basic.ContentSHA256 != wantHash || basic.ContentSizeBytes != 5 {
		t.Fatalf("basic hash/size = %s/%d", basic.ContentSHA256, basic.ContentSizeBytes)
	}
	basicJSON, err := json.Marshal(basic)
	if err != nil {
		t.Fatalf("Marshal basic memory: %v", err)
	}
	if strings.Contains(string(basicJSON), `"content"`) {
		t.Fatalf("basic memory JSON contains content: %s", basicJSON)
	}
	fullJSON, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("Marshal full memory: %v", err)
	}
	if !strings.Contains(string(fullJSON), `"content":"hello"`) {
		t.Fatalf("full memory JSON omits content: %s", fullJSON)
	}
}

func TestMemoryCreateContentValidationAndServerOwnedFields(t *testing.T) {
	for _, body := range []string{
		`{"path":"/a.md"}`,
		`{"path":"/a.md","content":"x","content_sha256":"client"}`,
		`{"path":"/a.md","content":"x","content_size_bytes":1}`,
	} {
		if _, err := memory.DecodeCreateMemoryRequest([]byte(body)); err == nil {
			t.Fatalf("DecodeCreateMemoryRequest(%s...) succeeded; want error", body[:min(len(body), 80)])
		}
	}
	_, err := memory.DecodeCreateMemoryRequest([]byte(`{"path":"/a.md","content":"` + strings.Repeat("x", 102401) + `"}`))
	assertMemoryValidationError(t, err)
	req, err := memory.DecodeCreateMemoryRequest([]byte(`{"path":"/empty.md","content":""}`))
	if err != nil {
		t.Fatalf("Decode empty content: %v", err)
	}
	if !req.ContentSet || req.Content != "" {
		t.Fatalf("empty content request = %+v", req)
	}
	req, err = memory.DecodeCreateMemoryRequest([]byte(`{"path":"/max.md","content":"` + strings.Repeat("x", 102400) + `"}`))
	if err != nil {
		t.Fatalf("Decode max content: %v", err)
	}
	if len(req.Content) != 102400 {
		t.Fatalf("max content len = %d", len(req.Content))
	}
}

func TestMemoryCreateRejectsOversizedContentAsValidation(t *testing.T) {
	service := memory.NewService(memory.NewPostgreSQLStore(nil))
	ctx := context.Background()
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_create_content_validation"} //nolint:gosec // Public test API key id, not raw API key material.

	_, err := service.CreateMemory(ctx, workspace.DefaultID, "memstore_content_validation", memory.CreateMemoryRequest{
		Path:       "/too-large.md",
		Content:    strings.Repeat("x", 102401),
		ContentSet: true,
	}, actor)

	assertMemoryValidationError(t, err)
}

func TestMemoryDeleteWritesTombstoneAndFreesPath(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_delete")
	store := createMemoryStore(t, service, "delete")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_delete"}
	created, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
		Path:       "/delete.md",
		Content:    "secret-delete-content",
		ContentSet: true,
		View:       memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}

	wrongHash := "wrong"
	_, err = service.DeleteMemory(ctx, workspace.DefaultID, store.ID, created.ID, &wrongHash, actor)
	var precondition *memory.PreconditionFailedError
	if !errors.As(err, &precondition) {
		t.Fatalf("stale delete err = %T %v; want PreconditionFailedError", err, err)
	}
	if strings.Contains(err.Error(), "secret-delete-content") {
		t.Fatalf("precondition error leaked content: %v", err)
	}

	deleted, err := service.DeleteMemory(ctx, workspace.DefaultID, store.ID, created.ID, &created.ContentSHA256, actor)
	if err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	if deleted.Type != "memory_deleted" || deleted.ID != created.ID {
		t.Fatalf("delete result = %+v", deleted)
	}
	if _, err := service.GetMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.ViewFull); !isMemoryNotFound(err) {
		t.Fatalf("Get deleted err = %T %v; want not found", err, err)
	}
	active, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{})
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(active.Data) != 0 {
		t.Fatalf("active memories after delete = %+v", active)
	}
	versions := listMemoryVersionsByStorageSequence(t, admin, store.ID, created.ID)
	if len(versions) != 2 {
		t.Fatalf("version count = %d; want created+deleted", len(versions))
	}
	deletedVersion := versions[1]
	if deletedVersion.Operation != memory.OperationDeleted || deletedVersion.Path == nil || *deletedVersion.Path != "/delete.md" {
		t.Fatalf("deleted version shape = %+v", deletedVersion)
	}
	if deletedVersion.Content != nil || deletedVersion.ContentSHA256 != nil || deletedVersion.ContentSizeBytes != nil {
		t.Fatalf("deleted version payload not cleared: %+v", deletedVersion)
	}
	if _, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/delete.md", Content: "new", ContentSet: true}, actor); err != nil {
		t.Fatalf("path reuse after delete: %v", err)
	}
}

func TestMemoryArchivedStoreRejectsCreateDelete(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_archive")
	store := createMemoryStore(t, service, "archived")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_archive"}
	created, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/a.md", Content: "a", ContentSet: true}, actor)
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}
	if _, err := service.ArchiveStore(ctx, workspace.DefaultID, store.ID); err != nil {
		t.Fatalf("ArchiveStore: %v", err)
	}
	_, err = service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/b.md", Content: "b", ContentSet: true}, actor)
	assertMemoryValidationError(t, err)
	_, err = service.DeleteMemory(ctx, workspace.DefaultID, store.ID, created.ID, nil, actor)
	assertMemoryValidationError(t, err)
}

func TestMemoryQuotaStoreCountAndRetainedBytes(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	for i := 0; i < memory.MaxMemoryStoresPerWorkspace; i++ {
		if _, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: fmt.Sprintf("store-%03d", i)}); err != nil {
			t.Fatalf("CreateStore %d: %v", i, err)
		}
	}
	if _, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "over"}); !isMemoryQuota(err) {
		t.Fatalf("101st store err = %T %v; want quota", err, err)
	}

	storeID := seedMemoryStoreBySQL(t, admin, "quota-bytes")
	seedMemoryAPIKey(t, admin, "ak_quota")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_quota"}
	seedRetainedBytes(t, admin, storeID, memory.MaxRetainedMemoryPayloadBytesPerStore)
	_, err := service.CreateMemory(ctx, workspace.DefaultID, storeID, memory.CreateMemoryRequest{Path: "/too-large.md", Content: "x", ContentSet: true}, actor)
	var tooLarge *memory.RequestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("retained bytes err = %T %v; want RequestTooLargeError", err, err)
	}
}

func TestMemoryQuotaConcurrentCreateStoreIsLocked(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	for i := 0; i < memory.MaxMemoryStoresPerWorkspace-1; i++ {
		seedMemoryStoreBySQL(t, admin, fmt.Sprintf("memstore_quota_concurrent_%03d", i))
	}

	createErrors := runConcurrentMemoryOperations(func(index int) error {
		_, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: fmt.Sprintf("concurrent-store-%d", index)})
		return err
	})
	assertOneSuccessAndOneQuota(t, "concurrent store create", createErrors)
	assertStoreCount(t, admin, workspace.DefaultID, memory.MaxMemoryStoresPerWorkspace)
}

func TestMemoryQuotaMemoryIdentityAndVersionCounts(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_count_quota")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_count_quota"}

	memoryCountStore := seedMemoryStoreBySQL(t, admin, "quota-memory-count")
	seedMemoryIdentities(t, admin, memoryCountStore, memory.MaxMemoriesPerStore)
	_, err := service.CreateMemory(ctx, workspace.DefaultID, memoryCountStore, memory.CreateMemoryRequest{
		Path:       "/over-memory-count.md",
		Content:    "x",
		ContentSet: true,
	}, actor)
	if !isMemoryQuota(err) {
		t.Fatalf("10001st memory err = %T %v; want quota", err, err)
	}

	versionCountStore := seedMemoryStoreBySQL(t, admin, "quota-version-count")
	memoryID := seedVersionCount(t, admin, versionCountStore, "mem_version_count", memory.MaxMemoryVersionsPerStore)
	_, err = service.CreateMemory(ctx, workspace.DefaultID, versionCountStore, memory.CreateMemoryRequest{
		Path:       "/over-version-count.md",
		Content:    "x",
		ContentSet: true,
	}, actor)
	if !isMemoryQuota(err) {
		t.Fatalf("create at version quota err = %T %v; want quota", err, err)
	}
	_, err = service.DeleteMemory(ctx, workspace.DefaultID, versionCountStore, "mem_missing_at_version_count", nil, actor)
	if !isMemoryNotFound(err) {
		t.Fatalf("delete missing at version quota err = %T %v; want not found", err, err)
	}
	wrongHash := "wrong"
	_, err = service.DeleteMemory(ctx, workspace.DefaultID, versionCountStore, memoryID, &wrongHash, actor)
	var precondition *memory.PreconditionFailedError
	if !errors.As(err, &precondition) {
		t.Fatalf("delete stale hash at version quota err = %T %v; want PreconditionFailedError", err, err)
	}
	_, err = service.DeleteMemory(ctx, workspace.DefaultID, versionCountStore, memoryID, nil, actor)
	if !isMemoryQuota(err) {
		t.Fatalf("delete at version quota err = %T %v; want quota", err, err)
	}
}

func TestMemoryQuotaConcurrentCreateAndDeleteAreLocked(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_concurrent_quota")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_concurrent_quota"}

	createStore := seedMemoryStoreBySQL(t, admin, "quota-concurrent-create")
	seedMemoryIdentities(t, admin, createStore, memory.MaxMemoriesPerStore-1)
	createErrors := runConcurrentMemoryOperations(func(index int) error {
		_, err := service.CreateMemory(ctx, workspace.DefaultID, createStore, memory.CreateMemoryRequest{
			Path:       fmt.Sprintf("/concurrent-create-%d.md", index),
			Content:    "x",
			ContentSet: true,
		}, actor)
		return err
	})
	assertOneSuccessAndOneQuota(t, "concurrent create", createErrors)

	deleteStore := seedMemoryStoreBySQL(t, admin, "quota-concurrent-delete")
	firstID := "mem_delete_quota_first"
	secondID := "mem_delete_quota_second"
	seedActiveMemory(t, admin, deleteStore, firstID, "memver_delete_quota_first", "/delete-first.md", "x")
	seedActiveMemory(t, admin, deleteStore, secondID, "memver_delete_quota_second", "/delete-second.md", "x")
	seedAdditionalVersions(t, admin, deleteStore, firstID, memory.MaxMemoryVersionsPerStore-3)
	ids := []string{firstID, secondID}
	deleteErrors := runConcurrentMemoryOperations(func(index int) error {
		_, err := service.DeleteMemory(ctx, workspace.DefaultID, deleteStore, ids[index], nil, actor)
		return err
	})
	assertOneSuccessAndOneQuota(t, "concurrent delete", deleteErrors)
}

func TestMemoryQuotaConcurrentUpdateMemoryVersionCountIsLocked(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_concurrent_update_version_quota")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_concurrent_update_version_quota"}

	storeID := seedMemoryStoreBySQL(t, admin, "quota-concurrent-update-version")
	firstID := "mem_update_quota_first"
	secondID := "mem_update_quota_second"
	seedActiveMemory(t, admin, storeID, firstID, "memver_update_quota_first", "/update-first.md", "first")
	seedActiveMemory(t, admin, storeID, secondID, "memver_update_quota_second", "/update-second.md", "second")
	seedAdditionalVersions(t, admin, storeID, firstID, memory.MaxMemoryVersionsPerStore-3)
	assertVersionCount(t, admin, workspace.DefaultID, storeID, memory.MaxMemoryVersionsPerStore-1)

	ids := []string{firstID, secondID}
	updateErrors := runConcurrentMemoryOperations(func(index int) error {
		_, err := service.UpdateMemory(ctx, workspace.DefaultID, storeID, ids[index], memory.UpdateMemoryRequest{
			Content:    fmt.Sprintf("updated-%d", index),
			ContentSet: true,
		}, actor)
		return err
	})
	assertOneSuccessAndOneQuota(t, "concurrent update version quota", updateErrors)
	assertVersionCount(t, admin, workspace.DefaultID, storeID, memory.MaxMemoryVersionsPerStore)
}

func TestMemoryQuotaConcurrentRetainedPayloadCreateIsLocked(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_concurrent_create_retained_quota")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_concurrent_create_retained_quota"}

	storeID := seedMemoryStoreBySQL(t, admin, "quota-concurrent-create-retained")
	seedRetainedBytes(t, admin, storeID, memory.MaxRetainedMemoryPayloadBytesPerStore-1)

	createErrors := runConcurrentMemoryOperations(func(index int) error {
		_, err := service.CreateMemory(ctx, workspace.DefaultID, storeID, memory.CreateMemoryRequest{
			Path:       fmt.Sprintf("/retained-create-%d.md", index),
			Content:    "x",
			ContentSet: true,
		}, actor)
		return err
	})
	assertOneSuccessAndOneRequestTooLarge(t, "concurrent create retained quota", createErrors)
	assertRetainedPayloadBytesNotAboveCap(t, admin, workspace.DefaultID, storeID)
}

func TestMemoryQuotaConcurrentRetainedPayloadUpdateIsLocked(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_concurrent_update_retained_quota")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_concurrent_update_retained_quota"}

	storeID := seedMemoryStoreBySQL(t, admin, "quota-concurrent-update-retained")
	seedRetainedBytes(t, admin, storeID, memory.MaxRetainedMemoryPayloadBytesPerStore-1)
	firstID := "mem_update_retained_first"
	secondID := "mem_update_retained_second"
	seedActiveMemory(t, admin, storeID, firstID, "memver_update_retained_first", "/retained-update-first.md", "")
	seedActiveMemory(t, admin, storeID, secondID, "memver_update_retained_second", "/retained-update-second.md", "")

	ids := []string{firstID, secondID}
	updateErrors := runConcurrentMemoryOperations(func(index int) error {
		_, err := service.UpdateMemory(ctx, workspace.DefaultID, storeID, ids[index], memory.UpdateMemoryRequest{
			Content:    "x",
			ContentSet: true,
		}, actor)
		return err
	})
	assertOneSuccessAndOneRequestTooLarge(t, "concurrent update retained quota", updateErrors)
	assertRetainedPayloadBytesNotAboveCap(t, admin, workspace.DefaultID, storeID)
}

func TestMemorySensitiveErrorsDoNotEchoContent(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_sensitive")
	store := createMemoryStore(t, service, "sensitive")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_sensitive"}
	created, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
		Path:       "/same.md",
		Content:    "ultra-sensitive-content",
		ContentSet: true,
	}, actor)
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}
	for label, err := range map[string]error{
		"validation": func() error {
			_, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/bad.md", Content: strings.Repeat("s", 102401), ContentSet: true}, actor)
			return err
		}(),
		"conflict": func() error {
			_, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/same.md", Content: "ultra-sensitive-content", ContentSet: true}, actor)
			return err
		}(),
		"precondition": func() error {
			wrongHash := "wrong"
			_, err := service.DeleteMemory(ctx, workspace.DefaultID, store.ID, created.ID, &wrongHash, actor)
			return err
		}(),
		"quota": func() error {
			quotaStoreID := seedMemoryStoreBySQL(t, admin, "sensitive-quota")
			seedMemoryIdentities(t, admin, quotaStoreID, memory.MaxMemoriesPerStore)
			_, err := service.CreateMemory(ctx, workspace.DefaultID, quotaStoreID, memory.CreateMemoryRequest{
				Path:       "/sensitive-quota.md",
				Content:    "ultra-sensitive-content",
				ContentSet: true,
			}, actor)
			return err
		}(),
	} {
		if err == nil {
			t.Fatalf("%s error was nil", label)
		}
		if strings.Contains(err.Error(), "ultra-sensitive-content") {
			t.Fatalf("%s error leaked content: %v", label, err)
		}
	}

	runtime := storagetest.NewPostgreSQLDB(t)
	closedService := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))
	if err := runtime.Close(); err != nil {
		t.Fatalf("close runtime db: %v", err)
	}
	_, err = closedService.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: "closed"})
	var internal *memory.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("closed db err = %T %v; want InternalError", err, err)
	}
	for _, forbidden := range []string{"ultra-sensitive-content", "sql", "database", "driver", "postgres"} {
		if strings.Contains(strings.ToLower(err.Error()), forbidden) {
			t.Fatalf("internal error leaked %q: %v", forbidden, err)
		}
	}
}

func createMemoryStore(t *testing.T, service *memory.Service, name string) *memory.Store {
	t.Helper()
	store, err := service.CreateStore(context.Background(), workspace.DefaultID, memory.CreateStoreRequest{Name: name})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	return store
}

func sha256Hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func isMemoryQuota(err error) bool {
	var quota *memory.QuotaError
	return errors.As(err, &quota)
}

func seedMemoryStoreBySQL(t *testing.T, admin anySQL, storeID string) string {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO memory_stores (workspace_id, memory_store_id, name, created_at, updated_at)
		 VALUES ('default', $1, $1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		storeID); err != nil {
		t.Fatalf("seed memory store: %v", err)
	}
	return storeID
}

func seedMemoryAPIKey(t *testing.T, admin anySQL, apiKeyID string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO api_keys (id, workspace_id, name, key_prefix, key_digest, key_kind, created_at)
		 VALUES ($1, 'default', $1, $1, decode(md5($1), 'hex'), 'standard', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		apiKeyID); err != nil {
		t.Fatalf("seed api key %s: %v", apiKeyID, err)
	}
}

func assertStoreCount(t *testing.T, admin *sql.DB, workspaceID workspace.ID, want int) {
	t.Helper()
	var got int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM memory_stores WHERE workspace_id = $1`,
		string(workspaceID)).Scan(&got); err != nil {
		t.Fatalf("count memory stores: %v", err)
	}
	if got != want {
		t.Fatalf("store count = %d; want %d", got, want)
	}
}

func assertVersionCount(t *testing.T, admin *sql.DB, workspaceID workspace.ID, storeID string, want int) {
	t.Helper()
	var got int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM memory_versions WHERE workspace_id = $1 AND memory_store_id = $2`,
		string(workspaceID), storeID).Scan(&got); err != nil {
		t.Fatalf("count memory versions: %v", err)
	}
	if got != want {
		t.Fatalf("version count = %d; want %d", got, want)
	}
}

func assertRetainedPayloadBytesNotAboveCap(t *testing.T, admin *sql.DB, workspaceID workspace.ID, storeID string) {
	t.Helper()
	var got int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COALESCE(sum(octet_length(content)), 0)
		   FROM memory_versions
		  WHERE workspace_id = $1 AND memory_store_id = $2 AND content IS NOT NULL`,
		string(workspaceID), storeID).Scan(&got); err != nil {
		t.Fatalf("sum retained payload bytes: %v", err)
	}
	if got > int64(memory.MaxRetainedMemoryPayloadBytesPerStore) {
		t.Fatalf("retained payload bytes = %d; want <= %d", got, memory.MaxRetainedMemoryPayloadBytesPerStore)
	}
}

func seedRetainedBytes(t *testing.T, admin *sql.DB, storeID string, bytes int) {
	t.Helper()
	ctx := context.Background()
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin retained bytes seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
		 VALUES ('default', $1, 'mem_quota_bytes', 'memver_quota_bytes', '/quota.md', 'sha', $2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
		`, storeID, bytes); err != nil {
		t.Fatalf("seed retained memory: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id)
		 VALUES ('default', $1, 'mem_quota_bytes', 'memver_quota_bytes', 'created', '/quota.md', repeat('x', $2), 'sha', $2, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_quota');
		`, storeID, bytes); err != nil {
		t.Fatalf("seed retained version: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit retained bytes seed: %v", err)
	}
}

func seedMemoryIdentities(t *testing.T, admin *sql.DB, storeID string, count int) {
	t.Helper()
	ctx := context.Background()
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin memory identity seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
		 SELECT 'default', $1, 'mem_quota_identity_' || g, 'memver_quota_identity_' || g,
		        '/quota-identity-' || g || '.md', 'sha', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		   FROM generate_series(1, $2) AS g`,
		storeID, count); err != nil {
		t.Fatalf("seed memory identities: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id)
		 SELECT 'default', $1, 'mem_quota_identity_' || g, 'memver_quota_identity_' || g,
		        'created', '/quota-identity-' || g || '.md', 'x', 'sha', 1, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_quota'
		   FROM generate_series(1, $2) AS g`,
		storeID, count); err != nil {
		t.Fatalf("seed memory identity versions: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit memory identity seed: %v", err)
	}
}

func seedVersionCount(t *testing.T, admin *sql.DB, storeID string, memoryID string, count int) string {
	t.Helper()
	if count < 1 {
		t.Fatal("version count seed requires at least one version")
	}
	seedActiveMemory(t, admin, storeID, memoryID, memoryID+"_version_0", "/version-count.md", "x")
	seedAdditionalVersions(t, admin, storeID, memoryID, count-1)
	return memoryID
}

func seedActiveMemory(t *testing.T, admin *sql.DB, storeID string, memoryID string, versionID string, path string, content string) {
	t.Helper()
	ctx := context.Background()
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin active memory seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	hash := sha256Hex(content)
	size := len([]byte(content))
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
		 VALUES ('default', $1, $2, $3, $4, $5, $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		storeID, memoryID, versionID, path, hash, size); err != nil {
		t.Fatalf("seed active memory: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id)
		 VALUES ('default', $1, $2, $3, 'created', $4, $5, $6, $7, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_quota')`,
		storeID, memoryID, versionID, path, content, hash, size); err != nil {
		t.Fatalf("seed active memory version: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit active memory seed: %v", err)
	}
}

func seedAdditionalVersions(t *testing.T, admin *sql.DB, storeID string, memoryID string, count int) {
	t.Helper()
	if count == 0 {
		return
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id)
		 SELECT 'default', $1, $2, $2 || '_extra_' || g,
		        'modified', '/extra-' || g || '.md', 'x', 'sha', 1, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_quota'
		   FROM generate_series(1, $3) AS g`,
		storeID, memoryID, count); err != nil {
		t.Fatalf("seed additional versions: %v", err)
	}
}

func runConcurrentMemoryOperations(operation func(int) error) []error {
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			errs[index] = operation(index)
		}(i)
	}
	close(start)
	wg.Wait()
	return errs
}

func assertOneSuccessAndOneQuota(t *testing.T, label string, errs []error) {
	t.Helper()
	var successes, quotas int
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		if isMemoryQuota(err) {
			quotas++
			continue
		}
		t.Fatalf("%s err = %T %v; want nil or quota", label, err, err)
	}
	if successes != 1 || quotas != 1 {
		t.Fatalf("%s successes/quotas = %d/%d; want 1/1 (errs=%v)", label, successes, quotas, errs)
	}
}

func assertOneSuccessAndOneRequestTooLarge(t *testing.T, label string, errs []error) {
	t.Helper()
	var successes, tooLargeErrors int
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		var tooLarge *memory.RequestTooLargeError
		if errors.As(err, &tooLarge) {
			tooLargeErrors++
			continue
		}
		t.Fatalf("%s err = %T %v; want nil or RequestTooLargeError", label, err, err)
	}
	if successes != 1 || tooLargeErrors != 1 {
		t.Fatalf("%s successes/request-too-large = %d/%d; want 1/1 (errs=%v)", label, successes, tooLargeErrors, errs)
	}
}

type anySQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
