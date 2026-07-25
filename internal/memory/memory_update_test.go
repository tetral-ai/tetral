package memory_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMemoryUpdateContentPathAndCombinedCreateOneVersion(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_update")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update"}
	store := createMemoryStore(t, service, "updates")
	created := createTestMemory(t, service, store.ID, "/update.md", "original", actor)

	contentOnly, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Content:    "content-only",
		ContentSet: true,
		View:       memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory content-only: %v", err)
	}
	assertMemoryProjection(t, contentOnly, "/update.md", "content-only")
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 2, memory.OperationModified, "/update.md", "content-only", actor)

	pathOnly, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Path: strPtr("/renamed.md"),
		View: memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory path-only: %v", err)
	}
	assertMemoryProjection(t, pathOnly, "/renamed.md", "content-only")
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 3, memory.OperationModified, "/renamed.md", "content-only", actor)

	combined, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Path:       strPtr("/combined.md"),
		Content:    "combined",
		ContentSet: true,
		View:       memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory combined: %v", err)
	}
	assertMemoryProjection(t, combined, "/combined.md", "combined")
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 4, memory.OperationModified, "/combined.md", "combined", actor)
}

func TestDecodeUpdateMemoryRejectsOversizedContentAsValidation(t *testing.T) {
	_, err := memory.DecodeUpdateMemoryRequest([]byte(`{"content":"` + strings.Repeat("x", 102401) + `"}`))

	assertMemoryValidationError(t, err)
}

func TestMemoryUpdateRejectsOversizedContentAsValidation(t *testing.T) {
	service := memory.NewService(memory.NewPostgreSQLStore(nil))
	ctx := context.Background()
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update_content_validation"} //nolint:gosec // Public test API key id, not raw API key material.

	_, err := service.UpdateMemory(ctx, workspace.DefaultID, "memstore_content_validation", "mem_content_validation", memory.UpdateMemoryRequest{
		Content:    strings.Repeat("x", 102401),
		ContentSet: true,
	}, actor)

	assertMemoryValidationError(t, err)
}

func TestMemoryUpdateMatchingPreconditionAllowsNonNoOpChange(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_update_precondition")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update_precondition"}
	store := createMemoryStore(t, service, "update-precondition")
	created := createTestMemory(t, service, store.ID, "/precondition.md", "before", actor)
	matching := memory.MemoryPrecondition{Type: memory.PreconditionContentSHA256, ContentSHA256: created.ContentSHA256}

	updated, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Path:         strPtr("/precondition-renamed.md"),
		Content:      "after",
		ContentSet:   true,
		Precondition: &matching,
		View:         memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory matching precondition: %v", err)
	}
	assertMemoryProjection(t, updated, "/precondition-renamed.md", "after")
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 2, memory.OperationModified, "/precondition-renamed.md", "after", actor)
}

func TestMemoryUpdateNoOpAndStalePreconditionShortCircuit(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_update_noop")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update_noop"}
	store := createMemoryStore(t, service, "update-noop")
	created := createTestMemory(t, service, store.ID, "/noop.md", "same", actor)
	stale := memory.MemoryPrecondition{Type: memory.PreconditionContentSHA256, ContentSHA256: "0000000000000000000000000000000000000000000000000000000000000000"}

	noOp, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Path:         strPtr("/noop.md"),
		Content:      "same",
		ContentSet:   true,
		Precondition: &stale,
		View:         memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory no-op with stale precondition: %v", err)
	}
	if noOp.MemoryVersionID != created.MemoryVersionID {
		t.Fatalf("no-op version id = %s; want original %s", noOp.MemoryVersionID, created.MemoryVersionID)
	}
	assertMemoryProjection(t, noOp, "/noop.md", "same")
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 1, memory.OperationCreated, "/noop.md", "same", actor)

	emptyNoOp, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Content:    "same",
		ContentSet: true,
		View:       memory.ViewBasic,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory exact no-op: %v", err)
	}
	if emptyNoOp.Content != nil || emptyNoOp.MemoryVersionID != created.MemoryVersionID {
		t.Fatalf("basic no-op projection/version = %+v", emptyNoOp)
	}
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 1, memory.OperationCreated, "/noop.md", "same", actor)

	omittedNoOp, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Precondition: &stale,
		View:         memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory omitted-fields no-op with stale precondition: %v", err)
	}
	if omittedNoOp.MemoryVersionID != created.MemoryVersionID {
		t.Fatalf("omitted-fields no-op version id = %s; want original %s", omittedNoOp.MemoryVersionID, created.MemoryVersionID)
	}
	assertMemoryProjection(t, omittedNoOp, "/noop.md", "same")
	assertVersionCountAndSnapshot(t, admin, store.ID, created.ID, 1, memory.OperationCreated, "/noop.md", "same", actor)
}

func TestMemoryUpdatePreconditionPathConflictAndArchivedStore(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_update_errors")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update_errors"}
	store := createMemoryStore(t, service, "update-errors")
	first := createTestMemory(t, service, store.ID, "/first.md", "first", actor)
	second := createTestMemory(t, service, store.ID, "/second.md", "second", actor)

	stale := memory.MemoryPrecondition{Type: memory.PreconditionContentSHA256, ContentSHA256: "0000000000000000000000000000000000000000000000000000000000000000"}
	_, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, first.ID, memory.UpdateMemoryRequest{
		Content:      "changed",
		ContentSet:   true,
		Precondition: &stale,
	}, actor)
	var precondition *memory.PreconditionFailedError
	if !errors.As(err, &precondition) {
		t.Fatalf("stale precondition err = %T %v; want PreconditionFailedError", err, err)
	}
	assertVersionCountAndSnapshot(t, admin, store.ID, first.ID, 1, memory.OperationCreated, "/first.md", "first", actor)

	invalidHash := memory.MemoryPrecondition{Type: memory.PreconditionContentSHA256, ContentSHA256: "not-hex"}
	_, err = service.UpdateMemory(ctx, workspace.DefaultID, store.ID, first.ID, memory.UpdateMemoryRequest{
		Content:      "changed",
		ContentSet:   true,
		Precondition: &invalidHash,
	}, actor)
	assertMemoryValidationError(t, err)
	assertVersionCountAndSnapshot(t, admin, store.ID, first.ID, 1, memory.OperationCreated, "/first.md", "first", actor)

	_, err = service.UpdateMemory(ctx, workspace.DefaultID, store.ID, first.ID, memory.UpdateMemoryRequest{
		Path: strPtr("/second.md"),
	}, actor)
	var conflict *memory.PathConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("path conflict err = %T %v; want PathConflictError", err, err)
	}
	if conflict.ConflictingMemoryID != second.ID || conflict.ConflictingPath != "/second.md" {
		t.Fatalf("conflict fields = %+v; want id %s path /second.md", conflict, second.ID)
	}
	assertVersionCountAndSnapshot(t, admin, store.ID, first.ID, 1, memory.OperationCreated, "/first.md", "first", actor)

	if _, err := service.ArchiveStore(ctx, workspace.DefaultID, store.ID); err != nil {
		t.Fatalf("ArchiveStore: %v", err)
	}
	_, err = service.UpdateMemory(ctx, workspace.DefaultID, store.ID, first.ID, memory.UpdateMemoryRequest{
		Content:    "blocked",
		ContentSet: true,
	}, actor)
	assertMemoryValidationError(t, err)
	assertVersionCountAndSnapshot(t, admin, store.ID, first.ID, 1, memory.OperationCreated, "/first.md", "first", actor)
	afterArchiveReject, err := service.GetMemory(ctx, workspace.DefaultID, store.ID, first.ID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemory after archived update rejection: %v", err)
	}
	if afterArchiveReject.MemoryVersionID != first.MemoryVersionID {
		t.Fatalf("head version after archived update rejection = %s; want %s", afterArchiveReject.MemoryVersionID, first.MemoryVersionID)
	}
	assertMemoryProjection(t, afterArchiveReject, "/first.md", "first")
}

func createTestMemory(t *testing.T, service *memory.Service, storeID string, path string, content string, actor memory.Actor) *memory.Memory {
	t.Helper()
	created, err := service.CreateMemory(context.Background(), workspace.DefaultID, storeID, memory.CreateMemoryRequest{
		Path:       path,
		Content:    content,
		ContentSet: true,
		View:       memory.ViewFull,
	}, actor)
	if err != nil {
		t.Fatalf("CreateMemory %s: %v", path, err)
	}
	return created
}

func assertMemoryProjection(t *testing.T, actual *memory.Memory, path string, content string) {
	t.Helper()
	if actual.Path != path {
		t.Fatalf("memory path = %q; want %q", actual.Path, path)
	}
	if actual.Content == nil || *actual.Content != content {
		t.Fatalf("memory content = %v; want %q", actual.Content, content)
	}
	if actual.ContentSHA256 != sha256Hex(content) || actual.ContentSizeBytes != int64(len([]byte(content))) {
		t.Fatalf("hash/size = %s/%d; want hash(%q)/%d", actual.ContentSHA256, actual.ContentSizeBytes, content, len([]byte(content)))
	}
}

func assertVersionCountAndSnapshot(t *testing.T, admin *sql.DB, storeID string, memoryID string, count int, lastOperation string, path string, content string, actor memory.Actor) {
	t.Helper()
	versions := listMemoryVersionsByStorageSequence(t, admin, storeID, memoryID)
	if len(versions) != count {
		t.Fatalf("version count = %d; want %d (%+v)", len(versions), count, versions)
	}
	if versions[len(versions)-1].Operation != lastOperation {
		t.Fatalf("last operation = %q; want %q", versions[len(versions)-1].Operation, lastOperation)
	}
	last := versions[len(versions)-1]
	if last.Path == nil || *last.Path != path {
		t.Fatalf("last version path = %v; want %q", last.Path, path)
	}
	if last.Content == nil || *last.Content != content {
		t.Fatalf("last version content = %v; want %q", last.Content, content)
	}
	if last.ContentSHA256 == nil || *last.ContentSHA256 != sha256Hex(content) {
		t.Fatalf("last version hash = %v; want hash(%q)", last.ContentSHA256, content)
	}
	if last.ContentSizeBytes == nil || *last.ContentSizeBytes != int64(len([]byte(content))) {
		t.Fatalf("last version size = %v; want %d", last.ContentSizeBytes, len([]byte(content)))
	}
	if last.CreatedBy != actor {
		t.Fatalf("last version actor = %+v; want %+v", last.CreatedBy, actor)
	}
}
