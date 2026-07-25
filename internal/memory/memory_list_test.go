package memory_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMemoryListPrefixDepthExamplesAndRawPrefix(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list"}
	store := createMemoryStore(t, service, "list-prefix")
	createTestMemory(t, service, store.ID, "/foo.md", "foo", actor)
	createTestMemory(t, service, store.ID, "/notes/a.md", "a", actor)
	createTestMemory(t, service, store.ID, "/notes/deep/c.md", "c", actor)

	root, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByPath,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories root depth: %v", err)
	}
	assertListPaths(t, root.Data, []string{"/foo.md", "/notes/"})
	assertListTypes(t, root.Data, []string{"memory", "memory_prefix"})

	notes, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		PathPrefix: "/notes/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByPath,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories notes depth: %v", err)
	}
	assertListPaths(t, notes.Data, []string{"/notes/a.md", "/notes/deep/"})
	assertListTypes(t, notes.Data, []string{"memory", "memory_prefix"})

	rawPrefixStore := createMemoryStore(t, service, "raw-prefix")
	createTestMemory(t, service, rawPrefixStore.ID, "/notes/a.md", "notes", actor)
	createTestMemory(t, service, rawPrefixStore.ID, "/notes_backup/a.md", "backup", actor)
	raw, err := service.ListMemories(ctx, workspace.DefaultID, rawPrefixStore.ID, memory.ListMemoriesOptions{
		PathPrefix: "/notes/",
		OrderBy:    memory.MemoryOrderByPath,
	})
	if err != nil {
		t.Fatalf("ListMemories raw prefix: %v", err)
	}
	assertListPaths(t, raw.Data, []string{"/notes/a.md"})

	_, err = service.ListMemories(ctx, workspace.DefaultID, rawPrefixStore.ID, memory.ListMemoriesOptions{
		PathPrefix: "/notes",
		Depth:      1,
		DepthSet:   true,
	})
	assertMemoryValidationError(t, err)
	if _, err := service.ListMemories(ctx, workspace.DefaultID, rawPrefixStore.ID, memory.ListMemoriesOptions{
		PathPrefix: "/notes/",
		Depth:      1,
		DepthSet:   true,
	}); err != nil {
		t.Fatalf("directory path_prefix with depth rejected: %v", err)
	}
}

func TestMemoryListPathPrefixValidation(t *testing.T) {
	service, _ := newMemoryStoreTestEnv(t)
	store := createMemoryStore(t, service, "list-prefix-validation")
	for _, prefix := range []string{
		"foo",
		"//notes",
		"/notes/./",
		"/notes/../",
		"/notes/\n",
		"/notes/\u200b",
	} {
		if _, err := service.ListMemories(context.Background(), workspace.DefaultID, store.ID, memory.ListMemoriesOptions{PathPrefix: prefix}); err == nil {
			t.Fatalf("path_prefix %q succeeded; want validation error", prefix)
		}
	}
	if _, err := service.ListMemories(context.Background(), workspace.DefaultID, store.ID, memory.ListMemoriesOptions{PathPrefix: "/valid/"}); err != nil {
		t.Fatalf("valid trailing-slash prefix rejected: %v", err)
	}
}

func TestMemoryListOrderingTieBreaks(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_order")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_order"}
	store := createMemoryStore(t, service, "list-order")
	bPath := createTestMemory(t, service, store.ID, "/b.md", "b", actor)
	aPath := createTestMemory(t, service, store.ID, "/a.md", "a", actor)
	cPath := createTestMemory(t, service, store.ID, "/c.md", "c", actor)
	setMemoryTime(t, admin, store.ID, bPath.ID, "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z")
	setMemoryTime(t, admin, store.ID, aPath.ID, "2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z")
	setMemoryTime(t, admin, store.ID, cPath.ID, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")

	pathOrder, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending})
	if err != nil {
		t.Fatalf("ListMemories path order: %v", err)
	}
	assertListPaths(t, pathOrder.Data, []string{"/a.md", "/b.md", "/c.md"})

	createdOrder, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{OrderBy: memory.MemoryOrderByCreatedAt, Order: memory.SortAscending})
	if err != nil {
		t.Fatalf("ListMemories created_at order: %v", err)
	}
	assertListPaths(t, createdOrder.Data, []string{"/c.md", "/a.md", "/b.md"})

	updatedOrder, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{OrderBy: memory.MemoryOrderByUpdatedAt, Order: memory.SortAscending})
	if err != nil {
		t.Fatalf("ListMemories updated_at order: %v", err)
	}
	assertListPaths(t, updatedOrder.Data, []string{"/a.md", "/b.md", "/c.md"})

	desc, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{OrderBy: memory.MemoryOrderByPath, Order: memory.SortDescending})
	if err != nil {
		t.Fatalf("ListMemories path desc: %v", err)
	}
	assertListPaths(t, desc.Data, []string{"/c.md", "/b.md", "/a.md"})
}

func TestMemoryListPrefixSyntheticOrdering(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_prefix_order")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_prefix_order"}
	store := createMemoryStore(t, service, "prefix-order")
	alpha := createTestMemory(t, service, store.ID, "/alpha/z.md", "alpha", actor)
	beta := createTestMemory(t, service, store.ID, "/beta/a.md", "beta", actor)
	root := createTestMemory(t, service, store.ID, "/root.md", "root", actor)
	setMemoryTime(t, admin, store.ID, alpha.ID, "2026-01-03T00:00:00Z", "2026-01-01T00:00:00Z")
	setMemoryTime(t, admin, store.ID, beta.ID, "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z")
	setMemoryTime(t, admin, store.ID, root.ID, "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z")

	createdOrder, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByCreatedAt,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories prefix created_at order: %v", err)
	}
	assertListPaths(t, createdOrder.Data, []string{"/beta/", "/root.md", "/alpha/"})

	updatedOrder, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByUpdatedAt,
		Order:      memory.SortDescending,
	})
	if err != nil {
		t.Fatalf("ListMemories prefix updated_at order: %v", err)
	}
	assertListPaths(t, updatedOrder.Data, []string{"/beta/", "/root.md", "/alpha/"})
}

func TestMemoryListPaginationTokenBinding(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryWorkspace(t, admin, "workspace_b")
	seedMemoryAPIKey(t, admin, "ak_list_token")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_token"}
	store := createMemoryStore(t, service, "list-token")
	otherStore := createMemoryStore(t, service, "list-token-other")
	otherWorkspaceStore, err := service.CreateStore(ctx, "workspace_b", memory.CreateStoreRequest{Name: "other-workspace"})
	if err != nil {
		t.Fatalf("CreateStore workspace_b: %v", err)
	}
	for _, path := range []string{"/a.md", "/b.md", "/c.md"} {
		createTestMemory(t, service, store.ID, path, strings.Trim(path, "/."), actor)
	}

	pageOne, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:   1,
		View:    memory.ViewBasic,
		OrderBy: memory.MemoryOrderByPath,
		Order:   memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories page one: %v", err)
	}
	if pageOne.NextPage == nil {
		t.Fatal("page one token is nil")
	}
	pageTwo, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:   1,
		Page:    *pageOne.NextPage,
		View:    memory.ViewBasic,
		OrderBy: memory.MemoryOrderByPath,
		Order:   memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories page two: %v", err)
	}
	assertListPaths(t, pageTwo.Data, []string{"/b.md"})

	for label, call := range map[string]func() error{
		"workspace": func() error {
			_, err := service.ListMemories(ctx, "workspace_b", otherWorkspaceStore.ID, memory.ListMemoriesOptions{Limit: 1, Page: *pageOne.NextPage, View: memory.ViewBasic, OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending})
			return err
		},
		"store": func() error {
			_, err := service.ListMemories(ctx, workspace.DefaultID, otherStore.ID, memory.ListMemoriesOptions{Limit: 1, Page: *pageOne.NextPage, View: memory.ViewBasic, OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending})
			return err
		},
		"filter": func() error {
			_, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 1, Page: *pageOne.NextPage, PathPrefix: "/a", View: memory.ViewBasic, OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending})
			return err
		},
		"view": func() error {
			_, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 1, Page: *pageOne.NextPage, View: memory.ViewFull, OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending})
			return err
		},
		"order": func() error {
			_, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 1, Page: *pageOne.NextPage, View: memory.ViewBasic, OrderBy: memory.MemoryOrderByUpdatedAt, Order: memory.SortAscending})
			return err
		},
	} {
		if err := call(); err == nil {
			t.Fatalf("%s token replay succeeded; want validation error", label)
		}
	}
}

func TestMemoryListParentStoreBoundary(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryWorkspace(t, admin, "workspace_b")
	seedMemoryAPIKey(t, admin, "ak_list_parent")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_parent"}
	store := createMemoryStore(t, service, "list-parent")
	createTestMemory(t, service, store.ID, "/kept.md", "kept", actor)
	otherWorkspaceStore, err := service.CreateStore(ctx, "workspace_b", memory.CreateStoreRequest{Name: "other"})
	if err != nil {
		t.Fatalf("CreateStore workspace_b: %v", err)
	}

	for label, storeID := range map[string]string{
		"missing":         "memstore_missing",
		"cross_workspace": otherWorkspaceStore.ID,
	} {
		if _, err := service.ListMemories(ctx, workspace.DefaultID, storeID, memory.ListMemoriesOptions{}); !isMemoryNotFound(err) {
			t.Fatalf("%s ListMemories err = %T %v; want NotFoundError", label, err, err)
		}
	}

	deleted := createMemoryStore(t, service, "list-parent-deleted")
	if err := service.DeleteStore(ctx, workspace.DefaultID, deleted.ID); err != nil {
		t.Fatalf("DeleteStore: %v", err)
	}
	if _, err := service.ListMemories(ctx, workspace.DefaultID, deleted.ID, memory.ListMemoriesOptions{}); !isMemoryNotFound(err) {
		t.Fatalf("deleted store ListMemories err = %T %v; want NotFoundError", err, err)
	}

	if _, err := service.ArchiveStore(ctx, workspace.DefaultID, store.ID); err != nil {
		t.Fatalf("ArchiveStore: %v", err)
	}
	archivedList, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{View: memory.ViewFull})
	if err != nil {
		t.Fatalf("archived store ListMemories: %v", err)
	}
	assertListPaths(t, archivedList.Data, []string{"/kept.md"})
	if archivedList.Data[0].Memory.Content == nil || *archivedList.Data[0].Memory.Content != "kept" {
		t.Fatalf("archived list content = %v; want kept", archivedList.Data[0].Memory.Content)
	}
}

func TestMemoryListPaginationCountsPrefixesInFinalProjection(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_prefix_page")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_prefix_page"}
	store := createMemoryStore(t, service, "prefix-page")
	for _, path := range []string{"/foo.md", "/notes/a.md", "/notes/deep/c.md"} {
		createTestMemory(t, service, store.ID, path, strings.Trim(path, "/."), actor)
	}
	pageOne, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:      1,
		LimitSet:   true,
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByPath,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories projected page one: %v", err)
	}
	assertListPaths(t, pageOne.Data, []string{"/foo.md"})
	if pageOne.NextPage == nil {
		t.Fatal("projected page one next_page is nil")
	}
	pageTwo, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:      1,
		LimitSet:   true,
		Page:       *pageOne.NextPage,
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByPath,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories projected page two: %v", err)
	}
	assertListPaths(t, pageTwo.Data, []string{"/notes/"})
	assertListTypes(t, pageTwo.Data, []string{"memory_prefix"})
	for label, options := range map[string]memory.ListMemoriesOptions{
		"depth":     {Limit: 1, LimitSet: true, Page: *pageOne.NextPage, PathPrefix: "/", Depth: 2, DepthSet: true, OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending},
		"depth_set": {Limit: 1, LimitSet: true, Page: *pageOne.NextPage, PathPrefix: "/", OrderBy: memory.MemoryOrderByPath, Order: memory.SortAscending},
	} {
		if _, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, options); err == nil {
			t.Fatalf("%s token replay succeeded; want validation error", label)
		}
	}
}

func TestMemoryListPaginationResumesAfterDeletedMemoryCursor(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_deleted_cursor")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_deleted_cursor"}
	store := createMemoryStore(t, service, "deleted-cursor")
	first := createTestMemory(t, service, store.ID, "/z.md", "first", actor)
	second := createTestMemory(t, service, store.ID, "/a.md", "second", actor)
	third := createTestMemory(t, service, store.ID, "/m.md", "third", actor)
	setMemoryTime(t, admin, store.ID, first.ID, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
	setMemoryTime(t, admin, store.ID, second.ID, "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z")
	setMemoryTime(t, admin, store.ID, third.ID, "2026-01-03T00:00:00Z", "2026-01-03T00:00:00Z")

	pageOne, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:    1,
		LimitSet: true,
		OrderBy:  memory.MemoryOrderByCreatedAt,
		Order:    memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories deleted cursor page one: %v", err)
	}
	assertListPaths(t, pageOne.Data, []string{"/z.md"})
	if pageOne.NextPage == nil {
		t.Fatal("deleted cursor page one next_page is nil")
	}
	if _, err := service.DeleteMemory(ctx, workspace.DefaultID, store.ID, first.ID, nil, actor); err != nil {
		t.Fatalf("DeleteMemory cursor anchor: %v", err)
	}

	pageTwo, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:    1,
		LimitSet: true,
		Page:     *pageOne.NextPage,
		OrderBy:  memory.MemoryOrderByCreatedAt,
		Order:    memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories deleted cursor page two: %v", err)
	}
	assertListPaths(t, pageTwo.Data, []string{"/a.md"})
}

func TestMemoryListPaginationResumesAfterDeletedUpdatedAtCursor(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_deleted_updated_cursor")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_deleted_updated_cursor"}
	store := createMemoryStore(t, service, "deleted-updated-cursor")
	first := createTestMemory(t, service, store.ID, "/z.md", "first", actor)
	second := createTestMemory(t, service, store.ID, "/a.md", "second", actor)
	third := createTestMemory(t, service, store.ID, "/m.md", "third", actor)
	setMemoryTime(t, admin, store.ID, first.ID, "2026-01-03T00:00:00Z", "2026-01-01T00:00:00Z")
	setMemoryTime(t, admin, store.ID, second.ID, "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z")
	setMemoryTime(t, admin, store.ID, third.ID, "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z")

	pageOne, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:    1,
		LimitSet: true,
		OrderBy:  memory.MemoryOrderByUpdatedAt,
		Order:    memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories deleted updated cursor page one: %v", err)
	}
	assertListPaths(t, pageOne.Data, []string{"/z.md"})
	if pageOne.NextPage == nil {
		t.Fatal("deleted updated cursor page one next_page is nil")
	}
	if _, err := service.DeleteMemory(ctx, workspace.DefaultID, store.ID, first.ID, nil, actor); err != nil {
		t.Fatalf("DeleteMemory updated cursor anchor: %v", err)
	}

	pageTwo, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:    1,
		LimitSet: true,
		Page:     *pageOne.NextPage,
		OrderBy:  memory.MemoryOrderByUpdatedAt,
		Order:    memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories deleted updated cursor page two: %v", err)
	}
	assertListPaths(t, pageTwo.Data, []string{"/a.md"})
}

func TestMemoryListPaginationResumesAfterRemovedSyntheticPrefixCursor(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_removed_prefix_cursor")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_removed_prefix_cursor"}
	store := createMemoryStore(t, service, "removed-prefix-cursor")
	createTestMemory(t, service, store.ID, "/foo.md", "foo", actor)
	prefixChild := createTestMemory(t, service, store.ID, "/notes/a.md", "notes", actor)
	createTestMemory(t, service, store.ID, "/z.md", "z", actor)

	pageOne, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:      2,
		LimitSet:   true,
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByPath,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories removed prefix page one: %v", err)
	}
	assertListPaths(t, pageOne.Data, []string{"/foo.md", "/notes/"})
	assertListTypes(t, pageOne.Data, []string{"memory", "memory_prefix"})
	if pageOne.NextPage == nil {
		t.Fatal("removed prefix page one next_page is nil")
	}
	if _, err := service.DeleteMemory(ctx, workspace.DefaultID, store.ID, prefixChild.ID, nil, actor); err != nil {
		t.Fatalf("DeleteMemory prefix child: %v", err)
	}

	pageTwo, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{
		Limit:      2,
		LimitSet:   true,
		Page:       *pageOne.NextPage,
		PathPrefix: "/",
		Depth:      1,
		DepthSet:   true,
		OrderBy:    memory.MemoryOrderByPath,
		Order:      memory.SortAscending,
	})
	if err != nil {
		t.Fatalf("ListMemories removed prefix page two: %v", err)
	}
	assertListPaths(t, pageTwo.Data, []string{"/z.md"})
}

func TestMemoryListViewLimitsAndFullPageContent(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_view")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_view"}
	store := createMemoryStore(t, service, "list-view")
	for i := 0; i < 25; i++ {
		createTestMemory(t, service, store.ID, fmt.Sprintf("/%02d.md", i), fmt.Sprintf("content-%02d", i), actor)
	}

	basicDefault, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{})
	if err != nil {
		t.Fatalf("ListMemories basic default: %v", err)
	}
	if len(basicDefault.Data) != 20 || basicDefault.Data[0].Memory.Content != nil {
		t.Fatalf("basic default len/content = %d/%v", len(basicDefault.Data), basicDefault.Data[0].Memory.Content)
	}
	if basicDefault.Data[0].Memory.ContentSHA256 == "" || basicDefault.Data[0].Memory.ContentSizeBytes == 0 {
		t.Fatalf("basic default missing hash/size: %+v", basicDefault.Data[0].Memory)
	}
	basicBoundary, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 100, LimitSet: true})
	if err != nil {
		t.Fatalf("ListMemories basic boundary: %v", err)
	}
	if len(basicBoundary.Data) != 25 {
		t.Fatalf("basic boundary len = %d; want 25", len(basicBoundary.Data))
	}
	basicCapped, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 101, LimitSet: true})
	if err != nil {
		t.Fatalf("ListMemories basic capped: %v", err)
	}
	if len(basicCapped.Data) != 25 {
		t.Fatalf("basic capped len = %d; want 25", len(basicCapped.Data))
	}
	fullDefault, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{View: memory.ViewFull})
	if err != nil {
		t.Fatalf("ListMemories full default: %v", err)
	}
	if len(fullDefault.Data) != 20 || fullDefault.Data[0].Memory.Content == nil {
		t.Fatalf("full default len/content = %d/%v", len(fullDefault.Data), fullDefault.Data[0].Memory.Content)
	}
	fullCapped, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 100, LimitSet: true, View: memory.ViewFull})
	if err != nil {
		t.Fatalf("ListMemories full capped: %v", err)
	}
	if len(fullCapped.Data) != 20 {
		t.Fatalf("full capped len = %d; want 20", len(fullCapped.Data))
	}
	fullPage, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 1, LimitSet: true, View: memory.ViewFull, OrderBy: memory.MemoryOrderByPath})
	if err != nil {
		t.Fatalf("ListMemories full page: %v", err)
	}
	assertListPaths(t, fullPage.Data, []string{"/00.md"})
	if fullPage.Data[0].Memory.Content == nil || *fullPage.Data[0].Memory.Content != "content-00" {
		t.Fatalf("full page content = %v; want content-00", fullPage.Data[0].Memory.Content)
	}
	if len(fullPage.Data) != 1 || fullPage.Data[0].Path() == "/01.md" {
		t.Fatalf("full page included out-of-page memory: %+v", fullPage.Data)
	}
	for _, entry := range fullPage.Data {
		if entry.Memory != nil && entry.Memory.Content != nil && *entry.Memory.Content == "content-01" {
			t.Fatalf("full page content affected by out-of-page memory: %+v", entry.Memory)
		}
	}

	observedService, observedAdmin := newMemoryStoreTestEnv(t)
	seedMemoryAPIKey(t, observedAdmin, "ak_list_observed")
	//nolint:gosec // Public test API key id, not raw API key material.
	observedActor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_observed"}
	observableStore := createMemoryStore(t, observedService, "list-final-page-only")
	createTestMemory(t, observedService, observableStore.ID, "/00.md", "visible", observedActor)
	hidden := createTestMemory(t, observedService, observableStore.ID, "/01.md", "hidden", observedActor)
	pointMemoryAtDeletedVersionForHydrationSentinel(t, observedAdmin, observableStore.ID, hidden.ID, observedActor)
	observablePage, err := observedService.ListMemories(ctx, workspace.DefaultID, observableStore.ID, memory.ListMemoriesOptions{Limit: 1, LimitSet: true, View: memory.ViewFull, OrderBy: memory.MemoryOrderByPath})
	if err != nil {
		t.Fatalf("full page with corrupt out-of-page content: %v", err)
	}
	assertListPaths(t, observablePage.Data, []string{"/00.md"})
	if observablePage.Data[0].Memory.Content == nil || *observablePage.Data[0].Memory.Content != "visible" {
		t.Fatalf("observable full page content = %v; want visible", observablePage.Data[0].Memory.Content)
	}
	for _, limit := range []int{0, -1} {
		if _, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: limit, LimitSet: true}); err == nil {
			t.Fatalf("limit %d succeeded; want validation error", limit)
		}
	}
	var validation *memory.ValidationError
	_, err = service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{View: "wide"})
	if !errors.As(err, &validation) {
		t.Fatalf("invalid view err = %T %v; want ValidationError", err, err)
	}
}

func TestMemoryListBasicLimitHundredCap(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_list_limit")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_list_limit"}
	store := createMemoryStore(t, service, "list-limit")
	for i := 0; i < 105; i++ {
		createTestMemory(t, service, store.ID, fmt.Sprintf("/%03d.md", i), "x", actor)
	}
	boundary, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 100, LimitSet: true})
	if err != nil {
		t.Fatalf("ListMemories basic limit 100: %v", err)
	}
	if len(boundary.Data) != 100 || boundary.NextPage == nil {
		t.Fatalf("basic limit 100 len/next = %d/%v; want 100/non-nil", len(boundary.Data), boundary.NextPage)
	}
	capped, err := service.ListMemories(ctx, workspace.DefaultID, store.ID, memory.ListMemoriesOptions{Limit: 101, LimitSet: true})
	if err != nil {
		t.Fatalf("ListMemories basic limit 101: %v", err)
	}
	if len(capped.Data) != 100 || capped.NextPage == nil {
		t.Fatalf("basic limit 101 len/next = %d/%v; want 100/non-nil", len(capped.Data), capped.NextPage)
	}
	for _, entry := range capped.Data {
		if entry.Memory == nil || entry.Memory.Content != nil || entry.Memory.ContentSHA256 == "" || entry.Memory.ContentSizeBytes == 0 {
			t.Fatalf("bad basic projection entry: %+v", entry)
		}
	}
}

func TestMemoryListResultSerializesDirectHeterogeneousEntries(t *testing.T) {
	content := "hello"
	result := memory.MemoryListResult{Data: []memory.MemoryListEntry{
		{Memory: &memory.Memory{ID: "mem_a", Type: "memory", MemoryStoreID: "memstore_a", Path: "/a.md", Content: &content, ContentSHA256: sha256Hex(content), ContentSizeBytes: int64(len(content))}},
		{Prefix: &memory.MemoryPrefix{Type: "memory_prefix", Path: "/notes/"}},
	}}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal MemoryListResult: %v", err)
	}
	body := string(payload)
	if strings.Contains(body, `"Memory"`) || strings.Contains(body, `"Prefix"`) {
		t.Fatalf("serialized list exposed wrapper fields: %s", body)
	}
	if !strings.Contains(body, `"type":"memory"`) || !strings.Contains(body, `"type":"memory_prefix"`) || !strings.Contains(body, `"path":"/notes/"`) {
		t.Fatalf("serialized list missing direct entries: %s", body)
	}
}

func TestDecodeListMemoriesOptionsLimitValidation(t *testing.T) {
	for _, raw := range []string{"limit=abc", "limit=", "limit=0", "limit=-1"} {
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatalf("ParseQuery %s: %v", raw, err)
		}
		if _, err := memory.DecodeListMemoriesOptions(values); err == nil {
			t.Fatalf("DecodeListMemoriesOptions(%s) succeeded; want error", raw)
		}
	}
	values, err := url.ParseQuery("limit=7&page=tok&path_prefix=%2Fnotes%2F&depth=1&view=full&order_by=updated_at&order=desc")
	if err != nil {
		t.Fatalf("ParseQuery valid: %v", err)
	}
	options, err := memory.DecodeListMemoriesOptions(values)
	if err != nil {
		t.Fatalf("DecodeListMemoriesOptions valid: %v", err)
	}
	if options.Limit != 7 || !options.LimitSet || options.Page != "tok" || options.PathPrefix != "/notes/" || options.Depth != 1 || !options.DepthSet || options.View != memory.ViewFull || options.OrderBy != memory.MemoryOrderByUpdatedAt || options.Order != memory.SortDescending {
		t.Fatalf("options = %+v", options)
	}
}

func assertListPaths(t *testing.T, entries []memory.MemoryListEntry, want []string) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("list len = %d; want %d (%+v)", len(entries), len(want), entries)
	}
	for i, expected := range want {
		if got := entries[i].Path(); got != expected {
			t.Fatalf("entry %d path = %q; want %q (entries=%+v)", i, got, expected, entries)
		}
	}
}

func assertListTypes(t *testing.T, entries []memory.MemoryListEntry, want []string) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("list len = %d; want %d (%+v)", len(entries), len(want), entries)
	}
	for i, expected := range want {
		if got := entries[i].Type(); got != expected {
			t.Fatalf("entry %d type = %q; want %q (entries=%+v)", i, got, expected, entries)
		}
	}
}

func setMemoryTime(t *testing.T, admin anySQL, storeID string, memoryID string, createdAt string, updatedAt string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE memories SET created_at = $1, updated_at = $2 WHERE workspace_id = 'default' AND memory_store_id = $3 AND memory_id = $4`,
		createdAt, updatedAt, storeID, memoryID); err != nil {
		t.Fatalf("set memory times: %v", err)
	}
}

func pointMemoryAtDeletedVersionForHydrationSentinel(t *testing.T, admin *sql.DB, storeID string, memoryID string, actor memory.Actor) {
	t.Helper()
	versionID := "memver_hydration_sentinel_" + memoryID
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, created_at, created_actor_type, created_api_key_id, created_session_id, created_user_id)
		 VALUES ('default', $1, $2, $3, 'deleted', '/01.md', '2026-01-01T00:00:00Z', $4, $5, $6, $7)`,
		storeID, memoryID, versionID, actor.Type, nullableString(actor.APIKeyID), nullableString(actor.SessionID), nullableString(actor.UserID)); err != nil {
		t.Fatalf("seed hydration sentinel deleted version: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE memories SET current_version_id = $1 WHERE workspace_id = 'default' AND memory_store_id = $2 AND memory_id = $3`,
		versionID, storeID, memoryID); err != nil {
		t.Fatalf("point hidden memory at hydration sentinel: %v", err)
	}
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
