package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMemoryCreatePathConflictPrecedesQuotaBoundaries(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_create_conflict_precedence")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_create_conflict_precedence"}

	for _, testCase := range []struct {
		name                string
		seed                func(*testing.T, string)
		store               string
		conflictingMemoryID string
		conflictingPath     string
	}{
		{
			name:                "memory count quota",
			store:               "create-conflict-memory-count",
			conflictingMemoryID: "mem_quota_identity_1",
			conflictingPath:     "/quota-identity-1.md",
			seed: func(t *testing.T, storeID string) {
				seedMemoryIdentities(t, admin, storeID, memory.MaxMemoriesPerStore)
			},
		},
		{
			name:                "version count quota",
			store:               "create-conflict-version-count",
			conflictingMemoryID: "mem_version_conflict",
			conflictingPath:     "/version-count.md",
			seed: func(t *testing.T, storeID string) {
				seedVersionCount(t, admin, storeID, "mem_version_conflict", memory.MaxMemoryVersionsPerStore)
			},
		},
		{
			name:                "retained payload quota",
			store:               "create-conflict-retained-payload",
			conflictingMemoryID: "mem_quota_bytes",
			conflictingPath:     "/quota.md",
			seed: func(t *testing.T, storeID string) {
				seedRetainedBytes(t, admin, storeID, memory.MaxRetainedMemoryPayloadBytesPerStore)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			storeID := seedMemoryStoreBySQL(t, admin, testCase.store)
			testCase.seed(t, storeID)

			_, err := service.CreateMemory(ctx, workspace.DefaultID, storeID, memory.CreateMemoryRequest{
				Path:       testCase.conflictingPath,
				Content:    "new",
				ContentSet: true,
			}, actor)

			assertPathConflict(t, err, testCase.conflictingMemoryID, testCase.conflictingPath)
		})
	}
}

func TestMemoryUpdatePathConflictPrecedesQuotaBoundaries(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_update_conflict_precedence")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update_conflict_precedence"}

	for _, testCase := range []struct {
		name                string
		seed                func(*testing.T, string)
		store               string
		sourceMemoryID      string
		conflictingMemoryID string
	}{
		{
			name:                "memory count quota",
			store:               "update-conflict-memory-count",
			sourceMemoryID:      "mem_update_source_memory_count",
			conflictingMemoryID: "mem_update_target_memory_count",
			seed: func(t *testing.T, storeID string) {
				seedMemoryIdentities(t, admin, storeID, memory.MaxMemoriesPerStore-2)
				seedActiveMemory(t, admin, storeID, "mem_update_source_memory_count", "memver_update_source_memory_count", "/rename-source.md", "source")
				seedActiveMemory(t, admin, storeID, "mem_update_target_memory_count", "memver_update_target_memory_count", "/rename-target.md", "target")
			},
		},
		{
			name:                "version count quota",
			store:               "update-conflict-version-count",
			sourceMemoryID:      "mem_update_source_version_count",
			conflictingMemoryID: "mem_update_target_version_count",
			seed: func(t *testing.T, storeID string) {
				seedActiveMemory(t, admin, storeID, "mem_update_source_version_count", "memver_update_source_version_count", "/rename-source.md", "source")
				seedActiveMemory(t, admin, storeID, "mem_update_target_version_count", "memver_update_target_version_count", "/rename-target.md", "target")
				seedAdditionalVersions(t, admin, storeID, "mem_update_source_version_count", memory.MaxMemoryVersionsPerStore-2)
			},
		},
		{
			name:                "retained payload quota",
			store:               "update-conflict-retained-payload",
			sourceMemoryID:      "mem_update_source_retained_payload",
			conflictingMemoryID: "mem_update_target_retained_payload",
			seed: func(t *testing.T, storeID string) {
				seedRetainedBytes(t, admin, storeID, memory.MaxRetainedMemoryPayloadBytesPerStore)
				seedActiveMemory(t, admin, storeID, "mem_update_source_retained_payload", "memver_update_source_retained_payload", "/rename-source.md", "source")
				seedActiveMemory(t, admin, storeID, "mem_update_target_retained_payload", "memver_update_target_retained_payload", "/rename-target.md", "target")
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			storeID := seedMemoryStoreBySQL(t, admin, testCase.store)
			testCase.seed(t, storeID)

			_, err := service.UpdateMemory(ctx, workspace.DefaultID, storeID, testCase.sourceMemoryID, memory.UpdateMemoryRequest{
				Path: strPtr("/rename-target.md"),
			}, actor)

			assertPathConflict(t, err, testCase.conflictingMemoryID, "/rename-target.md")
		})
	}
}

func TestMemoryCreateRejectsPrefixConflictingPaths(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_create_prefix_conflict")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_create_prefix_conflict"}

	for _, testCase := range []struct {
		name         string
		existingPath string
		targetPath   string
	}{
		{name: "target descendant", existingPath: "/a", targetPath: "/a/b"},
		{name: "target ancestor", existingPath: "/a/b", targetPath: "/a"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := createMemoryStore(t, service, "create-prefix-"+testCase.name)
			existing := createTestMemory(t, service, store.ID, testCase.existingPath, "existing", actor)

			_, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
				Path:       testCase.targetPath,
				Content:    "new",
				ContentSet: true,
			}, actor)

			assertPathConflict(t, err, existing.ID, testCase.existingPath)
		})
	}
}

func TestMemoryUpdateRejectsPrefixConflictingPaths(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_update_prefix_conflict")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_update_prefix_conflict"}

	for _, testCase := range []struct {
		name         string
		existingPath string
		targetPath   string
	}{
		{name: "target descendant", existingPath: "/a", targetPath: "/a/b"},
		{name: "target ancestor", existingPath: "/a/b", targetPath: "/a"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := createMemoryStore(t, service, "update-prefix-"+testCase.name)
			source := createTestMemory(t, service, store.ID, "/source.md", "source", actor)
			existing := createTestMemory(t, service, store.ID, testCase.existingPath, "existing", actor)

			_, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, source.ID, memory.UpdateMemoryRequest{
				Path: strPtr(testCase.targetPath),
			}, actor)

			assertPathConflict(t, err, existing.ID, testCase.existingPath)
		})
	}
}

func TestMemoryPrefixConflictChecksDoNotTreatPercentOrUnderscoreAsWildcards(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_prefix_literal_chars")
	//nolint:gosec // Public test API key id, not raw API key material.
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_prefix_literal_chars"}
	store := createMemoryStore(t, service, "prefix-literal-chars")
	createTestMemory(t, service, store.ID, "/a_%", "literal", actor)

	created, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{
		Path:       "/aX/child",
		Content:    "not a wildcard match",
		ContentSet: true,
	}, actor)
	if err != nil {
		t.Fatalf("CreateMemory literal wildcard-shaped path: %v", err)
	}
	source := createTestMemory(t, service, store.ID, "/source.md", "source", actor)
	updated, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, source.ID, memory.UpdateMemoryRequest{
		Path: strPtr("/aY/child"),
	}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory literal wildcard-shaped path: %v", err)
	}
	if created.Path != "/aX/child" || updated.Path != "/aY/child" {
		t.Fatalf("literal wildcard-shaped paths not preserved: create=%s update=%s", created.Path, updated.Path)
	}
}

func assertPathConflict(t *testing.T, err error, conflictingMemoryID string, conflictingPath string) {
	t.Helper()
	var conflict *memory.PathConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %T %v; want PathConflictError", err, err)
	}
	if conflict.ConflictingMemoryID != conflictingMemoryID || conflict.ConflictingPath != conflictingPath {
		t.Fatalf("conflict = %+v; want id %s path %s", conflict, conflictingMemoryID, conflictingPath)
	}
}
