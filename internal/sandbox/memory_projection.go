package sandbox

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// Memory projection subsystem — MaterializeMemoryProjections renders the durable
// memory head into the sandbox /mnt/memory tree during resource convergence.
//
// OWNS: the /mnt/memory projection layout and its activation-time (re)materialization
// and deleted-store removal. It reads durable truth through MemorySnapshotReader
// and writes the sandbox through MemoryStoreMaterializer (driver side).
//
// INVARIANTS:
//   - The /mnt/memory tree is a disposable, one-way READ projection of PostgreSQL
//     truth: bytes flow durable -> sandbox only and are never read back. A lost or
//     corrupted projection is repaired by full re-materialization, never trusted as
//     a source of truth. The paths are a read-only projection of the CURRENT memory
//     head; the live post-mutation refresh (driven by Sandbox Service from the
//     durable memory-projection queue) touches only the current session's bound
//     sandbox projection.
//   - Every attached store is materialized regardless of access mode — read_write
//     and read_only alike (the loop here applies no access filter). Access governs
//     only whether the mutation path (resolveWritableMemoryStoreTx) selects a store
//     for writes, not whether it is projected.
//   - MountPath is consumed verbatim from session_memory_store_resources, which
//     internal/session mount derivation (internal/session/service.go) writes once at
//     session create. It is never re-derived from the store slug here, because
//     re-deriving would diverge from the stored path on collision suffixes.
//   - The per-store mutation lock (WithMemoryStoreMutationLocks) is held across BOTH
//     the snapshot read AND the materialize swap — one read-modify-write. Releasing
//     it between them would let a concurrent RunMemory commit clobber the store that
//     the swap then overwrites.
//   - Deleted-store mount removal is processed BEFORE any materialization and routed
//     through the session resource cleanup coordinator, so the durable detach commits
//     only after the sandbox rm ACK and before any same-path successor materializes;
//     this prevents a crash-retry from rm -rf'ing a successor's tree.
//   - Prefix-freeness (no active path is a prefix of another) is required because a
//     filesystem cannot represent /a and /a/b both as active. The durable model
//     permits such overlaps, so it is enforced at THREE sites: memory tool
//     create/rename, the public Memory API, and this materialization snapshot scan
//     (validatePrefixFreeMemorySnapshot). The two SQL sites use an exact
//     left(path,len)=..||'/' predicate rather than LIKE, because a path may legally
//     contain % and _. The scan here checks each path's ancestor prefixes against a
//     SET rather than scanning sorted adjacent pairs: byte order places '.' before
//     '/', so {/a, /a.txt, /a/b} sorts /a.txt between the conflicting pair and an
//     adjacency scan would miss it.
//   - The projection directory holds memory file CONTENT and nothing else: store
//     name, description, and instructions are never written as files here; they are
//     rendered into the system-prompt memory section by Runtime Core instead.
//
// UPDATE-WITH: internal/sandbox/driver/memory_projection.go (the driver command
// sequences); internal/session/service.go (mount derivation);
// internal/memory/postgresql_store.go and the memory tool / public Memory API
// prefix-free sites.
type MemorySnapshotReader interface {
	ReadMemoryStoreSnapshot(ctx context.Context, ws workspace.ID, memoryStoreID string) ([]MemorySnapshotFile, error)
}

type MemoryStoreMutationLocker interface {
	WithMemoryStoreMutationLocks(ctx context.Context, ws workspace.ID, memoryStoreIDs []string, fn func(context.Context) error) error
}

type MemorySnapshotFile struct {
	Path          string
	Content       string
	ContentSHA256 string
}

type MemoryStoreMaterializer interface {
	MaterializeMemoryStore(ctx context.Context, materialization MemoryStoreMaterialization) error
	RemoveMemoryStore(ctx context.Context, providerSandboxID string, mount MemoryStoreMount) error
}

type MemoryStoreMaterialization struct {
	ProviderSandboxID string
	MountPath         string
	Files             []MemorySnapshotFile
}

type MemoryProjectionMaterializationRequest struct {
	WorkspaceID       workspace.ID
	ProviderSandboxID string
	Resources         ResourceSetup
	MutationLocker    MemoryStoreMutationLocker
}

func MaterializeMemoryProjections(ctx context.Context, reader MemorySnapshotReader, materializer MemoryStoreMaterializer, request MemoryProjectionMaterializationRequest) error {
	if len(request.Resources.MemoryStores) == 0 && len(request.Resources.DeletedMemoryStores) == 0 {
		return nil
	}
	if reader == nil {
		return errors.New("memory snapshot reader is required")
	}
	if materializer == nil {
		return errors.New("memory store materializer is required")
	}
	if request.MutationLocker == nil {
		return errors.New("memory store mutation locker is required")
	}
	if request.WorkspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if request.ProviderSandboxID == "" {
		return errors.New("provider sandbox id is required")
	}
	for _, mount := range append(append([]MemoryStoreMount(nil), request.Resources.MemoryStores...), request.Resources.DeletedMemoryStores...) {
		if mount.MemoryStoreID == "" {
			return errors.New("memory_store_id is required")
		}
		if err := validateMemoryStoreMountPath(mount.MountPath); err != nil {
			return err
		}
	}
	lockedMounts := append(append([]MemoryStoreMount(nil), request.Resources.MemoryStores...), request.Resources.DeletedMemoryStores...)
	return request.MutationLocker.WithMemoryStoreMutationLocks(ctx, request.WorkspaceID, memoryStoreIDsForLocks(lockedMounts), func(ctx context.Context) error {
		for _, mount := range request.Resources.DeletedMemoryStores {
			remove := func(ctx context.Context) error {
				return materializer.RemoveMemoryStore(ctx, request.ProviderSandboxID, mount)
			}
			if err := remove(ctx); err != nil {
				return err
			}
		}
		materializations := make([]MemoryStoreMaterialization, 0, len(request.Resources.MemoryStores))
		for _, mount := range request.Resources.MemoryStores {
			files, err := reader.ReadMemoryStoreSnapshot(ctx, request.WorkspaceID, mount.MemoryStoreID)
			if err != nil {
				return err
			}
			if err := validatePrefixFreeMemorySnapshot(files); err != nil {
				return err
			}
			materializations = append(materializations, MemoryStoreMaterialization{
				ProviderSandboxID: request.ProviderSandboxID,
				MountPath:         mount.MountPath,
				Files:             files,
			})
		}
		for _, materialization := range materializations {
			if err := materializer.MaterializeMemoryStore(ctx, MemoryStoreMaterialization{
				ProviderSandboxID: materialization.ProviderSandboxID,
				MountPath:         materialization.MountPath,
				Files:             materialization.Files,
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func memoryStoreIDsForLocks(mounts []MemoryStoreMount) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		if mount.MemoryStoreID == "" {
			continue
		}
		if _, ok := seen[mount.MemoryStoreID]; ok {
			continue
		}
		seen[mount.MemoryStoreID] = struct{}{}
		out = append(out, mount.MemoryStoreID)
	}
	sort.Strings(out)
	return out
}

func validateMemoryStoreMountPath(mountPath string) error {
	if mountPath == "" || strings.Contains(mountPath, "\x00") || !strings.HasPrefix(mountPath, "/") {
		return errors.New("memory mount path must be absolute and valid")
	}
	if clean := path.Clean(mountPath); clean != mountPath {
		return errors.New("memory mount path must be lexically clean")
	}
	if mountPath == "/mnt/memory" || !strings.HasPrefix(mountPath, "/mnt/memory/") {
		return errors.New("memory mount path must name a store below /mnt/memory")
	}
	if mountPath == "/mnt/memory/.staging" || strings.HasPrefix(mountPath, "/mnt/memory/.staging/") {
		return errors.New("memory mount path must not overlap projection staging")
	}
	return nil
}

func validatePrefixFreeMemorySnapshot(files []MemorySnapshotFile) error {
	paths := make(map[string]struct{}, len(files))
	for _, file := range files {
		if !strings.HasPrefix(file.Path, "/") || file.Path == "/" {
			return fmt.Errorf("memory snapshot path %q is invalid", file.Path)
		}
		paths[file.Path] = struct{}{}
	}
	for pathValue := range paths {
		for offset := 1; offset < len(pathValue); offset++ {
			if pathValue[offset] != '/' {
				continue
			}
			if _, ok := paths[pathValue[:offset]]; ok {
				return fmt.Errorf("memory snapshot path %q conflicts with ancestor %q", pathValue, pathValue[:offset])
			}
		}
	}
	return nil
}
