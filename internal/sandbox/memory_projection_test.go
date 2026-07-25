package sandbox

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMaterializeMemoryProjectionsBuildsVerifiedSnapshotPerStore(t *testing.T) {
	var events []string
	reader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"memstore_rw": {
				{Path: "/notes/todo.md", Content: "one", ContentSHA256: "sha-one"},
			},
			"memstore_ro": nil,
		},
		eventsRef: &events,
	}
	locker := &recordingMemoryStoreMutationLocker{eventsRef: &events}
	materializer := &recordingMemoryStoreMaterializer{eventsRef: &events}
	err := MaterializeMemoryProjections(context.Background(), reader, materializer, MemoryProjectionMaterializationRequest{
		WorkspaceID:       workspace.DefaultID,
		ProviderSandboxID: "provider_memory_prepare",
		MutationLocker:    locker,
		Resources: ResourceSetup{MemoryStores: []MemoryStoreMount{
			{MemoryStoreID: "memstore_rw", MountPath: "/mnt/memory/project", Access: "read_write"},
			{MemoryStoreID: "memstore_ro", MountPath: "/mnt/memory/archive", Access: "read_only"},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeMemoryProjections: %v", err)
	}
	if len(reader.reads) != 2 || reader.reads[0] != "memstore_rw" || reader.reads[1] != "memstore_ro" {
		t.Fatalf("snapshot reads = %v; want each attached store in order", reader.reads)
	}
	wantEvents := []string{
		"lock:memstore_ro,memstore_rw",
		"read:memstore_rw",
		"read:memstore_ro",
		"memory",
		"memory",
		"unlock",
	}
	if len(events) != len(wantEvents) {
		t.Fatalf("events = %v; want %v", events, wantEvents)
	}
	for i := range wantEvents {
		if events[i] != wantEvents[i] {
			t.Fatalf("events = %v; want %v", events, wantEvents)
		}
	}
	if len(materializer.calls) != 2 {
		t.Fatalf("materializer calls = %d; want one per memory mount", len(materializer.calls))
	}
	first := materializer.calls[0]
	if first.ProviderSandboxID != "provider_memory_prepare" || first.MountPath != "/mnt/memory/project" ||
		len(first.Files) != 1 || first.Files[0].Path != "/notes/todo.md" || first.Files[0].Content != "one" {
		t.Fatalf("first materialization = %+v; want writable store snapshot with verbatim mount", first)
	}
	second := materializer.calls[1]
	if second.MountPath != "/mnt/memory/archive" || len(second.Files) != 0 {
		t.Fatalf("second materialization = %+v; want empty read-only store directory branch", second)
	}

	materializer.err = errors.New("projection failed")
	if err := MaterializeMemoryProjections(context.Background(), reader, materializer, MemoryProjectionMaterializationRequest{
		WorkspaceID:       workspace.DefaultID,
		ProviderSandboxID: "provider_memory_prepare",
		MutationLocker:    locker,
		Resources:         ResourceSetup{MemoryStores: []MemoryStoreMount{{MemoryStoreID: "memstore_rw", MountPath: "/mnt/memory/project"}}},
	}); err == nil {
		t.Fatal("MaterializeMemoryProjections ignored materializer error")
	}
}

func TestMaterializeMemoryProjectionsRemovesDeletedStoresUnderMutationLock(t *testing.T) {
	var events []string
	reader := &recordingMemorySnapshotReader{snapshots: map[string][]MemorySnapshotFile{"memstore_successor": nil}, eventsRef: &events}
	locker := &recordingMemoryStoreMutationLocker{eventsRef: &events}
	materializer := &recordingMemoryStoreMaterializer{eventsRef: &events}
	cleanup := &recordingSessionResourceCleanupCoordinator{eventsRef: &events, pending: true}
	err := MaterializeMemoryProjections(context.Background(), reader, materializer, MemoryProjectionMaterializationRequest{
		WorkspaceID:       workspace.DefaultID,
		ProviderSandboxID: "provider_memory_delete",
		MutationLocker:    locker,
		ResourceCleanup:   cleanup,
		Resources: ResourceSetup{
			DeletedMemoryStores: []MemoryStoreMount{{
				ResourceID: "sesrsc_memory_deleted", MemoryStoreID: "memstore_deleted", MountPath: "/mnt/memory/replacement",
			}},
			MemoryStores: []MemoryStoreMount{{
				ResourceID: "sesrsc_memory_successor", MemoryStoreID: "memstore_successor", MountPath: "/mnt/memory/replacement",
			}},
		},
	})
	if err != nil {
		t.Fatalf("MaterializeMemoryProjections delete: %v", err)
	}
	if len(materializer.removals) != 1 || materializer.removals[0].MountPath != "/mnt/memory/replacement" {
		t.Fatalf("memory removals = %+v; want deleted mount", materializer.removals)
	}
	if len(materializer.calls) != 1 || materializer.calls[0].MountPath != "/mnt/memory/replacement" {
		t.Fatalf("memory materializations = %+v; want same-path successor through ordinary materializer", materializer.calls)
	}
	if want := []string{"lock:memstore_deleted,memstore_successor", "memory-remove", "resource-detach", "read:memstore_successor", "memory", "unlock"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v; want %v", events, want)
	}
}

func TestMaterializeMemoryProjectionsRemovalFailureRetriesBeforeDetachAndSuccessor(t *testing.T) {
	var events []string
	wantErr := errors.New("memory removal unavailable")
	reader := &recordingMemorySnapshotReader{snapshots: map[string][]MemorySnapshotFile{"memstore_successor": nil}, eventsRef: &events}
	locker := &recordingMemoryStoreMutationLocker{eventsRef: &events}
	materializer := &recordingMemoryStoreMaterializer{eventsRef: &events, err: wantErr}
	cleanup := &recordingSessionResourceCleanupCoordinator{eventsRef: &events, pending: true}
	request := MemoryProjectionMaterializationRequest{
		WorkspaceID: workspace.DefaultID, ProviderSandboxID: "provider_memory_retry", MutationLocker: locker, ResourceCleanup: cleanup,
		Resources: ResourceSetup{
			DeletedMemoryStores: []MemoryStoreMount{{ResourceID: "sesrsc_memory_deleted", MemoryStoreID: "memstore_deleted", MountPath: "/mnt/memory/project"}},
			MemoryStores:        []MemoryStoreMount{{ResourceID: "sesrsc_memory_successor", MemoryStoreID: "memstore_successor", MountPath: "/mnt/memory/project"}},
		},
	}
	if err := MaterializeMemoryProjections(context.Background(), reader, materializer, request); !errors.Is(err, wantErr) {
		t.Fatalf("first removal error = %v; want %v", err, wantErr)
	}
	if want := []string{"lock:memstore_deleted,memstore_successor", "memory-remove", "unlock"}; !slices.Equal(events, want) {
		t.Fatalf("failure events=%v; want %v with no detach/read/successor", events, want)
	}
	if len(reader.reads) != 0 || len(materializer.calls) != 0 || !cleanup.pending {
		t.Fatalf("failure reads=%v successors=%d pending=%v; want blocked successor and durable pending", reader.reads, len(materializer.calls), cleanup.pending)
	}
	materializer.err = nil
	if err := MaterializeMemoryProjections(context.Background(), reader, materializer, request); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if want := []string{"lock:memstore_deleted,memstore_successor", "memory-remove", "unlock", "lock:memstore_deleted,memstore_successor", "memory-remove", "resource-detach", "read:memstore_successor", "memory", "unlock"}; !slices.Equal(events, want) {
		t.Fatalf("retry events=%v; want %v", events, want)
	}
}

type recordingSessionResourceCleanupCoordinator struct {
	eventsRef *[]string
	pending   bool
	err       error
}

func (c *recordingSessionResourceCleanupCoordinator) CleanupSessionResource(ctx context.Context, _ string, remove func(context.Context) error) error {
	if c.err != nil {
		return c.err
	}
	if !c.pending {
		return nil
	}
	if err := remove(ctx); err != nil {
		return err
	}
	if c.eventsRef != nil {
		*c.eventsRef = append(*c.eventsRef, "resource-detach")
	}
	c.pending = false
	return nil
}

func TestMaterializeMemoryProjectionsRejectsPrefixConflictingSnapshot(t *testing.T) {
	reader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"memstore_valid": {
				{Path: "/notes/todo.md", Content: "one", ContentSHA256: "sha-one"},
			},
			"memstore_conflict": {
				{Path: "/a", Content: "a", ContentSHA256: "sha-a"},
				{Path: "/a.txt", Content: "a.txt", ContentSHA256: "sha-atxt"},
				{Path: "/a/b", Content: "b", ContentSHA256: "sha-b"},
			},
		},
	}
	materializer := &recordingMemoryStoreMaterializer{}
	locker := &recordingMemoryStoreMutationLocker{}
	err := MaterializeMemoryProjections(context.Background(), reader, materializer, MemoryProjectionMaterializationRequest{
		WorkspaceID:       workspace.DefaultID,
		ProviderSandboxID: "provider_memory_prepare",
		MutationLocker:    locker,
		Resources: ResourceSetup{MemoryStores: []MemoryStoreMount{
			{MemoryStoreID: "memstore_valid", MountPath: "/mnt/memory/valid"},
			{MemoryStoreID: "memstore_conflict", MountPath: "/mnt/memory/project"},
		}},
	})
	if err == nil {
		t.Fatal("MaterializeMemoryProjections accepted prefix-conflicting snapshot")
	}
	if len(reader.reads) != 2 || reader.reads[0] != "memstore_valid" || reader.reads[1] != "memstore_conflict" {
		t.Fatalf("snapshot reads = %v; want validation to inspect every store before writing", reader.reads)
	}
	if len(materializer.calls) != 0 {
		t.Fatalf("materializer calls = %d; want fail before partial write", len(materializer.calls))
	}
}

func TestMaterializeMemoryProjectionsRejectsMemoryRootMountPath(t *testing.T) {
	reader := &recordingMemorySnapshotReader{}
	locker := &recordingMemoryStoreMutationLocker{}
	materializer := &recordingMemoryStoreMaterializer{}
	err := MaterializeMemoryProjections(context.Background(), reader, materializer, MemoryProjectionMaterializationRequest{
		WorkspaceID:       workspace.DefaultID,
		ProviderSandboxID: "provider_memory_prepare",
		MutationLocker:    locker,
		Resources: ResourceSetup{MemoryStores: []MemoryStoreMount{{
			MemoryStoreID: "memstore_root",
			MountPath:     "/mnt/memory",
		}}},
	})
	if err == nil {
		t.Fatal("MaterializeMemoryProjections accepted /mnt/memory as a store mount path")
	}
	if len(locker.calls) != 0 || len(reader.reads) != 0 || len(materializer.calls) != 0 {
		t.Fatalf("calls after invalid mount: locks=%v reads=%v materializations=%d; want none", locker.calls, reader.reads, len(materializer.calls))
	}
}

type recordingMemorySnapshotReader struct {
	snapshots map[string][]MemorySnapshotFile
	reads     []string
	err       error
	eventsRef *[]string
}

func (r *recordingMemorySnapshotReader) ReadMemoryStoreSnapshot(_ context.Context, _ workspace.ID, memoryStoreID string) ([]MemorySnapshotFile, error) {
	r.reads = append(r.reads, memoryStoreID)
	if r.eventsRef != nil {
		*r.eventsRef = append(*r.eventsRef, "read:"+memoryStoreID)
	}
	if r.err != nil {
		return nil, r.err
	}
	return append([]MemorySnapshotFile(nil), r.snapshots[memoryStoreID]...), nil
}

func (r *recordingMemorySnapshotReader) WithMemoryStoreMutationLocks(ctx context.Context, ws workspace.ID, memoryStoreIDs []string, fn func(context.Context) error) error {
	locker := recordingMemoryStoreMutationLocker{eventsRef: r.eventsRef}
	return locker.WithMemoryStoreMutationLocks(ctx, ws, memoryStoreIDs, fn)
}

type recordingMemoryStoreMutationLocker struct {
	calls     [][]string
	err       error
	eventsRef *[]string
}

func (l *recordingMemoryStoreMutationLocker) WithMemoryStoreMutationLocks(ctx context.Context, _ workspace.ID, memoryStoreIDs []string, fn func(context.Context) error) error {
	ids := append([]string(nil), memoryStoreIDs...)
	l.calls = append(l.calls, ids)
	if l.eventsRef != nil {
		*l.eventsRef = append(*l.eventsRef, "lock:"+strings.Join(ids, ","))
		defer func() {
			*l.eventsRef = append(*l.eventsRef, "unlock")
		}()
	}
	if l.err != nil {
		return l.err
	}
	return fn(ctx)
}

type recordingMemoryStoreMaterializer struct {
	calls     []MemoryStoreMaterialization
	removals  []MemoryStoreMount
	err       error
	eventsRef *[]string
}

func (m *recordingMemoryStoreMaterializer) RemoveMemoryStore(_ context.Context, _ string, mount MemoryStoreMount) error {
	if m.eventsRef != nil {
		*m.eventsRef = append(*m.eventsRef, "memory-remove")
	}
	m.removals = append(m.removals, mount)
	return m.err
}

func (m *recordingMemoryStoreMaterializer) MaterializeMemoryStore(_ context.Context, materialization MemoryStoreMaterialization) error {
	if m.eventsRef != nil {
		*m.eventsRef = append(*m.eventsRef, "memory")
	}
	copied := MemoryStoreMaterialization{
		ProviderSandboxID: materialization.ProviderSandboxID,
		MountPath:         materialization.MountPath,
		Files:             append([]MemorySnapshotFile(nil), materialization.Files...),
	}
	m.calls = append(m.calls, copied)
	return m.err
}
