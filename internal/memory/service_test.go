package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestServiceInternalErrorPreservesDiagnosticCause(t *testing.T) {
	diagnostic := &dbconnect.DiagnosticError{
		Provider:  dbconnect.ProviderPlainDSN,
		Phase:     dbconnect.PhaseRuntimeQuery,
		Kind:      dbconnect.KindInternalError,
		Operation: "memory.stores.get",
		Message:   "private database diagnostic",
		Cause:     errors.New("private driver detail"),
	}
	service := memory.NewService(&diagnosticMemoryBackend{getStoreError: diagnostic})

	_, err := service.GetStore(context.Background(), workspace.DefaultID, "memstore_missing")
	var internal *memory.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("GetStore error = %T %v; want InternalError", err, err)
	}
	var gotDiagnostic *dbconnect.DiagnosticError
	if !errors.As(err, &gotDiagnostic) {
		t.Fatalf("GetStore error = %T %v; want DiagnosticError cause", err, err)
	}
	if gotDiagnostic != diagnostic {
		t.Fatalf("diagnostic cause = %p; want %p", gotDiagnostic, diagnostic)
	}
	if err.Error() != "memory operation failed" {
		t.Fatalf("public error = %q; want safe memory message", err.Error())
	}
}

type diagnosticMemoryBackend struct {
	getStoreError error
}

func (b *diagnosticMemoryBackend) CreateStore(context.Context, workspace.ID, memory.CreateStoreRequest) (*memory.Store, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) GetStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	return nil, b.getStoreError
}

func (b *diagnosticMemoryBackend) UpdateStore(context.Context, workspace.ID, string, memory.StorePatch) (*memory.Store, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) ListStores(context.Context, workspace.ID, memory.ListStoresOptions) (memory.StoreListResult, error) {
	return memory.StoreListResult{}, nil
}

func (b *diagnosticMemoryBackend) ArchiveStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) DeleteStore(context.Context, workspace.ID, string) error {
	return nil
}

func (b *diagnosticMemoryBackend) CreateMemory(context.Context, workspace.ID, string, memory.CreateMemoryRequest, memory.Actor) (*memory.Memory, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) GetMemory(context.Context, workspace.ID, string, string, string) (*memory.Memory, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) UpdateMemory(context.Context, workspace.ID, string, string, memory.UpdateMemoryRequest, memory.Actor) (*memory.Memory, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) DeleteMemory(context.Context, workspace.ID, string, string, *string, memory.Actor) (*memory.DeleteMemoryResult, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) ListMemories(context.Context, workspace.ID, string, memory.ListMemoriesOptions) (memory.MemoryListResult, error) {
	return memory.MemoryListResult{}, nil
}

func (b *diagnosticMemoryBackend) ListMemoryVersions(context.Context, workspace.ID, string, memory.ListMemoryVersionsOptions) (memory.MemoryVersionListResult, error) {
	return memory.MemoryVersionListResult{}, nil
}

func (b *diagnosticMemoryBackend) GetMemoryVersion(context.Context, workspace.ID, string, string, string) (*memory.MemoryVersion, error) {
	return nil, nil
}

func (b *diagnosticMemoryBackend) RedactMemoryVersion(context.Context, workspace.ID, string, string, memory.Actor) (*memory.MemoryVersion, error) {
	return nil, nil
}
