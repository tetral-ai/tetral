package memory

import (
	"context"
	"errors"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type StoreBackend interface {
	CreateStore(context.Context, workspace.ID, CreateStoreRequest) (*Store, error)
	GetStore(context.Context, workspace.ID, string) (*Store, error)
	UpdateStore(context.Context, workspace.ID, string, StorePatch) (*Store, error)
	ListStores(context.Context, workspace.ID, ListStoresOptions) (StoreListResult, error)
	ArchiveStore(context.Context, workspace.ID, string) (*Store, error)
	DeleteStore(context.Context, workspace.ID, string) error
	CreateMemory(context.Context, workspace.ID, string, CreateMemoryRequest, Actor) (*Memory, error)
	GetMemory(context.Context, workspace.ID, string, string, string) (*Memory, error)
	UpdateMemory(context.Context, workspace.ID, string, string, UpdateMemoryRequest, Actor) (*Memory, error)
	DeleteMemory(context.Context, workspace.ID, string, string, *string, Actor) (*DeleteMemoryResult, error)
	ListMemories(context.Context, workspace.ID, string, ListMemoriesOptions) (MemoryListResult, error)
	ListMemoryVersions(context.Context, workspace.ID, string, ListMemoryVersionsOptions) (MemoryVersionListResult, error)
	GetMemoryVersion(context.Context, workspace.ID, string, string, string) (*MemoryVersion, error)
	RedactMemoryVersion(context.Context, workspace.ID, string, string, Actor) (*MemoryVersion, error)
}

type Service struct {
	store StoreBackend
}

func NewService(store StoreBackend) *Service {
	return &Service{store: store}
}

func (s *Service) CreateStore(ctx context.Context, ws workspace.ID, request CreateStoreRequest) (*Store, error) {
	store, err := s.store.CreateStore(ctx, ws, request)
	return store, safePublicError(err)
}

func (s *Service) GetStore(ctx context.Context, ws workspace.ID, storeID string) (*Store, error) {
	store, err := s.store.GetStore(ctx, ws, storeID)
	return store, safePublicError(err)
}

func (s *Service) UpdateStore(ctx context.Context, ws workspace.ID, storeID string, patch StorePatch) (*Store, error) {
	store, err := s.store.UpdateStore(ctx, ws, storeID, patch)
	return store, safePublicError(err)
}

func (s *Service) ListStores(ctx context.Context, ws workspace.ID, options ListStoresOptions) (StoreListResult, error) {
	result, err := s.store.ListStores(ctx, ws, options)
	return result, safePublicError(err)
}

func (s *Service) ArchiveStore(ctx context.Context, ws workspace.ID, storeID string) (*Store, error) {
	store, err := s.store.ArchiveStore(ctx, ws, storeID)
	return store, safePublicError(err)
}

func (s *Service) DeleteStore(ctx context.Context, ws workspace.ID, storeID string) error {
	return safePublicError(s.store.DeleteStore(ctx, ws, storeID))
}

func (s *Service) CreateMemory(ctx context.Context, ws workspace.ID, storeID string, request CreateMemoryRequest, actor Actor) (*Memory, error) {
	result, err := s.store.CreateMemory(ctx, ws, storeID, request, actor)
	return result, safePublicError(err)
}

func (s *Service) GetMemory(ctx context.Context, ws workspace.ID, storeID string, memoryID string, view string) (*Memory, error) {
	result, err := s.store.GetMemory(ctx, ws, storeID, memoryID, view)
	return result, safePublicError(err)
}

func (s *Service) UpdateMemory(ctx context.Context, ws workspace.ID, storeID string, memoryID string, request UpdateMemoryRequest, actor Actor) (*Memory, error) {
	result, err := s.store.UpdateMemory(ctx, ws, storeID, memoryID, request, actor)
	return result, safePublicError(err)
}

func (s *Service) DeleteMemory(ctx context.Context, ws workspace.ID, storeID string, memoryID string, expectedHash *string, actor Actor) (*DeleteMemoryResult, error) {
	result, err := s.store.DeleteMemory(ctx, ws, storeID, memoryID, expectedHash, actor)
	return result, safePublicError(err)
}

func (s *Service) ListMemories(ctx context.Context, ws workspace.ID, storeID string, options ListMemoriesOptions) (MemoryListResult, error) {
	result, err := s.store.ListMemories(ctx, ws, storeID, options)
	return result, safePublicError(err)
}

func (s *Service) ListMemoryVersions(ctx context.Context, ws workspace.ID, storeID string, options ListMemoryVersionsOptions) (MemoryVersionListResult, error) {
	result, err := s.store.ListMemoryVersions(ctx, ws, storeID, options)
	return result, safePublicError(err)
}

func (s *Service) GetMemoryVersion(ctx context.Context, ws workspace.ID, storeID string, versionID string, view string) (*MemoryVersion, error) {
	result, err := s.store.GetMemoryVersion(ctx, ws, storeID, versionID, view)
	return result, safePublicError(err)
}

func (s *Service) RedactMemoryVersion(ctx context.Context, ws workspace.ID, storeID string, versionID string, actor Actor) (*MemoryVersion, error) {
	result, err := s.store.RedactMemoryVersion(ctx, ws, storeID, versionID, actor)
	return result, safePublicError(err)
}

func safePublicError(err error) error {
	if err == nil {
		return nil
	}
	var validation *ValidationError
	var notFound *NotFoundError
	var pathConflict *PathConflictError
	var precondition *PreconditionFailedError
	var quota *QuotaError
	var tooLarge *RequestTooLargeError
	var internal *InternalError
	switch {
	case errors.As(err, &validation),
		errors.As(err, &notFound),
		errors.As(err, &pathConflict),
		errors.As(err, &precondition),
		errors.As(err, &quota),
		errors.As(err, &tooLarge),
		errors.As(err, &internal):
		return err
	default:
		return &InternalError{Message: "memory operation failed", Cause: err}
	}
}
