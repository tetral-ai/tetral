package files

import (
	"context"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type StoreBackend interface {
	CreateUploadedFile(context.Context, workspace.ID, *StagedUpload) (*FileMetadata, error)
	ListFiles(context.Context, workspace.ID, ListOptions) (ListResult, error)
	GetFile(context.Context, workspace.ID, string) (*FileMetadata, error)
	DeleteFile(context.Context, workspace.ID, string) (*DeleteResponse, error)
	OpenContent(context.Context, workspace.ID, string) (*ContentStream, error)
	ResolveSource(context.Context, workspace.ID, string) (*Source, error)
	ResolveMountSource(context.Context, workspace.ID, string, string) (*MountSource, error)
	ValidateSessionFileSource(context.Context, workspace.ID, string) error
	CreateSessionFileIdentity(context.Context, SessionTransaction, workspace.ID, SessionFileIdentityRequest) (*FileMetadata, error)
	TombstoneSessionFileIdentity(context.Context, SessionTransaction, workspace.ID, string, string) error
}

type Service struct {
	backend StoreBackend
}

func NewService(backend StoreBackend) *Service {
	return &Service{backend: backend}
}

func (s *Service) CreateUploadedFile(ctx context.Context, ws workspace.ID, staged *StagedUpload) (*FileMetadata, error) {
	return s.backend.CreateUploadedFile(ctx, ws, staged)
}

func (s *Service) ListFiles(ctx context.Context, ws workspace.ID, options ListOptions) (ListResult, error) {
	return s.backend.ListFiles(ctx, ws, options)
}

func (s *Service) GetFile(ctx context.Context, ws workspace.ID, fileID string) (*FileMetadata, error) {
	return s.backend.GetFile(ctx, ws, fileID)
}

func (s *Service) DeleteFile(ctx context.Context, ws workspace.ID, fileID string) (*DeleteResponse, error) {
	return s.backend.DeleteFile(ctx, ws, fileID)
}

func (s *Service) OpenContent(ctx context.Context, ws workspace.ID, fileID string) (*ContentStream, error) {
	return s.backend.OpenContent(ctx, ws, fileID)
}

func (s *Service) ResolveSource(ctx context.Context, ws workspace.ID, fileID string) (*Source, error) {
	return s.backend.ResolveSource(ctx, ws, fileID)
}

func (s *Service) ResolveMountSource(ctx context.Context, ws workspace.ID, sessionID string, fileID string) (*MountSource, error) {
	return s.backend.ResolveMountSource(ctx, ws, sessionID, fileID)
}

func (s *Service) ValidateSessionFileSource(ctx context.Context, ws workspace.ID, fileID string) error {
	return s.backend.ValidateSessionFileSource(ctx, ws, fileID)
}

func (s *Service) CreateSessionFileIdentity(ctx context.Context, tx SessionTransaction, ws workspace.ID, request SessionFileIdentityRequest) (*FileMetadata, error) {
	return s.backend.CreateSessionFileIdentity(ctx, tx, ws, request)
}

func (s *Service) TombstoneSessionFileIdentity(ctx context.Context, tx SessionTransaction, ws workspace.ID, sessionID string, fileID string) error {
	return s.backend.TombstoneSessionFileIdentity(ctx, tx, ws, sessionID, fileID)
}
