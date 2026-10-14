package files

import (
	"context"
	"crypto/rand"

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
	backend         StoreBackend
	pageTokenSecret []byte
}

type ServiceOption func(*Service)

func WithPageTokenSecret(secret []byte) ServiceOption {
	return func(s *Service) { s.pageTokenSecret = append([]byte(nil), secret...) }
}

func NewService(backend StoreBackend, options ...ServiceOption) *Service {
	s := &Service{backend: backend}
	for _, option := range options {
		option(s)
	}
	if len(s.pageTokenSecret) == 0 {
		s.pageTokenSecret = make([]byte, 32)
		if _, err := rand.Read(s.pageTokenSecret); err != nil {
			panic("files: page token entropy unavailable")
		}
	}
	return s
}

func (s *Service) CreateUploadedFile(ctx context.Context, ws workspace.ID, staged *StagedUpload) (*FileMetadata, error) {
	return s.backend.CreateUploadedFile(ctx, ws, staged)
}

func (s *Service) ListFiles(ctx context.Context, ws workspace.ID, options ListOptions) (ListResult, error) {
	if options.Page != "" {
		sequence, err := s.decodePageToken(options.Page, ws, options.ScopeID)
		if err != nil {
			return ListResult{}, err
		}
		options.cursorSequence = sequence
	}
	result, err := s.backend.ListFiles(ctx, ws, options)
	if err != nil {
		return ListResult{}, err
	}
	result.NextPage = nil
	if result.HasMore && len(result.Data) > 0 {
		token, err := s.encodePageToken(ws, options.ScopeID, result.Data[len(result.Data)-1].storageSequence)
		if err != nil {
			return ListResult{}, err
		}
		result.NextPage = &token
	}
	return result, nil
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
