package environment

import (
	"context"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type StoreBackend interface {
	Create(context.Context, workspace.ID, CreateEnvironmentRequest) (*Environment, error)
	Get(context.Context, workspace.ID, string) (*Environment, error)
	List(context.Context, workspace.ID, ListOptions) (ListResult, error)
	Update(context.Context, workspace.ID, string, EnvironmentPatch) (*Environment, error)
	Archive(context.Context, workspace.ID, string) (*Environment, error)
	Delete(context.Context, workspace.ID, string) (*DeleteResult, error)
}

type Service struct {
	backend StoreBackend
}

func NewService(backend StoreBackend) *Service {
	return &Service{backend: backend}
}

func (s *Service) Create(ctx context.Context, ws workspace.ID, request CreateEnvironmentRequest) (*Environment, error) {
	return s.backend.Create(ctx, ws, request)
}

func (s *Service) Get(ctx context.Context, ws workspace.ID, environmentID string) (*Environment, error) {
	return s.backend.Get(ctx, ws, environmentID)
}

func (s *Service) List(ctx context.Context, ws workspace.ID, options ListOptions) (ListResult, error) {
	return s.backend.List(ctx, ws, options)
}

func (s *Service) Update(ctx context.Context, ws workspace.ID, environmentID string, patch EnvironmentPatch) (*Environment, error) {
	return s.backend.Update(ctx, ws, environmentID, patch)
}

func (s *Service) Archive(ctx context.Context, ws workspace.ID, environmentID string) (*Environment, error) {
	return s.backend.Archive(ctx, ws, environmentID)
}

func (s *Service) Delete(ctx context.Context, ws workspace.ID, environmentID string) (*DeleteResult, error) {
	return s.backend.Delete(ctx, ws, environmentID)
}
