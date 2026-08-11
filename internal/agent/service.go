package agent

import (
	"context"
	"crypto/rand"
	"database/sql"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Store is the persistence surface the Agent service needs. It is
// intentionally transaction-scoped so Service owns validation,
// lifecycle, no-op, and Skill-reference ordering while the concrete
// PostgreSQL store owns only SQL primitives.
type Store interface {
	withWorkspaceTx(ctx context.Context, ws workspace.ID, fn func(Transaction) error) error
	insertAgentSnapshot(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, cfg AgentConfig, canonicalBytes []byte, configHash string, createdAt time.Time) (string, error)
	loadCurrentAgent(ctx context.Context, tx Transaction, ws workspace.ID, agentID string) (*storedAgent, error)
	loadCurrentAgentForUpdate(ctx context.Context, tx Transaction, ws workspace.ID, agentID string) (*storedAgent, error)
	updateAgentSnapshot(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, expectedVersion int, cfg AgentConfig, canonicalBytes []byte, configHash string, updatedAt time.Time) (string, error)
	loadAgentVersion(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, version int) (*Agent, error)
	archiveAgent(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, archivedAt time.Time) error
	listCurrentAgents(ctx context.Context, tx Transaction, ws workspace.ID, options ListOptions) ([]*Agent, bool, error)
	listAgentVersions(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, options ListVersionsOptions) ([]*Agent, bool, error)
}

type Transaction interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRows(ctx context.Context, query string, args ...any) (interface {
		Next() bool
		Scan(dest ...any) error
		Err() error
		Close() error
	}, error)
	QueryRowScanner(ctx context.Context, query string, args ...any) interface {
		Scan(dest ...any) error
	}
}

// SkillReferenceResolver keeps custom Skill validation inside the Agent write
// transaction while the Skill service owns reference lookup semantics.
type SkillReferenceResolver interface {
	ValidateAgentSkillReferences(ctx context.Context, tx Transaction, workspaceID string, refs []SkillReference) error
}

// Service owns Agent business behavior and coordinates Store
// persistence with transaction-aware Skill reference validation.
type Service struct {
	store           Store
	resolver        SkillReferenceResolver
	pageTokenSecret []byte
}

type ServiceOption func(*Service)

func WithPageTokenSecret(secret []byte) ServiceOption {
	return func(s *Service) {
		s.pageTokenSecret = append([]byte(nil), secret...)
	}
}

// NewService constructs an Agent service. A nil resolver is fail-closed
// for non-empty skills[] while still accepting skills-free Agents.
func NewService(store Store, resolver SkillReferenceResolver, options ...ServiceOption) *Service {
	if resolver == nil {
		resolver = unavailableSkillReferenceResolver{}
	}
	service := &Service{store: store, resolver: resolver}
	for _, option := range options {
		option(service)
	}
	if len(service.pageTokenSecret) == 0 {
		service.pageTokenSecret = make([]byte, 32)
		if _, err := rand.Read(service.pageTokenSecret); err != nil {
			panic("agent: page token entropy unavailable")
		}
	}
	return service
}

func (s *Service) Create(ctx context.Context, ws workspace.ID, request CreateAgentRequest) (*Agent, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	cfg := request.Normalize()
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	if err := ValidateGenericLimits(cfg); err != nil {
		return nil, err
	}
	cfg, err := ValidateAndCanonicalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	canonicalBytes, configHash, err := Canonicalize(cfg)
	if err != nil {
		return nil, err
	}
	skillRefs, err := decodeSkillReferences(cfg.Skills)
	if err != nil {
		return nil, err
	}

	now := storage.Now()
	agentID := id.New("agent_")
	var versionID string
	if err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		if err := s.resolver.ValidateAgentSkillReferences(ctx, tx, string(ws), skillRefs); err != nil {
			return err
		}
		var err error
		versionID, err = s.store.insertAgentSnapshot(ctx, tx, ws, agentID, cfg, canonicalBytes, configHash, now)
		return err
	}); err != nil {
		return nil, err
	}
	return &Agent{
		ID:          agentID,
		Type:        "agent",
		VersionID:   versionID,
		Version:     1,
		AgentConfig: cfg,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (s *Service) Get(ctx context.Context, ws workspace.ID, agentID string) (*Agent, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Agent
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		row, err := s.store.loadCurrentAgent(ctx, tx, ws, agentID)
		if err != nil {
			return err
		}
		result = row.Agent
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) List(ctx context.Context, ws workspace.ID, options ListOptions) (ListResult, error) {
	if ws == "" {
		return ListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	if options.Limit <= 0 {
		options.Limit = 20
	}
	if options.Limit > 100 {
		options.Limit = 100
	}
	if options.Page != "" {
		cursor, err := s.decodeAgentListPageToken(options.Page, ws, options)
		if err != nil {
			return ListResult{}, err
		}
		createdAt, err := time.Parse(agentCreatedAtTokenFormat, cursor.CreatedAt)
		if err != nil {
			return ListResult{}, &ValidationError{Message: "invalid page token"}
		}
		options.cursorCreatedAt = createdAt.UTC()
		options.cursorID = cursor.AgentID
	}
	var (
		results []*Agent
		hasMore bool
	)
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		results, hasMore, err = s.store.listCurrentAgents(ctx, tx, ws, options)
		return err
	})
	if err != nil {
		return ListResult{}, err
	}
	out := ListResult{Data: results}
	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		token, err := s.encodeAgentListPageToken(ws, options, last.CreatedAt, last.ID)
		if err != nil {
			return ListResult{}, err
		}
		out.NextPage = &token
	}
	return out, nil
}

func (s *Service) Update(ctx context.Context, ws workspace.ID, agentID string, request UpdateAgentRequest) (*Agent, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Agent
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		current, err := s.store.loadCurrentAgentForUpdate(ctx, tx, ws, agentID)
		if err != nil {
			return err
		}
		result, err = s.updateCurrentLocked(ctx, tx, ws, current, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) UpdatePatch(ctx context.Context, ws workspace.ID, agentID string, patch AgentPatch) (*Agent, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Agent
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		current, err := s.store.loadCurrentAgentForUpdate(ctx, tx, ws, agentID)
		if err != nil {
			return err
		}
		if current.Agent.Version != patch.Version {
			return &ConflictError{Message: "agent version conflict"}
		}
		materialized, err := patch.Materialize(current.Agent.AgentConfig)
		if err != nil {
			return err
		}
		result, err = s.updateCurrentLocked(ctx, tx, ws, current, UpdateAgentRequest{
			Version:     patch.Version,
			AgentConfig: materialized,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) updateCurrentLocked(ctx context.Context, tx Transaction, ws workspace.ID, current *storedAgent, request UpdateAgentRequest) (*Agent, error) {
	if current.Agent.ArchivedAt != nil {
		return nil, &ValidationError{Message: "archived agents cannot be updated"}
	}
	if current.Agent.Version != request.Version {
		return nil, &ConflictError{Message: "agent version conflict"}
	}
	cfg := request.Normalize()
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	if err := ValidateGenericLimits(cfg); err != nil {
		return nil, err
	}
	cfg, err := ValidateAndCanonicalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	targetBytes, targetHash, err := Canonicalize(cfg)
	if err != nil {
		return nil, err
	}
	skillRefs, err := decodeSkillReferences(cfg.Skills)
	if err != nil {
		return nil, err
	}
	if err := s.resolver.ValidateAgentSkillReferences(ctx, tx, string(ws), skillRefs); err != nil {
		return nil, err
	}

	storedHash := current.ConfigHash
	if storedHash == "" {
		storedHash = CanonicalHashOfBytes([]byte(current.ConfigJSON))
	}
	if storedHash == targetHash && current.ConfigJSON == string(targetBytes) {
		return current.Agent, nil
	}

	updatedAt := storage.Now()
	versionID, err := s.store.updateAgentSnapshot(ctx, tx, ws, current.Agent.ID, current.Agent.Version, cfg, targetBytes, targetHash, updatedAt)
	if err != nil {
		return nil, err
	}
	return &Agent{
		ID:          current.Agent.ID,
		Type:        "agent",
		VersionID:   versionID,
		Version:     current.Agent.Version + 1,
		AgentConfig: cfg,
		CreatedAt:   current.Agent.CreatedAt,
		UpdatedAt:   updatedAt,
	}, nil
}

type unavailableSkillReferenceResolver struct{}

func (unavailableSkillReferenceResolver) ValidateAgentSkillReferences(_ context.Context, _ Transaction, _ string, refs []SkillReference) error {
	if len(refs) > 0 {
		return &ValidationError{Message: "skills[] is not yet supported on this Engine instance"}
	}
	return nil
}

func (s *Service) GetVersion(ctx context.Context, ws workspace.ID, agentID string, version int) (*Agent, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if version <= 0 {
		return nil, &ValidationError{Message: "version must be positive"}
	}
	var result *Agent
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		result, err = s.store.loadAgentVersion(ctx, tx, ws, agentID, version)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) ListVersions(ctx context.Context, ws workspace.ID, agentID string, options ListVersionsOptions) (ListResult, error) {
	if ws == "" {
		return ListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	if options.Limit <= 0 {
		options.Limit = 20
	}
	if options.Limit > 100 {
		options.Limit = 100
	}
	if options.Page != "" {
		cursor, err := s.decodeAgentVersionsPageToken(options.Page, ws, agentID)
		if err != nil {
			return ListResult{}, err
		}
		options.cursorVersion = cursor.VersionValue
	}
	var (
		results []*Agent
		hasMore bool
	)
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		results, hasMore, err = s.store.listAgentVersions(ctx, tx, ws, agentID, options)
		return err
	})
	if err != nil {
		return ListResult{}, err
	}
	out := ListResult{Data: results}
	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		token, err := s.encodeAgentVersionsPageToken(ws, agentID, last.Version)
		if err != nil {
			return ListResult{}, err
		}
		out.NextPage = &token
	}
	return out, nil
}

func (s *Service) Archive(ctx context.Context, ws workspace.ID, agentID string) (*Agent, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Agent
	err := s.store.withWorkspaceTx(ctx, ws, func(tx Transaction) error {
		current, err := s.store.loadCurrentAgentForUpdate(ctx, tx, ws, agentID)
		if err != nil {
			return err
		}
		if current.Agent.ArchivedAt != nil {
			result = current.Agent
			return nil
		}
		archivedAt := storage.Now()
		if err := s.store.archiveAgent(ctx, tx, ws, agentID, archivedAt); err != nil {
			return err
		}
		archived := *current.Agent
		archived.ArchivedAt = &archivedAt
		result = &archived
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
