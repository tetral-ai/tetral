package skill

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const maxDisplayTitleRunes = 1024

// StagedUploadPart is the service input representation of one staged
// multipart files[] part used during package validation and normalized
// archive production.
type StagedUploadPart struct {
	Filename  string
	SizeBytes int64
	SHA256    string

	TempPath string
}

// CreateSkillInput carries service-layer inputs for creating a parent
// Skill plus its initial immutable version.
type CreateSkillInput struct {
	DisplayTitle *string
	Files        []StagedUploadPart
	Package      *StagedPackage
}

// CreateVersionInput carries service-layer inputs for uploading a new
// immutable version under an existing parent Skill.
type CreateVersionInput struct {
	Files   []StagedUploadPart
	Package *StagedPackage
}

// ListSkillsOptions carries Anthropic Skills list query state.
type ListSkillsOptions struct {
	Limit          int
	Page           string
	Source         string
	cursorSequence int64
	cursorID       string
}

// ListVersionsOptions carries Anthropic SkillVersions list query state.
type ListVersionsOptions struct {
	Limit          int
	Page           string
	cursorSequence int64
	cursorVersion  string
}

// SkillBackend is the persistence boundary consumed by Service.
type SkillBackend interface {
	CreateSkill(context.Context, workspace.ID, CreateSkillInput) (*Skill, error)
	CreateVersion(context.Context, workspace.ID, string, CreateVersionInput) (*SkillVersion, error)
	GetSkill(context.Context, workspace.ID, string) (*Skill, error)
	ListSkills(context.Context, workspace.ID, ListSkillsOptions) (SkillListResult, error)
	DeleteSkill(context.Context, workspace.ID, string) error
	GetVersion(context.Context, workspace.ID, string, string) (*SkillVersion, error)
	OpenVersionContent(context.Context, workspace.ID, string, string) (io.ReadCloser, error)
	ListVersions(context.Context, workspace.ID, string, ListVersionsOptions) (SkillVersionListResult, error)
	DeleteVersion(context.Context, workspace.ID, string, string) error
	ValidateAgentSkillReferences(context.Context, agent.Transaction, string, []agent.SkillReference) error
}

// Service owns Skills control-plane behavior above storage and below HTTP:
// validation, package normalization, persistence orchestration, and page
// tokens.
type Service struct {
	backend         SkillBackend
	packageStageDir string
	pageTokenSecret []byte
}

type ServiceOption func(*Service)

func WithPackageStageDir(stageDir string) ServiceOption {
	return func(s *Service) {
		s.packageStageDir = stageDir
	}
}

func WithPageTokenSecret(secret []byte) ServiceOption {
	return func(s *Service) {
		s.pageTokenSecret = append([]byte(nil), secret...)
	}
}

// NewService constructs a Skills service over a persistence backend.
func NewService(backend SkillBackend, options ...ServiceOption) *Service {
	service := &Service{backend: backend}
	for _, option := range options {
		option(service)
	}
	if len(service.pageTokenSecret) == 0 {
		service.pageTokenSecret = make([]byte, 32)
		if _, err := rand.Read(service.pageTokenSecret); err != nil {
			panic("skill: page token entropy unavailable")
		}
	}
	return service
}

// CreateSkill validates create-only parent inputs, normalizes the
// uploaded files[] package, and delegates only the normalized package
// to the backend.
func (s *Service) CreateSkill(ctx context.Context, ws workspace.ID, input CreateSkillInput) (*Skill, error) {
	if err := validateDisplayTitle(input.DisplayTitle); err != nil {
		return nil, err
	}
	pkg, err := BuildNormalizedPackage(ctx, input.Files, s.packageStageDir)
	if err != nil {
		return nil, safePublicError(err)
	}
	defer func() { _ = pkg.Cleanup() }()
	input.Files = nil
	input.Package = pkg
	result, err := s.backend.CreateSkill(ctx, ws, input)
	return result, safePublicError(err)
}

func (s *Service) CreateVersion(ctx context.Context, ws workspace.ID, skillID string, input CreateVersionInput) (*SkillVersion, error) {
	pkg, err := BuildNormalizedPackage(ctx, input.Files, s.packageStageDir)
	if err != nil {
		return nil, safePublicError(err)
	}
	defer func() { _ = pkg.Cleanup() }()
	input.Files = nil
	input.Package = pkg
	result, err := s.backend.CreateVersion(ctx, ws, skillID, input)
	return result, safePublicError(err)
}

func (s *Service) GetSkill(ctx context.Context, ws workspace.ID, skillID string) (*Skill, error) {
	result, err := s.backend.GetSkill(ctx, ws, skillID)
	return result, safePublicError(err)
}

func (s *Service) ListSkills(ctx context.Context, ws workspace.ID, options ListSkillsOptions) (SkillListResult, error) {
	if options.Source == "" {
		options.Source = "custom"
	}
	if options.Page != "" {
		cursor, err := s.decodeSkillPageToken(options.Page, ws, options.Source)
		if err != nil {
			return SkillListResult{}, err
		}
		options.cursorSequence = cursor.Sequence
		options.cursorID = cursor.SkillID
	}
	if options.Source != "custom" {
		return SkillListResult{}, &ValidationError{Message: "source must be custom"}
	}
	result, err := s.backend.ListSkills(ctx, ws, options)
	if err != nil {
		return result, safePublicError(err)
	}
	if result.HasMore && len(result.Data) > 0 {
		last := result.Data[len(result.Data)-1]
		token, err := s.encodeSkillPageToken(ws, options.Source, last.storageSequence, last.ID)
		if err != nil {
			return SkillListResult{}, err
		}
		result.NextPage = &token
	} else {
		result.NextPage = nil
	}
	return result, nil
}

func (s *Service) DeleteSkill(ctx context.Context, ws workspace.ID, skillID string) error {
	return safePublicError(s.backend.DeleteSkill(ctx, ws, skillID))
}

func (s *Service) GetVersion(ctx context.Context, ws workspace.ID, skillID string, version string) (*SkillVersion, error) {
	result, err := s.backend.GetVersion(ctx, ws, skillID, version)
	return result, safePublicError(err)
}

func (s *Service) OpenVersionContent(ctx context.Context, ws workspace.ID, skillID string, version string) (io.ReadCloser, error) {
	content, err := s.backend.OpenVersionContent(ctx, ws, skillID, version)
	return content, safePublicError(err)
}

func (s *Service) ListVersions(ctx context.Context, ws workspace.ID, skillID string, options ListVersionsOptions) (SkillVersionListResult, error) {
	if options.Page != "" {
		cursor, err := s.decodeVersionPageToken(options.Page, ws, skillID)
		if err != nil {
			return SkillVersionListResult{}, err
		}
		options.cursorSequence = cursor.Sequence
		options.cursorVersion = cursor.VersionValue
	}
	result, err := s.backend.ListVersions(ctx, ws, skillID, options)
	if err != nil {
		return result, safePublicError(err)
	}
	if result.HasMore && len(result.Data) > 0 {
		last := result.Data[len(result.Data)-1]
		token, err := s.encodeVersionPageToken(ws, skillID, last.storageSequence, last.Version)
		if err != nil {
			return SkillVersionListResult{}, err
		}
		result.NextPage = &token
	} else {
		result.NextPage = nil
	}
	return result, nil
}

func (s *Service) DeleteVersion(ctx context.Context, ws workspace.ID, skillID string, version string) error {
	return safePublicError(s.backend.DeleteVersion(ctx, ws, skillID, version))
}

func (s *Service) ValidateAgentSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) error {
	return safePublicError(s.backend.ValidateAgentSkillReferences(ctx, tx, workspaceID, refs))
}

func validateDisplayTitle(title *string) error {
	if title == nil {
		return nil
	}
	if !utf8.ValidString(*title) {
		return &ValidationError{Message: "display_title must be valid UTF-8"}
	}
	if utf8.RuneCountInString(*title) > maxDisplayTitleRunes {
		return &ValidationError{Message: "display_title must be at most 1024 characters"}
	}
	for _, r := range *title {
		if r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		if unicode.IsControl(r) {
			return &ValidationError{Message: "display_title must not contain control characters"}
		}
	}
	return nil
}

func safePublicError(err error) error {
	if err == nil {
		return nil
	}
	var validation *ValidationError
	var tooLarge *RequestTooLargeError
	var notFound *NotFoundError
	var conflict *ConflictError
	var quota *QuotaError
	var internal *InternalError
	var agentValidation *agent.ValidationError
	switch {
	case errors.As(err, &validation),
		errors.As(err, &tooLarge),
		errors.As(err, &notFound),
		errors.As(err, &conflict),
		errors.As(err, &quota),
		errors.As(err, &internal),
		errors.As(err, &agentValidation):
		return err
	default:
		return &InternalError{Message: "skill operation failed", Cause: err}
	}
}
