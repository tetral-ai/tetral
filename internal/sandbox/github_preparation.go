package sandbox

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/url"
	"path"
	"strings"
	"time"
	"unicode"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// A GitHub re-clone failure is classified at the driver
// (internal/sandbox/driver/github_repository.go CloneGitHubRepositories) into
// three arms by the manifestation of the clone command output:
//
//	arm                          reason (internal)               retry            capture
//	---------------------------  ------------------------------  ---------------  ----------------------
//	credential manifestation     github_credential_required      none, terminal   failing repo resource_id + url
//	not-found manifestation      github_repository_unavailable   none, terminal   failing repo resource_id + url
//	every other failure          (error returned as-is)          retryable, then  captured on terminal settle
//	                                                             settled terminal
//
// The two named reasons are terminal preparation outcomes: retryableSessionPreparationError
// (session_prepare.go) returns false for them and sessionPreparationFailureForError
// records the reason plus the failing repository's resource_id/url onto the
// preparation row. This internal reason is NEVER what the caller sees: the driving
// input settles through the terminal-preparation path carrying only upstream
// vocabulary (unknown_error, a human-readable message, retry_status=exhausted).
// GitHubPreparationFailure is the carrier; IsGitHubCredentialRequired /
// IsGitHubRepositoryUnavailable are the classifiers the settle reads.
//
// UPDATE-WITH: internal/sandbox/driver/github_repository.go (gitHubCredentialFailure,
// gitHubRepositoryUnavailable); internal/sandbox/session_prepare.go
// (sessionPreparationFailureForError, retryableSessionPreparationError).
const (
	GitHubCredentialRequiredReason    = "github_credential_required" //nolint:gosec // Contract failure reason string, not credential material.
	GitHubRepositoryUnavailableReason = "github_repository_unavailable"
)

type GitTicketRotator interface {
	CreatePending(ctx context.Context, ws workspace.ID, sessionID string, ticketID string, tokenHash []byte, now time.Time) (*gitticket.Ticket, error)
	ActivatePending(ctx context.Context, ws workspace.ID, sessionID string, ticketID string, now time.Time) (*gitticket.Ticket, error)
	FindBySessionTokenHash(ctx context.Context, ws workspace.ID, sessionID string, tokenHash []byte) (*gitticket.Ticket, error)
}

type GitHubRepositoryMaterializer interface {
	InstalledGitTicketHash(context.Context, string, string) ([]byte, bool, error)
	InstallGitHubRepositoryConfiguration(context.Context, GitHubRepositoryConfiguration) error
	CloneGitHubRepositories(context.Context, GitHubRepositoryPreparation) error
	RemoveGitHubRepository(context.Context, string, GitHubRepositoryMount) error
}

type GitHubRepositoryConfiguration struct {
	WorkspaceID       workspace.ID
	SessionID         string
	ProviderSandboxID string
	GitProxyHost      string
	Ticket            string
}

type GitHubRepositoryPreparation struct {
	WorkspaceID       workspace.ID
	SessionID         string
	ProviderSandboxID string
	Repositories      []GitHubRepositoryMount
}

type GitHubPreparationFailure struct {
	Reason      string
	ResourceID  string
	ResourceURL string
	Cause       error
}

func (e *GitHubPreparationFailure) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Reason != "" {
		return e.Reason
	}
	return "github_repository preparation failed"
}

func (e *GitHubPreparationFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func IsGitHubCredentialRequired(err error) bool {
	var failure *GitHubPreparationFailure
	return errors.As(err, &failure) && failure.Reason == GitHubCredentialRequiredReason
}

func IsGitHubRepositoryUnavailable(err error) bool {
	var failure *GitHubPreparationFailure
	return errors.As(err, &failure) && failure.Reason == GitHubRepositoryUnavailableReason
}

func gitHubPreparationFailureIdentity(err error) (string, string) {
	var failure *GitHubPreparationFailure
	if !errors.As(err, &failure) {
		return "", ""
	}
	return failure.ResourceID, failure.ResourceURL
}

func (s *Service) prepareGitHubRepositories(ctx context.Context, ws workspace.ID, sessionID string, handle ProviderHandle, resources ResourceSetup) error {
	if len(resources.GitHubRepositories) == 0 {
		return nil
	}
	if s.gitHubMaterializer == nil {
		return &ValidationError{Message: "github_repository materializer is required"}
	}
	if handle.SandboxID == "" {
		return &ValidationError{Message: "provider sandbox id is required"}
	}
	if s.gitTicketRotator == nil {
		return &ValidationError{Message: "git ticket rotator is required"}
	}
	gitProxyHost := strings.TrimSpace(s.gitProxyHost)
	if gitProxyHost == "" {
		return &ValidationError{Message: "git proxy host is required"}
	}
	repositories, err := normalizeGitHubRepositoryMounts(resources.GitHubRepositories)
	if err != nil {
		return err
	}
	if recovered, err := s.recoverInstalledGitTicket(ctx, ws, sessionID, handle.SandboxID, gitProxyHost, resources.PreparationAttemptCreatedAt, repositories); err != nil {
		return err
	} else if recovered {
		return nil
	}
	random := s.gitTicketRandom
	if random == nil {
		random = rand.Reader
	}
	token, err := gitticket.GenerateToken(random)
	if err != nil {
		return err
	}
	hash, err := gitticket.HashToken(token)
	if err != nil {
		return err
	}
	ticketID := id.New("gitt_")
	if _, err := s.gitTicketRotator.CreatePending(ctx, ws, sessionID, ticketID, hash, s.clock().UTC()); err != nil {
		return err
	}
	if err := s.gitHubMaterializer.InstallGitHubRepositoryConfiguration(ctx, GitHubRepositoryConfiguration{
		WorkspaceID:       ws,
		SessionID:         sessionID,
		ProviderSandboxID: handle.SandboxID,
		GitProxyHost:      gitProxyHost,
		Ticket:            token,
	}); err != nil {
		return err
	}
	if _, err := s.gitTicketRotator.ActivatePending(ctx, ws, sessionID, ticketID, s.clock().UTC()); err != nil {
		return err
	}
	return s.gitHubMaterializer.CloneGitHubRepositories(ctx, GitHubRepositoryPreparation{
		WorkspaceID: ws, SessionID: sessionID, ProviderSandboxID: handle.SandboxID, Repositories: repositories,
	})
}

func (s *Service) recoverInstalledGitTicket(ctx context.Context, ws workspace.ID, sessionID, providerSandboxID, gitProxyHost string, attemptCreatedAt time.Time, repositories []GitHubRepositoryMount) (bool, error) {
	if attemptCreatedAt.IsZero() {
		return false, nil
	}
	hash, installed, err := s.gitHubMaterializer.InstalledGitTicketHash(ctx, providerSandboxID, gitProxyHost)
	if err != nil || !installed {
		return false, err
	}
	ticket, err := s.gitTicketRotator.FindBySessionTokenHash(ctx, ws, sessionID, hash)
	if err != nil {
		var notFound *gitticket.NotFoundError
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	if ticket.CreatedAt.Before(attemptCreatedAt) {
		return false, nil
	}
	switch ticket.Status {
	case gitticket.StatusPending:
		if _, err := s.gitTicketRotator.ActivatePending(ctx, ws, sessionID, ticket.TicketID, s.clock().UTC()); err != nil {
			return false, err
		}
	case gitticket.StatusLive:
	default:
		return false, nil
	}
	if err := s.gitHubMaterializer.CloneGitHubRepositories(ctx, GitHubRepositoryPreparation{
		WorkspaceID: ws, SessionID: sessionID, ProviderSandboxID: providerSandboxID, Repositories: repositories,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) removeDeletedGitHubRepositories(ctx context.Context, handle ProviderHandle, repositories []GitHubRepositoryMount, cleanup SessionResourceCleanupCoordinator) error {
	if len(repositories) == 0 {
		return nil
	}
	if s.gitHubMaterializer == nil {
		return &ValidationError{Message: "github_repository materializer is required"}
	}
	if handle.SandboxID == "" {
		return &ValidationError{Message: "provider sandbox id is required"}
	}
	normalized := make([]GitHubRepositoryMount, 0, len(repositories))
	for _, repository := range repositories {
		mountPath, err := resolvedGitHubRepositoryMountPath(repository)
		if err != nil {
			return err
		}
		if err := rejectReservedGitHubRepositoryMountPath(mountPath); err != nil {
			return err
		}
		repository.MountPath = mountPath
		normalized = append(normalized, repository)
	}
	for _, repository := range normalized {
		remove := func(ctx context.Context) error {
			return s.gitHubMaterializer.RemoveGitHubRepository(ctx, handle.SandboxID, repository)
		}
		if cleanup == nil {
			if err := remove(ctx); err != nil {
				return err
			}
			continue
		}
		if err := cleanup.CleanupSessionResource(ctx, repository.ResourceID, remove); err != nil {
			return err
		}
	}
	return nil
}

func validateSessionResourceMountPaths(resources ResourceSetup) error {
	repositories, err := normalizeGitHubRepositoryMounts(resources.GitHubRepositories)
	if err != nil {
		return err
	}
	filePaths := make([]string, 0, len(resources.Files))
	for _, file := range resources.Files {
		mountPath := file.MountPath
		if mountPath == "" {
			if file.SessionFileID == "" {
				return &ValidationError{Message: "file resource mount_path is unresolved"}
			}
			mountPath = "/mnt/session/uploads/" + file.SessionFileID
		}
		if err := validateActiveFileMountPath(mountPath); err != nil {
			return err
		}
		for _, existing := range filePaths {
			if pathContains(existing, mountPath) || pathContains(mountPath, existing) {
				return &ValidationError{Message: "file resource mount_path is duplicated or nested"}
			}
		}
		for _, repository := range repositories {
			if pathContains(repository.MountPath, mountPath) || pathContains(mountPath, repository.MountPath) {
				return &ValidationError{Message: "file resource mount_path conflicts with github_repository mount_path"}
			}
		}
		filePaths = append(filePaths, mountPath)
	}
	memoryPaths := make([]string, 0, len(resources.MemoryStores))
	for _, memory := range resources.MemoryStores {
		if err := validateActiveMemoryMountPath(memory.MountPath); err != nil {
			return err
		}
		for _, existing := range memoryPaths {
			if pathContains(existing, memory.MountPath) || pathContains(memory.MountPath, existing) {
				return &ValidationError{Message: "memory_store mount_path is duplicated or nested"}
			}
		}
		memoryPaths = append(memoryPaths, memory.MountPath)
	}
	skillPaths := make(map[string]struct{}, len(resources.Skills))
	for _, skill := range resources.Skills {
		if skill.Directory == "" || skill.Directory == "." || skill.Directory == ".." ||
			path.Clean(skill.Directory) != skill.Directory || strings.Contains(skill.Directory, "/") ||
			strings.Contains(skill.Directory, "\x00") {
			return &ValidationError{Message: "skill directory must be one clean relative path segment"}
		}
		mountPath := "/skills/" + skill.Directory
		if _, exists := skillPaths[mountPath]; exists {
			return &ValidationError{Message: "multiple skill bundles claim the same /skills directory"}
		}
		skillPaths[mountPath] = struct{}{}
	}
	return nil
}

func validateActiveFileMountPath(mountPath string) error {
	if !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath || mountPath == "/" || strings.Contains(mountPath, "\x00") {
		return &ValidationError{Message: "file resource mount_path must be absolute and lexically clean"}
	}
	for _, reserved := range reservedGitHubRepositoryMountSubtrees() {
		if pathContains(reserved, mountPath) || pathContains(mountPath, reserved) {
			return &ValidationError{Message: "file resource mount_path overlaps a reserved path"}
		}
	}
	if mountPath == "/workspace" || mountPath == "/mnt/session/uploads" {
		return &ValidationError{Message: "file resource mount_path overlaps a reserved path"}
	}
	return nil
}

func validateActiveMemoryMountPath(mountPath string) error {
	if !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath || strings.Contains(mountPath, "\x00") ||
		mountPath == "/mnt/memory" || !pathContains("/mnt/memory", mountPath) {
		return &ValidationError{Message: "memory_store mount_path must name a store below /mnt/memory"}
	}
	if pathContains("/mnt/memory/.staging", mountPath) {
		return &ValidationError{Message: "memory_store mount_path overlaps memory projection staging"}
	}
	return nil
}

func normalizeGitHubRepositoryMounts(repositories []GitHubRepositoryMount) ([]GitHubRepositoryMount, error) {
	paths := make([]struct {
		resourceID string
		mountPath  string
	}, 0, len(repositories))
	normalized := make([]GitHubRepositoryMount, 0, len(repositories))
	for _, repo := range repositories {
		mountPath, err := resolvedGitHubRepositoryMountPath(repo)
		if err != nil {
			return nil, err
		}
		if err := rejectReservedGitHubRepositoryMountPath(mountPath); err != nil {
			return nil, err
		}
		for _, existing := range paths {
			if mountPath == existing.mountPath {
				return nil, &ValidationError{Message: "github_repository mount_path is duplicated"}
			}
			if pathContains(existing.mountPath, mountPath) || pathContains(mountPath, existing.mountPath) {
				return nil, &ValidationError{Message: "github_repository mount_path is nested"}
			}
		}
		repo.MountPath = mountPath
		normalized = append(normalized, repo)
		paths = append(paths, struct {
			resourceID string
			mountPath  string
		}{resourceID: repo.ResourceID, mountPath: mountPath})
	}
	return normalized, nil
}

func resolvedGitHubRepositoryMountPath(repo GitHubRepositoryMount) (string, error) {
	repoName, err := validatedGitHubRepositoryName(repo.URL)
	if err != nil {
		return "", err
	}
	if repo.MountPath != "" {
		clean := path.Clean(repo.MountPath)
		if !path.IsAbs(repo.MountPath) || hasUnsafeGitHubMountPathCharacter(repo.MountPath) {
			return "", &ValidationError{Message: "github_repository mount_path must be absolute and valid"}
		}
		if clean != repo.MountPath || clean == "/" {
			return "", &ValidationError{Message: "github_repository mount_path must be lexically clean"}
		}
		if !pathContains("/workspace", clean) {
			return "", &ValidationError{Message: "github_repository mount_path must be under /workspace"}
		}
		return clean, nil
	}
	return "/workspace/" + repoName, nil
}

func rejectReservedGitHubRepositoryMountPath(mountPath string) error {
	for _, reserved := range reservedGitHubRepositoryMountSubtrees() {
		if mountPath == reserved || pathContains(reserved, mountPath) || pathContains(mountPath, reserved) {
			return &ValidationError{Message: "github_repository mount_path overlaps a reserved path"}
		}
	}
	if mountPath == "/workspace" {
		return &ValidationError{Message: "github_repository mount_path overlaps a reserved path"}
	}
	return nil
}

func reservedGitHubRepositoryMountSubtrees() []string {
	return []string{
		"/mnt/tetral/r2",
		"/tmp/tetral-runtime",
		"/dev/shm/tetral-runtime",
		"/tmp/tetral/session-prepare",
		"/mnt/memory",
		"/skills",
		"/mnt/session/outputs",
	}
}

func hasUnsafeGitHubMountPathCharacter(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return true
		}
	}
	return false
}

func validatedGitHubRepositoryName(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" {
		return "", &ValidationError{Message: "github_repository url is invalid"}
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", &ValidationError{Message: "github_repository url is invalid"}
	}
	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", &ValidationError{Message: "github_repository url is invalid"}
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", &ValidationError{Message: "github_repository url is invalid"}
	}
	name, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", &ValidationError{Message: "github_repository url is invalid"}
	}
	name = strings.TrimSuffix(name, ".git")
	if !safeGitHubRepositoryComponent(owner) || !safeGitHubRepositoryComponent(name) {
		return "", &ValidationError{Message: "github_repository url is invalid"}
	}
	return name, nil
}

func safeGitHubRepositoryComponent(value string) bool {
	if value == "" || value == "." || value == ".." || strings.Contains(value, "/") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) || strings.ContainsRune("?#&=:@\\", r) {
			return false
		}
	}
	return true
}

func pathContains(parent string, child string) bool {
	if parent == child {
		return true
	}
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return strings.HasPrefix(child, parent+"/")
}

func WithGitHubRepositoryPreparation(rotator GitTicketRotator, materializer GitHubRepositoryMaterializer, gitProxyHost string) Option {
	return func(s *Service) {
		s.gitTicketRotator = rotator
		s.gitHubMaterializer = materializer
		s.gitProxyHost = gitProxyHost
	}
}

func WithGitTicketRandomSource(random io.Reader) Option {
	return func(s *Service) { s.gitTicketRandom = random }
}
