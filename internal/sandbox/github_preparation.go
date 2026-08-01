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

// GitHub checkout failures are classified by the provider driver from the
// clone command output:
//
//	arm                          reason (internal)               retry            capture
//	---------------------------  ------------------------------  ---------------  ----------------------
//	credential manifestation     github_credential_required      none, terminal   failing repo resource_id + url
//	not-found manifestation      github_repository_unavailable   none, terminal   failing repo resource_id + url
//	every other failure          (error returned as-is)          retryable, then  captured on terminal settle
//	                                                             settled terminal
//
// The named reasons are non-retryable materialization outcomes. The internal
// reason and repository identity remain diagnostics; the tool-facing result is
// normalized by Sandbox Service. GitHubMaterializationFailure carries that
// classification across the provider boundary.
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

// GitHubRepositoryConverger owns the disposable sandbox checkout and its
// bounded Git proxy credential. PostgreSQL tickets remain durable authority;
// provider files and configuration are rebuilt from the requested snapshot.
type GitHubRepositoryConverger struct {
	Rotator      GitTicketRotator
	Materializer GitHubRepositoryMaterializer
	GitProxyHost string
	Random       io.Reader
	Clock        func() time.Time
}

func (c *GitHubRepositoryConverger) MaterializeGitHubRepositories(ctx context.Context, setup SandboxSetup, handle ProviderHandle) error {
	return c.materialize(ctx, setup.WorkspaceID, setup.SessionID, handle, setup.Resources.GitHubRepositories)
}

func (c *GitHubRepositoryConverger) RemoveDeletedGitHubRepositories(ctx context.Context, setup SandboxSetup, handle ProviderHandle) error {
	return c.removeDeleted(ctx, handle, setup.Resources.DeletedGitHubRepositories)
}

type GitHubMaterializationFailure struct {
	Reason      string
	ResourceID  string
	ResourceURL string
	Cause       error
}

func (e *GitHubMaterializationFailure) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Reason != "" {
		return e.Reason
	}
	return "github_repository preparation failed"
}

func (e *GitHubMaterializationFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func IsGitHubCredentialRequired(err error) bool {
	var failure *GitHubMaterializationFailure
	return errors.As(err, &failure) && failure.Reason == GitHubCredentialRequiredReason
}

func IsGitHubRepositoryUnavailable(err error) bool {
	var failure *GitHubMaterializationFailure
	return errors.As(err, &failure) && failure.Reason == GitHubRepositoryUnavailableReason
}

func (c *GitHubRepositoryConverger) materialize(ctx context.Context, ws workspace.ID, sessionID string, handle ProviderHandle, requested []GitHubRepositoryMount) error {
	if len(requested) == 0 {
		return nil
	}
	if c == nil || c.Materializer == nil {
		return &ValidationError{Message: "github_repository materializer is required"}
	}
	if handle.SandboxID == "" {
		return &ValidationError{Message: "provider sandbox id is required"}
	}
	if c.Rotator == nil {
		return &ValidationError{Message: "git ticket rotator is required"}
	}
	gitProxyHost := strings.TrimSpace(c.GitProxyHost)
	if gitProxyHost == "" {
		return &ValidationError{Message: "git proxy host is required"}
	}
	repositories, err := normalizeGitHubRepositoryMounts(requested)
	if err != nil {
		return err
	}
	if recovered, err := c.recoverInstalledGitTicket(ctx, ws, sessionID, handle.SandboxID, gitProxyHost, repositories); err != nil {
		return err
	} else if recovered {
		return nil
	}
	random := c.Random
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
	clock := c.Clock
	if clock == nil {
		clock = time.Now
	}
	if _, err := c.Rotator.CreatePending(ctx, ws, sessionID, ticketID, hash, clock().UTC()); err != nil {
		return err
	}
	if err := c.Materializer.InstallGitHubRepositoryConfiguration(ctx, GitHubRepositoryConfiguration{
		WorkspaceID:       ws,
		SessionID:         sessionID,
		ProviderSandboxID: handle.SandboxID,
		GitProxyHost:      gitProxyHost,
		Ticket:            token,
	}); err != nil {
		return err
	}
	if _, err := c.Rotator.ActivatePending(ctx, ws, sessionID, ticketID, clock().UTC()); err != nil {
		return err
	}
	return c.Materializer.CloneGitHubRepositories(ctx, GitHubRepositoryPreparation{
		WorkspaceID: ws, SessionID: sessionID, ProviderSandboxID: handle.SandboxID, Repositories: repositories,
	})
}

func (c *GitHubRepositoryConverger) recoverInstalledGitTicket(ctx context.Context, ws workspace.ID, sessionID, providerSandboxID, gitProxyHost string, repositories []GitHubRepositoryMount) (bool, error) {
	hash, installed, err := c.Materializer.InstalledGitTicketHash(ctx, providerSandboxID, gitProxyHost)
	if err != nil || !installed {
		return false, err
	}
	ticket, err := c.Rotator.FindBySessionTokenHash(ctx, ws, sessionID, hash)
	if err != nil {
		var notFound *gitticket.NotFoundError
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	switch ticket.Status {
	case gitticket.StatusPending:
		clock := c.Clock
		if clock == nil {
			clock = time.Now
		}
		if _, err := c.Rotator.ActivatePending(ctx, ws, sessionID, ticket.TicketID, clock().UTC()); err != nil {
			return false, err
		}
	case gitticket.StatusLive:
	default:
		return false, nil
	}
	if err := c.Materializer.CloneGitHubRepositories(ctx, GitHubRepositoryPreparation{
		WorkspaceID: ws, SessionID: sessionID, ProviderSandboxID: providerSandboxID, Repositories: repositories,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (c *GitHubRepositoryConverger) removeDeleted(ctx context.Context, handle ProviderHandle, repositories []GitHubRepositoryMount) error {
	if len(repositories) == 0 {
		return nil
	}
	if c == nil || c.Materializer == nil {
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
			return c.Materializer.RemoveGitHubRepository(ctx, handle.SandboxID, repository)
		}
		if err := remove(ctx); err != nil {
			return err
		}
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
