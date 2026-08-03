package sandbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestIsGitHubCredentialRequiredDetectsWrappedMaterializationFailure(t *testing.T) {
	err := &SandboxError{
		Code:  SandboxErrorMountFailed,
		Cause: &GitHubMaterializationFailure{Reason: GitHubCredentialRequiredReason, Cause: errors.New("no credential")},
	}
	if !IsGitHubCredentialRequired(err) {
		t.Fatalf("IsGitHubCredentialRequired(%v) = false; want true", err)
	}
	if IsGitHubCredentialRequired(errors.New("plain failure")) {
		t.Fatal("plain failure classified as github_credential_required")
	}
}

func TestGitHubRepositoryConvergerRejectsMountPathCollisionsBeforeTicketRotation(t *testing.T) {
	tests := []struct {
		name         string
		repositories []GitHubRepositoryMount
	}{
		{
			name: "duplicate explicit paths",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/a", MountPath: "/workspace/project"},
				{ResourceID: "sesrsc_repo_b", URL: "https://github.com/tetral-ai/b", MountPath: "/workspace/project"},
			},
		},
		{
			name: "nested explicit paths",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/a", MountPath: "/workspace/project"},
				{ResourceID: "sesrsc_repo_b", URL: "https://github.com/tetral-ai/b", MountPath: "/workspace/project/nested"},
			},
		},
		{
			name: "duplicate default paths",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/tetral"},
				{ResourceID: "sesrsc_repo_b", URL: "https://github.com/other/tetral.git"},
			},
		},
		{
			name: "root path",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/a", MountPath: "/"},
			},
		},
		{
			name: "reserved outputs path",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/a", MountPath: "/mnt/session/outputs/repo"},
			},
		},
		{
			name: "nul path",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/a", MountPath: "/tmp/repos/a\x00b"},
			},
		},
		{
			name: "outside workspace path",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/a", MountPath: "/tmp/repos/a"},
			},
		},
		{
			name: "invalid url with explicit path",
			repositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://example.com/tetral-ai/a", MountPath: "/workspace/a"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rotator := &recordingGitTicketRotator{}
			materializer := &recordingGitHubRepositoryMaterializer{}
			converger := &GitHubRepositoryConverger{Rotator: rotator, Materializer: materializer, GitProxyHost: "git.tetral.test"}

			err := converger.materialize(context.Background(), workspace.DefaultID, "sesn_git_preflight", ProviderHandle{SandboxID: "provider_sandbox"}, tc.repositories)
			if err == nil {
				t.Fatal("MaterializeGitHubRepositories succeeded; want mount-path validation error")
			}
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("err = %T %v; want ValidationError", err, err)
			}
			if len(rotator.pendingCalls) != 0 || len(rotator.activationCalls) != 0 || len(materializer.calls) != 0 {
				t.Fatalf("side effects pending=%d activation=%d materializer=%d; want fail-before-partial-write", len(rotator.pendingCalls), len(rotator.activationCalls), len(materializer.calls))
			}
		})
	}
}

func TestGitHubRepositoryConvergerMaterializesExplicitMountPathUnderWorkspace(t *testing.T) {
	rotator := &recordingGitTicketRotator{}
	materializer := &recordingGitHubRepositoryMaterializer{}
	converger := &GitHubRepositoryConverger{
		Rotator: rotator, Materializer: materializer, GitProxyHost: "git.tetral.test",
		Clock: func() time.Time { return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC) },
	}

	err := converger.materialize(context.Background(), workspace.DefaultID, "sesn_git_explicit", ProviderHandle{SandboxID: "provider_sandbox"}, []GitHubRepositoryMount{{
		ResourceID: "sesrsc_repo",
		URL:        "https://github.com/tetral-ai/tetral",
		MountPath:  "/workspace/repos/tetral",
	}})
	if err != nil {
		t.Fatalf("MaterializeGitHubRepositories: %v", err)
	}
	repositories := materializer.calls[0].Repositories
	if len(repositories) != 1 || repositories[0].MountPath != "/workspace/repos/tetral" {
		t.Fatalf("materialized repositories = %+v; want explicit mount path preserved", repositories)
	}
}

func TestGitHubRepositoryConvergerMaterializesDefaultMountPath(t *testing.T) {
	events := []string{}
	rotator := &recordingGitTicketRotator{}
	rotator.eventsRef = &events
	materializer := &recordingGitHubRepositoryMaterializer{eventsRef: &events}
	converger := &GitHubRepositoryConverger{
		Rotator: rotator, Materializer: materializer, GitProxyHost: "git.tetral.test",
		Clock: func() time.Time { return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC) },
	}

	err := converger.materialize(context.Background(), workspace.DefaultID, "sesn_git_default", ProviderHandle{SandboxID: "provider_sandbox"}, []GitHubRepositoryMount{{
		ResourceID: "sesrsc_repo",
		URL:        "https://github.com/tetral-ai/tetral.git",
	}})
	if err != nil {
		t.Fatalf("MaterializeGitHubRepositories: %v", err)
	}
	if len(rotator.pendingCalls) != 1 || len(rotator.activationCalls) != 1 || len(materializer.installs) != 1 || len(materializer.calls) != 1 {
		t.Fatalf("side effects pending=%d installs=%d activation=%d clones=%d", len(rotator.pendingCalls), len(materializer.installs), len(rotator.activationCalls), len(materializer.calls))
	}
	if got := strings.Join(events, " -> "); got != "pending -> config install -> activate -> clone" {
		t.Fatalf("phase order = %s; want pending -> config install -> activate -> clone", got)
	}
	repositories := materializer.calls[0].Repositories
	if len(repositories) != 1 || repositories[0].MountPath != "/workspace/tetral" {
		t.Fatalf("materialized repositories = %+v; want normalized default mount path", repositories)
	}
}

func TestGitHubRepositoryConvergerRecoversInstalledTicket(t *testing.T) {
	events := []string{}
	hash := []byte("installed-ticket-hash")
	rotator := &recordingGitTicketRotator{
		eventsRef: &events,
		findTicket: &gitticket.Ticket{
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_git_converge",
			TicketID:    "gitt_existing",
			TokenHash:   hash,
			Status:      gitticket.StatusPending,
		},
	}
	materializer := &recordingGitHubRepositoryMaterializer{eventsRef: &events, installedHash: hash}
	converger := &GitHubRepositoryConverger{
		Rotator: rotator, Materializer: materializer, GitProxyHost: "git.tetral.test",
		Clock: func() time.Time { return time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC) },
	}
	err := converger.MaterializeGitHubRepositories(context.Background(), SandboxSetup{
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_git_converge",
		Resources: ResourceSetup{GitHubRepositories: []GitHubRepositoryMount{{
			ResourceID: "sesrsc_repo", URL: "https://github.com/tetral-ai/tetral",
		}}},
	}, ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("MaterializeGitHubRepositories: %v", err)
	}
	if got := strings.Join(events, " -> "); got != "activate -> clone" {
		t.Fatalf("phase order = %s; want activate -> clone", got)
	}
	if len(rotator.pendingCalls) != 0 || len(materializer.installs) != 0 {
		t.Fatalf("recovery minted a replacement credential: pending=%d installs=%d", len(rotator.pendingCalls), len(materializer.installs))
	}
}

func TestGitHubRepositoryConvergerRemovesDeletedCheckoutWithoutRotatingTicket(t *testing.T) {
	rotator := &recordingGitTicketRotator{}
	materializer := &recordingGitHubRepositoryMaterializer{}
	converger := &GitHubRepositoryConverger{Rotator: rotator, Materializer: materializer, GitProxyHost: "git.tetral.test"}
	err := converger.removeDeleted(context.Background(), ProviderHandle{SandboxID: "provider_sandbox"}, []GitHubRepositoryMount{{
		ResourceID: "sesrsc_repo_deleted", URL: "https://github.com/tetral-ai/tetral", MountPath: "/workspace/tetral",
	}})
	if err != nil {
		t.Fatalf("RemoveDeletedGitHubRepositories: %v", err)
	}
	if len(materializer.removals) != 1 || materializer.removals[0].MountPath != "/workspace/tetral" {
		t.Fatalf("GitHub removals = %+v; want deleted checkout", materializer.removals)
	}
	if len(rotator.pendingCalls) != 0 || len(rotator.activationCalls) != 0 || len(materializer.calls) != 0 {
		t.Fatalf("delete minted ticket or cloned: pending=%d active=%d calls=%d", len(rotator.pendingCalls), len(rotator.activationCalls), len(materializer.calls))
	}
}

func TestRemoveDeletedGitHubRepositoriesValidatesAllPathsBeforeRemoval(t *testing.T) {
	materializer := &recordingGitHubRepositoryMaterializer{}
	converger := &GitHubRepositoryConverger{Materializer: materializer}
	err := converger.removeDeleted(context.Background(), ProviderHandle{SandboxID: "provider_sandbox"}, []GitHubRepositoryMount{
		{ResourceID: "sesrsc_repo_valid", URL: "https://github.com/tetral-ai/valid", MountPath: "/workspace/valid"},
		{ResourceID: "sesrsc_repo_invalid", URL: "https://github.com/tetral-ai/invalid", MountPath: "/workspace"},
	})
	if err == nil {
		t.Fatal("removeDeletedGitHubRepositories succeeded; want reserved-path error")
	}
	if len(materializer.removals) != 0 {
		t.Fatalf("GitHub removals = %+v; want validation before the first removal", materializer.removals)
	}
}

type recordingGitTicketRotator struct {
	pendingCalls    []recordingGitTicketRotation
	activationCalls []recordingGitTicketActivation
	err             error
	eventsRef       *[]string
	findTicket      *gitticket.Ticket
}

type recordingGitTicketRotation struct {
	ws        workspace.ID
	sessionID string
	ticketID  string
	tokenHash []byte
	when      time.Time
}

type recordingGitTicketActivation struct {
	ws        workspace.ID
	sessionID string
	ticketID  string
	when      time.Time
}

func (r *recordingGitTicketRotator) CreatePending(_ context.Context, ws workspace.ID, sessionID string, ticketID string, tokenHash []byte, now time.Time) (*gitticket.Ticket, error) {
	if r.eventsRef != nil {
		*r.eventsRef = append(*r.eventsRef, "pending")
	}
	r.pendingCalls = append(r.pendingCalls, recordingGitTicketRotation{
		ws:        ws,
		sessionID: sessionID,
		ticketID:  ticketID,
		tokenHash: append([]byte(nil), tokenHash...),
		when:      now,
	})
	if r.err != nil {
		return nil, r.err
	}
	return &gitticket.Ticket{
		WorkspaceID: ws,
		SessionID:   sessionID,
		TicketID:    ticketID,
		TokenHash:   append([]byte(nil), tokenHash...),
		Status:      gitticket.StatusPending,
		CreatedAt:   now,
	}, nil
}

func (r *recordingGitTicketRotator) ActivatePending(_ context.Context, ws workspace.ID, sessionID string, ticketID string, now time.Time) (*gitticket.Ticket, error) {
	if r.eventsRef != nil {
		*r.eventsRef = append(*r.eventsRef, "activate")
	}
	r.activationCalls = append(r.activationCalls, recordingGitTicketActivation{ws: ws, sessionID: sessionID, ticketID: ticketID, when: now})
	if r.err != nil {
		return nil, r.err
	}
	return &gitticket.Ticket{WorkspaceID: ws, SessionID: sessionID, TicketID: ticketID, Status: gitticket.StatusLive, CreatedAt: now}, nil
}

func (r *recordingGitTicketRotator) FindBySessionTokenHash(_ context.Context, _ workspace.ID, _ string, _ []byte) (*gitticket.Ticket, error) {
	if r.findTicket == nil {
		return nil, &gitticket.NotFoundError{Message: "git ticket not found"}
	}
	copyTicket := *r.findTicket
	return &copyTicket, nil
}

type recordingGitHubRepositoryMaterializer struct {
	calls         []GitHubRepositoryPreparation
	installs      []GitHubRepositoryConfiguration
	removals      []GitHubRepositoryMount
	err           error
	cloneErr      error
	eventsRef     *[]string
	installedHash []byte
}

func (m *recordingGitHubRepositoryMaterializer) RemoveGitHubRepository(_ context.Context, _ string, repository GitHubRepositoryMount) error {
	if m.eventsRef != nil {
		*m.eventsRef = append(*m.eventsRef, "github_remove")
	}
	m.removals = append(m.removals, repository)
	return m.err
}

func (m *recordingGitHubRepositoryMaterializer) InstalledGitTicketHash(context.Context, string, string) ([]byte, bool, error) {
	if len(m.installedHash) == 0 {
		return nil, false, nil
	}
	return append([]byte(nil), m.installedHash...), true, nil
}

func (m *recordingGitHubRepositoryMaterializer) InstallGitHubRepositoryConfiguration(_ context.Context, configuration GitHubRepositoryConfiguration) error {
	if m.eventsRef != nil {
		*m.eventsRef = append(*m.eventsRef, "config install")
	}
	m.installs = append(m.installs, configuration)
	hash, err := gitticket.HashToken(configuration.Ticket)
	if err == nil {
		m.installedHash = hash
	}
	return m.err
}

func (m *recordingGitHubRepositoryMaterializer) CloneGitHubRepositories(_ context.Context, preparation GitHubRepositoryPreparation) error {
	if m.eventsRef != nil {
		*m.eventsRef = append(*m.eventsRef, "clone")
	}
	preparation.Repositories = append([]GitHubRepositoryMount(nil), preparation.Repositories...)
	m.calls = append(m.calls, preparation)
	if m.cloneErr != nil {
		return m.cloneErr
	}
	return m.err
}
