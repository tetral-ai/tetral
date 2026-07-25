package sandbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPrepareGitHubRepositoriesPostgreSQLPhaseOrderRequiresLiveBeforeClone(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_phase")
	events := []string{}
	rotator := &eventingGitTicketRotator{delegate: store, events: &events}
	materializer := &persistedGitConfigMaterializer{admin: admin, events: &events}
	service := newGitHubPreparationService(rotator, materializer, bytes.NewReader(bytes.Repeat([]byte{1}, gitticket.TokenBytes)))

	if err := service.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_phase", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(time.Time{})); err != nil {
		t.Fatalf("prepareGitHubRepositories: %v", err)
	}
	if got := strings.Join(events, " -> "); got != "pending -> config install -> activate -> clone" {
		t.Fatalf("phase order = %s; want pending -> config install -> activate -> clone", got)
	}
	assertGitTicketCounts(t, admin, "sesn_git_phase", 0, 1, 0)
}

func TestPrepareGitHubRepositoriesExistingCheckoutSkipsCloneAfterActivation(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_skip")
	events := []string{}
	rotator := &eventingGitTicketRotator{delegate: store, events: &events}
	materializer := &persistedGitConfigMaterializer{admin: admin, events: &events, skipClone: true}
	service := newGitHubPreparationService(rotator, materializer, bytes.NewReader(bytes.Repeat([]byte{2}, gitticket.TokenBytes)))

	if err := service.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_skip", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(time.Time{})); err != nil {
		t.Fatalf("prepareGitHubRepositories: %v", err)
	}
	if got := strings.Join(events, " -> "); got != "pending -> config install -> activate -> clone skipped" {
		t.Fatalf("phase order = %s; want pending -> config install -> activate -> clone skipped", got)
	}
	if materializer.cloneNetworkUses != 0 {
		t.Fatalf("clone network uses = %d; want 0 for an existing checkout", materializer.cloneNetworkUses)
	}
	assertGitTicketCounts(t, admin, "sesn_git_skip", 0, 1, 0)
}

func TestPrepareGitHubRepositoriesRetryBeforeInstallMintsSafely(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_before_install")
	attemptCreatedAt := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	oldToken, oldHash := deterministicGitHubPreparationTicket(t, 3)
	old, err := store.CreatePending(ctx, workspace.DefaultID, "sesn_git_before_install", "gitt_old", oldHash, attemptCreatedAt.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CreatePending old: %v", err)
	}
	if _, err := store.ActivatePending(ctx, workspace.DefaultID, "sesn_git_before_install", old.TicketID, attemptCreatedAt.Add(-time.Hour)); err != nil {
		t.Fatalf("ActivatePending old: %v", err)
	}
	materializer := &persistedGitConfigMaterializer{admin: admin, installedHash: oldHash, failInstallOnce: true}
	first := newGitHubPreparationService(store, materializer, bytes.NewReader(bytes.Repeat([]byte{4}, gitticket.TokenBytes)))

	if err := first.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_before_install", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(attemptCreatedAt)); err == nil {
		t.Fatal("first preparation succeeded; want pre-install failure")
	}
	assertGitTicketCounts(t, admin, "sesn_git_before_install", 1, 1, 0)
	if !bytes.Equal(materializer.installedHash, oldHash) {
		t.Fatal("pre-install failure replaced the previously installed live ticket")
	}

	// The retry has a fresh service and store and receives no raw ticket from the
	// failed service. It may abandon the unreachable pending row and mint anew.
	freshStore := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
	retry := newGitHubPreparationService(freshStore, materializer, bytes.NewReader(bytes.Repeat([]byte{5}, gitticket.TokenBytes)))
	if err := retry.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_before_install", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(attemptCreatedAt)); err != nil {
		t.Fatalf("retry prepareGitHubRepositories: %v", err)
	}
	assertGitTicketCounts(t, admin, "sesn_git_before_install", 1, 1, 1)
	assertGitTicketStatus(t, admin, oldHash, gitticket.StatusRotated)
	_, abandonedHash := deterministicGitHubPreparationTicket(t, 4)
	assertGitTicketStatus(t, admin, abandonedHash, gitticket.StatusPending)
	if materializer.cloneNetworkUses != 1 {
		t.Fatalf("clone network uses = %d; want 1 after replacement activation", materializer.cloneNetworkUses)
	}
	if strings.Contains(materializer.persistedState(), oldToken) {
		t.Fatal("persisted sandbox recovery state retained a raw ticket")
	}
}

func TestPrepareGitHubRepositoriesRetryBetweenInstallAndActivationUsesInstalledPending(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_before_activate")
	attemptCreatedAt := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	materializer := &persistedGitConfigMaterializer{admin: admin}
	firstRotator := &eventingGitTicketRotator{delegate: store, failActivationOnce: true}
	first := newGitHubPreparationService(firstRotator, materializer, bytes.NewReader(bytes.Repeat([]byte{6}, gitticket.TokenBytes)))

	if err := first.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_before_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(attemptCreatedAt)); err == nil {
		t.Fatal("first preparation succeeded; want activation-boundary failure")
	}
	assertGitTicketCounts(t, admin, "sesn_git_before_activate", 1, 0, 0)
	if materializer.cloneInvocations != 0 || materializer.cloneNetworkUses != 0 {
		t.Fatalf("pending ticket reached clone: invocations=%d network=%d", materializer.cloneInvocations, materializer.cloneNetworkUses)
	}

	freshStore := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
	retry := newGitHubPreparationService(freshStore, materializer, errorReader{})
	if err := retry.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_before_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(attemptCreatedAt)); err != nil {
		t.Fatalf("retry prepareGitHubRepositories: %v", err)
	}
	assertGitTicketCounts(t, admin, "sesn_git_before_activate", 0, 1, 0)
	if materializer.cloneNetworkUses != 1 {
		t.Fatalf("clone network uses = %d; want exact installed pending activated then cloned", materializer.cloneNetworkUses)
	}
}

func TestPrepareGitHubRepositoriesRetryAfterActivationContinuesWithoutMinting(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_after_activate")
	attemptCreatedAt := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	materializer := &persistedGitConfigMaterializer{admin: admin, failCloneOnce: true}
	first := newGitHubPreparationService(store, materializer, bytes.NewReader(bytes.Repeat([]byte{7}, gitticket.TokenBytes)))

	if err := first.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_after_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(attemptCreatedAt)); err == nil {
		t.Fatal("first preparation succeeded; want post-activation/pre-network failure")
	}
	assertGitTicketCounts(t, admin, "sesn_git_after_activate", 0, 1, 0)
	if materializer.cloneNetworkUses != 0 {
		t.Fatalf("clone network uses = %d; want 0 before retry", materializer.cloneNetworkUses)
	}

	freshStore := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
	retry := newGitHubPreparationService(freshStore, materializer, errorReader{})
	if err := retry.prepareGitHubRepositories(ctx, workspace.DefaultID, "sesn_git_after_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubPreparationResources(attemptCreatedAt)); err != nil {
		t.Fatalf("retry prepareGitHubRepositories: %v", err)
	}
	assertGitTicketCounts(t, admin, "sesn_git_after_activate", 0, 1, 0)
	if materializer.installCalls != 1 {
		t.Fatalf("config install calls = %d; want retry to reuse installed live ticket", materializer.installCalls)
	}
	if materializer.cloneNetworkUses != 1 {
		t.Fatalf("clone network uses = %d; want retry to continue clone", materializer.cloneNetworkUses)
	}
}

type eventingGitTicketRotator struct {
	delegate           *gitticket.PostgreSQLGitTicketStore
	events             *[]string
	failActivationOnce bool
}

func (r *eventingGitTicketRotator) CreatePending(ctx context.Context, ws workspace.ID, sessionID, ticketID string, tokenHash []byte, now time.Time) (*gitticket.Ticket, error) {
	ticket, err := r.delegate.CreatePending(ctx, ws, sessionID, ticketID, tokenHash, now)
	if err == nil && r.events != nil {
		*r.events = append(*r.events, "pending")
	}
	return ticket, err
}

func (r *eventingGitTicketRotator) ActivatePending(ctx context.Context, ws workspace.ID, sessionID, ticketID string, now time.Time) (*gitticket.Ticket, error) {
	if r.events != nil {
		*r.events = append(*r.events, "activate")
	}
	if r.failActivationOnce {
		r.failActivationOnce = false
		return nil, errors.New("injected activation-boundary failure")
	}
	return r.delegate.ActivatePending(ctx, ws, sessionID, ticketID, now)
}

func (r *eventingGitTicketRotator) FindBySessionTokenHash(ctx context.Context, ws workspace.ID, sessionID string, tokenHash []byte) (*gitticket.Ticket, error) {
	return r.delegate.FindBySessionTokenHash(ctx, ws, sessionID, tokenHash)
}

type persistedGitConfigMaterializer struct {
	admin            *sql.DB
	events           *[]string
	installedHash    []byte
	failInstallOnce  bool
	failCloneOnce    bool
	skipClone        bool
	installCalls     int
	cloneInvocations int
	cloneNetworkUses int
}

func (m *persistedGitConfigMaterializer) InstalledGitTicketHash(context.Context, string, string) ([]byte, bool, error) {
	if len(m.installedHash) == 0 {
		return nil, false, nil
	}
	return append([]byte(nil), m.installedHash...), true, nil
}

func (m *persistedGitConfigMaterializer) InstallGitHubRepositoryConfiguration(_ context.Context, configuration GitHubRepositoryConfiguration) error {
	m.installCalls++
	if m.failInstallOnce {
		m.failInstallOnce = false
		return errors.New("injected pre-install failure")
	}
	hash, err := gitticket.HashToken(configuration.Ticket)
	if err != nil {
		return err
	}
	m.installedHash = append([]byte(nil), hash...)
	if m.events != nil {
		*m.events = append(*m.events, "config install")
	}
	return nil
}

func (m *persistedGitConfigMaterializer) CloneGitHubRepositories(ctx context.Context, preparation GitHubRepositoryPreparation) error {
	m.cloneInvocations++
	if m.failCloneOnce {
		m.failCloneOnce = false
		return errors.New("injected post-activation/pre-network failure")
	}
	var status string
	if err := m.admin.QueryRowContext(ctx,
		`SELECT status FROM session_git_tickets WHERE workspace_id = $1 AND session_id = $2 AND token_hash = $3`,
		string(preparation.WorkspaceID), preparation.SessionID, m.installedHash,
	).Scan(&status); err != nil {
		return err
	}
	if status != gitticket.StatusLive {
		return errors.New("clone attempted without a live installed ticket")
	}
	if m.events != nil {
		if m.skipClone {
			*m.events = append(*m.events, "clone skipped")
		} else {
			*m.events = append(*m.events, "clone")
		}
	}
	if !m.skipClone {
		m.cloneNetworkUses++
	}
	return nil
}

func (*persistedGitConfigMaterializer) RemoveGitHubRepository(context.Context, string, GitHubRepositoryMount) error {
	return nil
}

func (m *persistedGitConfigMaterializer) persistedState() string {
	return string(m.installedHash)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("retry unexpectedly attempted to mint a ticket")
}

func newGitHubPreparationTicketStore(t *testing.T, sessionID string) (*gitticket.PostgreSQLGitTicketStore, *sql.DB) {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	return gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB)), admin
}

func newGitHubPreparationService(rotator GitTicketRotator, materializer GitHubRepositoryMaterializer, random io.Reader) *Service {
	return NewService(nil, nil,
		WithGitHubRepositoryPreparation(rotator, materializer, "git.tetral.test"),
		WithGitTicketRandomSource(random),
		WithClock(func() time.Time { return time.Date(2026, 7, 14, 11, 0, 0, 0, time.UTC) }),
	)
}

func githubPreparationResources(attemptCreatedAt time.Time) ResourceSetup {
	return ResourceSetup{
		PreparationAttemptCreatedAt: attemptCreatedAt,
		GitHubRepositories: []GitHubRepositoryMount{{
			ResourceID: "sesrsc_repo",
			URL:        "https://github.com/tetral-ai/tetral",
			MountPath:  "/workspace/tetral",
		}},
	}
}

func deterministicGitHubPreparationTicket(t *testing.T, fill byte) (string, []byte) {
	t.Helper()
	token, err := gitticket.GenerateToken(bytes.NewReader(bytes.Repeat([]byte{fill}, gitticket.TokenBytes)))
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	hash, err := gitticket.HashToken(token)
	if err != nil {
		t.Fatalf("HashToken: %v", err)
	}
	return token, hash
}

func assertGitTicketCounts(t *testing.T, admin *sql.DB, sessionID string, pending, live, rotated int) {
	t.Helper()
	for status, want := range map[string]int{
		gitticket.StatusPending: pending,
		gitticket.StatusLive:    live,
		gitticket.StatusRotated: rotated,
	} {
		var got int
		if err := admin.QueryRow(
			`SELECT count(*) FROM session_git_tickets WHERE workspace_id = $1 AND session_id = $2 AND status = $3`,
			string(workspace.DefaultID), sessionID, status,
		).Scan(&got); err != nil {
			t.Fatalf("count %s tickets: %v", status, err)
		}
		if got != want {
			t.Fatalf("%s ticket count = %d; want %d", status, got, want)
		}
	}
}

func assertGitTicketStatus(t *testing.T, admin *sql.DB, hash []byte, want string) {
	t.Helper()
	var got string
	if err := admin.QueryRow(`SELECT status FROM session_git_tickets WHERE token_hash = $1`, hash).Scan(&got); err != nil {
		t.Fatalf("read git ticket status: %v", err)
	}
	if got != want {
		t.Fatalf("git ticket status = %q; want %q", got, want)
	}
}
