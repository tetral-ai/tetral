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

func TestGitHubRepositoryConvergerPostgreSQLPhaseOrderRequiresLiveBeforeClone(t *testing.T) {
	for _, privilege := range []string{"SELECT", "INSERT", "UPDATE"} {
		t.Run(privilege, func(t *testing.T) {
			ctx := context.Background()
			_, admin := newGitHubPreparationTicketStore(t, "sesn_git_phase")
			workload := storagetest.OpenWorkloadDB(t, admin, "sandbox")
			store := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(workload.DB))
			events := []string{}
			rotator := &eventingGitTicketRotator{delegate: store, events: &events}
			materializer := &persistedGitConfigMaterializer{admin: admin, events: &events}
			service := newGitHubRepositoryConverger(rotator, materializer, bytes.NewReader(bytes.Repeat([]byte{1}, 2*gitticket.TokenBytes)))
			run := func() error {
				return service.materialize(ctx, workspace.DefaultID, "sesn_git_phase", ProviderHandle{SandboxID: "provider_sandbox"}, githubRepositoryResources())
			}
			workload.RequirePrivilege(t, "session_git_tickets", privilege, run)
			if materializer.cloneInvocations != 0 {
				t.Fatal("permission failure reached clone")
			}
			want := "pending -> config install -> activate -> clone"
			if privilege == "UPDATE" {
				assertGitTicketCounts(t, admin, "sesn_git_phase", 1, 0, 0)
				// Recovery must find and activate the installed pending ticket, without minting.
				service.Random = errorReader{}
				want = "activate -> clone"
			} else {
				assertGitTicketCounts(t, admin, "sesn_git_phase", 0, 0, 0)
			}
			events = nil
			if err := run(); err != nil {
				t.Fatalf("MaterializeGitHubRepositories: %v", err)
			}
			if got := strings.Join(events, " -> "); got != want {
				t.Fatalf("phase order = %s; want %s", got, want)
			}
			assertGitTicketCounts(t, admin, "sesn_git_phase", 0, 1, 0)
		})
	}
}

func TestGitHubRepositoryConvergerExistingCheckoutSkipsCloneAfterActivation(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_skip")
	events := []string{}
	rotator := &eventingGitTicketRotator{delegate: store, events: &events}
	materializer := &persistedGitConfigMaterializer{admin: admin, events: &events, skipClone: true}
	service := newGitHubRepositoryConverger(rotator, materializer, bytes.NewReader(bytes.Repeat([]byte{2}, gitticket.TokenBytes)))

	if err := service.materialize(ctx, workspace.DefaultID, "sesn_git_skip", ProviderHandle{SandboxID: "provider_sandbox"}, githubRepositoryResources()); err != nil {
		t.Fatalf("MaterializeGitHubRepositories: %v", err)
	}
	if got := strings.Join(events, " -> "); got != "pending -> config install -> activate -> clone skipped" {
		t.Fatalf("phase order = %s; want pending -> config install -> activate -> clone skipped", got)
	}
	if materializer.cloneNetworkUses != 0 {
		t.Fatalf("clone network uses = %d; want 0 for an existing checkout", materializer.cloneNetworkUses)
	}
	assertGitTicketCounts(t, admin, "sesn_git_skip", 0, 1, 0)
}

func TestGitHubRepositoryConvergerRetryBetweenInstallAndActivationUsesInstalledPending(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_before_activate")
	materializer := &persistedGitConfigMaterializer{admin: admin}
	firstRotator := &eventingGitTicketRotator{delegate: store, failActivationOnce: true}
	first := newGitHubRepositoryConverger(firstRotator, materializer, bytes.NewReader(bytes.Repeat([]byte{6}, gitticket.TokenBytes)))

	if err := first.materialize(ctx, workspace.DefaultID, "sesn_git_before_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubRepositoryResources()); err == nil {
		t.Fatal("first preparation succeeded; want activation-boundary failure")
	}
	assertGitTicketCounts(t, admin, "sesn_git_before_activate", 1, 0, 0)
	if materializer.cloneInvocations != 0 || materializer.cloneNetworkUses != 0 {
		t.Fatalf("pending ticket reached clone: invocations=%d network=%d", materializer.cloneInvocations, materializer.cloneNetworkUses)
	}

	freshStore := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
	retry := newGitHubRepositoryConverger(freshStore, materializer, errorReader{})
	if err := retry.materialize(ctx, workspace.DefaultID, "sesn_git_before_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubRepositoryResources()); err != nil {
		t.Fatalf("retry MaterializeGitHubRepositories: %v", err)
	}
	assertGitTicketCounts(t, admin, "sesn_git_before_activate", 0, 1, 0)
	if materializer.cloneNetworkUses != 1 {
		t.Fatalf("clone network uses = %d; want exact installed pending activated then cloned", materializer.cloneNetworkUses)
	}
}

func TestGitHubRepositoryConvergerRetryAfterActivationContinuesWithoutMinting(t *testing.T) {
	ctx := context.Background()
	store, admin := newGitHubPreparationTicketStore(t, "sesn_git_after_activate")
	materializer := &persistedGitConfigMaterializer{admin: admin, failCloneOnce: true}
	first := newGitHubRepositoryConverger(store, materializer, bytes.NewReader(bytes.Repeat([]byte{7}, gitticket.TokenBytes)))

	if err := first.materialize(ctx, workspace.DefaultID, "sesn_git_after_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubRepositoryResources()); err == nil {
		t.Fatal("first preparation succeeded; want post-activation/pre-network failure")
	}
	assertGitTicketCounts(t, admin, "sesn_git_after_activate", 0, 1, 0)
	if materializer.cloneNetworkUses != 0 {
		t.Fatalf("clone network uses = %d; want 0 before retry", materializer.cloneNetworkUses)
	}

	freshStore := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
	retry := newGitHubRepositoryConverger(freshStore, materializer, errorReader{})
	if err := retry.materialize(ctx, workspace.DefaultID, "sesn_git_after_activate", ProviderHandle{SandboxID: "provider_sandbox"}, githubRepositoryResources()); err != nil {
		t.Fatalf("retry MaterializeGitHubRepositories: %v", err)
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

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("retry unexpectedly attempted to mint a ticket")
}

func newGitHubPreparationTicketStore(t *testing.T, sessionID string) (*gitticket.PostgreSQLGitTicketStore, *sql.DB) {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedGitHubMaterializationSession(t, admin, workspace.DefaultID, sessionID)
	return gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB)), admin
}

func newGitHubRepositoryConverger(rotator GitTicketRotator, materializer GitHubRepositoryMaterializer, random io.Reader) *GitHubRepositoryConverger {
	return &GitHubRepositoryConverger{
		Rotator: rotator, Materializer: materializer, GitProxyHost: "git.tetral.test", Random: random,
		Clock: func() time.Time { return time.Date(2026, 7, 14, 11, 0, 0, 0, time.UTC) },
	}
}

func githubRepositoryResources() []GitHubRepositoryMount {
	return []GitHubRepositoryMount{{
		ResourceID: "sesrsc_repo",
		URL:        "https://github.com/tetral-ai/tetral",
		MountPath:  "/workspace/tetral",
	}}
}

func seedGitHubMaterializationSession(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string) {
	t.Helper()
	const now = "2026-07-14T10:00:00Z"
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, 'agent_git_materialization', 'Git materialization agent', 1, $2, $2)`, string(ws), now); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agv_git_materialization', 'agent_git_materialization', 1, '{}', 'hash', $2)`, string(ws), now); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, 'env_git_materialization', 'Git materialization environment', '{}', $2, $2)`, string(ws), now); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (
			workspace_id, id, type, metadata_json, status, lifecycle_state,
			agent_id, agent_version, environment_id, vault_ids_json, created_at, updated_at
		) VALUES ($1, $2, 'session', '{}', 'idle', 'active',
		          'agent_git_materialization', 1, 'env_git_materialization', '[]', $3, $3)`,
		string(ws), sessionID, now); err != nil {
		t.Fatalf("seed session: %v", err)
	}
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
