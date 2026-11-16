// Layer 2: the Agent service and its PostgreSQL store against a real
// PostgreSQL.
package agent_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func newPGAgentEnv(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func seedAgentWorkspace(t *testing.T, admin *sql.DB, ws workspace.ID, name string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`, string(ws), name); err != nil {
		t.Fatalf("seed workspace %s: %v", ws, err)
	}
}

func basicAgentRequest() agent.CreateAgentRequest {
	return agent.CreateAgentRequest{
		AgentConfig: agent.AgentConfig{
			Name:  "ci-agent",
			Model: "anthropic/claude-opus-4-8",
			Tools: basicAgentTools(),
		},
	}
}

func basicAgentTools() agent.RawArray {
	return agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)}
}

func TestPostgreSQLAgentStoreCreateAndGet(t *testing.T) {
	runtime, _ := newPGAgentEnv(t)
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, basicAgentRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Version != 1 {
		t.Errorf("Version = %d; want 1", created.Version)
	}
	got, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "ci-agent" {
		t.Errorf("Name = %q; want ci-agent", got.Name)
	}
}

func TestPostgreSQLAgentStoreCrossWorkspaceGetReturnsNotFound(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	seedAgentWorkspace(t, admin, "workspace_b", "B")
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()

	bAgent, err := store.Create(ctx, "workspace_b", basicAgentRequest())
	if err != nil {
		t.Fatalf("Create workspace_b agent: %v", err)
	}
	_, err = store.Get(ctx, workspace.DefaultID, bAgent.ID)
	if err == nil {
		t.Fatal("default workspace must not Get a workspace_b agent")
	}
	var nf *agent.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *agent.NotFoundError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLAgentStoreListScopesByWorkspace(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	seedAgentWorkspace(t, admin, "workspace_b", "B")
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		req := basicAgentRequest()
		req.Name = "default-" + string(rune('a'+i))
		if _, err := store.Create(ctx, workspace.DefaultID, req); err != nil {
			t.Fatalf("Create default %d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		req := basicAgentRequest()
		req.Name = "b-" + string(rune('a'+i))
		if _, err := store.Create(ctx, "workspace_b", req); err != nil {
			t.Fatalf("Create workspace_b %d: %v", i, err)
		}
	}
	defaultList, err := store.List(ctx, workspace.DefaultID, agent.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List default: %v", err)
	}
	if len(defaultList.Data) != 3 {
		t.Errorf("default workspace List size = %d; want 3", len(defaultList.Data))
	}
	workspaceList, err := store.List(ctx, "workspace_b", agent.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List workspace_b: %v", err)
	}
	if len(workspaceList.Data) != 2 {
		t.Errorf("workspace_b List size = %d; want 2", len(workspaceList.Data))
	}
}

func TestPostgreSQLAgentStoreUpdateVersionConflict(t *testing.T) {
	runtime, _ := newPGAgentEnv(t)
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, basicAgentRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	stale := agent.UpdateAgentRequest{Version: created.Version + 1, AgentConfig: created.AgentConfig}
	_, err = store.Update(ctx, workspace.DefaultID, created.ID, stale)
	if err == nil {
		t.Fatal("Update with stale version must fail")
	}
	var c *agent.ConflictError
	if !errors.As(err, &c) {
		t.Errorf("expected *agent.ConflictError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLAgentStoreUpdatePatchVersionConflict(t *testing.T) {
	runtime, _ := newPGAgentEnv(t)
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, basicAgentRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var patch agent.AgentPatch
	if err := json.Unmarshal([]byte(`{"version":2,"system":"new"}`), &patch); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	_, err = store.UpdatePatch(ctx, workspace.DefaultID, created.ID, patch)
	if err == nil {
		t.Fatal("UpdatePatch with stale version must fail")
	}
	var c *agent.ConflictError
	if !errors.As(err, &c) {
		t.Errorf("expected *agent.ConflictError, got %T (%v)", err, err)
	}
	got, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get after conflict: %v", err)
	}
	if got.Version != created.Version {
		t.Fatalf("stale UpdatePatch changed version: got %d want %d", got.Version, created.Version)
	}
	if got.System != nil {
		t.Fatalf("stale UpdatePatch persisted system = %q; want nil", *got.System)
	}
}

// heldSkillReferenceResolver holds the first write that reaches it inside its
// transaction, where the write already holds the Agent's row lock, until the
// test releases it.
type heldSkillReferenceResolver struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (r *heldSkillReferenceResolver) ValidateAgentSkillReferences(ctx context.Context, _ agent.Transaction, _ string, _ []agent.SkillReference) error {
	first := false
	r.once.Do(func() { first = true })
	if !first {
		return nil
	}
	close(r.entered)
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A write that waited for the Agent's row lock while an update committed a new
// version acts on that version.
func TestPostgreSQLAgentStoreWriteBehindAConcurrentUpdateSeesItsVersion(t *testing.T) {
	system := func(value string) *string { return &value }
	for _, testCase := range []struct {
		name  string
		write func(context.Context, *agent.Service, *agent.Agent) (*agent.Agent, error)
		check func(*testing.T, *agent.Agent, error)
	}{
		{
			name: "an update with the version the first update replaced",
			write: func(ctx context.Context, service *agent.Service, created *agent.Agent) (*agent.Agent, error) {
				config := created.AgentConfig
				config.System = system("second")
				return service.Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{Version: created.Version, AgentConfig: config})
			},
			check: assertAgentVersionConflict,
		},
		{
			name: "a patch with the version the first update replaced",
			write: func(ctx context.Context, service *agent.Service, created *agent.Agent) (*agent.Agent, error) {
				var patch agent.AgentPatch
				if err := json.Unmarshal([]byte(`{"version":1,"system":"second"}`), &patch); err != nil {
					return nil, err
				}
				return service.UpdatePatch(ctx, workspace.DefaultID, created.ID, patch)
			},
			check: assertAgentVersionConflict,
		},
		{
			name: "an archive",
			write: func(ctx context.Context, service *agent.Service, created *agent.Agent) (*agent.Agent, error) {
				return service.Archive(ctx, workspace.DefaultID, created.ID)
			},
			check: func(t *testing.T, archived *agent.Agent, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Archive behind an update: %v", err)
				}
				if archived.Version != 2 || archived.ArchivedAt == nil || archived.System == nil || *archived.System != "first" {
					t.Fatalf("archived Agent = version %d, archived_at %v, system %v; want the first update's version 2, archived", archived.Version, archived.ArchivedAt, archived.System)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := newPGAgentEnv(t)
			store := agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			created, err := agent.NewService(store, nil).Create(ctx, workspace.DefaultID, basicAgentRequest())
			if err != nil {
				t.Fatalf("Create: %v", err)
			}

			held := &heldSkillReferenceResolver{entered: make(chan struct{}), release: make(chan struct{})}
			firstDone := make(chan error, 1)
			go func() {
				config := created.AgentConfig
				config.System = system("first")
				_, err := agent.NewService(store, held).Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{Version: created.Version, AgentConfig: config})
				firstDone <- err
			}()
			select {
			case <-held.entered:
			case err := <-firstDone:
				t.Fatalf("first Update ended before it was held: %v", err)
			}

			secondDone := make(chan agentWriteOutcome, 1)
			go func() {
				got, err := testCase.write(ctx, agent.NewService(store, nil), created)
				secondDone <- agentWriteOutcome{got, err}
			}()
			waitForRowLockWaiter(ctx, t, admin, secondDone)
			close(held.release)
			if err := <-firstDone; err != nil {
				t.Fatalf("first Update: %v", err)
			}
			second := <-secondDone
			testCase.check(t, second.agent, second.err)
		})
	}
}

func assertAgentVersionConflict(t *testing.T, _ *agent.Agent, err error) {
	t.Helper()
	var conflict *agent.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("write with a replaced version = %T (%v); want *agent.ConflictError", err, err)
	}
}

type agentWriteOutcome struct {
	agent *agent.Agent
	err   error
}

// waitForRowLockWaiter returns once a backend of the test's database waits
// for a row lock. The write that should wait must not end before that.
func waitForRowLockWaiter(ctx context.Context, t *testing.T, admin *sql.DB, write <-chan agentWriteOutcome) {
	t.Helper()
	for {
		var waiting int
		if err := admin.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_stat_activity
			  WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatalf("read lock waiters: %v", err)
		}
		if waiting > 0 {
			return
		}
		select {
		case ended := <-write:
			t.Fatalf("the second write ended before it waited for the Agent's row lock: %v", ended.err)
		case <-ctx.Done():
			t.Fatal("the second write never waited for the Agent's row lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestPostgreSQLAgentStoreUpdateNoOpReturnsCurrent(t *testing.T) {
	runtime, _ := newPGAgentEnv(t)
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, basicAgentRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	noop := agent.UpdateAgentRequest{Version: created.Version, AgentConfig: created.AgentConfig}
	updated, err := store.Update(ctx, workspace.DefaultID, created.ID, noop)
	if err != nil {
		t.Fatalf("no-op Update: %v", err)
	}
	if updated.Version != created.Version {
		t.Errorf("no-op Update bumped version: %d → %d", created.Version, updated.Version)
	}
}

func TestPostgreSQLAgentStoreCrossWorkspaceUpdateReturnsNotFound(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	seedAgentWorkspace(t, admin, "workspace_b", "B")
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()
	bAgent, err := store.Create(ctx, "workspace_b", basicAgentRequest())
	if err != nil {
		t.Fatalf("Create workspace_b: %v", err)
	}
	upd := agent.UpdateAgentRequest{Version: bAgent.Version, AgentConfig: bAgent.AgentConfig}
	_, err = store.Update(ctx, workspace.DefaultID, bAgent.ID, upd)
	if err == nil {
		t.Fatal("default workspace must not Update workspace_b's agent")
	}
	var nf *agent.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *agent.NotFoundError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLAgentStoreGetVersionScopedToWorkspace(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	seedAgentWorkspace(t, admin, "workspace_b", "B")
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()
	bAgent, err := store.Create(ctx, "workspace_b", basicAgentRequest())
	if err != nil {
		t.Fatalf("Create workspace_b: %v", err)
	}
	_, err = store.GetVersion(ctx, workspace.DefaultID, bAgent.ID, bAgent.Version)
	if err == nil {
		t.Fatal("default workspace must not GetVersion of workspace_b agent")
	}
	var nf *agent.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *agent.NotFoundError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLAgentStoreCrossWorkspacePageTokenReturnsInvalidRequest(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	seedAgentWorkspace(t, admin, "workspace_b", "B")
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		req := basicAgentRequest()
		req.Name = "b-page-" + string(rune('a'+i))
		if _, err := store.Create(ctx, "workspace_b", req); err != nil {
			t.Fatalf("Create workspace_b %d: %v", i, err)
		}
	}

	workspaceList, err := store.List(ctx, "workspace_b", agent.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List workspace_b: %v", err)
	}
	if workspaceList.NextPage == nil {
		t.Fatal("workspace_b List did not return next_page")
	}
	_, err = store.List(ctx, workspace.DefaultID, agent.ListOptions{Limit: 1, Page: *workspaceList.NextPage})
	if err == nil {
		t.Fatal("default workspace must reject workspace_b page token")
	}
	var validation *agent.ValidationError
	if !errors.As(err, &validation) {
		t.Errorf("expected *agent.ValidationError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLAgentStoreListRequiresVisibleCurrentVersion(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()

	firstReq := basicAgentRequest()
	firstReq.Name = "first"
	first, err := store.Create(ctx, workspace.DefaultID, firstReq)
	if err != nil {
		t.Fatalf("Create first: %v", err)
	}
	hiddenReq := basicAgentRequest()
	hiddenReq.Name = "hidden"
	hidden, err := store.Create(ctx, workspace.DefaultID, hiddenReq)
	if err != nil {
		t.Fatalf("Create hidden: %v", err)
	}
	lastReq := basicAgentRequest()
	lastReq.Name = "last"
	last, err := store.Create(ctx, workspace.DefaultID, lastReq)
	if err != nil {
		t.Fatalf("Create last: %v", err)
	}

	if _, err := admin.ExecContext(ctx,
		`DELETE FROM agent_versions
		  WHERE workspace_id = $1 AND agent_id = $2 AND version = $3`,
		string(workspace.DefaultID), hidden.ID, hidden.Version,
	); err != nil {
		t.Fatalf("delete hidden current version: %v", err)
	}

	results, err := store.List(ctx, workspace.DefaultID, agent.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(results.Data) != 2 {
		t.Fatalf("List returned %d agents; want 2", len(results.Data))
	}
	if results.Data[0].ID != first.ID || results.Data[1].ID != last.ID {
		t.Fatalf("List returned IDs %q, %q; want %q, %q", results.Data[0].ID, results.Data[1].ID, first.ID, last.ID)
	}
}

func TestPostgreSQLAgentStoreConfigJsonByteStability(t *testing.T) {
	runtime, admin := newPGAgentEnv(t)
	store := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), nil)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, basicAgentRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Compute the canonical bytes the way the store did so the
	// assertion proves "stored == canonical(config)" exactly,
	// not just "non-empty". This catches any future regression that
	// re-encodes config_json (e.g. switching to jsonb, which
	// PostgreSQL canonicalizes by stripping whitespace).
	canonicalBytes, _, canonErr := agent.Canonicalize(created.AgentConfig)
	if canonErr != nil {
		t.Fatalf("Canonicalize: %v", canonErr)
	}

	// Read the raw bytes through the admin (BYPASSRLS) connection
	// so the test is independent of the runtime role's workspace
	// transaction requirement.
	var stored string
	if err := admin.QueryRowContext(ctx,
		`SELECT av.config_json FROM agent_versions av JOIN agents a ON a.id = av.agent_id
		  WHERE av.agent_id = $1 AND av.version = $2`,
		created.ID, created.Version,
	).Scan(&stored); err != nil {
		t.Fatalf("admin read agent_versions.config_json: %v", err)
	}
	if stored != string(canonicalBytes) {
		t.Errorf("agent_versions.config_json byte round-trip:\n stored:    %q\n canonical: %q", stored, string(canonicalBytes))
	}

	// Column type must remain TEXT; converting to jsonb would
	// canonicalize the bytes and break config_hash semantics.
	var dataType string
	if err := admin.QueryRowContext(ctx,
		`SELECT data_type FROM information_schema.columns
		   WHERE table_name = 'agent_versions' AND column_name = 'config_json'
		   AND table_schema = current_schema()`,
	).Scan(&dataType); err != nil {
		t.Fatalf("read column type: %v", err)
	}
	if dataType != "text" {
		t.Errorf("agent_versions.config_json data_type = %q; want text (jsonb is forbidden)", dataType)
	}
}
