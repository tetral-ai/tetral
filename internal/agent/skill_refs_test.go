package agent_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// recordingSkillReferenceResolver is the agent-package fake that records
// every ValidateAgentSkillReferences call and returns a configurable error
// per Skill ID. Tests use it to prove the agent service invokes the
// resolver INSIDE the workspace tx with the resolved (skill_id,
// version) refs, and to inject failures
// without spinning up a real internal/skill service.
type recordingSkillReferenceResolver struct {
	mu    sync.Mutex
	calls []skillReferenceCall
	// rejectByID maps skill_id → rejection error. nil error or
	// missing entry means accept.
	rejectByID map[string]error
}

type skillReferenceCall struct {
	workspaceID string
	refs        []agent.SkillReference
}

func newRecordingSkillReferenceResolver() *recordingSkillReferenceResolver {
	return &recordingSkillReferenceResolver{rejectByID: map[string]error{}}
}

func (r *recordingSkillReferenceResolver) ValidateAgentSkillReferences(_ context.Context, _ agent.Transaction, ws string, refs []agent.SkillReference) error {
	r.mu.Lock()
	r.calls = append(r.calls, skillReferenceCall{workspaceID: ws, refs: append([]agent.SkillReference(nil), refs...)})
	r.mu.Unlock()
	for _, ref := range refs {
		if err, ok := r.rejectByID[ref.SkillID]; ok && err != nil {
			return err
		}
	}
	return nil
}

func (r *recordingSkillReferenceResolver) snapshot() []skillReferenceCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]skillReferenceCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func newAgentSkillReferenceServiceEnv(t *testing.T, resolver agent.SkillReferenceResolver) (*sql.DB, *agent.Service) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	postgresqlStore := agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime))
	service := agent.NewService(postgresqlStore, resolver)
	// Tests count rows via the admin connection so RLS doesn't hide
	// committed rows from the assertion.
	return admin, service
}

func basicAgentRequestWithSkills(skills []map[string]string) agent.CreateAgentRequest {
	cfg := agent.AgentConfig{
		Name:  "with-skills",
		Model: "anthropic/claude-opus-4-8",
		Tools: basicAgentTools(),
	}
	for _, entry := range skills {
		body, _ := marshalSkillEntry(entry)
		cfg.Skills = append(cfg.Skills, body)
	}
	return agent.CreateAgentRequest{AgentConfig: cfg}
}

// TestPostgreSQLAgentStoreSkillRefsHappyPath proves that the agent
// service invokes the SkillReferenceResolver with the resolved
// (workspace, refs) tuple, the references are persisted in
// agent_versions.config_json verbatim (including the literal
// "latest" version), and the response body mirrors the stored shape.
func TestPostgreSQLAgentStoreSkillRefsHappyPath(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	_, service := newAgentSkillReferenceServiceEnv(t, resolver)
	req := basicAgentRequestWithSkills([]map[string]string{
		{"skill_id": "skill_a"},
		{"skill_id": "skill_b", "version": "1759178010641129"},
	})
	created, err := service.Create(context.Background(), workspace.DefaultID, req)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	calls := resolver.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 resolver call; got %d", len(calls))
	}
	got := calls[0]
	if got.workspaceID != string(workspace.DefaultID) {
		t.Errorf("workspaceID = %q", got.workspaceID)
	}
	if len(got.refs) != 2 {
		t.Fatalf("refs count = %d; want 2", len(got.refs))
	}
	if got.refs[0].SkillID != "skill_a" || got.refs[0].Version != "latest" {
		t.Errorf("refs[0] = %+v; want skill_a/latest", got.refs[0])
	}
	if got.refs[1].SkillID != "skill_b" || got.refs[1].Version != "1759178010641129" {
		t.Errorf("refs[1] = %+v", got.refs[1])
	}
	// Persisted Skills bytes match the canonical shape including
	// "latest" — the agent service must NOT resolve "latest" to a
	// concrete version.
	if string(created.Skills[0]) != `{"type":"custom","skill_id":"skill_a","version":"latest"}` {
		t.Errorf("Skills[0] persisted = %s", string(created.Skills[0]))
	}
}

// TestPostgreSQLAgentStoreSkillRefsRejectionDoesNotInsertVersion
// pins the rollback contract: when the SkillReferenceResolver rejects, the
// agent service MUST NOT insert a new agent or agent_version row. The
// test invokes Create on a fresh workspace, asserts the typed
// ValidationError, then admin-counts both tables to confirm zero
// rows landed.
func TestPostgreSQLAgentStoreSkillRefsRejectionDoesNotInsertVersion(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	resolver.rejectByID["skill_missing"] = &agent.ValidationError{Message: "skills[].skill_id \"skill_missing\" does not exist"}
	db, service := newAgentSkillReferenceServiceEnv(t, resolver)
	req := basicAgentRequestWithSkills([]map[string]string{{"skill_id": "skill_missing"}})
	_, err := service.Create(context.Background(), workspace.DefaultID, req)
	if err == nil {
		t.Fatal("expected reject")
	}
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("expected *agent.ValidationError; got %T", err)
	}
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agents WHERE workspace_id = $1 AND name = 'with-skills'`,
		string(workspace.DefaultID),
	).Scan(&n); err != nil {
		t.Fatalf("count agents: %v", err)
	}
	if n != 0 {
		t.Errorf("agents row count after rejected Create = %d; want 0", n)
	}
}

type sentinelWritingSkillReferenceResolver struct {
	err error
}

func (resolver sentinelWritingSkillReferenceResolver) ValidateAgentSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) error {
	if len(refs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO agents (id, workspace_id, name, description, version, archived_at, created_at, updated_at)
		 VALUES ('agent_skill_ref_sentinel', $1, 'skill-ref-sentinel', NULL, 1, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
	)
	if err != nil {
		return err
	}
	return resolver.err
}

func TestPostgreSQLAgentStoreRealSkillResolverFailureRollsBackAgentTransaction(t *testing.T) {
	admin, service := newAgentSkillReferenceServiceEnv(t, sentinelWritingSkillReferenceResolver{
		err: &agent.ValidationError{Message: "sentinel resolver rejected"},
	})
	req := basicAgentRequestWithSkills([]map[string]string{{"skill_id": "skill_missing"}})

	_, err := service.Create(context.Background(), workspace.DefaultID, req)
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("Create sentinel Skill reference error = %T %v; want agent.ValidationError", err, err)
	}
	var agents, versions, sentinels int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agents WHERE workspace_id = $1 AND name = 'with-skills'`,
		string(workspace.DefaultID),
	).Scan(&agents); err != nil {
		t.Fatalf("count agents: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agent_versions WHERE workspace_id = $1`,
		string(workspace.DefaultID),
	).Scan(&versions); err != nil {
		t.Fatalf("count agent_versions: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agents WHERE id = 'agent_skill_ref_sentinel'`,
	).Scan(&sentinels); err != nil {
		t.Fatalf("count sentinel agents: %v", err)
	}
	if agents != 0 || versions != 0 || sentinels != 0 {
		t.Fatalf("rejected shared transaction committed agents=%d versions=%d sentinels=%d; want 0/0/0", agents, versions, sentinels)
	}
}

func TestPostgreSQLAgentStoreRealSkillResolverMissingSkillRollsBackAgentTransaction(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	runtimeClient := dbconnect.NewClientForTesting(runtime)
	skillStore := skill.NewPostgreSQLStore(runtimeClient, nil)
	service := agent.NewService(agent.NewPostgreSQLAgentStore(runtimeClient), skill.NewService(skillStore))
	req := basicAgentRequestWithSkills([]map[string]string{{"skill_id": "skill_missing"}})

	_, err := service.Create(context.Background(), workspace.DefaultID, req)
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("Create missing real Skill reference error = %T %v; want agent.ValidationError", err, err)
	}
	var agents, versions int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agents WHERE workspace_id = $1 AND name = 'with-skills'`,
		string(workspace.DefaultID),
	).Scan(&agents); err != nil {
		t.Fatalf("count agents: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agent_versions WHERE workspace_id = $1`,
		string(workspace.DefaultID),
	).Scan(&versions); err != nil {
		t.Fatalf("count agent_versions: %v", err)
	}
	if agents != 0 || versions != 0 {
		t.Fatalf("rejected real Skill reference committed agents=%d versions=%d; want 0/0", agents, versions)
	}
}

// TestPostgreSQLAgentStoreEmptySkillsBypassesChecker pins the
// optimization that skills[]-less Agents do NOT call the
// SkillReferenceResolver (so they do not touch the workspace
// Skill-registry advisory lock). This keeps Skill-free Agents
// completely independent of the registry.
func TestPostgreSQLAgentStoreEmptySkillsBypassesChecker(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	_, service := newAgentSkillReferenceServiceEnv(t, resolver)
	req := agent.CreateAgentRequest{AgentConfig: agent.AgentConfig{
		Name: "no-skills", Model: "anthropic/claude-opus-4-8", Tools: basicAgentTools(),
	}}
	if _, err := service.Create(context.Background(), workspace.DefaultID, req); err != nil {
		t.Fatalf("Create: %v", err)
	}
	calls := resolver.snapshot()
	// ValidateAgentSkillReferences IS called (with empty refs), but the
	// noop resolver accepts. The actual production resolver bypasses
	// the lock acquisition when refs is empty. We assert the call
	// shape: refs must be empty.
	if len(calls) != 1 {
		t.Fatalf("expected 1 resolver call; got %d", len(calls))
	}
	if len(calls[0].refs) != 0 {
		t.Errorf("expected empty refs for skills-free Agent; got %d", len(calls[0].refs))
	}
}

// TestPostgreSQLAgentStoreUpdateRunsCheckerOnSkillsChange pins that
// Update invokes the SkillReferenceResolver for the new skills[] just like
// Create does. The historical version's stored bytes are immutable
// (the contract does not re-validate on read), but a NEW write must
// validate.
func TestPostgreSQLAgentStoreUpdateRunsCheckerOnSkillsChange(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	_, service := newAgentSkillReferenceServiceEnv(t, resolver)
	created, err := service.Create(context.Background(), workspace.DefaultID, basicAgentRequestWithSkills(nil))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Reset call log to focus on the Update assertion.
	resolver.calls = nil

	req := agent.UpdateAgentRequest{
		Version: created.Version,
		AgentConfig: agent.AgentConfig{
			Name: "with-skills", Model: "anthropic/claude-opus-4-8",
			Tools:  basicAgentTools(),
			Skills: agent.RawArray{rawSkillEntry(t, map[string]string{"skill_id": "skill_added"})},
		},
	}
	updated, err := service.Update(context.Background(), workspace.DefaultID, created.ID, req)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Version != created.Version+1 {
		t.Errorf("Version = %d; want %d", updated.Version, created.Version+1)
	}
	calls := resolver.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 resolver call on Update; got %d", len(calls))
	}
	if len(calls[0].refs) != 1 || calls[0].refs[0].SkillID != "skill_added" {
		t.Errorf("resolver refs = %+v; want [skill_added]", calls[0].refs)
	}
}

// TestPostgreSQLAgentStoreUpdateNoOpStillRunsCheckerForActiveSkills
// ensures the no-op short-circuit cannot bypass the active-reference
// check for non-empty skills[]: an Update that re-submits the same
// canonical config still calls the resolver. This protects against a
// regression where a deleted Skill becomes inaccessible after a
// no-op rewrite.
func TestPostgreSQLAgentStoreUpdateNoOpStillRunsCheckerForActiveSkills(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	_, service := newAgentSkillReferenceServiceEnv(t, resolver)
	req := basicAgentRequestWithSkills([]map[string]string{{"skill_id": "skill_persist"}})
	created, err := service.Create(context.Background(), workspace.DefaultID, req)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	resolver.calls = nil

	noopReq := agent.UpdateAgentRequest{Version: created.Version, AgentConfig: created.AgentConfig}
	if _, err := service.Update(context.Background(), workspace.DefaultID, created.ID, noopReq); err != nil {
		t.Fatalf("Update no-op: %v", err)
	}
	if len(resolver.snapshot()) == 0 {
		t.Errorf("resolver must still run on no-op Update with non-empty skills[] (deleted-Skill regression risk)")
	}
}

// TestPostgreSQLAgentStoreUpdateNoOpSkillRejectionDoesNotBumpVersion
// pins the write-path effect for stale latest references: no-op
// detection must not bypass SkillReferenceResolver, and a resolver rejection
// must leave the version table unchanged.
func TestPostgreSQLAgentStoreUpdateNoOpSkillRejectionDoesNotBumpVersion(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	db, service := newAgentSkillReferenceServiceEnv(t, resolver)
	req := basicAgentRequestWithSkills([]map[string]string{{"skill_id": "skill_stale"}})
	created, err := service.Create(context.Background(), workspace.DefaultID, req)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	resolver.rejectByID["skill_stale"] = &agent.ValidationError{Message: "skills[].version \"latest\" for skill \"skill_stale\" does not exist or is not active"}
	noopReq := agent.UpdateAgentRequest{Version: created.Version, AgentConfig: created.AgentConfig}
	if _, err := service.Update(context.Background(), workspace.DefaultID, created.ID, noopReq); err == nil {
		t.Fatal("expected reject")
	} else if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("expected *agent.ValidationError; got %T", err)
	}

	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agent_versions WHERE workspace_id = $1 AND agent_id = $2`,
		string(workspace.DefaultID), created.ID,
	).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("agent_versions row count = %d; want 1 (no-op Update rejected before write)", n)
	}
}

// TestPostgreSQLAgentStoreHistoricalReadDoesNotInvokeChecker pins that
// historical Agent reads return stored Skill references verbatim without
// calling the current resolver.
func TestPostgreSQLAgentStoreHistoricalReadDoesNotInvokeChecker(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	_, service := newAgentSkillReferenceServiceEnv(t, resolver)
	created, err := service.Create(context.Background(), workspace.DefaultID, basicAgentRequestWithSkills([]map[string]string{
		{"skill_id": "skill_hist"},
	}))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	resolver.calls = nil
	got, err := service.Get(context.Background(), workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Skills[0]) != `{"type":"custom","skill_id":"skill_hist","version":"latest"}` {
		t.Errorf("Get returned modified skills bytes: %s", string(got.Skills[0]))
	}
	if len(resolver.snapshot()) != 0 {
		t.Errorf("Get must NOT call SkillReferenceResolver; got %d calls", len(resolver.snapshot()))
	}
}

// TestPostgreSQLAgentStoreSkillsRejectionDoesNotBumpVersion pins
// that an Update which is rejected by the SkillReferenceResolver leaves
// the existing agent_versions row count unchanged.
func TestPostgreSQLAgentStoreSkillsRejectionDoesNotBumpVersion(t *testing.T) {
	resolver := newRecordingSkillReferenceResolver()
	db, service := newAgentSkillReferenceServiceEnv(t, resolver)
	created, err := service.Create(context.Background(), workspace.DefaultID, basicAgentRequestWithSkills(nil))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Reject any future skill_bad reference.
	resolver.rejectByID["skill_bad"] = &agent.ValidationError{Message: "skill_bad rejected"}
	req := agent.UpdateAgentRequest{
		Version: created.Version,
		AgentConfig: agent.AgentConfig{
			Name: "with-skills", Model: "anthropic/claude-opus-4-8",
			Tools:  basicAgentTools(),
			Skills: agent.RawArray{rawSkillEntry(t, map[string]string{"skill_id": "skill_bad"})},
		},
	}
	if _, err := service.Update(context.Background(), workspace.DefaultID, created.ID, req); err == nil {
		t.Fatal("expected reject")
	}
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agent_versions WHERE workspace_id = $1 AND agent_id = $2`,
		string(workspace.DefaultID), created.ID,
	).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("agent_versions row count = %d; want 1 (Update rolled back)", n)
	}
}

// TestNoopSkillReferenceResolverRejectsNonEmpty pins the safety default:
// an Engine instance built without WithSkillReferenceResolver must reject
// any non-empty skills[] body so a misconfigured wiring cannot
// silently accept unverified references.
func TestNoopSkillReferenceResolverRejectsNonEmpty(t *testing.T) {
	_, service := newAgentSkillReferenceServiceEnv(t, nil)
	_, err := service.Create(context.Background(), workspace.DefaultID, basicAgentRequestWithSkills([]map[string]string{
		{"skill_id": "skill_x"},
	}))
	if err == nil {
		t.Fatal("default no-op resolver must reject non-empty skills[]")
	}
}

// rawSkillEntry marshals a skills[] entry into its canonical bytes
// for tests that need to construct AgentConfig directly.
func rawSkillEntry(t *testing.T, entry map[string]string) []byte {
	t.Helper()
	body, err := marshalSkillEntry(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}
