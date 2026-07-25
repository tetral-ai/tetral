package agent_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type serviceSkillResolver struct {
	err   error
	calls int
	refs  [][]agent.SkillReference
}

func (resolver *serviceSkillResolver) ValidateAgentSkillReferences(_ context.Context, tx agent.Transaction, _ string, refs []agent.SkillReference) error {
	if tx == nil {
		return errors.New("skill resolver received nil transaction")
	}
	resolver.calls++
	copied := append([]agent.SkillReference(nil), refs...)
	resolver.refs = append(resolver.refs, copied)
	return resolver.err
}

func newAgentServiceEnv(t *testing.T) (*sql.DB, *agent.Service, *serviceSkillResolver) {
	t.Helper()
	runtime, admin := newPGAgentEnv(t)
	resolver := &serviceSkillResolver{}
	service := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtime)), resolver)
	return admin, service, resolver
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func countAgentVersions(t *testing.T, db *sql.DB, agentID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agent_versions WHERE agent_id = $1`, agentID,
	).Scan(&count); err != nil {
		t.Fatalf("count agent_versions: %v", err)
	}
	return count
}

func serviceCreateRequest() agent.CreateAgentRequest {
	return agent.CreateAgentRequest{AgentConfig: agent.AgentConfig{
		Name:  "service-agent",
		Model: "anthropic/claude-opus-4-8",
		Tools: basicAgentTools(),
	}}
}

func serviceStringPointer(value string) *string {
	return &value
}

func TestAgentServiceCreatePersistsCurrentAndVersionSnapshot(t *testing.T) {
	admin, service, resolver := newAgentServiceEnv(t)
	ctx := context.Background()

	created, err := service.Create(ctx, workspace.DefaultID, serviceCreateRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !strings.HasPrefix(created.ID, "agent_") {
		t.Fatalf("ID = %q; want agent_ prefix", created.ID)
	}
	if created.Type != "agent" {
		t.Fatalf("Type = %q; want agent", created.Type)
	}
	if created.Version != 1 {
		t.Fatalf("Version = %d; want 1", created.Version)
	}
	if created.ArchivedAt != nil {
		t.Fatalf("ArchivedAt = %v; want nil", created.ArchivedAt)
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatalf("timestamps must be non-zero: created_at=%s updated_at=%s", created.CreatedAt, created.UpdatedAt)
	}
	if created.CreatedAt.Location() != time.UTC || created.UpdatedAt.Location() != time.UTC {
		t.Fatalf("timestamps must be UTC: created_at=%s updated_at=%s", created.CreatedAt.Location(), created.UpdatedAt.Location())
	}
	if !created.CreatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("create timestamps differ: created_at=%s updated_at=%s", created.CreatedAt, created.UpdatedAt)
	}
	if len(created.Tools) != 1 || len(created.MCPServers) != 0 || len(created.Skills) != 0 || len(created.Metadata) != 0 {
		t.Fatalf("required tools and empty optional collections not normalized: tools=%d mcp=%d skills=%d metadata=%d", len(created.Tools), len(created.MCPServers), len(created.Skills), len(created.Metadata))
	}
	if count := countRows(t, admin, "agents"); count != 1 {
		t.Fatalf("agents row count = %d; want 1", count)
	}
	if count := countRows(t, admin, "agent_versions"); count != 1 {
		t.Fatalf("agent_versions row count = %d; want 1", count)
	}
	if resolver.calls != 1 {
		t.Fatalf("skill resolver calls = %d; want 1", resolver.calls)
	}

	got, err := service.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID || got.Name != created.Name || got.ArchivedAt != nil {
		t.Fatalf("Get returned %+v; want created agent with nil archived_at", got)
	}
	if !got.CreatedAt.Equal(created.CreatedAt) || !got.UpdatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("Get timestamps changed: got created_at=%s updated_at=%s want created_at=%s updated_at=%s", got.CreatedAt, got.UpdatedAt, created.CreatedAt, created.UpdatedAt)
	}
}

func TestAgentServiceUpdateNoOpDoesNotAppendVersion(t *testing.T) {
	admin, service, _ := newAgentServiceEnv(t)
	ctx := context.Background()

	created, err := service.Create(ctx, workspace.DefaultID, serviceCreateRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	updated, err := service.Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{
		Version:     created.Version,
		AgentConfig: created.AgentConfig,
	})
	if err != nil {
		t.Fatalf("Update no-op: %v", err)
	}
	if updated.Version != created.Version {
		t.Fatalf("no-op version = %d; want %d", updated.Version, created.Version)
	}
	if !updated.UpdatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("no-op updated_at changed: got %s want %s", updated.UpdatedAt, created.UpdatedAt)
	}
	if count := countAgentVersions(t, admin, created.ID); count != 1 {
		t.Fatalf("agent_versions count = %d; want 1", count)
	}
}

func TestAgentServiceUpdateChangedSnapshotAppendsVersion(t *testing.T) {
	admin, service, _ := newAgentServiceEnv(t)
	ctx := context.Background()

	created, err := service.Create(ctx, workspace.DefaultID, serviceCreateRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	target := created.AgentConfig
	target.Name = "renamed-agent"
	updated, err := service.Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{
		Version:     created.Version,
		AgentConfig: target,
	})
	if err != nil {
		t.Fatalf("Update changed: %v", err)
	}
	if updated.Version != 2 {
		t.Fatalf("changed version = %d; want 2", updated.Version)
	}
	if updated.Name != "renamed-agent" {
		t.Fatalf("Name = %q; want renamed-agent", updated.Name)
	}
	if count := countAgentVersions(t, admin, created.ID); count != 2 {
		t.Fatalf("agent_versions count = %d; want 2", count)
	}
}

func TestAgentServiceArchiveIsIdempotentAndRejectsLaterUpdates(t *testing.T) {
	admin, service, _ := newAgentServiceEnv(t)
	ctx := context.Background()

	created, err := service.Create(ctx, workspace.DefaultID, serviceCreateRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	first, err := service.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive first: %v", err)
	}
	if first.ArchivedAt == nil {
		t.Fatal("first archive returned nil archived_at")
	}
	second, err := service.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive second: %v", err)
	}
	if second.Version != first.Version {
		t.Fatalf("second archive bumped version: got %d want %d", second.Version, first.Version)
	}
	if !second.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("second archive changed updated_at: got %s want %s", second.UpdatedAt, first.UpdatedAt)
	}
	if !second.ArchivedAt.Equal(*first.ArchivedAt) {
		t.Fatalf("second archive changed archived_at: got %s want %s", second.ArchivedAt, first.ArchivedAt)
	}

	target := created.AgentConfig
	target.Name = "must-not-write"
	_, err = service.Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{
		Version:     first.Version,
		AgentConfig: target,
	})
	if err == nil {
		t.Fatal("Update of archived agent must fail")
	}
	var validation *agent.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Update archived error = %T (%v); want ValidationError", err, err)
	}
	if count := countAgentVersions(t, admin, created.ID); count != 1 {
		t.Fatalf("agent_versions count after archived update = %d; want 1", count)
	}
}

func TestAgentServiceGetVersionReturnsFullHistoricalSnapshotWithLifecycle(t *testing.T) {
	admin, service, _ := newAgentServiceEnv(t)
	ctx := context.Background()

	created, err := service.Create(ctx, workspace.DefaultID, serviceCreateRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	versionOneBytes := agentVersionConfigJSON(t, admin, created.ID, created.Version)

	target := created.AgentConfig
	target.System = serviceStringPointer("new system")
	updated, err := service.Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{
		Version:     created.Version,
		AgentConfig: target,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	archived, err := service.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}

	historical, err := service.GetVersion(ctx, workspace.DefaultID, created.ID, created.Version)
	if err != nil {
		t.Fatalf("GetVersion v1: %v", err)
	}
	if historical.ID != created.ID || historical.Type != "agent" || historical.Version != 1 {
		t.Fatalf("historical identity = %+v; want full v1 agent", historical)
	}
	if historical.System != nil {
		t.Fatalf("historical v1 system = %q; want nil", *historical.System)
	}
	if historical.ArchivedAt == nil || !historical.ArchivedAt.Equal(*archived.ArchivedAt) {
		t.Fatalf("historical archived_at = %v; want current lifecycle %v", historical.ArchivedAt, archived.ArchivedAt)
	}

	latestVersion, err := service.GetVersion(ctx, workspace.DefaultID, created.ID, updated.Version)
	if err != nil {
		t.Fatalf("GetVersion v2: %v", err)
	}
	if latestVersion.System == nil || *latestVersion.System != "new system" {
		t.Fatalf("v2 system = %v; want new system", latestVersion.System)
	}
	if got := agentVersionConfigJSON(t, admin, created.ID, created.Version); got != versionOneBytes {
		t.Fatalf("historical config_json bytes changed after update:\n got:  %q\n want: %q", got, versionOneBytes)
	}
	_, err = service.GetVersion(ctx, workspace.DefaultID, created.ID, 999)
	var notFound *agent.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("missing version error = %T (%v); want NotFoundError", err, err)
	}
	_, err = service.GetVersion(ctx, workspace.DefaultID, "agent_missing", 1)
	if !errors.As(err, &notFound) {
		t.Fatalf("missing agent version error = %T (%v); want NotFoundError", err, err)
	}
}

func TestAgentServiceValidationFailureRollsBackCreate(t *testing.T) {
	admin, service, resolver := newAgentServiceEnv(t)
	ctx := context.Background()

	request := serviceCreateRequest()
	request.Model = ""
	_, err := service.Create(ctx, workspace.DefaultID, request)
	if err == nil {
		t.Fatal("Create with invalid model must fail")
	}
	if resolver.calls != 0 {
		t.Fatalf("skill resolver calls = %d; want 0 before validation rollback", resolver.calls)
	}
	if count := countRows(t, admin, "agents"); count != 0 {
		t.Fatalf("agents row count = %d; want 0", count)
	}
	if count := countRows(t, admin, "agent_versions"); count != 0 {
		t.Fatalf("agent_versions row count = %d; want 0", count)
	}
}

func TestAgentServiceValidationFailureRollsBackUpdate(t *testing.T) {
	admin, service, resolver := newAgentServiceEnv(t)
	ctx := context.Background()

	created, err := service.Create(ctx, workspace.DefaultID, serviceCreateRequest())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	resolver.calls = 0

	invalid := created.AgentConfig
	invalid.Name = ""
	_, err = service.Update(ctx, workspace.DefaultID, created.ID, agent.UpdateAgentRequest{
		Version:     created.Version,
		AgentConfig: invalid,
	})
	if err == nil {
		t.Fatal("Update with invalid name must fail")
	}
	if resolver.calls != 0 {
		t.Fatalf("skill resolver calls = %d; want 0 before validation rollback", resolver.calls)
	}
	if count := countAgentVersions(t, admin, created.ID); count != 1 {
		t.Fatalf("agent_versions count after invalid update = %d; want 1", count)
	}
	got, err := service.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get after invalid update: %v", err)
	}
	if got.Name != created.Name || got.Version != created.Version {
		t.Fatalf("invalid update changed agent: got name=%q version=%d want name=%q version=%d", got.Name, got.Version, created.Name, created.Version)
	}
}

func TestAgentServiceSkillValidationFailureRollsBackWrite(t *testing.T) {
	admin, service, resolver := newAgentServiceEnv(t)
	ctx := context.Background()
	resolver.err = &agent.ValidationError{Message: "skill reference rejected"}

	request := serviceCreateRequest()
	request.Skills = agent.RawArray{json.RawMessage(`{"type":"custom","skill_id":"skill_missing","version":"latest"}`)}
	_, err := service.Create(ctx, workspace.DefaultID, request)
	if err == nil {
		t.Fatal("Create with rejected skill reference must fail")
	}
	if count := countRows(t, admin, "agents"); count != 0 {
		t.Fatalf("agents row count = %d; want 0", count)
	}
	if count := countRows(t, admin, "agent_versions"); count != 0 {
		t.Fatalf("agent_versions row count = %d; want 0", count)
	}
}

func agentVersionConfigJSON(t *testing.T, db *sql.DB, agentID string, version int) string {
	t.Helper()
	var configJSON string
	if err := db.QueryRowContext(context.Background(),
		`SELECT config_json FROM agent_versions WHERE agent_id = $1 AND version = $2`,
		agentID, version,
	).Scan(&configJSON); err != nil {
		t.Fatalf("read agent version config_json: %v", err)
	}
	return configJSON
}
