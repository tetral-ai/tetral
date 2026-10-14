package integration

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testAPIKey = "test-api-key-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// The lifecycle proof uses uploaded packages and public/service calls throughout.
// Saved Session bindings, Sandbox's real index and retained ZIP bytes must agree
// across new versions, individual version deletion and parent deletion.
func TestSkillUploadAgentReferenceLifecycleEndToEnd(t *testing.T) {
	ctx := context.Background()
	ws := workspace.DefaultID
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	if err := auth.RefreshBootstrap(ctx, auth.NewAPIKeyStore(runtimeDB), ws, testAPIKey); err != nil {
		t.Fatal(err)
	}
	blobs := blob.NewFakeBlobStore()
	skills := skill.NewService(skill.NewPostgreSQLStore(client, blobs), skill.WithPackageStageDir(t.TempDir()))
	agents := agent.NewService(agent.NewPostgreSQLAgentStore(client), skills)
	environments := environment.NewService(environment.NewPostgreSQLEnvironmentStore(client, environment.WithDefaultArtifactRef("artifact_skill_e2e")))
	env, err := environments.Create(ctx, ws, environment.CreateEnvironmentRequest{Name: "skill-e2e", Config: environment.EnvironmentConfig{Type: "cloud"}})
	if err != nil {
		t.Fatal(err)
	}
	sessionStore := session.NewPostgreSQLSessionStore(client)
	sessions := session.NewService(agents, environments, nil, nil, nil, sessionStore, nil, session.WithSkillResolver(skills))
	router := httpapi.NewRouter(httpapi.NewSessionHandler(sessions), "",
		httpapi.WithAuthenticator(auth.AuthenticatorFunc(auth.NewAuthorityResolver(runtimeDB, "", nil).AuthenticateKey)),
		httpapi.WithAgentHandler(httpapi.NewAgentHandler(agents)),
		httpapi.WithSkillHandler(httpapi.NewSkillHandler(skills, t.TempDir())),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	readResponse := func(resp *http.Response, status int) []byte {
		t.Helper()
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != status {
			t.Fatalf("HTTP status=%d want=%d body=%s error=%v", resp.StatusCode, status, raw, err)
		}
		return raw
	}
	request := func(method, path string, body []byte, status int) []byte {
		t.Helper()
		return readResponse(doJSON(t, method, server.URL+path, body, testAPIKey), status)
	}
	const name = "e2e-skill"
	upload := func(path, description string) []byte {
		t.Helper()
		body, contentType := newSkillUpload(t, name, description)
		return readResponse(doMultipart(t, http.MethodPost, server.URL+path, contentType, body, testAPIKey), http.StatusOK)
	}
	var parent skill.Skill
	if err := json.Unmarshal(upload("/v1/skills?beta=true", "Version one."), &parent); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(parent.LatestVersionID, "skver_") || parent.DisplayName != name {
		t.Fatalf("uploaded parent = %+v", parent)
	}
	type createdAgent struct {
		ID      string          `json:"id"`
		Version int             `json:"version"`
		Skills  json.RawMessage `json:"skills"`
	}
	createAgent := func(agentName, version string) (createdAgent, []byte) {
		t.Helper()
		versionField := ""
		if version != "" {
			versionField = fmt.Sprintf(",\"version\":%q", version)
		}
		body := []byte(fmt.Sprintf(`{"name":%q,"model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}],"skills":[{"type":"custom","skill_id":%q%s}]}`, agentName, parent.ID, versionField))
		var created createdAgent
		if err := json.Unmarshal(request(http.MethodPost, "/v1/agents?beta=true", body, http.StatusOK), &created); err != nil {
			t.Fatal(err)
		}
		return created, body
	}
	newSession := func(a createdAgent) session.Response {
		t.Helper()
		var created session.Response
		body := []byte(fmt.Sprintf(`{"agent":{"type":"agent","id":%q,"version":%d},"environment_id":%q,"vault_ids":[]}`, a.ID, a.Version, env.ID))
		if err := json.Unmarshal(request(http.MethodPost, "/v1/sessions?beta=true", body, http.StatusOK), &created); err != nil {
			t.Fatal(err)
		}
		return created
	}
	checkSession := func(sessionID, version, description string) {
		t.Helper()
		saved, err := sessionStore.Get(ctx, ws, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if len(saved.ResolvedSkills) != 1 || saved.ResolvedSkills[0] != (session.ResolvedSkillReference{Type: "custom", SkillID: parent.ID, Version: version}) {
			t.Fatalf("saved bindings = %+v", saved.ResolvedSkills)
		}
		expected := fmt.Sprintf(`{"type":"custom","skill_id":%q,"version":%q}`, parent.ID, version)
		var public session.Response
		if err := json.Unmarshal(request(http.MethodGet, "/v1/sessions/"+sessionID+"?beta=true", nil, http.StatusOK), &public); err != nil {
			t.Fatal(err)
		}
		if len(public.Agent.Skills) != 1 || string(public.Agent.Skills[0]) != expected {
			t.Fatalf("Session skills = %s want %s", public.Agent.Skills, expected)
		}
		threads, err := sessions.ListThreads(ctx, ws, sessionID, session.ThreadListOptions{})
		if err != nil || len(threads.Data) != 1 {
			t.Fatalf("threads = %+v, %v", threads, err)
		}
		thread, err := sessions.GetThread(ctx, ws, sessionID, threads.Data[0].ID)
		if err != nil || len(thread.Agent.Skills) != 1 || string(thread.Agent.Skills[0]) != expected {
			t.Fatalf("Thread skills = %+v, %v", thread, err)
		}
		var blobKey string
		if err := client.WithWorkspaceTx(ctx, string(ws), "skill.e2e.read", func(tx *dbconnect.Tx) error {
			index, err := sandbox.ResolveSessionSkillIndex(ctx, tx, ws, sessionID)
			if err != nil {
				return err
			}
			if len(index) != 1 || index[0].Version != version || index[0].SkillVersionID != version || index[0].Directory != name {
				t.Fatalf("Sandbox index = %+v", index)
			}
			return tx.QueryRow(ctx, `SELECT blob_key FROM skill_versions WHERE workspace_id=$1 AND skill_id=$2 AND version=$3`, string(ws), parent.ID, version).Scan(&blobKey)
		}); err != nil {
			t.Fatal(err)
		}
		reader, err := blobs.Get(ctx, blobKey)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, file := range archive.File {
			if file.Name != name+"/SKILL.md" {
				continue
			}
			content, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(content)
			_ = content.Close()
			if err != nil {
				t.Fatal(err)
			}
			want := "---\nname: " + name + "\ndescription: " + description + "\n---\nBody.\n"
			if string(body) != want {
				t.Fatalf("bound package body=%q want=%q", body, want)
			}
			found = true
		}
		if !found {
			t.Fatal("bound package lacks uploaded SKILL.md")
		}
	}
	pinned, pinnedBody := createAgent("pinned-agent", parent.LatestVersionID)
	pinnedSession := newSession(pinned)
	checkSession(pinnedSession.ID, parent.LatestVersionID, "Version one.")
	latest, latestBody := createAgent("latest-agent", "")
	sessionA := newSession(latest)
	var v2 skill.SkillVersion
	if err := json.Unmarshal(upload("/v1/skills/"+parent.ID+"/versions?beta=true", "Version two."), &v2); err != nil {
		t.Fatal(err)
	}
	checkSession(sessionA.ID, parent.LatestVersionID, "Version one.")
	sessionB := newSession(latest)
	checkSession(sessionB.ID, v2.ID, "Version two.")
	request(http.MethodDelete, "/v1/skills/"+parent.ID+"/versions/"+v2.ID+"?beta=true", nil, http.StatusOK)
	checkSession(sessionB.ID, v2.ID, "Version two.")
	sessionC := newSession(latest)
	checkSession(sessionC.ID, parent.LatestVersionID, "Version one.")
	request(http.MethodDelete, "/v1/skills/"+parent.ID+"?beta=true", nil, http.StatusOK)
	checkSession(pinnedSession.ID, parent.LatestVersionID, "Version one.")
	checkSession(sessionA.ID, parent.LatestVersionID, "Version one.")
	checkSession(sessionB.ID, v2.ID, "Version two.")
	checkSession(sessionC.ID, parent.LatestVersionID, "Version one.")
	assertRejected := func(raw []byte) {
		t.Helper()
		var failure struct {
			Error struct{ Type, Message string } `json:"error"`
		}
		if err := json.Unmarshal(raw, &failure); err != nil {
			t.Fatal(err)
		}
		if failure.Error.Type != "invalid_request_error" || !strings.Contains(failure.Error.Message, parent.ID) {
			t.Fatalf("wrong rejection: %s", raw)
		}
	}
	for _, entry := range []struct {
		a    createdAgent
		body []byte
	}{{pinned, pinnedBody}, {latest, latestBody}} {
		var historical createdAgent
		if err := json.Unmarshal(request(http.MethodGet, "/v1/agents/"+entry.a.ID+"?beta=true", nil, http.StatusOK), &historical); err != nil {
			t.Fatal(err)
		}
		if string(historical.Skills) != string(entry.a.Skills) {
			t.Fatalf("historical Agent skills changed: %s -> %s", entry.a.Skills, historical.Skills)
		}
		assertRejected(request(http.MethodPost, "/v1/agents?beta=true", entry.body, http.StatusBadRequest))
		update := append([]byte(fmt.Sprintf(`{"version":%d,`, entry.a.Version)), entry.body[1:]...)
		assertRejected(request(http.MethodPost, "/v1/agents/"+entry.a.ID+"?beta=true", update, http.StatusBadRequest))
		body := []byte(fmt.Sprintf(`{"agent":{"type":"agent","id":%q,"version":%d},"environment_id":%q,"vault_ids":[]}`, entry.a.ID, entry.a.Version, env.ID))
		assertRejected(request(http.MethodPost, "/v1/sessions?beta=true", body, http.StatusBadRequest))
	}
}

func TestAgentSkillReferenceTransactionsSerializeConcurrentSkillDelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *agent.Service, *agent.Service, workspace.ID, string) (*agent.Agent, error)
	}{
		{
			name: "create",
			run: func(ctx context.Context, _ *agent.Service, store *agent.Service, ws workspace.ID, skillID string) (*agent.Agent, error) {
				return store.Create(ctx, ws, agentCreateRequestWithSkill("lock-create", skillID))
			},
		},
		{
			name: "update",
			run: func(ctx context.Context, setupStore *agent.Service, store *agent.Service, ws workspace.ID, skillID string) (*agent.Agent, error) {
				created, err := setupStore.Create(ctx, ws, agentCreateRequest("lock-update"))
				if err != nil {
					return nil, fmt.Errorf("setup Create: %w", err)
				}
				return store.Update(ctx, ws, created.ID, agent.UpdateAgentRequest{
					Version:     created.Version,
					AgentConfig: agentConfigWithSkill("lock-update-renamed", skillID),
				})
			},
		},
		{
			name: "noop_update",
			run: func(ctx context.Context, setupStore *agent.Service, store *agent.Service, ws workspace.ID, skillID string) (*agent.Agent, error) {
				created, err := setupStore.Create(ctx, ws, agentCreateRequestWithSkill("lock-noop", skillID))
				if err != nil {
					return nil, fmt.Errorf("setup Create with Skill: %w", err)
				}
				return store.Update(ctx, ws, created.ID, agent.UpdateAgentRequest{
					Version:     created.Version,
					AgentConfig: created.AgentConfig,
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtimeDB, adminDB, skillStore, stageDir := newAgentSkillStoreIntegrationEnvWithAdmin(t)
			skillService := skill.NewService(skillStore, skill.WithPackageStageDir(stageDir))
			ws := workspace.DefaultID
			name := strings.ReplaceAll(tc.name, "_", "-") + "-skill"
			createdSkill := createIntegrationSkill(t, skillStore, stageDir, name)
			var budget skill.UploadBudget
			part, err := skill.StageUploadPart(context.Background(), strings.NewReader("---\nname: "+name+"\ndescription: Second version.\n---\nBody.\n"), stageDir, name+"/SKILL.md", &budget)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = skill.CleanupStagedUploadParts([]skill.StagedUploadPart{part}) }()
			if _, err := skillService.CreateVersion(context.Background(), ws, createdSkill.ID, skill.CreateVersionInput{Files: []skill.StagedUploadPart{part}}); err != nil {
				t.Fatal(err)
			}

			resolver := newBlockingSkillReferenceResolver(skillService)
			defer resolver.release()
			setupStore := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), skillService)
			agentService := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), resolver)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			agentDone := make(chan struct {
				agent *agent.Agent
				err   error
			}, 1)
			go func() {
				a, err := tc.run(ctx, setupStore, agentService, ws, createdSkill.ID)
				agentDone <- struct {
					agent *agent.Agent
					err   error
				}{agent: a, err: err}
			}()

			waitForSignal(t, resolver.locked, "real Skill reference resolver to acquire the registry lock")

			deleteDone := make(chan error, 1)
			go func() {
				deleteDone <- skillStore.DeleteVersion(context.Background(), ws, createdSkill.ID, createdSkill.LatestVersionID)
			}()
			waitForBlockedSkillRegistryLock(t, adminDB, ws)
			select {
			case err := <-deleteDone:
				resolver.release()
				t.Fatalf("DeleteVersion completed before the Agent transaction released the shared Skill-registry lock: %v", err)
			default:
			}

			resolver.release()
			opResult := waitForAgentResult(t, agentDone)
			if opResult.err != nil {
				t.Fatalf("Agent %s: %v", tc.name, opResult.err)
			}
			if opResult.agent == nil {
				t.Fatalf("Agent %s returned nil agent", tc.name)
			}
			if len(opResult.agent.Skills) != 1 {
				t.Fatalf("Agent %s persisted %d skills; want 1", tc.name, len(opResult.agent.Skills))
			}

			waitForDeleteResult(t, deleteDone)
		})
	}
}

func TestAgentSkillReferenceTransactionsSerializeConcurrentSkillParentDelete(t *testing.T) {
	for _, tc := range []struct {
		name      string
		agentName string
	}{
		{name: "create", agentName: "parent-delete-create"},
		{name: "update", agentName: "parent-delete-update"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtimeDB, adminDB, skillStore, stageDir := newAgentSkillStoreIntegrationEnvWithAdmin(t)
			skillService := skill.NewService(skillStore, skill.WithPackageStageDir(stageDir))
			ws := workspace.DefaultID
			createdSkill := createIntegrationSkill(t, skillStore, stageDir, "parent-delete-"+tc.name+"-skill")

			resolver := newBlockingBeforeValidationSkillReferenceResolver(skillService)
			defer resolver.release()
			setupStore := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), skillService)
			agentService := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), resolver)

			var setupAgent *agent.Agent
			var beforeVersions int
			if tc.name == "update" {
				var err error
				setupAgent, err = setupStore.Create(context.Background(), ws, agentCreateRequest("parent-delete-update"))
				if err != nil {
					t.Fatalf("setup Create: %v", err)
				}
				beforeVersions = countAgentVersions(t, adminDB, ws, setupAgent.ID)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			agentDone := make(chan struct {
				agent *agent.Agent
				err   error
			}, 1)
			go func() {
				var (
					a   *agent.Agent
					err error
				)
				if tc.name == "update" {
					a, err = agentService.Update(ctx, ws, setupAgent.ID, agent.UpdateAgentRequest{
						Version:     setupAgent.Version,
						AgentConfig: agentConfigWithSkill("parent-delete-update-renamed", createdSkill.ID),
					})
				} else {
					a, err = agentService.Create(ctx, ws, agentCreateRequestWithSkill(tc.agentName, createdSkill.ID))
				}
				agentDone <- struct {
					agent *agent.Agent
					err   error
				}{agent: a, err: err}
			}()

			waitForSignal(t, resolver.locked, "Agent Skill resolver to acquire the registry lock before validation")

			deleteDone := make(chan error, 1)
			go func() {
				deleteDone <- skillStore.DeleteSkill(context.Background(), ws, createdSkill.ID)
			}()
			waitForBlockedSkillRegistryLock(t, adminDB, ws)
			select {
			case err := <-deleteDone:
				resolver.release()
				t.Fatalf("DeleteSkill completed before the Agent transaction released the shared Skill-registry lock: %v", err)
			default:
			}

			resolver.release()
			opResult := waitForAgentResult(t, agentDone)
			if opResult.err != nil || opResult.agent == nil {
				t.Fatalf("Agent %s admitted before deletion: agent=%+v error=%v", tc.name, opResult.agent, opResult.err)
			}
			if tc.name == "update" {
				if got := countAgentVersions(t, adminDB, ws, setupAgent.ID); got != beforeVersions+1 {
					t.Fatalf("Update left %d agent_versions; want %d", got, beforeVersions+1)
				}
			} else if got := countAgentVersionsByName(t, adminDB, ws, tc.agentName); got != 1 {
				t.Fatalf("Create inserted %d agent_versions; want 1", got)
			}

			waitForDeleteResult(t, deleteDone)
		})
	}
}

func TestAgentSkillReferenceResolverDoesNotRequireRuntime(t *testing.T) {
	runtimeDB, adminDB, skillStore, _ := newAgentSkillStoreIntegrationEnvWithAdmin(t)
	skillService := skill.NewService(skillStore)
	ws := workspace.DefaultID
	seededSkillID := seedActiveSkill(t, adminDB, ws)
	agentService := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), skillService)

	created, err := agentService.Create(context.Background(), ws, agent.CreateAgentRequest{AgentConfig: agent.AgentConfig{
		Name:   "runtime-neutral-skill-ref",
		Model:  "anthropic/claude-opus-4-8",
		Tools:  agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		Skills: agent.RawArray{json.RawMessage(fmt.Sprintf(`{"type":"custom","skill_id":%q}`, seededSkillID))},
	}})
	if err != nil {
		t.Fatalf("Create with runtime-neutral and active Skill ref: %v", err)
	}
	if len(created.Skills) != 1 || string(created.Skills[0]) != fmt.Sprintf(`{"type":"custom","skill_id":%q,"version":"latest"}`, seededSkillID) {
		t.Fatalf("created skills = %s; want defaulted latest ref", created.Skills)
	}
}

func TestAgentRejectsLatestReferenceWhenCurrentSkillVersionHidden(t *testing.T) {
	runtimeDB, adminDB, skillStore, stageDir := newAgentSkillStoreIntegrationEnvWithAdmin(t)
	skillService := skill.NewService(skillStore, skill.WithPackageStageDir(stageDir))
	ws := workspace.DefaultID
	createdSkill := createIntegrationSkill(t, skillStore, stageDir, "stale-latest")
	agentService := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), skillService)

	createdAgent, err := agentService.Create(context.Background(), ws, agentCreateRequestWithSkill("stale-latest-existing", createdSkill.ID))
	if err != nil {
		t.Fatalf("setup Create with active Skill: %v", err)
	}
	beforeVersions := countAgentVersions(t, adminDB, ws, createdAgent.ID)

	if _, err := adminDB.ExecContext(context.Background(),
		`UPDATE skill_versions SET deleted_at = $1
		  WHERE workspace_id = $2 AND skill_id = $3 AND version = $4`,
		"2026-04-30T00:00:00Z", string(ws), createdSkill.ID, createdSkill.LatestVersionID,
	); err != nil {
		t.Fatalf("hide current Skill version: %v", err)
	}

	_, err = agentService.Create(context.Background(), ws, agentCreateRequestWithSkill("stale-latest-create", createdSkill.ID))
	assertAgentValidationError(t, err)
	if got := countAgentVersionsByName(t, adminDB, ws, "stale-latest-create"); got != 0 {
		t.Fatalf("rejected Create inserted %d agent_versions; want 0", got)
	}

	_, err = agentService.Update(context.Background(), ws, createdAgent.ID, agent.UpdateAgentRequest{
		Version:     createdAgent.Version,
		AgentConfig: createdAgent.AgentConfig,
	})
	assertAgentValidationError(t, err)
	if got := countAgentVersions(t, adminDB, ws, createdAgent.ID); got != beforeVersions {
		t.Fatalf("rejected no-op Update left %d agent_versions; want %d", got, beforeVersions)
	}

	_, err = agentService.Update(context.Background(), ws, createdAgent.ID, agent.UpdateAgentRequest{
		Version:     createdAgent.Version,
		AgentConfig: agentConfigWithSkill("stale-latest-update", createdSkill.ID),
	})
	assertAgentValidationError(t, err)
	if got := countAgentVersions(t, adminDB, ws, createdAgent.ID); got != beforeVersions {
		t.Fatalf("rejected Update left %d agent_versions; want %d", got, beforeVersions)
	}
}

func TestAgentRejectsConcreteReferenceWhenPinnedSkillVersionHidden(t *testing.T) {
	runtimeDB, adminDB, skillStore, stageDir := newAgentSkillStoreIntegrationEnvWithAdmin(t)
	skillService := skill.NewService(skillStore, skill.WithPackageStageDir(stageDir))
	ws := workspace.DefaultID
	createdSkill := createIntegrationSkill(t, skillStore, stageDir, "stale-concrete")
	pinnedVersion := createdSkill.LatestVersionID
	agentService := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), skillService)

	createdAgent, err := agentService.Create(context.Background(), ws, agentCreateRequestWithSkillVersion("stale-concrete-existing", createdSkill.ID, pinnedVersion))
	if err != nil {
		t.Fatalf("setup Create with active concrete Skill version: %v", err)
	}
	beforeVersions := countAgentVersions(t, adminDB, ws, createdAgent.ID)

	if _, err := adminDB.ExecContext(context.Background(),
		`UPDATE skill_versions SET deleted_at = $1
		  WHERE workspace_id = $2 AND skill_id = $3 AND version = $4`,
		"2026-04-30T00:00:00Z", string(ws), createdSkill.ID, pinnedVersion,
	); err != nil {
		t.Fatalf("hide pinned Skill version: %v", err)
	}

	_, err = agentService.Create(context.Background(), ws, agentCreateRequestWithSkillVersion("stale-concrete-create", createdSkill.ID, pinnedVersion))
	assertAgentValidationError(t, err)
	if got := countAgentVersionsByName(t, adminDB, ws, "stale-concrete-create"); got != 0 {
		t.Fatalf("rejected Create inserted %d agent_versions; want 0", got)
	}

	_, err = agentService.Update(context.Background(), ws, createdAgent.ID, agent.UpdateAgentRequest{
		Version:     createdAgent.Version,
		AgentConfig: createdAgent.AgentConfig,
	})
	assertAgentValidationError(t, err)
	if got := countAgentVersions(t, adminDB, ws, createdAgent.ID); got != beforeVersions {
		t.Fatalf("rejected no-op Update left %d agent_versions; want %d", got, beforeVersions)
	}

	_, err = agentService.Update(context.Background(), ws, createdAgent.ID, agent.UpdateAgentRequest{
		Version:     createdAgent.Version,
		AgentConfig: agentConfigWithSkillVersion("stale-concrete-update", createdSkill.ID, pinnedVersion),
	})
	assertAgentValidationError(t, err)
	if got := countAgentVersions(t, adminDB, ws, createdAgent.ID); got != beforeVersions {
		t.Fatalf("rejected Update left %d agent_versions; want %d", got, beforeVersions)
	}
}

func assertAgentValidationError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected validation error")
	}
	var validationErr *agent.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected *agent.ValidationError; got %T (%v)", err, err)
	}
}

// newSkillUpload builds an Anthropic-compatible multipart files[] body
// for POST /v1/skills.
func newSkillUpload(t *testing.T, name, description string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	w, err := mw.CreateFormFile("files[]", name+"/SKILL.md")
	if err != nil {
		t.Fatalf("create SKILL.md file part: %v", err)
	}
	body := []byte("---\nname: " + name + "\ndescription: " + description + "\n---\nBody.\n")
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write SKILL.md file part: %v", err)
	}
	w, err = mw.CreateFormFile("files[]", name+"/README.md")
	if err != nil {
		t.Fatalf("create README file part: %v", err)
	}
	if _, err := w.Write([]byte("# " + name + "\n")); err != nil {
		t.Fatalf("write README file part: %v", err)
	}
	_ = mw.Close()
	return &buf, mw.FormDataContentType()
}

func newAgentSkillStoreIntegrationEnvWithAdmin(t *testing.T) (*sql.DB, *sql.DB, *skill.PostgreSQLSkillStore, string) {
	t.Helper()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	skillStore := skill.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB), blob.NewFakeBlobStore())
	return runtimeDB, adminDB, skillStore, t.TempDir()
}

func waitForBlockedSkillRegistryLock(t *testing.T, adminDB *sql.DB, ws workspace.ID) {
	t.Helper()
	const skillRegistryAdvisoryLockCategory = uint32(0x736B_696C)
	resource := workspaceSkillRegistryLockResourceForTest(string(ws))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := adminDB.QueryRowContext(context.Background(),
			`SELECT count(*)
			   FROM pg_locks
			  WHERE locktype = 'advisory'
			    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			    AND mode = 'ExclusiveLock'
			    AND granted = false
			    AND classid = $1::oid
			    AND objid = $2::oid
			    AND objsubid = 2`,
			int64(skillRegistryAdvisoryLockCategory), int64(resource),
		).Scan(&waiting); err != nil {
			t.Fatalf("query blocked advisory lock: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for DeleteSkill to block on the workspace Skill-registry advisory lock")
}

func workspaceSkillRegistryLockResourceForTest(workspaceID string) uint32 {
	sum := sha256.Sum256([]byte(workspaceID))
	return binary.BigEndian.Uint32(sum[:4])
}

func createIntegrationSkill(t *testing.T, store *skill.PostgreSQLSkillStore, stageDir, name string) *skill.Skill {
	t.Helper()
	var budget skill.UploadBudget
	frontmatter := []byte("---\nname: " + name + "\ndescription: Integration Skill.\n---\nBody.\n")
	staged, err := skill.StageUploadPart(context.Background(), bytes.NewReader(frontmatter), stageDir, name+"/SKILL.md", &budget)
	if err != nil {
		t.Fatalf("StageUploadPart SKILL.md: %v", err)
	}
	parts := []skill.StagedUploadPart{staged}
	t.Cleanup(func() { _ = skill.CleanupStagedUploadParts(parts) })
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir))
	created, err := service.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Files: parts})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	return created
}

type blockingSkillReferenceResolver struct {
	real    *skill.Service
	locked  chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func newBlockingSkillReferenceResolver(real *skill.Service) *blockingSkillReferenceResolver {
	return &blockingSkillReferenceResolver{
		real:    real,
		locked:  make(chan struct{}),
		unblock: make(chan struct{}),
	}
}

func (b *blockingSkillReferenceResolver) ValidateAgentSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) error {
	if err := b.real.ValidateAgentSkillReferences(ctx, tx, workspaceID, refs); err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	b.once.Do(func() { close(b.locked) })
	select {
	case <-b.unblock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingSkillReferenceResolver) release() {
	defer func() { _ = recover() }()
	close(b.unblock)
}

type blockingBeforeValidationSkillReferenceResolver struct {
	real    *skill.Service
	locked  chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func newBlockingBeforeValidationSkillReferenceResolver(real *skill.Service) *blockingBeforeValidationSkillReferenceResolver {
	return &blockingBeforeValidationSkillReferenceResolver{
		real:    real,
		locked:  make(chan struct{}),
		unblock: make(chan struct{}),
	}
}

func (b *blockingBeforeValidationSkillReferenceResolver) ValidateAgentSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) error {
	if len(refs) == 0 {
		return b.real.ValidateAgentSkillReferences(ctx, tx, workspaceID, refs)
	}
	if err := storage.AcquireWorkspaceSkillRegistryLock(ctx, tx, workspaceID); err != nil {
		return err
	}
	b.once.Do(func() { close(b.locked) })
	select {
	case <-b.unblock:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.real.ValidateAgentSkillReferences(ctx, tx, workspaceID, refs)
}

func (b *blockingBeforeValidationSkillReferenceResolver) release() {
	defer func() { _ = recover() }()
	close(b.unblock)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", reason)
	}
}

func waitForAgentResult(t *testing.T, ch <-chan struct {
	agent *agent.Agent
	err   error
}) struct {
	agent *agent.Agent
	err   error
} {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Agent operation")
	}
	return struct {
		agent *agent.Agent
		err   error
	}{}
}

func waitForDeleteResult(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("DeleteSkill after Agent transaction: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DeleteSkill after Agent transaction")
	}
}

func agentCreateRequest(name string) agent.CreateAgentRequest {
	return agent.CreateAgentRequest{AgentConfig: agentConfig(name)}
}

func agentCreateRequestWithSkill(name, skillID string) agent.CreateAgentRequest {
	return agent.CreateAgentRequest{AgentConfig: agentConfigWithSkill(name, skillID)}
}

func agentCreateRequestWithSkillVersion(name, skillID, version string) agent.CreateAgentRequest {
	return agent.CreateAgentRequest{AgentConfig: agentConfigWithSkillVersion(name, skillID, version)}
}

func agentConfig(name string) agent.AgentConfig {
	return agent.AgentConfig{
		Name:  name,
		Model: "anthropic/claude-opus-4-8",
		Tools: agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
	}
}

func agentConfigWithSkill(name, skillID string) agent.AgentConfig {
	cfg := agentConfig(name)
	cfg.Skills = agent.RawArray{json.RawMessage(fmt.Sprintf(`{"type":"custom","skill_id":%q}`, skillID))}
	return cfg
}

func agentConfigWithSkillVersion(name, skillID, version string) agent.AgentConfig {
	cfg := agentConfig(name)
	cfg.Skills = agent.RawArray{json.RawMessage(fmt.Sprintf(`{"type":"custom","skill_id":%q,"version":%q}`, skillID, version))}
	return cfg
}

func seedActiveSkill(t *testing.T, adminDB *sql.DB, ws workspace.ID) string {
	t.Helper()
	skillID := "skill_seeded_active"
	version := "seeded-v1"
	now := "2026-04-30T00:00:00Z"
	if _, err := adminDB.ExecContext(context.Background(),
		`INSERT INTO skills (skill_id, workspace_id, display_title, latest_version, created_at, updated_at)
		 VALUES ($1, $2, NULL, $3, $4, $4)`,
		skillID, string(ws), version, now,
	); err != nil {
		t.Fatalf("seed active skill: %v", err)
	}
	if _, err := adminDB.ExecContext(context.Background(),
		`INSERT INTO skill_versions (workspace_id, skill_id, skill_version_id, version, name, description, directory, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, 'skill_version_seeded_active', $3, 'seeded', 'Seeded active Skill.', 'seeded', $4, 1, $5, $6)`,
		string(ws), skillID, version, "skills/"+string(ws)+"/"+skillID+"/versions/"+version+"/package.zip", strings.Repeat("0", 64), now,
	); err != nil {
		t.Fatalf("seed active skill version: %v", err)
	}
	return skillID
}

func countAgentVersionsByName(t *testing.T, db *sql.DB, ws workspace.ID, name string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM agent_versions av
		   JOIN agents a ON a.workspace_id = av.workspace_id AND a.id = av.agent_id
		  WHERE av.workspace_id = $1 AND a.name = $2`,
		string(ws), name,
	).Scan(&n); err != nil {
		t.Fatalf("count agent_versions by name: %v", err)
	}
	return n
}

func countAgentVersions(t *testing.T, db *sql.DB, ws workspace.ID, agentID string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM agent_versions WHERE workspace_id = $1 AND agent_id = $2`,
		string(ws), agentID,
	).Scan(&n); err != nil {
		t.Fatalf("count agent_versions: %v", err)
	}
	return n
}

func doMultipart(t *testing.T, method, url, contentType string, body io.Reader, apiKey string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-api-key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func doJSON(t *testing.T, method, url string, body []byte, apiKey string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("x-api-key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}
