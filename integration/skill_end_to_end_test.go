package integration

import (
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
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testAPIKey = "test-api-key-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// TestSkillUploadAgentReferenceLifecycleEndToEnd is the end-to-end proof:
// upload a Skill via /v1/skills, create an Agent
// referencing it, read the Agent back, delete the Skill, then verify
// (a) the historical Agent read returns the stored skills[] bytes
// unchanged, and (b) a NEW Agent referencing the deleted Skill is
// rejected with 400 invalid_request_error. The test does NOT assert
// runtime materialization; that belongs to session preparation.
func TestSkillUploadAgentReferenceLifecycleEndToEnd(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	apiKeyStore := auth.NewAPIKeyStore(runtimeDB)
	if err := auth.RefreshBootstrap(context.Background(), apiKeyStore, workspace.DefaultID, testAPIKey); err != nil {
		t.Fatalf("seed bootstrap: %v", err)
	}

	blobStore := blob.NewFakeBlobStore()
	skillStore := skill.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	stageDir := t.TempDir()
	skillService := skill.NewService(skillStore, skill.WithPackageStageDir(stageDir))
	skillHandler := httpapi.NewSkillHandler(skillService, stageDir)

	agentService := agent.NewService(agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(runtimeDB)), skillService)
	agentHandler := httpapi.NewAgentHandler(agentService)

	authenticator := &auth.StoreAuthenticator{Store: apiKeyStore}
	router := httpapi.NewRouter(nil, "",
		httpapi.WithAuthenticator(authenticator),
		httpapi.WithAgentHandler(agentHandler),
		httpapi.WithSkillHandler(skillHandler),
	)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	// 1. Upload a Skill.
	uploadURL := server.URL + "/v1/skills?beta=true"
	uploadBody, contentType := newSkillUpload(t, "e2e-skill", "End-to-end smoke fixture.")
	uploadResp := doMultipart(t, http.MethodPost, uploadURL, contentType, uploadBody, testAPIKey)
	if uploadResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(uploadResp.Body)
		t.Fatalf("upload status = %d (body=%q)", uploadResp.StatusCode, body)
	}
	var uploadedSkill skill.Skill
	if err := json.NewDecoder(uploadResp.Body).Decode(&uploadedSkill); err != nil {
		t.Fatalf("decode skill envelope: %v", err)
	}
	_ = uploadResp.Body.Close()
	skillID := uploadedSkill.ID

	// 2. Create an Agent referencing the Skill (omitted version
	// defaults to "latest").
	agentBody := []byte(fmt.Sprintf(`{
		"name": "e2e-agent",
		"model": "anthropic/claude-opus-4-8",
		"tools": [{"type":"tetral_agent_toolset","family":"claude"}],
		"skills": [{"type":"custom","skill_id": %q}]
	}`, skillID))
	createResp := doJSON(t, http.MethodPost, server.URL+"/v1/agents?beta=true", agentBody, testAPIKey)
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("create agent status = %d (body=%q)", createResp.StatusCode, body)
	}
	var createdAgent struct {
		ID      string          `json:"id"`
		Skills  json.RawMessage `json:"skills"`
		Version int             `json:"version"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&createdAgent); err != nil {
		t.Fatalf("decode created agent: %v", err)
	}
	_ = createResp.Body.Close()
	wantSkills := fmt.Sprintf(`[{"type":"custom","skill_id":%q,"version":"latest"}]`, skillID)
	if string(createdAgent.Skills) != wantSkills {
		t.Errorf("created agent skills = %s; want %s", string(createdAgent.Skills), wantSkills)
	}

	// 3. Read the Agent back; skills[] is byte-equal.
	getResp := doJSON(t, http.MethodGet, server.URL+"/v1/agents/"+createdAgent.ID+"?beta=true", nil, testAPIKey)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get agent status = %d", getResp.StatusCode)
	}
	var gotAgent struct {
		Skills json.RawMessage `json:"skills"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&gotAgent); err != nil {
		t.Fatalf("decode got agent: %v", err)
	}
	_ = getResp.Body.Close()
	if string(gotAgent.Skills) != wantSkills {
		t.Errorf("Get agent skills = %s; want %s", string(gotAgent.Skills), wantSkills)
	}

	// 4. Delete the uploaded version, then delete the parent Skill.
	versionResp := doJSON(t, http.MethodDelete, server.URL+"/v1/skills/"+skillID+"/versions/"+*uploadedSkill.LatestVersion+"?beta=true", nil, testAPIKey)
	if versionResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(versionResp.Body)
		t.Fatalf("delete skill version status = %d (body=%q)", versionResp.StatusCode, body)
	}
	_ = versionResp.Body.Close()
	delResp := doJSON(t, http.MethodDelete, server.URL+"/v1/skills/"+skillID+"?beta=true", nil, testAPIKey)
	if delResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(delResp.Body)
		t.Fatalf("delete skill status = %d (body=%q)", delResp.StatusCode, body)
	}
	_ = delResp.Body.Close()

	// 5. Historical Agent read returns the stored skills[] unchanged.
	getResp2 := doJSON(t, http.MethodGet, server.URL+"/v1/agents/"+createdAgent.ID+"?beta=true", nil, testAPIKey)
	if getResp2.StatusCode != http.StatusOK {
		t.Fatalf("post-delete get agent status = %d", getResp2.StatusCode)
	}
	var historicalAgent struct {
		Skills json.RawMessage `json:"skills"`
	}
	if err := json.NewDecoder(getResp2.Body).Decode(&historicalAgent); err != nil {
		t.Fatalf("decode historical agent: %v", err)
	}
	_ = getResp2.Body.Close()
	if string(historicalAgent.Skills) != wantSkills {
		t.Errorf("historical Get must return stored skills bytes verbatim; got %s want %s", string(historicalAgent.Skills), wantSkills)
	}

	// 6. A NEW Agent referencing the deleted Skill MUST reject with
	// 400 invalid_request_error.
	rejectBody := []byte(fmt.Sprintf(`{
		"name": "e2e-after-delete",
		"model": "anthropic/claude-opus-4-8",
		"tools": [{"type":"tetral_agent_toolset","family":"claude"}],
		"skills": [{"type":"custom","skill_id": %q}]
	}`, skillID))
	rejectResp := doJSON(t, http.MethodPost, server.URL+"/v1/agents?beta=true", rejectBody, testAPIKey)
	if rejectResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(rejectResp.Body)
		t.Fatalf("post-delete create-agent status = %d (body=%q); want 400", rejectResp.StatusCode, body)
	}
	var errResp struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rejectResp.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	_ = rejectResp.Body.Close()
	if errResp.Error.Type != "invalid_request_error" {
		t.Errorf("error.type = %q; want invalid_request_error", errResp.Error.Type)
	}
	if !strings.Contains(errResp.Error.Message, skillID) {
		t.Errorf("error message must name the deleted skill_id; got %q", errResp.Error.Message)
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
			createdSkill := createIntegrationSkill(t, skillStore, stageDir, strings.ReplaceAll(tc.name, "_", "-")+"-skill")

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
				deleteDone <- skillStore.DeleteVersion(context.Background(), ws, createdSkill.ID, *createdSkill.LatestVersion)
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
			if err := skillStore.DeleteVersion(context.Background(), ws, createdSkill.ID, *createdSkill.LatestVersion); err != nil {
				t.Fatalf("setup DeleteVersion to make parent Skill deletable: %v", err)
			}

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
			assertAgentValidationError(t, opResult.err)
			if opResult.agent != nil {
				t.Fatalf("Agent %s returned persisted Agent on validation failure: %+v", tc.name, opResult.agent)
			}
			if tc.name == "update" {
				if got := countAgentVersions(t, adminDB, ws, setupAgent.ID); got != beforeVersions {
					t.Fatalf("rejected Update left %d agent_versions; want %d", got, beforeVersions)
				}
			} else if got := countAgentVersionsByName(t, adminDB, ws, tc.agentName); got != 0 {
				t.Fatalf("rejected Create inserted %d agent_versions; want 0", got)
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
		"2026-04-30T00:00:00Z", string(ws), createdSkill.ID, *createdSkill.LatestVersion,
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
	pinnedVersion := *createdSkill.LatestVersion
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
