package tetralapi_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralapi "github.com/tetral-ai/tetral/services/api"
)

const tetralAPITestAPIKeyID = "ak_tetralapi_test" //nolint:gosec // Test API key id, not a secret.
const tetralAPITestVaultKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

var tetralAPITestPrivateKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))

func TestTetralAPIProductionRouterInstallsMatureHandlersWithoutBlobStore(t *testing.T) {
	env := newTetralAPITestEnv(t)

	assertTetralAPIStatus(t, env.router, http.MethodPost, "/v1/api_keys", `{"name":"secondary key"}`, tetralAPITestAPIKeyID, http.StatusNotFound, "")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/files", "", tetralAPITestAPIKeyID, http.StatusNotImplemented, "not_implemented")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/skills", "", tetralAPITestAPIKeyID, http.StatusNotImplemented, "not_implemented")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/agents", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/environments", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/vaults", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/memory_stores", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/sessions?beta=true", "", tetralAPITestAPIKeyID, http.StatusOK, "")
}

func TestTetralAPIProductionRouterExercisesRepresentativeMatureFamilies(t *testing.T) {
	blobStore := blob.NewFakeBlobStore()
	env := newTetralAPITestEnv(t, withTetralAPIBlobStore(blobStore))

	skillID := uploadTetralAPISkill(t, env.router)
	if blobStore.Len() != 1 {
		t.Fatalf("skill upload blob writes = %d; want 1", blobStore.Len())
	}
	fileID := uploadTetralAPIFile(t, env.router)
	if blobStore.Len() != 2 {
		t.Fatalf("file+skill blob writes = %d; want 2", blobStore.Len())
	}

	agentID := createTetralAPIResource(t, env.router, "/v1/agents", `{"name":"router-agent","model":"anthropic/claude-opus-4-8","system":"v1","tools":[{"type":"tetral_agent_toolset","family":"claude"}],"skills":[{"type":"custom","skill_id":"`+skillID+`","version":"latest"}]}`)
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/agents/"+agentID+"/versions", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	environmentID := createTetralAPIResource(t, env.router, "/v1/environments", `{"name":"router-env","config":{"networking":{"type":"unrestricted"}}}`)
	vaultID := createTetralAPIResource(t, env.router, "/v1/vaults", `{"display_name":"router vault"}`)
	credentialSecret := "credential-secret-do-not-leak" //nolint:gosec // G101: synthetic credential sentinel for production-router redaction proof.
	credentialID := createTetralAPIResource(t, env.router, "/v1/vaults/"+vaultID+"/credentials", `{"display_name":"router credential","auth":{"type":"static_bearer","mcp_server_url":"https://mcp.example.com","token":"`+credentialSecret+`"}}`)
	credentialRecord := requestTetralAPI(env.router, http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+credentialID, "", tetralAPITestAPIKeyID)
	if credentialRecord.Code != http.StatusOK {
		t.Fatalf("GET credential status = %d body=%s", credentialRecord.Code, credentialRecord.Body.String())
	}
	if strings.Contains(credentialRecord.Body.String(), credentialSecret) {
		t.Fatalf("credential response leaked secret: %s", credentialRecord.Body.String())
	}
	assertCredentialEncryptedAtRest(t, env.admin, credentialID, credentialSecret)

	memoryStoreID := createTetralAPIResource(t, env.router, "/v1/memory_stores", `{"name":"router-memory"}`)
	memoryID := createTetralAPIResource(t, env.router, "/v1/memory_stores/"+memoryStoreID+"/memories?view=full", `{"path":"/notes/a.md","content":"memory content"}`)
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/memory_stores/"+memoryStoreID+"/memory_versions?memory_id="+memoryID, "", tetralAPITestAPIKeyID, http.StatusOK, "")

	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/files", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/skills", "", tetralAPITestAPIKeyID, http.StatusOK, "")

	sessionID := createTetralAPIResource(t, env.router, "/v1/sessions?beta=true", `{"agent":"`+agentID+`","environment_id":"`+environmentID+`","vault_ids":[],"resources":[{"type":"file","file_id":"`+fileID+`","mount_path":"/workspace/router-file.txt"}]}`)
	sessionFileID := assertTetralAPISessionResources(t, env.admin, sessionID)
	assertTetralAPIStatus(t, env.router, http.MethodPost, "/v1/sessions/"+sessionID+"/events", `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`, tetralAPITestAPIKeyID, http.StatusOK, "")
	eventList := requestTetralAPI(env.router, http.MethodGet, "/v1/sessions/"+sessionID+"/events?limit=10&order=asc", "", tetralAPITestAPIKeyID)
	if eventList.Code != http.StatusOK {
		t.Fatalf("GET session events status = %d body=%s", eventList.Code, eventList.Body.String())
	}
	if !strings.Contains(eventList.Body.String(), `"type":"user.message"`) {
		t.Fatalf("GET session events body missing admitted user event: %s", eventList.Body.String())
	}
	sessionPrefix := "workspaces/default/sessions/" + sessionID + "/"
	sessionResourcesPrefix := sessionPrefix + "resources/"
	sessionCopyA := sessionResourcesPrefix + "sesrsc_router/file"
	sessionCopyB := sessionResourcesPrefix + "sesrsc_router_nested/file"
	canonicalKey := "files/default/fobj_router_canonical"
	siblingSessionCopy := "workspaces/default/sessions/sesn_other/resources/sesrsc_other/file"
	putTetralAPIBlob(t, blobStore, sessionCopyA, "session-copy-a")
	putTetralAPIBlob(t, blobStore, sessionCopyB, "session-copy-b")
	putTetralAPIBlob(t, blobStore, canonicalKey, "canonical")
	putTetralAPIBlob(t, blobStore, siblingSessionCopy, "sibling")

	assertTetralAPIStatus(t, env.router, http.MethodDelete, "/v1/sessions/"+sessionID+"?beta=true", "", tetralAPITestAPIKeyID, http.StatusOK, "")
	if !sessionFileIdentityTombstoned(t, env.admin, sessionID, sessionFileID) {
		t.Fatalf("session file identity %s was not tombstoned on delete", sessionFileID)
	}
	if publicFileTombstoned(t, env.admin, fileID) {
		t.Fatalf("source public file %s was tombstoned by session delete", fileID)
	}
	if !blobStore.Has(sessionCopyA) || !blobStore.Has(sessionCopyB) {
		t.Fatalf("session resource prefix was synchronously removed by API delete; want durable GC marker instead")
	}
	if !blobStore.Has(canonicalKey) || !blobStore.Has(siblingSessionCopy) {
		t.Fatalf("session delete removed canonical or sibling resource copy outside its prefix")
	}
	if deletes := blobStore.Deletes(); len(deletes) != 0 {
		t.Fatalf("API delete blob deletes = %v; want no synchronous object-store cleanup", deletes)
	}
	assertTetralAPIResourcePrefixGCMarker(t, env.admin, sessionID, sessionPrefix)
}

func TestTetralAPIProductionRouterKeepsSkillRefsWithoutBlobStore(t *testing.T) {
	env := newTetralAPITestEnv(t)
	seedTetralAPISkillReference(t, env.admin, "skill_existing", "20260509120000")

	assertTetralAPIStatus(t, env.router, http.MethodPost, "/v1/skills", "", tetralAPITestAPIKeyID, http.StatusNotImplemented, "not_implemented")
	recorder := requestTetralAPI(env.router, http.MethodPost, "/v1/agents", `{
		"name":"with-existing-skill",
		"model":"anthropic/claude-opus-4-8",
		"tools":[{"type":"tetral_agent_toolset","family":"claude"}],
		"skills":[{"type":"custom","skill_id":"skill_existing","version":"latest"}]
	}`, tetralAPITestAPIKeyID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("Agent create with existing Skill reference status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"skill_id":"skill_existing"`) {
		t.Fatalf("Agent response missing canonical custom Skill reference: %s", recorder.Body.String())
	}
}

func TestTetralAPIProductionRouterDoesNotOwnAPIKeyRoutes(t *testing.T) {
	env := newTetralAPITestEnv(t)

	assertTetralAPIStatus(t, env.router, http.MethodGet, "/v1/agents?api_key=tetral_sk_query_sentinel", "", "", http.StatusUnauthorized, "authentication_error")
	assertTetralAPIStatus(t, env.router, http.MethodPost, "/v1/api_keys", `{"name":"generated key"}`, tetralAPITestAPIKeyID, http.StatusNotFound, "")
}

func TestTetralAPIProductionRouterValidatesOwnedVaultConfig(t *testing.T) {
	for _, tt := range []struct {
		name     string
		vaultKey string
	}{
		{name: "missing vault key", vaultKey: ""},
		{name: "weak vault key", vaultKey: "short"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tetralapi.BuildRouter(context.Background(), tetralapi.RouterConfig{
				VaultKey: tt.vaultKey,
				DataDir:  secureTetralAPIDataDir(t),
			})
			if err == nil {
				t.Fatal("BuildRouter returned nil error for invalid public API bootstrap config")
			}
			if _, ok := workload.AsConfigError(err); !ok {
				t.Fatalf("BuildRouter %s returned %T; want a workload.ConfigError", tt.name, err)
			}
		})
	}
}

type tetralAPITestEnv struct {
	router http.Handler
	admin  *sql.DB
}

type tetralAPITestOption func(*tetralapi.RouterConfig)

func withTetralAPIBlobStore(store blob.BlobStore) tetralAPITestOption {
	return func(config *tetralapi.RouterConfig) {
		config.BlobStore = store
	}
}

func putTetralAPIBlob(t *testing.T, store blob.BlobStore, key string, body string) {
	t.Helper()
	if err := store.Put(context.Background(), key, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("put blob %s: %v", key, err)
	}
}

func newTetralAPITestEnv(t *testing.T, options ...tetralAPITestOption) tetralAPITestEnv {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedTetralAPIPrincipalAPIKey(t, adminDB)
	runtimeClient := dbconnect.NewClientForTesting(runtimeDB)
	config := tetralapi.RouterConfig{
		RuntimeClient:     runtimeClient,
		RawDatabase:       adminDB,
		VaultKey:          tetralAPITestVaultKey,
		DataDir:           secureTetralAPIDataDir(t),
		Env:               tetralAPITestEnvMap{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_default_test"},
		PrincipalVerifier: tetralAPITestPrincipalVerifier(t),
	}
	for _, option := range options {
		option(&config)
	}
	router, err := tetralapi.BuildRouter(context.Background(), config)
	if err != nil {
		t.Fatalf("BuildRouter: %v", err)
	}
	return tetralAPITestEnv{router: router, admin: adminDB}
}

type tetralAPITestEnvMap map[string]string

func (m tetralAPITestEnvMap) Getenv(key string) string { return m[key] }

func secureTetralAPIDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // directory must be owner-only, not 0600
		t.Fatalf("chmod secure data dir: %v", err)
	}
	return dir
}

func seedTetralAPIPrincipalAPIKey(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO api_keys (id, workspace_id, name, key_prefix, key_digest, key_kind, created_at)
		 VALUES ($1, $2, 'api signed principal test key', 'tetral_test', decode(repeat('0', 64), 'hex'), 'standard', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		tetralAPITestAPIKeyID, string(workspace.DefaultID),
	)
	if err != nil {
		t.Fatalf("seed api principal api key metadata: %v", err)
	}
}

func createTetralAPIResource(t *testing.T, router http.Handler, path string, body string) string {
	t.Helper()
	recorder := requestTetralAPI(router, http.MethodPost, path, body, tetralAPITestAPIKeyID)
	if recorder.Code != http.StatusOK {
		t.Fatalf("POST %s status = %d body=%s", path, recorder.Code, recorder.Body.String())
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode POST %s response: %v body=%s", path, err, recorder.Body.String())
	}
	if response.ID == "" {
		t.Fatalf("POST %s response missing id: %s", path, recorder.Body.String())
	}
	return response.ID
}

func assertTetralAPIStatus(t *testing.T, router http.Handler, method string, path string, body string, apiKey string, wantStatus int, wantErrorType string) {
	t.Helper()
	recorder := requestTetralAPI(router, method, path, body, apiKey)
	if recorder.Code != wantStatus {
		t.Fatalf("%s %s status = %d; want %d body=%s", method, path, recorder.Code, wantStatus, recorder.Body.String())
	}
	if wantErrorType == "" {
		return
	}
	if !strings.Contains(recorder.Body.String(), wantErrorType) {
		t.Fatalf("%s %s body missing %q: %s", method, path, wantErrorType, recorder.Body.String())
	}
}

func requestTetralAPI(router http.Handler, method string, path string, body string, apiKeyID string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	addTetralAPIBetaQueryMarker(request)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodPost && strings.HasPrefix(path, "/v1/sessions/") && strings.HasSuffix(path, "/events") {
		request.Header.Set("Idempotency-Key", "idem_tetralapi_test_"+strings.Trim(path, "/"))
	}
	if apiKeyID != "" {
		request.Header.Set("X-Tetral-Internal-Principal", tetralAPITestPrincipalToken(method, request.URL.Path, apiKeyID))
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func addTetralAPIBetaQueryMarker(request *http.Request) {
	path := request.URL.Path
	mapped := path == "/v1/agents" || strings.HasPrefix(path, "/v1/agents/") ||
		path == "/v1/environments" || strings.HasPrefix(path, "/v1/environments/") ||
		path == "/v1/files" || strings.HasPrefix(path, "/v1/files/") ||
		path == "/v1/memory_stores" || strings.HasPrefix(path, "/v1/memory_stores/") ||
		path == "/v1/sessions" || strings.HasPrefix(path, "/v1/sessions/") ||
		path == "/v1/skills" || strings.HasPrefix(path, "/v1/skills/") ||
		path == "/v1/vaults" || strings.HasPrefix(path, "/v1/vaults/")
	if !mapped {
		return
	}
	if _, ok := request.URL.Query()["beta"]; ok {
		return
	}
	if request.URL.RawQuery == "" {
		request.URL.RawQuery = "beta=true"
		return
	}
	request.URL.RawQuery += "&beta=true"
}

func tetralAPITestPrincipalVerifier(t *testing.T) *auth.InternalPrincipalVerifier {
	t.Helper()
	publicKey, ok := tetralAPITestPrivateKey.Public().(ed25519.PublicKey)
	if !ok {
		t.Fatal("test internal-principal public key derivation failed")
	}
	verifier, err := auth.NewInternalPrincipalVerifier(publicKey)
	if err != nil {
		t.Fatalf("test internal-principal verifier: %v", err)
	}
	return verifier
}

func tetralAPITestPrincipalToken(method string, path string, apiKeyID string) string {
	signer, err := auth.NewInternalPrincipalSigner(tetralAPITestPrivateKey)
	if err != nil {
		panic(err)
	}
	token, err := signer.Mint(auth.Principal{
		Workspace: workspace.Workspace{ID: workspace.DefaultID, Type: "workspace"},
		APIKeyID:  apiKeyID,
	}, method, path, "req_tetralapi_test", time.Minute)
	if err != nil {
		panic(err)
	}
	return token
}

func uploadTetralAPISkill(t *testing.T, router http.Handler) string {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("display_title", "Wired Skill"); err != nil {
		t.Fatalf("write skill display_title: %v", err)
	}
	part, err := writer.CreateFormFile("files[]", "wired-skill/SKILL.md")
	if err != nil {
		t.Fatalf("create skill files[] part: %v", err)
	}
	if _, err := io.WriteString(part, "---\nname: wired-skill\ndescription: Wiring proof.\n---\nBody.\n"); err != nil {
		t.Fatalf("write skill files[] part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close skill multipart: %v", err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/skills", &body)
	addTetralAPIBetaQueryMarker(request)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-Tetral-Internal-Principal", tetralAPITestPrincipalToken(http.MethodPost, request.URL.Path, tetralAPITestAPIKeyID))
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("POST /v1/skills status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		ID           string  `json:"id"`
		Type         string  `json:"type"`
		Source       string  `json:"source"`
		DisplayTitle *string `json:"display_title"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode skill response: %v body=%s", err, recorder.Body.String())
	}
	if response.ID == "" || response.Type != "skill" || response.Source != "custom" || response.DisplayTitle == nil || *response.DisplayTitle != "Wired Skill" {
		t.Fatalf("skill response = %+v", response)
	}
	return response.ID
}

func uploadTetralAPIFile(t *testing.T, router http.Handler) string {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "router-file.txt")
	if err != nil {
		t.Fatalf("create file part: %v", err)
	}
	if _, err := io.WriteString(part, "router file content"); err != nil {
		t.Fatalf("write file part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close file multipart: %v", err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/files", &body)
	addTetralAPIBetaQueryMarker(request)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-Tetral-Internal-Principal", tetralAPITestPrincipalToken(http.MethodPost, request.URL.Path, tetralAPITestAPIKeyID))
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("POST /v1/files status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Filename string `json:"filename"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode file response: %v body=%s", err, recorder.Body.String())
	}
	if response.ID == "" || response.Type != "file" || response.Filename != "router-file.txt" {
		t.Fatalf("file response = %+v", response)
	}
	return response.ID
}

func assertCredentialEncryptedAtRest(t *testing.T, db *sql.DB, credentialID string, forbiddenSecret string) {
	t.Helper()
	var encryptedAuth []byte
	var publicAuth string
	err := db.QueryRowContext(context.Background(),
		`SELECT encrypted_auth, auth_public_json FROM credentials WHERE id = $1 AND workspace_id = $2`,
		credentialID, string("default"),
	).Scan(&encryptedAuth, &publicAuth)
	if err != nil {
		t.Fatalf("load credential storage: %v", err)
	}
	if len(encryptedAuth) == 0 {
		t.Fatal("credential encrypted_auth is empty")
	}
	if bytes.Contains(encryptedAuth, []byte(forbiddenSecret)) || strings.Contains(publicAuth, forbiddenSecret) {
		t.Fatalf("credential storage leaked secret encrypted=%q public=%s", string(encryptedAuth), publicAuth)
	}
}

func assertTetralAPISessionResources(t *testing.T, db *sql.DB, sessionID string) string {
	t.Helper()
	var sessionFileID string
	var mountPath string
	if err := db.QueryRowContext(context.Background(),
		`SELECT file_id, mount_path
		   FROM session_file_resources
		  WHERE workspace_id = $1 AND session_id = $2`,
		"default", sessionID,
	).Scan(&sessionFileID, &mountPath); err != nil {
		t.Fatalf("load session file resource: %v", err)
	}
	if sessionFileID == "" || mountPath != "/workspace/router-file.txt" {
		t.Fatalf("session file resource file_id/mount_path = %q/%q; want session id and router mount", sessionFileID, mountPath)
	}
	return sessionFileID
}

func seedTetralAPISkillReference(t *testing.T, db *sql.DB, skillID string, version string) {
	t.Helper()
	const timestamp = "2026-05-09T00:00:00Z"
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO skills (workspace_id, skill_id, latest_version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $4)`,
		"default", skillID, version, timestamp)
	if err != nil {
		t.Fatalf("seed skill parent: %v", err)
	}
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO skill_versions (workspace_id, skill_id, skill_version_id, version, name, description, directory, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $10)`,
		"default", skillID, "skv_existing", version, "existing", "Existing skill", "existing", "skills/default/skill_existing/package.zip", strings.Repeat("0", 64), timestamp)
	if err != nil {
		t.Fatalf("seed skill version: %v", err)
	}
}

func sessionFileIdentityTombstoned(t *testing.T, db *sql.DB, sessionID string, fileID string) bool {
	t.Helper()
	var tombstoned bool
	err := db.QueryRowContext(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM files WHERE workspace_id = $1 AND scope_type = 'session' AND scope_id = $2 AND file_id = $3`,
		"default", sessionID, fileID,
	).Scan(&tombstoned)
	if err != nil {
		t.Fatalf("load session file identity tombstone: %v", err)
	}
	return tombstoned
}

func publicFileTombstoned(t *testing.T, db *sql.DB, fileID string) bool {
	t.Helper()
	var tombstoned bool
	err := db.QueryRowContext(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM files WHERE workspace_id = $1 AND file_id = $2`,
		"default", fileID,
	).Scan(&tombstoned)
	if err != nil {
		t.Fatalf("load public file tombstone: %v", err)
	}
	return tombstoned
}

func assertTetralAPIResourcePrefixGCMarker(t *testing.T, db *sql.DB, sessionID string, wantPrefix string) {
	t.Helper()
	var prefix string
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT prefix, status
		   FROM session_resource_prefix_gc
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		"default", sessionID,
	).Scan(&prefix, &status); err != nil {
		t.Fatalf("load session resource prefix gc marker: %v", err)
	}
	if prefix != wantPrefix || status != "pending" {
		t.Fatalf("resource prefix gc marker = %q/%q; want %q/pending", prefix, status, wantPrefix)
	}
}
