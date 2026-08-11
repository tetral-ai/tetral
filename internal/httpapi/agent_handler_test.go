package httpapi_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func newAgentTestRouter(t *testing.T) http.Handler {
	t.Helper()
	router, _ := newAgentTestRouterAndDB(t, nil)
	return router
}

func newAgentTestRouterAndDB(t *testing.T, pageTokenSecret []byte) (http.Handler, *sql.DB) {
	t.Helper()
	db := newTestDBFromStorage(t)
	agentStore := agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(db))
	options := []agent.ServiceOption{}
	if len(pageTokenSecret) > 0 {
		options = append(options, agent.WithPageTokenSecret(pageTokenSecret))
	}
	agentService := agent.NewService(agentStore, nil, options...)
	agentHandler := httpapi.NewAgentHandler(agentService)
	return withAgentToolsetFixture(newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithAgentHandler(agentHandler))), db
}

func newAgentTestRouterWithStaticWorkspaceAuth(t *testing.T, db *sql.DB, pageTokenSecret []byte) http.Handler {
	t.Helper()
	agentStore := agent.NewPostgreSQLAgentStore(dbconnect.NewClientForTesting(db))
	agentService := agent.NewService(agentStore, nil, agent.WithPageTokenSecret(pageTokenSecret))
	agentHandler := httpapi.NewAgentHandler(agentService)
	authenticator := auth.AuthenticatorFunc(func(_ context.Context, rawKey string) (auth.Principal, error) {
		switch rawKey {
		case testAPIKey:
			return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_default"}, nil
		case "workspace-b-key":
			return auth.Principal{Workspace: workspace.Workspace{ID: "workspace_b"}, APIKeyID: "ak_b"}, nil
		default:
			return auth.Principal{}, &auth.AuthenticationError{Message: "invalid api key"}
		}
	})
	return withAgentToolsetFixture(httpapi.NewRouter(newTestHandler(t), "", httpapi.WithAuthenticator(authenticator), httpapi.WithAgentHandler(agentHandler)))
}

func withAgentToolsetFixture(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/v1/agents" && request.Body != nil {
			body, err := io.ReadAll(request.Body)
			if err == nil {
				originalLength := len(body)
				var object map[string]json.RawMessage
				if json.Unmarshal(body, &object) == nil {
					var tools []json.RawMessage
					raw, present := object["tools"]
					if !present || json.Unmarshal(raw, &tools) == nil {
						found := false
						for _, tool := range tools {
							var entry struct {
								Type string `json:"type"`
							}
							if json.Unmarshal(tool, &entry) == nil && entry.Type == "tetral_agent_toolset" {
								found = true
								break
							}
						}
						if !found && (!present || len(tools) > 0) {
							tools = append(tools, json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`))
							object["tools"], _ = json.Marshal(tools)
							body, _ = json.Marshal(object)
							if len(body) < originalLength {
								body = append(body, bytes.Repeat([]byte(" "), originalLength-len(body))...)
							}
						}
					}
				}
				request.Body = io.NopCloser(bytes.NewReader(body))
				request.ContentLength = int64(len(body))
			}
		}
		next.ServeHTTP(response, request)
	})
}

func postAgentRequest(t *testing.T, router http.Handler, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)
	return recorder
}

func assertAgentInvalidRequestError(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	if errObj["type"] != "invalid_request_error" {
		t.Errorf("error.type = %v; want invalid_request_error", errObj["type"])
	}
}

func assertAgentErrorBodyOmits(t *testing.T, recorder *httptest.ResponseRecorder, forbidden ...string) {
	t.Helper()
	body := recorder.Body.String()
	for _, value := range forbidden {
		if strings.Contains(body, value) {
			t.Errorf("error response must not echo %q; body=%s", value, body)
		}
	}
}

func getAgentPath(t *testing.T, router http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s expected 200, got %d: %s", path, recorder.Code, recorder.Body.String())
	}
	return recorder
}

func decodeAgentListNames(t *testing.T, body []byte) []string {
	t.Helper()
	var response struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode Agent list: %v\n%s", err, body)
	}
	names := make([]string, 0, len(response.Data))
	for _, item := range response.Data {
		names = append(names, item.Name)
	}
	return names
}

func assertAgentPageTokenRedacted(t *testing.T, token string) {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decode next_page token: %v", err)
	}
	for _, forbidden := range []string{
		string(workspace.DefaultID),
		"workspace_b",
		"storage_sequence",
		"agv_",
		"SELECT",
		"credential",
		"secret",
	} {
		if bytes.Contains(decoded, []byte(forbidden)) {
			t.Fatalf("page token payload leaked %q: %s", forbidden, decoded)
		}
	}
}

func tamperLastByte(token string) string {
	if token == "" {
		return "x"
	}
	last := token[len(token)-1]
	if last == 'A' {
		return token[:len(token)-1] + "B"
	}
	return token[:len(token)-1] + "A"
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func setAgentCreatedAtForTest(t *testing.T, db *sql.DB, agentName string, timestamp string) {
	t.Helper()
	err := storage.WithWorkspaceTx(context.Background(), db, string(workspace.DefaultID), func(tx *sql.Tx) error {
		result, err := tx.ExecContext(context.Background(), `UPDATE agents SET created_at = $1, updated_at = $1 WHERE name = $2`, timestamp, agentName)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			t.Fatalf("set created_at for %s affected %d rows; want 1", agentName, affected)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("set created_at for %s: %v", agentName, err)
	}
}

// assertAgentRequestTooLarge pins the centralized oversized-request envelope
// shape: HTTP 413 + error.type = "request_too_large".
// Used by every oversized create/update body case so a regression that
// drops back to one-off 400 wiring fails loudly.
func assertAgentRequestTooLarge(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	if errObj["type"] != "request_too_large" {
		t.Errorf("error.type = %v; want request_too_large", errObj["type"])
	}
}

func assertAgentListCount(t *testing.T, router http.Handler, want int) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list agents: expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []json.RawMessage `json:"data"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Data) != want {
		t.Fatalf("list agents count = %d; want %d; body=%s", len(response.Data), want, recorder.Body.String())
	}
}

func createAgentForStrictDecodeTest(t *testing.T, router http.Handler) string {
	t.Helper()
	recorder := postAgentRequest(t, router, "/v1/agents",
		`{"name":"strict-update","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("setup create: expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &created)
	agentID, _ := created["id"].(string)
	if agentID == "" {
		t.Fatalf("setup create response missing id: %s", recorder.Body.String())
	}
	return agentID
}

func getAgentForStrictDecodeTest(t *testing.T, router http.Handler, agentID string) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/agents/"+agentID, nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("get agent: expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	return response
}

func TestCreateAgentValid(t *testing.T) {
	router := newAgentTestRouter(t)

	body := `{"name":"my agent","model":"anthropic/claude-opus-4-8","system":"You are helpful."}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	id, _ := response["id"].(string)
	if !strings.HasPrefix(id, "agent_") {
		t.Errorf("expected agent_ prefix, got %q", id)
	}
	if response["version"].(float64) != 1 {
		t.Errorf("expected version 1, got %v", response["version"])
	}
}

func TestAgentHTTPCompatibilityExactlyOneToolsetAndNullClearMessages(t *testing.T) {
	router := newAgentTestRouter(t)
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "zero", body: `{"name":"zero","model":"anthropic/claude-opus-4-8","tools":[]}`, want: "tools must contain exactly one tetral_agent_toolset entry"},
		{name: "duplicate", body: `{"name":"duplicate","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"tetral_agent_toolset","family":"gpt"}]}`, want: "tools[1] duplicates the tetral_agent_toolset entry; exactly one is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := postAgentRequest(t, router, "/v1/agents", tc.body)
			assertAgentInvalidRequestError(t, recorder)
			if !strings.Contains(recorder.Body.String(), tc.want) {
				t.Fatalf("body = %s; want message %q", recorder.Body.String(), tc.want)
			}
		})
	}

	created := postAgentRequest(t, router, "/v1/agents", `{"name":"null-clear","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d %s", created.Code, created.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(created.Body.Bytes(), &response)
	cleared := postAgentRequest(t, router, "/v1/agents/"+response["id"].(string), `{"version":1,"tools":null}`)
	assertAgentInvalidRequestError(t, cleared)
	if !strings.Contains(cleared.Body.String(), "tools must contain exactly one tetral_agent_toolset entry") {
		t.Fatalf("tools:null body = %s; want zero-entry message", cleared.Body.String())
	}
}

func TestCreateAgentAcceptsStandardModelSpeedAndHidesInternalVariant(t *testing.T) {
	router := newAgentTestRouter(t)

	recorder := postAgentRequest(t, router, "/v1/agents", `{"name":"standard","model":{"id":"openai/gpt-5.5","speed":"standard"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s; want 200", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if model, ok := response["model"].(map[string]any); !ok || model["id"] != "openai/gpt-5.5" {
		t.Fatalf("model = %v; want canonical provider/model id", response["model"])
	}
	if _, ok := response["model_variant"]; ok {
		t.Fatalf("response leaked model_variant: %s", recorder.Body.String())
	}
}

func TestCreateAgentRejectsFastModelSpeed(t *testing.T) {
	router := newAgentTestRouter(t)

	recorder := postAgentRequest(t, router, "/v1/agents", `{"name":"fast","model":{"id":"openai/gpt-5.5","speed":"fast"}}`)
	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "speed") {
		t.Fatalf("error.message = %q; want speed admission error", msg)
	}
	assertAgentListCount(t, router, 0)
}

func TestCreateAgentDefaultsApprovalModeAndReturnsNullMultiagent(t *testing.T) {
	router := newAgentTestRouter(t)

	recorder := postAgentRequest(t, router, "/v1/agents", `{"name":"approval-default","model":"anthropic/claude-opus-4-8"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s; want 200", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if response["approval_mode"] != "ask_for_approval" {
		t.Fatalf("approval_mode = %v; want ask_for_approval", response["approval_mode"])
	}
	if value, ok := response["multiagent"]; !ok || value != nil {
		t.Fatalf("multiagent = %v present=%v; want null", value, ok)
	}
}

func TestCreateAgentAcceptsApprovalModeAndExplicitNullMultiagent(t *testing.T) {
	router := newAgentTestRouter(t)

	recorder := postAgentRequest(t, router, "/v1/agents", `{"name":"approval-explicit","model":"anthropic/claude-opus-4-8","approval_mode":"approve_for_me","multiagent":null}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s; want 200", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if response["approval_mode"] != "approve_for_me" {
		t.Fatalf("approval_mode = %v; want approve_for_me", response["approval_mode"])
	}
	if value, ok := response["multiagent"]; !ok || value != nil {
		t.Fatalf("multiagent = %v present=%v; want null", value, ok)
	}
}

func TestUpdateAgentApprovalModePreserveAndModify(t *testing.T) {
	router := newAgentTestRouter(t)

	createRec := postAgentRequest(t, router, "/v1/agents", `{"name":"approval-update","model":"anthropic/claude-opus-4-8","approval_mode":"approve_for_me","system":"v1"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d body=%s; want 200", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	preserveRec := postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":1,"system":"v2"}`)
	if preserveRec.Code != http.StatusOK {
		t.Fatalf("preserve status = %d body=%s; want 200", preserveRec.Code, preserveRec.Body.String())
	}
	var preserved map[string]any
	_ = json.Unmarshal(preserveRec.Body.Bytes(), &preserved)
	if preserved["approval_mode"] != "approve_for_me" {
		t.Fatalf("preserved approval_mode = %v; want approve_for_me", preserved["approval_mode"])
	}

	nullMultiagentRec := postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":2,"multiagent":null}`)
	if nullMultiagentRec.Code != http.StatusOK {
		t.Fatalf("multiagent null update status = %d body=%s; want 200", nullMultiagentRec.Code, nullMultiagentRec.Body.String())
	}
	var nullMultiagent map[string]any
	_ = json.Unmarshal(nullMultiagentRec.Body.Bytes(), &nullMultiagent)
	if value, ok := nullMultiagent["multiagent"]; !ok || value != nil {
		t.Fatalf("multiagent after update = %v present=%v; want null", value, ok)
	}

	modifyRec := postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":2,"approval_mode":"full_access"}`)
	if modifyRec.Code != http.StatusOK {
		t.Fatalf("modify status = %d body=%s; want 200", modifyRec.Code, modifyRec.Body.String())
	}
	var modified map[string]any
	_ = json.Unmarshal(modifyRec.Body.Bytes(), &modified)
	if modified["approval_mode"] != "full_access" {
		t.Fatalf("modified approval_mode = %v; want full_access", modified["approval_mode"])
	}
}

func TestCreateAgentRejectsUnsupportedApprovalAndMultiagentTopology(t *testing.T) {
	router := newAgentTestRouter(t)

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "approval null", body: `{"name":"x","model":"anthropic/claude-opus-4-8","approval_mode":null}`},
		{name: "approval unknown", body: `{"name":"x","model":"anthropic/claude-opus-4-8","approval_mode":"always"}`},
		{name: "multiagent object", body: `{"name":"x","model":"anthropic/claude-opus-4-8","multiagent":{}}`},
		{name: "multiagent array", body: `{"name":"x","model":"anthropic/claude-opus-4-8","multiagent":[]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := postAgentRequest(t, router, "/v1/agents", test.body)
			assertAgentInvalidRequestError(t, recorder)
		})
	}
	assertAgentListCount(t, router, 0)
}

func TestCreateAgentMissingName(t *testing.T) {
	router := newAgentTestRouter(t)

	body := `{"model":"anthropic/claude-opus-4-8"}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetAgentExisting(t *testing.T) {
	router := newAgentTestRouter(t)

	// Create
	createBody := `{"name":"test","model":"anthropic/claude-opus-4-8"}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	// Get
	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/agents/"+agentID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRec.Code)
	}
}

func TestGetAgentNonexistent(t *testing.T) {
	router := newAgentTestRouter(t)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/agents/agent_fake", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}

func TestListAgents(t *testing.T) {
	router := newAgentTestRouter(t)

	for i := 0; i < 2; i++ {
		body := `{"name":"agent` + string(rune('a'+i)) + `","model":"anthropic/claude-opus-4-8"}`
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	var response struct {
		Data []any `json:"data"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Data) != 2 {
		t.Errorf("expected 2 agents, got %d", len(response.Data))
	}
}

func TestUpdateAgentValid(t *testing.T) {
	router := newAgentTestRouter(t)

	// Create
	createBody := `{"name":"test","model":"anthropic/claude-opus-4-8","system":"v1"}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	// Update
	updateBody := `{"name":"test","version":1,"model":"anthropic/claude-opus-4-8","system":"v2"}`
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID, strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}

	var updated map[string]any
	_ = json.Unmarshal(updateRec.Body.Bytes(), &updated)
	if updated["version"].(float64) != 2 {
		t.Errorf("expected version 2, got %v", updated["version"])
	}
}

func TestUpdateAgentAcceptsModelObject(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := postAgentRequest(t, router, "/v1/agents", `{"name":"test","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create: expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateRec := postAgentRequest(t, router, "/v1/agents/"+agentID, `{"name":"test","version":1,"model":{"id":"deepseek/deepseek-v4-pro","speed":"standard"},"system":"v1"}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update: expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	var updated map[string]any
	_ = json.Unmarshal(updateRec.Body.Bytes(), &updated)
	if updated["version"].(float64) != 2 {
		t.Fatalf("version = %v; want 2", updated["version"])
	}
	if model, ok := updated["model"].(map[string]any); !ok || model["id"] != "deepseek/deepseek-v4-pro" {
		t.Fatalf("model = %v; want canonical provider/model id", updated["model"])
	}
	if _, ok := updated["model_variant"]; ok {
		t.Fatalf("response leaked model_variant: %s", updateRec.Body.String())
	}
}

func TestUpdateAgentPatchWithMatchingVersionStillUpdates(t *testing.T) {
	router := newAgentTestRouter(t)

	recorder := postAgentRequest(t, router, "/v1/agents",
		`{"name":"patch-route","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create: expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateRecorder := postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":1,"system":"v2"}`)
	if updateRecorder.Code != http.StatusOK {
		t.Fatalf("update: expected 200, got %d: %s", updateRecorder.Code, updateRecorder.Body.String())
	}
	var updated map[string]any
	_ = json.Unmarshal(updateRecorder.Body.Bytes(), &updated)
	if updated["version"].(float64) != 2 {
		t.Errorf("version = %v; want 2", updated["version"])
	}
	if updated["system"] != "v2" {
		t.Errorf("system = %v; want v2", updated["system"])
	}
}

func TestUpdateAgentStaleVersion(t *testing.T) {
	router := newAgentTestRouter(t)

	// Create + Update to v2
	createBody := `{"name":"test","model":"anthropic/claude-opus-4-8","system":"v1"}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateBody := `{"name":"test","version":1,"model":"anthropic/claude-opus-4-8","system":"v2"}`
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID, strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)

	// Stale version=1 update
	staleBody := `{"name":"test","version":1,"model":"anthropic/claude-opus-4-8","system":"v3"}`
	staleRec := httptest.NewRecorder()
	staleReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID, strings.NewReader(staleBody))
	staleReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(staleReq)
	router.ServeHTTP(staleRec, staleReq)

	if staleRec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", staleRec.Code, staleRec.Body.String())
	}
}

func TestUpdateAgentNoOp(t *testing.T) {
	router := newAgentTestRouter(t)

	createBody := `{"name":"test","model":"anthropic/claude-opus-4-8","system":"v1"}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	// Same config → no-op
	noopBody := `{"name":"test","version":1,"model":"anthropic/claude-opus-4-8","system":"v1"}`
	noopRec := httptest.NewRecorder()
	noopReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID, strings.NewReader(noopBody))
	noopReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(noopReq)
	router.ServeHTTP(noopRec, noopReq)

	if noopRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", noopRec.Code)
	}

	var result map[string]any
	_ = json.Unmarshal(noopRec.Body.Bytes(), &result)
	if result["version"].(float64) != 1 {
		t.Errorf("expected version 1 (no-op), got %v", result["version"])
	}
}

// TestUpdateAgentRejectsExplicitClearOnRequiredScalar proves required
// scalar fields cannot be cleared with null or "" under patch semantics.
func TestUpdateAgentRejectsExplicitClearOnRequiredScalar(t *testing.T) {
	router := newAgentTestRouter(t)

	createBody := `{"name":"x","model":"anthropic/claude-opus-4-8"}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("setup create: expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	cases := []struct {
		name      string
		body      string
		fieldHint string
	}{
		{"name null", `{"version":1,"name":null}`, "name"},
		{"name empty", `{"version":1,"name":""}`, "name"},
		{"model null", `{"version":1,"model":null}`, "model"},
		{"model empty", `{"version":1,"model":""}`, "model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			setAuthHeader(req)
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			var response map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &response)
			errObj, _ := response["error"].(map[string]any)
			if errObj["type"] != "invalid_request_error" {
				t.Errorf("error.type = %v; want invalid_request_error", errObj["type"])
			}
			msg, _ := errObj["message"].(string)
			if !strings.Contains(msg, tc.fieldHint) {
				t.Errorf("error.message = %q; want it to mention %s", msg, tc.fieldHint)
			}
		})
	}
}

// TestUpdateAgentOmittedRequiredScalarPreserves — the dual of the
// reject test. Omitting a required scalar from the patch must preserve
// the current value; the materialized config equals current and the
// Service no-op detector returns the existing record without bumping
// version. This is the structural difference from replacement-style
// updates.
func TestUpdateAgentOmittedRequiredScalarPreserves(t *testing.T) {
	router := newAgentTestRouter(t)

	createBody := `{"name":"keep-me","model":"anthropic/claude-opus-4-8"}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	// Patch only carries version — every required scalar is omitted.
	// Service materialization must preserve all of them; the no-op
	// detector then sees materialized == current and returns version 1.
	updateBody := `{"version":1}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID, strings.NewReader(updateBody))
	req.Header.Set("Content-Type", "application/json")
	setAuthHeader(req)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for omit-only patch, got %d: %s", rec.Code, rec.Body.String())
	}
	var result map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &result)
	if result["version"].(float64) != 1 {
		t.Errorf("omit-only patch must be no-op (version stays 1); got %v", result["version"])
	}
	if result["name"] != "keep-me" {
		t.Errorf("omitted name must preserve 'keep-me'; got %v", result["name"])
	}
}

func TestCreateAgentRejectsUnsupportedHTTPFields(t *testing.T) {
	router := newAgentTestRouter(t)
	cases := []struct {
		name      string
		body      string
		fieldHint string
	}{
		{"speed", `{"name":"x","model":"anthropic/claude-opus-4-8","speed":"fast"}`, "speed"},
		{"effort", `{"name":"x","model":"anthropic/claude-opus-4-8","effort":"high"}`, "effort"},
		{"reasoning effort", `{"name":"x","model":"anthropic/claude-opus-4-8","reasoning_effort":"high"}`, "reasoning_effort"},
		{"callable agents", `{"name":"x","model":"anthropic/claude-opus-4-8","callable_agents":[]}`, "callable_agents"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := postAgentRequest(t, router, "/v1/agents", tc.body)
			assertAgentInvalidRequestError(t, recorder)
			var response map[string]any
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			errObj, _ := response["error"].(map[string]any)
			msg, _ := errObj["message"].(string)
			if !strings.Contains(msg, tc.fieldHint) {
				t.Errorf("error.message = %q; want it to mention %s", msg, tc.fieldHint)
			}
			assertAgentListCount(t, router, 0)
		})
	}
}

func TestCreateAgentRejectsUnapprovedModels(t *testing.T) {
	router := newAgentTestRouter(t)
	for _, body := range []string{
		`{"name":"x","model":"claude-opus-4-8"}`,
		`{"name":"x","model":"anthropic/claude/opus"}`,
		`{"name":"x","model":"anthropic/claude-opus-4-7"}`,
		`{"name":"x","model":{"id":"openai/gpt-5.4"}}`,
	} {
		recorder := postAgentRequest(t, router, "/v1/agents", body)
		assertAgentInvalidRequestError(t, recorder)
		assertAgentListCount(t, router, 0)
	}
}

func TestUpdateAgentRejectsUnsupportedHTTPFieldsWithoutVersionBump(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	cases := []struct {
		name      string
		body      string
		fieldHint string
	}{
		{"speed", `{"version":1,"speed":"fast"}`, "speed"},
		{"effort", `{"version":1,"effort":"high"}`, "effort"},
		{"reasoning effort", `{"version":1,"reasoning_effort":"high"}`, "reasoning_effort"},
		{"callable agents", `{"version":1,"callable_agents":[]}`, "callable_agents"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, tc.body)
			assertAgentInvalidRequestError(t, recorder)
			var response map[string]any
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			errObj, _ := response["error"].(map[string]any)
			msg, _ := errObj["message"].(string)
			if !strings.Contains(msg, tc.fieldHint) {
				t.Errorf("error.message = %q; want it to mention %s", msg, tc.fieldHint)
			}
			agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
			if agentResponse["version"].(float64) != 1 {
				t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
			}
			if agentResponse["system"] != "v1" {
				t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
			}
		})
	}
}

func TestAgentArchiveAndVersionsRoutesAreRealWhenHandlerInstalled(t *testing.T) {
	router := newAgentTestRouter(t)

	createRec := postAgentRequest(t, router, "/v1/agents", `{"name":"routes","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateRec := postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":1,"system":"v2"}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}

	versionRec := httptest.NewRecorder()
	versionReq := httptest.NewRequest(http.MethodGet, "/v1/agents/"+agentID+"/versions", nil)
	setAuthHeader(versionReq)
	router.ServeHTTP(versionRec, versionReq)
	if versionRec.Code != http.StatusOK {
		t.Fatalf("versions expected real 200 handler, got %d: %s", versionRec.Code, versionRec.Body.String())
	}
	var versions struct {
		Data     []json.RawMessage `json:"data"`
		NextPage *string           `json:"next_page"`
	}
	if err := json.Unmarshal(versionRec.Body.Bytes(), &versions); err != nil {
		t.Fatalf("decode versions: %v", err)
	}
	if len(versions.Data) != 2 || versions.NextPage != nil {
		t.Fatalf("versions data=%d next=%v; want 2 snapshots and nil token", len(versions.Data), versions.NextPage)
	}
	for _, item := range versions.Data {
		assertAgentResponseShape(t, item)
	}

	archiveRec := postAgentRequest(t, router, "/v1/agents/"+agentID+"/archive", `{}`)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive expected real 200 handler, got %d: %s", archiveRec.Code, archiveRec.Body.String())
	}
	var archived map[string]any
	_ = json.Unmarshal(archiveRec.Body.Bytes(), &archived)
	if archived["archived_at"] == nil {
		t.Fatalf("archive response archived_at = nil; body=%s", archiveRec.Body.String())
	}
}

// Agent HTTP response-shape contract.

// targetTopLevelKeys is the top-level field set every Agent response must expose at the flat
// top level. archived_at is optional ("when/if archive becomes real")
// and not asserted here.
var targetTopLevelKeys = []string{
	"id", "type", "version",
	"name", "description", "model", "approval_mode", "system",
	"multiagent", "tools", "mcp_servers", "skills", "metadata",
	"created_at", "updated_at", "archived_at",
}

// assertAgentResponseShape pins the public Agent response shape: flat top-level
// keys, no nested "config" wrapper, and tools/mcp_servers/skills/
// metadata always present (never null) — empty values render as
// []/[]/[]/{}.
func assertAgentResponseShape(t *testing.T, body []byte) {
	t.Helper()
	var got map[string]json.RawMessage
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("response body is not a JSON object: %v\n%s", err, body)
	}
	if _, ok := got["config"]; ok {
		t.Errorf("Agent response must not contain nested 'config' wrapper; body=%s", body)
	}
	for _, k := range targetTopLevelKeys {
		if _, ok := got[k]; !ok {
			t.Errorf("Agent response missing top-level key %q; body=%s", k, body)
		}
	}
	var model struct {
		ID    string `json:"id"`
		Speed string `json:"speed,omitempty"`
	}
	if err := json.Unmarshal(got["model"], &model); err != nil || model.ID != "anthropic/claude-opus-4-8" || model.Speed != "" {
		t.Errorf("Agent response model = %s; want canonical object with id and no speed", got["model"])
	}
	var tools []json.RawMessage
	if err := json.Unmarshal(got["tools"], &tools); err != nil || len(tools) != 1 {
		t.Errorf("Agent response tools = %s; want the one admitted family declaration", got["tools"])
	}
	mustFragment := []string{`"mcp_servers":[]`, `"skills":[]`, `"metadata":{}`, `"multiagent":null`}
	for _, frag := range mustFragment {
		if !strings.Contains(string(body), frag) {
			t.Errorf("expected %q in body when container is empty; body=%s", frag, body)
		}
	}
	forbidFragment := []string{`"tools":null`, `"mcp_servers":null`, `"skills":null`, `"metadata":null`}
	for _, frag := range forbidFragment {
		if strings.Contains(string(body), frag) {
			t.Errorf("Agent response must not contain %q; body=%s", frag, body)
		}
	}
}

func TestCreateAgentResponseShapeIsFlat(t *testing.T) {
	router := newAgentTestRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/agents",
		strings.NewReader(`{"name":"shape","model":"anthropic/claude-opus-4-8"}`))
	req.Header.Set("Content-Type", "application/json")
	setAuthHeader(req)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	assertAgentResponseShape(t, rec.Body.Bytes())
}

func TestGetAgentResponseShapeIsFlat(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents",
		strings.NewReader(`{"name":"shape-get","model":"anthropic/claude-opus-4-8"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/agents/"+agentID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	assertAgentResponseShape(t, getRec.Body.Bytes())
}

func TestListAgentItemShapeIsFlat(t *testing.T) {
	router := newAgentTestRouter(t)
	for _, n := range []string{"a", "b"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/agents",
			strings.NewReader(`{"name":"shape-list-`+n+`","model":"anthropic/claude-opus-4-8"}`))
		req.Header.Set("Content-Type", "application/json")
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	setAuthHeader(listReq)
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	var listResp struct {
		Data []json.RawMessage `json:"data"`
	}
	_ = json.Unmarshal(listRec.Body.Bytes(), &listResp)
	if len(listResp.Data) == 0 {
		t.Fatal("list returned empty data")
	}
	for i, item := range listResp.Data {
		t.Run("item "+string(rune('0'+i)), func(t *testing.T) {
			assertAgentResponseShape(t, item)
		})
	}
}

func TestListAgentsUsesAnthropicEnvelopeAndExcludesArchivedByDefault(t *testing.T) {
	router := newAgentTestRouter(t)
	liveRec := postAgentRequest(t, router, "/v1/agents", `{"name":"list-live","model":"anthropic/claude-opus-4-8"}`)
	if liveRec.Code != http.StatusOK {
		t.Fatalf("create live: %d %s", liveRec.Code, liveRec.Body.String())
	}
	archivedRec := postAgentRequest(t, router, "/v1/agents", `{"name":"list-archived","model":"anthropic/claude-opus-4-8"}`)
	if archivedRec.Code != http.StatusOK {
		t.Fatalf("create archived: %d %s", archivedRec.Code, archivedRec.Body.String())
	}
	var archived map[string]any
	_ = json.Unmarshal(archivedRec.Body.Bytes(), &archived)
	archiveRec := postAgentRequest(t, router, "/v1/agents/"+archived["id"].(string)+"/archive", `{}`)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive: %d %s", archiveRec.Code, archiveRec.Body.String())
	}

	defaultRec := getAgentPath(t, router, "/v1/agents")
	var defaultRaw map[string]json.RawMessage
	if err := json.Unmarshal(defaultRec.Body.Bytes(), &defaultRaw); err != nil {
		t.Fatalf("decode default list: %v", err)
	}
	if _, ok := defaultRaw["has_more"]; ok {
		t.Fatalf("Agent list exposed legacy has_more: %s", defaultRec.Body.String())
	}
	if _, ok := defaultRaw["next_page"]; !ok {
		t.Fatalf("Agent list missing next_page: %s", defaultRec.Body.String())
	}
	defaultNames := decodeAgentListNames(t, defaultRec.Body.Bytes())
	if len(defaultNames) != 1 || defaultNames[0] != "list-live" {
		t.Fatalf("default Agent list names = %v; want only live Agent", defaultNames)
	}

	includeRec := getAgentPath(t, router, "/v1/agents?include_archived=true")
	includeNames := decodeAgentListNames(t, includeRec.Body.Bytes())
	if len(includeNames) != 2 {
		t.Fatalf("include_archived Agent list names = %v; want live and archived", includeNames)
	}
}

func TestListAgentsRejectsMalformedQueryParams(t *testing.T) {
	router := newAgentTestRouter(t)
	for _, path := range []string{
		"/v1/agents?after_id=agent_old",
		"/v1/agents?before_id=agent_old",
		"/v1/agents?unknown=x",
		"/v1/agents?limit=",
		"/v1/agents?limit=0",
		"/v1/agents?limit=-1",
		"/v1/agents?limit=abc",
		"/v1/agents?limit=1&limit=2",
		"/v1/agents?page=",
		"/v1/agents?include_archived=maybe",
		"/v1/agents?created_at%5Bgte%5D=not-time",
		"/v1/agents?created_at%5Blte%5D=not-time",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("%s: expected 400, got %d: %s", path, recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

func TestListAgentsLimitDefaultsAndClampsAtOneHundred(t *testing.T) {
	router := newAgentTestRouter(t)
	for i := 0; i < 101; i++ {
		name := "limit-agent-" + string(rune('a'+(i%26))) + "-" + string(rune('a'+(i/26)))
		rec := postAgentRequest(t, router, "/v1/agents", `{"name":"`+name+`","model":"anthropic/claude-opus-4-8"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("create %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}

	defaultRec := getAgentPath(t, router, "/v1/agents")
	var defaultPage struct {
		Data     []json.RawMessage `json:"data"`
		NextPage *string           `json:"next_page"`
	}
	if err := json.Unmarshal(defaultRec.Body.Bytes(), &defaultPage); err != nil {
		t.Fatalf("decode default page: %v", err)
	}
	if len(defaultPage.Data) != 20 || defaultPage.NextPage == nil {
		t.Fatalf("default page data=%d next=%v; want 20 rows and token", len(defaultPage.Data), defaultPage.NextPage)
	}

	cappedRec := getAgentPath(t, router, "/v1/agents?limit=101")
	var cappedPage struct {
		Data     []json.RawMessage `json:"data"`
		NextPage *string           `json:"next_page"`
	}
	if err := json.Unmarshal(cappedRec.Body.Bytes(), &cappedPage); err != nil {
		t.Fatalf("decode capped page: %v", err)
	}
	if len(cappedPage.Data) != 100 || cappedPage.NextPage == nil {
		t.Fatalf("capped page data=%d next=%v; want 100 rows and token", len(cappedPage.Data), cappedPage.NextPage)
	}
}

func TestListAgentsCreatedAtFiltersAreInclusive(t *testing.T) {
	router, db := newAgentTestRouterAndDB(t, bytes.Repeat([]byte{4}, 32))
	names := []string{"before", "lower", "middle", "upper", "after"}
	for _, name := range names {
		rec := postAgentRequest(t, router, "/v1/agents", `{"name":"filter-`+name+`","model":"anthropic/claude-opus-4-8"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("create %s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	timestamps := map[string]string{
		"filter-before": "2026-01-01T09:59:59.999Z",
		"filter-lower":  "2026-01-01T10:00:00Z",
		"filter-middle": "2026-01-01T10:00:00.5Z",
		"filter-upper":  "2026-01-01T11:00:00Z",
		"filter-after":  "2026-01-01T11:00:00.001Z",
	}
	for name, timestamp := range timestamps {
		setAgentCreatedAtForTest(t, db, name, timestamp)
	}

	recorder := getAgentPath(t, router, "/v1/agents?created_at%5Bgte%5D=2026-01-01T10%3A00%3A00Z&created_at%5Blte%5D=2026-01-01T11%3A00%3A00Z")
	got := decodeAgentListNames(t, recorder.Body.Bytes())
	want := []string{"filter-lower", "filter-middle", "filter-upper"}
	if !equalStrings(got, want) {
		t.Fatalf("filtered names = %v; want %v", got, want)
	}
}

func TestListAgentsPaginatesWithScopedOpaqueTokens(t *testing.T) {
	secret := bytes.Repeat([]byte{7}, 32)
	router, db := newAgentTestRouterAndDB(t, secret)
	for _, name := range []string{"page-a", "page-b", "page-c"} {
		rec := postAgentRequest(t, router, "/v1/agents", `{"name":"`+name+`","model":"anthropic/claude-opus-4-8"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("create %s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	for name, timestamp := range map[string]string{
		"page-a": "2026-01-01T10:00:00Z",
		"page-b": "2026-01-01T10:00:00Z",
		"page-c": "2026-01-01T10:00:00.5Z",
	} {
		setAgentCreatedAtForTest(t, db, name, timestamp)
	}

	pagePath := "/v1/agents?limit=1"
	gotNames := []string{}
	seenNames := map[string]bool{}
	var firstToken string
	for pageNumber := 0; pageNumber < 3; pageNumber++ {
		pageRec := getAgentPath(t, router, pagePath)
		var page struct {
			Data     []map[string]any `json:"data"`
			NextPage *string          `json:"next_page"`
		}
		if err := json.Unmarshal(pageRec.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode page %d: %v", pageNumber+1, err)
		}
		if len(page.Data) != 1 {
			t.Fatalf("page %d data=%d; want 1 row: %s", pageNumber+1, len(page.Data), pageRec.Body.String())
		}
		name := page.Data[0]["name"].(string)
		if seenNames[name] {
			t.Fatalf("page %d repeated Agent %s; all names so far %v", pageNumber+1, name, gotNames)
		}
		seenNames[name] = true
		gotNames = append(gotNames, name)
		if pageNumber < 2 {
			if page.NextPage == nil {
				t.Fatalf("page %d next_page = nil; want token", pageNumber+1)
			}
			if firstToken == "" {
				firstToken = *page.NextPage
				assertAgentPageTokenRedacted(t, firstToken)
			}
			pagePath = "/v1/agents?limit=1&page=" + *page.NextPage
		} else if page.NextPage != nil {
			t.Fatalf("terminal next_page = %q; want nil", *page.NextPage)
		}
	}
	for _, want := range []string{"page-a", "page-b", "page-c"} {
		if !seenNames[want] {
			t.Fatalf("paginated names = %v; missing %s", gotNames, want)
		}
	}
	if gotNames[2] != "page-c" {
		t.Fatalf("paginated names = %v; fractional timestamp Agent must sort after exact-second peers", gotNames)
	}

	for _, path := range []string{
		"/v1/agents?limit=1&page=not-a-token",
		"/v1/agents?limit=1&page=" + tamperLastByte(firstToken),
		"/v1/agents?limit=1&page=" + firstToken + "&include_archived=true",
		"/v1/agents?limit=1&page=" + firstToken + "&created_at%5Bgte%5D=2026-01-01T00%3A00%3A00Z",
		"/v1/agents?limit=1&page=" + firstToken + "&created_at%5Blte%5D=2026-01-01T23%3A00%3A00Z",
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d: %s", path, recorder.Code, recorder.Body.String())
		}
	}

	crossWorkspaceRouter := newAgentTestRouterWithStaticWorkspaceAuth(t, db, secret)
	crossRec := httptest.NewRecorder()
	crossReq := httptest.NewRequest(http.MethodGet, "/v1/agents?limit=1&page="+firstToken, nil)
	crossReq.Header.Set("x-api-key", "workspace-b-key")
	addSDKBetaQueryMarker(crossReq)
	crossWorkspaceRouter.ServeHTTP(crossRec, crossReq)
	if crossRec.Code != http.StatusBadRequest {
		t.Fatalf("cross-workspace token expected 400, got %d: %s", crossRec.Code, crossRec.Body.String())
	}
}

func TestGetAgentVersionAndVersionListPagination(t *testing.T) {
	secret := bytes.Repeat([]byte{9}, 32)
	router, db := newAgentTestRouterAndDB(t, secret)
	createRec := postAgentRequest(t, router, "/v1/agents", `{"name":"versioned","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)
	if postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":1,"system":"v2"}`).Code != http.StatusOK {
		t.Fatal("update to v2 failed")
	}
	if postAgentRequest(t, router, "/v1/agents/"+agentID, `{"version":2,"system":"v3"}`).Code != http.StatusOK {
		t.Fatal("update to v3 failed")
	}

	versionOne := getAgentPath(t, router, "/v1/agents/"+agentID+"?version=1")
	var snapshot map[string]any
	_ = json.Unmarshal(versionOne.Body.Bytes(), &snapshot)
	if snapshot["version"].(float64) != 1 || snapshot["system"] != "v1" {
		t.Fatalf("version=1 snapshot = %+v; want v1", snapshot)
	}
	for _, path := range []string{"/v1/agents/" + agentID + "?version=0", "/v1/agents/" + agentID + "?version=abc", "/v1/agents/" + agentID + "?version=999", "/v1/agents/" + agentID + "?version=1&version=2", "/v1/agents/" + agentID + "?unknown=x"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
		if rec.Code < 400 {
			t.Fatalf("%s: expected error, got %d: %s", path, rec.Code, rec.Body.String())
		}
	}

	first := getAgentPath(t, router, "/v1/agents/"+agentID+"/versions?limit=2")
	var firstPage struct {
		Data     []json.RawMessage `json:"data"`
		NextPage *string           `json:"next_page"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first version page: %v", err)
	}
	if len(firstPage.Data) != 2 || firstPage.NextPage == nil {
		t.Fatalf("first version page data=%d next=%v; want 2 rows and token", len(firstPage.Data), firstPage.NextPage)
	}
	for _, item := range firstPage.Data {
		assertAgentResponseShape(t, item)
	}
	assertAgentPageTokenRedacted(t, *firstPage.NextPage)

	second := getAgentPath(t, router, "/v1/agents/"+agentID+"/versions?limit=2&page="+*firstPage.NextPage)
	var secondPage struct {
		Data     []map[string]any `json:"data"`
		NextPage *string          `json:"next_page"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second version page: %v", err)
	}
	if len(secondPage.Data) != 1 || secondPage.Data[0]["version"].(float64) != 3 {
		t.Fatalf("second version page = %+v; want version 3", secondPage.Data)
	}
	if secondPage.NextPage != nil {
		t.Fatalf("terminal version next_page = %q; want nil", *secondPage.NextPage)
	}

	otherRec := postAgentRequest(t, router, "/v1/agents", `{"name":"other-versioned","model":"anthropic/claude-opus-4-8"}`)
	var other map[string]any
	_ = json.Unmarshal(otherRec.Body.Bytes(), &other)
	invalidPaths := []string{
		"/v1/agents/" + agentID + "/versions?limit=0",
		"/v1/agents/" + agentID + "/versions?limit=-1",
		"/v1/agents/" + agentID + "/versions?limit=abc",
		"/v1/agents/" + agentID + "/versions?limit=1&limit=2",
		"/v1/agents/" + agentID + "/versions?page=x&page=y",
		"/v1/agents/" + agentID + "/versions?unknown=x",
		"/v1/agents/" + agentID + "/versions?limit=1&page=not-a-token",
		"/v1/agents/" + agentID + "/versions?limit=1&page=" + tamperLastByte(*firstPage.NextPage),
		"/v1/agents/" + other["id"].(string) + "/versions?limit=1&page=" + *firstPage.NextPage,
		"/v1/agents?limit=1&page=" + *firstPage.NextPage,
	}
	for _, path := range invalidPaths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d: %s", path, rec.Code, rec.Body.String())
		}
	}

	crossWorkspaceRouter := newAgentTestRouterWithStaticWorkspaceAuth(t, db, secret)
	crossRec := httptest.NewRecorder()
	crossReq := httptest.NewRequest(http.MethodGet, "/v1/agents/"+agentID+"/versions?limit=2&page="+*firstPage.NextPage, nil)
	crossReq.Header.Set("x-api-key", "workspace-b-key")
	addSDKBetaQueryMarker(crossReq)
	crossWorkspaceRouter.ServeHTTP(crossRec, crossReq)
	if crossRec.Code != http.StatusBadRequest {
		t.Fatalf("cross-workspace version token expected 400, got %d: %s", crossRec.Code, crossRec.Body.String())
	}
}

func TestAgentVersionListLimitDefaultsAndClampsAtOneHundred(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := postAgentRequest(t, router, "/v1/agents", `{"name":"many-versions","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)
	for version := 1; version < 101; version++ {
		nextVersion := version + 1
		system := "v-" + string(rune('a'+(version%26))) + "-" + string(rune('a'+(version/26)))
		rawBody, err := json.Marshal(map[string]any{"version": version, "system": system})
		if err != nil {
			t.Fatalf("marshal update body: %v", err)
		}
		rec := postAgentRequest(t, router, "/v1/agents/"+agentID, string(rawBody))
		if rec.Code != http.StatusOK {
			t.Fatalf("update to version %d: %d %s", nextVersion, rec.Code, rec.Body.String())
		}
	}

	defaultRec := getAgentPath(t, router, "/v1/agents/"+agentID+"/versions")
	var defaultPage struct {
		Data     []json.RawMessage `json:"data"`
		NextPage *string           `json:"next_page"`
	}
	if err := json.Unmarshal(defaultRec.Body.Bytes(), &defaultPage); err != nil {
		t.Fatalf("decode default version page: %v", err)
	}
	if len(defaultPage.Data) != 20 || defaultPage.NextPage == nil {
		t.Fatalf("default version page data=%d next=%v; want 20 rows and token", len(defaultPage.Data), defaultPage.NextPage)
	}

	cappedRec := getAgentPath(t, router, "/v1/agents/"+agentID+"/versions?limit=101")
	var cappedPage struct {
		Data     []json.RawMessage `json:"data"`
		NextPage *string           `json:"next_page"`
	}
	if err := json.Unmarshal(cappedRec.Body.Bytes(), &cappedPage); err != nil {
		t.Fatalf("decode capped version page: %v", err)
	}
	if len(cappedPage.Data) != 100 || cappedPage.NextPage == nil {
		t.Fatalf("capped version page data=%d next=%v; want 100 rows and token", len(cappedPage.Data), cappedPage.NextPage)
	}
}

func TestUpdateAgentResponseShapeIsFlat(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents",
		strings.NewReader(`{"name":"shape-upd","model":"anthropic/claude-opus-4-8","system":"v1"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID,
		strings.NewReader(`{"version":1,"system":"v2"}`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	assertAgentResponseShape(t, updateRec.Body.Bytes())
}

// Patch semantics through the real Agent route.

// TestUpdateAgentPatchClearsOptionalScalarWithNull proves the
// optional-scalar clear semantic (description, system) reaches the
// store via the route and is reflected in the response.
func TestUpdateAgentPatchClearsOptionalScalarWithNull(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents",
		strings.NewReader(`{"name":"opt","model":"anthropic/claude-opus-4-8","system":"keep-or-clear"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID,
		strings.NewReader(`{"version":1,"system":null}`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	var result map[string]any
	_ = json.Unmarshal(updateRec.Body.Bytes(), &result)
	if result["system"] != nil {
		t.Errorf("system must clear to null after null patch; got %v", result["system"])
	}
	if result["version"].(float64) != 2 {
		t.Errorf("expected version 2 after non-no-op patch; got %v", result["version"])
	}
}

// TestUpdateAgentPatchMergesMetadata proves the key-by-key metadata
// merge (upsert + per-key delete) reaches the store via the route.
func TestUpdateAgentPatchMergesMetadata(t *testing.T) {
	router := newAgentTestRouter(t)
	// Seed: {team:core, env:prod}
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents",
		strings.NewReader(`{"name":"meta","model":"anthropic/claude-opus-4-8","metadata":{"team":"core","env":"prod"}}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("setup create: expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	// Patch upserts region, deletes env, leaves team alone.
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID,
		strings.NewReader(`{"version":1,"metadata":{"region":"us","env":null}}`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	var result map[string]any
	_ = json.Unmarshal(updateRec.Body.Bytes(), &result)
	meta, _ := result["metadata"].(map[string]any)
	if meta == nil {
		t.Fatalf("metadata is nil; result=%v", result)
	}
	if meta["region"] != "us" {
		t.Errorf("metadata.region must be 'us'; got %v", meta["region"])
	}
	if meta["team"] != "core" {
		t.Errorf("metadata.team must be preserved; got %v", meta["team"])
	}
	if _, present := meta["env"]; present {
		t.Errorf("metadata.env must be deleted by per-key null; got %v", meta)
	}
}

// TestUpdateAgentPatchMissingVersionRejects proves the patch decoder
// rejects a body without `version` at the wire boundary, before any
// store access. The message must mention "version".
func TestUpdateAgentPatchMissingVersionRejects(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/agents",
		strings.NewReader(`{"name":"need-version","model":"anthropic/claude-opus-4-8"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/agents/"+agentID,
		strings.NewReader(`{"system":"v2"}`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing version, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(updateRec.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	if errObj["type"] != "invalid_request_error" {
		t.Errorf("error.type = %v; want invalid_request_error", errObj["type"])
	}
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "version") {
		t.Errorf("error.message = %q; want it to mention version", msg)
	}
}

func TestUpdateAgentPrevalidatesInvalidPatchBeforeNotFound(t *testing.T) {
	router := newAgentTestRouter(t)

	cases := []struct {
		name      string
		body      string
		fieldHint string
	}{
		{"required scalar clear", `{"version":1,"name":null}`, "name"},
		{"malformed array", `{"version":1,"tools":{}}`, "tools"},
		{"invalid metadata", `{"version":1,"metadata":{"k":42}}`, "metadata"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := postAgentRequest(t, router, "/v1/agents/agent_does_not_exist", tc.body)

			assertAgentInvalidRequestError(t, recorder)
			var response map[string]any
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			errObj, _ := response["error"].(map[string]any)
			msg, _ := errObj["message"].(string)
			if !strings.Contains(msg, tc.fieldHint) {
				t.Errorf("error.message = %q; want it to mention %s", msg, tc.fieldHint)
			}
		})
	}
}

// Agent request body size boundary.

// TestCreateAgentRejectsOversizedBody proves a body that exceeds the
// 1 MiB cap is rejected at the wire boundary before any store call.
// The response is a centralized 413 request_too_large envelope.
func TestCreateAgentRejectsOversizedBody(t *testing.T) {
	router := newAgentTestRouter(t)
	pad := strings.Repeat("a", (1<<20)+1024) // > 1 MiB
	body := `{"name":"big","model":"anthropic/claude-opus-4-8","system":"` + pad + `"}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentRequestTooLarge(t, recorder)
	assertAgentListCount(t, router, 0)
}

// TestUpdateAgentRejectsOversizedBody — same 413 cap on the patch route;
// rejected update must not bump version or change stored content.
func TestUpdateAgentRejectsOversizedBody(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)

	pad := strings.Repeat("a", (1<<20)+1024)
	body := `{"version":1,"system":"` + pad + `"}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentRequestTooLarge(t, recorder)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
}

// ===== Strict create/update body decoding =====

func TestCreateAgentRejectsSecondJSONValueAndDoesNotCreate(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"strict-create","model":"anthropic/claude-opus-4-8"}` +
		`{"name":"second","model":"anthropic/claude-opus-4-8"}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentListCount(t, router, 0)
}

func TestUpdateAgentRejectsSecondJSONValueWithoutVersionBump(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	body := `{"version":1,"system":"v2"}` + `{"version":1,"system":"v3"}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
}

// Oversized trailing padding after an otherwise valid JSON object must
// also surface as 413 request_too_large through the centralized error
// envelope. The trailing-data MaxBytesError is the same fault class as
// a body whose first JSON value already exceeded the cap.
func TestCreateAgentRejectsOversizedTrailingPaddingAndDoesNotCreate(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"strict-padding","model":"anthropic/claude-opus-4-8"}` +
		strings.Repeat(" ", (1<<20)+1024)

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentRequestTooLarge(t, recorder)
	assertAgentListCount(t, router, 0)
}

func TestUpdateAgentRejectsOversizedTrailingPaddingWithoutVersionBump(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	body := `{"version":1,"system":"v2"}` + strings.Repeat(" ", (1<<20)+1024)

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentRequestTooLarge(t, recorder)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
}

// Strict top-level Agent field rejection.

// TestCreateAgentRejectsUnknownTopLevelField requires unknown create top-level
// fields to reject before store writes. The error must be 400
// invalid_request_error and no Agent row is created.
func TestCreateAgentRejectsUnknownTopLevelField(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"unk","model":"anthropic/claude-opus-4-8","extra_field":"x"}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "extra_field") {
		t.Errorf("error.message = %q; want it to name the unknown field", msg)
	}
	assertAgentListCount(t, router, 0)
}

// TestUpdateAgentRejectsUnknownTopLevelField allows update keys = create keys
// + version. Anything else rejects, and the rejection must happen before the
// store loads the target Agent, so version stays put on a real ID and the
// response is 400 not 404.
func TestUpdateAgentRejectsUnknownTopLevelField(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	body := `{"version":1,"system":"v2","extra_field":true}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "extra_field") {
		t.Errorf("error.message = %q; want it to name the unknown field", msg)
	}
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
}

// Strict create-body type semantics.

// TestCreateAgentRejectsNullArrayContainers pins create body type semantics:
// tools/mcp_servers/metadata null reject as strict type errors. Skills null is
// the deliberate exception covered separately.
func TestCreateAgentRejectsNullArrayContainers(t *testing.T) {
	router := newAgentTestRouter(t)
	cases := []struct {
		field string
		body  string
	}{
		{"tools", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":null}`},
		{"mcp_servers", `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":null}`},
		{"metadata", `{"name":"x","model":"anthropic/claude-opus-4-8","metadata":null}`},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			recorder := postAgentRequest(t, router, "/v1/agents", tc.body)
			assertAgentInvalidRequestError(t, recorder)
			var response map[string]any
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			errObj, _ := response["error"].(map[string]any)
			msg, _ := errObj["message"].(string)
			if !strings.Contains(msg, tc.field) {
				t.Errorf("error.message = %q; want it to mention %q", msg, tc.field)
			}
		})
	}
	assertAgentListCount(t, router, 0)
}

func TestCreateAgentAcceptsNullOptionalScalarsAsAbsent(t *testing.T) {
	router := newAgentTestRouter(t)
	cases := []string{"description", "system"}
	for _, field := range cases {
		t.Run(field, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","` + field + `":null}`
			recorder := postAgentRequest(t, router, "/v1/agents", body)
			if recorder.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
			}
			var response map[string]any
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			if response[field] != nil {
				t.Errorf("%s = %v; want null", field, response[field])
			}
		})
	}
}

func TestCreateAgentRejectsNullSkills(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"skills-null","model":"anthropic/claude-opus-4-8","skills":null}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentListCount(t, router, 0)
}

// Generic Agent limits at the HTTP boundary.

// TestCreateAgentRejectsOverlongName — name > 256 Unicode code points
// rejects with 400 invalid_request_error and no Agent row is created.
func TestCreateAgentRejectsOverlongName(t *testing.T) {
	router := newAgentTestRouter(t)
	overlong := strings.Repeat("a", 257)
	body := `{"name":"` + overlong + `","model":"anthropic/claude-opus-4-8"}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "name") {
		t.Errorf("error.message = %q; want it to mention name", msg)
	}
	assertAgentListCount(t, router, 0)
}

// TestCreateAgentRejectsOverlongMetadataKey — metadata key length cap
// is 64 Unicode code points; over the cap returns 400.
func TestCreateAgentRejectsOverlongMetadataKey(t *testing.T) {
	router := newAgentTestRouter(t)
	overlongKey := strings.Repeat("k", 65)
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","metadata":{"` + overlongKey + `":"v"}}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentListCount(t, router, 0)
}

// TestCreateAgentRejectsTooManyTools — tools cardinality cap is 128
// entries; one over rejects.
func TestCreateAgentRejectsTooManyTools(t *testing.T) {
	router := newAgentTestRouter(t)
	entries := make([]string, 129)
	for i := range entries {
		entries[i] = `{"type":"tetral_agent_toolset","family":"claude"}`
	}
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[` +
		strings.Join(entries, ",") + `]}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentListCount(t, router, 0)
}

// Generic limits are enforced after patch materialization. These tests exercise
// the update path end-to-end so removing ValidateGenericLimits from
// Service.UpdatePatch fails even when create-path validation remains intact.

func TestUpdateAgentRejectsOverlongMetadataKeyAfterMaterialize(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)

	overlongKey := strings.Repeat("k", 65) // > 64 code points
	body := `{"version":1,"metadata":{"` + overlongKey + `":"v"}}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
}

func TestUpdateAgentRejectsTooManyToolsAfterMaterialize(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)

	entries := make([]string, 129) // > 128 cap
	for i := range entries {
		entries[i] = `{"type":"tetral_agent_toolset","family":"claude"}`
	}
	body := `{"version":1,"tools":[` + strings.Join(entries, ",") + `]}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
}

// TestUpdateAgentRejectsInvalidPatchEvenWhenOtherwiseNoOp proves validation
// precedes no-op detection. The patch repeats the current value of system but adds a metadata
// pair whose key is over the 64-cp limit. ValidateGenericLimits must
// fire before the no-op DeepEqual short-circuit; otherwise an
// invalid-but-otherwise-equal target would silently return version 1.
func TestUpdateAgentRejectsInvalidPatchEvenWhenOtherwiseNoOp(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)

	overlongKey := strings.Repeat("k", 65)
	body := `{"version":1,"system":"v1","metadata":{"` + overlongKey + `":"v"}}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected invalid no-op patch", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected patch", agentResponse["system"])
	}
	meta, _ := agentResponse["metadata"].(map[string]any)
	if _, present := meta[overlongKey]; present {
		t.Errorf("metadata must not contain the rejected key; got %v", meta)
	}
}

// End-to-end normalization, cross-array validation, and no-op behavior.

// TestCreateAgentTetralToolsetReturnsCanonicalConfig proves the stored shorthand
// is projected into the SDK-compatible resolved response policy.
func TestCreateAgentTetralToolsetReturnsCanonicalConfig(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"shorthand","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`

	rec := postAgentRequest(t, router, "/v1/agents", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	want := `"tools":[{"type":"tetral_agent_toolset","family":"claude","default_config":{"enabled":true,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"read","enabled":true,"permission_policy":{"type":"always_allow"}},{"name":"grep","enabled":true,"permission_policy":{"type":"always_allow"}},{"name":"glob","enabled":true,"permission_policy":{"type":"always_allow"}}]}]`
	if !strings.Contains(rec.Body.String(), want) {
		t.Errorf("response missing canonical tools fragment %q\nbody=%s", want, rec.Body.String())
	}
}

func TestCreateAgentRejectsTetralToolsetMissingFamily(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"missing-family","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset"}]}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentListCount(t, router, 0)
}

func TestCreateAgentMCPToolsetEmptyDefaultConfigReturnsCanonicalObject(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"mcp-default","model":"anthropic/claude-opus-4-8",` +
		`"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],` +
		`"tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{}}]}`

	rec := postAgentRequest(t, router, "/v1/agents", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	want := `"default_config":{"enabled":true,"permission_policy":{"type":"always_ask"}}`
	if !strings.Contains(rec.Body.String(), want) {
		t.Errorf("response missing canonical empty default_config; body=%s", rec.Body.String())
	}
}

// TestCreateAgentMCPToolsetExplicitAlwaysAllowReturnsAlwaysAllow —
// explicit policy passes through unchanged.
func TestCreateAgentMCPToolsetExplicitAlwaysAllowReturnsAlwaysAllow(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"mcp-explicit","model":"anthropic/claude-opus-4-8",` +
		`"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],` +
		`"tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"permission_policy":{"type":"always_allow"}}}]}`

	rec := postAgentRequest(t, router, "/v1/agents", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	want := `"default_config":{"enabled":true,"permission_policy":{"type":"always_allow"}}`
	if !strings.Contains(rec.Body.String(), want) {
		t.Errorf("response missing explicit always_allow; body=%s", rec.Body.String())
	}
}

func TestCreateAgentRejectsUnsafeMCPServerURLsWithoutPersisting(t *testing.T) {
	cases := []struct {
		name     string
		url      string
		mustOmit []string
	}{
		{ //nolint:gosec // G101: deliberate fixture proving secret-bearing URL rejection.
			name:     "embedded credentials",
			url:      "https://user:pass@example.com/mcp",
			mustOmit: []string{"https://user:pass@example.com/mcp", "user:pass", "user", "pass"},
		},
		{
			name:     "query string",
			url:      "https://api.githubcopilot.com/mcp/?token=secret",
			mustOmit: []string{"https://api.githubcopilot.com/mcp/?token=secret", "token=secret", "token", "secret"},
		},
		{
			name:     "fragment",
			url:      "https://api.githubcopilot.com/mcp/#token",
			mustOmit: []string{"https://api.githubcopilot.com/mcp/#token", "#token", "token"},
		},
		{
			name:     "localhost",
			url:      "https://localhost/mcp",
			mustOmit: []string{"https://localhost/mcp"},
		},
		{
			name:     "legacy IPv4 literal",
			url:      "https://2130706433/mcp",
			mustOmit: []string{"https://2130706433/mcp"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := newAgentTestRouter(t)
			body := `{"name":"unsafe-mcp","model":"anthropic/claude-opus-4-8",` +
				`"mcp_servers":[{"type":"url","name":"github","url":"` + tc.url + `"}]}`

			recorder := postAgentRequest(t, router, "/v1/agents", body)

			assertAgentInvalidRequestError(t, recorder)
			assertAgentErrorBodyOmits(t, recorder, tc.mustOmit...)
			assertAgentListCount(t, router, 0)
		})
	}
}

func TestCreateAgentRejectsNonCatalogMCPServerURLWithoutPersisting(t *testing.T) {
	router := newAgentTestRouter(t)
	rawURL := "https://api.githubcopilot.com/mcp/extra-secret"
	body := `{"name":"non-catalog-mcp","model":"anthropic/claude-opus-4-8",` +
		`"mcp_servers":[{"type":"url","name":"github","url":"` + rawURL + `"}]}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentErrorBodyOmits(t, recorder, rawURL, "extra-secret")
	assertAgentListCount(t, router, 0)
}

func TestUpdateAgentRejectsUnsafeMCPServerURLWithoutPersisting(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	body := `{"version":1,"system":"mutated",` +
		`"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/?token=secret"}]}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentErrorBodyOmits(t, recorder, "https://api.githubcopilot.com/mcp/?token=secret", "token=secret", "token", "secret")
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["name"] != "strict-update" {
		t.Fatalf("name = %v; want strict-update after rejected update", agentResponse["name"])
	}
	if model, ok := agentResponse["model"].(map[string]any); !ok || model["id"] != "anthropic/claude-opus-4-8" {
		t.Fatalf("model = %v; want anthropic/claude-opus-4-8 after rejected update", agentResponse["model"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
	mcpServers, _ := agentResponse["mcp_servers"].([]any)
	if len(mcpServers) != 0 {
		t.Errorf("mcp_servers = %v; want [] after rejected update", mcpServers)
	}
}

func TestUpdateAgentRejectsNonCatalogMCPServerURLWithoutPersisting(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	rawURL := "https://not-github.example.com/mcp"
	body := `{"version":1,"system":"mutated",` +
		`"mcp_servers":[{"type":"url","name":"github","url":"` + rawURL + `"}]}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	assertAgentErrorBodyOmits(t, recorder, rawURL)
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
	mcpServers, _ := agentResponse["mcp_servers"].([]any)
	if len(mcpServers) != 0 {
		t.Errorf("mcp_servers = %v; want [] after rejected update", mcpServers)
	}
}

func TestCreateAgentRejectsNullDefaultConfigAndDoesNotCreate(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"null-default","model":"anthropic/claude-opus-4-8",` +
		`"tools":[{"type":"tetral_agent_toolset","family":"claude","default_config":null}]}`

	recorder := postAgentRequest(t, router, "/v1/agents", body)

	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "tools[0].default_config") {
		t.Errorf("error.message = %q; want it to name tools[0].default_config", msg)
	}
	assertAgentListCount(t, router, 0)
}

func TestUpdateAgentRejectsNullDefaultConfigWithoutVersionBump(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)
	body := `{"version":1,` +
		`"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],` +
		`"tools":[{"type":"tetral_agent_toolset","family":"claude"},` +
		`{"type":"mcp_toolset","mcp_server_name":"github","default_config":null}]}`

	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "tools[1].default_config") {
		t.Errorf("error.message = %q; want it to name tools[1].default_config", msg)
	}
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
	tools, _ := agentResponse["tools"].([]any)
	if len(tools) != 1 {
		t.Errorf("tools = %v; want the original family declaration after rejected update", tools)
	}
}

// TestGetAndListReturnSameNormalizedConfigAfterCreate proves read paths apply
// the same response-only projection to the same stored canonical bytes.
func TestGetAndListReturnSameNormalizedConfigAfterCreate(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"read-shape","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`
	createRec := postAgentRequest(t, router, "/v1/agents", body)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create: expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	want := `"tools":[{"type":"tetral_agent_toolset","family":"claude","default_config":{"enabled":true,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"read","enabled":true,"permission_policy":{"type":"always_allow"}},{"name":"grep","enabled":true,"permission_policy":{"type":"always_allow"}},{"name":"glob","enabled":true,"permission_policy":{"type":"always_allow"}}]}]`

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/agents/"+agentID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)
	if !strings.Contains(getRec.Body.String(), want) {
		t.Errorf("Get response missing canonical tools fragment; body=%s", getRec.Body.String())
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	setAuthHeader(listReq)
	router.ServeHTTP(listRec, listReq)
	if !strings.Contains(listRec.Body.String(), want) {
		t.Errorf("List response missing canonical tools fragment; body=%s", listRec.Body.String())
	}
}

// TestUpdateAgentShorthandReplayReturnsSameVersion proves re-sending the same
// shorthand the first create normalized returns the existing version.
func TestUpdateAgentShorthandReplayReturnsSameVersion(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"replay","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`
	createRec := postAgentRequest(t, router, "/v1/agents", body)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create: expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	// Replay: same shorthand, version 1.
	patchBody := `{"version":1,"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`
	patchRec := postAgentRequest(t, router, "/v1/agents/"+agentID, patchBody)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch: expected 200, got %d: %s", patchRec.Code, patchRec.Body.String())
	}
	var updated map[string]any
	_ = json.Unmarshal(patchRec.Body.Bytes(), &updated)
	if updated["version"].(float64) != 1 {
		t.Errorf("expected version 1 after no-op shorthand replay; got %v", updated["version"])
	}
}

// TestCreateAgentRejectsCrossArrayMissingReference — cross-array reject
// surfaces as 400 invalid_request_error and creates no Agent.
func TestCreateAgentRejectsCrossArrayMissingReference(t *testing.T) {
	router := newAgentTestRouter(t)
	body := `{"name":"xref","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"missing","default_config":{"permission_policy":{"type":"always_ask"}}}]}`

	rec := postAgentRequest(t, router, "/v1/agents", body)
	assertAgentInvalidRequestError(t, rec)
	var response map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "missing") {
		t.Errorf("error.message = %q; want it to name the unresolved reference", msg)
	}
	assertAgentListCount(t, router, 0)
}

// TestUpdateAgentRejectsCrossArrayMissingReference is symmetric to the
// create-side test: an update whose materialized config trips a
// cross-array reject must surface 400 invalid_request_error and must
// not bump the existing version.
func TestUpdateAgentRejectsCrossArrayMissingReference(t *testing.T) {
	router := newAgentTestRouter(t)
	createRec := postAgentRequest(t, router, "/v1/agents",
		`{"name":"xref-update","model":"anthropic/claude-opus-4-8","system":"v1"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("setup create: expected 200, got %d: %s", createRec.Code, createRec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	agentID := created["id"].(string)

	body := `{"version":1,"tools":[{"type":"tetral_agent_toolset","family":"claude"},` +
		`{"type":"mcp_toolset","mcp_server_name":"missing","default_config":{"permission_policy":{"type":"always_ask"}}}]}`
	rec := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, rec)
	var response map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "missing") {
		t.Errorf("error.message = %q; want it to name the unresolved reference", msg)
	}
	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 (no overwrite after rejected update)", agentResponse["system"])
	}
	tools, _ := agentResponse["tools"].([]any)
	if len(tools) != 1 {
		t.Errorf("tools = %v; want the original family declaration (no rewrite after rejected update)", tools)
	}
}

func TestUpdateAgentToolFormatErrorWinsOverCrossArray(t *testing.T) {
	router := newAgentTestRouter(t)
	agentID := createAgentForStrictDecodeTest(t, router)

	body := `{"version":1,"system":"mutated",` +
		`"tools":[{"type":"tetral_agent_toolset","family":"claude","default_config":null},` +
		`{"type":"mcp_toolset","mcp_server_name":"missing","default_config":{"permission_policy":{"type":"always_ask"}}}]}`
	recorder := postAgentRequest(t, router, "/v1/agents/"+agentID, body)

	assertAgentInvalidRequestError(t, recorder)
	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "default_config") {
		t.Errorf("error.message = %q; expected tool format error to win", msg)
	}
	if strings.Contains(msg, "missing") {
		t.Errorf("error.message = %q; cross-array error must NOT have surfaced first", msg)
	}

	agentResponse := getAgentForStrictDecodeTest(t, router, agentID)
	if agentResponse["version"].(float64) != 1 {
		t.Fatalf("version = %v; want 1 after rejected update", agentResponse["version"])
	}
	if agentResponse["system"] != "v1" {
		t.Fatalf("system = %v; want v1 after rejected update", agentResponse["system"])
	}
	tools, _ := agentResponse["tools"].([]any)
	if len(tools) != 1 {
		t.Errorf("tools = %v; want the original family declaration after rejected update", tools)
	}
}

func TestCreateAgentToolFormatErrorWinsOverCrossArray(t *testing.T) {
	router := newAgentTestRouter(t)
	// Body has TWO problems:
	//   1. Tool format: tools[0] has an unsupported nested key
	//   2. Cross-array: tools[1] mcp_toolset references a missing server
	body := `{"name":"order","model":"anthropic/claude-opus-4-8",` +
		`"tools":[{"type":"tetral_agent_toolset","family":"claude","default_config":null},` +
		`{"type":"mcp_toolset","mcp_server_name":"missing","default_config":{"permission_policy":{"type":"always_ask"}}}]}`

	rec := postAgentRequest(t, router, "/v1/agents", body)
	assertAgentInvalidRequestError(t, rec)
	var response map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "default_config") {
		t.Errorf("error.message = %q; expected tool format error to win", msg)
	}
	if strings.Contains(msg, "missing") {
		t.Errorf("error.message = %q; cross-array error must NOT have surfaced first", msg)
	}
}
