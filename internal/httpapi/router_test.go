package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentpkg "github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// newTestHandler creates a SessionHandler backed by a lightweight fake service.
// Suitable for router-level tests that don't exercise session logic.
func newTestHandler(t *testing.T) *httpapi.SessionHandler {
	t.Helper()
	return httpapi.NewSessionHandler(fakeSessionService{})
}

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	return newAuthenticatedRouter(t, newTestHandler(t))
}

type fakeSessionService struct{}

func (fakeSessionService) Create(context.Context, workspace.ID, session.CreateRequest) (*session.Response, error) {
	now := time.Now().UTC()
	return &session.Response{
		ID:            "sesn_router",
		Type:          "session",
		Status:        session.StatusIdle,
		Agent:         fakePublicAgent(now),
		EnvironmentID: "env_router",
		Metadata:      map[string]string{},
		VaultIDs:      []string{},
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func (fakeSessionService) Get(context.Context, workspace.ID, string) (*session.Response, error) {
	now := time.Now().UTC()
	return &session.Response{
		ID:            "sesn_router",
		Type:          "session",
		Status:        session.StatusIdle,
		Agent:         fakePublicAgent(now),
		EnvironmentID: "env_router",
		Metadata:      map[string]string{},
		VaultIDs:      []string{},
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func fakePublicAgent(_ time.Time) *session.SessionAgentResponse {
	return &session.SessionAgentResponse{
		ID:           "agent_router",
		Type:         "agent",
		Version:      1,
		Name:         "router-agent",
		Model:        agentpkg.ModelConfig{ID: "anthropic/claude-opus-4-8"},
		ApprovalMode: agentpkg.ApprovalModeAskForApproval,
		Tools:        agentpkg.RawArray{},
		MCPServers:   agentpkg.RawArray{},
		Skills:       agentpkg.RawArray{},
	}
}

func (fakeSessionService) List(context.Context, workspace.ID, session.ListOptions) (*session.ListResult, error) {
	return &session.ListResult{Data: []*session.Response{}}, nil
}

func (fakeSessionService) ListThreads(context.Context, workspace.ID, string, session.ThreadListOptions) (*session.ThreadListResult, error) {
	response := fakeThreadResponse()
	return &session.ThreadListResult{Data: []*session.ThreadResponse{&response}}, nil
}

func (fakeSessionService) GetThread(context.Context, workspace.ID, string, string) (*session.ThreadResponse, error) {
	response := fakeThreadResponse()
	return &response, nil
}

func (fakeSessionService) ArchiveThread(context.Context, workspace.ID, string, string) (*session.ThreadResponse, error) {
	response := fakeThreadResponse()
	now := time.Now().UTC()
	response.ArchivedAt = &now
	response.UpdatedAt = now
	return &response, nil
}

func (fakeSessionService) Update(context.Context, workspace.ID, string, session.UpdateRequest) (*session.Response, error) {
	return fakeSessionService{}.Get(context.Background(), workspace.DefaultID, "sesn_router")
}

func (fakeSessionService) Archive(context.Context, workspace.ID, string) (*session.Response, error) {
	return fakeSessionService{}.Get(context.Background(), workspace.DefaultID, "sesn_router")
}

func (fakeSessionService) Delete(context.Context, workspace.ID, string) (*session.DeleteResponse, error) {
	return &session.DeleteResponse{ID: "sesn_router", Type: "session_deleted"}, nil
}

func (fakeSessionService) AddResource(context.Context, workspace.ID, string, session.ResourceRequest) (*session.ResourceResponse, error) {
	now := time.Now().UTC()
	return &session.ResourceResponse{
		ID:        "sesrsc_router",
		Type:      string(session.ResourceTypeFile),
		FileID:    "file_router",
		MountPath: "/workspace/router.txt",
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func (fakeSessionService) ListResources(context.Context, workspace.ID, string, session.ResourceListOptions) (*session.ResourceListResult, error) {
	return &session.ResourceListResult{Data: []*session.ResourceResponse{}}, nil
}

func (fakeSessionService) GetResource(context.Context, workspace.ID, string, string) (*session.ResourceResponse, error) {
	return fakeSessionService{}.AddResource(context.Background(), workspace.DefaultID, "sesn_router", session.ResourceRequest{})
}

func (fakeSessionService) UpdateResource(context.Context, workspace.ID, string, string, string) (*session.ResourceResponse, error) {
	return &session.ResourceResponse{ID: "sesrsc_router", Type: string(session.ResourceTypeGitHubRepository)}, nil
}

func (fakeSessionService) DeleteResource(context.Context, workspace.ID, string, string) (*session.ResourceDeleteResponse, error) {
	return &session.ResourceDeleteResponse{ID: "sesrsc_router", Type: "session_resource_deleted"}, nil
}

func fakeThreadResponse() session.ThreadResponse {
	now := time.Now().UTC()
	return session.ThreadResponse{
		ID:        "thread_router",
		Type:      "session_thread",
		SessionID: "sesn_router",
		Agent: session.ThreadAgentResponse{
			ID:           "agent_router",
			Type:         "agent",
			Version:      1,
			Name:         "router agent",
			Model:        session.ThreadAgentModelConfig{ID: "anthropic/claude-opus-4-8"},
			ApprovalMode: "ask_for_approval",
		},
		Status:    session.StatusIdle,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestHealthReturns200(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	// No auth header — health is exempt.

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if recorder.Body.String() != "ok" {
		t.Errorf("expected body 'ok', got %q", recorder.Body.String())
	}
}

func TestHealthExemptFromAuth(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	// Explicitly no auth header.

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 for /health without auth, got %d", recorder.Code)
	}
	if recorder.Body.String() != "ok" {
		t.Errorf("expected body 'ok', got %q", recorder.Body.String())
	}
}

func TestUnsupportedGeneratedSDKSurfaceFamiliesReturnInvalidRequest(t *testing.T) {
	router := newTestRouter(t)
	routes := []struct {
		name   string
		method string
		path   string
	}{
		{name: "messages", method: http.MethodPost, path: "/v1/messages"},
		{name: "message batches", method: http.MethodGet, path: "/v1/messages/batches"},
		{name: "user profiles", method: http.MethodGet, path: "/v1/user_profiles/me"},
		{name: "webhooks", method: http.MethodPost, path: "/v1/webhooks"},
		{name: "deployments", method: http.MethodGet, path: "/v1/deployments"},
		{name: "deployment runs", method: http.MethodGet, path: "/v1/deployment_runs"},
		{name: "multiagent topology", method: http.MethodGet, path: "/v1/multiagent/topology"},
		{name: "self host environment work", method: http.MethodPost, path: "/v1/environments/env_router/work"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			assertErrorRequestID(t, recorder)
		})
	}
}

func TestUnknownV1RouteStillReturnsNotFound(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/unknown_surface", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404 body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestAllRoutesReturn501(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		method string
		path   string
	}{
		// Agents
		{http.MethodPost, "/v1/agents"},
		{http.MethodGet, "/v1/agents"},
		{http.MethodGet, "/v1/agents/agent_test"},
		{http.MethodPost, "/v1/agents/agent_test"},
		{http.MethodPost, "/v1/agents/agent_test/archive"},
		{http.MethodGet, "/v1/agents/agent_test/versions"},

		// Environments
		{http.MethodPost, "/v1/environments"},
		{http.MethodGet, "/v1/environments"},
		{http.MethodGet, "/v1/environments/env_test"},
		{http.MethodPost, "/v1/environments/env_test"},
		{http.MethodDelete, "/v1/environments/env_test"},
		{http.MethodPost, "/v1/environments/env_test/archive"},

		// Vaults
		{http.MethodPost, "/v1/vaults"},
		{http.MethodGet, "/v1/vaults"},
		{http.MethodGet, "/v1/vaults/vlt_test"},
		{http.MethodPost, "/v1/vaults/vlt_test"},
		{http.MethodDelete, "/v1/vaults/vlt_test"},
		{http.MethodPost, "/v1/vaults/vlt_test/archive"},

		// Credentials
		{http.MethodPost, "/v1/vaults/vlt_test/credentials"},
		{http.MethodGet, "/v1/vaults/vlt_test/credentials"},
		{http.MethodGet, "/v1/vaults/vlt_test/credentials/cred_test"},
		{http.MethodPost, "/v1/vaults/vlt_test/credentials/cred_test"},
		{http.MethodDelete, "/v1/vaults/vlt_test/credentials/cred_test"},
		{http.MethodPost, "/v1/vaults/vlt_test/credentials/cred_test/archive"},

		// Files
		{http.MethodPost, "/v1/files"},
		{http.MethodGet, "/v1/files"},
		{http.MethodGet, "/v1/files/file_test"},
		{http.MethodDelete, "/v1/files/file_test"},
		{http.MethodGet, "/v1/files/file_test/content"},

		// Memory stores
		{http.MethodPost, "/v1/memory_stores"},
		{http.MethodGet, "/v1/memory_stores"},
		{http.MethodGet, "/v1/memory_stores/memstore_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test"},
		{http.MethodDelete, "/v1/memory_stores/memstore_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/archive"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/memories"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memories"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memories/mem_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/memories/mem_test"},
		{http.MethodDelete, "/v1/memory_stores/memstore_test/memories/mem_test"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memory_versions"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memory_versions/memver_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/memory_versions/memver_test/redact"},

		// Skills
		{http.MethodPost, "/v1/skills"},
		{http.MethodGet, "/v1/skills"},
		{http.MethodGet, "/v1/skills/skill_test"},
		{http.MethodDelete, "/v1/skills/skill_test"},
		{http.MethodPost, "/v1/skills/skill_test/versions"},
		{http.MethodGet, "/v1/skills/skill_test/versions"},
		{http.MethodGet, "/v1/skills/skill_test/versions/1"},
		{http.MethodGet, "/v1/skills/skill_test/versions/1/content"},
		{http.MethodDelete, "/v1/skills/skill_test/versions/1"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(route.method, route.path, nil)
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusNotImplemented {
				t.Fatalf("expected 501, got %d", recorder.Code)
			}

			var response struct {
				Type  string `json:"type"`
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
				RequestID string `json:"request_id"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("invalid JSON response: %v", err)
			}
			if response.Type != "error" {
				t.Errorf("expected type 'error', got %q", response.Type)
			}
			if response.Error.Type != "not_implemented" {
				t.Errorf("expected error type 'not_implemented', got %q", response.Error.Type)
			}
			if response.RequestID == "" {
				t.Error("request_id is empty")
			}
		})
	}
}

func TestFileRoutesWithFileHandlerOptionInstallsRealReceivers(t *testing.T) {
	store := &recordingFileService{
		createResult: &files.FileMetadata{ID: "file_create", Type: "file", Filename: "created.bin", MIMEType: "application/octet-stream"},
		listResult: files.ListResult{Data: []*files.FileMetadata{{
			ID: "file_list", Type: "file", Filename: "listed.bin", MIMEType: "application/octet-stream",
		}}},
		getResult:    &files.FileMetadata{ID: "file_get", Type: "file", Filename: "got.bin", MIMEType: "application/octet-stream"},
		deleteResult: &files.DeleteResponse{ID: "file_delete", Type: "file_deleted"},
		openResult: &files.ContentStream{
			Metadata: &files.FileMetadata{ID: "file_content", Type: "file", Filename: "content.txt", MIMEType: "text/plain"},
			Reader:   io.NopCloser(strings.NewReader("content")),
		},
	}
	handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
	uploadBody, uploadContentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", body: []byte("upload")}})

	routes := []struct {
		method      string
		path        string
		contentType string
		body        io.Reader
	}{
		{method: http.MethodPost, path: "/v1/files", contentType: uploadContentType, body: uploadBody},
		{method: http.MethodGet, path: "/v1/files"},
		{method: http.MethodGet, path: "/v1/files/file_get"},
		{method: http.MethodDelete, path: "/v1/files/file_delete"},
		{method: http.MethodGet, path: "/v1/files/file_content/content"},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(route.method, route.path, route.body)
			if route.contentType != "" {
				request.Header.Set("Content-Type", route.contentType)
			}
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code == http.StatusNotImplemented {
				t.Fatalf("Files route was served by stub: %s", recorder.Body.String())
			}
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if store.createCalls != 1 || store.listCalls != 1 || store.getCalls != 1 || store.deleteCalls != 1 || store.openCalls != 1 {
		t.Fatalf("route calls = create:%d list:%d get:%d delete:%d open:%d; want all 1",
			store.createCalls, store.listCalls, store.getCalls, store.deleteCalls, store.openCalls)
	}
}

func TestEnvironmentHandlerOnlyMakesEnvironmentArchiveLive(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	createRecorder := httptest.NewRecorder()
	createRequest := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(`{"name":"archive-live","config":{}}`))
	createRequest.Header.Set("Content-Type", "application/json")
	setAuthHeader(createRequest)
	router.ServeHTTP(createRecorder, createRequest)
	if createRecorder.Code != http.StatusOK {
		t.Fatalf("create environment status = %d; body=%s", createRecorder.Code, createRecorder.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(createRecorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	archiveRecorder := httptest.NewRecorder()
	archiveRequest := httptest.NewRequest(http.MethodPost, "/v1/environments/"+created.ID+"/archive", nil)
	setAuthHeader(archiveRequest)
	router.ServeHTTP(archiveRecorder, archiveRequest)
	if archiveRecorder.Code == http.StatusNotImplemented {
		t.Fatalf("Environment archive routed to stub: %s", archiveRecorder.Body.String())
	}
	if archiveRecorder.Code != http.StatusOK {
		t.Fatalf("Environment archive status = %d; want 200 body=%s", archiveRecorder.Code, archiveRecorder.Body.String())
	}

	stubRoutes := []struct {
		method string
		path   string
	}{
		// Agents
		{http.MethodPost, "/v1/agents"},
		{http.MethodGet, "/v1/agents"},
		{http.MethodGet, "/v1/agents/agent_test"},
		{http.MethodPost, "/v1/agents/agent_test"},
		{http.MethodPost, "/v1/agents/agent_test/archive"},
		{http.MethodGet, "/v1/agents/agent_test/versions"},

		// Vaults
		{http.MethodPost, "/v1/vaults"},
		{http.MethodGet, "/v1/vaults"},
		{http.MethodGet, "/v1/vaults/vlt_test"},
		{http.MethodPost, "/v1/vaults/vlt_test"},
		{http.MethodDelete, "/v1/vaults/vlt_test"},
		{http.MethodPost, "/v1/vaults/vlt_test/archive"},

		// Credentials
		{http.MethodPost, "/v1/vaults/vlt_test/credentials"},
		{http.MethodGet, "/v1/vaults/vlt_test/credentials"},
		{http.MethodGet, "/v1/vaults/vlt_test/credentials/cred_test"},
		{http.MethodPost, "/v1/vaults/vlt_test/credentials/cred_test"},
		{http.MethodDelete, "/v1/vaults/vlt_test/credentials/cred_test"},
		{http.MethodPost, "/v1/vaults/vlt_test/credentials/cred_test/archive"},

		// Files
		{http.MethodPost, "/v1/files"},
		{http.MethodGet, "/v1/files"},
		{http.MethodGet, "/v1/files/file_test"},
		{http.MethodDelete, "/v1/files/file_test"},
		{http.MethodGet, "/v1/files/file_test/content"},

		// Skills
		{http.MethodPost, "/v1/skills"},
		{http.MethodGet, "/v1/skills"},
		{http.MethodGet, "/v1/skills/skill_test"},
		{http.MethodDelete, "/v1/skills/skill_test"},
		{http.MethodPost, "/v1/skills/skill_test/versions"},
		{http.MethodGet, "/v1/skills/skill_test/versions"},
		{http.MethodGet, "/v1/skills/skill_test/versions/1"},
		{http.MethodGet, "/v1/skills/skill_test/versions/1/content"},
		{http.MethodDelete, "/v1/skills/skill_test/versions/1"},
	}
	for _, route := range stubRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(route.method, route.path, nil)
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d; want 501 body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestUnregisteredPathReturns404(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/nonexistent", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}

func TestWrongMethodReturns405(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	// DELETE /v1/agents is not registered (only POST and GET)
	request := httptest.NewRequest(http.MethodDelete, "/v1/agents", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", recorder.Code)
	}
}

func TestRecoveryMiddlewareCatchesPanic(t *testing.T) {
	router := httpapi.NewRouterWithPanicHandler(newTestHandler(t), testAPIKey)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/__test_panic", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", recorder.Code)
	}

	contentType := recorder.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	var response struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if response.Type != "error" {
		t.Errorf("expected type 'error', got %q", response.Type)
	}
	if response.Error.Type != "api_error" {
		t.Errorf("expected error type 'api_error', got %q", response.Error.Type)
	}
	if response.Error.Message != "internal error" {
		t.Errorf("expected message 'internal error', got %q", response.Error.Message)
	}
	if response.RequestID == "" {
		t.Error("request_id is empty in panic recovery response")
	}
}

// TestSkillRoutesWithSkillHandlerOptionInstallsRealReceivers pins
// that all nine `/v1/skills` routes dispatch to the SkillHandler
// receivers when the WithSkillHandler option is installed. The fake
// store records the receiver name; the test asserts that the route
// reached the receiver (not the generic stubHandler). Behavioral
// coverage of each receiver lives in
// engine/internal/httpapi/skill_handler_test.go; this test only
// proves the option seam wiring.
//
// The two POST routes are exercised through the
// expectsPreReceiverFailure column: their bodies are absent so the
// multipart parser fails BEFORE reaching the receiver, which is the
// correct behavior — the rejection still proves the route is wired
// to the SkillHandler (a generic stubHandler would have returned
// 501 instead).
func TestSkillRoutesWithSkillHandlerOptionInstallsRealReceivers(t *testing.T) {
	store := newRouterTestSkillStore()
	skillHandler := httpapi.NewSkillHandler(store, t.TempDir())
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithSkillHandler(skillHandler))

	routes := []struct {
		method, path             string
		expectedReceiver         string
		expectedCode             int
		expectsPreReceiverReject bool // body is missing/invalid; multipart parse fails before reaching the store receiver
	}{
		{http.MethodPost, "/v1/skills", "create_skill", http.StatusBadRequest, true},
		{http.MethodPost, "/v1/skills/skill_x/versions", "create_version", http.StatusBadRequest, true},
		{http.MethodGet, "/v1/skills", "list_skills", http.StatusOK, false},
		{http.MethodGet, "/v1/skills/skill_x", "get_skill", http.StatusOK, false},
		{http.MethodDelete, "/v1/skills/skill_x", "delete_skill", http.StatusOK, false},
		{http.MethodGet, "/v1/skills/skill_x/versions", "list_versions", http.StatusOK, false},
		{http.MethodGet, "/v1/skills/skill_x/versions/1", "get_version", http.StatusOK, false},
		{http.MethodGet, "/v1/skills/skill_x/versions/1/content?beta=true", "open_version_content", http.StatusOK, false},
		{http.MethodDelete, "/v1/skills/skill_x/versions/1", "delete_version", http.StatusOK, false},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			store.reset()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(route.method, route.path, nil)
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)
			if recorder.Code != route.expectedCode {
				t.Fatalf("status = %d; want %d (body=%q)", recorder.Code, route.expectedCode, recorder.Body.String())
			}
			if route.expectsPreReceiverReject {
				// Receiver should NOT be reached; 400 from the multipart
				// parse is the receiver-specific signature (a generic
				// stubHandler would have returned 501).
				if store.wasCalled(route.expectedReceiver) {
					t.Errorf("receiver %s unexpectedly invoked despite multipart parse failure", route.expectedReceiver)
				}
				if recorder.Code == http.StatusNotImplemented {
					t.Errorf("status 501 means the route fell through to stubHandler instead of SkillHandler")
				}
				return
			}
			if !store.wasCalled(route.expectedReceiver) {
				t.Errorf("expected %s receiver to be invoked; calls=%v", route.expectedReceiver, store.calls)
			}
		})
	}
}

func TestLoggingMiddlewareRunsWithoutError(t *testing.T) {
	router := newTestRouter(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 through full middleware chain, got %d", recorder.Code)
	}
}
