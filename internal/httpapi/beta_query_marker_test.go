package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestBetaQueryMarkerRejectsInvalidRequestsBeforeDomainServices(t *testing.T) {
	router := newBetaQueryMarkerTestRouter(
		httpapi.WithAgentHandler(httpapi.NewAgentHandler((*agent.Service)(nil))),
		httpapi.WithEnvironmentHandler(httpapi.NewEnvironmentHandler(environmentListQueryTestService{})),
		httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(panicMemoryService{})),
		httpapi.WithSkillHandler(httpapi.NewSkillHandler(panicSkillService{}, t.TempDir())),
	)

	routes := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"agents create", http.MethodPost, "/v1/agents", `{"name":"marker","model":"anthropic/claude-opus-4-8"}`},
		{"agents list", http.MethodGet, "/v1/agents", ""},
		{"agents retrieve", http.MethodGet, "/v1/agents/agent_marker", ""},
		{"agents update", http.MethodPost, "/v1/agents/agent_marker", `{"version":1}`},
		{"agents archive", http.MethodPost, "/v1/agents/agent_marker/archive", ""},
		{"agent versions list", http.MethodGet, "/v1/agents/agent_marker/versions", ""},
		{"environments create", http.MethodPost, "/v1/environments", `{"name":"marker"}`},
		{"environments list", http.MethodGet, "/v1/environments", ""},
		{"environments retrieve", http.MethodGet, "/v1/environments/env_marker", ""},
		{"environments update", http.MethodPost, "/v1/environments/env_marker", `{}`},
		{"environments delete", http.MethodDelete, "/v1/environments/env_marker", ""},
		{"environments archive", http.MethodPost, "/v1/environments/env_marker/archive", ""},
		{"memory stores create", http.MethodPost, "/v1/memory_stores", `{"name":"marker"}`},
		{"memory stores list", http.MethodGet, "/v1/memory_stores", ""},
		{"memory stores retrieve", http.MethodGet, "/v1/memory_stores/memstore_marker", ""},
		{"memory stores update", http.MethodPost, "/v1/memory_stores/memstore_marker", `{}`},
		{"memory stores delete", http.MethodDelete, "/v1/memory_stores/memstore_marker", ""},
		{"memory stores archive", http.MethodPost, "/v1/memory_stores/memstore_marker/archive", ""},
		{"memories create", http.MethodPost, "/v1/memory_stores/memstore_marker/memories", `{"path":"/marker","content":"x"}`},
		{"memories list", http.MethodGet, "/v1/memory_stores/memstore_marker/memories", ""},
		{"memories retrieve", http.MethodGet, "/v1/memory_stores/memstore_marker/memories/mem_marker", ""},
		{"memories update", http.MethodPost, "/v1/memory_stores/memstore_marker/memories/mem_marker", `{"content":"x"}`},
		{"memories delete", http.MethodDelete, "/v1/memory_stores/memstore_marker/memories/mem_marker", ""},
		{"memory versions list", http.MethodGet, "/v1/memory_stores/memstore_marker/memory_versions", ""},
		{"memory versions retrieve", http.MethodGet, "/v1/memory_stores/memstore_marker/memory_versions/memver_marker", ""},
		{"memory versions redact", http.MethodPost, "/v1/memory_stores/memstore_marker/memory_versions/memver_marker/redact", ""},
		{"skills list", http.MethodGet, "/v1/skills", ""},
		{"skills create", http.MethodPost, "/v1/skills", ""},
		{"skills retrieve", http.MethodGet, "/v1/skills/skl_marker", ""},
		{"skills delete", http.MethodDelete, "/v1/skills/skl_marker", ""},
		{"skill versions create", http.MethodPost, "/v1/skills/skl_marker/versions", ""},
		{"skill versions list", http.MethodGet, "/v1/skills/skl_marker/versions", ""},
		{"skill versions retrieve", http.MethodGet, "/v1/skills/skl_marker/versions/1", ""},
		{"skill version content", http.MethodGet, "/v1/skills/skl_marker/versions/1/content", ""},
		{"skill versions delete", http.MethodDelete, "/v1/skills/skl_marker/versions/1", ""},
	}
	queries := []struct {
		name  string
		query string
	}{
		{"missing", ""},
		{"false", "?beta=false"},
		{"duplicate", "?beta=true&beta=true"},
		{"unknown", "?beta=true&unknown=1"},
	}

	for _, route := range routes {
		for _, query := range queries {
			t.Run(route.name+"/"+query.name, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(route.method, route.path+query.query, strings.NewReader(route.body))
				request.Header.Set("x-api-key", testAPIKey)
				router.ServeHTTP(recorder, request)

				if recorder.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, body = %s; want 400", recorder.Code, recorder.Body.String())
				}
				assertErrorType(t, recorder, "invalid_request_error")
			})
		}
	}
}

func TestMemoryBetaQueryMarkerCombinesWithViewAndPassesOnlyViewToService(t *testing.T) {
	service := &createMemoryCaptureService{}
	router := newBetaQueryMarkerTestRouter(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/memory_stores/memstore_marker/memories?beta=true&view=full", strings.NewReader(`{"path":"/marker","content":""}`))
	request.Header.Set("x-api-key", testAPIKey)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s; want 200", recorder.Code, recorder.Body.String())
	}
	if !service.called || service.request == nil {
		t.Fatal("valid beta marker did not reach Memory service")
	}
	if service.request.View != memory.ViewFull {
		t.Fatalf("service view = %q; want %q", service.request.View, memory.ViewFull)
	}
}

func TestFilesAndVaultsRequireBetaMarkerBeforeDomainServices(t *testing.T) {
	router := newBetaQueryMarkerTestRouter(
		httpapi.WithFileHandler(httpapi.NewFileHandler(nil, t.TempDir(), httpapi.FileHandlerLimits{})),
		httpapi.WithVaultHandler(httpapi.NewVaultHandler(nil)),
	)
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/files"},
		{http.MethodGet, "/v1/files"},
		{http.MethodGet, "/v1/files/file_marker"},
		{http.MethodDelete, "/v1/files/file_marker"},
		{http.MethodGet, "/v1/files/file_marker/content"},
		{http.MethodPost, "/v1/vaults"},
		{http.MethodGet, "/v1/vaults"},
		{http.MethodGet, "/v1/vaults/vlt_marker"},
		{http.MethodPost, "/v1/vaults/vlt_marker"},
		{http.MethodPost, "/v1/vaults/vlt_marker/archive"},
		{http.MethodDelete, "/v1/vaults/vlt_marker"},
		{http.MethodPost, "/v1/vaults/vlt_marker/credentials"},
		{http.MethodGet, "/v1/vaults/vlt_marker/credentials"},
		{http.MethodGet, "/v1/vaults/vlt_marker/credentials/cred_marker"},
		{http.MethodPost, "/v1/vaults/vlt_marker/credentials/cred_marker"},
		{http.MethodPost, "/v1/vaults/vlt_marker/credentials/cred_marker/archive"},
		{http.MethodPost, "/v1/vaults/vlt_marker/credentials/cred_marker/mcp_oauth_validate"},
		{http.MethodDelete, "/v1/vaults/vlt_marker/credentials/cred_marker"},
	}
	for _, route := range routes {
		for _, query := range []string{"", "?beta=false", "?beta=true&beta=true", "?beta=true&unknown=1"} {
			t.Run(route.method+" "+route.path+query, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(route.method, route.path+query, strings.NewReader(`{}`))
				request.Header.Set("x-api-key", testAPIKey)
				router.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, body = %s; want 400", recorder.Code, recorder.Body.String())
				}
				assertErrorType(t, recorder, "invalid_request_error")
			})
		}
	}
}

func TestSessionSubresourcesRequireBetaMarkerBeforeDomainServices(t *testing.T) {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_beta_marker"}, nil
	})
	router := httpapi.NewRouter(httpapi.NewSessionHandler(nil), "", httpapi.WithAuthenticator(authenticator))
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/sessions/sesn_marker/threads"},
		{http.MethodGet, "/v1/sessions/sesn_marker/threads/thr_marker"},
		{http.MethodPost, "/v1/sessions/sesn_marker/threads/thr_marker/archive"},
		{http.MethodPost, "/v1/sessions/sesn_marker/resources"},
		{http.MethodGet, "/v1/sessions/sesn_marker/resources"},
		{http.MethodGet, "/v1/sessions/sesn_marker/resources/sesrsc_marker"},
		{http.MethodPost, "/v1/sessions/sesn_marker/resources/sesrsc_marker"},
		{http.MethodDelete, "/v1/sessions/sesn_marker/resources/sesrsc_marker"},
	}
	for _, route := range routes {
		for _, query := range []string{"", "?beta=false", "?beta=true&beta=true", "?beta=true&unknown=1"} {
			t.Run(route.method+" "+route.path+query, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(route.method, route.path+query, strings.NewReader(`{}`))
				request.Header.Set("x-api-key", testAPIKey)
				router.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, body = %s; want 400", recorder.Code, recorder.Body.String())
				}
				assertErrorType(t, recorder, "invalid_request_error")
			})
		}
	}
}

func newBetaQueryMarkerTestRouter(options ...httpapi.RouterOption) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_beta_marker"}, nil
	})
	options = append([]httpapi.RouterOption{httpapi.WithAuthenticator(authenticator)}, options...)
	return httpapi.NewRouter(nil, "", options...)
}

type panicSkillService struct{}

func (panicSkillService) CreateSkill(context.Context, workspace.ID, skill.CreateSkillInput) (*skill.Skill, error) {
	panic("skill service should not be called")
}

func (panicSkillService) CreateVersion(context.Context, workspace.ID, string, skill.CreateVersionInput) (*skill.SkillVersion, error) {
	panic("skill service should not be called")
}

func (panicSkillService) GetSkill(context.Context, workspace.ID, string) (*skill.Skill, error) {
	panic("skill service should not be called")
}

func (panicSkillService) ListSkills(context.Context, workspace.ID, skill.ListSkillsOptions) (skill.SkillListResult, error) {
	panic("skill service should not be called")
}

func (panicSkillService) DeleteSkill(context.Context, workspace.ID, string) error {
	panic("skill service should not be called")
}

func (panicSkillService) GetVersion(context.Context, workspace.ID, string, string) (*skill.SkillVersion, error) {
	panic("skill service should not be called")
}

func (panicSkillService) OpenVersionContent(context.Context, workspace.ID, string, string) (io.ReadCloser, error) {
	panic("skill service should not be called")
}

func (panicSkillService) ListVersions(context.Context, workspace.ID, string, skill.ListVersionsOptions) (skill.SkillVersionListResult, error) {
	panic("skill service should not be called")
}

func (panicSkillService) DeleteVersion(context.Context, workspace.ID, string, string) error {
	panic("skill service should not be called")
}
