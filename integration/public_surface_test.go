// Layer 3: the edge stand-in, Auth, the API and Event Stream over HTTP, each
// with its own database role on a real PostgreSQL.
package integration

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// A developer walks the resources of the public API through the edge, Auth,
// the API and Event Stream as each service's own database role: every
// resource is created, read back, listed and removed, and each shape Engine
// does not take is refused. The client holds every request and answer to
// Engine's OpenAPI file; the assertions here are the values the file cannot
// state.
func TestADeveloperWalksThePublicResources(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	base, key := startContentPublicEdge(t, storagetest.OpenWorkloadDB(t, admin, "auth"), blob.NewFakeBlobStore())
	d := newPublicDeveloper(t, base, key)
	get := func(t *testing.T, path string) map[string]any { return d.object(t, http.MethodGet, path, nil) }
	post := func(t *testing.T, path string, body any) map[string]any {
		return d.object(t, http.MethodPost, path, body)
	}
	id := func(t *testing.T, object map[string]any) string {
		t.Helper()
		value, _ := object["id"].(string)
		if value == "" {
			t.Fatalf("%v has no id", object)
		}
		return value
	}
	// first is the one object of a list read with limit 1.
	first := func(t *testing.T, path string) map[string]any {
		t.Helper()
		data, _ := get(t, path)["data"].([]any)
		if len(data) != 1 {
			t.Fatalf("GET %s lists %d objects; want one", path, len(data))
		}
		return data[0].(map[string]any)
	}
	invalid := func(t *testing.T, method, path string, body any, outside bool, message string) {
		t.Helper()
		if errorType, got := d.refusal(t, method, path, body, outside, http.StatusBadRequest); errorType != "invalid_request_error" || !strings.Contains(got, message) {
			t.Fatalf("%s %s refused as %s %q; want invalid_request_error holding %q", method, path, errorType, got, message)
		}
	}
	toolset := []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}
	environmentAndAgent := func(t *testing.T) (environment string, agent map[string]any) {
		t.Helper()
		environment = id(t, post(t, "/v1/environments?beta=true", map[string]any{"name": "surface-" + t.Name(),
			"config": map[string]any{"type": "cloud", "networking": map[string]any{"type": "blocked"}}}))
		return environment, post(t, "/v1/agents?beta=true", map[string]any{"name": "surface-" + t.Name(), "model": "anthropic/claude-opus-4-8",
			"approval_mode": "ask_for_approval", "tools": toolset})
	}
	session := func(t *testing.T, environment string, agent map[string]any, more map[string]any) map[string]any {
		t.Helper()
		body := map[string]any{"agent": map[string]any{"type": "agent", "id": agent["id"], "version": agent["version"]}, "environment_id": environment, "vault_ids": []string{}}
		for name, value := range more {
			body[name] = value
		}
		return post(t, "/v1/sessions?beta=true", body)
	}

	t.Run("vaults and the four kinds of credential", func(t *testing.T) {
		vault := "/v1/vaults/" + id(t, post(t, "/v1/vaults?beta=true", map[string]any{"display_name": "surface vault"}))
		get(t, vault+"?beta=true")
		var credentials []string
		for name, auth := range map[string]map[string]any{
			"mcp oauth":        {"type": "mcp_oauth", "mcp_server_url": "https://mcp.example/sse", "access_token": "mcp-access-token"},
			"static bearer":    {"type": "static_bearer", "mcp_server_url": "https://static-bearer.mcp.example/sse", "token": "static-bearer-token"},
			"provider api key": {"type": "provider_api_key", "provider_id": "anthropic", "access_mode": "model_inference", "token": "provider-api-key"},
			"provider oauth": {"type": "provider_oauth", "provider_id": "openai", "access_mode": "oauth", "access_token": "provider-access-token",
				"refresh_token": "provider-refresh-token", "expires_at": "2099-01-01T00:00:00Z", "account_id": "provider-account"},
		} {
			credentials = append(credentials, id(t, post(t, vault+"/credentials?beta=true", map[string]any{"display_name": name, "auth": auth})))
		}
		get(t, vault+"/credentials/"+credentials[0]+"?beta=true")
		if listed, _ := get(t, vault+"/credentials?beta=true")["data"].([]any); len(listed) != len(credentials) {
			t.Fatalf("the vault lists %d credentials; want %d", len(listed), len(credentials))
		}
	})

	t.Run("agents and their versions", func(t *testing.T) {
		_, agent := environmentAndAgent(t)
		path := "/v1/agents/" + id(t, agent)
		get(t, path+"?beta=true")
		updated := post(t, path+"?beta=true", map[string]any{"version": agent["version"], "approval_mode": "approve_for_me"})
		if updated["version"] != agent["version"].(float64)+1 || updated["approval_mode"] != "approve_for_me" {
			t.Fatalf("updated agent = %v; want the next version with the new approval mode", updated)
		}
		first(t, "/v1/agents?beta=true&limit=1")
		first(t, path+"/versions?beta=true&limit=1")
	})

	t.Run("a repository's Git identity is kept as given and cannot be changed", func(t *testing.T) {
		environment, agent := environmentAndAgent(t)
		identities := []map[string]any{{"name": "山田 O'Brien", "email": "bot+first@example.com"}, {"name": "Second Bot", "email": "second@example.com"}, nil}
		var resources []any
		for index, identity := range identities {
			resource := map[string]any{"type": "github_repository", "url": fmt.Sprintf("https://github.com/example/repository-%d", index), "authorization_token": "surface-repository-token"}
			if identity != nil {
				resource["git_identity"] = identity
			}
			resources = append(resources, resource)
		}
		created := session(t, environment, agent, map[string]any{"resources": resources})
		path := "/v1/sessions/" + id(t, created)
		byURL := func(t *testing.T, list any, url string) map[string]any {
			t.Helper()
			for _, resource := range list.([]any) {
				if resource := resource.(map[string]any); resource["url"] == url {
					return resource
				}
			}
			t.Fatalf("no repository %s in %v", url, list)
			return nil
		}
		read, listed := get(t, path+"?beta=true"), get(t, path+"/resources?beta=true")
		for index, identity := range identities {
			url := fmt.Sprintf("https://github.com/example/repository-%d", index)
			resource := byURL(t, created["resources"], url)
			one := path + "/resources/" + id(t, resource) + "?beta=true"
			retrieved := get(t, one)
			rotated := post(t, one, map[string]any{"authorization_token": "surface-rotated-token"})
			invalid(t, http.MethodPost, one, map[string]any{"authorization_token": "surface-rotated-token",
				"git_identity": map[string]any{"name": "Changed Bot", "email": "changed@example.com"}}, true, "")
			for name, seen := range map[string]map[string]any{"created": resource, "read with the Session": byURL(t, read["resources"], url),
				"listed": byURL(t, listed["data"], url), "read alone": retrieved, "after the token's rotation": rotated, "after the refused change": get(t, one)} {
				got, present := seen["git_identity"]
				if _, token := seen["authorization_token"]; token || present != (identity != nil) || (present && !reflect.DeepEqual(got, any(identity))) {
					t.Fatalf("repository %d %s = %v; want git_identity %v, absent when none was given, and no token", index, name, seen, identity)
				}
			}
		}
		// A null and an empty identity are outside the file; a name with a
		// leading space is within it and refused by Engine's own rule.
		for _, malformed := range []struct {
			identity any
			outside  bool
		}{{nil, true}, {map[string]any{}, true}, {map[string]any{"name": " Bot", "email": "bot@example.com"}, false}} {
			invalid(t, http.MethodPost, "/v1/sessions?beta=true", map[string]any{"agent": agent["id"], "environment_id": environment, "vault_ids": []string{},
				"resources": []any{map[string]any{"type": "github_repository", "url": "https://github.com/example/invalid-identity",
					"authorization_token": "surface-repository-token", "git_identity": malformed.identity}}}, malformed.outside, "")
		}
		d.request(t, http.MethodDelete, path+"?beta=true", nil)
	})

	t.Run("a Session with its files, resources, events and threads", func(t *testing.T) {
		environment, agent := environmentAndAgent(t)
		path := "/v1/sessions/" + id(t, session(t, environment, agent, nil))
		get(t, path+"?beta=true")
		if updated := post(t, path+"?beta=true", map[string]any{"title": "surface session"}); updated["title"] != "surface session" {
			t.Fatalf("updated Session = %v; want the title", updated)
		}
		first(t, "/v1/sessions?beta=true&limit=1")

		file := id(t, d.form(t, "/v1/files?beta=true", formPart{"file", "surface.csv", "text/csv", "city,revenue\nSF,42\n"}))
		get(t, "/v1/files/"+file+"?beta=true")
		second := id(t, d.form(t, "/v1/files?beta=true", formPart{"file", "surface-page.csv", "text/csv", "city,revenue\nNYC,7\n"}))
		listed := map[string]int{}
		for page := ""; ; {
			one := get(t, "/v1/files?beta=true&limit=1"+page)
			for _, entry := range one["data"].([]any) {
				listed[id(t, entry.(map[string]any))]++
			}
			next, more := one["next_page"].(string)
			if !more {
				break
			}
			page = "&page=" + url.QueryEscape(next)
		}
		if listed[file] != 1 || listed[second] != 1 {
			t.Fatalf("paging the files one by one lists %v; want each uploaded file once", listed)
		}
		resource := path + "/resources/" + id(t, post(t, path+"/resources?beta=true", map[string]any{"type": "file", "file_id": file, "mount_path": "/uploads/surface.csv"})) + "?beta=true"
		get(t, resource)
		first(t, path+"/resources?beta=true&limit=1")
		d.request(t, http.MethodDelete, resource, nil)
		post(t, path+"/archive?beta=true", nil)
		if deleted := d.object(t, http.MethodDelete, path+"?beta=true", nil); deleted["type"] != "session_deleted" {
			t.Fatalf("deleted Session = %v", deleted)
		}

		// No Runtime takes input here, so the events belong to a second
		// Session that only receives a message.
		spoken := id(t, session(t, environment, agent, nil))
		path = "/v1/sessions/" + spoken
		d.send(t, spoken, "hello")
		first(t, path+"/events?beta=true&limit=1")
		thread := id(t, first(t, path+"/threads?beta=true&limit=1"))
		get(t, path+"/threads/"+thread+"?beta=true")
		first(t, path+"/threads/"+thread+"/events?beta=true&limit=1")
		d.open(t, "session", spoken, nil, "")
		d.open(t, "thread", spoken, nil, thread)
		publicWait(t, "both streams end once idle", func() bool { return d.snapshot(t, "session").Ended && d.snapshot(t, "thread").Ended })
	})

	t.Run("a Skill and its versions", func(t *testing.T) {
		manifest := func(body string) formPart {
			return formPart{"files[]", "surface/SKILL.md", "text/markdown",
				"---\nname: surface-skill\ndescription: Skill of the public surface test.\n---\n\n# Surface Skill\n\n" + body + "\n"}
		}
		skill := d.form(t, "/v1/skills?beta=true", formPart{name: "display_name", content: "surface skill"}, manifest("First version."))
		path := "/v1/skills/" + id(t, skill)
		get(t, path+"?beta=true")
		first(t, "/v1/skills?beta=true&limit=1")
		version := id(t, d.form(t, path+"/versions?beta=true", manifest("Second version.")))
		get(t, path+"/versions/"+version+"?beta=true")
		first(t, path+"/versions?beta=true&limit=1")
		if latest := get(t, path+"/versions/latest?beta=true"); latest["id"] != version {
			t.Fatalf("latest version = %v; want %s", latest["id"], version)
		}
		d.request(t, http.MethodDelete, path+"/versions/"+version+"?beta=true", nil)
		original, _ := skill["latest_version_id"].(string)
		if after := get(t, path+"?beta=true"); original == "" || after["latest_version_id"] != original {
			t.Fatalf("after its second version's deletion the Skill's latest is %v; want its first, %q", after["latest_version_id"], original)
		}
		invalid(t, http.MethodDelete, path+"/versions/"+original+"?beta=true", nil, false, "last version")
		if deleted := d.object(t, http.MethodDelete, path+"?beta=true", nil); deleted["type"] != "skill_deleted" {
			t.Fatalf("deleted Skill = %v", deleted)
		}
		d.refusal(t, http.MethodGet, path+"/versions/"+original+"?beta=true", nil, false, http.StatusNotFound)
	})

	t.Run("a memory store, its memories and their versions", func(t *testing.T) {
		store := "/v1/memory_stores/" + id(t, post(t, "/v1/memory_stores?beta=true", map[string]any{"name": "surface memory", "description": "Surface memory store",
			"metadata": map[string]any{"purpose": "surface"}}))
		get(t, store+"?beta=true")
		if updated := post(t, store+"?beta=true", map[string]any{"description": "Updated surface store"}); updated["description"] != "Updated surface store" {
			t.Fatalf("updated store = %v", updated)
		}
		first(t, "/v1/memory_stores?beta=true&limit=1")
		memory := post(t, store+"/memories?beta=true&view=full", map[string]any{"path": "/notes/surface.md", "content": "hello memory"})
		one := store + "/memories/" + id(t, memory) + "?beta=true"
		get(t, one)
		updated := post(t, one+"&view=full", map[string]any{"content": "updated memory"})
		first(t, store+"/memories?beta=true&limit=1")
		versions, _ := get(t, store+"/memory_versions?beta=true&limit=100&memory_id="+id(t, memory))["data"].([]any)
		var head, earlier string
		for _, version := range versions {
			if versionID := id(t, version.(map[string]any)); versionID == updated["memory_version_id"] {
				head = versionID
			} else {
				earlier = versionID
			}
		}
		if len(versions) != 2 || head == "" || earlier == "" {
			t.Fatalf("the memory lists versions %v; want its head %v and one earlier", versions, updated["memory_version_id"])
		}
		get(t, store+"/memory_versions/"+earlier+"?beta=true")
		redacted := post(t, store+"/memory_versions/"+earlier+"/redact?beta=true", nil)
		for _, field := range []string{"content", "content_sha256", "content_size_bytes", "path"} {
			if value, present := redacted[field]; !present || value != nil {
				t.Fatalf("redacted version = %v; want %s null", redacted, field)
			}
		}
		invalid(t, http.MethodPost, store+"/memory_versions/"+head+"/redact?beta=true", nil, false, "cannot redact current live memory head")

		invalid(t, http.MethodPost, store+"/memories?beta=true&view=full", map[string]any{"path": "/null-create.md", "content": nil}, true, "")
		if empty := post(t, store+"/memories?beta=true&view=full", map[string]any{"path": "/empty-create.md", "content": ""}); empty["content"] != "" || empty["content_size_bytes"] != float64(0) {
			t.Fatalf("memory created with empty content = %v", empty)
		}
		invalid(t, http.MethodPost, one, map[string]any{"content": nil}, true, "")
		invalid(t, http.MethodPost, one, map[string]any{"path": nil}, true, "")
		if unchanged := post(t, one+"&view=full", map[string]any{}); unchanged["content"] != "updated memory" || unchanged["path"] != "/notes/surface.md" {
			t.Fatalf("memory after an update that names no field = %v; want its content and path unchanged", unchanged)
		}
	})

	t.Run("an Environment from creation to deletion", func(t *testing.T) {
		networking := func(kind string) map[string]any {
			return map[string]any{"config": map[string]any{"type": "cloud", "networking": map[string]any{"type": kind}}}
		}
		create := networking("unrestricted")
		create["name"] = "surface environment"
		path := "/v1/environments/" + id(t, post(t, "/v1/environments?beta=true", create))
		get(t, path+"?beta=true")
		post(t, path+"?beta=true", networking("blocked"))
		first(t, "/v1/environments?beta=true&limit=1")
		post(t, path+"/archive?beta=true", nil)
		d.request(t, http.MethodDelete, path+"?beta=true", nil)
	})

	t.Run("the model catalog", func(t *testing.T) {
		model := first(t, "/v1/models?beta=true&limit=1")
		if read := get(t, "/v1/models/"+url.PathEscape(id(t, model))+"?beta=true"); !reflect.DeepEqual(read, model) {
			t.Fatalf("model read alone = %v; listed as %v", read, model)
		}
	})

	t.Run("shapes Engine does not take", func(t *testing.T) {
		environment, agent := environmentAndAgent(t)
		created := session(t, environment, agent, nil)
		events := "/v1/sessions/" + id(t, created) + "/events?beta=true"
		for _, event := range []map[string]any{
			{"type": "user.custom_tool_result", "custom_tool_use_id": "ctu_123"},
			{"type": "user.tool_result", "tool_use_id": "toolu_123"},
			{"type": "user.define_outcome", "description": "produce the requested output", "rubric": map[string]any{"type": "text", "content": "complete"}},
			{"type": "system.message", "content": []any{map[string]any{"type": "text", "text": "internal context"}}},
		} {
			invalid(t, http.MethodPost, events, map[string]any{"events": []any{event}}, true, "")
		}
		vault := id(t, post(t, "/v1/vaults?beta=true", map[string]any{"display_name": "surface refusals"}))
		credentials := "/v1/vaults/" + vault + "/credentials?beta=true"
		invalid(t, http.MethodPost, credentials, map[string]any{"display_name": "environment variable", "auth": map[string]any{"type": "environment_variable",
			"networking": map[string]any{"type": "unrestricted"}, "secret_name": "MODEL_PROVIDER_API_KEY", "secret_value": "provider-secret-value"}}, true, "")
		invalid(t, http.MethodPost, "/v1/sessions/"+id(t, created)+"?beta=true", map[string]any{"vault_ids": []string{vault}}, true, "")
		// The one cause of this refusal: a live credential of another
		// provider than the agent's model, in a vault the Session binds.
		other := id(t, post(t, credentials, map[string]any{"display_name": "another provider's",
			"auth": map[string]any{"type": "provider_api_key", "provider_id": "openai", "access_mode": "model_inference", "token": "provider-api-key"}}))
		invalid(t, http.MethodPost, "/v1/sessions?beta=true", map[string]any{"agent": map[string]any{"type": "agent", "id": agent["id"], "version": agent["version"]},
			"environment_id": environment, "vault_ids": []string{vault}, "providers": map[string]any{"openai": map[string]any{"credential_id": other}}}, false, "")

		valid := func(more map[string]any) map[string]any {
			body := map[string]any{"name": "surface admission", "model": "anthropic/claude-opus-4-8", "tools": toolset}
			for name, value := range more {
				body[name] = value
			}
			return body
		}
		post(t, "/v1/agents?beta=true", valid(nil))
		invalid(t, http.MethodPost, "/v1/agents?beta=true", valid(map[string]any{"tools": append(toolset[:1:1], map[string]any{"type": "agent_toolset_20260401"})}),
			true, "tools[1].type is unsupported")
		invalid(t, http.MethodPost, "/v1/agents?beta=true", valid(map[string]any{"tools": append(toolset[:1:1], map[string]any{"type": "custom", "name": "lookup",
			"description": "unsupported custom tool", "input_schema": map[string]any{"type": "object"}})}), true, "tools[1].type custom tools are not supported")
		invalid(t, http.MethodPost, "/v1/agents?beta=true", valid(map[string]any{"multiagent": map[string]any{"type": "coordinator", "agents": []any{map[string]any{"type": "self"}}}}),
			true, "multiagent must be null when provided")
	})
}
