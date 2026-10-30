package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type computerObject struct {
	ID                string          `json:"id"`
	Kind              string          `json:"kind"`
	EnvironmentID     *string         `json:"environment_id"`
	InstanceID        *string         `json:"instance_id"`
	CurrentInstanceID *string         `json:"current_instance_id"`
	WorkingDirectory  string          `json:"working_directory"`
	CWD               *string         `json:"cwd"`
	Networking        json.RawMessage `json:"networking"`
	Repositories      []struct {
		ID        string  `json:"id"`
		MountPath *string `json:"mount_path"`
		Path      string  `json:"path"`
	} `json:"repositories"`
	AssociationState string          `json:"association_state"`
	ExecutionState   string          `json:"execution_state"`
	ConnectionID     *string         `json:"connection_id"`
	BindingID        *string         `json:"binding_id"`
	Error            json.RawMessage `json:"error"`
}

type computerSession struct {
	ID            string           `json:"id"`
	EnvironmentID string           `json:"environment_id"`
	Computers     []computerObject `json:"computers"`
	Resources     []struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"resources"`
}

// requestWithKey sends one Session route request with an Idempotency-Key;
// an empty key sends none.
func (e *sessionIntegrationEnv) requestWithKey(method, path, key, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	setAuthHeader(request)
	e.router.ServeHTTP(recorder, request)
	return recorder
}

func (e *sessionIntegrationEnv) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := e.admin.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func (e *sessionIntegrationEnv) computerRows(t *testing.T) int {
	t.Helper()
	return countSessionIntegrationRows(t, e.admin, `SELECT count(*) FROM computers WHERE workspace_id = $1`, workspace.DefaultID)
}

// seedComputerInstances writes two active user instances, a cloud instance,
// a deregistered user instance and an archived Environment.
func seedComputerInstances(t *testing.T, env *sessionIntegrationEnv) {
	t.Helper()
	env.exec(t, `INSERT INTO instances (workspace_id,id,kind,name,registration_state) VALUES ('default','inst_mac','user','mac','active'),
		('default','inst_mini','user','mini','active'),('default','inst_cloud','cloud',NULL,'active')`)
	env.exec(t, `INSERT INTO instances (workspace_id,id,kind,name,registration_state,deregistered_at) VALUES ('default','inst_gone','user','gone','deregistered',clock_timestamp())`)
	env.exec(t, `INSERT INTO environments (workspace_id,id,name,config_json,archived_at,created_at,updated_at)
		VALUES ('default','env_archived','archived','{}',clock_timestamp(),clock_timestamp(),clock_timestamp())`)
}

func errorBody(t *testing.T, recorder *httptest.ResponseRecorder) (string, string, bool) {
	t.Helper()
	var response struct {
		Error map[string]any `json:"error"`
	}
	decodeSessionIntegrationJSON(t, recorder, &response)
	_, hasCode := response.Error["code"]
	errorType, _ := response.Error["type"].(string)
	message, _ := response.Error["message"].(string)
	return errorType, message, hasCode
}

// DE-6 for the computer routes, CM-14 and CM-19's computer part: each entry
// check answers its status and error.type with no code field and adds no
// computer; a replay answers the same computer, a changed replay conflicts.
func TestSessionComputerAddRules(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	seedComputerInstances(t, env)
	created := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)
	route := "/v1/sessions/" + created.ID + "/computers"
	repo := func(url, mountPath string) string {
		item := `{"url":"` + url + `","authorization_token":"github_token_rules"`
		if mountPath != "" {
			item += `,"mount_path":"` + mountPath + `"`
		}
		return item + `}`
	}
	if status := env.requestWithKey(http.MethodPost, route, "attach-mac", `{"instance_id":"inst_mac","working_directory":"/Users/me/shop"}`).Code; status != http.StatusCreated {
		t.Fatalf("first user computer = %d; want 201", status)
	}
	before := env.computerRows(t)
	for _, row := range []struct {
		name, key, body string
		status          int
		errorType       string
		message         string
	}{
		{"both definitions", "r1", `{"environment_id":"env_http_session","instance_id":"inst_mac","working_directory":"/w"}`, 400, "invalid_request_error", "exactly one"},
		{"no definition", "r2", `{"working_directory":"/w"}`, 400, "invalid_request_error", "exactly one"},
		{"unknown environment", "r3", `{"environment_id":"env_missing"}`, 404, "not_found_error", ""},
		{"archived environment", "r4", `{"environment_id":"env_archived"}`, 400, "invalid_request_error", "archived"},
		{"unknown instance", "r5", `{"instance_id":"inst_missing","working_directory":"/w"}`, 404, "not_found_error", ""},
		{"cloud instance", "r6", `{"instance_id":"inst_cloud","working_directory":"/w"}`, 409, "instance_not_attachable_error", ""},
		{"deregistered instance", "r7", `{"instance_id":"inst_gone","working_directory":"/w"}`, 409, "instance_not_attachable_error", ""},
		{"instance already attached", "r8", `{"instance_id":"inst_mac","working_directory":"/Users/me/other"}`, 409, "computer_already_attached_error", ""},
		{"relative working_directory", "r9", `{"environment_id":"env_http_session","working_directory":"workspace"}`, 400, "invalid_request_error", "working_directory"},
		{"working_directory with U+0000", "r10", `{"environment_id":"env_http_session","working_directory":"/w\u0000x"}`, 400, "invalid_request_error", "working_directory"},
		{"user computer without working_directory", "r11", `{"instance_id":"inst_mini"}`, 400, "invalid_request_error", "working_directory"},
		{"networking on a cloud computer", "r12", `{"environment_id":"env_http_session","networking":{"type":"blocked"}}`, 400, "invalid_request_error", "networking"},
		{"other networking on a user computer", "r13", `{"instance_id":"inst_mini","working_directory":"/w","networking":{"type":"cidr_allow_list"}}`, 400, "invalid_request_error", "networking"},
		{"repositories on a user computer", "r14", `{"instance_id":"inst_mini","working_directory":"/w","repositories":[` + repo("https://github.com/acme/app", "") + `]}`, 400, "invalid_request_error", "repositories"},
		{"two repositories with one path", "r15", `{"environment_id":"env_http_session","repositories":[` + repo("https://github.com/acme/app", "/src") + `,` + repo("https://github.com/acme/lib", "/src") + `]}`, 400, "invalid_request_error", "/src"},
		{"drive_exclude without drive_id", "r16", `{"environment_id":"env_http_session","drive_exclude":[{"drive_id":"","paths":["a"]}]}`, 400, "invalid_request_error", "drive_id"},
		{"drive_exclude path outside the rules", "r17", `{"environment_id":"env_http_session","drive_exclude":[{"drive_id":"drive_a","paths":["../up"]}]}`, 400, "invalid_request_error", `"../up"`},
		{"missing Idempotency-Key", "", `{"environment_id":"env_http_session"}`, 400, "invalid_request_error", "Idempotency-Key"},
		{"body over 1 MiB", "r18", `{"environment_id":"env_http_session","working_directory":"/` + strings.Repeat("a", 1<<20) + `"}`, 413, "request_too_large", ""},
	} {
		recorder := env.requestWithKey(http.MethodPost, route, row.key, row.body)
		errorType, message, hasCode := errorBody(t, recorder)
		if recorder.Code != row.status || errorType != row.errorType || hasCode || !strings.Contains(message, row.message) {
			t.Errorf("%s = %d %s; want %d %s naming %q and no code", row.name, recorder.Code, recorder.Body.String(), row.status, row.errorType, row.message)
		}
	}
	if after := env.computerRows(t); after != before {
		t.Fatalf("computer rows = %d after refused entries; want %d", after, before)
	}

	// An entry that excludes a drive the Session does not bind is accepted.
	const body = `{"environment_id":"env_http_session","repositories":[{"url":"https://github.com/acme/app","authorization_token":"github_token_cm19"}],"drive_exclude":[{"drive_id":"drive_unbound","paths":["datasets/"]}]}`
	first := env.requestWithKey(http.MethodPost, route, "add-cloud", body)
	assertHTTPStatus(t, first, http.StatusCreated)
	if strings.Contains(first.Body.String(), "github_token_cm19") {
		t.Fatalf("computer object carries the repository token: %s", first.Body.String())
	}
	var added computerObject
	decodeSessionIntegrationJSON(t, first, &added)
	if added.AssociationState != "attached" || added.ExecutionState != "waiting" || added.CWD != nil || added.ConnectionID != nil || added.BindingID != nil ||
		string(added.Error) != "null" || added.WorkingDirectory != "/workspace" || len(added.Repositories) != 1 || added.Repositories[0].Path != "/workspace/app" ||
		added.Repositories[0].MountPath != nil {
		t.Fatalf("new computer = %s; want attached, waiting, /workspace, the repository at /workspace/app and null cwd, connection, binding and error", first.Body.String())
	}
	replay := env.requestWithKey(http.MethodPost, route, "add-cloud", body)
	var replayed computerObject
	decodeSessionIntegrationJSON(t, replay, &replayed)
	if replay.Code != http.StatusCreated || replayed.ID != added.ID || env.computerRows(t) != before+1 {
		t.Fatalf("replay = %d %s with %d rows; want 201 %s and one new row", replay.Code, replay.Body.String(), env.computerRows(t), added.ID)
	}
	changed := env.requestWithKey(http.MethodPost, route, "add-cloud", `{"environment_id":"env_http_session"}`)
	if errorType, _, _ := errorBody(t, changed); changed.Code != http.StatusConflict || errorType != "idempotency_conflict_error" {
		t.Fatalf("changed replay = %d %s; want 409 idempotency_conflict_error", changed.Code, changed.Body.String())
	}
	read := env.request(http.MethodGet, route+"/"+added.ID, "")
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), `"id":"`+added.ID+`"`) {
		t.Fatalf("computer read = %d %s", read.Code, read.Body.String())
	}
	assertHTTPStatus(t, env.request(http.MethodGet, route+"/comp_missing", ""), http.StatusNotFound)

	// CM-14: one user instance attaches to three Sessions, and its
	// computer_count counts them.
	for i := 0; i < 2; i++ {
		other := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)
		recorder := env.requestWithKey(http.MethodPost, "/v1/sessions/"+other.ID+"/computers", "attach-mac", `{"instance_id":"inst_mac","working_directory":"/Users/me/shop"}`)
		assertHTTPStatus(t, recorder, http.StatusCreated)
	}
	if count := instanceComputerCount(t, env, "inst_mac"); count != 3 {
		t.Fatalf("inst_mac computer_count = %d; want 3", count)
	}
}

func instanceComputerCount(t *testing.T, env *sessionIntegrationEnv, instanceID string) int {
	t.Helper()
	var count int
	client := dbconnect.NewClientForTesting(env.admin)
	if err := client.WithWorkspaceReadOnlyTx(context.Background(), string(workspace.DefaultID), "test.instance_read", func(tx *dbconnect.Tx) error {
		value, err := instance.ReadInstance(context.Background(), tx, workspace.DefaultID, instanceID)
		if err != nil {
			return err
		}
		now, err := instance.Now(context.Background(), tx)
		if err != nil {
			return err
		}
		object, err := instance.ReadObject(context.Background(), tx, workspace.DefaultID, value, "", now)
		count = object.ComputerCount
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

// CM-15: an addition while the Session runs is refused, and one to an idle
// Session leaves it idle.
func TestSessionComputerAddNeedsAnIdleSession(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)
	route := "/v1/sessions/" + created.ID + "/computers"
	setHTTPSessionRuntimeStatus(t, env.admin, workspace.DefaultID, created.ID, session.StatusRunning)
	running := env.requestWithKey(http.MethodPost, route, "while-running", `{"environment_id":"env_http_session"}`)
	if errorType, _, _ := errorBody(t, running); running.Code != http.StatusConflict || errorType != "invalid_request_error" {
		t.Fatalf("addition while running = %d %s; want 409 invalid_request_error", running.Code, running.Body.String())
	}
	setHTTPSessionRuntimeStatus(t, env.admin, workspace.DefaultID, created.ID, session.StatusIdle)
	assertHTTPStatus(t, env.requestWithKey(http.MethodPost, route, "while-idle", `{"environment_id":"env_http_session"}`), http.StatusCreated)
	if status := loadHTTPSessionRuntimeStatus(t, env.admin, workspace.DefaultID, created.ID); status != "idle" {
		t.Fatalf("Session status after an addition = %s; want idle", status)
	}
}

// Session creation's computer forms, and the Session object's computers and
// environment_id on creation, its replay, retrieve, list and after an
// addition.
func TestSessionCreationComputers(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	seedComputerInstances(t, env)
	for _, row := range []struct{ name, body, message string }{
		{"no cloud computer", `{"agent":"agent_http_session","vault_ids":[],"computers":[{"instance_id":"inst_mac","working_directory":"/w"}]}`, "cloud computer"},
		{"resources with computers", `{"agent":"agent_http_session","vault_ids":[],"computers":[{"environment_id":"env_http_session"}],"resources":[{"type":"github_repository","url":"https://github.com/acme/app","authorization_token":"t"}]}`, "resources"},
		{"computers with environment_id", `{"agent":"agent_http_session","vault_ids":[],"environment_id":"env_http_session","computers":[{"environment_id":"env_http_session"}]}`, "environment_id"},
	} {
		recorder := env.request(http.MethodPost, "/v1/sessions", row.body)
		if errorType, message, _ := errorBody(t, recorder); recorder.Code != http.StatusBadRequest || errorType != "invalid_request_error" || !strings.Contains(message, row.message) {
			t.Errorf("%s = %d %s; want 400 invalid_request_error naming %q", row.name, recorder.Code, recorder.Body.String(), row.message)
		}
	}
	twice := env.request(http.MethodPost, "/v1/sessions", `{"agent":"agent_http_session","vault_ids":[],"computers":[{"environment_id":"env_http_session"},
		{"instance_id":"inst_mac","working_directory":"/a"},{"instance_id":"inst_mac","working_directory":"/b"}]}`)
	if errorType, _, _ := errorBody(t, twice); twice.Code != http.StatusConflict || errorType != "computer_already_attached_error" {
		t.Errorf("one instance in two entries = %d %s; want 409 computer_already_attached_error", twice.Code, twice.Body.String())
	}
	if rows := countSessionIntegrationRows(t, env.admin, `SELECT count(*) FROM sessions WHERE workspace_id = $1`, workspace.DefaultID); rows != 0 {
		t.Fatalf("refused creations left %d Sessions", rows)
	}

	// The one-computer form: its repository resource lands at
	// /workspace/<name>, and the computer keeps the Environment's networking
	// after the Environment changes.
	env.exec(t, `UPDATE environments SET config_json='{"networking":{"type":"blocked"}}' WHERE id='env_http_session'`)
	one := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[],
		"resources":[{"type":"github_repository","url":"https://github.com/acme/app.git","authorization_token":"github_token_one"}]}`)
	var oneSession computerSession
	decodeSessionIntegrationJSON(t, env.request(http.MethodGet, "/v1/sessions/"+one.ID, ""), &oneSession)
	if len(oneSession.Computers) != 1 || oneSession.Computers[0].WorkingDirectory != "/workspace" || len(oneSession.Computers[0].Repositories) != 1 ||
		oneSession.Computers[0].Repositories[0].Path != "/workspace/app" || oneSession.EnvironmentID != "env_http_session" {
		t.Fatalf("one-computer Session = %+v; want one cloud computer at /workspace with the repository at /workspace/app", oneSession)
	}
	env.exec(t, `UPDATE environments SET config_json='{"networking":{"type":"unrestricted"}}' WHERE id='env_http_session'`)
	var later computerObject
	decodeSessionIntegrationJSON(t, env.request(http.MethodGet, "/v1/sessions/"+one.ID+"/computers/"+oneSession.Computers[0].ID, ""), &later)
	if string(later.Networking) != `{"type":"blocked"}` {
		t.Fatalf("computer networking after the Environment changed = %s; want the blocked copy", later.Networking)
	}

	const body = `{"agent":"agent_http_session","vault_ids":[],"computers":[{"environment_id":"env_http_session"},{"instance_id":"inst_mac","working_directory":"/Users/me/shop"}]}`
	first := env.requestWithKey(http.MethodPost, "/v1/sessions", "create-two", body)
	assertHTTPStatus(t, first, http.StatusOK)
	var created computerSession
	decodeSessionIntegrationJSON(t, first, &created)
	if len(created.Computers) != 2 || created.Computers[0].Kind != "cloud" || created.Computers[1].Kind != "user" || created.EnvironmentID != "env_http_session" {
		t.Fatalf("created Session = %s; want the cloud computer, then the user computer, and the cloud computer's environment_id", first.Body.String())
	}
	ids := []string{created.Computers[0].ID, created.Computers[1].ID}
	var replayed computerSession
	decodeSessionIntegrationJSON(t, env.requestWithKey(http.MethodPost, "/v1/sessions", "create-two", body), &replayed)
	if replayed.ID != created.ID || len(replayed.Computers) != 2 || replayed.Computers[0].ID != ids[0] || replayed.Computers[1].ID != ids[1] {
		t.Fatalf("creation replay = %+v; want Session %s with computers %v", replayed, created.ID, ids)
	}
	added := env.requestWithKey(http.MethodPost, "/v1/sessions/"+created.ID+"/computers", "add-third", `{"environment_id":"env_http_session"}`)
	assertHTTPStatus(t, added, http.StatusCreated)
	var third computerObject
	decodeSessionIntegrationJSON(t, added, &third)
	want := append(ids, third.ID)
	var retrieved computerSession
	decodeSessionIntegrationJSON(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID, ""), &retrieved)
	var listed struct {
		Data []computerSession `json:"data"`
	}
	decodeSessionIntegrationJSON(t, env.request(http.MethodGet, "/v1/sessions", ""), &listed)
	listedIndex := slices.IndexFunc(listed.Data, func(item computerSession) bool { return item.ID == created.ID })
	if listedIndex < 0 || !slices.Equal(computerIDs(retrieved.Computers), want) || !slices.Equal(computerIDs(listed.Data[listedIndex].Computers), want) {
		t.Fatalf("retrieved %v and listed computers; want %v in addition order", computerIDs(retrieved.Computers), want)
	}
}

func computerIDs(computers []computerObject) []string {
	ids := make([]string, len(computers))
	for i, object := range computers {
		ids[i] = object.ID
	}
	return ids
}

// A computer's repositories are fixed once it is added: adding a repository
// resource and deleting one are refused and change nothing, while token
// rotation works.
func TestSessionComputerRepositoriesAreFixed(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{"agent":"agent_http_session","vault_ids":[],"computers":[{"environment_id":"env_http_session",
		"repositories":[{"url":"https://github.com/acme/app","authorization_token":"github_token_fixed","mount_path":"/src/app"}]}]}`)
	var before computerSession
	decodeSessionIntegrationJSON(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID, ""), &before)
	if len(before.Computers) != 1 || len(before.Computers[0].Repositories) != 1 {
		t.Fatalf("created Session = %+v; want one computer with one repository", before)
	}
	repositoryID := before.Computers[0].Repositories[0].ID
	resources := "/v1/sessions/" + created.ID + "/resources"
	for name, recorder := range map[string]*httptest.ResponseRecorder{
		"add":    env.request(http.MethodPost, resources, `{"type":"github_repository","url":"https://github.com/acme/lib","authorization_token":"t"}`),
		"delete": env.request(http.MethodDelete, resources+"/"+repositoryID, ""),
	} {
		if errorType, _, _ := errorBody(t, recorder); recorder.Code != http.StatusBadRequest || errorType != "invalid_request_error" {
			t.Errorf("repository %s = %d %s; want 400 invalid_request_error", name, recorder.Code, recorder.Body.String())
		}
	}
	assertHTTPStatus(t, env.request(http.MethodPost, resources+"/"+repositoryID, `{"authorization_token":"github_token_rotated"}`), http.StatusOK)
	var after computerSession
	decodeSessionIntegrationJSON(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID, ""), &after)
	repositories := after.Computers[0].Repositories
	if len(repositories) != 1 || repositories[0].ID != repositoryID || repositories[0].Path != "/src/app" || repositories[0].MountPath == nil || *repositories[0].MountPath != "/src/app" {
		t.Fatalf("repositories after the refused changes = %+v; want only %s at /src/app", repositories, repositoryID)
	}
}
