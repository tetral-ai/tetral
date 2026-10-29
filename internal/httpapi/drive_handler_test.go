package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// driveAPI is the drive routes on the real router, drive service, upload
// budget and change listener over a private database, served over HTTP.
type driveAPI struct {
	t       *testing.T
	env     *authTestEnv
	drives  *drive.Service
	handler http.Handler
	url     string
}

func newDriveAPI(t *testing.T, budgetBytesPerMinute int64) *driveAPI {
	t.Helper()
	table, err := os.ReadFile("../../services/api/observability.json")
	if err != nil {
		t.Fatal(err)
	}
	workload.RegisterDeclarations("api", table)
	env := newAuthTestEnv(t)
	client := dbconnect.NewClientForTesting(env.runtime)
	drives, err := drive.NewService(client, blob.NewFakeBlobStore(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	budget, err := drive.NewUploadBudget(client, budgetBytesPerMinute, workload.NewRegistry("api"))
	if err != nil {
		t.Fatal(err)
	}
	changes := httpapi.NewDriveChanges(client, drives, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = changes.Run(ctx)
	}()
	handler := env.router(httpapi.WithDriveHandler(httpapi.NewDriveHandler(drives, budget, changes, []byte("drive-page-key"))))
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		cancel()
		<-done
	})
	return &driveAPI{t: t, env: env, drives: drives, handler: handler, url: server.URL}
}

type driveResponse struct {
	status int
	header http.Header
	body   []byte
}

func (a *driveAPI) request(method, path string, body io.Reader, contentType string) driveResponse {
	a.t.Helper()
	request, err := http.NewRequest(method, a.url+path, body)
	if err != nil {
		a.t.Fatal(err)
	}
	request.Header.Set("x-api-key", a.env.envKey)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		a.t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		a.t.Fatal(err)
	}
	return driveResponse{response.StatusCode, response.Header, data}
}

func (a *driveAPI) json(method, path, body string) driveResponse {
	a.t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	return a.request(method, path, reader, "application/json")
}

// upload sends a multipart upload of content to path with the given extra
// fields, in the order given.
func (a *driveAPI) upload(driveID, path, content string, fields ...string) driveResponse {
	a.t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("path", path)
	part, _ := writer.CreateFormFile("file", "upload")
	_, _ = part.Write([]byte(content))
	for i := 0; i+1 < len(fields); i += 2 {
		_ = writer.WriteField(fields[i], fields[i+1])
	}
	_ = writer.Close()
	return a.request(http.MethodPost, "/v1/drives/"+driveID+"/files", &body, writer.FormDataContentType())
}

func (a *driveAPI) newDrive(body string) string {
	a.t.Helper()
	response := a.json(http.MethodPost, "/v1/drives", body)
	var created struct{ ID string }
	decodeDrive(a.t, response, http.StatusCreated, &created)
	return created.ID
}

func decodeDrive(t *testing.T, response driveResponse, status int, into any) {
	t.Helper()
	if response.status != status {
		t.Fatalf("status = %d, body %s; want %d", response.status, response.body, status)
	}
	if into != nil {
		if err := json.Unmarshal(response.body, into); err != nil {
			t.Fatalf("decode %s: %v", response.body, err)
		}
	}
}

// driveVersionAnswer is the body of an upload or a file delete.
type driveVersionAnswer struct {
	VersionID     string `json:"version_id"`
	ContentSHA256 string `json:"content_sha256"`
}

// expectError checks the status, error.type and message of an error answer
// and that it carries no code field.
func expectError(t *testing.T, response driveResponse, status int, errorType, message string) {
	t.Helper()
	var envelope struct {
		Type  string                     `json:"type"`
		Error map[string]json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(response.body, &envelope); err != nil || response.status != status || envelope.Type != "error" {
		t.Fatalf("answer = %d %s; want %d %s", response.status, response.body, status, errorType)
	}
	var gotType, gotMessage string
	_ = json.Unmarshal(envelope.Error["type"], &gotType)
	_ = json.Unmarshal(envelope.Error["message"], &gotMessage)
	if _, hasCode := envelope.Error["code"]; hasCode || gotType != errorType || (message != "" && gotMessage != message) {
		t.Fatalf("error = %s; want type %q, message %q and no code", response.body, errorType, message)
	}
}

func digest(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func TestDriveRoutes(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)

	t.Run("drives", func(t *testing.T) {
		var created map[string]any
		decodeDrive(t, api.json(http.MethodPost, "/v1/drives", `{"skills_path":"team/skills","metadata":{"team":"ai"}}`), http.StatusCreated, &created)
		want := map[string]any{"skills_path": "team/skills/", "memory": map[string]any{"path": "memory/", "instructions": nil},
			"limits": map[string]any{"max_bytes": nil, "max_files": nil}, "usage": map[string]any{"bytes": float64(0), "files": float64(0)},
			"stored_bytes": nil, "metadata": map[string]any{"team": "ai"}}
		for key, value := range want {
			if fmt.Sprint(created[key]) != fmt.Sprint(value) {
				t.Fatalf("created %s = %v, want %v", key, created[key], value)
			}
		}
		id := created["id"].(string)
		var updated map[string]any
		decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id, `{"memory":{"instructions":"keep notes"},"limits":{"max_files":5},"metadata":{"team":null,"region":"iad"}}`), http.StatusOK, &updated)
		if fmt.Sprint(updated["memory"]) != "map[instructions:keep notes path:memory/]" || fmt.Sprint(updated["limits"]) != "map[max_bytes:<nil> max_files:5]" || fmt.Sprint(updated["metadata"]) != "map[region:iad]" {
			t.Fatalf("updated = %v", updated)
		}
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id, ""), http.StatusOK, nil)
		decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id, ""), http.StatusNoContent, nil)
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id, ""), http.StatusNotFound, "not_found_error", "drive not found")
		expectError(t, api.json(http.MethodGet, "/v1/drives/drive_unknown", ""), http.StatusNotFound, "not_found_error", "drive not found")

		api.env.seedWorkspace(t, "wksp_other", "other")
		other, err := api.drives.Create(context.Background(), "wksp_other", drive.Settings{})
		if err != nil {
			t.Fatal(err)
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+other.ID, ""), http.StatusNotFound, "not_found_error", "drive not found")
		expectError(t, api.json(http.MethodPost, "/v1/drives/"+other.ID, `{}`), http.StatusNotFound, "not_found_error", "drive not found")
	})

	t.Run("invalid fields", func(t *testing.T) {
		for _, tc := range []struct{ body, message string }{
			{`{"name":"x"}`, "unknown field name"},
			{`{"skills_path":"/abs"}`, "skills_path: path must be relative, without a leading /"},
			{`{"memory":{"path":"a//b"}}`, "memory.path: path must not contain empty, . or .. segments"},
			{`{"memory":{"instructions":"` + strings.Repeat("x", 4097) + `"}}`, "memory.instructions must be a string of at most 4096 characters, or null"},
			{`{"limits":{"max_bytes":0}}`, "limits.max_bytes must be an integer from 1 to 500000000000, or null"},
			{`{"limits":{"max_files":100001}}`, "limits.max_files must be an integer from 1 to 100000, or null"},
			{`{"metadata":{"k":null}}`, "metadata values must be strings of at most 512 characters"},
			{`{"metadata":{"` + strings.Repeat("k", 65) + `":"v"}}`, "metadata keys must be at most 64 characters"},
			{`[]`, "request body must be a JSON object"},
		} {
			expectError(t, api.json(http.MethodPost, "/v1/drives", tc.body), http.StatusBadRequest, "invalid_request_error", tc.message)
		}
		many := map[string]string{}
		for i := range 17 {
			many[fmt.Sprint(i)] = "v"
		}
		body, _ := json.Marshal(map[string]any{"metadata": many})
		expectError(t, api.json(http.MethodPost, "/v1/drives", string(body)), http.StatusBadRequest, "invalid_request_error", "metadata must have at most 16 keys")
	})

	t.Run("files and versions", func(t *testing.T) {
		id := api.newDrive(`{}`)
		var first driveVersionAnswer
		decodeDrive(t, api.upload(id, "a.txt", "one", "must_not_exist", "true"), http.StatusCreated, &first)
		if first.ContentSHA256 != digest("one") {
			t.Fatalf("upload = %+v", first)
		}
		// An identical upload with a stale precondition adds no version.
		var again driveVersionAnswer
		decodeDrive(t, api.upload(id, "a.txt", "one", "expected_content_sha256", digest("stale")), http.StatusOK, &again)
		if again.VersionID != first.VersionID {
			t.Fatalf("identical upload answered %s, want the current %s", again.VersionID, first.VersionID)
		}
		expectError(t, api.upload(id, "a.txt", "two", "must_not_exist", "true"), http.StatusConflict, "invalid_request_error", "precondition failed: the current content of a.txt does not match")
		expectError(t, api.upload(id, "a.txt", "two", "must_not_exist", "true", "expected_content_sha256", digest("one")), http.StatusBadRequest, "invalid_request_error", "give at most one of expected_content_sha256 and must_not_exist")
		expectError(t, api.upload(id, "a.txt", "two", "must_not_exist", "yes"), http.StatusBadRequest, "invalid_request_error", "must_not_exist must be true")
		expectError(t, api.upload(id, "/a.txt", "two"), http.StatusConflict, "invalid_request_error", "path must be relative, without a leading /")
		expectError(t, api.upload(id, "A.txt", "two"), http.StatusConflict, "invalid_request_error", "another file's path equals this path under case folding")
		var second driveVersionAnswer
		decodeDrive(t, api.upload(id, "a.txt", "two", "expected_content_sha256", digest("one")), http.StatusCreated, &second)

		var files struct {
			Data     []map[string]any
			NextPage *string `json:"next_page"`
		}
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files?path_prefix=a", ""), http.StatusOK, &files)
		if len(files.Data) != 1 || files.Data[0]["version_id"] != second.VersionID || files.Data[0]["executable"] != false || files.NextPage != nil {
			t.Fatalf("files = %+v", files)
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files?path_prefix=/a", ""), http.StatusBadRequest, "invalid_request_error", "path_prefix: path must be relative, without a leading /")

		content := api.json(http.MethodGet, "/v1/drives/"+id+"/files/content?path=a.txt", "")
		if content.status != http.StatusOK || string(content.body) != "two" || content.header.Get("Content-Type") != "application/octet-stream" {
			t.Fatalf("download = %d %q %s", content.status, content.body, content.header.Get("Content-Type"))
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/content?path=missing.txt", ""), http.StatusNotFound, "not_found_error", "not found")
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/content", ""), http.StatusBadRequest, "invalid_request_error", "path is required")

		// DR-39: an old version's content after an overwrite.
		old := api.json(http.MethodGet, "/v1/drives/"+id+"/versions/"+first.VersionID+"/content", "")
		if old.status != http.StatusOK || string(old.body) != "one" {
			t.Fatalf("old version content = %d %q", old.status, old.body)
		}
		var version map[string]any
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/versions/"+first.VersionID, ""), http.StatusOK, &version)
		if writer, _ := version["writer"].(map[string]any); version["operation"] != "created" || version["redacted"] != false || writer["type"] != "api_actor" || writer["api_key_id"] == "" {
			t.Fatalf("version = %v", version)
		}
		other := api.newDrive(`{}`)
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+other+"/versions/"+first.VersionID, ""), http.StatusNotFound, "not_found_error", "not found")

		// DR-32: the current version cannot be redacted; a redacted old
		// version keeps its record and loses its content.
		expectError(t, api.json(http.MethodPost, "/v1/drives/"+id+"/versions/"+second.VersionID+"/redact", ""), http.StatusConflict, "invalid_request_error", "the current version cannot be redacted; overwrite or delete the file first")
		var redacted map[string]any
		decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id+"/versions/"+first.VersionID+"/redact", ""), http.StatusOK, &redacted)
		if redacted["redacted"] != true || redacted["redacted_by"] == nil {
			t.Fatalf("redacted = %v", redacted)
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/versions/"+first.VersionID+"/content", ""), http.StatusNotFound, "not_found_error", "not found")

		// DR-46: deleting twice answers the first deletion; a path never held
		// is the non-disclosing 404.
		var deleted, deletedAgain driveVersionAnswer
		decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path=a.txt&expected_content_sha256="+digest("two"), ""), http.StatusOK, &deleted)
		decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path=a.txt&expected_content_sha256="+digest("stale"), ""), http.StatusOK, &deletedAgain)
		if deletedAgain.VersionID != deleted.VersionID {
			t.Fatalf("second delete answered %s, want %s", deletedAgain.VersionID, deleted.VersionID)
		}
		expectError(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path=never.txt&expected_content_sha256="+digest("x"), ""), http.StatusNotFound, "not_found_error", "file not found")
		expectError(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path=a.txt", ""), http.StatusBadRequest, "invalid_request_error", "expected_content_sha256 must be 64 lowercase hexadecimal characters")
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/versions/"+deleted.VersionID+"/content", ""), http.StatusNotFound, "not_found_error", "not found")

		var versions struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions?path=a.txt", ""), http.StatusOK, &versions)
		var deletions int
		for _, v := range versions.Data {
			if v["operation"] == "deleted" {
				deletions++
				if v["size_bytes"] != float64(0) || v["content_sha256"] != nil {
					t.Fatalf("deletion = %v", v)
				}
			}
		}
		if len(versions.Data) != 3 || deletions != 1 {
			t.Fatalf("versions = %v; want created, modified and one deletion", versions.Data)
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions", ""), http.StatusBadRequest, "invalid_request_error", "path is required")
	})

	t.Run("limits", func(t *testing.T) {
		id := api.newDrive(`{"limits":{"max_bytes":3}}`)
		expectError(t, api.upload(id, "big.txt", "four"), http.StatusRequestEntityTooLarge, "request_too_large", "the write exceeds the drive's max_bytes limit of 3 bytes")

		// A declared length over the route cap is refused before the budget.
		var before string
		if err := api.env.admin.QueryRow(`SELECT tokens::text||' '||refilled_at::text FROM drive_upload_budgets`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/v1/drives/"+id+"/files", strings.NewReader(""))
		request.ContentLength = 501_000_001
		request.Header.Set("x-api-key", api.env.envKey)
		request.Header.Set("Content-Type", "multipart/form-data; boundary=x")
		recorder := httptest.NewRecorder()
		api.handler.ServeHTTP(recorder, request)
		expectError(t, driveResponse{recorder.Code, recorder.Header(), recorder.Body.Bytes()}, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the limit of 501000000 bytes")
		var after string
		if err := api.env.admin.QueryRow(`SELECT tokens::text||' '||refilled_at::text FROM drive_upload_budgets`).Scan(&after); err != nil || after != before {
			t.Fatalf("budget = %q, %v after the refusal; want %q untouched", after, err, before)
		}
	})

	t.Run("usage", func(t *testing.T) {
		// DR-52: the Workspace totals equal the sums over its drives, and the
		// route is never taken for a drive ID.
		var before struct{ Bytes, Files int64 }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/usage", ""), http.StatusOK, &before)
		first, second := api.newDrive(`{}`), api.newDrive(`{}`)
		decodeDrive(t, api.upload(first, "x", "12345"), http.StatusCreated, nil)
		decodeDrive(t, api.upload(second, "y", "123"), http.StatusCreated, nil)
		decodeDrive(t, api.upload(second, "z", "1"), http.StatusCreated, nil)
		var usage struct {
			Bytes    int64 `json:"bytes"`
			Files    int64 `json:"files"`
			MaxBytes int64 `json:"max_bytes"`
			MaxFiles int64 `json:"max_files"`
		}
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/usage", ""), http.StatusOK, &usage)
		if usage.Bytes-before.Bytes != 9 || usage.Files-before.Files != 3 || usage.MaxBytes != 500_000_000_000 || usage.MaxFiles != 100_000 {
			t.Fatalf("usage = %+v after %+v", usage, before)
		}
	})

	t.Run("lists", func(t *testing.T) {
		for range 101 {
			api.newDrive(`{}`)
		}
		var page struct {
			Data     []map[string]any
			NextPage *string `json:"next_page"`
		}
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives?limit=101", ""), http.StatusOK, &page)
		if len(page.Data) != 100 || page.NextPage == nil {
			t.Fatalf("limit=101 listed %d, next %v; want 100 and a next page", len(page.Data), page.NextPage)
		}
		var rest struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives?limit=100&page="+url.QueryEscape(*page.NextPage), ""), http.StatusOK, &rest)
		if len(rest.Data) == 0 || rest.Data[0]["id"] == page.Data[99]["id"] {
			t.Fatalf("second page = %v", rest.Data)
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives?limit=0", ""), http.StatusBadRequest, "invalid_request_error", "limit must be a positive integer")
		expectError(t, api.json(http.MethodGet, "/v1/drives?page=forged", ""), http.StatusBadRequest, "invalid_request_error", "page is not a page token of this list")
	})

	t.Run("changes", func(t *testing.T) {
		// DR-44 on a drive with more than 1,000 change records.
		id := api.newDrive(`{}`)
		ctx := context.Background()
		content, err := api.drives.PutContent(ctx, workspace.DefaultID, id, strings.NewReader("x"), 1, "")
		if err != nil {
			t.Fatal(err)
		}
		changes := make([]drive.Change, 0, 1001)
		for i := range 1001 {
			changes = append(changes, drive.Change{Path: fmt.Sprintf("f%04d", i), Base: drive.Base{Kind: drive.BaseMustNotExist}, ContentSHA256: content.SHA256})
		}
		if _, err := api.drives.Commit(ctx, workspace.DefaultID, id, drive.Writer{Kind: drive.WriterAPIKey, ID: "apikey_fixture"}, drive.ModeMachine, changes); err != nil {
			t.Fatal(err)
		}
		for query, want := range map[string]int{"": 20, "?limit=1000": 1000} {
			var list struct {
				Data    []map[string]any
				HasMore bool `json:"has_more"`
			}
			decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes"+query, ""), http.StatusOK, &list)
			if len(list.Data) != want || !list.HasMore {
				t.Fatalf("changes%s = %d records, has_more %v; want %d and more", query, len(list.Data), list.HasMore, want)
			}
		}
		for _, query := range []string{"?limit=1001", "?limit=0"} {
			expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes"+query, ""), http.StatusBadRequest, "invalid_request_error", "limit must be an integer from 1 to 1000")
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?after=-1", ""), http.StatusBadRequest, "invalid_request_error", "after must be an integer of at least 0")

		decodeDrive(t, api.upload(id, "late.txt", "late"), http.StatusCreated, nil)
		if _, err := api.env.admin.Exec(`UPDATE drive_changes SET created_at=now()-interval '30 days 1 minute' WHERE drive_id=$1 AND seq=1`, id); err != nil {
			t.Fatal(err)
		}
		expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?after=0", ""), http.StatusGone, "drive_cursor_expired_error", "after is below the oldest change record the drive keeps")
		var listed struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?after=1", ""), http.StatusOK, &listed)
		if len(listed.Data) != 1 || listed.Data[0]["path"] != "late.txt" {
			t.Fatalf("changes after 1 = %v", listed.Data)
		}
	})

	t.Run("change stream", func(t *testing.T) {
		id := api.newDrive(`{}`)
		decodeDrive(t, api.upload(id, "a", "1"), http.StatusCreated, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, api.url+"/v1/drives/"+id+"/changes/stream", nil)
		request.Header.Set("x-api-key", api.env.envKey)
		response, err := http.DefaultClient.Do(request)
		if err != nil || response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatalf("stream = %v, %v", response, err)
		}
		defer func() { _ = response.Body.Close() }()
		events := bufio.NewReader(response.Body)
		next := func() string {
			t.Helper()
			var event strings.Builder
			for {
				line, err := events.ReadString('\n')
				if err != nil {
					t.Fatalf("stream ended: %v", err)
				}
				if line == "\n" {
					return event.String()
				}
				event.WriteString(line)
			}
		}
		event := func(seq int) string {
			return fmt.Sprintf("event: drive.changed\ndata: {\"drive_id\":%q,\"seq\":%d}\n", id, seq)
		}
		if got := next(); got != event(1) {
			t.Fatalf("first event = %q; want the drive's last_seq", got)
		}
		decodeDrive(t, api.upload(id, "b", "2"), http.StatusCreated, nil)
		// A listener reconnect may repeat the last seq; the commit's hint
		// follows.
		for got := next(); got != event(2); got = next() {
			if got != event(1) {
				t.Fatalf("event after a commit = %q; want seq 2", got)
			}
		}
	})

	t.Run("restore", func(t *testing.T) {
		// DR-50: the file whose version at as_of the count cap removed is
		// listed; the other is restored.
		id := api.newDrive(`{}`)
		decodeDrive(t, api.upload(id, "kept.txt", "k0"), http.StatusCreated, nil)
		decodeDrive(t, api.upload(id, "gone.txt", "g0"), http.StatusCreated, nil)
		var at struct {
			Data []struct {
				CreatedAt string `json:"created_at"`
			}
		}
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions?path=gone.txt", ""), http.StatusOK, &at)
		decodeDrive(t, api.upload(id, "kept.txt", "k1"), http.StatusCreated, nil)
		for i := 1; i <= 11; i++ {
			decodeDrive(t, api.upload(id, "gone.txt", fmt.Sprintf("g%d", i)), http.StatusCreated, nil)
		}
		body, _ := json.Marshal(map[string]string{"as_of": at.Data[0].CreatedAt})
		var restored struct {
			Seq           *int64
			Restored      int
			RedactedPaths []string `json:"redacted_paths"`
			RemovedPaths  []string `json:"removed_paths"`
		}
		decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", string(body)), http.StatusOK, &restored)
		if restored.Seq == nil || restored.Restored != 1 || len(restored.RedactedPaths) != 0 || strings.Join(restored.RemovedPaths, ",") != "gone.txt" {
			t.Fatalf("restore = %+v", restored)
		}
		content := api.json(http.MethodGet, "/v1/drives/"+id+"/files/content?path=kept.txt", "")
		if string(content.body) != "k0" {
			t.Fatalf("kept.txt = %q after restore", content.body)
		}
		expectError(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", `{"as_of":"yesterday"}`), http.StatusBadRequest, "invalid_request_error", "as_of must be an RFC 3339 time")
		expectError(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", `{}`), http.StatusBadRequest, "invalid_request_error", "as_of is required")
	})
}

// A file part over the per-file limit, in a body under the route cap, on a
// full budget of its own.
func TestDriveUploadOverThePerFileLimit(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	reader, writer := io.Pipe()
	form := multipart.NewWriter(writer)
	go func() {
		_ = form.WriteField("path", "huge.bin")
		part, _ := form.CreateFormFile("file", "huge.bin")
		_, _ = io.CopyN(part, zeros{}, drive.MaxFileBytes+1)
		_ = form.Close()
		_ = writer.Close()
	}()
	response := api.request(http.MethodPost, "/v1/drives/"+api.newDrive(`{}`)+"/files", reader, form.FormDataContentType())
	_ = reader.CloseWithError(io.ErrClosedPipe)
	expectError(t, response, http.StatusRequestEntityTooLarge, "request_too_large", "file exceeds the limit of 500000000 bytes per file")
}

// zeros reads zero bytes without end.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestDriveUploadBudgetRefusal(t *testing.T) {
	api := newDriveAPI(t, drive.MinUploadCount)
	id := api.newDrive(`{}`)
	decodeDrive(t, api.upload(id, "a", "1"), http.StatusCreated, nil)
	refused := api.upload(id, "b", "1")
	expectError(t, refused, http.StatusTooManyRequests, "rate_limit_error", "")
	if retry := refused.header.Get("Retry-After"); retry == "" || retry == "0" {
		t.Fatalf("Retry-After = %q; want whole seconds of at least 1", retry)
	}
}

// DR-31 and DR-49 through the API, with rows aged in Postgres before any
// cleanup runs.
func TestDriveRetentionThroughTheAPI(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)

	t.Run("a file rewritten 12 times", func(t *testing.T) {
		id := api.newDrive(`{}`)
		for i := range 13 {
			decodeDrive(t, api.upload(id, "r.txt", fmt.Sprintf("v%02d", i)), http.StatusCreated, nil)
		}
		var versions struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions?path=r.txt", ""), http.StatusOK, &versions)
		var changes struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?limit=100", ""), http.StatusOK, &changes)
		var usage struct{ Usage struct{ Bytes, Files int64 } }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id, ""), http.StatusOK, &usage)
		if len(versions.Data) != 11 || versions.Data[0]["content_sha256"] != digest("v02") || len(changes.Data) != 13 || usage.Usage.Bytes != 3 || usage.Usage.Files != 1 {
			t.Fatalf("versions %d (first %v), changes %d, usage %+v; want 11 from v02, 13 and one 3-byte file", len(versions.Data), versions.Data[0]["content_sha256"], len(changes.Data), usage.Usage)
		}
	})

	t.Run("versions aged 30 days", func(t *testing.T) {
		id := api.newDrive(`{}`)
		for _, content := range []string{"o0", "o1", "o2", "o3"} {
			decodeDrive(t, api.upload(id, "old.txt", content), http.StatusCreated, nil)
		}
		decodeDrive(t, api.upload(id, "deleted.txt", "d0"), http.StatusCreated, nil)
		decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path=deleted.txt&expected_content_sha256="+digest("d0"), ""), http.StatusOK, nil)
		// One of old.txt's three old versions was replaced 30 days ago, and
		// deleted.txt was deleted 30 days ago; their change records are as old.
		for _, statement := range []string{
			`UPDATE drive_versions SET created_at=now()-interval '31 days',superseded_at=now()-interval '30 days 1 minute' WHERE drive_id=$1 AND (content_sha256=$2 OR (path='deleted.txt' AND operation='created'))`,
			`UPDATE drive_versions SET created_at=now()-interval '30 days 1 minute' WHERE drive_id=$1 AND path=$2 AND operation='deleted'`,
			`UPDATE drive_changes SET created_at=now()-interval '30 days 1 minute' WHERE drive_id=$1 AND (path='deleted.txt' OR content_sha256=$2)`,
		} {
			arg := digest("o0")
			if strings.Contains(statement, "path=$2") {
				arg = "deleted.txt"
			}
			if _, err := api.env.admin.Exec(statement, id, arg); err != nil {
				t.Fatal(err)
			}
		}
		var oldVersions, deletedVersions struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions?path=old.txt", ""), http.StatusOK, &oldVersions)
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions?path=deleted.txt", ""), http.StatusOK, &deletedVersions)
		if len(oldVersions.Data) != 3 || len(deletedVersions.Data) != 0 {
			t.Fatalf("old.txt versions %d, deleted.txt versions %d; want 3 and none", len(oldVersions.Data), len(deletedVersions.Data))
		}
		var changes struct{ Data []map[string]any }
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?limit=100", ""), http.StatusOK, &changes)
		for _, change := range changes.Data {
			if change["path"] == "deleted.txt" {
				t.Fatalf("change records list deleted.txt: %v", changes.Data)
			}
		}
		if len(changes.Data) != 3 {
			t.Fatalf("change records = %d; want old.txt's three later ones", len(changes.Data))
		}
		body, _ := json.Marshal(map[string]string{"as_of": time.Now().Add(-40 * 24 * time.Hour).UTC().Format(time.RFC3339)})
		var restored struct {
			Restored     int
			RemovedPaths []string `json:"removed_paths"`
		}
		decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", string(body)), http.StatusOK, &restored)
		if restored.Restored != 0 || strings.Join(restored.RemovedPaths, ",") != "old.txt" {
			t.Fatalf("restore = %+v; want old.txt listed as removed and deleted.txt untouched", restored)
		}
	})
}
