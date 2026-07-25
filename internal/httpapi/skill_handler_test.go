package httpapi_test

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type skillHandlerEnv struct {
	blob     *blob.FakeBlobStore
	store    *skill.PostgreSQLSkillStore
	handler  *httpapi.SkillHandler
	stageDir string
}

func newSkillHandlerEnv(t *testing.T) *skillHandlerEnv {
	t.Helper()
	runtime, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	blobStore := blob.NewFakeBlobStore()
	store := skill.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime), blobStore)
	stageDir := t.TempDir()
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir), skill.WithPageTokenSecret(bytes.Repeat([]byte{7}, 32)))
	return &skillHandlerEnv{
		blob:     blobStore,
		store:    store,
		handler:  httpapi.NewSkillHandler(service, stageDir),
		stageDir: stageDir,
	}
}

func newSkillHandlerEnvWithAdmin(t *testing.T) (*skillHandlerEnv, *sql.DB) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	blobStore := blob.NewFakeBlobStore()
	store := skill.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime), blobStore)
	stageDir := t.TempDir()
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir), skill.WithPageTokenSecret(bytes.Repeat([]byte{7}, 32)))
	return &skillHandlerEnv{
		blob:     blobStore,
		store:    store,
		handler:  httpapi.NewSkillHandler(service, stageDir),
		stageDir: stageDir,
	}, admin
}

func (e *skillHandlerEnv) router(t *testing.T) http.Handler {
	t.Helper()
	return newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithSkillHandler(e.handler))
}

type uploadPart struct {
	name        string
	body        []byte
	shape       string
	fileName    string
	fileNameSet bool
}

func buildMultipartBody(t *testing.T, parts []uploadPart) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, part := range parts {
		var (
			writer io.Writer
			err    error
		)
		shape := part.shape
		if shape == "" {
			if part.name == "files[]" {
				shape = "file"
			} else {
				shape = "text"
			}
		}
		switch shape {
		case "file":
			filename := part.fileName
			if filename == "" && !part.fileNameSet {
				filename = "finance/SKILL.md"
			}
			writer, err = mw.CreateFormFile(part.name, filename)
		case "text":
			writer, err = mw.CreateFormField(part.name)
		default:
			t.Fatalf("unknown part shape %q", shape)
		}
		if err != nil {
			t.Fatalf("CreateFormPart(%s): %v", part.name, err)
		}
		if _, err := writer.Write(part.body); err != nil {
			t.Fatalf("write %s: %v", part.name, err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	return &buf, mw.FormDataContentType()
}

func skillMD(name, description string) []byte {
	return []byte("---\nname: " + name + "\ndescription: " + description + "\n---\nBody.\n")
}

func individualSkillParts(name, description string) []uploadPart {
	return []uploadPart{
		{name: "files[]", fileName: "finance/SKILL.md", body: skillMD(name, description)},
		{name: "files[]", fileName: "finance/analyze.py", body: []byte("print('ok')\n")},
	}
}

func buildSkillZip(t *testing.T, name, description string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, file := range []struct {
		name string
		body []byte
	}{
		{name: "finance/SKILL.md", body: skillMD(name, description)},
		{name: "finance/analyze.py", body: []byte("print('ok')\n")},
	} {
		writer, err := zw.Create(file.name)
		if err != nil {
			t.Fatalf("zip create %s: %v", file.name, err)
		}
		if _, err := writer.Write(file.body); err != nil {
			t.Fatalf("zip write %s: %v", file.name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func performMultipartRequest(t *testing.T, h http.Handler, method, path, contentType string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", contentType)
	setAuthHeader(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func performRequest(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	setAuthHeader(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func createSkillViaHTTP(t *testing.T, router http.Handler, name string) skill.Skill {
	t.Helper()
	body, contentType := buildMultipartBody(t, individualSkillParts(name, "Analyze CSV files."))
	rec := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", contentType, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("create skill status = %d (body=%q); want 200", rec.Code, rec.Body.String())
	}
	return decodeSkill(t, rec)
}

func createVersionViaHTTP(t *testing.T, router http.Handler, skillID, name, description string) skill.SkillVersion {
	t.Helper()
	body, contentType := buildMultipartBody(t, individualSkillParts(name, description))
	rec := performMultipartRequest(t, router, http.MethodPost, "/v1/skills/"+skillID+"/versions", contentType, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("create version status = %d (body=%q); want 200", rec.Code, rec.Body.String())
	}
	return decodeSkillVersion(t, rec)
}

func decodeSkill(t *testing.T, rec *httptest.ResponseRecorder) skill.Skill {
	t.Helper()
	var out skill.Skill
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode Skill (status=%d body=%q): %v", rec.Code, rec.Body.String(), err)
	}
	return out
}

func decodeSkillVersion(t *testing.T, rec *httptest.ResponseRecorder) skill.SkillVersion {
	t.Helper()
	var out skill.SkillVersion
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode SkillVersion (status=%d body=%q): %v", rec.Code, rec.Body.String(), err)
	}
	return out
}

func decodeSkillList(t *testing.T, rec *httptest.ResponseRecorder) skill.SkillListResult {
	t.Helper()
	var out skill.SkillListResult
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode Skill list (status=%d body=%q): %v", rec.Code, rec.Body.String(), err)
	}
	return out
}

func decodeVersionList(t *testing.T, rec *httptest.ResponseRecorder) skill.SkillVersionListResult {
	t.Helper()
	var out skill.SkillVersionListResult
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode SkillVersion list (status=%d body=%q): %v", rec.Code, rec.Body.String(), err)
	}
	return out
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) (errType, errMsg, requestID string) {
	t.Helper()
	var resp struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response (body=%q): %v", rec.Body.String(), err)
	}
	if resp.Type != "error" {
		t.Errorf("envelope type = %q; want error", resp.Type)
	}
	return resp.Error.Type, resp.Error.Message, resp.RequestID
}

func assertSkillUploadError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantType string) string {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d; want %d (body=%q)", rec.Code, wantStatus, rec.Body.String())
	}
	errType, msg, requestID := decodeError(t, rec)
	if errType != wantType {
		t.Fatalf("error.type = %q; want %q (body=%q)", errType, wantType, rec.Body.String())
	}
	if requestID == "" {
		t.Fatal("error response must include request_id")
	}
	return msg
}

func assertJSONKeys(t *testing.T, body []byte, want []string) {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw object: %v", err)
	}
	var got []string
	for key := range raw {
		got = append(got, key)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("json keys = %v; want %v (body=%s)", got, want, string(body))
	}
}

func TestSkillHandlerCreateSkillUsesFilesAndDisplayTitleShape(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	parts := append([]uploadPart{{name: "display_title", body: []byte("Financial Analysis")}}, individualSkillParts("financial-analysis", "Analyze CSV files.")...)
	body, contentType := buildMultipartBody(t, parts)

	rec := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", contentType, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body=%q); want 200", rec.Code, rec.Body.String())
	}
	assertJSONKeys(t, rec.Body.Bytes(), []string{"id", "type", "source", "display_title", "latest_version", "created_at", "updated_at"})
	got := decodeSkill(t, rec)
	if !strings.HasPrefix(got.ID, skill.IDPrefix) || got.Type != "skill" || got.Source != "custom" {
		t.Fatalf("Skill identity = %+v", got)
	}
	if got.DisplayTitle == nil || *got.DisplayTitle != "Financial Analysis" {
		t.Fatalf("display_title = %v", got.DisplayTitle)
	}
	if got.LatestVersion == nil || *got.LatestVersion == "" {
		t.Fatalf("latest_version = %v", got.LatestVersion)
	}
	if strings.Contains(rec.Body.String(), `"runtime"`) || strings.Contains(rec.Body.String(), `"current_version"`) || strings.Contains(rec.Body.String(), `"description"`) {
		t.Fatalf("response leaked legacy fields: %s", rec.Body.String())
	}
	if env.blob.Len() != 1 {
		t.Fatalf("blob count = %d; want 1", env.blob.Len())
	}
}

func TestSkillHandlerCreateSkillWithoutDisplayTitleAndZip(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	body, contentType := buildMultipartBody(t, []uploadPart{
		{name: "files[]", fileName: "package.zip", body: buildSkillZip(t, "zip-skill", "Analyze zip packages.")},
	})

	rec := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", contentType, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body=%q); want 200", rec.Code, rec.Body.String())
	}
	assertJSONKeys(t, rec.Body.Bytes(), []string{"id", "type", "source", "display_title", "latest_version", "created_at", "updated_at"})
	got := decodeSkill(t, rec)
	if got.DisplayTitle != nil {
		t.Fatalf("display_title = %v; want nil", got.DisplayTitle)
	}
	if !strings.Contains(rec.Body.String(), `"display_title":null`) {
		t.Fatalf("display_title must be explicit JSON null: %s", rec.Body.String())
	}
}

func TestSkillHandlerCreateVersionShapeAndRejectsDisplayTitle(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	parent := createSkillViaHTTP(t, router, "versioned-skill")

	rejectedBody, rejectedType := buildMultipartBody(t, append([]uploadPart{{name: "display_title", body: []byte("Nope")}}, individualSkillParts("versioned-skill", "v2")...))
	rejected := performMultipartRequest(t, router, http.MethodPost, "/v1/skills/"+parent.ID+"/versions", rejectedType, rejectedBody)
	assertSkillUploadError(t, rejected, http.StatusBadRequest, "invalid_request_error")

	version := createVersionViaHTTP(t, router, parent.ID, "versioned-skill", "Second version.")
	if !strings.HasPrefix(version.ID, skill.VersionIDPrefix) || version.Type != "skill_version" || version.SkillID != parent.ID {
		t.Fatalf("SkillVersion identity = %+v", version)
	}
	if version.Name != "versioned-skill" || version.Description != "Second version." || version.Directory != "finance" || version.Version == "" {
		t.Fatalf("SkillVersion metadata = %+v", version)
	}

	rec := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions/"+version.Version)
	if rec.Code != http.StatusOK {
		t.Fatalf("get version status = %d (body=%q); want 200", rec.Code, rec.Body.String())
	}
	assertJSONKeys(t, rec.Body.Bytes(), []string{"id", "type", "skill_id", "name", "description", "directory", "version", "created_at"})
	if strings.Contains(rec.Body.String(), `"size_bytes"`) || strings.Contains(rec.Body.String(), `"sha256"`) {
		t.Fatalf("version response leaked storage metadata: %s", rec.Body.String())
	}
}

func TestSkillHandlerMultipartValidationRejectsInvalidFields(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	longTitle := strings.Repeat("a", 1025)
	cases := []struct {
		name   string
		method string
		path   string
		parts  []uploadPart
	}{
		{name: "duplicate display_title", method: http.MethodPost, path: "/v1/skills", parts: append([]uploadPart{{name: "display_title", body: []byte("one")}, {name: "display_title", body: []byte("two")}}, individualSkillParts("dup-title", "d")...)},
		{name: "file display_title", method: http.MethodPost, path: "/v1/skills", parts: append([]uploadPart{{name: "display_title", shape: "file", fileName: "title.txt", body: []byte("bad")}}, individualSkillParts("file-title", "d")...)},
		{name: "invalid utf8 display_title", method: http.MethodPost, path: "/v1/skills", parts: append([]uploadPart{{name: "display_title", body: []byte{0xff}}}, individualSkillParts("bad-utf8", "d")...)},
		{name: "nul display_title", method: http.MethodPost, path: "/v1/skills", parts: append([]uploadPart{{name: "display_title", body: []byte("secret\x00title")}}, individualSkillParts("nul-title", "d")...)},
		{name: "oversized display_title", method: http.MethodPost, path: "/v1/skills", parts: append([]uploadPart{{name: "display_title", body: []byte(longTitle)}}, individualSkillParts("long-title", "d")...)},
		{name: "missing files", method: http.MethodPost, path: "/v1/skills", parts: []uploadPart{{name: "display_title", body: []byte("Only title")}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, contentType := buildMultipartBody(t, tc.parts)
			rec := performMultipartRequest(t, router, tc.method, tc.path, contentType, body)
			msg := assertSkillUploadError(t, rec, http.StatusBadRequest, "invalid_request_error")
			if strings.Contains(msg, "secret") || strings.Contains(msg, longTitle) {
				t.Fatalf("error message echoed submitted display_title: %q", msg)
			}
		})
	}
}

func TestSkillHandlerFileBudgetAndRouteCap(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)

	exactCapBody, exactCapType := buildMultipartBody(t, []uploadPart{
		{name: "files[]", fileName: "finance/a.bin", body: bytes.Repeat([]byte("x"), 10_000_000)},
		{name: "files[]", fileName: "finance/b.bin", body: bytes.Repeat([]byte("x"), 10_000_000)},
		{name: "files[]", fileName: "finance/c.bin", body: bytes.Repeat([]byte("x"), 10_000_000)},
	})
	exactCap := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", exactCapType, exactCapBody)
	assertSkillUploadError(t, exactCap, http.StatusRequestEntityTooLarge, "request_too_large")

	underCapBody, underCapType := buildMultipartBody(t, []uploadPart{
		{name: "files[]", fileName: "finance/a.bin", body: bytes.Repeat([]byte("x"), 10_000_000)},
		{name: "files[]", fileName: "finance/b.bin", body: bytes.Repeat([]byte("x"), 10_000_000)},
		{name: "files[]", fileName: "finance/c.bin", body: bytes.Repeat([]byte("x"), 9_999_999)},
	})
	underCap := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", underCapType, underCapBody)
	assertSkillUploadError(t, underCap, http.StatusBadRequest, "invalid_request_error")
	if env.blob.Len() != 0 {
		t.Fatalf("rejected uploads wrote blobs: %d", env.blob.Len())
	}
}

func TestSkillHandlerListQueryAndPageTokens(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	for _, name := range []string{"page-one", "page-two", "page-three"} {
		createSkillViaHTTP(t, router, name)
	}

	first := performRequest(t, router, http.MethodGet, "/v1/skills?limit=2")
	if first.Code != http.StatusOK {
		t.Fatalf("first page status = %d (body=%q); want 200", first.Code, first.Body.String())
	}
	firstList := decodeSkillList(t, first)
	if len(firstList.Data) != 2 || !firstList.HasMore || firstList.NextPage == nil {
		t.Fatalf("first list = %+v", firstList)
	}
	second := performRequest(t, router, http.MethodGet, "/v1/skills?page="+*firstList.NextPage)
	if second.Code != http.StatusOK {
		t.Fatalf("second page status = %d (body=%q); want 200", second.Code, second.Body.String())
	}
	secondList := decodeSkillList(t, second)
	if len(secondList.Data) != 1 || secondList.HasMore || secondList.NextPage != nil {
		t.Fatalf("second list = %+v", secondList)
	}

	custom := performRequest(t, router, http.MethodGet, "/v1/skills?source=custom")
	if custom.Code != http.StatusOK {
		t.Fatalf("source=custom status = %d (body=%q); want 200", custom.Code, custom.Body.String())
	}
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?source=anthropic"), http.StatusBadRequest, "invalid_request_error")
	if boundary := performRequest(t, router, http.MethodGet, "/v1/skills?limit=100"); boundary.Code != http.StatusOK {
		t.Fatalf("limit=100 status = %d (body=%q); want 200", boundary.Code, boundary.Body.String())
	}
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?limit=0"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?limit=not-a-number"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?limit=101"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?limit=1001"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?limit=1&limit=2"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?after_id=skl_old"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?page="+*firstList.NextPage+"x"), http.StatusBadRequest, "invalid_request_error")
}

func TestSkillHandlerVersionListPageTokenScope(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	parent := createSkillViaHTTP(t, router, "version-page")
	createVersionViaHTTP(t, router, parent.ID, "version-page", "v2")

	firstVersions := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions?limit=1")
	if firstVersions.Code != http.StatusOK {
		t.Fatalf("first versions status = %d (body=%q); want 200", firstVersions.Code, firstVersions.Body.String())
	}
	versionList := decodeVersionList(t, firstVersions)
	if len(versionList.Data) != 1 || versionList.NextPage == nil {
		t.Fatalf("version list = %+v", versionList)
	}
	secondVersions := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions?page="+*versionList.NextPage)
	if secondVersions.Code != http.StatusOK {
		t.Fatalf("second versions status = %d (body=%q); want 200", secondVersions.Code, secondVersions.Body.String())
	}
	if boundary := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions?limit=1000"); boundary.Code != http.StatusOK {
		t.Fatalf("version limit=1000 status = %d (body=%q); want 200", boundary.Code, boundary.Body.String())
	}
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions?limit=1001"), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills?page="+*versionList.NextPage), http.StatusBadRequest, "invalid_request_error")
	assertSkillUploadError(t, performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions?before_id=old"), http.StatusBadRequest, "invalid_request_error")
}

func TestSkillHandlerMalformedMultipartAndRouteCapErrors(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)

	malformed := strings.NewReader("--broken\r\nContent-Disposition: form-data; name=\"files[]\"; filename=\"finance/SKILL.md\"\r\n\r\n---\nname: broken\ndescription: Broken.\n---\n")
	malformedRec := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", "multipart/form-data; boundary=broken", malformed)
	assertSkillUploadError(t, malformedRec, http.StatusBadRequest, "invalid_request_error")

	parts := make([]uploadPart, 0, skill.MaxUploadFileParts)
	for i := 0; i < skill.MaxUploadFileParts; i++ {
		parts = append(parts, uploadPart{
			name:     "files[]",
			fileName: strings.Repeat("a", 33_000) + fmt.Sprintf("%04d/SKILL.md", i),
			body:     []byte("x"),
		})
	}
	routeCapBody, routeCapType := buildMultipartBody(t, parts)
	routeCapRec := performMultipartRequest(t, router, http.MethodPost, "/v1/skills", routeCapType, routeCapBody)
	assertSkillUploadError(t, routeCapRec, http.StatusRequestEntityTooLarge, "request_too_large")
}

func TestSkillHandlerDeleteOrderAndNullLatestVersion(t *testing.T) {
	env := newSkillHandlerEnv(t)
	router := env.router(t)
	parent := createSkillViaHTTP(t, router, "delete-order")

	rejectedParentDelete := performRequest(t, router, http.MethodDelete, "/v1/skills/"+parent.ID)
	assertSkillUploadError(t, rejectedParentDelete, http.StatusBadRequest, "invalid_request_error")

	deletedVersion := performRequest(t, router, http.MethodDelete, "/v1/skills/"+parent.ID+"/versions/"+*parent.LatestVersion)
	if deletedVersion.Code != http.StatusOK {
		t.Fatalf("delete version status = %d (body=%q); want 200", deletedVersion.Code, deletedVersion.Body.String())
	}
	assertJSONKeys(t, deletedVersion.Body.Bytes(), []string{"id", "type"})
	if !strings.Contains(deletedVersion.Body.String(), `"id":"`+*parent.LatestVersion+`"`) || !strings.Contains(deletedVersion.Body.String(), `"type":"skill_version_deleted"`) {
		t.Fatalf("delete version response = %s", deletedVersion.Body.String())
	}
	deletedContent := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID+"/versions/"+*parent.LatestVersion+"/content")
	assertSkillUploadError(t, deletedContent, http.StatusNotFound, "not_found_error")

	getParent := performRequest(t, router, http.MethodGet, "/v1/skills/"+parent.ID)
	if getParent.Code != http.StatusOK {
		t.Fatalf("get parent status = %d (body=%q); want 200", getParent.Code, getParent.Body.String())
	}
	got := decodeSkill(t, getParent)
	if got.LatestVersion != nil {
		t.Fatalf("latest_version = %v; want nil", got.LatestVersion)
	}
	if !strings.Contains(getParent.Body.String(), `"latest_version":null`) {
		t.Fatalf("latest_version must be explicit JSON null: %s", getParent.Body.String())
	}

	deletedParent := performRequest(t, router, http.MethodDelete, "/v1/skills/"+parent.ID)
	if deletedParent.Code != http.StatusOK {
		t.Fatalf("delete parent status = %d (body=%q); want 200", deletedParent.Code, deletedParent.Body.String())
	}
}

func TestSkillHandlerCrossWorkspaceIsolation(t *testing.T) {
	env, admin := newSkillHandlerEnvWithAdmin(t)
	router := env.router(t)
	otherWorkspace := workspace.ID("other_workspace")
	otherParent := createSkillDirectly(t, env.store, env.stageDir, otherWorkspace, "other-skill")

	for _, request := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/skills/" + otherParent.ID},
		{http.MethodGet, "/v1/skills/" + otherParent.ID + "/versions/" + *otherParent.LatestVersion},
		{http.MethodGet, "/v1/skills/" + otherParent.ID + "/versions/" + *otherParent.LatestVersion + "/content"},
		{http.MethodDelete, "/v1/skills/" + otherParent.ID + "/versions/" + *otherParent.LatestVersion},
		{http.MethodDelete, "/v1/skills/" + otherParent.ID},
	} {
		rec := performRequest(t, router, request.method, request.path)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d (body=%q); want 404", request.method, request.path, rec.Code, rec.Body.String())
		}
	}

	var active int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM skills WHERE workspace_id = $1 AND skill_id = $2 AND deleted_at IS NULL`,
		string(otherWorkspace), otherParent.ID,
	).Scan(&active); err != nil {
		t.Fatalf("count other workspace Skill: %v", err)
	}
	if active != 1 {
		t.Fatalf("other workspace active rows = %d; want 1", active)
	}
}

func createSkillDirectly(t *testing.T, store *skill.PostgreSQLSkillStore, stageDir string, workspaceID workspace.ID, name string) *skill.Skill {
	t.Helper()
	var budget skill.UploadBudget
	parts := []skill.StagedUploadPart{}
	for _, part := range individualSkillParts(name, "Other workspace.") {
		staged, err := skill.StageUploadPart(context.Background(), bytes.NewReader(part.body), stageDir, part.fileName, &budget)
		if err != nil {
			_ = skill.CleanupStagedUploadParts(parts)
			t.Fatalf("StageUploadPart: %v", err)
		}
		parts = append(parts, staged)
	}
	defer func() { _ = skill.CleanupStagedUploadParts(parts) }()
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir), skill.WithPageTokenSecret(bytes.Repeat([]byte{7}, 32)))
	created, err := service.CreateSkill(context.Background(), workspaceID, skill.CreateSkillInput{Files: parts})
	if err != nil {
		t.Fatalf("CreateSkill direct: %v", err)
	}
	return created
}
