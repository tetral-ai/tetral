package httpapi_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/drive/skillpackage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type zipEntry struct {
	name   string
	body   []byte
	mode   fs.FileMode
	stored bool
}

func file(name, body string) zipEntry { return zipEntry{name: name, body: []byte(body), mode: 0o644} }

// skillZip writes entries with Unix modes and no extra fields.
func skillZip(t *testing.T, entries ...zipEntry) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		if entry.stored {
			header.Method = zip.Store
		}
		header.SetMode(entry.mode)
		part, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

func reportSkillMD(description string) string {
	return "---\nname: report\ndescription: " + description + "\n---\nWrite the report.\n"
}

func (a *driveAPI) uploadSkill(driveID string, archive []byte) driveResponse {
	a.t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, _ := writer.CreateFormFile("archive", "skill.zip")
	_, _ = part.Write(archive)
	_ = writer.Close()
	return a.request(http.MethodPost, "/v1/drives/"+driveID+"/skills", &body, writer.FormDataContentType())
}

type driveSkillAnswer struct {
	Name        string  `json:"name"`
	Description *string `json:"description"`
	Error       *string `json:"error"`
}

func TestDriveSkillArchiveRules(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	md := file("report/SKILL.md", reportSkillMD("Writes the weekly report."))
	entries := func(count int) []zipEntry {
		list := []zipEntry{md}
		for i := 1; i < count; i++ {
			list = append(list, file("report/notes/"+strconv.Itoa(i)+".md", "x"))
		}
		return list
	}
	zeros := make([]byte, skillpackage.MaxEntryBytes+1)
	expanded := func(extra int) []zipEntry {
		list := []zipEntry{md}
		for i := 0; i < 19; i++ {
			list = append(list, zipEntry{name: "report/data/" + strconv.Itoa(i) + ".bin", body: zeros[:skillpackage.MaxEntryBytes], mode: 0o644})
		}
		last := skillpackage.MaxEntryBytes - len(md.body) + extra
		return append(list, zipEntry{name: "report/data/last.bin", body: zeros[:last], mode: 0o644})
	}
	// sized builds an archive of exactly total bytes from stored entries,
	// whose framing does not depend on their length.
	sized := func(total int) []byte {
		build := func(last int) []byte {
			return skillZip(t, md,
				zipEntry{name: "report/a.bin", body: zeros[:10_000_000], mode: 0o644, stored: true},
				zipEntry{name: "report/b.bin", body: zeros[:10_000_000], mode: 0o644, stored: true},
				zipEntry{name: "report/c.bin", body: zeros[:last], mode: 0o644, stored: true})
		}
		archive := build(total - len(build(0)))
		if len(archive) != total {
			t.Fatalf("archive is %d bytes, want %d", len(archive), total)
		}
		return archive
	}
	description := "Writes the weekly report."
	for _, tt := range []struct {
		name    string
		archive []byte
		status  int
		message string
	}{
		{"1000 entries", skillZip(t, entries(1000)...), http.StatusCreated, ""},
		{"1001 entries", skillZip(t, entries(1001)...), http.StatusBadRequest, "the archive has more than 1000 entries"},
		{"an entry of 10485760 bytes", skillZip(t, md, zipEntry{name: "report/big.bin", body: zeros[:skillpackage.MaxEntryBytes], mode: 0o644}), http.StatusCreated, ""},
		{"an entry of 10485761 bytes", skillZip(t, md, zipEntry{name: "report/big.bin", body: zeros, mode: 0o644}), http.StatusBadRequest, "an archive entry expands to more than 10485760 bytes"},
		{"209715200 expanded bytes", skillZip(t, expanded(0)...), http.StatusCreated, ""},
		{"209715201 expanded bytes", skillZip(t, expanded(1)...), http.StatusBadRequest, "the archive expands to more than 209715200 bytes"},
		{"an archive of 30000000 bytes", sized(30_000_000), http.StatusCreated, ""},
		{"an archive of 30000001 bytes", sized(30_000_001), http.StatusRequestEntityTooLarge, "archive exceeds the limit of 30000000 bytes"},
		{"a symbolic link", skillZip(t, md, zipEntry{name: "report/link", body: []byte("../../etc"), mode: fs.ModeSymlink | 0o777}), http.StatusBadRequest, "archive entries must be regular files or directories, not links or special files"},
		{"a ../x entry", skillZip(t, md, file("../x", "x")), http.StatusBadRequest, "archive entry paths must be relative paths inside the root folder, without empty, ., .. or \\ segments or a drive letter"},
		{"two root folders", skillZip(t, md, file("other/x.md", "x")), http.StatusBadRequest, "the archive must hold exactly one root folder"},
		{"no SKILL.md", skillZip(t, file("report/x.md", "x")), http.StatusBadRequest, "the archive's root folder must hold SKILL.md"},
		{"a 2000-character description", skillZip(t, file("report/SKILL.md", reportSkillMD(strings.Repeat("d", 2000)))), http.StatusBadRequest, "SKILL.md frontmatter `description` must be at most 1024 characters (got 2000)"},
		{"license and metadata keys", skillZip(t, file("report/SKILL.md", "---\nname: report\ndescription: "+description+"\nlicense: Complete terms in LICENSE.txt\nmetadata:\n  owner: team-ai\n  tags: [weekly]\n---\n")), http.StatusCreated, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before, err := api.drives.LastSeq(context.Background(), workspace.DefaultID, id)
			if err != nil {
				t.Fatal(err)
			}
			response := api.uploadSkill(id, tt.archive)
			if tt.status != http.StatusCreated {
				expectError(t, response, tt.status, map[int]string{http.StatusBadRequest: "invalid_request_error", http.StatusRequestEntityTooLarge: "request_too_large"}[tt.status], tt.message)
				if after, err := api.drives.LastSeq(context.Background(), workspace.DefaultID, id); err != nil || after != before {
					t.Fatalf("last_seq = %d, %v after the refusal; want %d", after, err, before)
				}
				return
			}
			var skill driveSkillAnswer
			decodeDrive(t, response, http.StatusCreated, &skill)
			if skill.Name != "report" || skill.Description == nil || *skill.Description != description || skill.Error != nil {
				t.Fatalf("published = %+v; want report with its description and no error", skill)
			}
		})
	}

	t.Run("a part besides archive", func(t *testing.T) {
		before, _ := api.drives.LastSeq(context.Background(), workspace.DefaultID, id)
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		part, _ := writer.CreateFormFile("archive", "skill.zip")
		_, _ = part.Write(skillZip(t, file("report/SKILL.md", reportSkillMD("Changed."))))
		_ = writer.WriteField("path", "x")
		_ = writer.Close()
		expectError(t, api.request(http.MethodPost, "/v1/drives/"+id+"/skills", &body, writer.FormDataContentType()), http.StatusBadRequest, "invalid_request_error", "the body must hold exactly one multipart part, archive")
		if after, _ := api.drives.LastSeq(context.Background(), workspace.DefaultID, id); after != before {
			t.Fatalf("last_seq = %d after the refusal; want %d", after, before)
		}
	})

	t.Run("a declared length over the route cap", func(t *testing.T) {
		var before string
		if err := api.env.admin.QueryRow(`SELECT tokens::text||' '||refilled_at::text FROM drive_upload_budgets`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/v1/drives/"+id+"/skills", strings.NewReader(""))
		request.ContentLength = 32_505_857
		request.Header.Set("x-api-key", api.env.envKey)
		request.Header.Set("Content-Type", "multipart/form-data; boundary=x")
		recorder := httptest.NewRecorder()
		api.handler.ServeHTTP(recorder, request)
		expectError(t, driveResponse{recorder.Code, recorder.Header(), recorder.Body.Bytes()}, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the limit of 32505856 bytes")
		var after string
		if err := api.env.admin.QueryRow(`SELECT tokens::text||' '||refilled_at::text FROM drive_upload_budgets`).Scan(&after); err != nil || after != before {
			t.Fatalf("budget = %q, %v after the refusal; want %q untouched", after, err, before)
		}
	})
}

// A republish that drops a file and changes a mode is one commit: one
// deletion and two writes, each version with its entry's executable bit.
func TestDriveSkillRepublish(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	ctx := context.Background()
	id := api.newDrive(`{}`)
	decodeDrive(t, api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("First.")), file("report/run.sh", "echo"), file("report/drop.txt", "x"))), http.StatusCreated, nil)
	before, err := api.drives.LastSeq(ctx, workspace.DefaultID, id)
	if err != nil {
		t.Fatal(err)
	}
	executable := zipEntry{name: "report/run.sh", body: []byte("echo"), mode: 0o755}
	var skill driveSkillAnswer
	decodeDrive(t, api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("Second.")), executable)), http.StatusCreated, &skill)
	if skill.Description == nil || *skill.Description != "Second." {
		t.Fatalf("republished = %+v", skill)
	}
	records, _, err := api.drives.ListChanges(ctx, workspace.DefaultID, id, drive.ChangeQuery{AfterSeq: &before, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, record := range records {
		if record.Seq != before+1 {
			t.Fatalf("record %+v outside seq %d", record, before+1)
		}
		got[record.Path] = string(record.Operation) + map[bool]string{true: " executable", false: ""}[record.Executable]
	}
	want := map[string]string{"skills/report/SKILL.md": "modified", "skills/report/run.sh": "modified executable", "skills/report/drop.txt": "deleted"}
	if len(records) != len(want) || len(got) != len(want) {
		t.Fatalf("records = %v; want %v in one seq", got, want)
	}
	for path, operation := range want {
		if got[path] != operation {
			t.Fatalf("records = %v; want %v", got, want)
		}
	}
	versions, err := api.drives.ListVersions(ctx, workspace.DefaultID, id, "skills/report/run.sh")
	if err != nil || len(versions) != 2 || versions[0].Executable || !versions[1].Executable {
		t.Fatalf("run.sh versions = %+v, %v; want not executable, then executable", versions, err)
	}
}

// Two publishes of one Skill that overlap leave the folder equal to one of
// the two archives: each publish lists the folder inside its own commit, so
// the later one deletes what the earlier one wrote and the archive lacks.
func TestDriveSkillOverlappingPublishesLeaveOneArchive(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	ctx := context.Background()
	id := api.newDrive(`{}`)
	decodeDrive(t, api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("First.")), file("report/old.txt", "old"))), http.StatusCreated, nil)
	archives := []map[string]string{
		{"report/SKILL.md": reportSkillMD("A."), "report/a.txt": "a"},
		{"report/SKILL.md": reportSkillMD("B."), "report/b.txt": "b"},
	}
	lock, err := api.env.admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.ExecContext(ctx, `SELECT 1 FROM drives WHERE drive_id=$1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	statuses := make(chan int, len(archives))
	for i, archive := range archives {
		var entries []zipEntry
		for path, body := range archive {
			entries = append(entries, file(path, body))
		}
		data := skillZip(t, entries...)
		go func() { statuses <- api.uploadSkill(id, data).status }()
		waitForLockWaits(t, api.env.admin, i+1)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	for range archives {
		if status := <-statuses; status != http.StatusCreated {
			t.Fatalf("overlapping publish = %d; want %d", status, http.StatusCreated)
		}
	}
	files, _, err := api.drives.ListFiles(ctx, workspace.DefaultID, id, drive.FileQuery{Prefix: "skills/report/", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	folder := map[string]string{}
	for _, f := range files {
		folder[strings.TrimPrefix(f.Path, "skills/")] = f.ContentSHA256
	}
	for _, archive := range archives {
		if len(folder) == len(archive) {
			matches := true
			for path, body := range archive {
				sum := sha256.Sum256([]byte(body))
				matches = matches && folder[path] == hex.EncodeToString(sum[:])
			}
			if matches {
				return
			}
		}
	}
	t.Fatalf("folder after two overlapping publishes holds %d files %v; want exactly one archive's files", len(folder), files)
}

// waitForLockWaits returns once n sessions of the test database wait for a
// lock.
func waitForLockWaits(t *testing.T, admin *sql.DB, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := admin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions waited for a lock within 10s; want %d", waiting, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The entries of a Skills folder: folders holding SKILL.md directly, in byte
// order of name, each checked when read.
func TestDriveSkillEntries(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	ctx := context.Background()
	id := api.newDrive(`{}`)
	write := func(path, body string) {
		t.Helper()
		content, err := api.drives.PutContent(ctx, workspace.DefaultID, id, strings.NewReader(body), int64(len(body)), "")
		if err != nil {
			t.Fatal(err)
		}
		// An agent's edit: an ordinary commit from a Session's computer.
		agent := drive.Writer{Kind: drive.WriterSession, ID: "sesn_agent", ComputerID: "comp_agent"}
		if _, err := api.drives.Commit(ctx, workspace.DefaultID, id, agent, drive.ModeMachine, []drive.Change{{Path: path, Base: drive.Base{Kind: drive.BaseAny}, ContentSHA256: content.SHA256}}); err != nil {
			t.Fatal(err)
		}
	}
	valid := func(name string) string { return "---\nname: " + name + "\ndescription: " + name + " skill\n---\n" }
	write("skills/a-b/SKILL.md", valid("a-b"))
	write("skills/a/SKILL.md", "---\nname: a\ndescription: "+strings.Repeat("d", 2000)+"\n---\n")
	write("skills/nested/inner/SKILL.md", valid("inner"))
	write("skills/SKILL.md", valid("root"))

	var first struct {
		Data     []driveSkillAnswer `json:"data"`
		NextPage *string            `json:"next_page"`
	}
	decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/skills?limit=1", ""), http.StatusOK, &first)
	tooLong := "SKILL.md frontmatter `description` must be at most 1024 characters (got 2000)"
	if len(first.Data) != 1 || first.Data[0].Name != "a" || first.Data[0].Description != nil || first.Data[0].Error == nil || *first.Data[0].Error != tooLong || first.NextPage == nil {
		t.Fatalf("first page = %+v", first)
	}
	var second struct {
		Data     []driveSkillAnswer `json:"data"`
		NextPage *string            `json:"next_page"`
	}
	decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/skills?limit=1&page="+*first.NextPage, ""), http.StatusOK, &second)
	if len(second.Data) != 1 || second.Data[0].Name != "a-b" || second.Data[0].Description == nil || *second.Data[0].Description != "a-b skill" || second.NextPage != nil {
		t.Fatalf("second page = %+v; want a-b and no further page", second)
	}

	var one driveSkillAnswer
	decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/skills/a", ""), http.StatusOK, &one)
	if one.Error == nil || *one.Error != tooLong {
		t.Fatalf("retrieved = %+v", one)
	}
	expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/skills/nested", ""), http.StatusNotFound, "not_found_error", "")
	expectError(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/skills/nested", ""), http.StatusNotFound, "not_found_error", "")

	write("skills/a-b/scripts/run.sh", "echo")
	decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/skills/a-b", ""), http.StatusNoContent, nil)
	files, _, err := api.drives.ListFiles(ctx, workspace.DefaultID, id, drive.FileQuery{Prefix: "skills/a-b/", Limit: 10})
	if err != nil || len(files) != 0 {
		t.Fatalf("files under the deleted Skill = %+v, %v", files, err)
	}
	expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/skills/a-b", ""), http.StatusNotFound, "not_found_error", "")
}
