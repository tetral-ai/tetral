package httpapi_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDrivePublishesCommonZIPProducers(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	paths, err := filepath.Glob("../drive/skillpackage/testdata/*.zip")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 5 {
		t.Fatalf("producer fixtures: %v", paths)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			id := api.newDrive(`{}`)
			archive, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			decodeDrive(t, api.uploadSkill(id, archive), http.StatusCreated, nil)
			files, _, err := api.drives.ListFiles(context.Background(), workspace.DefaultID, id, drive.FileQuery{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			expected := map[string]string{"skills/report/SKILL.md": "---\nname: report\ndescription: Writes reports.\n---\nReport instructions.\n", "skills/report/run.sh": "#!/bin/sh\necho report\n", "skills/report/café.txt": "non-ASCII filename\n"}
			if len(files) != len(expected) {
				t.Fatalf("published files: %+v", files)
			}
			for _, file := range files {
				content, ok := expected[file.Path]
				if !ok || file.ContentSHA256 != digest(content) || file.Executable != strings.HasSuffix(file.Path, "run.sh") {
					t.Fatalf("published file: %+v", file)
				}
			}
		})
	}
}

func TestDriveSkillCommitRefusalPreservesPublishedFiles(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	for _, kind := range []string{"path conflict", "quota"} {
		t.Run(kind, func(t *testing.T) {
			id := api.newDrive(`{}`)
			original := skillZip(t, file("report/SKILL.md", reportSkillMD("original")), file("report/old.txt", "keep"))
			decodeDrive(t, api.uploadSkill(id, original), http.StatusCreated, nil)
			before, _, err := api.drives.ListFiles(context.Background(), workspace.DefaultID, id, drive.FileQuery{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			seq, err := api.drives.LastSeq(context.Background(), workspace.DefaultID, id)
			if err != nil {
				t.Fatal(err)
			}
			entries := []zipEntry{file("report/SKILL.md", reportSkillMD("replacement")), file("report/a.txt", "new"), file("report/A.txt", "other")}
			want := http.StatusConflict
			if kind == "quota" {
				decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id, `{"limits":{"max_files":2}}`), http.StatusOK, nil)
				entries[2] = file("report/b.txt", "other")
				want = http.StatusRequestEntityTooLarge
			}
			response := api.uploadSkill(id, skillZip(t, entries...))
			if response.status != want {
				t.Fatalf("rejection: %d %s", response.status, response.body)
			}
			if kind == "path conflict" && response.header.Get("x-should-retry") != "false" {
				t.Fatal("path conflict missing no-retry hint")
			}
			after, _, err := api.drives.ListFiles(context.Background(), workspace.DefaultID, id, drive.FileQuery{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			afterSeq, err := api.drives.LastSeq(context.Background(), workspace.DefaultID, id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || afterSeq != seq {
				t.Fatalf("rejected publish changed files/seq: before %+v/%d after %+v/%d", before, seq, after, afterSeq)
			}
		})
	}
}

// A Skill published into a drive whose Skills path is a file is refused as
// a conflict that the same request would meet again.
func TestDriveSkillPublicationIsRefusedWhereTheSkillsPathIsAFile(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	decodeDrive(t, api.upload(id, "skills", "occupied"), http.StatusCreated, nil)
	response := api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("Writes reports."))))
	expectError(t, response, http.StatusConflict, "invalid_request_error", "")
	if response.header.Get("x-should-retry") != "false" {
		t.Fatalf("x-should-retry = %q; want false", response.header.Get("x-should-retry"))
	}
}

func TestDriveSkillPublicationDoesNotNormalizePaths(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	response := api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("description")), file("report/cafe\u0301.txt", "NFD")))
	if response.status != http.StatusConflict || !strings.Contains(string(response.body), "NFC") {
		t.Fatalf("NFD publication: %d %s", response.status, response.body)
	}
	files, _, err := api.drives.ListFiles(context.Background(), workspace.DefaultID, id, drive.FileQuery{Limit: 100})
	if err != nil || len(files) != 0 {
		t.Fatalf("invalid path partially published: %+v %v", files, err)
	}
}
