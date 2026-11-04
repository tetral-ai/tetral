package skillpackage_test

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive/skillpackage"
)

const skillText = "---\nname: report\ndescription: Writes reports.\n---\nReport instructions.\n"

type archiveEntry struct {
	header zip.FileHeader
	body   string
}

func normalEntry(name, body string) archiveEntry {
	h := zip.FileHeader{Name: name, Method: zip.Deflate}
	h.SetMode(0644)
	return archiveEntry{h, body}
}
func archiveBytes(t *testing.T, entries ...archiveEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	for _, e := range entries {
		w, err := z.CreateHeader(&e.header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write([]byte(e.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func readArchive(t *testing.T, b []byte) (*skillpackage.Package, error) {
	t.Helper()
	return skillpackage.Read(context.Background(), bytes.NewReader(b), int64(len(b)))
}

func TestProducerArchivesPreservePublishedContent(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.zip")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 5 {
		t.Fatalf("producer archives: %v", paths)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			pkg, err := readArchive(t, b)
			if err != nil {
				t.Fatal(err)
			}
			assertPackage(t, pkg)
		})
	}
	t.Run("Go FileInfoHeader timestamp metadata", func(t *testing.T) {
		dir := t.TempDir()
		var entries []archiveEntry
		for name, body := range map[string]string{"SKILL.md": skillText, "run.sh": "#!/bin/sh\necho report\n", "café.txt": "non-ASCII filename\n"} {
			mode := fs.FileMode(0644)
			if name == "run.sh" {
				mode = 0755
			}
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, []byte(body), mode); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			header, err := zip.FileInfoHeader(info)
			if err != nil {
				t.Fatal(err)
			}
			header.Name = "report/" + name
			entries = append(entries, archiveEntry{*header, body})
		}
		pkg, err := readArchive(t, archiveBytes(t, entries...))
		if err != nil {
			t.Fatal(err)
		}
		assertPackage(t, pkg)
	})
}
func assertPackage(t *testing.T, pkg *skillpackage.Package) {
	t.Helper()
	want := map[string]string{"SKILL.md": skillText, "run.sh": "#!/bin/sh\necho report\n", "café.txt": "non-ASCII filename\n"}
	if pkg.Name != "report" || pkg.Description != "Writes reports." || len(pkg.Files) != len(want) {
		t.Fatalf("package: %+v", pkg)
	}
	for _, f := range pkg.Files {
		r, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		text, ok := want[f.Path]
		if !ok || string(b) != text || f.Executable != (f.Path == "run.sh") {
			t.Fatalf("file %+v: %q", f, b)
		}
	}
}

func TestBenignZIPMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*zip.FileHeader)
	}{
		{"comment", func(h *zip.FileHeader) { h.Comment = "ordinary archive comment" }},
		{"timestamp extra", func(h *zip.FileHeader) { h.Modified = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"synthetic Windows Explorer archive bit", func(h *zip.FileHeader) { h.CreatorVersion = 20; h.ExternalAttrs = 0x20 }},
		{"synthetic WinRAR DOS flags", func(h *zip.FileHeader) { h.CreatorVersion = 11<<8 | 20; h.ExternalAttrs = 0x27 }},
		{"Unix permissions without type", func(h *zip.FileHeader) { h.CreatorVersion = 3<<8 | 20; h.ExternalAttrs = 0644 << 16 }},
		{"raw UTF8 without flag", func(h *zip.FileHeader) { h.Name = "report/café.txt"; h.NonUTF8 = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := normalEntry("report/extra.txt", "data")
			tc.edit(&e.header)
			if _, err := readArchive(t, archiveBytes(t, normalEntry("report/SKILL.md", skillText), e)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArchivePathAndTypeRules(t *testing.T) {
	md := normalEntry("report/SKILL.md", skillText)
	for _, tc := range []struct {
		name    string
		entries []archiveEntry
		want    string
	}{
		{"traversal", []archiveEntry{normalEntry("report/../outside", "x")}, "archive entry paths"},
		{"absolute", []archiveEntry{normalEntry("/report/outside", "x")}, "archive entry paths"},
		{"invalid UTF8", []archiveEntry{normalEntry("report/\xff", "x")}, "archive entry names must be UTF-8"},
		{"duplicate", []archiveEntry{normalEntry("report/dup", "x"), normalEntry("report/dup", "y")}, "the archive holds a path twice"},
		{"file then folder", []archiveEntry{normalEntry("report/node", "x"), normalEntry("report/node/child", "y")}, "file where another entry needs a folder"},
		{"folder then file", []archiveEntry{normalEntry("report/node/child", "y"), normalEntry("report/node", "x")}, "file where another entry needs a folder"},
		{"macOS sidecar root", []archiveEntry{normalEntry("__MACOSX/._report", "x")}, "exactly one root folder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readArchive(t, archiveBytes(t, append([]archiveEntry{md}, tc.entries...)...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %s", err, tc.want)
			}
		})
	}
	for _, mode := range []fs.FileMode{fs.ModeSymlink | 0777, fs.ModeNamedPipe | 0600, fs.ModeDevice | 0600, fs.ModeSocket | 0600, fs.ModeSetuid | 0755, fs.ModeSetgid | 0755, fs.ModeSticky | 0755} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			e := normalEntry("report/special", "x")
			e.header.SetMode(mode)
			if _, err := readArchive(t, archiveBytes(t, md, e)); err == nil {
				t.Fatal("special entry accepted")
			}
		})
	}
}
