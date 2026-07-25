package skill_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/skill"
)

func TestBuildNormalizedPackageFromIndividualFiles(t *testing.T) {
	stageDir := t.TempDir()
	parts := stageUploadParts(t, stageDir, []uploadFile{
		{filename: "finance/data.txt", body: []byte("a,b\n1,2\n")},
		{filename: "finance/reports/q1.txt", body: []byte("q1\n")},
		{filename: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")},
	})
	defer cleanupParts(t, parts)

	pkg, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	if err != nil {
		t.Fatalf("BuildNormalizedPackage: %v", err)
	}
	defer func() { _ = pkg.Cleanup() }()

	if pkg.Directory != "finance" || pkg.Name != "financial-analysis" || pkg.Description != "Analyze financial data." {
		t.Fatalf("metadata = directory:%q name:%q description:%q", pkg.Directory, pkg.Name, pkg.Description)
	}
	if pkg.SizeBytes <= 0 || pkg.SHA256 == "" {
		t.Fatalf("normalized metadata missing size/sha: %+v", pkg)
	}

	entries := readNormalizedZip(t, pkg)
	assertZipNames(t, entries, []string{"finance/", "finance/SKILL.md", "finance/data.txt", "finance/reports/", "finance/reports/q1.txt"})
	assertZipEntry(t, entries["finance/"], zipEntryWant{mode: 0o755, dir: true})
	assertZipEntry(t, entries["finance/SKILL.md"], zipEntryWant{mode: 0o644, body: skillMD("financial-analysis", "Analyze financial data.")})
	assertZipEntry(t, entries["finance/data.txt"], zipEntryWant{mode: 0o644, body: []byte("a,b\n1,2\n")})
	assertZipEntry(t, entries["finance/reports/"], zipEntryWant{mode: 0o755, dir: true})
	assertZipEntry(t, entries["finance/reports/q1.txt"], zipEntryWant{mode: 0o644, body: []byte("q1\n")})
}

func TestBuildNormalizedPackageAcceptsLargeSkillMDBody(t *testing.T) {
	body := []byte("---\nname: large-body\ndescription: Keeps full instructions.\n---\n" + strings.Repeat("x", skill.MaxFrontmatterBytes+1024) + "\n")
	tests := []struct {
		name  string
		files []uploadFile
	}{
		{name: "individual files", files: []uploadFile{{filename: "finance/SKILL.md", body: body}}},
		{name: "zip package", files: []uploadFile{{filename: "upload.zip", body: buildZip(t, []zipFixtureEntry{{name: "finance/SKILL.md", body: body}})}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			parts := stageUploadParts(t, stageDir, tc.files)
			defer cleanupParts(t, parts)

			pkg, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
			if err != nil {
				t.Fatalf("BuildNormalizedPackage: %v", err)
			}
			defer func() { _ = pkg.Cleanup() }()

			if pkg.Name != "large-body" || pkg.Description != "Keeps full instructions." {
				t.Fatalf("metadata = name:%q description:%q", pkg.Name, pkg.Description)
			}
			entries := readNormalizedZip(t, pkg)
			assertZipNames(t, entries, []string{"finance/", "finance/SKILL.md"})
			assertZipEntry(t, entries["finance/SKILL.md"], zipEntryWant{mode: 0o644, body: body})
		})
	}
}

func TestBuildNormalizedPackageFromZipIsDeterministic(t *testing.T) {
	stageDir := t.TempDir()
	firstZip := buildZip(t, []zipFixtureEntry{
		{name: "finance/data.txt", body: []byte("a,b\n1,2\n"), modTime: time.Date(2025, 5, 1, 1, 2, 3, 0, time.UTC), mode: 0o600},
		{name: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data."), modTime: time.Date(2024, 4, 1, 1, 2, 3, 0, time.UTC), mode: 0o640},
	})
	secondZip := buildZip(t, []zipFixtureEntry{
		{name: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data."), modTime: time.Date(2030, 4, 1, 1, 2, 3, 0, time.UTC), mode: 0o777},
		{name: "finance/data.txt", body: []byte("a,b\n1,2\n"), modTime: time.Date(2031, 5, 1, 1, 2, 3, 0, time.UTC), mode: 0o777},
	})

	firstParts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: firstZip}})
	defer cleanupParts(t, firstParts)
	secondParts := stageUploadParts(t, stageDir, []uploadFile{{filename: "renamed-package", body: secondZip}})
	defer cleanupParts(t, secondParts)

	first, err := skill.BuildNormalizedPackage(context.Background(), firstParts, stageDir)
	if err != nil {
		t.Fatalf("BuildNormalizedPackage first: %v", err)
	}
	defer func() { _ = first.Cleanup() }()
	second, err := skill.BuildNormalizedPackage(context.Background(), secondParts, stageDir)
	if err != nil {
		t.Fatalf("BuildNormalizedPackage second: %v", err)
	}
	defer func() { _ = second.Cleanup() }()

	if first.SHA256 != second.SHA256 {
		t.Fatalf("normalized SHA mismatch: %s vs %s", first.SHA256, second.SHA256)
	}
	if first.SizeBytes != second.SizeBytes {
		t.Fatalf("normalized sizes differ: %d vs %d", first.SizeBytes, second.SizeBytes)
	}
	entries := readNormalizedZip(t, first)
	assertZipNames(t, entries, []string{"finance/", "finance/SKILL.md", "finance/data.txt"})
	for _, entry := range entries {
		if !entry.modified.Equal(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("%s modified = %s; want stable zip timestamp", entry.name, entry.modified)
		}
		if entry.comment != "" || len(entry.extra) != 0 || entry.nonUTF8 {
			t.Fatalf("%s retained unsafe metadata: comment=%q extra=%d nonUTF8=%v", entry.name, entry.comment, len(entry.extra), entry.nonUTF8)
		}
	}
}

func TestBuildNormalizedPackageFromZipAcceptsExplicitRootDirectory(t *testing.T) {
	tests := []struct {
		name    string
		entries []zipFixtureEntry
	}{
		{name: "root directory before files", entries: []zipFixtureEntry{
			{name: "finance/"},
			{name: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")},
			{name: "finance/data.txt", body: []byte("a,b\n1,2\n")},
		}},
		{name: "root directory after files", entries: []zipFixtureEntry{
			{name: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")},
			{name: "finance/data.txt", body: []byte("a,b\n1,2\n")},
			{name: "finance/"},
		}},
		{name: "nested directory after child", entries: []zipFixtureEntry{
			{name: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")},
			{name: "finance/reports/q1.txt", body: []byte("q1")},
			{name: "finance/reports/"},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			raw := buildZip(t, tc.entries)
			parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
			defer cleanupParts(t, parts)

			pkg, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
			if err != nil {
				t.Fatalf("BuildNormalizedPackage: %v", err)
			}
			defer func() { _ = pkg.Cleanup() }()

			if pkg.Directory != "finance" || pkg.Name != "financial-analysis" {
				t.Fatalf("metadata = directory:%q name:%q", pkg.Directory, pkg.Name)
			}
			entries := readNormalizedZip(t, pkg)
			if _, ok := entries["finance/"]; !ok {
				t.Fatalf("normalized zip missing explicit root directory: %v", keysOfZipEntries(entries))
			}
			assertZipEntry(t, entries["finance/"], zipEntryWant{mode: 0o755, dir: true})
		})
	}
}

func TestBuildNormalizedPackageRejectsMixedUploadModes(t *testing.T) {
	stageDir := t.TempDir()
	parts := stageUploadParts(t, stageDir, []uploadFile{
		{filename: "upload.zip", body: buildZip(t, []zipFixtureEntry{{name: "finance/SKILL.md", body: skillMD("n", "d")}})},
		{filename: "finance/data.txt", body: []byte("data")},
	})
	defer cleanupParts(t, parts)

	_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	var validation *skill.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
	}
}

func TestBuildNormalizedPackageRejectsInvalidRootsAndPaths(t *testing.T) {
	tests := []struct {
		name  string
		files []uploadFile
	}{
		{name: "missing root SKILL.md", files: []uploadFile{{filename: "finance/readme.md", body: []byte("readme")}}},
		{name: "rootless individual path", files: []uploadFile{
			{filename: "SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data.txt", body: []byte("data")},
		}},
		{name: "absolute path", files: []uploadFile{
			{filename: "/finance/SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data.txt", body: []byte("data")},
		}},
		{name: "drive letter path", files: []uploadFile{
			{filename: "C:/finance/SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data.txt", body: []byte("data")},
		}},
		{name: "nul byte path", files: []uploadFile{
			{filename: "finance/\x00bad", body: []byte("data")},
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
		}},
		{name: "invalid utf8 path", files: []uploadFile{
			{filename: "finance/" + string([]byte{0xff, 0xfe}) + ".txt", body: []byte("data")},
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
		}},
		{name: "oversized path", files: []uploadFile{
			{filename: "finance/" + strings.Repeat("a", 4097) + ".txt", body: []byte("data")},
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
		}},
		{name: "dot segment", files: []uploadFile{
			{filename: "finance/./SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data.txt", body: []byte("data")},
		}},
		{name: "empty segment", files: []uploadFile{
			{filename: "finance//SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data.txt", body: []byte("data")},
		}},
		{name: "unclean path", files: []uploadFile{
			{filename: "finance/a/../SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data.txt", body: []byte("data")},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			parts := stageUploadParts(t, stageDir, tc.files)
			defer cleanupParts(t, parts)

			_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
			var validation *skill.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
			}
		})
	}
}

func TestBuildNormalizedPackageRejectsMalformedZip(t *testing.T) {
	stageDir := t.TempDir()
	parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: []byte("not a zip")}})
	defer cleanupParts(t, parts)

	_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	var validation *skill.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
	}
}

func TestBuildNormalizedPackageRejectsAttackPathsAndZipMetadata(t *testing.T) {
	tests := []struct {
		name  string
		files []uploadFile
	}{
		{name: "parent path", files: []uploadFile{{filename: "finance/../SKILL.md", body: skillMD("n", "d")}}},
		{name: "backslash", files: []uploadFile{{filename: "finance\\SKILL.md", body: skillMD("n", "d")}}},
		{name: "duplicate", files: []uploadFile{
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
		}},
		{name: "file directory conflict", files: []uploadFile{
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
			{filename: "finance/data", body: []byte("file")},
			{filename: "finance/data/child.txt", body: []byte("child")},
		}},
		{name: "multiple roots", files: []uploadFile{
			{filename: "finance/SKILL.md", body: skillMD("n", "d")},
			{filename: "other/data.txt", body: []byte("data")},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			parts := stageUploadParts(t, stageDir, tc.files)
			defer cleanupParts(t, parts)

			_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
			var validation *skill.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
			}
		})
	}

	t.Run("zip symlink", func(t *testing.T) {
		stageDir := t.TempDir()
		raw := buildZip(t, []zipFixtureEntry{
			{name: "finance/SKILL.md", body: skillMD("n", "d")},
			{name: "finance/link", body: []byte("target"), mode: os.ModeSymlink | 0o777},
		})
		parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
		defer cleanupParts(t, parts)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var validation *skill.ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
		}
	})

	t.Run("duplicate explicit directory", func(t *testing.T) {
		stageDir := t.TempDir()
		raw := buildZip(t, []zipFixtureEntry{
			{name: "finance/"},
			{name: "finance/"},
			{name: "finance/SKILL.md", body: skillMD("n", "d")},
		})
		parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
		defer cleanupParts(t, parts)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var validation *skill.ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
		}
	})

	for _, tc := range []struct {
		name                  string
		mode                  os.FileMode
		extra                 []byte
		comment               string
		nonUTF8               bool
		creatorVersion        uint16
		externalAttrs         uint32
		overrideExternalAttrs bool
	}{
		{name: "zip extra metadata", mode: 0o644, extra: []byte{0x55, 0x54, 0x00, 0x00}},
		{name: "zip comment metadata", mode: 0o644, comment: "comment"},
		{name: "zip non utf8 metadata", mode: 0o644, nonUTF8: true},
		{name: "zip setuid regular", mode: os.ModeSetuid | 0o755},
		{name: "zip setgid regular", mode: os.ModeSetgid | 0o755},
		{name: "zip sticky regular", mode: os.ModeSticky | 0o755},
		{name: "zip device", mode: os.ModeDevice | 0o644},
		{name: "zip fifo", mode: os.ModeNamedPipe | 0o644},
		{name: "zip socket", mode: os.ModeSocket | 0o644},
		{name: "zip unknown creator external attrs", creatorVersion: 99 << 8, externalAttrs: uint32(0xa000|0o777) << 16, overrideExternalAttrs: true},
		{name: "zip fat hidden system attrs", creatorVersion: 0, externalAttrs: 0x26, overrideExternalAttrs: true},
		{name: "zip unix unknown low attrs", creatorVersion: 3 << 8, externalAttrs: uint32(0x8000|0o644)<<16 | 0x80, overrideExternalAttrs: true},
		{name: "zip unix hardlink like type attrs", creatorVersion: 3 << 8, externalAttrs: uint32(0x1000|0o644) << 16, overrideExternalAttrs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			entryName := "finance/SKILL.md"
			if tc.nonUTF8 {
				entryName = "finance/雪.md"
			}
			raw := buildZip(t, []zipFixtureEntry{
				{
					name:                  entryName,
					body:                  skillMD("n", "d"),
					mode:                  tc.mode,
					extra:                 tc.extra,
					comment:               tc.comment,
					nonUTF8:               tc.nonUTF8,
					creatorVersion:        tc.creatorVersion,
					externalAttrs:         tc.externalAttrs,
					overrideExternalAttrs: tc.overrideExternalAttrs,
				},
			})
			parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
			defer cleanupParts(t, parts)

			_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
			var validation *skill.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
			}
		})
	}
}

func TestBuildNormalizedPackageRejectsSizeLimits(t *testing.T) {
	t.Run("oversized regular file", func(t *testing.T) {
		stageDir := t.TempDir()
		part := stageDirectPart(t, stageDir, "finance/SKILL.md", skillMD("n", "d"))
		part.SizeBytes = skill.MaxPackageEntryBytes + 1
		defer cleanupParts(t, []skill.StagedUploadPart{part})

		_, err := skill.BuildNormalizedPackage(context.Background(), []skill.StagedUploadPart{part}, stageDir)
		var tooLarge *skill.RequestTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
		}
	})

	t.Run("oversized frontmatter", func(t *testing.T) {
		stageDir := t.TempDir()
		body := []byte("---\nname: n\ndescription: " + strings.Repeat("a", skill.MaxFrontmatterBytes) + "\n---\n")
		parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: body}})
		defer cleanupParts(t, parts)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var tooLarge *skill.RequestTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
		}
	})

	t.Run("expanded package cap", func(t *testing.T) {
		stageDir := t.TempDir()
		parts := make([]skill.StagedUploadPart, 0, 22)
		for i := 0; i < 21; i++ {
			parts = append(parts, stageSparsePart(t, stageDir, "finance/big-"+packageItoa(i)+".bin", skill.MaxPackageEntryBytes))
		}
		parts = append(parts, stageDirectPart(t, stageDir, "finance/SKILL.md", skillMD("n", "d")))
		defer cleanupParts(t, parts)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var tooLarge *skill.RequestTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
		}
	})

	t.Run("zip expanded package cap cleans staged entries", func(t *testing.T) {
		stageDir := t.TempDir()
		zeroBody := bytes.Repeat([]byte{0}, int(skill.MaxPackageEntryBytes))
		entries := []zipFixtureEntry{{name: "finance/SKILL.md", body: skillMD("n", "d")}}
		for i := 0; i < 20; i++ {
			entries = append(entries, zipFixtureEntry{name: "finance/big-" + packageItoa(i) + ".bin", body: zeroBody})
		}
		parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: buildZip(t, entries)}})
		defer cleanupParts(t, parts)
		before := listStageFiles(t, stageDir)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var tooLarge *skill.RequestTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
		}
		after := listStageFiles(t, stageDir)
		if strings.Join(after, "\n") != strings.Join(before, "\n") {
			t.Fatalf("stage files after failure = %v; want only original staged files %v", after, before)
		}
	})

	t.Run("zip entry one byte over cap", func(t *testing.T) {
		stageDir := t.TempDir()
		raw := buildZip(t, []zipFixtureEntry{
			{name: "finance/SKILL.md", body: skillMD("n", "d")},
			{name: "finance/big.bin", body: bytes.Repeat([]byte{0}, int(skill.MaxPackageEntryBytes)+1)},
		})
		parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
		defer cleanupParts(t, parts)
		before := listStageFiles(t, stageDir)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var tooLarge *skill.RequestTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
		}
		after := listStageFiles(t, stageDir)
		if strings.Join(after, "\n") != strings.Join(before, "\n") {
			t.Fatalf("stage files after failure = %v; want only original staged files %v", after, before)
		}
	})

	t.Run("normalized output cap cleans partial zip", func(t *testing.T) {
		stageDir := t.TempDir()
		parts := []skill.StagedUploadPart{
			stageDirectPart(t, stageDir, "finance/SKILL.md", skillMD("n", "d")),
			stageDirectPart(t, stageDir, "finance/random-1.bin", deterministicBytes(8*1024*1024, 1)),
			stageDirectPart(t, stageDir, "finance/random-2.bin", deterministicBytes(8*1024*1024, 2)),
			stageDirectPart(t, stageDir, "finance/random-3.bin", deterministicBytes(8*1024*1024, 3)),
			stageDirectPart(t, stageDir, "finance/random-4.bin", deterministicBytes(8*1024*1024, 4)),
		}
		defer cleanupParts(t, parts)
		before := listStageFiles(t, stageDir)

		_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
		var tooLarge *skill.RequestTooLargeError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
		}
		after := listStageFiles(t, stageDir)
		if strings.Join(after, "\n") != strings.Join(before, "\n") {
			t.Fatalf("stage files after failure = %v; want only original staged files %v", after, before)
		}
	})
}

func TestBuildNormalizedPackageRejectsInvalidFrontmatter(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "missing opening delimiter", body: []byte("Body without frontmatter.\n")},
		{name: "missing closing delimiter", body: []byte("---\nname: n\ndescription: d\nBody without closing delimiter.\n")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: tc.body}})
			defer cleanupParts(t, parts)

			_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
			var validation *skill.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
			}
		})
	}
}

func TestStageUploadPartEnforcesBudgetAndPartCount(t *testing.T) {
	stageDir := t.TempDir()
	var budget skill.UploadBudget

	_, err := skill.StageUploadPart(context.Background(), strings.NewReader(strings.Repeat("x", 30_000_000)), stageDir, "finance/big.bin", &budget)
	var tooLarge *skill.RequestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("StageUploadPart exact cap error = %T %v; want RequestTooLargeError", err, err)
	}

	budget = skill.UploadBudget{Parts: skill.MaxUploadFileParts}
	_, err = skill.StageUploadPart(context.Background(), strings.NewReader("x"), stageDir, "finance/overflow.txt", &budget)
	if !errors.As(err, &tooLarge) {
		t.Fatalf("StageUploadPart part overflow error = %T %v; want RequestTooLargeError", err, err)
	}

	budget = skill.UploadBudget{}
	first, err := skill.StageUploadPart(context.Background(), strings.NewReader(strings.Repeat("x", 29_999_990)), stageDir, "finance/first.bin", &budget)
	if err != nil {
		t.Fatalf("StageUploadPart first: %v", err)
	}
	defer cleanupParts(t, []skill.StagedUploadPart{first})
	before := listStageFiles(t, stageDir)
	_, err = skill.StageUploadPart(context.Background(), strings.NewReader(strings.Repeat("x", 20)), stageDir, "finance/second.bin", &budget)
	if !errors.As(err, &tooLarge) {
		t.Fatalf("StageUploadPart cumulative overflow error = %T %v; want RequestTooLargeError", err, err)
	}
	if budget.Bytes != 29_999_990 || budget.Parts != 1 {
		t.Fatalf("budget after cumulative overflow = bytes:%d parts:%d; want bytes:29999990 parts:1", budget.Bytes, budget.Parts)
	}
	after := listStageFiles(t, stageDir)
	if strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("stage files after cumulative overflow = %v; want %v", after, before)
	}
}

func TestStageUploadPartCleansPartialTempsOnFailure(t *testing.T) {
	t.Run("reader error", func(t *testing.T) {
		stageDir := t.TempDir()
		var budget skill.UploadBudget
		_, err := skill.StageUploadPart(context.Background(), failingReader{}, stageDir, "finance/data.txt", &budget)
		var validation *skill.ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("StageUploadPart error = %T %v; want ValidationError", err, err)
		}
		if files := listStageFiles(t, stageDir); len(files) != 0 {
			t.Fatalf("stage files after read failure = %v; want none", files)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		stageDir := t.TempDir()
		var budget skill.UploadBudget
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := skill.StageUploadPart(ctx, cancelingReader{cancel: cancel}, stageDir, "finance/data.txt", &budget)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StageUploadPart error = %T %v; want context.Canceled", err, err)
		}
		if files := listStageFiles(t, stageDir); len(files) != 0 {
			t.Fatalf("stage files after cancellation = %v; want none", files)
		}
	})
}

func TestBuildNormalizedPackageRejectsZipEntryOverflowBeforeStaging(t *testing.T) {
	stageDir := t.TempDir()
	entries := make([]zipFixtureEntry, 0, skill.MaxUploadFileParts+1)
	for i := 0; i < skill.MaxUploadFileParts+1; i++ {
		entries = append(entries, zipFixtureEntry{name: "finance/dir" + packageItoa(i) + "/"})
	}
	raw := buildZip(t, entries)
	parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
	defer cleanupParts(t, parts)

	_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	var tooLarge *skill.RequestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("BuildNormalizedPackage error = %T %v; want RequestTooLargeError", err, err)
	}
}

func TestBuildNormalizedPackageCleansZipEntryTempsOnFailure(t *testing.T) {
	stageDir := t.TempDir()
	raw := buildZip(t, []zipFixtureEntry{
		{name: "finance/data.txt", body: []byte("data")},
		{name: "other/data.txt", body: []byte("other")},
	})
	parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: raw}})
	defer cleanupParts(t, parts)
	before := listStageFiles(t, stageDir)

	_, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	var validation *skill.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("BuildNormalizedPackage error = %T %v; want ValidationError", err, err)
	}
	after := listStageFiles(t, stageDir)
	if strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("stage files after failure = %v; want only original staged files %v", after, before)
	}
}

func TestNormalizedZipEntryOrderIsLexicographic(t *testing.T) {
	stageDir := t.TempDir()
	parts := stageUploadParts(t, stageDir, []uploadFile{
		{filename: "finance/z.txt", body: []byte("z")},
		{filename: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")},
		{filename: "finance/a.txt", body: []byte("a")},
	})
	defer cleanupParts(t, parts)
	pkg, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	if err != nil {
		t.Fatalf("BuildNormalizedPackage: %v", err)
	}
	defer func() { _ = pkg.Cleanup() }()

	if got, want := readNormalizedZipOrder(t, pkg), []string{"finance/", "finance/SKILL.md", "finance/a.txt", "finance/z.txt"}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("zip order = %v; want %v", got, want)
	}
}

type uploadFile struct {
	filename string
	body     []byte
}

func stageUploadParts(t *testing.T, stageDir string, files []uploadFile) []skill.StagedUploadPart {
	t.Helper()
	var budget skill.UploadBudget
	parts := make([]skill.StagedUploadPart, 0, len(files))
	for _, file := range files {
		part, err := skill.StageUploadPart(context.Background(), bytes.NewReader(file.body), stageDir, file.filename, &budget)
		if err != nil {
			t.Fatalf("StageUploadPart %s: %v", file.filename, err)
		}
		parts = append(parts, part)
	}
	return parts
}

func cleanupParts(t *testing.T, parts []skill.StagedUploadPart) {
	t.Helper()
	if err := skill.CleanupStagedUploadParts(parts); err != nil {
		t.Fatalf("CleanupStagedUploadParts: %v", err)
	}
}

func skillMD(name, description string) []byte {
	return []byte("---\nname: " + name + "\ndescription: " + description + "\n---\nBody.\n")
}

type zipFixtureEntry struct {
	name                  string
	body                  []byte
	modTime               time.Time
	mode                  os.FileMode
	extra                 []byte
	comment               string
	nonUTF8               bool
	creatorVersion        uint16
	externalAttrs         uint32
	overrideExternalAttrs bool
}

func buildZip(t *testing.T, entries []zipFixtureEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		header.Extra = entry.extra
		header.Comment = entry.comment
		header.NonUTF8 = entry.nonUTF8
		if !entry.modTime.IsZero() {
			year, month, day := entry.modTime.Date()
			hour, minute, second := entry.modTime.Clock()
			header.ModifiedTime = uint16(hour<<11 | minute<<5 | second/2)      //nolint:staticcheck
			header.ModifiedDate = uint16((year-1980)<<9 | int(month)<<5 | day) //nolint:staticcheck
		}
		if entry.mode != 0 {
			header.SetMode(entry.mode)
		} else if strings.HasSuffix(entry.name, "/") {
			header.SetMode(os.ModeDir | 0o755)
		} else {
			header.SetMode(0o644)
		}
		if entry.overrideExternalAttrs {
			header.CreatorVersion = entry.creatorVersion
			header.ExternalAttrs = entry.externalAttrs
		}
		w, err := zw.CreateHeader(header)
		if err != nil {
			t.Fatalf("CreateHeader %s: %v", entry.name, err)
		}
		if len(entry.body) > 0 {
			if _, err := w.Write(entry.body); err != nil {
				t.Fatalf("Write %s: %v", entry.name, err)
			}
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close zip: %v", err)
	}
	return buf.Bytes()
}

func stageDirectPart(t *testing.T, stageDir, filename string, body []byte) skill.StagedUploadPart {
	t.Helper()
	temp, err := os.CreateTemp(stageDir, "skill-test-*.part")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := temp.Write(body); err != nil {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
		t.Fatalf("Write temp: %v", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		t.Fatalf("Close temp: %v", err)
	}
	sum := sha256.Sum256(body)
	return skill.StagedUploadPart{
		Filename:  filename,
		SizeBytes: int64(len(body)),
		SHA256:    hex.EncodeToString(sum[:]),
		TempPath:  temp.Name(),
	}
}

func stageSparsePart(t *testing.T, stageDir, filename string, size int64) skill.StagedUploadPart {
	t.Helper()
	temp, err := os.CreateTemp(stageDir, "skill-test-*.part")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if err := temp.Truncate(size); err != nil {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
		t.Fatalf("Truncate temp: %v", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		t.Fatalf("Close temp: %v", err)
	}
	return skill.StagedUploadPart{
		Filename:  filename,
		SizeBytes: size,
		TempPath:  temp.Name(),
	}
}

func deterministicBytes(size int, seed uint32) []byte {
	out := make([]byte, size)
	x := seed
	for i := range out {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		out[i] = byte(x)
	}
	return out
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

type cancelingReader struct {
	cancel context.CancelFunc
	done   bool
}

func (r cancelingReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.cancel()
	p[0] = 'x'
	return 1, nil
}

func readNormalizedZipOrder(t *testing.T, pkg *skill.StagedPackage) []string {
	t.Helper()
	rc, err := pkg.Open()
	if err != nil {
		t.Fatalf("Open package: %v", err)
	}
	defer func() { _ = rc.Close() }()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Read package: %v", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Read normalized zip: %v", err)
	}
	names := make([]string, 0, len(zr.File))
	for _, file := range zr.File {
		names = append(names, file.Name)
	}
	return names
}

func listStageFiles(t *testing.T, stageDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(stageDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func packageItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}

type normalizedZipEntry struct {
	name     string
	body     []byte
	mode     os.FileMode
	modified time.Time
	comment  string
	extra    []byte
	nonUTF8  bool
}

func readNormalizedZip(t *testing.T, pkg *skill.StagedPackage) map[string]normalizedZipEntry {
	t.Helper()
	rc, err := pkg.Open()
	if err != nil {
		t.Fatalf("Open package: %v", err)
	}
	defer func() { _ = rc.Close() }()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Read package: %v", err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != pkg.SHA256 {
		t.Fatalf("SHA256 = %s; want %s", got, pkg.SHA256)
	}
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Read normalized zip: %v", err)
	}
	entries := map[string]normalizedZipEntry{}
	for _, file := range zr.File {
		rc, err := file.Open()
		if err != nil {
			t.Fatalf("Open zip entry %s: %v", file.Name, err)
		}
		body, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("Read zip entry %s: %v", file.Name, err)
		}
		entries[file.Name] = normalizedZipEntry{
			name:     file.Name,
			body:     body,
			mode:     file.Mode(),
			modified: file.Modified.UTC(),
			comment:  file.Comment,
			extra:    file.Extra,
			nonUTF8:  file.NonUTF8,
		}
	}
	return entries
}

func assertZipNames(t *testing.T, entries map[string]normalizedZipEntry, want []string) {
	t.Helper()
	got := keysOfZipEntries(entries)
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("zip entries = %v; want %v", got, want)
	}
}

func keysOfZipEntries(entries map[string]normalizedZipEntry) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type zipEntryWant struct {
	mode os.FileMode
	dir  bool
	body []byte
}

func assertZipEntry(t *testing.T, entry normalizedZipEntry, want zipEntryWant) {
	t.Helper()
	if entry.mode.Perm() != want.mode {
		t.Fatalf("%s mode = %o; want %o", entry.name, entry.mode.Perm(), want.mode)
	}
	if want.dir != entry.mode.IsDir() {
		t.Fatalf("%s dir = %v; want %v", entry.name, entry.mode.IsDir(), want.dir)
	}
	if !want.dir && !bytes.Equal(entry.body, want.body) {
		t.Fatalf("%s body = %q; want %q", entry.name, entry.body, want.body)
	}
	if !entry.modified.Equal(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("%s modified = %s; want stable zip timestamp", entry.name, entry.modified)
	}
}
