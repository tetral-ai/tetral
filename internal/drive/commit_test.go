package drive_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/drive"
)

// Two commits of one path with the same base, one through each service's pool,
// race 100 times. The drive lock must decide them one after the other: one
// gets a new version and the other a conflict naming it.
func TestConcurrentCommitsOfOnePathAcceptExactlyOne(t *testing.T) {
	f := newFixture(t)
	driveID := f.newDrive(drive.Settings{})
	contents := []string{f.put(driveID, "x"), f.put(driveID, "y"), f.put(driveID, "z")}
	base, current := newFile, ""
	for round := range 100 {
		var candidates []string
		for _, sha := range contents {
			if sha != current {
				candidates = append(candidates, sha)
			}
		}
		type outcome struct {
			writer drive.Writer
			result drive.CommitResult
			err    error
		}
		outcomes := make([]outcome, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, side := range []struct {
			service *drive.Service
			writer  drive.Writer
		}{{f.api, apiWriter}, {f.hub, sessionWriter}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				result, err := side.service.Commit(f.ctx, f.ws, driveID, side.writer, drive.ModeMachine, []drive.Change{write("race.txt", candidates[i], base)})
				outcomes[i] = outcome{side.writer, result, err}
			}()
		}
		close(start)
		wg.Wait()
		var winner, loser *outcome
		for i := range outcomes {
			if outcomes[i].err != nil {
				t.Fatalf("round %d: %v", round, outcomes[i].err)
			}
			if outcomes[i].result.Results[0].NewVersion {
				winner = &outcomes[i]
			} else {
				loser = &outcomes[i]
			}
		}
		if winner == nil || loser == nil {
			t.Fatalf("round %d: want one accepted commit and one rejected, got %+v", round, outcomes)
		}
		rejected := loser.result.Results[0]
		if rejected.Accepted || rejected.Reason != drive.ReasonConflict || rejected.Current == nil ||
			rejected.Current.VersionID != *winner.result.Results[0].VersionID || rejected.Current.Writer != winner.writer {
			t.Fatalf("round %d: loser = %+v, want a conflict naming the winner's version and writer", round, rejected)
		}
		for i := range outcomes {
			if &outcomes[i] == winner {
				current = candidates[i]
			}
		}
		base = basedOn(current)
	}
	f.assertUsage()
}

func TestCommitPathRules(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name     string
		existing []string
		path     string
		rule     string
	}{
		{"leading slash", nil, "/notes.md", "path must be relative, without a leading /"},
		{"over 1024 bytes", nil, strings.Repeat("a/", 512) + "b", "path must be at most 1024 bytes"},
		{"not NFC", nil, "e\u0301.md", "path must be valid UTF-8 in NFC"},
		{"over 30 non-starters", nil, "a" + strings.Repeat("\u0301", 31) + ".md", "path must be valid UTF-8 in NFC"},
		{"invalid UTF-8", nil, "a\xff.md", "path must be valid UTF-8 in NFC"},
		{"control character", nil, "a\u0007.md", "path must not contain control or format characters"},
		{"NUL", nil, "a\x00.md", "path must not contain control or format characters"},
		{"format character", nil, "a\u200b.md", "path must not contain control or format characters"},
		{"empty segment", nil, "a//b.md", "path must not contain empty, . or .. segments"},
		{"dot segment", nil, "a/./b.md", "path must not contain empty, . or .. segments"},
		{"dot-dot segment", nil, "../b.md", "path must not contain empty, . or .. segments"},
		{"segment over 255 bytes", nil, strings.Repeat("s", 256), "each path segment must be at most 255 bytes"},
		{"case-folded twin", []string{"report.md"}, "Report.md", "another file's path equals this path under case folding"},
		{"full case folding", []string{"Straße.md"}, "STRASSE.md", "another file's path equals this path under case folding"},
		{"file at a folder", []string{"a/b"}, "a/b/c", "a file exists at a folder of this path"},
		{"file at a case-folded folder", []string{"A/b"}, "a/B/c", "a file exists at a folder of this path"},
		{"files below the path", []string{"a/b/c"}, "a/b", "files exist below this path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := f.on(t)
			driveID := f.newDrive(drive.Settings{})
			sha := f.put(driveID, "content")
			for _, path := range tc.existing {
				f.commit(driveID, drive.ModeMachine, write(path, sha, newFile))
			}
			result := f.commit(driveID, drive.ModeMachine, write(tc.path, sha, newFile))
			got := result.Results[0]
			if got.Accepted || got.Reason != drive.ReasonPathRule || got.Rule != tc.rule || result.Seq != nil {
				t.Fatalf("result = %+v, seq %v; want path_rule %q", got, result.Seq, tc.rule)
			}
		})
	}

	t.Run("deletion first frees the folded key", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		sha := f.put(driveID, "content")
		f.commit(driveID, drive.ModeMachine, write("report.md", sha, newFile))
		result := f.commit(driveID, drive.ModeMachine, write("Report.md", sha, newFile), deletion("report.md", sha))
		for i, got := range result.Results {
			if !got.NewVersion {
				t.Fatalf("change %d = %+v, want a new version", i, got)
			}
		}
	})
}

func TestCommitDecisionRules(t *testing.T) {
	f := newFixture(t)

	t.Run("identical write keeps the current version", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		sha := f.put(driveID, "same")
		created := f.commit(driveID, drive.ModeMachine, write("a.txt", sha, newFile))
		again := f.commit(driveID, drive.ModeMachine, write("a.txt", sha, basedOn(hashOf("stale"))))
		got := again.Results[0]
		if !got.Accepted || got.NewVersion || *got.VersionID != *created.Results[0].VersionID || again.Seq != nil {
			t.Fatalf("identical write = %+v, seq %v; want the current version and no seq", got, again.Seq)
		}
	})

	t.Run("one conflict leaves the other change", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		old, fresh := f.put(driveID, "old"), f.put(driveID, "fresh")
		first := f.commit(driveID, drive.ModeMachine, write("a.txt", old, newFile))
		result := f.commit(driveID, drive.ModeMachine, write("a.txt", fresh, basedOn(hashOf("other"))), write("b.txt", fresh, newFile))
		if got := result.Results[0]; got.Reason != drive.ReasonConflict || got.Current.VersionID != *first.Results[0].VersionID {
			t.Fatalf("conflicting change = %+v", got)
		}
		if got := result.Results[1]; !got.NewVersion || result.Seq == nil || *result.Seq != *first.Seq+1 {
			t.Fatalf("other change = %+v with seq %v, want a new version in the next seq", got, result.Seq)
		}
	})

	t.Run("deletions free usage before writes are decided", func(t *testing.T) {
		f := f.on(t)
		limit := int64(10)
		driveID := f.newDrive(drive.Settings{MaxBytes: &limit})
		eight, five, six := f.put(driveID, "88888888"), f.put(driveID, "55555"), f.put(driveID, "666666")
		f.commit(driveID, drive.ModeMachine, write("a.txt", eight, newFile))
		result := f.commit(driveID, drive.ModeMachine, write("b.txt", five, newFile), write("c.txt", six, newFile), deletion("a.txt", eight))
		if !result.Results[2].NewVersion || !result.Results[0].NewVersion {
			t.Fatalf("deletion and the fitting write = %+v, %+v; want both accepted", result.Results[2], result.Results[0])
		}
		if got := result.Results[1]; got.Reason != drive.ReasonLimit || got.Rule != "the drive's max_bytes limit of 10 bytes" {
			t.Fatalf("crossing write = %+v", got)
		}
	})

	t.Run("a lowered cap blocks only growth", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		big, small, bigger := f.put(driveID, "0123456789"), f.put(driveID, "01"), f.put(driveID, "0123456789ab")
		f.commit(driveID, drive.ModeMachine, write("a.txt", big, newFile), write("b.txt", big, newFile), write("c.txt", big, newFile))
		limit := int64(5)
		if _, err := f.api.Update(f.ctx, f.ws, driveID, drive.SettingsUpdate{Limits: &drive.LimitSettings{MaxBytes: &limit}}); err != nil {
			t.Fatal(err)
		}
		result := f.commit(driveID, drive.ModeMachine, deletion("a.txt", big), write("b.txt", small, basedOn(big)), write("c.txt", bigger, basedOn(big)))
		if !result.Results[0].NewVersion || !result.Results[1].NewVersion {
			t.Fatalf("deletion and shrinking write = %+v, %+v; want both accepted", result.Results[0], result.Results[1])
		}
		if got := result.Results[2]; got.Reason != drive.ReasonLimit || got.Rule != "the drive's max_bytes limit of 5 bytes" {
			t.Fatalf("growing write = %+v", got)
		}
	})

	t.Run("file caps count new paths only", func(t *testing.T) {
		f := f.on(t)
		limit := int64(1)
		driveID := f.newDrive(drive.Settings{MaxFiles: &limit})
		one, two := f.put(driveID, "1"), f.put(driveID, "22")
		result := f.commit(driveID, drive.ModeMachine, write("a.txt", one, newFile), write("b.txt", one, newFile))
		if got := result.Results[1]; got.Reason != drive.ReasonLimit || got.Rule != "the drive's max_files limit of 1 files" {
			t.Fatalf("second file = %+v", got)
		}
		if got := f.commit(driveID, drive.ModeMachine, write("a.txt", two, basedOn(one))).Results[0]; !got.NewVersion {
			t.Fatalf("rewrite at the file cap = %+v", got)
		}
	})

	t.Run("workspace limits", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		sha := f.put(driveID, "abc")
		f.exec(`INSERT INTO drive_workspace_usage(workspace_id,bytes,files) VALUES ($1,$2,$3) ON CONFLICT (workspace_id) DO UPDATE SET bytes=EXCLUDED.bytes,files=EXCLUDED.files`,
			string(f.ws), drive.WorkspaceMaxBytes-2, 0)
		if got := commitDirect(t, f, driveID, write("a.txt", sha, newFile)); got.Rule != "the workspace limit of 500000000000 bytes" {
			t.Fatalf("write past the workspace bytes = %+v", got)
		}
		f.exec(`UPDATE drive_workspace_usage SET bytes=0,files=$2 WHERE workspace_id=$1`, string(f.ws), drive.WorkspaceMaxFiles)
		if got := commitDirect(t, f, driveID, write("a.txt", sha, newFile)); got.Rule != "the workspace limit of 100000 files" {
			t.Fatalf("write past the workspace files = %+v", got)
		}
		f.exec(`UPDATE drive_workspace_usage SET bytes=0,files=0 WHERE workspace_id=$1`, string(f.ws))
	})

	t.Run("missing content applies nothing", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		held := f.put(driveID, "held")
		missing := []string{hashOf("missing one"), hashOf("missing two")}
		_, err := f.api.Commit(f.ctx, f.ws, driveID, apiWriter, drive.ModeMachine, []drive.Change{
			write("a.txt", held, newFile), write("b.txt", missing[0], newFile), write("c.txt", missing[1], newFile), write("d.txt", missing[0], newFile),
		})
		var contentMissing *drive.ContentMissingError
		if !errors.As(err, &contentMissing) || strings.Join(contentMissing.MissingSHA256, ",") != strings.Join(missing, ",") {
			t.Fatalf("error = %v, want the distinct missing hashes in request order", err)
		}
		if seq, err := f.api.LastSeq(f.ctx, f.ws, driveID); err != nil || seq != 0 {
			t.Fatalf("last seq = %d, %v; want no commit", seq, err)
		}
	})

	t.Run("all or nothing rejects the whole commit", func(t *testing.T) {
		f := f.on(t)
		driveID := f.newDrive(drive.Settings{})
		sha := f.put(driveID, "content")
		_, err := f.api.Commit(f.ctx, f.ws, driveID, apiWriter, drive.ModeAllOrNothing, []drive.Change{write("ok.txt", sha, anyBase), write("/bad.txt", sha, anyBase)})
		var rejected *drive.CommitRejectedError
		if !errors.As(err, &rejected) || rejected.Index != 1 || rejected.Result.Reason != drive.ReasonPathRule {
			t.Fatalf("error = %v, want the second change's path_rule rejection", err)
		}
		if files, _, err := f.api.ListFiles(f.ctx, f.ws, driveID, drive.FileQuery{Limit: 10}); err != nil || len(files) != 0 {
			t.Fatalf("files = %v, %v; want none", files, err)
		}
	})
}

// commitDirect commits without the usage check, for fixtures that set the
// Workspace row by hand.
func commitDirect(t *testing.T, f *fixture, driveID string, change drive.Change) drive.ChangeResult {
	t.Helper()
	result, err := f.api.Commit(f.ctx, f.ws, driveID, apiWriter, drive.ModeMachine, []drive.Change{change})
	if err != nil {
		t.Fatal(err)
	}
	return result.Results[0]
}

func TestPutContentRefusesContentOverThePerFileLimit(t *testing.T) {
	f := newFixture(t)
	driveID := f.newDrive(drive.Settings{})
	_, err := f.api.PutContent(f.ctx, f.ws, driveID, strings.NewReader(""), drive.MaxFileBytes+1, "")
	if !errors.Is(err, drive.ErrContentTooLarge) {
		t.Fatalf("error = %v, want ErrContentTooLarge", err)
	}
}
