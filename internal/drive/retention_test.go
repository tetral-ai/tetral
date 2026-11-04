package drive_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/tetral-ai/tetral/internal/drive"
)

func TestCountCapKeepsTenOldVersionsAndEveryChangeRecord(t *testing.T) {
	f := newFixture(t)
	driveID := f.newDrive(drive.Settings{})
	sha := f.put(driveID, "v0")
	first := f.commit(driveID, drive.ModeMachine, write("a.txt", sha, newFile))
	for i := 1; i <= 12; i++ {
		next := f.put(driveID, fmt.Sprintf("v%d", i))
		f.commit(driveID, drive.ModeMachine, write("a.txt", next, basedOn(sha)))
		sha = next
	}
	versions, err := f.api.ListVersions(f.ctx, f.ws, driveID, "a.txt")
	if err != nil || len(versions) != 11 || *versions[0].ContentSHA256 != hashOf("v2") {
		t.Fatalf("versions = %d, %v; want v2 to v12", len(versions), err)
	}
	// The record of v0, whose version the cap removed, stays listed.
	zero := int64(0)
	records, more, err := f.api.ListChanges(f.ctx, f.ws, driveID, drive.ChangeQuery{AfterSeq: &zero, Limit: 100})
	if err != nil || more || len(records) != 13 || records[0].VersionID != *first.Results[0].VersionID {
		t.Fatalf("records = %d, more %v, %v; want all 13 from v0's", len(records), more, err)
	}
}

// Rows are aged in Postgres; no cleanup runs, so only the read rules can hide
// them.
func TestReadsDropVersionsPastThirtyDays(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		age  string
		kept bool
	}{
		{"30 days - 1 minute", true},
		{"30 days 1 minute", false},
	} {
		t.Run(tc.age, func(t *testing.T) {
			f := f.on(t)
			driveID := f.newDrive(drive.Settings{})
			old, fresh, gone := f.put(driveID, "old"), f.put(driveID, "fresh"), f.put(driveID, "gone")
			created := f.commit(driveID, drive.ModeMachine, write("a.txt", old, newFile), write("gone.txt", gone, newFile))
			removed := f.commit(driveID, drive.ModeMachine, write("a.txt", fresh, basedOn(old)), deletion("gone.txt", gone))
			f.exec(`UPDATE drive_versions SET created_at=now()-$2::interval-interval '1 hour',superseded_at=now()-$2::interval WHERE drive_id=$1 AND superseded_at IS NOT NULL`, driveID, tc.age)
			f.exec(`UPDATE drive_versions SET created_at=now()-$2::interval WHERE drive_id=$1 AND operation='deleted'`, driveID, tc.age)
			oldID, deletionID := *created.Results[0].VersionID, *removed.Results[1].VersionID

			wantErr := error(nil)
			if !tc.kept {
				wantErr = drive.ErrNotFound
			}
			for name, err := range map[string]error{
				"old version":         errOf(f.api.GetVersion(f.ctx, f.ws, driveID, oldID)),
				"current deletion":    errOf(f.api.GetVersion(f.ctx, f.ws, driveID, deletionID)),
				"old version content": f.readVersion(driveID, oldID),
				"old content by hash": f.readContent(driveID, old),
				"gone content":        f.readContent(driveID, gone),
			} {
				if !errors.Is(err, wantErr) {
					t.Errorf("%s: %v, want %v", name, err, wantErr)
				}
			}
			wantVersions := map[string]int{"a.txt": 1, "gone.txt": 0}
			if tc.kept {
				wantVersions = map[string]int{"a.txt": 2, "gone.txt": 2}
			}
			for path, want := range wantVersions {
				if versions, err := f.api.ListVersions(f.ctx, f.ws, driveID, path); err != nil || len(versions) != want {
					t.Errorf("%s versions = %d, %v; want %d", path, len(versions), err, want)
				}
			}
		})
	}
}

func TestChangeCursorExpiry(t *testing.T) {
	f := newFixture(t)
	driveID := f.newDrive(drive.Settings{})
	sha := f.put(driveID, "content")
	f.commit(driveID, drive.ModeMachine, write("a.txt", sha, newFile), write("b.txt", sha, newFile))
	f.commit(driveID, drive.ModeMachine, write("c.txt", sha, newFile))
	f.exec(`UPDATE drive_changes SET created_at=now()-interval '30 days 1 minute' WHERE drive_id=$1 AND seq=1`, driveID)

	if records, _, err := f.api.ListChanges(f.ctx, f.ws, driveID, drive.ChangeQuery{Limit: 10}); err != nil || len(records) != 1 || records[0].Seq != 2 {
		t.Fatalf("records = %+v, %v; want only seq 2", records, err)
	}
	for _, tc := range []struct {
		name    string
		seq     int64
		index   *int
		expired bool
	}{
		{"whole commits below the aged commit", 0, nil, true},
		{"whole commits at the aged commit", 1, nil, false},
		{"record below the newest aged record", 1, ptr(0), true},
		{"record at the newest aged record", 1, ptr(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, _, err := f.api.ListChanges(f.ctx, f.ws, driveID, drive.ChangeQuery{AfterSeq: &tc.seq, AfterIndex: tc.index, Limit: 10})
			if tc.expired {
				if !errors.Is(err, drive.ErrCursorExpired) {
					t.Fatalf("error = %v, want ErrCursorExpired", err)
				}
				return
			}
			if err != nil || len(records) != 1 || records[0].Seq != 2 {
				t.Fatalf("records = %+v, %v; want seq 2", records, err)
			}
		})
	}

	t.Run("the cleanup's removed record expires cursors below it", func(t *testing.T) {
		f.exec(`UPDATE drives SET removed_change_seq=2,removed_change_index=0 WHERE drive_id=$1`, driveID)
		one := int64(1)
		if _, _, err := f.api.ListChanges(f.ctx, f.ws, driveID, drive.ChangeQuery{AfterSeq: &one, Limit: 10}); !errors.Is(err, drive.ErrCursorExpired) {
			t.Fatalf("error = %v, want ErrCursorExpired", err)
		}
	})
}

func ptr[T any](v T) *T { return &v }

func errOf[T any](_ T, err error) error { return err }
