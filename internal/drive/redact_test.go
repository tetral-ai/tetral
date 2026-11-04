package drive_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive"
)

var userWriter = drive.Writer{Kind: drive.WriterUser, ID: "user_test"}

func TestRedact(t *testing.T) {
	f := newFixture(t)
	driveID := f.newDrive(drive.Settings{})
	secret, other := f.put(driveID, "secret"), f.put(driveID, "other")
	first := f.commit(driveID, drive.ModeMachine, write("a.txt", secret, newFile), write("copy.txt", secret, newFile))
	f.commit(driveID, drive.ModeMachine, write("a.txt", other, basedOn(secret)))
	oldID, copyID := *first.Results[0].VersionID, *first.Results[1].VersionID

	if _, err := f.api.Redact(f.ctx, f.ws, driveID, copyID, userWriter); !errors.Is(err, drive.ErrRedactCurrent) {
		t.Fatalf("redacting the current version: %v, want ErrRedactCurrent", err)
	}
	if _, err := f.api.Redact(f.ctx, f.ws, driveID, "dver_unknown", userWriter); !errors.Is(err, drive.ErrNotFound) {
		t.Fatalf("redacting an unknown version: %v, want ErrNotFound", err)
	}

	before, err := f.api.GetVersion(f.ctx, f.ws, driveID, oldID)
	if err != nil {
		t.Fatal(err)
	}
	redacted, err := f.api.Redact(f.ctx, f.ws, driveID, oldID, userWriter)
	if err != nil || redacted.RedactedAt == nil || redacted.RedactedBy == nil || *redacted.RedactedBy != userWriter {
		t.Fatalf("redacted = %+v, %v; want redacted_at and redacted_by set", redacted, err)
	}
	unredacted := redacted
	unredacted.RedactedAt, unredacted.RedactedBy = nil, nil
	if !reflect.DeepEqual(unredacted, before) {
		t.Fatalf("redaction changed more than redacted_at and redacted_by: %+v, was %+v", redacted, before)
	}
	if again, err := f.api.Redact(f.ctx, f.ws, driveID, oldID, apiWriter); err != nil || !reflect.DeepEqual(again, redacted) {
		t.Fatalf("second redaction = %+v, %v; want the version unchanged", again, err)
	}
	if err := f.readVersion(driveID, oldID); !errors.Is(err, drive.ErrNotFound) {
		t.Fatalf("redacted content read: %v, want ErrNotFound", err)
	}
	records, _, err := f.api.ListChanges(f.ctx, f.ws, driveID, drive.ChangeQuery{Limit: 10})
	if err != nil || len(records) != 3 || records[0].VersionID != oldID {
		t.Fatalf("records = %+v, %v; want the redacted version's record kept", records, err)
	}
	if err := f.readContent(driveID, secret); err != nil {
		t.Fatalf("content named by copy.txt: %v", err)
	}

	f.commit(driveID, drive.ModeMachine, write("copy.txt", other, basedOn(secret)))
	if _, err := f.api.Redact(f.ctx, f.ws, driveID, copyID, userWriter); err != nil {
		t.Fatal(err)
	}
	if err := f.readContent(driveID, secret); !errors.Is(err, drive.ErrNotFound) {
		t.Fatalf("content no version names: %v, want ErrNotFound", err)
	}
}

func TestRestore(t *testing.T) {
	f := newFixture(t)
	driveID := f.newDrive(drive.Settings{})
	content := func(name string) string { return f.put(driveID, name) }
	f.commit(driveID, drive.ModeMachine, write("r/deleted.txt", content("d0"), newFile))
	atAsOf := f.commit(driveID, drive.ModeMachine,
		deletion("r/deleted.txt", hashOf("d0")),
		write("r/file.txt", content("f0"), newFile),
		write("r/redacted.txt", content("s0"), newFile),
		write("r/removed.txt", content("m0"), newFile),
		write("r/unchanged.txt", content("u0"), newFile),
		write("outside.txt", content("o0"), newFile),
	)
	at, err := f.api.GetVersion(f.ctx, f.ws, driveID, *atAsOf.Results[1].VersionID)
	if err != nil {
		t.Fatal(err)
	}
	asOf := at.CreatedAt

	f.commit(driveID, drive.ModeMachine,
		write("r/deleted.txt", content("d1"), newFile),
		write("r/file.txt", content("f1"), basedOn(hashOf("f0"))),
		write("r/redacted.txt", content("s1"), basedOn(hashOf("s0"))),
		write("r/new.txt", content("n1"), newFile),
		write("outside.txt", content("o1"), basedOn(hashOf("o0"))),
	)
	if _, err := f.api.Redact(f.ctx, f.ws, driveID, *atAsOf.Results[2].VersionID, userWriter); err != nil {
		t.Fatal(err)
	}
	// Eleven rewrites push the version at asOf past the count cap.
	for i := 1; i <= 11; i++ {
		f.commit(driveID, drive.ModeMachine, write("r/removed.txt", content(fmt.Sprintf("m%d", i)), basedOn(hashOf(fmt.Sprintf("m%d", i-1)))))
	}

	result, err := f.api.Restore(f.ctx, f.ws, driveID, "r/", asOf, userWriter)
	if err != nil {
		t.Fatal(err)
	}
	f.assertUsage()
	if result.Seq == nil || result.Restored != 1 || strings.Join(result.RedactedPaths, ",") != "r/redacted.txt" || strings.Join(result.RemovedPaths, ",") != "r/removed.txt" {
		t.Fatalf("restore = seq %v, %+v; want r/file.txt restored, r/redacted.txt redacted, r/removed.txt removed", result.Seq, result)
	}
	files, _, err := f.api.ListFiles(f.ctx, f.ws, driveID, drive.FileQuery{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, file := range files {
		got[file.Path] = file.ContentSHA256
	}
	want := map[string]string{
		"r/file.txt":      hashOf("f0"),  // V is a file version: written back.
		"r/redacted.txt":  hashOf("s1"),  // V is redacted: listed, left as it is.
		"r/deleted.txt":   hashOf("d1"),  // V is a deletion: no file at asOf, left as it is.
		"r/new.txt":       hashOf("n1"),  // No V, created after asOf: left as it is.
		"r/removed.txt":   hashOf("m11"), // No V, oldest kept is a modification: listed.
		"r/unchanged.txt": hashOf("u0"),
		"outside.txt":     hashOf("o1"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("files after restore = %v, want %v", got, want)
	}

	var now time.Time
	if err := f.admin.QueryRowContext(f.ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	again, err := f.api.Restore(f.ctx, f.ws, driveID, "r/", now, userWriter)
	if err != nil || again.Seq != nil || again.Restored != 0 {
		t.Fatalf("restore with nothing changed since = seq %v, %+v, %v; want no seq", again.Seq, again, err)
	}
}
