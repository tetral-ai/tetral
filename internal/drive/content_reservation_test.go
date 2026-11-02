package drive_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type observedPutStore struct {
	blob.BlobStore
	put func(context.Context, string, io.Reader, int64) error
}

func (s observedPutStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return s.put(ctx, key, r, size)
}

func TestContentReservationSurvivesFailureAfterBytesAreStored(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	roles := storagetest.OpenWorkloadDB(t, admin, "api")
	fake := blob.NewFakeBlobStore()
	var observedKey string
	failure := errors.New("failed after writing object")
	store := observedPutStore{BlobStore: fake, put: func(ctx context.Context, key string, r io.Reader, size int64) error {
		observedKey = key
		var reserved bool
		if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM drive_blob_cleanup WHERE workspace_id='default' AND blob_key=$1 AND delete_after>now())`, key).Scan(&reserved); err != nil {
			t.Fatal(err)
		}
		if !reserved {
			t.Fatal("bytes started before committed cleanup reservation")
		}
		if err := fake.Put(ctx, key, r, size); err != nil {
			t.Fatal(err)
		}
		return failure
	}}
	service, err := drive.NewService(dbconnect.NewClientForTesting(roles.DB), store, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := service.Create(context.Background(), workspace.DefaultID, drive.Settings{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.PutContent(context.Background(), workspace.DefaultID, d.ID, strings.NewReader("partial bytes"), 13, "")
	if !errors.Is(err, failure) || observedKey == "" || !fake.Has(observedKey) {
		t.Fatalf("failed upload: %v key %q", err, observedKey)
	}
	var count int
	if err := admin.QueryRow(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key=$1`, observedKey).Scan(&count); err != nil || count != 1 {
		t.Fatalf("lost reservation: %d %v", count, err)
	}
	if _, err := admin.Exec(`UPDATE drive_blob_cleanup SET delete_after=now()-interval '1 minute' WHERE blob_key=$1`, observedKey); err != nil {
		t.Fatal(err)
	}
	cleaner := drive.NewCleaner(dbconnect.NewClientForTesting(roles.OpenWorkload(t, "cleanup", nil)), store)
	if err := cleaner.DeleteDueBlobs(context.Background(), workspace.DefaultID); err != nil {
		t.Fatal(err)
	}
	if fake.Has(observedKey) {
		t.Fatal("due cleanup retained failed upload bytes")
	}
	if err := admin.QueryRow(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key=$1`, observedKey).Scan(&count); err != nil || count != 0 {
		t.Fatalf("cleanup reservation remains: %d %v", count, err)
	}
}
