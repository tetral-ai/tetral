package blob_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
)

var _ blob.BlobStore = (*blob.FakeBlobStore)(nil)

func TestFakeBlobStorePutStoresBytesExactly(t *testing.T) {
	store := blob.NewFakeBlobStore()
	body := []byte("opaque archive bytes")
	if err := store.Put(context.Background(), "skills/ws/skl_1/v1.tar.gz", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := store.Bytes("skills/ws/skl_1/v1.tar.gz")
	if !ok {
		t.Fatal("Bytes: stored object missing")
	}
	if !bytes.Equal(got, body) {
		t.Errorf("stored bytes = %q; want %q", got, body)
	}
}

func TestFakeBlobStorePutCreateOnlyRejectsDuplicate(t *testing.T) {
	store := blob.NewFakeBlobStore()
	body := []byte("first")
	if err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	dup := []byte("second-attempt")
	err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader(dup), int64(len(dup)))
	if err == nil {
		t.Fatal("create-only Put must reject a duplicate key")
	}
	var dupErr *blob.DuplicateKeyError
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected *blob.DuplicateKeyError, got %T (%v)", err, err)
	}
	// Existing bytes must not be overwritten by the rejected attempt.
	got, ok := store.Bytes("skills/ws/skl/v1.tar.gz")
	if !ok {
		t.Fatal("after duplicate Put the original object should still exist")
	}
	if !bytes.Equal(got, body) {
		t.Errorf("after duplicate Put bytes = %q; want %q (original)", got, body)
	}
}

func TestFakeBlobStoreCompareAndSwapRequiresExactETag(t *testing.T) {
	store := blob.NewFakeBlobStore()
	if err := store.Put(context.Background(), "job", bytes.NewReader([]byte("claim")), int64(len("claim"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	metadata, err := store.HeadObject(context.Background(), "job")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if err = store.CompareAndSwap(context.Background(), "job", metadata.ETag, bytes.NewReader([]byte("result")), int64(len("result"))); err != nil {
		t.Fatalf("CompareAndSwap exact ETag: %v", err)
	}
	if err = store.CompareAndSwap(context.Background(), "job", metadata.ETag, bytes.NewReader([]byte("stale")), int64(len("stale"))); !errors.As(err, new(*blob.DuplicateKeyError)) {
		t.Fatalf("CompareAndSwap stale ETag = %v; want DuplicateKeyError", err)
	}
	got, ok := store.Bytes("job")
	if !ok || string(got) != "result" {
		t.Fatalf("stored bytes = %q/%t; want result/true", got, ok)
	}
}

func TestFakeBlobStorePutSizeMismatchRejects(t *testing.T) {
	store := blob.NewFakeBlobStore()
	body := []byte("ten-bytes!")
	err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader(body), 99)
	if err == nil {
		t.Fatal("Put must reject size mismatch")
	}
	if store.Has("skills/ws/skl/v1.tar.gz") {
		t.Error("size-mismatch Put must not persist bytes")
	}
}

func TestFakeBlobStoreGetReturnsExactBytes(t *testing.T) {
	store := blob.NewFakeBlobStore()
	body := []byte{0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef}
	if err := store.Put(context.Background(), "k", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := store.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Get returned %x; want %x", got, body)
	}
}

func TestFakeBlobStoreGetMissingReturnsNotFound(t *testing.T) {
	store := blob.NewFakeBlobStore()
	_, err := store.Get(context.Background(), "missing")
	if err == nil {
		t.Fatal("Get on missing key must error")
	}
	var nf *blob.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *blob.NotFoundError, got %T", err)
	}
}

func TestFakeBlobStoreHeadObjectReportsExistenceWithoutBytes(t *testing.T) {
	store := blob.NewFakeBlobStore()
	if err := store.Put(context.Background(), "present", bytes.NewReader([]byte("payload")), 7); err != nil {
		t.Fatalf("Put: %v", err)
	}
	meta, err := store.HeadObject(context.Background(), "present")
	if err != nil {
		t.Fatalf("HeadObject present: %v", err)
	}
	if meta.ETag == "" || meta.SizeBytes != int64(len("payload")) {
		t.Fatalf("HeadObject metadata = %+v; want stable etag and size", meta)
	}
	_, err = store.HeadObject(context.Background(), "missing")
	if err == nil {
		t.Fatal("HeadObject on missing key must error")
	}
	var nf *blob.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *blob.NotFoundError, got %T", err)
	}
}

func TestFakeBlobStoreDeleteRemovesOnlyRequestedKey(t *testing.T) {
	store := blob.NewFakeBlobStore()
	for _, k := range []string{"a", "b", "c"} {
		if err := store.Put(context.Background(), k, bytes.NewReader([]byte(k)), 1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	if err := store.Delete(context.Background(), "b"); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	if store.Has("b") {
		t.Error("Delete failed to remove b")
	}
	if !store.Has("a") || !store.Has("c") {
		t.Error("Delete must not touch other keys")
	}
	deletes := store.Deletes()
	if len(deletes) != 1 || deletes[0] != "b" {
		t.Errorf("Deletes log = %v; want [b]", deletes)
	}
}

func TestFakeBlobStoreDeleteMissingReturnsNotFound(t *testing.T) {
	store := blob.NewFakeBlobStore()
	err := store.Delete(context.Background(), "missing")
	if err == nil {
		t.Fatal("Delete on missing key must error")
	}
	var nf *blob.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *blob.NotFoundError, got %T", err)
	}
	deletes := store.Deletes()
	if len(deletes) != 1 || deletes[0] != "missing" {
		t.Errorf("Deletes log on missing = %v; want [missing] (call still recorded)", deletes)
	}
}

func TestFakeBlobStorePutHookRunsBeforeStorage(t *testing.T) {
	store := blob.NewFakeBlobStore()
	sentinel := errors.New("hook failure")
	store.SetPutHook(func(_ context.Context, _ string) error { return sentinel })
	body := []byte("payload")
	err := store.Put(context.Background(), "k", bytes.NewReader(body), int64(len(body)))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Put error = %v; want sentinel", err)
	}
	if store.Has("k") {
		t.Error("Put must not persist bytes when the hook rejects")
	}
}
