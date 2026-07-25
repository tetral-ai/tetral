package resourceprojection

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
)

func TestCopyExecutorCopiesFirstRunAndSkipsMatchingSecondRun(t *testing.T) {
	ctx := context.Background()
	inner := blob.NewFakeBlobStore()
	spy := &spyBlobStore{inner: inner}
	action := testCopyAction()
	if err := inner.Put(ctx, action.SourceKey, bytes.NewReader([]byte("canonical bytes")), int64(len("canonical bytes"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	executor := CopyExecutor{Blob: spy}
	first, err := executor.CopyIfNeeded(ctx, action)
	if err != nil {
		t.Fatalf("CopyIfNeeded first: %v", err)
	}
	if first.Status != CopyStatusCopied {
		t.Fatalf("first status = %q; want copied", first.Status)
	}
	second, err := executor.CopyIfNeeded(ctx, action)
	if err != nil {
		t.Fatalf("CopyIfNeeded second: %v", err)
	}
	if second.Status != CopyStatusSkippedMatching {
		t.Fatalf("second status = %q; want skipped_matching", second.Status)
	}
	if len(spy.copyCalls) != 1 {
		t.Fatalf("copy calls = %v; want exactly one provider copy", spy.copyCalls)
	}
	assertStoredBytes(t, inner, action.DestinationKey, "canonical bytes")
}

func TestCopyExecutorRecopiesMetadataMismatchWithDeleteThenCopy(t *testing.T) {
	ctx := context.Background()
	inner := blob.NewFakeBlobStore()
	spy := &spyBlobStore{inner: inner}
	action := testCopyAction()
	if err := inner.Put(ctx, action.SourceKey, bytes.NewReader([]byte("fresh canonical")), int64(len("fresh canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	if err := inner.Put(ctx, action.DestinationKey, bytes.NewReader([]byte("stale session copy")), int64(len("stale session copy"))); err != nil {
		t.Fatalf("put stale destination: %v", err)
	}
	result, err := (CopyExecutor{Blob: spy}).CopyIfNeeded(ctx, action)
	if err != nil {
		t.Fatalf("CopyIfNeeded: %v", err)
	}
	if result.Status != CopyStatusRecopiedMismatch {
		t.Fatalf("status = %q; want recopied_mismatched", result.Status)
	}
	if len(spy.deleteCalls) != 1 || spy.deleteCalls[0] != action.DestinationKey {
		t.Fatalf("delete calls = %v; want destination delete before recopy", spy.deleteCalls)
	}
	if len(spy.copyCalls) != 1 {
		t.Fatalf("copy calls = %v; want one recopy", spy.copyCalls)
	}
	assertStoredBytes(t, inner, action.DestinationKey, "fresh canonical")
}

func TestCopyExecutorCanonicalMissingFailsWithoutDestination(t *testing.T) {
	ctx := context.Background()
	inner := blob.NewFakeBlobStore()
	action := testCopyAction()
	_, err := (CopyExecutor{Blob: inner}).CopyIfNeeded(ctx, action)
	if err == nil {
		t.Fatal("CopyIfNeeded accepted missing canonical source")
	}
	var copyErr *CopyError
	if !errors.As(err, &copyErr) {
		t.Fatalf("error = %T %v; want *CopyError", err, err)
	}
	if copyErr.Kind != "canonical_missing" || copyErr.Operation != "copy_object" || copyErr.ResourceID != action.ResourceID {
		t.Fatalf("copy error = %+v; want canonical_missing for resource", copyErr)
	}
	if inner.Has(action.DestinationKey) {
		t.Fatal("destination was created despite canonical_missing")
	}
}

func TestCopyExecutorExistingSessionCopySurvivesWhenCanonicalMissing(t *testing.T) {
	ctx := context.Background()
	inner := blob.NewFakeBlobStore()
	spy := &spyBlobStore{inner: inner}
	action := testCopyAction()
	if err := inner.Put(ctx, action.DestinationKey, bytes.NewReader([]byte("session copy")), int64(len("session copy"))); err != nil {
		t.Fatalf("put destination: %v", err)
	}
	result, err := (CopyExecutor{Blob: spy}).CopyIfNeeded(ctx, action)
	if err != nil {
		t.Fatalf("CopyIfNeeded: %v", err)
	}
	if result.Status != CopyStatusSkippedMatching {
		t.Fatalf("status = %q; want existing session copy to survive", result.Status)
	}
	if len(spy.deleteCalls) != 0 || len(spy.copyCalls) != 0 {
		t.Fatalf("delete calls = %v copy calls = %v; want no mutation while serving existing session copy", spy.deleteCalls, spy.copyCalls)
	}
	assertStoredBytes(t, inner, action.DestinationKey, "session copy")
}

func TestCopyExecutorDuplicateDestinationRaceBecomesIdempotentSkip(t *testing.T) {
	ctx := context.Background()
	inner := blob.NewFakeBlobStore()
	action := testCopyAction()
	if err := inner.Put(ctx, action.SourceKey, bytes.NewReader([]byte("canonical bytes")), int64(len("canonical bytes"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	race := &duplicateRaceBlobStore{inner: inner}
	result, err := (CopyExecutor{Blob: race}).CopyIfNeeded(ctx, action)
	if err != nil {
		t.Fatalf("CopyIfNeeded: %v", err)
	}
	if result.Status != CopyStatusSkippedRace {
		t.Fatalf("status = %q; want skipped_after_race", result.Status)
	}
	assertStoredBytes(t, inner, action.DestinationKey, "canonical bytes")
}

func TestCopyExecutorDuplicateDestinationRaceStillRecopiesMismatch(t *testing.T) {
	ctx := context.Background()
	inner := blob.NewFakeBlobStore()
	action := testCopyAction()
	if err := inner.Put(ctx, action.SourceKey, bytes.NewReader([]byte("fresh canonical")), int64(len("fresh canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	race := &staleDuplicateRaceBlobStore{inner: inner, action: action}
	result, err := (CopyExecutor{Blob: race}).CopyIfNeeded(ctx, action)
	if err != nil {
		t.Fatalf("CopyIfNeeded: %v", err)
	}
	if result.Status != CopyStatusRecopiedMismatch {
		t.Fatalf("status = %q; want recopied_mismatched", result.Status)
	}
	if race.deleteCalls != 1 || race.copyCalls != 2 {
		t.Fatalf("deleteCalls=%d copyCalls=%d; want stale race delete then recopy", race.deleteCalls, race.copyCalls)
	}
	assertStoredBytes(t, inner, action.DestinationKey, "fresh canonical")
}

func TestCopyExecutorProviderErrorMessageIsSafe(t *testing.T) {
	action := testCopyAction()
	secret := "fixture-provider-secret"
	provider := &failingHeadBlobStore{err: errors.New(secret + " files/ws_test/obj_secret")}
	_, err := (CopyExecutor{Blob: provider}).CopyIfNeeded(context.Background(), action)
	if err == nil {
		t.Fatal("CopyIfNeeded accepted provider head failure")
	}
	text := err.Error()
	if strings.Contains(text, secret) || strings.Contains(text, action.SourceKey) || strings.Contains(text, action.DestinationKey) {
		t.Fatalf("safe error leaked provider/key detail: %q", text)
	}
	var copyErr *CopyError
	if !errors.As(err, &copyErr) || copyErr.Kind != "head_destination_failed" {
		t.Fatalf("error = %T %v; want head_destination_failed CopyError", err, err)
	}
}

func testCopyAction() Action {
	return Action{
		Type:           ActionCopyObject,
		ResourceID:     "sesrsc_file",
		SourceKey:      "files/ws_test/obj_file",
		DestinationKey: "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file",
	}
}

func assertStoredBytes(t *testing.T, store *blob.FakeBlobStore, key string, want string) {
	t.Helper()
	got, ok := store.Bytes(key)
	if !ok {
		t.Fatalf("missing key %q", key)
	}
	if string(got) != want {
		t.Fatalf("stored bytes at %q = %q; want %q", key, got, want)
	}
}

type spyBlobStore struct {
	inner       blob.BlobStore
	headCalls   []string
	copyCalls   [][2]string
	deleteCalls []string
}

func (s *spyBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *spyBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *spyBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	s.headCalls = append(s.headCalls, key)
	return s.inner.HeadObject(ctx, key)
}

func (s *spyBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	s.copyCalls = append(s.copyCalls, [2]string{sourceKey, destinationKey})
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *spyBlobStore) Delete(ctx context.Context, key string) error {
	s.deleteCalls = append(s.deleteCalls, key)
	return s.inner.Delete(ctx, key)
}

func (s *spyBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

type duplicateRaceBlobStore struct {
	inner *blob.FakeBlobStore
}

func (s *duplicateRaceBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *duplicateRaceBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *duplicateRaceBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *duplicateRaceBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	if err := s.inner.CopyObject(ctx, sourceKey, destinationKey); err != nil {
		return err
	}
	return &blob.DuplicateKeyError{Key: destinationKey}
}

func (s *duplicateRaceBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *duplicateRaceBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

type staleDuplicateRaceBlobStore struct {
	inner       *blob.FakeBlobStore
	action      Action
	headCalls   int
	copyCalls   int
	deleteCalls int
}

func (s *staleDuplicateRaceBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *staleDuplicateRaceBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *staleDuplicateRaceBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	s.headCalls++
	if key == s.action.DestinationKey && s.headCalls == 1 {
		return blob.ObjectMetadata{}, &blob.NotFoundError{Key: key}
	}
	return s.inner.HeadObject(ctx, key)
}

func (s *staleDuplicateRaceBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	s.copyCalls++
	if s.copyCalls == 1 {
		if err := s.inner.Put(ctx, destinationKey, bytes.NewReader([]byte("stale race copy")), int64(len("stale race copy"))); err != nil {
			return err
		}
		return &blob.DuplicateKeyError{Key: destinationKey}
	}
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *staleDuplicateRaceBlobStore) Delete(ctx context.Context, key string) error {
	s.deleteCalls++
	return s.inner.Delete(ctx, key)
}

func (s *staleDuplicateRaceBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

type failingHeadBlobStore struct {
	err error
}

func (s *failingHeadBlobStore) Put(context.Context, string, io.Reader, int64) error {
	panic("unexpected Put")
}

func (s *failingHeadBlobStore) Get(context.Context, string) (io.ReadCloser, error) {
	panic("unexpected Get")
}

func (s *failingHeadBlobStore) HeadObject(context.Context, string) (blob.ObjectMetadata, error) {
	return blob.ObjectMetadata{}, s.err
}

func (s *failingHeadBlobStore) CopyObject(context.Context, string, string) error {
	panic("unexpected CopyObject")
}

func (s *failingHeadBlobStore) Delete(context.Context, string) error {
	panic("unexpected Delete")
}

func (s *failingHeadBlobStore) DeletePrefix(context.Context, string) error {
	panic("unexpected DeletePrefix")
}
