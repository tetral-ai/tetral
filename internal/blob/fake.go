package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"sync"
)

// FakeBlobStore is the in-memory test fake for the BlobStore
// interface. It enforces the same create-only Put contract the S3-
// compatible adapter does and records every Delete invocation so
// Skill-store cleanup paths can assert against the exact key list.
//
// FakeBlobStore is safe for concurrent use; the mutex protects the
// objects map and the deletes log. Tests rely on this guarantee for
// concurrent upload races.
type FakeBlobStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	deletes []string

	// putHook fires inside Put after the create-only check passes but
	// before the bytes are stored. Tests use it to inject a write
	// failure or to interleave with another transaction. nil means
	// no-op.
	putHook func(ctx context.Context, key string) error

	// headHook fires inside HeadObject before metadata is returned. Tests use
	// it to distinguish transient provider failures from missing objects.
	// nil means no-op.
	headHook func(ctx context.Context, key string) error

	// deleteHook fires inside Delete before the in-memory entry is
	// removed. Tests use it to inject a delete failure shaped like a
	// real provider error so the secret-safe error path can be
	// asserted. nil means no-op.
	deleteHook func(ctx context.Context, key string) error

	// deletePrefixHook fires inside DeletePrefix before matching keys
	// are removed. Tests use it to inject provider failures for
	// durable GC retry paths. nil means no-op.
	deletePrefixHook func(ctx context.Context, prefix string) error
}

// NewFakeBlobStore returns a FakeBlobStore with no objects and no
// recorded deletes.
func NewFakeBlobStore() *FakeBlobStore {
	return &FakeBlobStore{objects: make(map[string][]byte)}
}

// Put writes content under key. Returns DuplicateKeyError if the key
// already exists; the existing bytes are not overwritten. Reading the
// body fully before commit means a partial read on a failing reader
// surfaces as the underlying read error, not as a half-written
// object.
func (f *FakeBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(body)) != size {
		return &ConfigError{Message: "blob: declared size does not match body length"}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.objects[key]; exists {
		return &DuplicateKeyError{Key: key}
	}
	if f.putHook != nil {
		if err := f.putHook(ctx, key); err != nil {
			return err
		}
	}
	stored := make([]byte, len(body))
	copy(stored, body)
	f.objects[key] = stored
	return nil
}

// Get returns the bytes stored at key. Missing keys surface as
// NotFoundError. The returned reader is over a private copy so the
// caller cannot mutate the stored bytes.
func (f *FakeBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.objects[key]
	if !ok {
		return nil, &NotFoundError{Key: key}
	}
	clone := make([]byte, len(stored))
	copy(clone, stored)
	return io.NopCloser(bytes.NewReader(clone)), nil
}

// GetRange mirrors S3 byte-range reads for consumers that need bounded chunks.
func (f *FakeBlobStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 {
		return nil, &ConfigError{Message: "blob: range must be positive"}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.objects[key]
	if !ok {
		return nil, &NotFoundError{Key: key}
	}
	if offset >= int64(len(stored)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := offset + length
	if end > int64(len(stored)) {
		end = int64(len(stored))
	}
	clone := make([]byte, end-offset)
	copy(clone, stored[offset:end])
	return io.NopCloser(bytes.NewReader(clone)), nil
}

// HeadObject reports whether key exists without returning bytes.
func (f *FakeBlobStore) HeadObject(ctx context.Context, key string) (ObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMetadata{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.headHook != nil {
		if err := f.headHook(ctx, key); err != nil {
			return ObjectMetadata{}, err
		}
	}
	stored, ok := f.objects[key]
	if !ok {
		return ObjectMetadata{}, &NotFoundError{Key: key}
	}
	return metadataForBytes(stored), nil
}

// CopyObject copies an existing key to a new key with create-only
// semantics. It mirrors the S3-compatible provider-side copy contract
// used by resource-materialization tests without exposing bytes to callers.
func (f *FakeBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	source, ok := f.objects[sourceKey]
	if !ok {
		return &NotFoundError{Key: sourceKey}
	}
	if _, exists := f.objects[destinationKey]; exists {
		return &DuplicateKeyError{Key: destinationKey}
	}
	copied := make([]byte, len(source))
	copy(copied, source)
	f.objects[destinationKey] = copied
	return nil
}

// Delete removes the key and records the call. Missing keys surface
// as NotFoundError so cleanup paths can distinguish "already gone"
// from "transport failure". When deleteHook is set, its return value
// short-circuits the in-memory removal so tests can simulate a
// provider error.
func (f *FakeBlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, key)
	if f.deleteHook != nil {
		if err := f.deleteHook(ctx, key); err != nil {
			return err
		}
	}
	if _, ok := f.objects[key]; !ok {
		return &NotFoundError{Key: key}
	}
	delete(f.objects, key)
	return nil
}

// DeletePrefix removes every object whose key starts with prefix.
// Records each removed key in the deletes log so tests can assert the
// exact set. Public Skill delete is soft and never calls DeletePrefix; bounded
// resource/session cleanup may use it for deterministic prefix removal.
func (f *FakeBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deletePrefixHook != nil {
		if err := f.deletePrefixHook(ctx, prefix); err != nil {
			return err
		}
	}
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			f.deletes = append(f.deletes, key)
			delete(f.objects, key)
		}
	}
	return nil
}

// Len returns the number of stored objects. Test-only accessor.
func (f *FakeBlobStore) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

// Bytes returns a copy of the bytes stored at key. Test-only accessor;
// returns false if the key is missing.
func (f *FakeBlobStore) Bytes(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.objects[key]
	if !ok {
		return nil, false
	}
	clone := make([]byte, len(stored))
	copy(clone, stored)
	return clone, true
}

func metadataForBytes(body []byte) ObjectMetadata {
	sum := sha256.Sum256(body)
	return ObjectMetadata{
		ETag:      "sha256:" + hex.EncodeToString(sum[:]),
		SizeBytes: int64(len(body)),
	}
}

// Has reports whether key is present. Test-only accessor.
func (f *FakeBlobStore) Has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

// Deletes returns a copy of the recorded delete-key log. Test-only
// accessor; the order is the order Delete/DeletePrefix were called.
func (f *FakeBlobStore) Deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deletes))
	copy(out, f.deletes)
	return out
}

// SetPutHook installs a hook fired inside Put after create-only
// passes but before bytes are stored. Test-only.
func (f *FakeBlobStore) SetPutHook(hook func(ctx context.Context, key string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putHook = hook
}

// SetHeadHook installs a hook fired inside HeadObject before object lookup.
func (f *FakeBlobStore) SetHeadHook(hook func(ctx context.Context, key string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headHook = hook
}

// SetDeleteHook installs a hook fired inside Delete before the
// in-memory entry is removed. Returning a non-nil error short-circuits
// the deletion. Test-only.
func (f *FakeBlobStore) SetDeleteHook(hook func(ctx context.Context, key string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteHook = hook
}

// SetDeletePrefixHook installs a test hook that runs during DeletePrefix.
// Passing nil clears the hook.
func (f *FakeBlobStore) SetDeletePrefixHook(hook func(ctx context.Context, prefix string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletePrefixHook = hook
}
