// Package blob defines the object-storage boundary Engine uses for
// Skill archive blobs and runtime resource projection objects. The
// interface is consumed by domain stores and sandbox preparation code, and is
// implemented by an S3-compatible production adapter. A test fake
// (FakeBlobStore in fake.go) exercises create-only writes, copy/head, and
// bounded cleanup without a real bucket.
//
// Behavioral contract:
//
//   - Put is create-only: writing an existing key fails without
//     overwriting existing bytes. S3-compatible adapters express this
//     through conditional object creation (If-None-Match: *). Tests
//     prove the same shape against the fake.
//   - Delete removes exactly the requested key. It is used by Skill
//     store request-cleanup paths after a failed SQL transaction or
//     commit so a successful blob upload does not leave an orphan.
//   - Get returns exact stored bytes for internal consumers; public APIs decide
//     separately whether those bytes are exposed.
//   - CopyObject and HeadObject support sandbox resource projection:
//     HeadObject checks for an already-materialized copy, while
//     CopyObject performs the create-only provider-side copy.
//   - DeletePrefix exists for bounded GC/resource-cleanup pipelines and MUST
//     NOT be used by public Skill delete paths, which are soft deletes.
//
// All methods take a context and return errors that map to typed
// values defined here so the HTTP error layer can translate them
// without leaking provider internals.
package blob

import (
	"context"
	"io"
)

// BlobStore is the consumer-side object-storage interface. Production
// wires an S3-compatible adapter; tests substitute FakeBlobStore.
//
// Engine-side guarantees:
//   - Put receives the exact uploaded archive bytes. Implementations
//     MUST honor create-only semantics (DuplicateKeyError on conflict)
//     and MUST NOT overwrite existing bytes.
//   - Get/HeadObject/Delete operate on a single key.
//   - CopyObject copies sourceKey to destinationKey with create-only
//     destination semantics.
//   - DeletePrefix exists only for bounded GC/resource cleanup; public delete
//     paths are soft deletes and do not call it.
type BlobStore interface {
	Put(ctx context.Context, key string, content io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	HeadObject(ctx context.Context, key string) (ObjectMetadata, error)
	CopyObject(ctx context.Context, sourceKey string, destinationKey string) error
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
}

// ObjectMetadata is the comparable provider metadata returned by
// HeadObject. Resource projection uses it to distinguish an already
// materialized matching copy from a stale/mismatched session object.
type ObjectMetadata struct {
	ETag      string
	SizeBytes int64
}

// DuplicateKeyError indicates a Put attempted to write an existing
// key. Adapters surface this typed error so callers can distinguish a
// race-induced collision from generic transport failure.
type DuplicateKeyError struct {
	Key string
}

func (e *DuplicateKeyError) Error() string {
	return "blob: object already exists"
}

// NotFoundError indicates a Get/Delete addressed a key that does not
// exist. Public-safe text; key contents are not embedded so caller
// log paths do not leak server-constructed identifiers into untrusted
// surfaces.
type NotFoundError struct {
	Key string
}

func (e *NotFoundError) Error() string {
	return "blob: object not found"
}

// ConfigError indicates that the BlobStore implementation cannot be
// constructed because its required configuration is missing or
// invalid. Public Error() text names the missing logical field but
// never echoes secret values; the implementation package owns the
// exact wording.
type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return e.Message
}
