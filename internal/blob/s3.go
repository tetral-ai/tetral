package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3API is the narrow subset of *s3.Client behavior the BlobStore
// implementation depends on. Defined at the consumer (this package)
// so tests can substitute an httptest-driven fake without dragging
// in the full SDK surface (engine/CLAUDE.md "interfaces at consumer").
type S3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3BlobStore implements BlobStore against any S3-compatible HTTPS
// endpoint (AWS S3, MinIO, R2, B2). Conditional create-only Put is
// expressed through `If-None-Match: *` (PutObjectInput.IfNoneMatch),
// which AWS S3 and S3-compatible providers respond to with a
// PreconditionFailed / 412 / Conflict on existing keys. The error
// mapping in mapPutErr translates each provider failure shape into
// the typed *DuplicateKeyError so consumers receive a stable surface
// regardless of provider. Other provider failures are mapped to fixed
// blob-layer text before they cross the adapter boundary.
//
// Imports of the AWS SDK live exclusively in this file (and the small
// constructor); a dependency-confinement test in s3_test.go pins that
// rule so no other Engine package imports the SDK directly.
type S3BlobStore struct {
	client S3API
	bucket string
}

// NewS3BlobStore constructs the production S3-backed store from a
// validated Config. Endpoint, region, and static credentials map to
// the SDK's standard option chain. Path-style addressing is enabled
// for non-AWS hosts so MinIO / R2 / B2 work identically to AWS S3.
func NewS3BlobStore(ctx context.Context, cfg *Config) (*S3BlobStore, error) {
	if cfg == nil {
		return nil, &ConfigError{Message: "blob: configuration is nil"}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)),
	)
	if err != nil {
		return nil, &ConfigError{Message: "blob: AWS SDK configuration failed"}
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			// Path-style is required by most S3-compatible providers
			// when a custom endpoint is supplied; AWS S3 itself accepts
			// either style.
			o.UsePathStyle = true
		}
	})
	return &S3BlobStore{client: client, bucket: cfg.Bucket}, nil
}

// ListPrefix lists object keys beneath prefix. It is intentionally read-only;
// callers that need teardown use DeletePrefix so the delete path keeps its
// separate provider-error mapping.
func (s *S3BlobStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, mapProviderErr("list prefix", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

// NewS3BlobStoreFromAPI constructs an S3BlobStore from an explicit
// S3API. Used by tests to inject an httptest-driven fake; production
// callers use NewS3BlobStore.
func NewS3BlobStoreFromAPI(api S3API, bucket string) *S3BlobStore {
	return &S3BlobStore{client: api, bucket: bucket}
}

// Put uploads content at key with create-only semantics. The
// `If-None-Match: *` header rejects writes when the key already
// exists; mapPutErr translates the provider failure into a typed
// *DuplicateKeyError so consumers can react without reaching into SDK
// internals. Non-create-only provider failures return fixed blob-layer
// text so provider response bodies, endpoints, and credential-shaped
// values do not escape through ordinary error strings.
func (s *S3BlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          content,
		ContentLength: aws.Int64(size),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		return mapPutErr(err)
	}
	return nil
}

// Get returns a streaming reader for the object at key.
func (s *S3BlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, mapGetErr(err, key)
	}
	return out.Body, nil
}

// GetRange returns exactly the requested inclusive object range when it exists.
// Consumers use this optional concrete capability for bounded chunk transports;
// it is deliberately not part of BlobStore's general-purpose interface.
func (s *S3BlobStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length <= 0 {
		return nil, &ConfigError{Message: "blob: range must be positive"}
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		return nil, mapGetErr(err, key)
	}
	return out.Body, nil
}

// HeadObject checks whether key exists without downloading object bytes.
func (s *S3BlobStore) HeadObject(ctx context.Context, key string) (ObjectMetadata, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ObjectMetadata{}, mapHeadErr(err, key)
	}
	return ObjectMetadata{
		ETag:      normalizeProviderETag(aws.ToString(out.ETag)),
		SizeBytes: aws.ToInt64(out.ContentLength),
	}, nil
}

// CopyObject copies an existing object to a new key using provider-side
// S3-compatible copy semantics. The destination is create-only so resource
// materialization cannot silently overwrite another projected file.
func (s *S3BlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		CopySource:        aws.String(copySourceForObject(s.bucket, sourceKey)),
		Key:               aws.String(destinationKey),
		IfNoneMatch:       aws.String("*"),
		MetadataDirective: types.MetadataDirectiveCopy,
	})
	if err != nil {
		return mapCopyErr(err, sourceKey)
	}
	return nil
}

func copySourceForObject(bucket string, key string) string {
	segments := strings.Split(key, "/")
	for i := range segments {
		segments[i] = url.PathEscape(segments[i])
	}
	return url.PathEscape(bucket) + "/" + strings.Join(segments, "/")
}

func normalizeProviderETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// Delete removes the object at key.
func (s *S3BlobStore) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return mapDeleteErr(err, key)
	}
	return nil
}

// DeletePrefix lists and deletes every object whose key begins with
// prefix. Public Skill delete is soft and never invokes this; the
// method exists for the future GC seam.
func (s *S3BlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return mapProviderErr("delete prefix", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    obj.Key,
			}); err != nil {
				return mapDeleteErr(err, *obj.Key)
			}
		}
	}
	return nil
}

// mapPutErr converts a PutObject error into a typed value. The
// create-only failure mode (`If-None-Match: *` collision) surfaces as
// either an HTTP 412 PreconditionFailed or a provider-specific
// PreconditionFailed/AlreadyExists/Conflict error code; we accept any
// of those shapes and translate to *DuplicateKeyError. Other failures
// return a fixed provider error so provider internals do not cross the
// BlobStore boundary.
func mapPutErr(err error) error {
	if err == nil {
		return nil
	}
	if isPreconditionFailed(err) {
		return &DuplicateKeyError{}
	}
	return mapProviderErr("put", err)
}

// mapCopyErr converts CopyObject failures. A missing copy source is
// the resource-projection canonical_missing path, while destination
// create-only conflicts remain DuplicateKeyError.
func mapCopyErr(err error, sourceKey string) error {
	if err == nil {
		return nil
	}
	if isPreconditionFailed(err) {
		return &DuplicateKeyError{}
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return &NotFoundError{Key: sourceKey}
	}
	if isAPIErrorCode(err, "NoSuchKey") || isHTTPStatus(err, http.StatusNotFound) {
		return &NotFoundError{Key: sourceKey}
	}
	return mapProviderErr("copy", err)
}

// mapGetErr converts a GetObject error. NoSuchKey / 404 surface as
// *NotFoundError; everything else is mapped to fixed provider text.
func mapGetErr(err error, key string) error {
	if err == nil {
		return nil
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return &NotFoundError{Key: key}
	}
	if isHTTPStatus(err, http.StatusNotFound) {
		return &NotFoundError{Key: key}
	}
	return mapProviderErr("get", err)
}

// mapHeadErr converts a HeadObject error for existence checks.
func mapHeadErr(err error, key string) error {
	if err == nil {
		return nil
	}
	if isHTTPStatus(err, http.StatusNotFound) {
		return &NotFoundError{Key: key}
	}
	return mapProviderErr("head", err)
}

// mapDeleteErr converts a DeleteObject error. AWS S3 returns success
// for missing keys (DeleteObject is idempotent), so 404-equivalents
// rarely surface here; we still translate them for parity with the
// fake.
func mapDeleteErr(err error, key string) error {
	if err == nil {
		return nil
	}
	if isHTTPStatus(err, http.StatusNotFound) {
		return &NotFoundError{Key: key}
	}
	return mapProviderErr("delete", err)
}

func mapProviderErr(operation string, err error) error {
	if err == nil {
		return nil
	}
	if isContextErr(err) {
		return err
	}
	return newProviderError(operation)
}

type providerError struct {
	operation string
}

func newProviderError(operation string) error {
	return &providerError{operation: operation}
}

func (e *providerError) Error() string {
	if e.operation == "" {
		return "blob: provider operation failed"
	}
	return "blob: " + e.operation + " failed"
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// isPreconditionFailed reports whether err represents an
// `If-None-Match: *` collision response. AWS S3 returns the API error
// code "PreconditionFailed" with HTTP 412; some S3-compatible
// providers return 409 Conflict or "AlreadyExists" / "ObjectExists".
// Match the broad shape so the create-only contract holds across
// providers.
func isPreconditionFailed(err error) bool {
	if isHTTPStatus(err, http.StatusPreconditionFailed) ||
		isHTTPStatus(err, http.StatusConflict) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "AlreadyExists", "ObjectExists", "Conflict":
			return true
		}
	}
	return false
}

func isAPIErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == code
	}
	return false
}

// isHTTPStatus returns true when err carries the supplied HTTP status
// (smithy wraps S3 HTTP responses in *smithyhttp.ResponseError, which
// in turn embeds *http.Response).
func isHTTPStatus(err error, status int) bool {
	type httpStatusReporter interface {
		HTTPStatusCode() int
	}
	var hr httpStatusReporter
	if errors.As(err, &hr) {
		return hr.HTTPStatusCode() == status
	}
	return false
}
