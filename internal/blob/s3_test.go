package blob_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/build"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/tetral-ai/tetral/internal/blob"
)

var _ blob.BlobStore = (*blob.S3BlobStore)(nil)

// requestRecorder is a thread-safe log of HTTP requests an
// httptest-driven S3 server received. Tests assert on captured
// method/path/headers/body.
type requestRecorder struct {
	mu       sync.Mutex
	requests []capturedRequest
}

type capturedRequest struct {
	method  string
	path    string
	headers http.Header
	body    []byte
}

type stubS3API struct {
	putObject     func(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	getObject     func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	headObject    func(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	copyObject    func(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	deleteObject  func(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	listObjectsV2 func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func (s stubS3API) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if s.putObject == nil {
		panic("unexpected PutObject")
	}
	return s.putObject(ctx, in, opts...)
}

func (s stubS3API) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if s.getObject == nil {
		panic("unexpected GetObject")
	}
	return s.getObject(ctx, in, opts...)
}

func (s stubS3API) HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if s.headObject == nil {
		panic("unexpected HeadObject")
	}
	return s.headObject(ctx, in, opts...)
}

func (s stubS3API) CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if s.copyObject == nil {
		panic("unexpected CopyObject")
	}
	return s.copyObject(ctx, in, opts...)
}

func (s stubS3API) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if s.deleteObject == nil {
		panic("unexpected DeleteObject")
	}
	return s.deleteObject(ctx, in, opts...)
}

func (s stubS3API) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if s.listObjectsV2 == nil {
		panic("unexpected ListObjectsV2")
	}
	return s.listObjectsV2(ctx, in, opts...)
}

func (r *requestRecorder) record(req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	_ = req.Body.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, capturedRequest{
		method:  req.Method,
		path:    req.URL.Path,
		headers: req.Header.Clone(),
		body:    body,
	})
}

func (r *requestRecorder) snapshot() []capturedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]capturedRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

// newTestS3Server builds an httptest server that responds with the
// supplied handler and a real *s3.Client pointed at it. Path-style
// addressing is enabled so paths look like /<bucket>/<key>.
func newTestS3Server(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *s3.Client) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(testAccessSentinel, testSecretSentinel, ""),
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		HTTPClient:   server.Client(),
	})
	return server, client
}

func TestS3BlobStorePutSendsIfNoneMatchAndCorrectBytes(t *testing.T) {
	rec := &requestRecorder{}
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusOK)
	})
	_ = server
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	body := []byte("opaque skill archive bytes")
	if err := store.Put(context.Background(), "skills/ws_a/skl_x/v1.tar.gz", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 request; got %d", len(got))
	}
	req := got[0]
	if req.method != http.MethodPut {
		t.Errorf("method = %q; want PUT", req.method)
	}
	if !strings.Contains(req.path, "tetral-bucket") {
		t.Errorf("path %q must include bucket name", req.path)
	}
	if !strings.Contains(req.path, "skills/ws_a/skl_x/v1.tar.gz") {
		t.Errorf("path %q must include the server-constructed key", req.path)
	}
	if got := req.headers.Get("If-None-Match"); got != "*" {
		t.Errorf("If-None-Match header = %q; want \"*\"", got)
	}
	if !bytes.Equal(req.body, body) {
		t.Errorf("body sent = %d bytes; want %d", len(req.body), len(body))
	}
}

func TestS3BlobStorePutBuildsObjectInputWithCallerSuppliedSize(t *testing.T) {
	const (
		bucket       = "tetral-bucket"
		key          = "skills/ws_a/skl_x/v1.tar.gz"
		suppliedSize = int64(8675309)
	)
	body := []byte("archive bytes whose reader length differs from supplied size")
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		putObject: func(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			if got := aws.ToString(in.Bucket); got != bucket {
				t.Errorf("Bucket = %q; want %q", got, bucket)
			}
			if got := aws.ToString(in.Key); got != key {
				t.Errorf("Key = %q; want %q", got, key)
			}
			if got := aws.ToString(in.IfNoneMatch); got != "*" {
				t.Errorf("IfNoneMatch = %q; want \"*\"", got)
			}
			if in.ContentLength == nil {
				t.Fatal("ContentLength is nil; want caller-supplied size")
			}
			if got := aws.ToInt64(in.ContentLength); got != suppliedSize {
				t.Errorf("ContentLength = %d; want caller-supplied size %d", got, suppliedSize)
			}
			gotBody, err := io.ReadAll(in.Body)
			if err != nil {
				t.Fatalf("reading Body: %v", err)
			}
			if !bytes.Equal(gotBody, body) {
				t.Errorf("Body bytes = %q; want %q", gotBody, body)
			}
			return &s3.PutObjectOutput{}, nil
		},
	}, bucket)

	if err := store.Put(context.Background(), key, bytes.NewReader(body), suppliedSize); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if int64(len(body)) == suppliedSize {
		t.Fatalf("test setup invalid: body length equals supplied size")
	}
}

func TestS3BlobStoreCompareAndSwapSendsExactIfMatch(t *testing.T) {
	const (
		bucket = "tetral-bucket"
		key    = "default/session/thread/jobs/event.job"
		etag   = "claim-etag"
	)
	body := []byte("completed job")
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		putObject: func(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			if got := aws.ToString(in.Bucket); got != bucket {
				t.Errorf("Bucket = %q; want %q", got, bucket)
			}
			if got := aws.ToString(in.Key); got != key {
				t.Errorf("Key = %q; want %q", got, key)
			}
			if got := aws.ToString(in.IfMatch); got != `"claim-etag"` {
				t.Errorf("IfMatch = %q; want quoted ETag", got)
			}
			if in.IfNoneMatch != nil {
				t.Errorf("IfNoneMatch = %q; want unset", aws.ToString(in.IfNoneMatch))
			}
			return &s3.PutObjectOutput{}, nil
		},
	}, bucket)
	if err := store.CompareAndSwap(context.Background(), key, etag, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
}

func TestS3BlobStoreCopyObjectBuildsCreateOnlyProviderCopyInput(t *testing.T) {
	const (
		bucket         = "tetral-bucket"
		sourceKey      = "files/default/a b/\u00e7.txt"
		destinationKey = "/sandbox_test/workspace/data.csv"
	)
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		copyObject: func(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			if got := aws.ToString(in.Bucket); got != bucket {
				t.Errorf("Bucket = %q; want %q", got, bucket)
			}
			if got := aws.ToString(in.Key); got != destinationKey {
				t.Errorf("Key = %q; want %q", got, destinationKey)
			}
			if got, want := aws.ToString(in.CopySource), "tetral-bucket/files/default/a%20b/%C3%A7.txt"; got != want {
				t.Errorf("CopySource = %q; want %q", got, want)
			}
			if got := aws.ToString(in.IfNoneMatch); got != "*" {
				t.Errorf("IfNoneMatch = %q; want \"*\"", got)
			}
			if in.MetadataDirective != types.MetadataDirectiveCopy {
				t.Errorf("MetadataDirective = %q; want COPY", in.MetadataDirective)
			}
			return &s3.CopyObjectOutput{}, nil
		},
	}, bucket)

	if err := store.CopyObject(context.Background(), sourceKey, destinationKey); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
}

func TestS3BlobStoreCopyObjectMissingSourceMapsToNotFound(t *testing.T) {
	_, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s; want PUT", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	err := store.CopyObject(context.Background(), "files/default/missing-object", "workspaces/default/sessions/sesn/resources/sesrsc/file")
	if err == nil {
		t.Fatal("expected NotFoundError for missing copy source")
	}
	if !errors.As(err, new(*blob.NotFoundError)) {
		t.Fatalf("expected *blob.NotFoundError; got %T (%v)", err, err)
	}
}

func TestS3BlobStoreCopyObjectDuplicateMapsPreconditionToTypedError(t *testing.T) {
	_, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	err := store.CopyObject(context.Background(), "files/default/object", "workspaces/default/sessions/sesn/resources/sesrsc/file")
	if err == nil {
		t.Fatal("expected DuplicateKeyError for destination collision")
	}
	if !errors.As(err, new(*blob.DuplicateKeyError)) {
		t.Fatalf("expected *blob.DuplicateKeyError; got %T (%v)", err, err)
	}
}

func TestS3BlobStoreCopyObjectNonTypedFailureDoesNotLeakProviderText(t *testing.T) {
	const (
		bucket           = "tetral-bucket"
		providerSentinel = "provider-copy-body-do-not-leak"
	)
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`<Error><Code>BadGateway</Code><Message>` + providerSentinel + ` ` + bucket + ` ` + testAccessSentinel + ` ` + testSecretSentinel + `</Message></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, bucket)

	err := store.CopyObject(context.Background(), "files/default/object", "workspaces/default/sessions/sesn/resources/sesrsc/file")
	if err == nil {
		t.Fatal("expected copy failure")
	}
	assertErrorExcludes(t, err, providerSentinel, server.URL, bucket, testAccessSentinel, testSecretSentinel)
}

func TestS3BlobStoreHeadObjectBuildsProviderInputWithoutBody(t *testing.T) {
	const (
		bucket = "tetral-bucket"
		key    = "sandbox_test/resources/object.dat"
	)
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		headObject: func(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			if got := aws.ToString(in.Bucket); got != bucket {
				t.Errorf("Bucket = %q; want %q", got, bucket)
			}
			if got := aws.ToString(in.Key); got != key {
				t.Errorf("Key = %q; want %q", got, key)
			}
			return &s3.HeadObjectOutput{ETag: aws.String(`"etag-value"`), ContentLength: aws.Int64(12)}, nil
		},
	}, bucket)

	meta, err := store.HeadObject(context.Background(), key)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if meta.ETag != "etag-value" || meta.SizeBytes != 12 {
		t.Fatalf("HeadObject metadata = %+v; want normalized etag and size", meta)
	}
}

func TestS3BlobStoreGetRangeBuildsExactInclusiveProviderRange(t *testing.T) {
	const (
		bucket = "tetral-bucket"
		key    = "files/default/fobj_media"
	)
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		getObject: func(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if got := aws.ToString(in.Bucket); got != bucket {
				t.Errorf("Bucket = %q; want %q", got, bucket)
			}
			if got := aws.ToString(in.Key); got != key {
				t.Errorf("Key = %q; want %q", got, key)
			}
			if got := aws.ToString(in.Range); got != "bytes=7-17" {
				t.Errorf("Range = %q; want bytes=7-17", got)
			}
			return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("eleven bytes"))}, nil
		},
	}, bucket)

	reader, err := store.GetRange(context.Background(), key, 7, 11)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if body, err := io.ReadAll(reader); err != nil || string(body) != "eleven bytes" {
		t.Fatalf("GetRange body = %q, %v; want eleven bytes", string(body), err)
	}
}

func TestS3BlobStoreHeadObjectMissingMapsToNotFound(t *testing.T) {
	_, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s; want HEAD", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
	})
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	_, err := store.HeadObject(context.Background(), "sandbox_test/resources/missing.dat")
	if err == nil {
		t.Fatal("expected NotFoundError for missing key")
	}
	if !errors.As(err, new(*blob.NotFoundError)) {
		t.Fatalf("expected *blob.NotFoundError; got %T (%v)", err, err)
	}
}

func TestS3BlobStorePutDuplicateMaps412ToTypedError(t *testing.T) {
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		// Real S3 sends 412 PreconditionFailed on If-None-Match collision.
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>PreconditionFailed</Code><Message>conditional create failed</Message></Error>`))
	})
	_ = server
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatal("expected error on 412 PreconditionFailed")
	}
	if !errors.As(err, new(*blob.DuplicateKeyError)) {
		t.Fatalf("expected *blob.DuplicateKeyError; got %T (%v)", err, err)
	}
}

func TestS3BlobStorePutDuplicateMaps409ConflictToTypedError(t *testing.T) {
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		// Some S3-compatible providers send 409 Conflict.
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>Conflict</Code><Message>already exists</Message></Error>`))
	})
	_ = server
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatal("expected error on 409 Conflict")
	}
	if !errors.As(err, new(*blob.DuplicateKeyError)) {
		t.Fatalf("expected *blob.DuplicateKeyError; got %T (%v)", err, err)
	}
}

func TestS3BlobStorePutNonPreconditionFailureDoesNotLeakProviderText(t *testing.T) {
	const bucket = "tetral-bucket"
	const providerSentinel = "provider-put-body-do-not-leak"
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`<Error><Code>BadGateway</Code><Message>` + providerSentinel + ` ` + bucket + ` ` + testAccessSentinel + ` ` + testSecretSentinel + `</Message></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, bucket)

	err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatal("expected put failure")
	}
	assertErrorExcludes(t, err, providerSentinel, server.URL, bucket, testAccessSentinel, testSecretSentinel)
}

func TestS3BlobStoreDeleteSendsDeleteForExactKey(t *testing.T) {
	rec := &requestRecorder{}
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusNoContent)
	})
	_ = server
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	if err := store.Delete(context.Background(), "skills/ws/skl/v1.tar.gz"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 request; got %d", len(got))
	}
	if got[0].method != http.MethodDelete {
		t.Errorf("method = %q; want DELETE", got[0].method)
	}
	if got[0].path != "/tetral-bucket/skills/ws/skl/v1.tar.gz" {
		t.Errorf("path = %q; want exact bucket/key path", got[0].path)
	}
}

func TestS3BlobStoreDeleteBuildsObjectInputWithExactKey(t *testing.T) {
	const (
		bucket = "tetral-bucket"
		key    = "skills/ws/skl/v1.tar.gz"
	)
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		deleteObject: func(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			if got := aws.ToString(in.Bucket); got != bucket {
				t.Errorf("Bucket = %q; want %q", got, bucket)
			}
			if got := aws.ToString(in.Key); got != key {
				t.Errorf("Key = %q; want exact cleanup key %q", got, key)
			}
			return &s3.DeleteObjectOutput{}, nil
		},
	}, bucket)

	if err := store.Delete(context.Background(), key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestS3BlobStoreDeleteNon404FailureDoesNotLeakProviderText(t *testing.T) {
	const bucket = "tetral-bucket"
	const providerSentinel = "provider-delete-body-do-not-leak"
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>` + providerSentinel + ` ` + testAccessSentinel + ` ` + testSecretSentinel + `</Message></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, bucket)

	err := store.Delete(context.Background(), "skills/ws/skl/v1.tar.gz")
	if err == nil {
		t.Fatal("expected delete failure")
	}
	assertErrorExcludes(t, err, providerSentinel, server.URL, bucket, testAccessSentinel, testSecretSentinel)
}

func TestS3BlobStoreDeleteMissingMapsToNotFound(t *testing.T) {
	_, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")

	err := store.Delete(context.Background(), "skills/ws/missing/v1.tar.gz")
	if err == nil {
		t.Fatal("expected NotFoundError for missing key")
	}
	if !errors.As(err, new(*blob.NotFoundError)) {
		t.Fatalf("expected *blob.NotFoundError; got %T (%v)", err, err)
	}
}

func TestS3BlobStoreDeleteContextErrorIsPreserved(t *testing.T) {
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		deleteObject: func(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return nil, context.Canceled
		},
	}, "tetral-bucket")

	err := store.Delete(context.Background(), "skills/ws/skl/v1.tar.gz")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete error = %T (%v); want context.Canceled", err, err)
	}
}

func TestS3BlobStoreDeletePrefixListFailureDoesNotLeakProviderText(t *testing.T) {
	const bucket = "tetral-bucket"
	const providerSentinel = "provider-list-body-do-not-leak"
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`<Error><Code>BadGateway</Code><Message>` + providerSentinel + ` ` + bucket + ` ` + testSecretSentinel + `</Message></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, bucket)

	err := store.DeletePrefix(context.Background(), "skills/ws/skl/")
	if err == nil {
		t.Fatal("expected list failure")
	}
	assertErrorExcludes(t, err, providerSentinel, server.URL, bucket, testAccessSentinel, testSecretSentinel)
}

func TestS3BlobStoreDeletePrefixContextErrorIsPreserved(t *testing.T) {
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		listObjectsV2: func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, context.DeadlineExceeded
		},
	}, "tetral-bucket")

	err := store.DeletePrefix(context.Background(), "skills/ws/skl/")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DeletePrefix error = %T (%v); want context.DeadlineExceeded", err, err)
	}
}

func TestS3BlobStoreListPrefixReturnsKeysWithoutDeleting(t *testing.T) {
	store := blob.NewS3BlobStoreFromAPI(stubS3API{
		listObjectsV2: func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			if aws.ToString(in.Bucket) != "tetral-bucket" || aws.ToString(in.Prefix) != "workspaces/ws/sessions/sesn/resources/" {
				t.Fatalf("ListObjectsV2 input = bucket=%q prefix=%q", aws.ToString(in.Bucket), aws.ToString(in.Prefix))
			}
			return &s3.ListObjectsV2Output{Contents: []types.Object{
				{Key: aws.String("workspaces/ws/sessions/sesn/resources/a/file")},
				{Key: aws.String("workspaces/ws/sessions/sesn/resources/b/file")},
			}}, nil
		},
	}, "tetral-bucket")

	keys, err := store.ListPrefix(context.Background(), "workspaces/ws/sessions/sesn/resources/")
	if err != nil {
		t.Fatalf("ListPrefix: %v", err)
	}
	if got := strings.Join(keys, ","); got != "workspaces/ws/sessions/sesn/resources/a/file,workspaces/ws/sessions/sesn/resources/b/file" {
		t.Fatalf("keys = %q; want listed keys", got)
	}
}

func TestS3BlobStoreDeletePrefixDeleteFailureDoesNotLeakProviderText(t *testing.T) {
	const bucket = "tetral-bucket"
	const providerSentinel = "provider-prefix-delete-body-do-not-leak"
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>` + bucket + `</Name><Prefix>skills/ws/skl/</Prefix><IsTruncated>false</IsTruncated><Contents><Key>skills/ws/skl/v1.tar.gz</Key></Contents></ListBucketResult>`))
		case http.MethodDelete:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>` + providerSentinel + ` ` + testAccessSentinel + ` ` + testSecretSentinel + `</Message></Error>`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	store := blob.NewS3BlobStoreFromAPI(client, bucket)

	err := store.DeletePrefix(context.Background(), "skills/ws/skl/")
	if err == nil {
		t.Fatal("expected prefix delete failure")
	}
	assertErrorExcludes(t, err, providerSentinel, server.URL, bucket, testAccessSentinel, testSecretSentinel)
}

func TestS3BlobStoreMinIOCopyDeleteAndPrefixIsolation(t *testing.T) {
	ctx := context.Background()
	endpoint := os.Getenv("TETRAL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("TETRAL_TEST_MINIO_ENDPOINT is required for MinIO-backed S3 compatibility smoke")
	}
	accessKey := valueOrDefault(os.Getenv("TETRAL_TEST_MINIO_ACCESS_KEY"), "tetralminio")
	secretKey := valueOrDefault(os.Getenv("TETRAL_TEST_MINIO_SECRET_KEY"), "tetralminio123")
	region := valueOrDefault(os.Getenv("TETRAL_TEST_MINIO_REGION"), "us-east-1")
	bucket := fmt.Sprintf("tetral-minio-%d", time.Now().UnixNano())
	admin := s3.New(s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
	})
	if _, err := admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() { cleanupMinIOBucket(context.Background(), t, admin, bucket) })
	store, err := blob.NewS3BlobStore(ctx, &blob.Config{
		Endpoint:      endpoint,
		Region:        region,
		Bucket:        bucket,
		AccessKey:     accessKey,
		SecretKey:     secretKey,
		AllowInsecure: strings.HasPrefix(endpoint, "http://"),
		LocalTestMode: true,
	})
	if err != nil {
		t.Fatalf("NewS3BlobStore: %v", err)
	}

	sourceKey := "files/ws_minio/object_a"
	copiedKey := "workspaces/ws_minio/sessions/sesn_a/resources/sesrsc_a/file"
	siblingKey := "workspaces/ws_minio/sessions/sesn_b/resources/sesrsc_b/file"
	exactDeleteKey := "workspaces/ws_minio/sessions/sesn_a/resources/delete-exact/file"
	for key, body := range map[string]string{
		sourceKey:                 "canonical bytes",
		siblingKey:                "sibling bytes",
		exactDeleteKey:            "delete me",
		"files/ws_minio/object_b": "other canonical",
	} {
		if err := store.Put(ctx, key, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	if err := store.CopyObject(ctx, sourceKey, copiedKey); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	body, err := store.Get(ctx, copiedKey)
	if err != nil {
		t.Fatalf("Get copied object: %v", err)
	}
	copiedBytes, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		t.Fatalf("Read copied object: %v", err)
	}
	if string(copiedBytes) != "canonical bytes" {
		t.Fatalf("copied object = %q; want canonical bytes", copiedBytes)
	}

	if err := store.Delete(ctx, exactDeleteKey); err != nil {
		t.Fatalf("Delete exact key: %v", err)
	}
	if _, err := store.HeadObject(ctx, exactDeleteKey); !errors.As(err, new(*blob.NotFoundError)) {
		t.Fatalf("HeadObject deleted exact key err = %T (%v); want NotFoundError", err, err)
	}
	if err := store.DeletePrefix(ctx, "workspaces/ws_minio/sessions/sesn_a/resources/"); err != nil {
		t.Fatalf("DeletePrefix session A: %v", err)
	}
	if _, err := store.HeadObject(ctx, copiedKey); !errors.As(err, new(*blob.NotFoundError)) {
		t.Fatalf("HeadObject deleted prefix key err = %T (%v); want NotFoundError", err, err)
	}
	if got, err := store.Get(ctx, siblingKey); err != nil {
		t.Fatalf("sibling prefix was deleted: %v", err)
	} else {
		_ = got.Close()
	}
}

func TestMinIOAcceptsAndReturnsSevenDayBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	endpoint := os.Getenv("TETRAL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("TETRAL_TEST_MINIO_ENDPOINT is required for MinIO lifecycle compatibility")
	}
	accessKey := valueOrDefault(os.Getenv("TETRAL_TEST_MINIO_ACCESS_KEY"), "tetralminio")
	secretKey := valueOrDefault(os.Getenv("TETRAL_TEST_MINIO_SECRET_KEY"), "tetralminio123")
	region := valueOrDefault(os.Getenv("TETRAL_TEST_MINIO_REGION"), "us-east-1")
	bucket := fmt.Sprintf("tetral-lifecycle-%d", time.Now().UnixNano())
	admin := s3.New(s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
	})
	if _, err := admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() { cleanupMinIOBucket(context.Background(), t, admin, bucket) })

	const ttlDays int32 = 7
	if _, err := admin.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
			ID:         aws.String("expire-web-cache"),
			Status:     types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(ttlDays)},
		}}},
	}); err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration: %v", err)
	}
	got, err := admin.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketLifecycleConfiguration: %v", err)
	}
	if len(got.Rules) != 1 || aws.ToString(got.Rules[0].ID) != "expire-web-cache" || got.Rules[0].Status != types.ExpirationStatusEnabled || got.Rules[0].Expiration == nil || aws.ToInt32(got.Rules[0].Expiration.Days) != ttlDays {
		t.Fatalf("lifecycle rules = %#v; want one enabled seven-day expiration", got.Rules)
	}
}

func TestS3BlobStoreGetMissingMapsToNotFound(t *testing.T) {
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code></Error>`))
	})
	_ = server
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")
	_, err := store.Get(context.Background(), "skills/ws/missing/v1.tar.gz")
	if err == nil {
		t.Fatal("expected NotFoundError for missing key")
	}
	if !errors.As(err, new(*blob.NotFoundError)) {
		t.Fatalf("expected *blob.NotFoundError; got %T (%v)", err, err)
	}
}

func TestS3BlobStoreGetNon404FailureDoesNotLeakProviderText(t *testing.T) {
	const bucket = "tetral-bucket"
	const providerSentinel = "provider-get-body-do-not-leak"
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>` + providerSentinel + ` ` + bucket + ` ` + testAccessSentinel + ` ` + testSecretSentinel + `</Message></Error>`))
	})
	store := blob.NewS3BlobStoreFromAPI(client, bucket)

	_, err := store.Get(context.Background(), "skills/ws/skl/v1.tar.gz")
	if err == nil {
		t.Fatal("expected get failure")
	}
	assertErrorExcludes(t, err, providerSentinel, server.URL, bucket, testAccessSentinel, testSecretSentinel)
}

func TestS3BlobStorePutErrorDoesNotLeakSecretsInTypedReturn(t *testing.T) {
	// Even when the provider returns a body containing what looks like
	// secret material, the typed *DuplicateKeyError surface contains
	// no provider strings — the message is hard-coded.
	server, client := newTestS3Server(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code><Message>` + testSecretSentinel + `</Message></Error>`))
	})
	_ = server
	store := blob.NewS3BlobStoreFromAPI(client, "tetral-bucket")
	err := store.Put(context.Background(), "skills/ws/skl/v1.tar.gz", bytes.NewReader([]byte("x")), 1)
	var dup *blob.DuplicateKeyError
	if !errors.As(err, &dup) {
		t.Fatalf("expected *blob.DuplicateKeyError; got %T", err)
	}
	if strings.Contains(dup.Error(), testSecretSentinel) {
		t.Errorf("DuplicateKeyError leaked secret-shaped substring: %q", dup.Error())
	}
}

// TestS3SDKImportConfinement pins the contract rule: AWS SDK imports
// must only appear in internal/blob. Other Engine packages must
// continue to consume the BlobStore interface, never the SDK
// directly.
func TestS3SDKImportConfinement(t *testing.T) {
	const allowedDir = "internal/blob"
	const sdkRoot = "github.com/aws/aws-sdk-go-v2"

	root := engineRootDir(t)
	violations := []string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || name == "testdata" || name == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if strings.HasPrefix(filepath.ToSlash(rel), allowedDir+"/") {
			return nil
		}
		// internal/storage's own documentation test names the SDK
		// import path constants to enforce the same confinement rule
		// from the storage side. Skip it from this scan.
		if filepath.ToSlash(rel) == "internal/storage/postgresql_documentation_test.go" {
			return nil
		}
		body, readErr := os.ReadFile(path) //nolint:gosec // engine source path, walked from root
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(body), `"`+sdkRoot) {
			violations = append(violations, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk engine sources: %v", err)
	}
	for _, file := range violations {
		t.Errorf("file %s imports forbidden AWS SDK outside internal/blob", file)
	}
}

// TestBlobPackageImportsAreConfined ensures the blob package's only
// non-stdlib imports are the AWS SDK family and smithy-go.
func TestBlobPackageImportsAreConfined(t *testing.T) {
	pkg, err := build.Default.Import("github.com/tetral-ai/tetral/internal/blob", "", 0)
	if err != nil {
		t.Fatalf("import internal/blob: %v", err)
	}
	allowed := map[string]bool{
		"github.com/aws/aws-sdk-go-v2":                  true,
		"github.com/aws/aws-sdk-go-v2/aws":              true,
		"github.com/aws/aws-sdk-go-v2/config":           true,
		"github.com/aws/aws-sdk-go-v2/credentials":      true,
		"github.com/aws/aws-sdk-go-v2/service/s3":       true,
		"github.com/aws/aws-sdk-go-v2/service/s3/types": true,
		"github.com/aws/smithy-go":                      true,
	}
	for _, dep := range pkg.Imports {
		if isStdlibImport(dep) {
			continue
		}
		if strings.HasPrefix(dep, "github.com/tetral-ai/tetral/") {
			continue
		}
		if !allowed[dep] {
			t.Errorf("internal/blob imports unexpected third-party dep: %s", dep)
		}
	}
}

// engineRootDir returns the engine module root path.
func engineRootDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// thisFile lives at engine/internal/blob/s3_test.go.
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func isStdlibImport(path string) bool {
	first := path
	if i := strings.Index(path, "/"); i >= 0 {
		first = path[:i]
	}
	return !strings.Contains(first, ".")
}

func cleanupMinIOBucket(ctx context.Context, t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup list MinIO bucket %s: %v", bucket, err)
			break
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key}); err != nil {
				t.Logf("cleanup delete MinIO object %s/%s: %v", bucket, aws.ToString(obj.Key), err)
			}
		}
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("cleanup delete MinIO bucket %s: %v", bucket, err)
	}
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func assertErrorExcludes(t *testing.T, err error, forbidden ...string) {
	t.Helper()
	surfaces := []string{
		err.Error(),
		fmt.Sprintf("%v", err),
		fmt.Sprintf("%+v", err),
	}
	for _, value := range forbidden {
		if value == "" {
			continue
		}
		for _, surface := range surfaces {
			if strings.Contains(surface, value) {
				t.Errorf("error surface %q leaked forbidden substring %q", surface, value)
			}
		}
	}
}
