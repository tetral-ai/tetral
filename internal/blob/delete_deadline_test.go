package blob

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDeleteTimeoutConfiguration(t *testing.T) {
	t.Setenv(EnvRegion, "us-east-1")
	t.Setenv(EnvBucket, "bucket")
	t.Setenv(EnvAccessKey, "fixture")
	t.Setenv(EnvSecretKey, "fixture")
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		invalid bool
	}{{"", 30 * time.Second, false}, {"17", 17 * time.Millisecond, false}, {"0", 0, true}, {"-1", 0, true}, {"bad", 0, true}, {"1.5", 0, true}, {"9223372036854775807", 0, true}} {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv(EnvDeleteTimeoutMS, tc.raw)
			cfg, err := LoadConfig()
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid timeout accepted")
				}
				return
			}
			if err != nil || cfg.DeleteTimeout != tc.want {
				t.Fatalf("timeout = %v %v", cfg, err)
			}
		})
	}
	cfg := &Config{Region: "us-east-1", Bucket: "bucket", AccessKey: "fixture", SecretKey: "fixture"}
	store, err := NewS3BlobStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if store.deleteTimeout != 30*time.Second {
		t.Fatalf("omitted programmatic timeout: %s", store.deleteTimeout)
	}
	cfg.DeleteTimeout = -time.Millisecond
	if _, err := NewS3BlobStore(context.Background(), cfg); err == nil {
		t.Fatal("negative programmatic timeout accepted")
	}
}

func TestS3DeleteDeadlineAndCallerCancellation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		callerTimeout time.Duration
		cancelEarly   bool
		want          error
	}{
		{"configured deadline", 0, false, context.DeadlineExceeded},
		{"earlier caller deadline", 20 * time.Millisecond, false, context.DeadlineExceeded},
		{"caller cancellation", 0, true, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			finished := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				defer close(finished)
				<-r.Context().Done()
			}))
			defer server.Close()
			store, err := NewS3BlobStore(context.Background(), &Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket", AccessKey: "fixture", SecretKey: "fixture", AllowInsecure: true, LocalTestMode: true, DeleteTimeout: 100 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.callerTimeout > 0 {
				var c context.CancelFunc
				ctx, c = context.WithTimeout(ctx, tc.callerTimeout)
				defer c()
			}
			done := make(chan error, 1)
			go func() { done <- store.Delete(ctx, "key") }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("DELETE did not reach server")
			}
			if tc.cancelEarly {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, tc.want) {
					t.Fatalf("Delete: %v, want %v", err, tc.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("DELETE exceeded bound")
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("server request did not end")
			}
		})
	}
}

type deadlineHTTPClient struct {
	mu        sync.Mutex
	deadlines []time.Time
	client    *http.Client
}

func (c *deadlineHTTPClient) Do(r *http.Request) (*http.Response, error) {
	d, ok := r.Context().Deadline()
	if !ok {
		return nil, errors.New("DELETE has no deadline")
	}
	c.mu.Lock()
	c.deadlines = append(c.deadlines, d)
	c.mu.Unlock()
	return c.client.Do(r)
}

func TestS3DeleteRetriesShareOneDeadline(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `<Error><Code>ServiceUnavailable</Code></Error>`)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	client := &deadlineHTTPClient{client: server.Client()}
	sdk := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("fixture", "fixture", ""), HTTPClient: client, Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
		o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
	})})
	store := &S3BlobStore{client: sdk, bucket: "bucket", deleteTimeout: 200 * time.Millisecond}
	if err := store.Delete(context.Background(), "key"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Delete: %v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.deadlines) < 2 {
		t.Fatalf("retry was not exercised: %d attempts", len(client.deadlines))
	}
	for _, d := range client.deadlines[1:] {
		if !d.Equal(client.deadlines[0]) {
			t.Fatal("retry reset total DELETE deadline")
		}
	}
}

func TestS3DeleteDeadlineDoesNotBoundPutOrGet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		time.Sleep(20 * time.Millisecond)
		switch r.Method {
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = io.WriteString(w, "contents")
		}
	}))
	defer server.Close()
	store, err := NewS3BlobStore(context.Background(), &Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket", AccessKey: "fixture", SecretKey: "fixture", AllowInsecure: true, LocalTestMode: true, DeleteTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), "key", strings.NewReader("contents"), 8); err != nil {
		t.Fatal(err)
	}
	body, err := store.Get(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "contents" {
		t.Fatalf("Get: %q %v", data, err)
	}
	// Use a comfortable configured bound for the ordinary successful delete.
	store.deleteTimeout = time.Second
	if err := store.Delete(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
}
