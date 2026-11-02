package tetralcleanup

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDriveCleanupRetainsTimedOutKeyAndContinues(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful retry", true: "already missing retry"}[missing], func(t *testing.T) {
			f := newDriveFixture(t)
			var retry atomic.Bool
			var mu sync.Mutex
			var keys []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimPrefix(r.URL.Path, "/bucket/")
				mu.Lock()
				keys = append(keys, key)
				mu.Unlock()
				if key == "a-stalled" && !retry.Load() {
					<-r.Context().Done()
					return
				}
				if key == "a-stalled" && missing {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			store, err := blob.NewS3BlobStore(context.Background(), &blob.Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket", AccessKey: "fixture", SecretKey: "fixture", AllowInsecure: true, LocalTestMode: true, DeleteTimeout: 50 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			f.exec(`INSERT INTO drive_blob_cleanup(workspace_id,blob_key,delete_after) VALUES ('default','a-stalled',now()),('default','b-ready',now())`)
			cleaner := drive.NewCleaner(f.runner, store)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := cleaner.DeleteDueBlobs(ctx, workspace.DefaultID); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cleanup error: %v", err)
			}
			if f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key='a-stalled'`) != 1 || f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key='b-ready'`) != 0 {
				t.Fatal("timeout lost custody or prevented next key")
			}
			mu.Lock()
			first := append([]string(nil), keys...)
			mu.Unlock()
			if strings.Join(first, ",") != "a-stalled,b-ready" {
				t.Fatalf("attempts: %v", first)
			}
			retry.Store(true)
			if err := cleaner.DeleteDueBlobs(ctx, workspace.DefaultID); err != nil {
				t.Fatal(err)
			}
			if f.count(`SELECT count(*) FROM drive_blob_cleanup`) != 0 {
				t.Fatal("retry did not release pending key")
			}
		})
	}
}
