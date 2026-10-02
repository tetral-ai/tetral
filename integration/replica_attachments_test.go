package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/blob/blobtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// These clients intentionally use explicit replica addresses. Distribution by
// the deployment transport is covered separately by the Envoy compositions.
func TestPostgreSQLReplicaAttachmentReads(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	newBlob := replicaMinIOStores(t)
	firstBlob, secondBlob := newBlob(), newBlob()
	const sessionID, threadID, bindingID, podUID = "sesn_replica_attachment", "sthr_replica_attachment", "bind_replica_attachment", "pod_replica_attachment"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	const chunk = 8 * 1024 * 1024
	bytes := make([]byte, chunk+17)
	for i := range bytes {
		bytes[i] = byte((i*31 + 7) % 251)
	}
	digest := sha256.Sum256(bytes)
	seedBridgeAPIFileAttachment(t, admin, firstBlob, "file_replica_attachment", "replica.png", "image/png", string(bytes))
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_replica_attachment", 1, "user.message", `{"content":[{"type":"image","source":{"type":"file","file_id":"file_replica_attachment"}}]}`)

	transientStore := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	transientStore.AttachmentBlobStore = firstBlob
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results(workspace_id,session_id,session_thread_id,tool_use_event_id,tool_kind,normalized_input_hash,tool_name,input_json,ack_status,result_json,model_tool_call_id,execution_state,execution_attempt_generation,result_digest,created_at,updated_at) VALUES('default',$1,$2,'sevt_replica_transient','sandbox_tool','fixture-hash','view_image','{}','committed','{"status":"success"}','call_replica_transient','terminal_unconsumed',1,'fixture-digest',now(),now())`, sessionID, threadID); err != nil {
		t.Fatal(err)
	}
	createBridgeTransientAttachmentForTest(t, admin, transientStore, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), "replica_transient", "sevt_replica_transient", []byte("independent-transient-bytes"))
	for _, variant := range []string{"transient", "assembled", "scope", "deleted", "malformed", "short", "abort", "deadline", "shutdown"} {
		t.Run(variant, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			// Every Bridge process owns a separate role pool and storage client.
			poolA := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil)
			poolB := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil)
			storeA := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(poolA))
			storeA.FileBlobStore = firstBlob
			controlled := &replicaAttachmentBlob{BlobStore: secondBlob, hold: variant == "abort" || variant == "deadline" || variant == "shutdown", entered: make(chan struct{}), closed: make(chan struct{})}
			storeB := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(poolB))
			storeB.FileBlobStore = controlled
			storeB.AttachmentBlobStore = secondBlob
			ledger := &replicaAttachmentLedger{}
			addressA := serveReplicaAttachmentBridge(t, storeA, "metadata", variant, ledger)
			addressB := serveReplicaAttachmentBridge(t, storeB, "bytes", variant, ledger)
			if variant == "deleted" {
				if _, err := admin.ExecContext(ctx, `UPDATE files SET deleted_at=now() WHERE file_id='file_replica_attachment'`); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := admin.ExecContext(context.Background(), `UPDATE files SET deleted_at=NULL WHERE file_id='file_replica_attachment'`); err != nil {
						t.Error(err)
					}
				}()
			}
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-controlled.entered:
					w.WriteHeader(200)
				case <-r.Context().Done():
				}
			}))
			defer control.Close()
			root, err := filepath.Abs("..")
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, "bun", "run", "packages/provider-gateway/test/fixtures/replica-attachments.ts", addressA, addressB, variant, control.URL, hex.EncodeToString(digest[:]))
			command.Dir = filepath.Join(root, "services/gateway")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("actual Gateway child: %v\n%s", err, output)
			}
			var report struct {
				CompletionSamples []replicaCompletionSample `json:"completionSamples"`
				OK                bool                      `json:"ok"`
				ProviderCalls     int                       `json:"providerCalls"`
				ClientCloses      int                       `json:"clientCloses"`
				Hash              string                    `json:"hash"`
			}
			if err := json.Unmarshal(output, &report); err != nil || !report.OK || report.ClientCloses != 1 {
				t.Fatalf("missing/invalid child report: %s (%v)", output, err)
			}
			if len(report.CompletionSamples) == 0 {
				t.Fatal("missing actual completion samples")
			}
			for _, sample := range report.CompletionSamples {
				replicaLogCompletion(t, sample)
			}
			ledger.mu.Lock()
			calls := append([]replicaAttachmentCall(nil), ledger.calls...)
			ledger.mu.Unlock()
			if len(calls) == 0 || (variant != "transient" && calls[0].Replica != "metadata") {
				t.Fatalf("metadata did not route through A: %+v", calls)
			}
			if variant == "transient" && (report.ProviderCalls != 1 || len(calls) != 1 || calls[0].Replica != "bytes") {
				t.Fatalf("transient resolution: %+v %+v", report, calls)
			}
			if variant == "assembled" {
				if report.Hash != hex.EncodeToString(digest[:]) || report.ProviderCalls != 1 || len(calls) != 3 {
					t.Fatalf("assembled report=%+v calls=%+v", report, calls)
				}
				if calls[1].Replica != "bytes" || calls[1].Offset != 0 || calls[1].Length != chunk || calls[2].Offset != chunk || calls[2].Length != 17 {
					t.Fatalf("wrong offset/order: %+v", calls)
				}
				if calls[0].Remaining <= calls[1].Remaining || calls[1].Remaining <= calls[2].Remaining {
					t.Fatalf("budget restarted: %+v", calls)
				}
			}
			if controlled.hold {
				select {
				case <-controlled.closed:
				case <-time.After(10 * time.Second):
					t.Fatal("cancelled RPC blob reader did not close")
				}
				if report.ProviderCalls != 0 || len(calls) != 2 {
					t.Fatalf("cancelled preparation invoked next chunk/provider: %+v %+v", report, calls)
				}
			}
			t.Logf("actual Gateway/Bridge replicas+PG+MinIO variant=%s calls=%+v closes=%d provider=%d", variant, calls, report.ClientCloses, report.ProviderCalls)
		})
	}
}

type replicaAttachmentCall struct {
	Replica        string
	Offset, Length int64
	Remaining      time.Duration
}
type replicaAttachmentLedger struct {
	mu    sync.Mutex
	calls []replicaAttachmentCall
}

func serveReplicaAttachmentBridge(t *testing.T, store bridge.BridgeAPIStore, replica, variant string, ledger *replicaAttachmentLedger) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(11*1024*1024), grpc.MaxSendMsgSize(11*1024*1024), grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer replica-provider" {
			return nil, status.Error(codes.Unauthenticated, "fixture TokenReview denied")
		}
		identity := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "provider-gateway"}}
		if err := bridge.BridgeAPIMethodAuthorizer(identity, info.FullMethod); err != nil {
			return nil, err
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "deadline required")
		}
		entry := replicaAttachmentCall{Replica: replica, Remaining: time.Until(deadline)}
		if chunk, ok := request.(*bridgev1.ReadFileAttachmentChunkRequest); ok {
			entry.Offset = chunk.Offset
			entry.Length = chunk.Length
		}
		ledger.mu.Lock()
		ledger.calls = append(ledger.calls, entry)
		ledger.mu.Unlock()
		response, err := handler(auth.ContextWithIdentity(ctx, identity), request)
		if err == nil && variant == "malformed" {
			if r, ok := response.(*bridgev1.ResolveFileAttachmentMetadataResponse); ok && len(r.Attachments) > 0 {
				r.Attachments[0].GetMetadata().Mime = "application/x-invalid"
			}
		}
		if err == nil && variant == "short" {
			if r, ok := response.(*bridgev1.ReadFileAttachmentChunkResponse); ok {
				r.Outcome = &bridgev1.ReadFileAttachmentChunkResponse_Data{Data: []byte{1}}
			}
		}
		return response, err
	}))
	bridge.RegisterBridgeAPI(server, store)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-joined:
		case <-time.After(10 * time.Second):
			t.Error("Bridge failed to join")
		}
	})
	return listener.Addr().String()
}

type replicaAttachmentBlob struct {
	blob.BlobStore
	hold            bool
	entered, closed chan struct{}
	once            sync.Once
}

func (s *replicaAttachmentBlob) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	reader, err := s.BlobStore.(interface {
		GetRange(context.Context, string, int64, int64) (io.ReadCloser, error)
	}).GetRange(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	return &replicaAttachmentReader{ReadCloser: reader, ctx: ctx, owner: s}, nil
}

type replicaAttachmentReader struct {
	io.ReadCloser
	ctx   context.Context
	owner *replicaAttachmentBlob
}

func (r *replicaAttachmentReader) Read(p []byte) (int, error) {
	if r.owner.hold {
		r.owner.once.Do(func() { close(r.owner.entered) })
		<-r.ctx.Done()
		return 0, r.ctx.Err()
	}
	return r.ReadCloser.Read(p)
}
func (r *replicaAttachmentReader) Close() error {
	err := r.ReadCloser.Close()
	if r.owner.hold {
		close(r.owner.closed)
	}
	return err
}

// A shared isolated bucket with independently constructed real S3 clients.
func replicaMinIOStores(t *testing.T) func() *blob.S3BlobStore {
	stores, _ := replicaMinIOStoresWithConfig(t)
	return stores
}
func replicaMinIOStoresWithConfig(t *testing.T) (func() *blob.S3BlobStore, *blob.Config) {
	t.Helper()
	endpoint := os.Getenv("TETRAL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Fatal("replica composition requires provisioned MinIO")
	}
	region := os.Getenv("TETRAL_TEST_MINIO_REGION")
	key := os.Getenv("TETRAL_TEST_MINIO_ACCESS_KEY")
	secret := os.Getenv("TETRAL_TEST_MINIO_SECRET_KEY")
	bucket := fmt.Sprintf("replica-%d", time.Now().UnixNano())
	ctx := context.Background()
	cfg := &blob.Config{Endpoint: endpoint, Region: region, Bucket: bucket, AccessKey: key, SecretKey: secret, AllowInsecure: true, LocalTestMode: true}
	if err := blobtest.CreateBucket(ctx, http.DefaultClient, *cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := blobtest.DeleteBucketContents(ctx, http.DefaultClient, *cfg); err != nil {
			t.Error(err)
		}
	})
	return func() *blob.S3BlobStore {
		store, err := blob.NewS3BlobStore(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	}, cfg
}
