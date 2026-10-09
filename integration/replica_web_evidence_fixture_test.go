package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	gatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	web "github.com/tetral-ai/tetral/services/web-connector"
)

// Observe successful writes and actual stored reads at the MinIO HTTP
// boundary. All requests still pass through the production S3 client and
// real MinIO; no claim or result is supplied by the observer.
type replicaWebObjects struct {
	mu             sync.Mutex
	puts           map[string]int
	inFlightPolled chan struct{}
	pollOnce       sync.Once
}

func replicaWebObservedStores(t *testing.T) ([]*blob.S3BlobStore, []*replicaWebObjects, *web.SnapshotStore) {
	t.Helper()
	direct, cfg := replicaMinIOStoresWithConfig(t)
	target, err := url.Parse(cfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	var stores []*blob.S3BlobStore
	var observers []*replicaWebObjects
	for i := 0; i < 3; i++ {
		observer := &replicaWebObjects{puts: make(map[string]int), inFlightPolled: make(chan struct{})}
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ModifyResponse = func(response *http.Response) error {
			key := strings.TrimPrefix(response.Request.URL.Path, "/"+cfg.Bucket+"/")
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				if response.Request.Method == http.MethodPut {
					observer.mu.Lock()
					observer.puts[key]++
					observer.mu.Unlock()
				}
				if response.Request.Method == http.MethodGet && strings.HasSuffix(key, "/jobs/event_concurrent.job") {
					raw, err := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if err != nil {
						return err
					}
					response.Body = io.NopCloser(bytes.NewReader(raw))
					var record struct {
						State string `json:"state"`
					}
					if json.Unmarshal(raw, &record) == nil && record.State == "in_flight" {
						observer.pollOnce.Do(func() { close(observer.inFlightPolled) })
					}
				}
			}
			return nil
		}
		server := httptest.NewServer(proxy)
		t.Cleanup(server.Close)
		observedConfig := *cfg
		observedConfig.Endpoint = server.URL
		store, err := blob.NewS3BlobStore(context.Background(), &observedConfig)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		stores, observers = append(stores, store), append(observers, observer)
	}
	return stores, observers, web.NewSnapshotStore(direct(), nil, time.Now)
}

func replicaWebPutCounts(observers []*replicaWebObjects, key string) (jobs, snapshots int) {
	for _, observer := range observers {
		observer.mu.Lock()
		jobs += observer.puts[key]
		for objectKey, count := range observer.puts {
			if strings.HasSuffix(objectKey, ".doc") || strings.HasSuffix(objectKey, ".meta") {
				snapshots += count
			}
		}
		observer.mu.Unlock()
	}
	return jobs, snapshots
}

func replicaWebStoredReceipt(ctx context.Context, store *web.SnapshotStore, request *gatewayv1.RunWebRequest, response *gatewayv1.RunWebResponse) (string, error) {
	raw, err := store.GetJob(ctx, web.Scope{WorkspaceID: request.WorkspaceId, SessionID: request.SessionId, ThreadID: request.SessionThreadId}, request.ToolUseEventId)
	if err != nil {
		return "", err
	}
	var record struct {
		State     string          `json:"state"`
		InputHash string          `json:"input_hash"`
		SettledAt string          `json:"settled_at"`
		Response  json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return "", err
	}
	stored := &gatewayv1.RunWebResponse{}
	if err := protojson.Unmarshal(record.Response, stored); err != nil {
		return "", err
	}
	if record.State != "completed" || len(record.InputHash) != 64 || record.SettledAt == "" || !proto.Equal(stored, response) {
		return "", fmt.Errorf("stored receipt is not the exact completed outcome: state=%s response=%+v", record.State, stored)
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

type replicaWebLossForwarder struct {
	gatewayv1.UnimplementedProviderGatewayServiceServer
	forward gatewayv1.ProviderGatewayServiceClient
	store   *web.SnapshotStore
	server  *grpc.Server
	result  chan replicaWebLostResult
}

type replicaWebLostResult struct {
	response *gatewayv1.RunWebResponse
	digest   string
	err      error
}

func (p *replicaWebLossForwarder) RunWeb(ctx context.Context, request *gatewayv1.RunWebRequest) (*gatewayv1.RunWebResponse, error) {
	response, err := p.forward.RunWeb(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer fixture"), request)
	var digest string
	if err == nil {
		digest, err = replicaWebStoredReceipt(ctx, p.store, request, response)
	}
	p.result <- replicaWebLostResult{response, digest, err}
	// Close the real HTTP/2 connection before this successful response can be
	// delivered. The observer above independently proves the receipt commit.
	p.server.Stop()
	return response, err
}

func replicaWebLoseCommittedResponse(ctx context.Context, t *testing.T, forward gatewayv1.ProviderGatewayServiceClient, store *web.SnapshotStore, request *gatewayv1.RunWebRequest) replicaWebLostResult {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	proxy := &replicaWebLossForwarder{forward: forward, store: store, server: server, result: make(chan replicaWebLostResult, 1)}
	gatewayv1.RegisterProviderGatewayServiceServer(server, proxy)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("response-loss proxy failed to join")
		}
	}()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	response, err := gatewayv1.NewProviderGatewayServiceClient(connection).RunWeb(ctx, request)
	if err == nil || response != nil {
		t.Fatalf("committed response unexpectedly reached client: %+v/%v", response, err)
	}
	select {
	case captured := <-proxy.result:
		if captured.err != nil || captured.response.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
			t.Fatalf("response-loss precondition not proved: %+v/%v", captured.response, captured.err)
		}
		return captured
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return replicaWebLostResult{}
}
