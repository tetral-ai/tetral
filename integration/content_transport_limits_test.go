package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// serveContentBridge uses the production Bridge transport ceilings. Only the
// external TokenReview result is supplied; method authorization, caller identity
// and the real store remain in the request path. The after hook may lose an
// already committed response without synthesizing a business result.
func serveContentBridge(t *testing.T, store bridge.BridgeAPIStore, identities map[string]string, after func(context.Context, string, any) error) replicaBridge {
	t.Helper()
	verified := make(map[string]auth.Identity, len(identities))
	for token, pod := range identities {
		verified[token] = auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: pod}
	}
	return serveContentBridgeIdentities(t, store, verified, after)
}

func serveContentBridgeIdentities(t *testing.T, store bridge.BridgeAPIStore, identities map[string]auth.Identity, after func(context.Context, string, any) error) replicaBridge {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			values := md.Get("authorization")
			if len(values) != 1 {
				return nil, status.Error(codes.Unauthenticated, "verified fixture identity required")
			}
			parts := strings.Split(values[0], " ")
			if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || parts[1] == "" {
				return nil, status.Error(codes.Unauthenticated, "verified fixture identity required")
			}
			identity, ok := identities[parts[1]]
			if !ok {
				return nil, status.Error(codes.Unauthenticated, "unknown fixture identity")
			}
			if err := bridge.BridgeAPIMethodAuthorizer(identity, info.FullMethod); err != nil {
				return nil, err
			}
			response, err := handler(auth.ContextWithIdentity(ctx, identity), request)
			if err == nil && after != nil {
				err = after(ctx, info.FullMethod, response)
			}
			return response, err
		}),
	)
	bridge.RegisterBridgeAPI(server, store)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes), grpc.MaxCallSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes)))
	if err != nil {
		server.Stop()
		_ = listener.Close()
		<-joined
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("content Bridge server did not join")
		}
	})
	return replicaBridge{Address: listener.Addr().String(), Client: bridgev1.NewAgentRuntimeBridgeServiceClient(conn)}
}

// TestContentTransportLimits separates the semantic text limit from Bridge's
// larger encoded-message fuse. Ordinary content crosses the actual Runtime
// writer, authenticated TCP Bridge and restricted PostgreSQL store. Raw vectors
// bypass the domain encoder only; they still enter the same real Bridge server.
func TestContentTransportLimits(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "bridge")
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(workload.DB))
	observed := &contentTransportObservedStore{BridgeAPIStore: store, outcomes: make(map[string]contentTransportWriteOutcome)}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { observed.join.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("content Bridge handlers did not join after server shutdown")
		}
	})
	server := serveContentBridge(t, observed, map[string]string{"content-runtime": "content-pod"}, nil)
	// Independent Python json.dumps('x' * (16777214 + delta), separators=(',', ':'))
	// byte counts and SHA-256; no production size constant computes these oracles.
	vectors := []struct {
		name              string
		length, canonical int
		digest            string
	}{
		{"below-text-limit", 16777213, 16777215, "88e2ab3eed76b3979a69b0526b56b554d615032aa2d114de294271dba32e78a2"},
		{"at-text-limit", 16777214, 16777216, "39f6c0e5384f684efe7ea132f3537f2f3dd642366b2e7e97f8f894dfe30b72a5"},
		{"above-text-limit", 16777215, 16777217, "8bc5b622fb6ad75c136758ac647a8139ce6b1db06f49fa526e8f9a3b37bc2b20"},
	}
	for i, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			session, thread, binding := "sesn_content_size_"+vector.name, "sthr_content_size_"+vector.name, "bind_content_size_"+vector.name
			sessionfixture.SeedBridgeAPISession(t, admin, "default", session, thread)
			seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, 1, "content-pod")
			scope := sessionfixture.BridgeAPIScope(session, thread, binding, 1, "content-pod")
			seedBridgeAPIRequestStart(t, store, scope, "start", "content-request", "agent_provider_request", 0)
			before := contentTransportDurableCounts(t, admin, session)
			eventID := fmt.Sprintf("evt_%032x", i+1)
			input := map[string]any{"address": server.Address, "scope": map[string]any{"workspaceId": "default", "sessionId": session, "sessionThreadId": thread, "bindingId": binding, "bindingGeneration": 1, "targetPodUid": "content-pod", "runtimeProcessId": "process_content-pod"}, "textLength": vector.length, "expectedCanonicalBytes": vector.canonical, "expectedSHA256": vector.digest, "eventId": eventID}
			// This tests size and durable bytes, not latency. Race instrumentation of
			// maximum-sized JSON needs an explicit test-only deadline allowance; the
			// production WriteEvent policy remains unchanged.
			input["writeEventTimeoutMs"] = 15_000
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/content-bridge-transport.ts", path) //nolint:gosec // Repository fixture and private input.
			command.Dir = "../services/agent-runtime"
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("Runtime writer child: %v: %s", err, output)
			}
			var result struct {
				Bytes  int    `json:"bytes"`
				Digest string `json:"digest"`
				Closed bool   `json:"closed"`
				Result struct {
					OK      bool   `json:"ok"`
					Type    string `json:"type"`
					EventID string `json:"eventId"`
					Error   struct {
						Code string `json:"code"`
					} `json:"error"`
				} `json:"result"`
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatalf("Runtime child result: %v: %s", err, output)
			}
			if !result.Closed || result.Bytes != vector.canonical || result.Digest != vector.digest {
				t.Fatalf("Runtime content/closure evidence differs: %s", output)
			}
			if vector.canonical > 16777216 {
				outcome := observed.outcome(session)
				if result.Result.OK || result.Result.Error.Code != "unknown" || outcome.code != codes.InvalidArgument || outcome.message != "runtime text context exceeds its provider bound" {
					t.Fatalf("semantic overflow outcome: %s", output)
				}
				if after := contentTransportDurableCounts(t, admin, session); after != before {
					t.Fatalf("rejected text changed durable state: %v -> %v", before, after)
				}
				return
			}
			if !result.Result.OK || result.Result.Type != "committed" || result.Result.EventID != eventID {
				t.Fatalf("maximum-valid content did not commit: %s; handler=%+v", output, observed.outcome(session))
			}
			var messageText, eventText string
			if err := admin.QueryRow(`SELECT data_json::jsonb #>> '{parts,0,text}' FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND model_request_id='content-request'`, session).Scan(&messageText); err != nil {
				t.Fatal(err)
			}
			if err := admin.QueryRow(`SELECT payload_json::jsonb #>> '{content,0,text}' FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, session, eventID).Scan(&eventText); err != nil {
				t.Fatal(err)
			}
			for name, value := range map[string]string{"message": messageText, "event": eventText} {
				canonical, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if len(canonical) != vector.canonical || fmt.Sprintf("%x", sha256.Sum256(canonical)) != vector.digest {
					t.Fatalf("%s content differs from independent oracle", name)
				}
			}
			after := contentTransportDurableCounts(t, admin, session)
			if after[0] != before[0]+1 || after[1] != before[1]+1 || after[2] != before[2]+1 {
				t.Fatalf("text commit durable counts: %v -> %v", before, after)
			}
		})
	}
	t.Run("encoded-bridge-receiver", func(t *testing.T) {
		// Only the sending endpoint is raised; production Bridge receive stays64MiB.
		conn, err := grpc.NewClient(server.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(128*1024*1024), grpc.MaxCallRecvMsgSize(128*1024*1024)))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		scope := sessionfixture.BridgeAPIScope("sesn_content_size_at-text-limit", "sthr_content_size_at-text-limit", "bind_content_size_at-text-limit", 1, "content-pod")
		base, err := proto.Marshal(&bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "raw-semantic-invalid", EventType: "session.status_running"})
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range []int{1024, 67108863, 67108864, 67108865} {
			t.Run(fmt.Sprintf("encoded-%d", target), func(t *testing.T) {
				raw := contentRawWriteEnvelope(t, base, target)
				before := contentTransportDurableCounts(t, admin, scope.SessionId)
				calls := observed.writes.Load()
				ctx, cancel := context.WithTimeout(replicaRuntimeContext(context.Background(), "content-runtime"), 15*time.Second)
				defer cancel()
				err := conn.Invoke(ctx, bridgev1.AgentRuntimeBridgeService_WriteEvent_FullMethodName, raw, &bridgev1.WriteEventResponse{}, grpc.ForceCodec(contentRawProtoCodec{}))
				if target > 67108864 {
					if status.Code(err) != codes.ResourceExhausted || observed.writes.Load() != calls {
						t.Fatalf("receiver fuse status/handler calls = %v/%d", err, observed.writes.Load()-calls)
					}
				} else if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "event payload must be JSON" || observed.writes.Load() != calls+1 {
					t.Fatalf("transport-valid domain-invalid status/handler calls = %v/%d", err, observed.writes.Load()-calls)
				}
				if after := contentTransportDurableCounts(t, admin, scope.SessionId); after != before {
					t.Fatalf("raw rejected envelope changed state %v -> %v", before, after)
				}
			})
		}
	})
}

type contentTransportWriteOutcome struct {
	code    codes.Code
	message string
	elapsed time.Duration
}
type contentTransportObservedStore struct {
	bridge.BridgeAPIStore
	writes   atomic.Int64
	join     sync.WaitGroup
	mu       sync.Mutex
	outcomes map[string]contentTransportWriteOutcome
}

func (s *contentTransportObservedStore) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	s.writes.Add(1)
	s.join.Add(1)
	defer s.join.Done()
	started := time.Now()
	response, err := s.BridgeAPIStore.WriteEvent(ctx, request)
	s.mu.Lock()
	s.outcomes[request.GetScope().GetSessionId()] = contentTransportWriteOutcome{code: status.Code(err), message: status.Convert(err).Message(), elapsed: time.Since(started)}
	s.mu.Unlock()
	return response, err
}
func (s *contentTransportObservedStore) outcome(session string) contentTransportWriteOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outcomes[session]
}

func contentTransportDurableCounts(t *testing.T, admin *sql.DB, session string) [3]int {
	t.Helper()
	var result [3]int
	for i, table := range []string{"session_events", "session_messages", "session_bridge_operations"} {
		if err := admin.QueryRow("SELECT count(*) FROM "+table+" WHERE workspace_id='default' AND session_id=$1", session).Scan(&result[i]); err != nil {
			t.Fatal(err)
		}
	} //nolint:gosec // Fixed table names only.
	return result
}

// Independent protobuf wire arithmetic: field5 tag 0x2a, unsigned base128 byte
// count and ASCII payload. The generated decoder and reencoder must agree with
// the raw envelope's exact target size; no semantic text helper constructs it.
func contentRawWriteEnvelope(t *testing.T, base []byte, target int) []byte {
	t.Helper()
	payloadBytes := target - len(base) - 2
	for {
		var length [10]byte
		n := binary.PutUvarint(length[:], uint64(payloadBytes))
		next := target - len(base) - 1 - n
		if next == payloadBytes {
			break
		}
		payloadBytes = next
	}
	raw := make([]byte, 0, target)
	raw = append(raw, base...)
	raw = append(raw, 0x2a)
	raw = binary.AppendUvarint(raw, uint64(payloadBytes))
	for range payloadBytes {
		raw = append(raw, 'x')
	}
	var decoded bridgev1.WriteEventRequest
	if err := proto.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(raw) != target || proto.Size(&decoded) != target || len(decoded.PayloadJson) != payloadBytes {
		t.Fatal("independent encoded-message size differs")
	}
	return raw
}

type contentRawProtoCodec struct{}

func (contentRawProtoCodec) Name() string                      { return "proto" }
func (contentRawProtoCodec) Marshal(value any) ([]byte, error) { return value.([]byte), nil }
func (contentRawProtoCodec) Unmarshal(raw []byte, value any) error {
	return proto.Unmarshal(raw, value.(proto.Message))
}
