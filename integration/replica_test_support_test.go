package integration

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type replicaBridge struct {
	Address string
	Client  bridgev1.AgentRuntimeBridgeServiceClient
}

// Each call owns a separately addressable server and channel. Tokens represent
// the verified TokenReview identity at the authentication seam; production
// method authorization and durable scope checks still execute unchanged.
func serveReplicaBridge(t *testing.T, store bridge.BridgeAPIStore, identities map[string]string, after func(context.Context, string, any) error) replicaBridge {
	verified := map[string]auth.Identity{}
	for token, podUID := range identities {
		verified[token] = auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: podUID}
	}
	return serveReplicaBridgeIdentities(t, store, verified, after)
}
func serveReplicaMCPBridge(t *testing.T, store bridge.BridgeAPIStore, token string, after func(context.Context, string, any) error) replicaBridge {
	return serveReplicaBridgeIdentities(t, store, map[string]auth.Identity{token: {ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "mcp-connector"}, KubernetesPodUID: "mcp-replica"}}, after)
}
func serveReplicaBridgeIdentities(t *testing.T, store bridge.BridgeAPIStore, identities map[string]auth.Identity, after func(context.Context, string, any) error) replicaBridge {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			return nil, status.Error(codes.Unauthenticated, "verified fixture identity required")
		}
		identity, ok := identities[strings.TrimPrefix(values[0], "Bearer ")]
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
	}))
	bridge.RegisterBridgeAPI(server, store)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
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
			t.Error("replica Bridge server did not join")
		}
	})
	return replicaBridge{Address: listener.Addr().String(), Client: bridgev1.NewAgentRuntimeBridgeServiceClient(conn)}
}

func replicaRuntimeContext(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
