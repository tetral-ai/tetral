package agentruntimebridge

import (
	"context"
	"net"
	"testing"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type childInterruptTransportStore struct {
	BridgeAPIStore
	admitted bool
	awaited  bool
}

func (s *childInterruptTransportStore) AdmitChildInterrupt(context.Context, *bridgev1.AdmitChildInterruptRequest) (*bridgev1.AdmitChildInterruptResponse, error) {
	s.admitted = true
	return &bridgev1.AdmitChildInterruptResponse{Ack: committedAck("", "source")}, nil
}

func (s *childInterruptTransportStore) AwaitChildInterrupt(context.Context, *bridgev1.AwaitChildInterruptRequest) (*bridgev1.AwaitChildInterruptResponse, error) {
	s.awaited = true
	return &bridgev1.AwaitChildInterruptResponse{Ack: committedAck("", "source")}, nil
}

func TestBridgeAPIMethodAuthorizerScopesGatewayServiceAccount(t *testing.T) {
	gateway := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "gateway"}}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_McpManifestChanged_FullMethodName); err != nil {
		t.Fatalf("gateway McpManifestChanged authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_ResolveTransientAttachment_FullMethodName); err != nil {
		t.Fatalf("gateway ResolveTransientAttachment authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_ResolveFileAttachmentMetadata_FullMethodName); err != nil {
		t.Fatalf("gateway ResolveFileAttachmentMetadata authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_ReadFileAttachmentChunk_FullMethodName); err != nil {
		t.Fatalf("gateway ReadFileAttachmentChunk authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_ClaimMcpToolResult_FullMethodName); err != nil {
		t.Fatalf("gateway ClaimMcpToolResult authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_CommitMcpToolResult_FullMethodName); err != nil {
		t.Fatalf("gateway CommitMcpToolResult authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_RelinquishMcpToolResult_FullMethodName); err != nil {
		t.Fatalf("gateway RelinquishMcpToolResult authorization error = %v; want nil", err)
	}
	err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gateway AcceptSandboxExecution authorization error = %v; want PermissionDenied", err)
	}

	runtimePod := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName); err != nil {
		t.Fatalf("runtime pod AcceptSandboxExecution authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_AwaitSandboxExecution_FullMethodName); err != nil {
		t.Fatalf("runtime pod AwaitSandboxExecution authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_CommitInternalToolRepair_FullMethodName); err != nil {
		t.Fatalf("runtime pod CommitInternalToolRepair authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_SettleToolResult_FullMethodName); err != nil {
		t.Fatalf("runtime pod SettleToolResult authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_RefreshRuntimeBindingToken_FullMethodName); err != nil {
		t.Fatalf("runtime pod RefreshRuntimeBindingToken authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_AdmitChildInterrupt_FullMethodName); err != nil {
		t.Fatalf("runtime pod AdmitChildInterrupt authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_AwaitChildInterrupt_FullMethodName); err != nil {
		t.Fatalf("runtime pod AwaitChildInterrupt authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_McpManifestChanged_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("runtime pod McpManifestChanged authorization error = %v; want PermissionDenied", err)
	}
	if err := BridgeAPIMethodAuthorizer(gateway, bridgev1.AgentRuntimeBridgeService_CommitInternalToolRepair_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gateway CommitInternalToolRepair authorization error = %v; want PermissionDenied", err)
	}

	bridge := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "bridge"}}
	if err := BridgeAPIMethodAuthorizer(bridge, bridgev1.AgentRuntimeBridgeService_ReadCommandResult_FullMethodName); err != nil {
		t.Fatalf("bridge ReadCommandResult authorization error = %v; want nil", err)
	}
	if err := BridgeAPIMethodAuthorizer(bridge, bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("bridge AcceptSandboxExecution authorization error = %v; want PermissionDenied", err)
	}

	unknown := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "unknown"}}
	if err := BridgeAPIMethodAuthorizer(unknown, bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unknown AcceptSandboxExecution authorization error = %v; want PermissionDenied", err)
	}
}

func TestRuntimePodChildInterruptRPCsCrossAuthorizedGRPCSurface(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	identity := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := BridgeAPIMethodAuthorizer(identity, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}))
	store := &childInterruptTransportStore{}
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///child-interrupt", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatalf("dial child interrupt transport: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	if _, err := client.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{}); err != nil {
		t.Fatalf("AdmitChildInterrupt transport: %v", err)
	}
	if _, err := client.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{}); err != nil {
		t.Fatalf("AwaitChildInterrupt transport: %v", err)
	}
	if !store.admitted || !store.awaited {
		t.Fatalf("forwarded calls = admitted %t awaited %t; want both", store.admitted, store.awaited)
	}
}
