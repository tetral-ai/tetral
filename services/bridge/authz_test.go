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
	return &bridgev1.AdmitChildInterruptResponse{Outcome: &bridgev1.AdmitChildInterruptResponse_Committed{Committed: &bridgev1.AdmitChildInterruptCommitted{ControlOperationId: "source"}}}, nil
}

func (s *childInterruptTransportStore) AwaitChildInterrupt(context.Context, *bridgev1.AwaitChildInterruptRequest) (*bridgev1.AwaitChildInterruptResponse, error) {
	s.awaited = true
	return &bridgev1.AwaitChildInterruptResponse{Outcome: &bridgev1.AwaitChildInterruptResponse_Completed{Completed: &bridgev1.AwaitChildInterruptCompleted{}}}, nil
}

func TestBridgeAPIMethodAuthorizerSeparatesConnectorPrivileges(t *testing.T) {
	provider := []string{
		bridgev1.AgentRuntimeBridgeService_ResolveTransientAttachment_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveFileAttachmentMetadata_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadFileAttachmentChunk_FullMethodName,
	}
	mcp := []string{
		bridgev1.AgentRuntimeBridgeService_McpManifestChanged_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ClaimMcpToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitMcpToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RelinquishMcpToolResult_FullMethodName,
	}
	for _, sa := range []string{"provider-gateway", "mcp-connector", "bridge", "job-runner", "gateway", "unknown"} {
		for _, method := range append(append(append([]string{}, provider...), mcp...), bridgev1.AgentRuntimeBridgeService_ReadCommandResult_FullMethodName, bridgev1.AgentRuntimeBridgeService_WriteEvent_FullMethodName, bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName, bridgev1.AgentRuntimeBridgeService_AwaitSandboxExecution_FullMethodName, bridgev1.AgentRuntimeBridgeService_CommitInternalToolRepair_FullMethodName, "/unknown") {
			want := sa == "provider-gateway" && containsAuthMethod(provider, method) || sa == "mcp-connector" && containsAuthMethod(mcp, method)
			for _, ns := range []string{"tetral-system", "wrong-system"} {
				err := BridgeAPIMethodAuthorizer(auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: ns, Name: sa}}, method)
				if want && ns == "tetral-system" {
					if err != nil {
						t.Fatalf("%s/%s %s: %v", ns, sa, method, err)
					}
				} else if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("%s/%s %s status=%v; want permission denied", ns, sa, method, err)
				}
			}
		}
	}
}

func TestBridgeAPIMethodAuthorizerPreservesRuntimePrivileges(t *testing.T) {
	caller := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}}
	for _, method := range []string{
		bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AwaitSandboxExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitInternalToolRepair_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_SettleToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RefreshRuntimeBindingToken_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AdmitChildInterrupt_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AwaitChildInterrupt_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AuthorizeWebToolExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadCommandResult_FullMethodName,
	} {
		if err := BridgeAPIMethodAuthorizer(caller, method); err != nil {
			t.Fatalf("Runtime %s: %v", method, err)
		}
	}
	for _, method := range []string{bridgev1.AgentRuntimeBridgeService_McpManifestChanged_FullMethodName, bridgev1.AgentRuntimeBridgeService_ClaimMcpToolResult_FullMethodName, "/unknown"} {
		if err := BridgeAPIMethodAuthorizer(caller, method); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("Runtime %s: %v; want denied", method, err)
		}
	}
}

func containsAuthMethod(methods []string, method string) bool {
	for _, m := range methods {
		if m == method {
			return true
		}
	}
	return false
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
