package agentruntimebridge

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	if err := BridgeAPIMethodAuthorizer(runtimePod, bridgev1.AgentRuntimeBridgeService_RefreshRuntimeBindingToken_FullMethodName); err != nil {
		t.Fatalf("runtime pod RefreshRuntimeBindingToken authorization error = %v; want nil", err)
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
