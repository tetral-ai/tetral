package agentruntimebridge

import (
	"context"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	gatewayServiceAccount    = "tetral-system/gateway"
	bridgeServiceAccount     = "tetral-system/bridge"
	runtimePodServiceAccount = "tetral-agent-runtime/agent-runtime"
)

// BridgeAPIMethodAuthorizer keeps gateway's Bridge credential scoped to
// the Gateway-owned read/notification surfaces. Sandbox and Runtime write
// methods stay Runtime Pod only.
func BridgeAPIMethodAuthorizer(identity auth.Identity, method string) error {
	switch identity.ServiceAccount.String() {
	case runtimePodServiceAccount:
		if isRuntimePodBridgeAPIMethod(method) {
			return nil
		}
	case gatewayServiceAccount:
		if isGatewayBridgeAPIMethod(method) {
			return nil
		}
	case bridgeServiceAccount:
		if method == bridgev1.AgentRuntimeBridgeService_ReadCommandResult_FullMethodName {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "method not allowed")
}

func isRuntimePodBridgeAPIMethod(method string) bool {
	switch method {
	case bridgev1.AgentRuntimeBridgeService_LoadContext_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RefreshRuntimeBindingToken_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitInputs_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitTaskNotificationResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_WriteEvent_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_SettleToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_WriteRequestEnd_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_FinishIdle_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CreateChildThread_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveChildThread_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ListChildThreads_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveInterAgentDelivery_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AdmitChildInterrupt_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AwaitChildInterrupt_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_MarkChildThreadClosed_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_MarkChildThreadActive_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AwaitSandboxExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadCommandResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_SendCommandInput_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CancelCommand_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RunMemory_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitInternalToolRepair_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitRuntimeTermination_FullMethodName:
		return true
	default:
		return false
	}
}

func isGatewayBridgeAPIMethod(method string) bool {
	switch method {
	case bridgev1.AgentRuntimeBridgeService_McpManifestChanged_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveTransientAttachment_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveFileAttachmentMetadata_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadFileAttachmentChunk_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ClaimMcpToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitMcpToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RelinquishMcpToolResult_FullMethodName:
		return true
	default:
		return false
	}
}

func verifyRuntimeCallerPodUID(ctx context.Context, scope *bridgev1.RuntimeScope) error {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.ServiceAccount.String() != runtimePodServiceAccount {
		return nil
	}
	if identity.KubernetesPodUID == "" {
		return status.Error(codes.PermissionDenied, "runtime caller pod UID is not verified")
	}
	if identity.KubernetesPodUID != scope.GetBinding().GetTargetPodUid() {
		return scopeSupersededError(status.Error(codes.PermissionDenied, "runtime caller pod UID does not match binding"))
	}
	return nil
}
