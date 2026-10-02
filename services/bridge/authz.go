package agentruntimebridge

import (
	"context"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	providerServiceAccount   = "tetral-system/provider-gateway"
	mcpServiceAccount        = "tetral-system/mcp-connector"
	runtimePodServiceAccount = "tetral-agent-runtime/agent-runtime"
)

// BridgeAPIMethodAuthorizer admits Provider only to attachment reads and MCP only
// to manifest/result custody. Sandbox and Runtime write methods stay Runtime Pod only.
func BridgeAPIMethodAuthorizer(identity auth.Identity, method string) error {
	switch identity.ServiceAccount.String() {
	case runtimePodServiceAccount:
		if isRuntimePodBridgeAPIMethod(method) {
			return nil
		}
	case providerServiceAccount:
		if isProviderAttachmentMethod(method) {
			return nil
		}
	case mcpServiceAccount:
		if isMcpConnectorBridgeMethod(method) {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "method not allowed")
}

func isRuntimePodBridgeAPIMethod(method string) bool {
	switch method {
	case bridgev1.AgentRuntimeBridgeService_RegisterRuntimeProcess_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReportRuntimeProcess_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReleaseRuntimeBinding_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_LoadContext_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RefreshRuntimeBindingToken_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitInputs_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitTaskNotificationResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_WriteEvent_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_SettleToolResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_WriteRequestEnd_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_FinishIdle_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CreateSubagentThread_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_EnsureApprovalReviewerTrunk_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_EnsureApprovalReviewerSidecar_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AdmitApprovalReviewInput_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveChildThread_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ListChildThreads_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_DeliverInterAgentMail_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadAgentMail_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AdmitChildInterrupt_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AwaitChildInterrupt_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CloseChildControl_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CloseApprovalReviewer_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_MarkChildThreadActive_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AwaitSandboxExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadCommandResult_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_SendCommandInput_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CancelCommand_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_AuthorizeWebToolExecution_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_RunMemory_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitInternalToolRepair_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_CommitRuntimeTermination_FullMethodName:
		return true
	default:
		return false
	}
}

func isProviderAttachmentMethod(method string) bool {
	switch method {
	case bridgev1.AgentRuntimeBridgeService_ResolveTransientAttachment_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ResolveFileAttachmentMetadata_FullMethodName,
		bridgev1.AgentRuntimeBridgeService_ReadFileAttachmentChunk_FullMethodName:
		return true
	default:
		return false
	}
}

func isMcpConnectorBridgeMethod(method string) bool {
	switch method {
	case bridgev1.AgentRuntimeBridgeService_McpManifestChanged_FullMethodName,
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
		return runtimecontrol.ScopeSupersededError(status.Error(codes.PermissionDenied, "runtime caller pod UID does not match binding"))
	}
	return nil
}
