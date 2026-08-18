package agentruntimebridge

import (
	"context"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BridgeAPIStore interface {
	LoadContext(context.Context, *bridgev1.LoadContextRequest) (*bridgev1.LoadContextResponse, error)
	RefreshRuntimeBindingToken(context.Context, *bridgev1.RefreshRuntimeBindingTokenRequest) (*bridgev1.RefreshRuntimeBindingTokenResponse, error)
	CommitInputs(context.Context, *bridgev1.CommitInputsRequest) (*bridgev1.CommitInputsResponse, error)
	CommitTaskNotificationResult(context.Context, *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error)
	WriteEvent(context.Context, *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error)
	SettleToolResult(context.Context, *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error)
	WriteRequestEnd(context.Context, *bridgev1.WriteRequestEndRequest) (*bridgev1.WriteRequestEndResponse, error)
	FinishIdle(context.Context, *bridgev1.FinishIdleRequest) (*bridgev1.FinishIdleResponse, error)
	CreateSubagentThread(context.Context, *bridgev1.CreateSubagentThreadRequest) (*bridgev1.CreateSubagentThreadResponse, error)
	EnsureApprovalReviewerTrunk(context.Context, *bridgev1.EnsureApprovalReviewerTrunkRequest) (*bridgev1.EnsureApprovalReviewerTrunkResponse, error)
	EnsureApprovalReviewerSidecar(context.Context, *bridgev1.EnsureApprovalReviewerSidecarRequest) (*bridgev1.EnsureApprovalReviewerSidecarResponse, error)
	AdmitApprovalReviewInput(context.Context, *bridgev1.AdmitApprovalReviewInputRequest) (*bridgev1.AdmitApprovalReviewInputResponse, error)
	ResolveChildThread(context.Context, *bridgev1.ResolveChildThreadRequest) (*bridgev1.ResolveChildThreadResponse, error)
	ListChildThreads(context.Context, *bridgev1.ListChildThreadsRequest) (*bridgev1.ListChildThreadsResponse, error)
	DeliverInterAgentMail(context.Context, *bridgev1.DeliverInterAgentMailRequest) (*bridgev1.DeliverInterAgentMailResponse, error)
	ReadAgentMail(context.Context, *bridgev1.ReadAgentMailRequest) (*bridgev1.ReadAgentMailResponse, error)
	AdmitChildInterrupt(context.Context, *bridgev1.AdmitChildInterruptRequest) (*bridgev1.AdmitChildInterruptResponse, error)
	AwaitChildInterrupt(context.Context, *bridgev1.AwaitChildInterruptRequest) (*bridgev1.AwaitChildInterruptResponse, error)
	CloseChildControl(context.Context, *bridgev1.CloseChildControlRequest) (*bridgev1.CloseChildControlResponse, error)
	CloseApprovalReviewer(context.Context, *bridgev1.CloseApprovalReviewerRequest) (*bridgev1.CloseApprovalReviewerResponse, error)
	MarkChildThreadActive(context.Context, *bridgev1.MarkChildThreadActiveRequest) (*bridgev1.MarkChildThreadActiveResponse, error)
	AcceptSandboxExecution(context.Context, *bridgev1.AcceptSandboxExecutionRequest) (*bridgev1.AcceptSandboxExecutionResponse, error)
	AwaitSandboxExecution(context.Context, *bridgev1.AwaitSandboxExecutionRequest) (*bridgev1.AwaitSandboxExecutionResponse, error)
	ReadCommandResult(context.Context, *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error)
	SendCommandInput(context.Context, *bridgev1.SendCommandInputRequest) (*bridgev1.SendCommandInputResponse, error)
	CancelCommand(context.Context, *bridgev1.CancelCommandRequest) (*bridgev1.CancelCommandResponse, error)
	RunMemory(context.Context, *bridgev1.RunMemoryRequest) (*bridgev1.RunMemoryResponse, error)
	ResolveTransientAttachment(context.Context, *bridgev1.ResolveTransientAttachmentRequest) (*bridgev1.ResolveTransientAttachmentResponse, error)
	ResolveFileAttachmentMetadata(context.Context, *bridgev1.ResolveFileAttachmentMetadataRequest) (*bridgev1.ResolveFileAttachmentMetadataResponse, error)
	ReadFileAttachmentChunk(context.Context, *bridgev1.ReadFileAttachmentChunkRequest) (*bridgev1.ReadFileAttachmentChunkResponse, error)
	McpManifestChanged(context.Context, *bridgev1.McpManifestChangedRequest) (*bridgev1.McpManifestChangedResponse, error)
	ClaimMcpToolResult(context.Context, *bridgev1.ClaimMcpToolResultRequest) (*bridgev1.ClaimMcpToolResultResponse, error)
	CommitMcpToolResult(context.Context, *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error)
	RelinquishMcpToolResult(context.Context, *bridgev1.RelinquishMcpToolResultRequest) (*bridgev1.RelinquishMcpToolResultResponse, error)
	CommitInternalToolRepair(context.Context, *bridgev1.CommitInternalToolRepairRequest) (*bridgev1.CommitInternalToolRepairResponse, error)
	CommitRuntimeTermination(context.Context, *bridgev1.CommitRuntimeTerminationRequest) (*bridgev1.CommitRuntimeTerminationResponse, error)
}

type BridgeAPIServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store BridgeAPIStore
}

func RegisterBridgeAPI(server *grpc.Server, store BridgeAPIStore) {
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, BridgeAPIServer{store: store})
}

func (s BridgeAPIServer) requireStore() (BridgeAPIStore, error) {
	if s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "bridge API store is unavailable")
	}
	return s.store, nil
}

func (s BridgeAPIServer) LoadContext(ctx context.Context, request *bridgev1.LoadContextRequest) (*bridgev1.LoadContextResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.LoadContext(ctx, request)
}

func (s BridgeAPIServer) RefreshRuntimeBindingToken(ctx context.Context, request *bridgev1.RefreshRuntimeBindingTokenRequest) (*bridgev1.RefreshRuntimeBindingTokenResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.RefreshRuntimeBindingToken(ctx, request)
}

func (s BridgeAPIServer) CommitInputs(ctx context.Context, request *bridgev1.CommitInputsRequest) (*bridgev1.CommitInputsResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.CommitInputs(ctx, request)
	if isScopeSupersededError(err) {
		return &bridgev1.CommitInputsResponse{Outcome: &bridgev1.CommitInputsResponse_Stale{Stale: &bridgev1.CommitInputsStale{}}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) CommitTaskNotificationResult(ctx context.Context, request *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.CommitTaskNotificationResult(ctx, request)
	if isScopeSupersededError(err) {
		return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Stale{Stale: &bridgev1.CommitTaskNotificationResultStale{}}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.WriteEvent(ctx, request)
	if isScopeSupersededError(err) {
		return &bridgev1.WriteEventResponse{Outcome: &bridgev1.WriteEventResponse_Stale{Stale: &bridgev1.WriteEventStale{}}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) SettleToolResult(ctx context.Context, request *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.SettleToolResult(ctx, request)
}

func (s BridgeAPIServer) WriteRequestEnd(ctx context.Context, request *bridgev1.WriteRequestEndRequest) (*bridgev1.WriteRequestEndResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.WriteRequestEnd(ctx, request)
	if isScopeSupersededError(err) {
		return &bridgev1.WriteRequestEndResponse{Outcome: &bridgev1.WriteRequestEndResponse_Stale{Stale: &bridgev1.WriteRequestEndStale{}}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) ResolveFileAttachmentMetadata(ctx context.Context, request *bridgev1.ResolveFileAttachmentMetadataRequest) (*bridgev1.ResolveFileAttachmentMetadataResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ResolveFileAttachmentMetadata(ctx, request)
}

func (s BridgeAPIServer) ReadFileAttachmentChunk(ctx context.Context, request *bridgev1.ReadFileAttachmentChunkRequest) (*bridgev1.ReadFileAttachmentChunkResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ReadFileAttachmentChunk(ctx, request)
}

func (s BridgeAPIServer) FinishIdle(ctx context.Context, request *bridgev1.FinishIdleRequest) (*bridgev1.FinishIdleResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.FinishIdle(ctx, request)
	if isScopeSupersededError(err) {
		return &bridgev1.FinishIdleResponse{Outcome: &bridgev1.FinishIdleResponse_Stale{Stale: &bridgev1.FinishIdleStale{}}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) CommitRuntimeTermination(ctx context.Context, request *bridgev1.CommitRuntimeTerminationRequest) (*bridgev1.CommitRuntimeTerminationResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CommitRuntimeTermination(ctx, request)
}

func (s BridgeAPIServer) CreateSubagentThread(ctx context.Context, request *bridgev1.CreateSubagentThreadRequest) (*bridgev1.CreateSubagentThreadResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CreateSubagentThread(ctx, request)
}

func (s BridgeAPIServer) EnsureApprovalReviewerTrunk(ctx context.Context, request *bridgev1.EnsureApprovalReviewerTrunkRequest) (*bridgev1.EnsureApprovalReviewerTrunkResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.EnsureApprovalReviewerTrunk(ctx, request)
	if isConversationMutationStaleError(err) {
		return &bridgev1.EnsureApprovalReviewerTrunkResponse{Outcome: &bridgev1.EnsureApprovalReviewerTrunkResponse_Stale{
			Stale: &bridgev1.EnsureApprovalReviewerTrunkStale{},
		}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) EnsureApprovalReviewerSidecar(ctx context.Context, request *bridgev1.EnsureApprovalReviewerSidecarRequest) (*bridgev1.EnsureApprovalReviewerSidecarResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.EnsureApprovalReviewerSidecar(ctx, request)
	if isConversationMutationStaleError(err) {
		return &bridgev1.EnsureApprovalReviewerSidecarResponse{Outcome: &bridgev1.EnsureApprovalReviewerSidecarResponse_Stale{
			Stale: &bridgev1.EnsureApprovalReviewerSidecarStale{},
		}}, nil
	}
	return response, err
}

func (s BridgeAPIServer) AdmitApprovalReviewInput(ctx context.Context, request *bridgev1.AdmitApprovalReviewInputRequest) (*bridgev1.AdmitApprovalReviewInputResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.AdmitApprovalReviewInput(ctx, request)
}

func (s BridgeAPIServer) ResolveChildThread(ctx context.Context, request *bridgev1.ResolveChildThreadRequest) (*bridgev1.ResolveChildThreadResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ResolveChildThread(ctx, request)
}

func (s BridgeAPIServer) ListChildThreads(ctx context.Context, request *bridgev1.ListChildThreadsRequest) (*bridgev1.ListChildThreadsResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ListChildThreads(ctx, request)
}

func (s BridgeAPIServer) DeliverInterAgentMail(ctx context.Context, request *bridgev1.DeliverInterAgentMailRequest) (*bridgev1.DeliverInterAgentMailResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.DeliverInterAgentMail(ctx, request)
}

func (s BridgeAPIServer) ReadAgentMail(ctx context.Context, request *bridgev1.ReadAgentMailRequest) (*bridgev1.ReadAgentMailResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ReadAgentMail(ctx, request)
}

func (s BridgeAPIServer) AdmitChildInterrupt(ctx context.Context, request *bridgev1.AdmitChildInterruptRequest) (*bridgev1.AdmitChildInterruptResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.AdmitChildInterrupt(ctx, request)
}

func (s BridgeAPIServer) AwaitChildInterrupt(ctx context.Context, request *bridgev1.AwaitChildInterruptRequest) (*bridgev1.AwaitChildInterruptResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.AwaitChildInterrupt(ctx, request)
}

func (s BridgeAPIServer) CloseChildControl(ctx context.Context, request *bridgev1.CloseChildControlRequest) (*bridgev1.CloseChildControlResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CloseChildControl(ctx, request)
}

func (s BridgeAPIServer) CloseApprovalReviewer(ctx context.Context, request *bridgev1.CloseApprovalReviewerRequest) (*bridgev1.CloseApprovalReviewerResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CloseApprovalReviewer(ctx, request)
}

func (s BridgeAPIServer) MarkChildThreadActive(ctx context.Context, request *bridgev1.MarkChildThreadActiveRequest) (*bridgev1.MarkChildThreadActiveResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.MarkChildThreadActive(ctx, request)
}

func (s BridgeAPIServer) AcceptSandboxExecution(ctx context.Context, request *bridgev1.AcceptSandboxExecutionRequest) (*bridgev1.AcceptSandboxExecutionResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.AcceptSandboxExecution(ctx, request)
}

func (s BridgeAPIServer) AwaitSandboxExecution(ctx context.Context, request *bridgev1.AwaitSandboxExecutionRequest) (*bridgev1.AwaitSandboxExecutionResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.AwaitSandboxExecution(ctx, request)
}

func (s BridgeAPIServer) ReadCommandResult(ctx context.Context, request *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ReadCommandResult(ctx, request)
}

func (s BridgeAPIServer) SendCommandInput(ctx context.Context, request *bridgev1.SendCommandInputRequest) (*bridgev1.SendCommandInputResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.SendCommandInput(ctx, request)
}

func (s BridgeAPIServer) CancelCommand(ctx context.Context, request *bridgev1.CancelCommandRequest) (*bridgev1.CancelCommandResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CancelCommand(ctx, request)
}

func (s BridgeAPIServer) RunMemory(ctx context.Context, request *bridgev1.RunMemoryRequest) (*bridgev1.RunMemoryResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.RunMemory(ctx, request)
}

func (s BridgeAPIServer) ResolveTransientAttachment(ctx context.Context, request *bridgev1.ResolveTransientAttachmentRequest) (*bridgev1.ResolveTransientAttachmentResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ResolveTransientAttachment(ctx, request)
}

func (s BridgeAPIServer) McpManifestChanged(ctx context.Context, request *bridgev1.McpManifestChangedRequest) (*bridgev1.McpManifestChangedResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.McpManifestChanged(ctx, request)
}

func (s BridgeAPIServer) ClaimMcpToolResult(ctx context.Context, request *bridgev1.ClaimMcpToolResultRequest) (*bridgev1.ClaimMcpToolResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ClaimMcpToolResult(ctx, request)
}

func (s BridgeAPIServer) CommitMcpToolResult(ctx context.Context, request *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CommitMcpToolResult(ctx, request)
}

func (s BridgeAPIServer) RelinquishMcpToolResult(ctx context.Context, request *bridgev1.RelinquishMcpToolResultRequest) (*bridgev1.RelinquishMcpToolResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.RelinquishMcpToolResult(ctx, request)
}

func (s BridgeAPIServer) CommitInternalToolRepair(ctx context.Context, request *bridgev1.CommitInternalToolRepairRequest) (*bridgev1.CommitInternalToolRepairResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CommitInternalToolRepair(ctx, request)
}
