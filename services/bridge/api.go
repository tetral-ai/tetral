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
	WriteRequestEnd(context.Context, *bridgev1.WriteRequestEndRequest) (*bridgev1.WriteRequestEndResponse, error)
	FinishIdle(context.Context, *bridgev1.FinishIdleRequest) (*bridgev1.FinishIdleResponse, error)
	CreateChildThread(context.Context, *bridgev1.CreateChildThreadRequest) (*bridgev1.CreateChildThreadResponse, error)
	ResolveChildThread(context.Context, *bridgev1.ResolveChildThreadRequest) (*bridgev1.ResolveChildThreadResponse, error)
	ListChildThreads(context.Context, *bridgev1.ListChildThreadsRequest) (*bridgev1.ListChildThreadsResponse, error)
	ResolveInterAgentDelivery(context.Context, *bridgev1.ResolveInterAgentDeliveryRequest) (*bridgev1.ResolveInterAgentDeliveryResponse, error)
	MarkChildThreadClosed(context.Context, *bridgev1.MarkChildThreadClosedRequest) (*bridgev1.MarkChildThreadClosedResponse, error)
	MarkChildThreadActive(context.Context, *bridgev1.MarkChildThreadActiveRequest) (*bridgev1.MarkChildThreadActiveResponse, error)
	RunTool(context.Context, *bridgev1.RunToolRequest) (*bridgev1.RunToolResponse, error)
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
	return store.CommitInputs(ctx, request)
}

func (s BridgeAPIServer) CommitTaskNotificationResult(ctx context.Context, request *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CommitTaskNotificationResult(ctx, request)
}

func (s BridgeAPIServer) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.WriteEvent(ctx, request)
	if errorCode, ok := closeoutWriteRejectionCode(err); ok {
		return &bridgev1.WriteEventResponse{Ack: rejectedAck(errorCode)}, nil
	}
	return response, err
}

func (s BridgeAPIServer) WriteRequestEnd(ctx context.Context, request *bridgev1.WriteRequestEndRequest) (*bridgev1.WriteRequestEndResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.WriteRequestEnd(ctx, request)
	if errorCode, ok := closeoutWriteRejectionCode(err); ok {
		return &bridgev1.WriteRequestEndResponse{Ack: rejectedAck(errorCode)}, nil
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
	if errorCode, ok := closeoutWriteRejectionCode(err); ok {
		return &bridgev1.FinishIdleResponse{Ack: rejectedAck(errorCode)}, nil
	}
	return response, err
}

func (s BridgeAPIServer) CommitRuntimeTermination(ctx context.Context, request *bridgev1.CommitRuntimeTerminationRequest) (*bridgev1.CommitRuntimeTerminationResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	response, err := store.CommitRuntimeTermination(ctx, request)
	if errorCode, ok := closeoutWriteRejectionCode(err); ok {
		return &bridgev1.CommitRuntimeTerminationResponse{Ack: rejectedAck(errorCode)}, nil
	}
	return response, err
}

func (s BridgeAPIServer) CreateChildThread(ctx context.Context, request *bridgev1.CreateChildThreadRequest) (*bridgev1.CreateChildThreadResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CreateChildThread(ctx, request)
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

func (s BridgeAPIServer) ResolveInterAgentDelivery(ctx context.Context, request *bridgev1.ResolveInterAgentDeliveryRequest) (*bridgev1.ResolveInterAgentDeliveryResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.ResolveInterAgentDelivery(ctx, request)
}

func (s BridgeAPIServer) MarkChildThreadClosed(ctx context.Context, request *bridgev1.MarkChildThreadClosedRequest) (*bridgev1.MarkChildThreadClosedResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.MarkChildThreadClosed(ctx, request)
}

func (s BridgeAPIServer) MarkChildThreadActive(ctx context.Context, request *bridgev1.MarkChildThreadActiveRequest) (*bridgev1.MarkChildThreadActiveResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.MarkChildThreadActive(ctx, request)
}

func (s BridgeAPIServer) RunTool(ctx context.Context, request *bridgev1.RunToolRequest) (*bridgev1.RunToolResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.RunTool(ctx, request)
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

func (s BridgeAPIServer) CommitInternalToolRepair(ctx context.Context, request *bridgev1.CommitInternalToolRepairRequest) (*bridgev1.CommitInternalToolRepairResponse, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}
	return store.CommitInternalToolRepair(ctx, request)
}
