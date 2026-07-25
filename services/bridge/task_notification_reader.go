package agentruntimebridge

import (
	"context"

	internalgrpc "github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BridgeAPITaskNotificationReader struct {
	Client bridgev1.AgentRuntimeBridgeServiceClient
}

func NewBridgeAPITaskNotificationReader(tokenSource internalgrpcauth.TokenSource, conn *grpc.ClientConn) *BridgeAPITaskNotificationReader {
	if conn == nil {
		return &BridgeAPITaskNotificationReader{}
	}
	return &BridgeAPITaskNotificationReader{Client: bridgev1.NewAgentRuntimeBridgeServiceClient(conn)}
}

func DialBridgeAPITaskNotificationReader(address string, tokenSource internalgrpcauth.TokenSource, dialOptions ...grpc.DialOption) (*BridgeAPITaskNotificationReader, *grpc.ClientConn, error) {
	options := append([]grpc.DialOption{}, internalgrpc.TaskResultRPCDialOptions()...)
	options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	options = append(options, grpc.WithPerRPCCredentials(internalgrpcauth.NewServiceAccountTokenCredentials(tokenSource)))
	options = append(options, dialOptions...)
	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, nil, err
	}
	return NewBridgeAPITaskNotificationReader(tokenSource, conn), conn, nil
}

func (r *BridgeAPITaskNotificationReader) ReadTaskNotificationResult(ctx context.Context, scope *bridgev1.RuntimeScope, taskID string, sourceToolUseEventID string) (string, error) {
	if r == nil || r.Client == nil {
		return "", runtimeDeliveryPrepareError{kind: "bridge_api_unavailable", message: "bridge API task notification reader is unavailable", retryable: true}
	}
	// The ReadCommandResult idempotency key (request_id + task_id + normalized
	// payload) is derived deterministically from the durable
	// source_tool_use_event_id, not from a random per-attempt request_id. The key
	// is the draining-poll owner identity: a same-key retry is the sole authorized
	// successor and may re-drain after an owner dies mid-drain. The scheme carries
	// a bounded at-most-once output-loss residual.
	requestID, _, _ := readCommandResultOwnerIdentity(sourceToolUseEventID, taskID, true, 0)
	response, err := r.Client.ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{
		Scope:                   copyRuntimeScopeWithRequestID(scope, requestID),
		TaskId:                  taskID,
		DeferTerminalSettlement: true,
		ToolUseEventId:          sourceToolUseEventID,
	})
	if err != nil {
		return "", err
	}
	return response.GetResultJson(), nil
}
