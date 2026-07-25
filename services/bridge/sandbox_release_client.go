package agentruntimebridge

import (
	"context"
	"errors"

	internalgrpc "github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	tetralsandboxv1 "github.com/tetral-ai/tetral/services/sandbox/gen/tetral/sandbox/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SandboxServiceReleaseClient struct {
	Address     string
	TokenSource internalgrpcauth.TokenSource
	DialOptions []grpc.DialOption
}

func NewSandboxServiceReleaseClient(address string, tokenSource internalgrpcauth.TokenSource, dialOptions ...grpc.DialOption) *SandboxServiceReleaseClient {
	return &SandboxServiceReleaseClient{Address: address, TokenSource: tokenSource, DialOptions: append([]grpc.DialOption(nil), dialOptions...)}
}

func (c *SandboxServiceReleaseClient) ReleaseSandbox(ctx context.Context, request SandboxReleaseRequest) (SandboxReleaseResult, error) {
	if request.Reason == "runtime_pod_lost" {
		if !request.DurableCleanupFence {
			return SandboxReleaseResult{}, errors.New("runtime pod-loss sandbox release requires a durable cleanup fence")
		}
	}
	if c == nil || c.Address == "" || c.TokenSource == nil {
		return SandboxReleaseResult{}, errors.New("sandbox release client is required")
	}
	options := append([]grpc.DialOption{}, internalgrpc.SessionRPCDialOptions()...)
	if len(c.DialOptions) == 0 {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		options = append(options, c.DialOptions...)
	}
	options = append(options, grpc.WithPerRPCCredentials(internalgrpcauth.NewServiceAccountTokenCredentials(c.TokenSource)))
	conn, err := grpc.NewClient(c.Address, options...)
	if err != nil {
		return SandboxReleaseResult{}, err
	}
	defer func() { _ = conn.Close() }()
	response, err := tetralsandboxv1.NewSandboxServiceClient(conn).ReleaseSandbox(ctx, &tetralsandboxv1.ReleaseSandboxRequest{
		WorkspaceId:          request.WorkspaceID,
		SessionId:            request.SessionID,
		SandboxId:            request.SandboxID,
		BindingId:            request.BindingID,
		BindingGeneration:    request.BindingGeneration,
		PreparationAttemptId: request.PreparationAttemptID,
		DeleteCleanupId:      request.DeleteCleanupID,
		Reason:               request.Reason,
		IdempotencyKey:       request.IdempotencyKey,
	})
	if err != nil {
		return SandboxReleaseResult{}, err
	}
	return SandboxReleaseResult{
		Status:        releaseStatusFromProto(response.GetStatus()),
		SandboxStatus: response.GetSandboxStatus(),
	}, nil
}

func releaseStatusFromProto(statusValue tetralsandboxv1.ReleaseSandboxStatus) SandboxReleaseStatus {
	switch statusValue {
	case tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_RELEASED:
		return SandboxReleaseReleased
	case tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_ALREADY_RELEASED:
		return SandboxReleaseAlreadyReleased
	case tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_RETRY_LATER:
		return SandboxReleaseRetryLater
	case tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_FAILED:
		return SandboxReleaseFailed
	case tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_ARCHIVED:
		return SandboxReleaseArchived
	default:
		return ""
	}
}
