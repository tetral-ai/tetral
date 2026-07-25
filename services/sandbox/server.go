package tetralsandbox

import (
	"context"

	tetralsandboxv1 "github.com/tetral-ai/tetral/services/sandbox/gen/tetral/sandbox/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReleaseSandboxRequest struct {
	WorkspaceID          string
	SessionID            string
	SandboxID            string
	BindingID            string
	BindingGeneration    int64
	PreparationAttemptID string
	DeleteCleanupID      string
	Reason               string
	IdempotencyKey       string
}

type ReleaseSandboxStatus string

const (
	ReleaseSandboxStatusReleased        ReleaseSandboxStatus = "released"
	ReleaseSandboxStatusAlreadyReleased ReleaseSandboxStatus = "already_released"
	ReleaseSandboxStatusRetryLater      ReleaseSandboxStatus = "retry_later"
	ReleaseSandboxStatusFailed          ReleaseSandboxStatus = "failed"
	ReleaseSandboxStatusArchived        ReleaseSandboxStatus = "archived"
)

type ReleaseSandboxResult struct {
	Status        ReleaseSandboxStatus
	SandboxStatus string
}

type ReleaseSandboxHandler interface {
	ReleaseSandbox(context.Context, ReleaseSandboxRequest) (ReleaseSandboxResult, error)
}

type Server struct {
	tetralsandboxv1.UnimplementedSandboxServiceServer
	Handler ReleaseSandboxHandler
}

func Register(server *grpc.Server, handler ReleaseSandboxHandler) {
	tetralsandboxv1.RegisterSandboxServiceServer(server, &Server{Handler: handler})
}

func (s *Server) ReleaseSandbox(ctx context.Context, request *tetralsandboxv1.ReleaseSandboxRequest) (*tetralsandboxv1.ReleaseSandboxResponse, error) {
	if s == nil || s.Handler == nil {
		return nil, status.Error(codes.FailedPrecondition, "sandbox release handler is required")
	}
	bindingIdentity := request.GetBindingId() != "" && request.GetBindingGeneration() > 0 && request.GetPreparationAttemptId() == "" && request.GetDeleteCleanupId() == ""
	preparationIdentity := request.GetBindingId() == "" && request.GetBindingGeneration() == 0 && request.GetPreparationAttemptId() != "" && request.GetDeleteCleanupId() != ""
	if request.GetWorkspaceId() == "" || request.GetSessionId() == "" || request.GetSandboxId() == "" ||
		(!bindingIdentity && !preparationIdentity) || request.GetReason() == "" || request.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox release request is incomplete")
	}
	result, err := s.Handler.ReleaseSandbox(ctx, ReleaseSandboxRequest{
		WorkspaceID:          request.GetWorkspaceId(),
		SessionID:            request.GetSessionId(),
		SandboxID:            request.GetSandboxId(),
		BindingID:            request.GetBindingId(),
		BindingGeneration:    request.GetBindingGeneration(),
		PreparationAttemptID: request.GetPreparationAttemptId(),
		DeleteCleanupID:      request.GetDeleteCleanupId(),
		Reason:               request.GetReason(),
		IdempotencyKey:       request.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, "sandbox release failed")
	}
	return &tetralsandboxv1.ReleaseSandboxResponse{
		Status:        releaseStatusToProto(result.Status),
		SandboxStatus: normalizeReleaseSandboxStatus(result.SandboxStatus),
	}, nil
}

func releaseStatusToProto(statusValue ReleaseSandboxStatus) tetralsandboxv1.ReleaseSandboxStatus {
	switch statusValue {
	case ReleaseSandboxStatusReleased:
		return tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_RELEASED
	case ReleaseSandboxStatusAlreadyReleased:
		return tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_ALREADY_RELEASED
	case ReleaseSandboxStatusRetryLater:
		return tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_RETRY_LATER
	case ReleaseSandboxStatusFailed:
		return tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_FAILED
	case ReleaseSandboxStatusArchived:
		return tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_ARCHIVED
	default:
		return tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_UNSPECIFIED
	}
}
