package tetralsandbox

import (
	"context"
	"testing"

	tetralsandboxv1 "github.com/tetral-ai/tetral/services/sandbox/gen/tetral/sandbox/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerReleaseSandboxRequiresReason(t *testing.T) {
	server := &Server{Handler: staticReleaseHandler{}}

	_, err := server.ReleaseSandbox(context.Background(), &tetralsandboxv1.ReleaseSandboxRequest{
		WorkspaceId:       "default",
		SessionId:         "sesn_1",
		SandboxId:         "sandbox_1",
		BindingId:         "rtbind_1",
		BindingGeneration: 1,
		IdempotencyKey:    "cleanup_session:cleanup_1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReleaseSandbox status = %v; want InvalidArgument", status.Code(err))
	}
}

func TestServerReleaseSandboxRequiresExactlyOneAuthorizedIdentity(t *testing.T) {
	server := &Server{Handler: staticReleaseHandler{}}
	for _, request := range []*tetralsandboxv1.ReleaseSandboxRequest{
		{WorkspaceId: "default", SessionId: "sesn_1", SandboxId: "sandbox_1", Reason: "delete", IdempotencyKey: "delete_missing_identity"},
		{WorkspaceId: "default", SessionId: "sesn_1", SandboxId: "sandbox_1", BindingId: "bind_1", BindingGeneration: 1, PreparationAttemptId: "prep_1", DeleteCleanupId: "delcln_1", Reason: "delete", IdempotencyKey: "delete_mixed_identity"},
		{WorkspaceId: "default", SessionId: "sesn_1", SandboxId: "sandbox_1", PreparationAttemptId: "prep_1", Reason: "delete", IdempotencyKey: "delete_partial_identity"},
	} {
		if _, err := server.ReleaseSandbox(context.Background(), request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ReleaseSandbox(%s) status = %v; want InvalidArgument", request.GetIdempotencyKey(), status.Code(err))
		}
	}
	response, err := server.ReleaseSandbox(context.Background(), &tetralsandboxv1.ReleaseSandboxRequest{
		WorkspaceId: "default", SessionId: "sesn_1", SandboxId: "sandbox_1",
		PreparationAttemptId: "prep_1", DeleteCleanupId: "delcln_1", Reason: "delete", IdempotencyKey: "delete_valid_identity",
	})
	if err != nil || response.GetStatus() != tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_RELEASED {
		t.Fatalf("valid preparation identity response=%#v err=%v", response, err)
	}
}

func TestServerReleaseSandboxMapsHandlerStatus(t *testing.T) {
	server := &Server{Handler: staticReleaseHandler{
		result: ReleaseSandboxResult{Status: ReleaseSandboxStatusAlreadyReleased, SandboxStatus: "released"},
	}}

	response, err := server.ReleaseSandbox(context.Background(), &tetralsandboxv1.ReleaseSandboxRequest{
		WorkspaceId:       "default",
		SessionId:         "sesn_1",
		SandboxId:         "sandbox_1",
		BindingId:         "rtbind_1",
		BindingGeneration: 1,
		Reason:            "cleanup",
		IdempotencyKey:    "cleanup_session:cleanup_1",
	})
	if err != nil {
		t.Fatalf("ReleaseSandbox: %v", err)
	}
	if response.GetStatus() != tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_ALREADY_RELEASED ||
		response.GetSandboxStatus() != "released" {
		t.Fatalf("response = %#v; want already_released/released", response)
	}
}

func TestServerReleaseSandboxNormalizesHandlerSandboxStatus(t *testing.T) {
	server := &Server{Handler: staticReleaseHandler{
		result: ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "legacy_unknown"},
	}}

	response, err := server.ReleaseSandbox(context.Background(), &tetralsandboxv1.ReleaseSandboxRequest{
		WorkspaceId:       "default",
		SessionId:         "sesn_1",
		SandboxId:         "sandbox_1",
		BindingId:         "rtbind_1",
		BindingGeneration: 1,
		Reason:            "cleanup",
		IdempotencyKey:    "cleanup_session:cleanup_1",
	})
	if err != nil {
		t.Fatalf("ReleaseSandbox: %v", err)
	}
	if response.GetSandboxStatus() != "failed" {
		t.Fatalf("sandbox_status = %q; want failed", response.GetSandboxStatus())
	}
}

type staticReleaseHandler struct {
	result ReleaseSandboxResult
}

func (h staticReleaseHandler) ReleaseSandbox(context.Context, ReleaseSandboxRequest) (ReleaseSandboxResult, error) {
	if h.result.Status == "" {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusReleased, SandboxStatus: "released"}, nil
	}
	return h.result, nil
}
