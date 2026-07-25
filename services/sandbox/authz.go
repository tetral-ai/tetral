package tetralsandbox

import (
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	tetralsandboxv1 "github.com/tetral-ai/tetral/services/sandbox/gen/tetral/sandbox/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const bridgeServiceAccount = "tetral-system/bridge"

func SandboxServiceMethodAuthorizer(identity auth.Identity, method string) error {
	if identity.ServiceAccount.String() == bridgeServiceAccount &&
		method == tetralsandboxv1.SandboxService_ReleaseSandbox_FullMethodName {
		return nil
	}
	return status.Error(codes.PermissionDenied, "method not allowed")
}
