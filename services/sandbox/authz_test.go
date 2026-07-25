package tetralsandbox

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	tetralsandboxv1 "github.com/tetral-ai/tetral/services/sandbox/gen/tetral/sandbox/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSandboxServiceMethodAuthorizerScopesReleaseSandboxToBridge(t *testing.T) {
	bridge := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "bridge"}}
	if err := SandboxServiceMethodAuthorizer(bridge, tetralsandboxv1.SandboxService_ReleaseSandbox_FullMethodName); err != nil {
		t.Fatalf("bridge ReleaseSandbox authorization error = %v; want nil", err)
	}
	if err := SandboxServiceMethodAuthorizer(bridge, "/grpc.health.v1.Health/Check"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("bridge health authorization error = %v; want PermissionDenied", err)
	}

	runtimePod := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}}
	if err := SandboxServiceMethodAuthorizer(runtimePod, tetralsandboxv1.SandboxService_ReleaseSandbox_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("runtime pod ReleaseSandbox authorization error = %v; want PermissionDenied", err)
	}
}
