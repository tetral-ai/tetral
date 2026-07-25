package webconnector

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

func MethodAuthorizer(identity grpcauth.Identity, method string) error {
	account := identity.ServiceAccount
	if account.Namespace != "tetral-agent-runtime" || account.Name != "agent-runtime" || identity.KubernetesPodUID == "" {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	if method != providergatewayv1.ProviderGatewayService_RunWeb_FullMethodName && method != providergatewayv1.ProviderGatewayService_StreamProviderRequest_FullMethodName {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return nil
}
