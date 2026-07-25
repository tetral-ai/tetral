package webconnector

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

func TestLoadConfigAppliesPinnedAddressesAndEndpoints(t *testing.T) {
	t.Parallel()
	env := mapEnv{"TETRAL_WEB_API_KEYS": `["first","second"]`, "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "binding-verifier-key-with-at-least-32-bytes"}
	cfg, err := LoadConfig(env)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddress != "0.0.0.0:9092" || cfg.MetricsAddress != "0.0.0.0:9464" {
		t.Fatalf("addresses = %q %q", cfg.GRPCAddress, cfg.MetricsAddress)
	}
	if cfg.SearchEndpoint != "https://s.jina.ai/" || cfg.ReaderEndpoint != "https://r.jina.ai/" {
		t.Fatalf("endpoints = %q %q", cfg.SearchEndpoint, cfg.ReaderEndpoint)
	}
	if len(cfg.APIKeys) != 2 || cfg.APIKeys[0] != "first" {
		t.Fatalf("keys = %#v", cfg.APIKeys)
	}
}

func TestLoadConfigRejectsMalformedOrEmptyKeyPoolAndShortBindingKey(t *testing.T) {
	t.Parallel()
	for _, env := range []mapEnv{{"TETRAL_WEB_API_KEYS": "[]", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "binding-verifier-key-with-at-least-32-bytes"}, {"TETRAL_WEB_API_KEYS": "not-json", "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "binding-verifier-key-with-at-least-32-bytes"}, {"TETRAL_WEB_API_KEYS": `["key"]`, "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "short"}} {
		if _, err := LoadConfig(env); err == nil {
			t.Fatalf("LoadConfig(%v) succeeded", env)
		}
	}
}

func TestMethodAuthorizerAdmitsOnlyRuntimeServiceAccountToProviderMethods(t *testing.T) {
	t.Parallel()
	runtime := grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "pod"}
	if err := MethodAuthorizer(runtime, providergatewayv1.ProviderGatewayService_RunWeb_FullMethodName); err != nil {
		t.Fatal(err)
	}
	other := grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-system", Name: "gateway"}, KubernetesPodUID: "pod"}
	if code := status.Code(MethodAuthorizer(other, providergatewayv1.ProviderGatewayService_RunWeb_FullMethodName)); code != codes.PermissionDenied {
		t.Fatalf("code = %s", code)
	}
}

type mapEnv map[string]string

func (e mapEnv) Getenv(key string) string { return e[key] }
