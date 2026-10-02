package webconnector

import (
	"testing"
	"time"

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

func TestDrainBudgetLeavesRoomForJoinAndProxy(t *testing.T) {
	for _, raw := range []string{"", "200", "20000", "0", "20001", "-1", "unbounded"} {
		env := mapEnv{EnvAPIKeys: `["fixture"]`, EnvBindingHMACKey: "binding-verifier-key-with-at-least-32-bytes", EnvDrainTimeout: raw}
		cfg, err := LoadConfig(env)
		switch raw {
		case "":
			if err != nil || cfg.DrainTimeout != 10*time.Second {
				t.Fatalf("default drain=%v/%v", cfg.DrainTimeout, err)
			}
		case "200", "20000":
			if err != nil || cfg.DrainTimeout+10*time.Second > 30*time.Second {
				t.Fatalf("drain/join/proxy exceeds Pod budget: %v/%v", cfg.DrainTimeout, err)
			}
		default:
			if err == nil {
				t.Fatalf("unbounded/invalid drain accepted: %q", raw)
			}
		}
	}
}

func TestWebLifecycleConfigFitsPodApplicationAllocation(t *testing.T) {
	for _, tc := range []struct {
		drain, join string
		valid       bool
	}{
		{"2000", "3000", true}, {"20000", "5000", true}, {"20000", "5001", false}, {"2000", "0", false}, {"2000", "9223372036854775807", false},
	} {
		cfg, err := LoadConfig(mapEnv{"TETRAL_WEB_API_KEYS": `["fixture"]`, "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY": "binding-verifier-key-with-at-least-32-bytes", EnvDrainTimeout: tc.drain, "TETRAL_CANCEL_JOIN_TIMEOUT_MS": tc.join})
		if (err == nil) != tc.valid {
			t.Fatalf("%+v error=%v", tc, err)
		}
		if tc.valid && tc.join == "3000" && cfg.CancelJoinTimeout != 3*time.Second {
			t.Fatal("join configuration lost")
		}
	}
}
