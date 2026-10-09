package agentruntimebridge

import (
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

type configTestEnv map[string]string

func (e configTestEnv) Getenv(key string) string {
	return e[key]
}

func TestBridgeAPIConfigRequiresMCPConnectorRoute(t *testing.T) {
	env := configTestEnv{
		EnvBridgeMCPConnectorGRPCAddr: "gateway.tetral-system.svc.cluster.local:9091",
		EnvBridgeGatewayTokenPath:     "/var/run/secrets/tetral-internal-grpc/gateway/token",
	}
	cfg, err := BridgeAPIConfigFromEnv(env)
	if err != nil {
		t.Fatalf("BridgeAPIConfigFromEnv: %v", err)
	}
	if cfg.MCPConnectorGRPCAddress != env[EnvBridgeMCPConnectorGRPCAddr] || cfg.GatewayTokenPath != env[EnvBridgeGatewayTokenPath] {
		t.Fatalf("BridgeAPIConfigFromEnv = %#v; want env projection", cfg)
	}
	if cfg.ProviderRescheduleBudget != 3 || cfg.CompactionRescheduleBudget != 2 {
		t.Fatalf("BridgeAPIConfigFromEnv retry budgets = %d/%d; want 3/2", cfg.ProviderRescheduleBudget, cfg.CompactionRescheduleBudget)
	}

	for _, missing := range []string{EnvBridgeMCPConnectorGRPCAddr, EnvBridgeGatewayTokenPath} {
		t.Run(missing, func(t *testing.T) {
			missingEnv := configTestEnv{
				EnvBridgeMCPConnectorGRPCAddr: env[EnvBridgeMCPConnectorGRPCAddr],
				EnvBridgeGatewayTokenPath:     env[EnvBridgeGatewayTokenPath],
			}
			delete(missingEnv, missing)
			if _, err := BridgeAPIConfigFromEnv(missingEnv); err == nil || !strings.Contains(err.Error(), missing+" is required") {
				t.Fatalf("BridgeAPIConfigFromEnv missing %s error = %v; want required validation", missing, err)
			}
		})
	}

	for _, test := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "provider negative", key: EnvProviderRescheduleBudget, value: "-1"},
		{name: "provider too large", key: EnvProviderRescheduleBudget, value: "11"},
		{name: "compaction malformed", key: EnvCompactionRescheduleBudget, value: "two"},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := configTestEnv{
				EnvBridgeMCPConnectorGRPCAddr: env[EnvBridgeMCPConnectorGRPCAddr],
				EnvBridgeGatewayTokenPath:     env[EnvBridgeGatewayTokenPath],
				test.key:                      test.value,
			}
			if _, err := BridgeAPIConfigFromEnv(invalid); err == nil || !strings.Contains(err.Error(), test.key+" must be an integer between 0 and 10") {
				t.Fatalf("BridgeAPIConfigFromEnv %s error = %v; want bounded integer validation", test.key, err)
			}
		})
	}
}

func TestBridgeAPIConfigRequiresTwoDatabaseConnections(t *testing.T) {
	for _, test := range []struct {
		name    string
		maxOpen string
		wantErr string
	}{
		{name: "unchanged default"},
		{name: "minimum", maxOpen: "2"},
		{name: "listener would consume only connection", maxOpen: "1", wantErr: dbconnect.EnvDBMaxOpenConns + " must be at least 2"},
		{name: "invalid pool config", maxOpen: "invalid", wantErr: "database pool config invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := configTestEnv{
				EnvBridgeMCPConnectorGRPCAddr: "gateway.tetral-system.svc.cluster.local:9091",
				EnvBridgeGatewayTokenPath:     "/var/run/secrets/tetral-internal-grpc/gateway/token",
				dbconnect.EnvDBMaxOpenConns:   test.maxOpen,
			}
			_, err := BridgeAPIConfigFromEnv(env)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("BridgeAPIConfigFromEnv max open %q error = %v; want %q", test.maxOpen, err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("BridgeAPIConfigFromEnv max open %q: %v", test.maxOpen, err)
			}
		})
	}
}
