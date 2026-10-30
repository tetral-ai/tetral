package agentruntimebridge

import (
	"strings"
	"testing"
	"time"

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
	if cfg.ProviderRescheduleBudget != 3 || cfg.CompactionRescheduleBudget != 2 || cfg.AttachmentLifetime != time.Hour {
		t.Fatalf("BridgeAPIConfigFromEnv retry budgets = %d/%d, attachment lifetime %s; want 3/2 and 1h", cfg.ProviderRescheduleBudget, cfg.CompactionRescheduleBudget, cfg.AttachmentLifetime)
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
		want  string
	}{
		{name: "provider negative", key: EnvProviderRescheduleBudget, value: "-1", want: " must be an integer between 0 and 10"},
		{name: "provider too large", key: EnvProviderRescheduleBudget, value: "11", want: " must be an integer between 0 and 10"},
		{name: "compaction malformed", key: EnvCompactionRescheduleBudget, value: "two", want: " must be an integer between 0 and 10"},
		{name: "attachment lifetime zero", key: EnvAttachmentLifetimeSeconds, value: "0", want: " must be a positive integer"},
		{name: "attachment lifetime with a unit", key: EnvAttachmentLifetimeSeconds, value: "1h", want: " must be a positive integer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := configTestEnv{
				EnvBridgeMCPConnectorGRPCAddr: env[EnvBridgeMCPConnectorGRPCAddr],
				EnvBridgeGatewayTokenPath:     env[EnvBridgeGatewayTokenPath],
				test.key:                      test.value,
			}
			if _, err := BridgeAPIConfigFromEnv(invalid); err == nil || !strings.Contains(err.Error(), test.key+test.want) {
				t.Fatalf("BridgeAPIConfigFromEnv %s=%s error = %v; want %q", test.key, test.value, err, test.key+test.want)
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
