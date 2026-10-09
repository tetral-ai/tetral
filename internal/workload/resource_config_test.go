package workload

import "testing"

func TestResourceMetadataDefaultsAndBlankPolicies(t *testing.T) {
	cfg := ResourceConfigFromEnv(func(string) string { return "" })
	if cfg.DeploymentEnvironment != "local" || cfg.ServiceVersion != "unknown" {
		t.Fatalf("defaults: %+v", cfg)
	}
	values := map[string]string{EnvDeploymentEnvironment: "prod-west", EnvServiceVersion: "revision-42"}
	cfg = ResourceConfigFromEnv(func(key string) string { return values[key] })
	if cfg.DeploymentEnvironment != "prod-west" || cfg.ServiceVersion != "revision-42" {
		t.Fatalf("overrides: %+v", cfg)
	}
	exact := ResourceConfigFromEnv(func(string) string { return " " })
	trimmed := ResourceConfigFromEnvWithTrimPolicy(func(string) string { return " " }, true)
	if exact.DeploymentEnvironment != " " || exact.ServiceVersion != " " || trimmed.DeploymentEnvironment != "local" || trimmed.ServiceVersion != "unknown" {
		t.Fatal("established blank policies changed")
	}
}

func TestResourceMetadataTrimPolicyPreservesDistinctNonblankValues(t *testing.T) {
	values := map[string]string{EnvDeploymentEnvironment: " production ", EnvServiceVersion: " release-1 "}
	exact := ResourceConfigFromEnv(func(key string) string { return values[key] })
	trimmed := ResourceConfigFromEnvWithTrimPolicy(func(key string) string { return values[key] }, true)
	if exact.DeploymentEnvironment != " production " || exact.ServiceVersion != " release-1 " || trimmed.DeploymentEnvironment != "production" || trimmed.ServiceVersion != "release-1" {
		t.Fatalf("nonblank metadata policies: exact=%+v trimmed=%+v", exact, trimmed)
	}
	if defaults := ResourceConfigFromEnvWithTrimPolicy(nil, true); defaults.DeploymentEnvironment != "local" || defaults.ServiceVersion != "unknown" {
		t.Fatalf("nil-env defaults: %+v", defaults)
	}
}
