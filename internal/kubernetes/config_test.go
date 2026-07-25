package kubernetes

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

func TestKubernetesConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := LoadConfig(envMap{
		"TETRAL_KUBERNETES_NAMESPACE": "tetral-runtime",
	})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Namespace != "tetral-runtime" {
		t.Fatalf("Namespace = %q", cfg.Namespace)
	}
	if cfg.AgentRuntimeServiceName != "agent-runtime" {
		t.Fatalf("AgentRuntimeServiceName = %q; want default agent-runtime", cfg.AgentRuntimeServiceName)
	}
	if cfg.AgentRuntimeLabelSelector.String() != "app.kubernetes.io/name=agent-runtime" {
		t.Fatalf("AgentRuntimeLabelSelector = %q", cfg.AgentRuntimeLabelSelector.String())
	}
}

func TestKubernetesConfigRejectsInvalidValues(t *testing.T) {
	valid := envMap{
		"TETRAL_KUBERNETES_NAMESPACE": "tetral-runtime",
	}
	for _, test := range []struct {
		name string
		key  string
		val  string
	}{
		{name: "namespace", key: "TETRAL_KUBERNETES_NAMESPACE", val: ""},
		{name: "selector", key: "TETRAL_AGENT_RUNTIME_LABEL_SELECTOR", val: "not in"},
		{name: "service", key: "TETRAL_AGENT_RUNTIME_SERVICE_NAME", val: "UPPER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := cloneEnv(valid)
			env[test.key] = test.val
			_, err := LoadConfig(env)
			if err == nil {
				t.Fatalf("LoadConfig accepted invalid %s", test.key)
			}
			configErr, ok := workload.AsConfigError(err)
			if !ok {
				t.Fatalf("LoadConfig invalid %s returned %T; want a workload.ConfigError", test.key, err)
			}
			if configErr.Error() == "" {
				t.Fatalf("LoadConfig invalid %s returned an empty ConfigError message", test.key)
			}
		})
	}
}

type envMap map[string]string

func (m envMap) Getenv(key string) string { return m[key] }

func cloneEnv(in envMap) envMap {
	out := envMap{}
	for key, value := range in {
		out[key] = value
	}
	return out
}
