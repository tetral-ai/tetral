package kubernetes

import (
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	defaultAgentRuntimeLabelSelector = "app.kubernetes.io/name=agent-runtime"
	defaultAgentRuntimeServiceName   = "agent-runtime"
)

// Agent Runtime Pod visibility config owns only Kubernetes namespace, selector,
// and service identity. Dispatcher does not own idle or expiry lifecycle state.
// Internal gRPC audience and the ServiceAccount allowlist live solely in
// internal/internalgrpc/auth; this visibility config owns only Kubernetes visibility keys.

type Env interface {
	Getenv(string) string
}

type Config struct {
	Namespace                 string
	AgentRuntimeLabelSelector labels.Selector
	AgentRuntimeServiceName   string
}

func LoadConfig(env Env) (Config, error) {
	namespace := strings.TrimSpace(env.Getenv("TETRAL_KUBERNETES_NAMESPACE"))
	if namespace == "" {
		return Config{}, workload.NewConfigError("TETRAL_KUBERNETES_NAMESPACE is required")
	}
	if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
		return Config{}, workload.NewConfigError("TETRAL_KUBERNETES_NAMESPACE has invalid shape")
	}
	rawSelector := env.Getenv("TETRAL_AGENT_RUNTIME_LABEL_SELECTOR")
	if rawSelector == "" {
		rawSelector = defaultAgentRuntimeLabelSelector
	}
	selector, err := labels.Parse(rawSelector)
	if err != nil {
		return Config{}, workload.NewConfigError("TETRAL_AGENT_RUNTIME_LABEL_SELECTOR has invalid shape")
	}
	serviceName := strings.TrimSpace(env.Getenv("TETRAL_AGENT_RUNTIME_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = defaultAgentRuntimeServiceName
	}
	if errs := validation.IsDNS1035Label(serviceName); len(errs) > 0 {
		return Config{}, workload.NewConfigError("TETRAL_AGENT_RUNTIME_SERVICE_NAME has invalid shape")
	}
	return Config{
		Namespace:                 namespace,
		AgentRuntimeLabelSelector: selector,
		AgentRuntimeServiceName:   serviceName,
	}, nil
}
