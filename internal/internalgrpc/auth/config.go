package auth

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/tetral-ai/tetral/internal/workload"
)

const Audience = "tetral-internal-grpc"

const (
	EnvKubernetesAPIServerURL                 = "KUBERNETES_API_SERVER_URL"
	EnvKubernetesAPICACertPath                = "KUBERNETES_API_CA_CERT_PATH"
	EnvKubernetesTokenReviewReviewerTokenPath = "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH" //nolint:gosec // env-var name, not a credential value.
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	Audience               string
	AllowedServiceAccounts []ServiceAccount
}

type ServiceAccount struct {
	Namespace string
	Name      string
}

func (s ServiceAccount) String() string {
	if s.Namespace == "" || s.Name == "" {
		return ""
	}
	return s.Namespace + "/" + s.Name
}

func LoadConfig(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment is required")
	}
	audience := strings.TrimSpace(env.Getenv("TETRAL_INTERNAL_GRPC_AUDIENCE"))
	if audience != Audience {
		return Config{}, workload.NewConfigError(fmt.Sprintf("TETRAL_INTERNAL_GRPC_AUDIENCE must be %s", Audience))
	}
	allowed, err := parseAllowedServiceAccounts(env.Getenv("TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS"))
	if err != nil {
		return Config{}, err
	}
	return Config{Audience: audience, AllowedServiceAccounts: allowed}, nil
}

func parseAllowedServiceAccounts(raw string) ([]ServiceAccount, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, workload.NewConfigError("TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS is required")
	}
	parts := strings.Split(raw, ",")
	accounts := make([]ServiceAccount, 0, len(parts))
	for _, part := range parts {
		account, err := ParseServiceAccount(part)
		if err != nil {
			return nil, workload.NewConfigError("TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS contains invalid service account")
		}
		accounts = append(accounts, account)
	}
	return accounts, nil
}

// ParseServiceAccount parses a namespace/name ServiceAccount identity and validates both
// halves as Kubernetes DNS-1123 labels (lowercase alphanumerics and '-', length 1-63,
// alphanumeric at both ends). Malformed identities are rejected so deployment typos fail
// config load (= startup) rather than producing a process that rejects every real
// TokenReview identity.
func ParseServiceAccount(raw string) (ServiceAccount, error) {
	value := strings.TrimSpace(raw)
	if value == "" || strings.Contains(value, "*") {
		return ServiceAccount{}, fmt.Errorf("service account must be namespace/name")
	}
	namespace, name, ok := strings.Cut(value, "/")
	if !ok || namespace == "" || name == "" || strings.Contains(name, "/") {
		return ServiceAccount{}, fmt.Errorf("service account must be namespace/name")
	}
	if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
		return ServiceAccount{}, fmt.Errorf("service account namespace is not a DNS-1123 label")
	}
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return ServiceAccount{}, fmt.Errorf("service account name is not a DNS-1123 label")
	}
	return ServiceAccount{Namespace: namespace, Name: name}, nil
}
