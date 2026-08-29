package testinfra

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed pr_producers.json
var prProducersJSON []byte

func LoadPRProducers() ([]string, error) {
	var document struct {
		Version   int      `json:"version"`
		Producers []string `json:"producers"`
	}
	if err := json.Unmarshal(prProducersJSON, &document); err != nil {
		return nil, fmt.Errorf("decode PR producer inventory: %w", err)
	}
	if document.Version != 1 || len(document.Producers) == 0 {
		return nil, fmt.Errorf("unsupported or empty PR producer inventory")
	}
	seen := map[string]bool{}
	for _, producer := range document.Producers {
		if producer == "" || seen[producer] {
			return nil, fmt.Errorf("invalid PR producer %q", producer)
		}
		seen[producer] = true
	}
	return append([]string(nil), document.Producers...), nil
}

func PRProducerJobs() map[string]string {
	return map[string]string{
		"repository": "repository-integrity", "go-static": "go-static-analysis",
		"go-0": "go-race", "go-1": "go-race", "go-2": "go-race", "go-3": "go-race",
		"runtime": "agent-runtime", "gateway": "provider-gateway", "protocol": "protocol-sdk",
		"deployment": "deployment-definitions", "sandbox-image": "sandbox-image", "security": "dependency-security",
	}
}
