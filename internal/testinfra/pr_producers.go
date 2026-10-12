package testinfra

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
)

//go:embed pr_producers.json
var prProducersJSON []byte

// PRProducerInventory lists the results Merge Gate requires. GoShards names
// the producers that run the Go selection's shards, which Merge Gate checks
// together as one partition.
type PRProducerInventory struct {
	Producers []string
	GoShards  []string
}

func LoadPRProducers() (PRProducerInventory, error) {
	var document struct {
		Version   int      `json:"version"`
		Producers []string `json:"producers"`
		GoShards  []string `json:"go_shards"`
	}
	if err := json.Unmarshal(prProducersJSON, &document); err != nil {
		return PRProducerInventory{}, fmt.Errorf("decode PR producer inventory: %w", err)
	}
	if document.Version != 1 || len(document.Producers) == 0 || len(document.GoShards) == 0 {
		return PRProducerInventory{}, fmt.Errorf("unsupported or empty PR producer inventory")
	}
	seen := map[string]bool{}
	for _, producer := range document.Producers {
		if producer == "" || seen[producer] {
			return PRProducerInventory{}, fmt.Errorf("invalid PR producer %q", producer)
		}
		seen[producer] = true
	}
	for index, shard := range document.GoShards {
		if !seen[shard] || slices.Contains(document.GoShards[:index], shard) {
			return PRProducerInventory{}, fmt.Errorf("go shard %q is not one PR producer", shard)
		}
	}
	return PRProducerInventory{Producers: slices.Clone(document.Producers), GoShards: slices.Clone(document.GoShards)}, nil
}

// PRProducerJobs maps each producer to the only job Merge Gate accepts its
// result from.
func PRProducerJobs(inventory PRProducerInventory) map[string]string {
	jobs := map[string]string{
		"repository": "repository-sandbox-security-runtime", "sandbox-image": "repository-sandbox-security-runtime",
		"security": "repository-sandbox-security-runtime", "runtime": "repository-sandbox-security-runtime",
		"go-static": "go-static-analysis", "gateway": "provider-gateway",
		"deployment": "deployment-protocol", "protocol": "deployment-protocol",
	}
	for _, shard := range inventory.GoShards {
		jobs[shard] = "go-race"
	}
	return jobs
}
