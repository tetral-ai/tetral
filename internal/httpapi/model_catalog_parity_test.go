// Layer 1: the public catalog against the Gateway's catalog, read by Bun as a
// tool.
package httpapi

import (
	"encoding/json"
	"maps"
	"os/exec"
	"path/filepath"
	"testing"
)

// The public catalog and the Gateway's routing catalog are written apart and
// must agree: the API tells a caller a model's limits and the Gateway enforces
// them. Both are compared as their code evaluates them.
//
// Only base limits are compared. A model reached through a caller's own OAuth
// credential can have a smaller window than on a platform key, and /v1/models
// has no route dimension to say so.
func TestModelCatalogLimitsMatchTheGateway(t *testing.T) {
	command := exec.Command("bun", "-e", `
import { GatewayModelCatalog } from "./packages/provider-gateway/src/providers/catalog.ts";
console.log(JSON.stringify(Object.fromEntries(GatewayModelCatalog.map((entry) =>
  [entry.providerId + "/" + entry.modelId, [entry.modelContextWindowTokens, entry.modelOutputTokenLimit]]))));`)
	command.Dir = filepath.Join("..", "..", "services", "gateway")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("evaluate the Gateway catalog: %v", err)
	}
	var gateway map[string][2]int
	if err := json.Unmarshal(output, &gateway); err != nil {
		t.Fatalf("decode the Gateway catalog %q: %v", output, err)
	}
	served := map[string][2]int{}
	for _, entry := range currentModelCatalog() {
		served[entry.ID] = [2]int{entry.MaxInputTokens, entry.MaxTokens}
	}
	if len(served) == 0 || !maps.Equal(served, gateway) {
		t.Fatalf("/v1/models serves [input, output] limits %v; the Gateway routes %v", served, gateway)
	}
}
