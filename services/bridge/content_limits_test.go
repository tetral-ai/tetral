package agentruntimebridge

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"
)

// These are shared provider-content limits at the real Bridge declaration
// validators. Tool-result output, attachment totals and Bridge RPC capacity
// describe different boundaries and intentionally have separate policies.
func TestBridgeContentLimitsMatchCanonicalProviderPolicy(t *testing.T) {
	body, err := os.ReadFile("../gateway/packages/protocol/src/content-limits.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy map[string]int
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatal(err)
	}
	projections := map[string]int{
		"MaxProviderContextTextJsonBytes":   runtimecontrol.RuntimeContextTextJSONMaxBytes,
		"MaxProviderToolCallInputJsonBytes": runtimecontrol.RuntimeToolInputJSONMaxBytes,
		"MaxMetadataBytes":                  runtimecontrol.RuntimeProviderMetadataMaxBytes,
		"MaxStableReasoningPartsPerRequest": MaxStableReasoningPartsPerRequest,
		"MaxStableReasoningBytesPerRequest": MaxStableReasoningBytesPerRequest,
	}
	projections["MaxProviderRequestToolOutputJsonBytes"] = runtimecontrol.RuntimeToolOutputJSONMaxBytes
	projections["MaxProviderRequestAttachments"] = MaxProviderRequestAttachments
	for name, value := range projections {
		if canonical, exists := policy[name]; !exists || canonical != value {
			t.Errorf("Bridge %s = %d; canonical provider-content policy = %d (present %v)", name, value, canonical, exists)
		}
	}
}
