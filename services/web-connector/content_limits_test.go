package webconnector

import (
	"encoding/json"
	"os"
	"testing"
)

func TestWebVisibleOutputMatchesCanonicalToolOutputPolicy(t *testing.T) {
	body, err := os.ReadFile("../gateway/packages/protocol/src/content-limits.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy map[string]int
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatal(err)
	}
	limit, exists := policy["MaxProviderRequestToolOutputJsonBytes"]
	if !exists || limit != maxModelVisibleToolOutputJSONBytes {
		t.Fatalf("Web tool-output JSON cap = %d; canonical policy = %d (present %v)", maxModelVisibleToolOutputJSONBytes, limit, exists)
	}
	// Actual worst-case JSON escaping, including its visible envelope, must fit
	// the provider-visible result budget. This is a raw-text derivation, not an
	// alias between the Web RPC fuse and the unrelated tool JSON policy.
	text := make([]byte, maxVisibleResultBytes)
	encoded, err := json.Marshal(map[string]string{"text": string(text)})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > limit {
		t.Fatalf("escape-dense visible text envelope = %d; limit %d", len(encoded), limit)
	}
	text = append(text, 0)
	encoded, err = json.Marshal(map[string]string{"text": string(text)})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= limit {
		t.Fatal("raw-text derivation leaves room for another worst-case escaped byte")
	}
}
