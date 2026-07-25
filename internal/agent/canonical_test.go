package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonicalizeProducesDeterministicBytesForEqualConfigs(t *testing.T) {
	a := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}
	b := AgentConfig{
		Name:       "x",
		Model:      "anthropic/claude-opus-4-8",
		Tools:      RawArray{},
		MCPServers: RawArray{},
		Skills:     RawArray{},
		Metadata:   StringMap{},
	}
	bytesA, hashA, err := Canonicalize(a)
	if err != nil {
		t.Fatalf("Canonicalize(a): %v", err)
	}
	bytesB, hashB, err := Canonicalize(b)
	if err != nil {
		t.Fatalf("Canonicalize(b): %v", err)
	}
	if string(bytesA) != string(bytesB) {
		t.Errorf("canonical bytes differ for equivalent configs:\n  a=%s\n  b=%s", bytesA, bytesB)
	}
	if hashA != hashB {
		t.Errorf("canonical hashes differ: a=%s b=%s", hashA, hashB)
	}
}

func TestCanonicalizeHashIsLowercaseHexSHA256(t *testing.T) {
	cfg := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	bytes, hash, err := Canonicalize(cfg)
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	want := sha256.Sum256(bytes)
	if hash != hex.EncodeToString(want[:]) {
		t.Errorf("hash = %q; want lowercase hex SHA-256 of canonical bytes", hash)
	}
}

func TestCanonicalizeOutputIsValidJSONAndRoundTripsContent(t *testing.T) {
	cfg := AgentConfig{
		Name:     "x",
		Model:    "anthropic/claude-opus-4-8",
		Metadata: StringMap{"team": "core"},
	}.Normalize()
	bytes, _, err := Canonicalize(cfg)
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	var back AgentConfig
	if err := json.Unmarshal(bytes, &back); err != nil {
		t.Fatalf("unmarshal canonical bytes: %v", err)
	}
	if back.Name != "x" || back.Metadata["team"] != "core" {
		t.Errorf("roundtrip lost content: %+v", back)
	}
}

func TestCanonicalizeNullsAndEmptyContainers(t *testing.T) {
	bytes, _, err := Canonicalize(AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"})
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	for _, fragment := range []string{`"system":null`, `"description":null`, `"tools":[]`, `"mcp_servers":[]`, `"skills":[]`, `"metadata":{}`} {
		if !strings.Contains(string(bytes), fragment) {
			t.Errorf("canonical bytes missing %q: %s", fragment, string(bytes))
		}
	}
}
