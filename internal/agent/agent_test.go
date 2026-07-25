package agent

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func stringPointerForTest(value string) *string {
	return &value
}

func TestAgentConfigJSONIncludesTargetKeys(t *testing.T) {
	cfg := AgentConfig{
		Name:        "demo",
		Model:       "anthropic/claude-opus-4-8",
		System:      stringPointerForTest("you are a bot"),
		Description: stringPointerForTest("desc"),
	}.Normalize()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal AgentConfig: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal back: %v", err)
	}

	for _, key := range []string{"name", "model", "system", "description", "tools", "mcp_servers", "skills", "metadata"} {
		if _, ok := got[key]; !ok {
			t.Errorf("AgentConfig JSON missing target key %q; got %s", key, string(data))
		}
	}
	for _, key := range []string{"id", "type", "version", "created_at", "updated_at", "config"} {
		if _, ok := got[key]; ok {
			t.Errorf("AgentConfig JSON must not contain key %q; got %s", key, string(data))
		}
	}
}

func TestAgentConfigNullableStringsAndEmptyContainersRenderCanonically(t *testing.T) {
	cfg := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal AgentConfig: %v", err)
	}
	body := string(data)
	for _, fragment := range []string{`"system":null`, `"description":null`, `"tools":[]`, `"mcp_servers":[]`, `"skills":[]`, `"metadata":{}`} {
		if !strings.Contains(body, fragment) {
			t.Errorf("AgentConfig JSON should contain %q; got %s", fragment, body)
		}
	}
	for _, fragment := range []string{`"tools":null`, `"mcp_servers":null`, `"skills":null`, `"metadata":null`} {
		if strings.Contains(body, fragment) {
			t.Errorf("AgentConfig JSON must not contain %q; got %s", fragment, body)
		}
	}
}

func TestAgentJSONIsFlatWithArchiveAndNoNestedConfig(t *testing.T) {
	a := Agent{
		ID:      "agent_1",
		Type:    "agent",
		Version: 1,
		AgentConfig: AgentConfig{
			Name:  "demo",
			Model: "x",
		}.Normalize(),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal Agent: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal back: %v", err)
	}
	if _, ok := got["config"]; ok {
		t.Errorf("Agent JSON must not contain nested config wrapper; got %s", string(data))
	}
	for _, key := range []string{"id", "type", "version", "name", "model", "system", "description", "tools", "mcp_servers", "skills", "metadata", "created_at", "updated_at", "archived_at"} {
		if _, ok := got[key]; !ok {
			t.Errorf("Agent JSON missing top-level key %q; got %s", key, string(data))
		}
	}
}

func TestAgentConfigNormalizeFillsNilContainers(t *testing.T) {
	cfg := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}
	if cfg.Tools != nil || cfg.MCPServers != nil || cfg.Skills != nil || cfg.Metadata != nil {
		t.Fatalf("test precondition: containers should be nil")
	}
	normalized := cfg.Normalize()
	if normalized.Tools == nil || normalized.MCPServers == nil || normalized.Skills == nil {
		t.Errorf("Normalize: array containers must be non-nil; got %+v", normalized)
	}
	if normalized.Metadata == nil {
		t.Errorf("Normalize: metadata must be non-nil; got %+v", normalized)
	}
}

func TestAgentConfigJSONRoundtripPreservesContent(t *testing.T) {
	cfg := AgentConfig{
		Name:        "x",
		Model:       "anthropic/claude-opus-4-8",
		System:      stringPointerForTest("system"),
		Description: stringPointerForTest("description"),
		Tools:       RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		MCPServers:  RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)},
		Skills:      RawArray{json.RawMessage(`{"type":"custom","skill_id":"skill_1","version":"latest"}`)},
		Metadata:    StringMap{"team": "core", "env": ""},
	}.Normalize()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back AgentConfig
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.System == nil || *back.System != "system" || back.Description == nil || *back.Description != "description" {
		t.Errorf("nullable strings lost content: %+v", back)
	}
	if len(back.Tools) != 1 || len(back.MCPServers) != 1 || len(back.Skills) != 1 {
		t.Errorf("array roundtrip lost content: tools=%v mcp=%v skills=%v", back.Tools, back.MCPServers, back.Skills)
	}
	if back.Metadata["team"] != "core" || back.Metadata["env"] != "" {
		t.Errorf("metadata roundtrip lost content: %v", back.Metadata)
	}
}

func TestAgentConfigUnmarshalIgnoresStoredModelVariant(t *testing.T) {
	var cfg AgentConfig
	err := json.Unmarshal([]byte(`{
		"name":"x",
		"model":"openai/gpt-5.5",
		"model_variant":"fast",
		"approval_mode":"ask_for_approval",
		"system":null,
		"description":null,
		"tools":[],
		"mcp_servers":[],
		"skills":[],
		"metadata":{}
	}`), &cfg)
	if err != nil {
		t.Fatalf("unmarshal stored AgentConfig: %v", err)
	}
	if cfg.Model != "openai/gpt-5.5" {
		t.Fatalf("Model = %q; want canonical model id", cfg.Model)
	}
	if cfg.ModelVariant != "" {
		t.Fatalf("ModelVariant = %q; want old stored model_variant ignored", cfg.ModelVariant)
	}
}

func TestAgentConfigNormalizeIsDeepEqualAfterRoundtrip(t *testing.T) {
	original := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var roundtrip AgentConfig
	if err := json.Unmarshal(data, &roundtrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	roundtrip = roundtrip.Normalize()

	if !reflect.DeepEqual(original, roundtrip) {
		t.Errorf("Normalize+Marshal+Unmarshal+Normalize must be DeepEqual.\noriginal:  %+v\nroundtrip: %+v", original, roundtrip)
	}
}
