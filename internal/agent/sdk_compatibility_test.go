package agent

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSDKCompatibilityAgentUpdateMetadataNullClearsAllMetadata(t *testing.T) {
	var patch AgentPatch
	if err := json.Unmarshal([]byte(`{"version":1,"metadata":null}`), &patch); err != nil {
		t.Fatalf("Unmarshal AgentPatch: %v", err)
	}
	if err := patch.Prevalidate(); err != nil {
		t.Fatalf("Prevalidate metadata:null: %v", err)
	}

	materialized, err := patch.Materialize(AgentConfig{
		Name:     "agent",
		Model:    "anthropic/claude-opus-4-8",
		Tools:    RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		Metadata: StringMap{"team": "runtime", "region": "iad"},
	})
	if err != nil {
		t.Fatalf("Materialize metadata:null: %v", err)
	}
	if len(materialized.Metadata) != 0 {
		t.Fatalf("metadata = %#v; want cleared map", materialized.Metadata)
	}
}

// T-COMPAT-AGENT-1
func TestSDKCompatibilityTCOMPATAgent1CreateRequiresOneTetralToolset(t *testing.T) {
	_, err := DecodeCreateAgentRequest([]byte(`{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[]}`))
	assertAgentValidationMessage(t, err, "tools must contain exactly one tetral_agent_toolset entry")
}

// T-COMPAT-AGENT-3
func TestSDKCompatibilityTCOMPATAgent3UpdateNullClearsThenRejectsZeroTetralToolsets(t *testing.T) {
	var patch AgentPatch
	if err := json.Unmarshal([]byte(`{"version":1,"tools":null}`), &patch); err != nil {
		t.Fatalf("Unmarshal AgentPatch: %v", err)
	}
	_, err := patch.Materialize(AgentConfig{
		Name:  "agent",
		Model: "anthropic/claude-opus-4-8",
		Tools: RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
	})
	assertAgentValidationMessage(t, err, "tools must contain exactly one tetral_agent_toolset entry")
}

// T-COMPAT-AGENT-17
func TestSDKCompatibilityTCOMPATAgent17RejectsDuplicateTetralToolsets(t *testing.T) {
	for _, tools := range []string{
		`[{"type":"tetral_agent_toolset","family":"claude"},{"type":"tetral_agent_toolset","family":"claude"}]`,
		`[{"type":"tetral_agent_toolset","family":"claude"},{"type":"tetral_agent_toolset","family":"gpt"}]`,
	} {
		_, err := DecodeCreateAgentRequest([]byte(`{"name":"agent","model":"anthropic/claude-opus-4-8","tools":` + tools + `}`))
		assertAgentValidationMessage(t, err, "tools[1] duplicates the tetral_agent_toolset entry; exactly one is required")
	}
}

func assertAgentValidationMessage(t *testing.T, err error, want string) {
	t.Helper()
	validation, ok := err.(*ValidationError)
	if !ok || validation.Message != want {
		t.Fatalf("error = %#v; want ValidationError %q", err, want)
	}
}

func TestSDKCompatibilityCustomSkillNullVersionCanonicalizesToLatest(t *testing.T) {
	request, err := DecodeCreateAgentRequest([]byte(`{
		"name":"agent",
		"model":"anthropic/claude-opus-4-8",
		"tools":[{"type":"tetral_agent_toolset","family":"claude"}],
		"skills":[{"type":"custom","skill_id":"skill_test","version":null}]
	}`))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest version:null: %v", err)
	}
	if len(request.Skills) != 1 || string(request.Skills[0]) != `{"type":"custom","skill_id":"skill_test","version":"latest"}` {
		t.Fatalf("skills = %s; want version latest", string(mustMarshalAgentSDKJSON(t, request.Skills)))
	}
}

func TestSDKCompatibilityAgentToolsetsProjectResolvedPolicyWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		family string
		names  []string
	}{
		{family: "claude", names: []string{"read", "grep", "glob"}},
		{family: "gpt", names: []string{"view_image"}},
	} {
		t.Run(tc.family, func(t *testing.T) {
			canonical := RawArray{
				json.RawMessage(`{"type":"tetral_agent_toolset","family":"` + tc.family + `"}`),
				json.RawMessage(`{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false},"configs":[{"name":"github_search","permission_policy":{"type":"always_allow"}}]}`),
			}
			original := make(RawArray, len(canonical))
			for index := range canonical {
				original[index] = append(json.RawMessage(nil), canonical[index]...)
			}
			body := mustMarshalAgentSDKJSON(t, Agent{AgentConfig: AgentConfig{Name: "agent", Model: "anthropic/claude-opus-4-8", Tools: canonical}})
			var response struct {
				Tools []map[string]any `json:"tools"`
			}
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("Unmarshal Agent response: %v", err)
			}
			if len(response.Tools) != 2 {
				t.Fatalf("tools count = %d; want 2 (%s)", len(response.Tools), body)
			}
			assertResolvedToolDefault(t, response.Tools[0], true, "always_ask")
			assertResolvedToolConfigs(t, response.Tools[0], tc.names)
			assertResolvedToolDefault(t, response.Tools[1], false, "always_ask")
			assertResolvedToolConfigs(t, response.Tools[1], []string{"github_search"})
			if !reflect.DeepEqual(canonical, original) {
				t.Fatalf("response projection mutated canonical tools:\n got %s\nwant %s", mustMarshalAgentSDKJSON(t, canonical), mustMarshalAgentSDKJSON(t, original))
			}
		})
	}
}

func assertResolvedToolDefault(t *testing.T, tool map[string]any, enabled bool, policy string) {
	t.Helper()
	defaults, ok := tool["default_config"].(map[string]any)
	if !ok {
		t.Fatalf("default_config = %#v; want object", tool["default_config"])
	}
	if defaults["enabled"] != enabled {
		t.Fatalf("default enabled = %#v; want %v", defaults["enabled"], enabled)
	}
	permission, ok := defaults["permission_policy"].(map[string]any)
	if !ok || permission["type"] != policy {
		t.Fatalf("default permission_policy = %#v; want %s", defaults["permission_policy"], policy)
	}
}

func assertResolvedToolConfigs(t *testing.T, tool map[string]any, names []string) {
	t.Helper()
	configs, ok := tool["configs"].([]any)
	if !ok {
		t.Fatalf("configs = %#v; want array", tool["configs"])
	}
	if len(configs) != len(names) {
		t.Fatalf("configs count = %d; want %d (%#v)", len(configs), len(names), configs)
	}
	for index, name := range names {
		config, ok := configs[index].(map[string]any)
		if !ok {
			t.Fatalf("configs[%d] = %#v; want object", index, configs[index])
		}
		if config["name"] != name || config["enabled"] != true {
			t.Fatalf("configs[%d] = %#v; want name=%s enabled=true", index, config, name)
		}
		permission, ok := config["permission_policy"].(map[string]any)
		if !ok || permission["type"] != "always_allow" {
			t.Fatalf("configs[%d].permission_policy = %#v; want always_allow", index, config["permission_policy"])
		}
	}
}

func mustMarshalAgentSDKJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal JSON: %v", err)
	}
	return body
}
