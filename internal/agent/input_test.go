package agent

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func decodePatchForTest(t *testing.T, body string) AgentPatch {
	t.Helper()
	var patch AgentPatch
	if err := json.Unmarshal([]byte(body), &patch); err != nil {
		t.Fatalf("decode patch %q: %v", body, err)
	}
	return patch
}

func decodeCreateAgentWithClaudeToolsetForTest(body []byte) (CreateAgentRequest, error) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		return CreateAgentRequest{}, err
	}
	var tools []json.RawMessage
	if raw, ok := request["tools"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &tools); err != nil {
			return DecodeCreateAgentRequest(body)
		}
	}
	found := false
	for _, raw := range tools {
		var entry struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &entry) == nil && entry.Type == "tetral_agent_toolset" {
			found = true
			break
		}
	}
	if !found {
		tools = append(tools, json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`))
		request["tools"], _ = json.Marshal(tools)
	}
	canonical, err := json.Marshal(request)
	if err != nil {
		return CreateAgentRequest{}, err
	}
	return DecodeCreateAgentRequest(canonical)
}

func TestDecodeCreateAgentAcceptsMinimalBody(t *testing.T) {
	request, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(`{"name":"demo","model":"anthropic/claude-opus-4-8"}`))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest(minimal): %v", err)
	}
	if request.Name != "demo" || request.Model != "anthropic/claude-opus-4-8" {
		t.Fatalf("decoded request = %+v", request)
	}
	if request.ModelVariant != "" {
		t.Fatalf("ModelVariant = %q; want empty reserved variant", request.ModelVariant)
	}
	if request.System != nil || request.Description != nil {
		t.Fatalf("omitted nullable strings should be nil: %+v", request)
	}
	if request.Tools == nil || request.MCPServers == nil || request.Skills == nil || request.Metadata == nil {
		t.Fatalf("containers must normalize to non-nil: %+v", request)
	}
}

func TestDecodeCreateAgentAcceptsAllSupportedFields(t *testing.T) {
	body := `{
		"name":"full",
		"model":"anthropic/claude-opus-4-8",
		"system":"sys",
		"description":"desc",
		"tools":[{"type":"tetral_agent_toolset","family":"claude"}],
		"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],
		"skills":[{"type":"custom","skill_id":"skill_1"}],
		"metadata":{"team":"core"}
	}`
	request, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest(full): %v", err)
	}
	if request.System == nil || *request.System != "sys" || request.Description == nil || *request.Description != "desc" {
		t.Fatalf("nullable strings not decoded: %+v", request)
	}
	if len(request.Tools) != 1 || len(request.MCPServers) != 1 || len(request.Skills) != 1 {
		t.Fatalf("arrays not decoded: %+v", request)
	}
	if request.Metadata["team"] != "core" {
		t.Fatalf("metadata not decoded: %+v", request.Metadata)
	}
}

func TestDecodeCreateAgentAcceptsStandardSpeedAsOmitted(t *testing.T) {
	omitted, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(`{"name":"omitted","model":{"id":"openai/gpt-5.5"}}`))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest(model object omitted speed): %v", err)
	}
	request, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(`{"name":"standard","model":{"id":"openai/gpt-5.5","speed":"standard"}}`))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest(model object standard speed): %v", err)
	}
	if request.Model != omitted.Model || request.Model != "openai/gpt-5.5" {
		t.Fatalf("model = %q omitted=%q; want same canonical provider/model id", request.Model, omitted.Model)
	}
	if request.ModelVariant != "" || omitted.ModelVariant != "" {
		t.Fatalf("ModelVariant standard=%q omitted=%q; want both empty", request.ModelVariant, omitted.ModelVariant)
	}
	data, err := json.Marshal(request.AgentConfig)
	if err != nil {
		t.Fatalf("marshal public config: %v", err)
	}
	if strings.Contains(string(data), "model_variant") {
		t.Fatalf("public AgentConfig JSON leaked model_variant: %s", data)
	}
}

func TestDecodeCreateAgentTreatsNullAndEmptyOptionalStringsAsNil(t *testing.T) {
	for _, body := range []string{
		`{"name":"x","model":"anthropic/claude-opus-4-8","system":null,"description":null}`,
		`{"name":"x","model":"anthropic/claude-opus-4-8","system":"","description":""}`,
	} {
		request, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
		if err != nil {
			t.Fatalf("DecodeCreateAgentRequest(%s): %v", body, err)
		}
		if request.System != nil || request.Description != nil {
			t.Fatalf("system/description should clear to nil for %s: %+v", body, request)
		}
	}
}

func TestDecodeCreateAgentRejectsRequiredFieldShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"missing name", `{"model":"anthropic/claude-opus-4-8"}`, "name"},
		{"null name", `{"name":null,"model":"anthropic/claude-opus-4-8"}`, "name"},
		{"non-string name", `{"name":1,"model":"anthropic/claude-opus-4-8"}`, "name"},
		{"empty name", `{"name":"","model":"anthropic/claude-opus-4-8"}`, "name"},
		{"missing model", `{"name":"x"}`, "model"},
		{"null model", `{"name":"x","model":null}`, "model"},
		{"non-string model", `{"name":"x","model":1}`, "model"},
		{"malformed model object", `{"name":"x","model":{"id":"m"}}`, "model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.name == "tools null" || tc.name == "tools object" {
				_, err = DecodeCreateAgentRequest([]byte(tc.body))
			} else {
				_, err = decodeCreateAgentWithClaudeToolsetForTest([]byte(tc.body))
			}
			if err == nil {
				t.Fatal("expected validation error")
			}
			if _, ok := err.(*ValidationError); !ok {
				t.Fatalf("error = %T %v; want *ValidationError", err, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodeCreateAgentRejectsUnapprovedModelIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"providerless", `{"name":"x","model":"claude-opus-4-8"}`},
		{"malformed", `{"name":"x","model":"anthropic/claude/opus"}`},
		{"unapproved", `{"name":"x","model":"anthropic/claude-opus-4-7"}`},
		{"object unapproved", `{"name":"x","model":{"id":"openai/gpt-5.4"}}`},
		{"object unknown field", `{"name":"x","model":{"id":"openai/gpt-5.5","provider_id":"openai"}}`},
		{"object fast speed", `{"name":"x","model":{"id":"openai/gpt-5.5","speed":"fast"}}`},
		{"object invalid speed", `{"name":"x","model":{"id":"openai/gpt-5.5","speed":"turbo"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(tc.body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "model") {
				t.Fatalf("error %q must mention model", err.Error())
			}
		})
	}
}

func TestValidateAndCanonicalizeConfigRejectsInternalModelVariant(t *testing.T) {
	_, err := ValidateAndCanonicalizeConfig(AgentConfig{Name: "x", Model: "openai/gpt-5.5", ModelVariant: "fast"})
	if err == nil {
		t.Fatal("expected internal model variant to reject")
	}
	if !strings.Contains(err.Error(), "variant") {
		t.Fatalf("error %q must mention variant", err.Error())
	}
}

func TestDecodeCreateAgentRejectsUnsupportedTopLevelFields(t *testing.T) {
	for _, field := range []string{"speed", "effort", "reasoning_effort", "callable_agents", "unknown"} {
		body := `{"name":"x","model":"anthropic/claude-opus-4-8","` + field + `":1}`
		_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
		if err == nil {
			t.Fatalf("field %s must reject", field)
		}
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("error %q must name field %q", err.Error(), field)
		}
	}
}

func TestDecodeCreateAgentRejectsInvalidContainers(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"tools null", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":null}`, "tools"},
		{"tools object", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":{}}`, "tools"},
		{"mcp null", `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":null}`, "mcp_servers"},
		{"skills null", `{"name":"x","model":"anthropic/claude-opus-4-8","skills":null}`, "skills"},
		{"metadata null", `{"name":"x","model":"anthropic/claude-opus-4-8","metadata":null}`, "metadata"},
		{"metadata value non-string", `{"name":"x","model":"anthropic/claude-opus-4-8","metadata":{"k":1}}`, "metadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.name == "tools null" || tc.name == "tools object" {
				_, err = DecodeCreateAgentRequest([]byte(tc.body))
			} else {
				_, err = decodeCreateAgentWithClaudeToolsetForTest([]byte(tc.body))
			}
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodeCreateAgentRejectsUnsupportedToolAndSkillShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"custom tool", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"custom","name":"run"}]}`, "custom tools are not supported in this stage"},
		{"unknown tool", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"future"}]}`, "tools[0].type"},
		{"old Tetral toolset spelling", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_toolset_20260508"}]}`, "tools[0].type"},
		{"missing Tetral tool family", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset"}]}`, "tools[0].family"},
		{"unsupported Tetral tool family", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"future"}]}`, "tools[0].family"},
		{"per built-in config", `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude","configs":[{"name":"bash"}]}]}`, "configs"},
		{"anthropic skill", `{"name":"x","model":"anthropic/claude-opus-4-8","skills":[{"type":"anthropic","skill_id":"xlsx"}]}`, "skills[0].type"},
		{"missing custom skill id", `{"name":"x","model":"anthropic/claude-opus-4-8","skills":[{"type":"custom"}]}`, "skill_id"},
		{"empty skill version", `{"name":"x","model":"anthropic/claude-opus-4-8","skills":[{"type":"custom","skill_id":"skill_1","version":""}]}`, "version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(tc.body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodeCreateAgentValidatesMCPToolConfigNames(t *testing.T) {
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"` +
		strings.Repeat("a", 129) + `"}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "configs[0].name") {
		t.Fatalf("error %q must name configs[0].name", err.Error())
	}
}

func TestDecodeCreateAgentRejectsDuplicateMCPToolConfigNames(t *testing.T) {
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"issues"},{"name":"issues"}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
	if err == nil {
		t.Fatal("duplicate configs[].name must reject")
	}
	if !strings.Contains(err.Error(), "configs[1].name") {
		t.Fatalf("error %q must name duplicate config path", err.Error())
	}
}

func TestAgentPatchMaterializeRejectsDuplicateMCPToolConfigNames(t *testing.T) {
	current := AgentConfig{
		Name:       "x",
		Model:      "anthropic/claude-opus-4-8",
		MCPServers: RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)},
	}.Normalize()
	patch := decodePatchForTest(t, `{"version":1,"tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"issues"},{"name":"issues"}]}]}`)
	materialized, err := patch.Materialize(current)
	if err == nil {
		t.Fatalf("duplicate configs[].name must reject; materialized=%+v", materialized)
	}
	if !strings.Contains(err.Error(), "configs[1].name") {
		t.Fatalf("error %q must name duplicate config path", err.Error())
	}
}

func TestDecodeCreateAgentValidatesMCPToolConfigNameUnicodeBoundaries(t *testing.T) {
	exactConfigName := strings.Repeat("界", maxMCPToolConfigNameCodePoints)
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"` +
		exactConfigName + `"}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	if _, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body)); err != nil {
		t.Fatalf("DecodeCreateAgentRequest(exact unicode config name): %v", err)
	}

	body = `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"` +
		strings.Repeat("界", maxMCPToolConfigNameCodePoints+1) + `"}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "configs[0].name") {
		t.Fatalf("error %q must name configs[0].name", err.Error())
	}
}

func TestDecodeCreateAgentRejectsInvalidMCPToolConfigNameLowerBound(t *testing.T) {
	cases := []struct {
		name       string
		configJSON string
	}{
		{"empty", `{"name":""}`},
		{"missing", `{"enabled":true}`},
		{"non-string", `{"name":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[` +
				tc.configJSON + `]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "configs[0].name") {
				t.Fatalf("error %q must name configs[0].name", err.Error())
			}
		})
	}
}

func TestAgentPatchMaterializeRejectsEmptyMCPToolConfigName(t *testing.T) {
	current := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	patch := decodePatchForTest(t, `{
		"version":1,
		"tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":""}]}],
		"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]
	}`)
	_, err := patch.Materialize(current)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "configs[0].name") {
		t.Fatalf("error %q must name configs[0].name", err.Error())
	}
}

func TestDecodeCreateAgentRejectsDuplicateSkillIDs(t *testing.T) {
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","skills":[{"type":"custom","skill_id":"skill_dup"},{"type":"custom","skill_id":"skill_dup","version":"latest"}]}`
	_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
	if err == nil {
		t.Fatal("duplicate skill_id must reject")
	}
	if !strings.Contains(err.Error(), "skill_dup") {
		t.Fatalf("error %q must name duplicated skill_id", err.Error())
	}
}

func TestValidateConfigRequiresNameAndModel(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  AgentConfig
		want string
	}{
		{"valid", AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}, ""},
		{"missing name", AgentConfig{Model: "anthropic/claude-opus-4-8"}, "name"},
		{"missing model", AgentConfig{Name: "x"}, "model"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.cfg)
			if tc.want == "" && err != nil {
				t.Fatalf("ValidateConfig(valid): %v", err)
			}
			if tc.want != "" {
				if err == nil {
					t.Fatal("expected validation error")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error %q must mention %q", err.Error(), tc.want)
				}
			}
		})
	}
}

func TestValidateGenericLimits(t *testing.T) {
	base := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	cases := []struct {
		name string
		mut  func(*AgentConfig)
		want string
	}{
		{"empty name", func(cfg *AgentConfig) { cfg.Name = "" }, "name"},
		{"name over", func(cfg *AgentConfig) { cfg.Name = strings.Repeat("a", 257) }, "name"},
		{"system over", func(cfg *AgentConfig) { cfg.System = stringPointerForTest(strings.Repeat("a", maxSystemBytes+1)) }, "system"},
		{"description over", func(cfg *AgentConfig) { cfg.Description = stringPointerForTest(strings.Repeat("a", 2049)) }, "description"},
		{"too many tools", func(cfg *AgentConfig) { cfg.Tools = make(RawArray, 129) }, "tools"},
		{"too many mcp servers", func(cfg *AgentConfig) { cfg.MCPServers = make(RawArray, 21) }, "mcp_servers"},
		{"too many skills", func(cfg *AgentConfig) { cfg.Skills = make(RawArray, 21) }, "skills"},
		{"too many metadata keys", func(cfg *AgentConfig) {
			cfg.Metadata = StringMap{}
			for i := 0; i < 17; i++ {
				cfg.Metadata[string(rune('a'+i))] = "v"
			}
		}, "metadata"},
		{"metadata key over", func(cfg *AgentConfig) { cfg.Metadata = StringMap{strings.Repeat("k", 65): "v"} }, "metadata"},
		{"metadata value over", func(cfg *AgentConfig) { cfg.Metadata = StringMap{"k": strings.Repeat("v", 513)} }, "metadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mut(&cfg)
			err := ValidateGenericLimits(cfg)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestValidateGenericLimitsCountsUnicodeCodePoints(t *testing.T) {
	base := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	cases := []struct {
		name    string
		mut     func(*AgentConfig)
		wantErr bool
		want    string
	}{
		{"name exact unicode", func(cfg *AgentConfig) { cfg.Name = strings.Repeat("界", maxNameCodePoints) }, false, ""},
		{"name over unicode", func(cfg *AgentConfig) { cfg.Name = strings.Repeat("界", maxNameCodePoints+1) }, true, "name"},
		{"description exact unicode", func(cfg *AgentConfig) {
			cfg.Description = stringPointerForTest(strings.Repeat("界", maxDescriptionCodePoints))
		}, false, ""},
		{"description over unicode", func(cfg *AgentConfig) {
			cfg.Description = stringPointerForTest(strings.Repeat("界", maxDescriptionCodePoints+1))
		}, true, "description"},
		{"metadata key exact unicode", func(cfg *AgentConfig) { cfg.Metadata = StringMap{strings.Repeat("界", maxMetadataKeyCodePoints): "v"} }, false, ""},
		{"metadata key over unicode", func(cfg *AgentConfig) {
			cfg.Metadata = StringMap{strings.Repeat("界", maxMetadataKeyCodePoints+1): "v"}
		}, true, "metadata"},
		{"metadata value exact unicode", func(cfg *AgentConfig) {
			cfg.Metadata = StringMap{"k": strings.Repeat("界", maxMetadataValueCodePoints)}
		}, false, ""},
		{"metadata value over unicode", func(cfg *AgentConfig) {
			cfg.Metadata = StringMap{"k": strings.Repeat("界", maxMetadataValueCodePoints+1)}
		}, true, "metadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mut(&cfg)
			err := ValidateGenericLimits(cfg)
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateGenericLimits: %v", err)
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected validation error")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error %q must mention %q", err.Error(), tc.want)
				}
			}
		})
	}
}

func TestValidateGenericLimitsBoundsSystemByUTF8Bytes(t *testing.T) {
	base := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	for _, test := range []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "ASCII exact", value: strings.Repeat("a", maxSystemBytes)},
		{name: "ASCII over", value: strings.Repeat("a", maxSystemBytes+1), wantErr: true},
		{name: "multibyte within", value: strings.Repeat("界", maxSystemBytes/len("界"))},
		{name: "multibyte over", value: strings.Repeat("界", maxSystemBytes/len("界")+1), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			cfg.System = stringPointerForTest(test.value)
			err := ValidateGenericLimits(cfg)
			if test.wantErr && err == nil {
				t.Fatal("expected system byte-limit error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("ValidateGenericLimits: %v", err)
			}
		})
	}
}

func TestDecodeCreateAgentValidatesMCPServerNameUnicodeBoundaries(t *testing.T) {
	exactName := strings.Repeat("界", maxMCPServerNameCodePoints)
	body := `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"` + exactName + `","url":"https://api.githubcopilot.com/mcp/"}]}`
	if _, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body)); err != nil {
		t.Fatalf("DecodeCreateAgentRequest(exact unicode MCP server name): %v", err)
	}

	cases := []struct {
		name       string
		serverName string
	}{
		{"empty", ""},
		{"over unicode", strings.Repeat("界", maxMCPServerNameCodePoints+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"` + tc.serverName + `","url":"https://api.githubcopilot.com/mcp/"}]}`
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "mcp_servers[0].name") {
				t.Fatalf("error %q must name mcp_servers[0].name", err.Error())
			}
		})
	}
}

func TestAgentPatchRequiresVersion(t *testing.T) {
	for _, body := range []string{`{"name":"x"}`, `{"version":null}`, `{"version":"abc"}`} {
		var patch AgentPatch
		err := json.Unmarshal([]byte(body), &patch)
		if err == nil {
			t.Fatalf("patch %s must reject", body)
		}
	}
}

func TestAgentPatchPrevalidateRejectsUnsupportedFields(t *testing.T) {
	for _, field := range []string{"speed", "effort", "reasoning_effort", "callable_agents", "unknown"} {
		patch := decodePatchForTest(t, `{"version":1,"`+field+`":1}`)
		err := patch.Prevalidate()
		if err == nil {
			t.Fatalf("field %s must reject", field)
		}
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("error %q must name %q", err.Error(), field)
		}
	}
}

func TestDecodeCreateAgentRejectsUnsupportedNestedToolConfigFields(t *testing.T) {
	cases := []struct {
		name string
		tool string
		want string
	}{
		{"top-level unknown", `{"type":"mcp_toolset","mcp_server_name":"github","unknown":true}`, "tools[0].unknown"},
		{"default config unknown", `{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"unknown":true}}`, "default_config.unknown"},
		{"configs unknown", `{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"issues","unknown":true}]}`, "configs[0].unknown"},
		{"permission policy unknown", `{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"permission_policy":{"type":"always_ask","unknown":true}}}`, "permission_policy.unknown"},
		{"enabled non-bool", `{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":"yes"}}`, "enabled"},
		{"policy invalid type", `{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"permission_policy":{"type":"sometimes"}}}`, "permission_policy.type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","tools":[` + tc.tool + `],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodeCreateAgentRejectsMCPServerCredentialAndAuthFields(t *testing.T) {
	for _, field := range []string{"unknown", "authorization", "headers", "credential", "credentials", "vault_id"} {
		t.Run(field, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/","` + field + `":"secret"}]}`
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "mcp_servers[0]."+field) {
				t.Fatalf("error %q must name rejected field %q", err.Error(), field)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error %q must not echo credential-like value", err.Error())
			}
		})
	}
}

func TestDecodeCreateAgentCanonicalizesMCPServerURL(t *testing.T) {
	request, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(`{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"github","url":"https://API.GITHUBCOPILOT.COM/mcp"}]}`))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest: %v", err)
	}
	var server struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(request.MCPServers[0], &server); err != nil {
		t.Fatalf("decode canonical server: %v", err)
	}
	if server.URL != githubMCPCatalogURL {
		t.Fatalf("canonical MCP server URL = %q; want %s", server.URL, githubMCPCatalogURL)
	}
}

func TestDecodeCreateAgentRejectsNonCatalogMCPServerURLWithoutEchoingRawInput(t *testing.T) {
	cases := []string{
		"https://not-github.example.com/mcp",
		"https://api.githubcopilot.com/mcp/extra-secret",
	}
	for _, rawURL := range cases {
		t.Run(rawURL, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"github","url":"` + rawURL + `"}]}`
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "mcp_servers[0].url") || !strings.Contains(err.Error(), "supported MCP catalog") {
				t.Fatalf("error %q must name catalog URL admission", err.Error())
			}
			if strings.Contains(err.Error(), rawURL) || strings.Contains(err.Error(), "extra-secret") {
				t.Fatalf("error %q must not echo raw non-catalog URL", err.Error())
			}
		})
	}
}

func TestDecodeCreateAgentRejectsUnsafeMCPServerURLsWithoutEchoingRawInput(t *testing.T) {
	cases := []struct {
		name        string
		raw         string
		mustMention string
		mustOmit    []string
	}{
		{"http scheme", "http://example.com/mcp", "https", []string{"http://example.com/mcp"}},
		{"userinfo", "https://user:pass@example.com/mcp", "credentials", []string{"user", "pass", "user:pass", "https://user:pass@example.com/mcp"}},
		{"query", "https://api.githubcopilot.com/mcp/?token=secret", "query", []string{"secret", "token=secret", "https://api.githubcopilot.com/mcp/?token=secret"}},
		{"fragment", "https://api.githubcopilot.com/mcp/#secret", "fragment", []string{"secret", "#secret", "https://api.githubcopilot.com/mcp/#secret"}},
		{"localhost", "https://localhost/mcp", "localhost", []string{"https://localhost/mcp"}},
		{"private ip", "https://10.0.0.1/mcp", "globally reachable", []string{"10.0.0.1", "https://10.0.0.1/mcp"}},
		{"loopback ip", "https://127.0.0.1/mcp", "globally reachable", []string{"127.0.0.1", "https://127.0.0.1/mcp"}},
		{"link local ip", "https://169.254.1.1/mcp", "globally reachable", []string{"169.254.1.1", "https://169.254.1.1/mcp"}},
		{"multicast ip", "https://224.0.0.1/mcp", "globally reachable", []string{"224.0.0.1", "https://224.0.0.1/mcp"}},
		{"unspecified ip", "https://0.0.0.0/mcp", "globally reachable", []string{"0.0.0.0", "https://0.0.0.0/mcp"}},
		{"ipv6 loopback", "https://[::1]/mcp", "globally reachable", []string{"::1", "https://[::1]/mcp"}},
		{"ipv6 unique local", "https://[fc00::1]/mcp", "globally reachable", []string{"fc00::1", "https://[fc00::1]/mcp"}},
		{"legacy ipv4", "https://0177.0.0.1/mcp", "legacy IPv4", []string{"0177.0.0.1", "https://0177.0.0.1/mcp"}},
		{"ipv6 zone", "https://[fe80::1%25eth0]/mcp", "zone", []string{"eth0", "fe80::1%25eth0", "https://[fe80::1%25eth0]/mcp"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"github","url":"` + tc.raw + `"}]}`
			_, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(body))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "mcp_servers[0].url") {
				t.Fatalf("error %q must name mcp_servers[0].url", err.Error())
			}
			if !strings.Contains(err.Error(), tc.mustMention) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.mustMention)
			}
			for _, forbidden := range tc.mustOmit {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("error %q must not echo %q", err.Error(), forbidden)
				}
			}
		})
	}
}

func TestCanonicalAgentSnapshotDoesNotPersistRejectedMCPServerCredentialFields(t *testing.T) {
	request, err := decodeCreateAgentWithClaudeToolsetForTest([]byte(`{"name":"x","model":"anthropic/claude-opus-4-8","mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`))
	if err != nil {
		t.Fatalf("DecodeCreateAgentRequest: %v", err)
	}
	bytes, _, err := Canonicalize(request.AgentConfig)
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	for _, forbidden := range []string{"authorization", "headers", "credential", "credentials", "vault_id"} {
		if strings.Contains(string(bytes), forbidden) {
			t.Fatalf("canonical bytes contain forbidden credential field %q: %s", forbidden, string(bytes))
		}
	}
}

func TestAgentPatchMaterializeArrayReplacementAndClearSemantics(t *testing.T) {
	current := AgentConfig{
		Name:       "current",
		Model:      "anthropic/claude-opus-4-8",
		Tools:      RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		MCPServers: RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)},
		Skills:     RawArray{json.RawMessage(`{"type":"custom","skill_id":"skill_a","version":"latest"}`)},
	}.Normalize()
	cases := []struct {
		name      string
		field     string
		valueJSON string
		wantEmpty bool
		wantError string
		contains  string
	}{
		{"tools replacement", "tools", `[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"issues"}]}]`, false, "", `"mcp_toolset"`},
		{"tools null clears then rejects", "tools", `null`, false, "tools must contain exactly one tetral_agent_toolset entry", ""},
		{"tools empty clears then rejects", "tools", `[]`, false, "tools must contain exactly one tetral_agent_toolset entry", ""},
		{"mcp servers replacement", "mcp_servers", `[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp"}]`, false, "", `"github"`},
		{"mcp servers null clears", "mcp_servers", `null`, true, "", ""},
		{"mcp servers empty clears", "mcp_servers", `[]`, true, "", ""},
		{"skills replacement", "skills", `[{"type":"custom","skill_id":"skill_b"}]`, false, "", `"skill_b"`},
		{"skills null clears", "skills", `null`, true, "", ""},
		{"skills empty clears", "skills", `[]`, true, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			patch := decodePatchForTest(t, `{"version":1,"`+tc.field+`":`+tc.valueJSON+`}`)
			materialized, err := patch.Materialize(current)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("Materialize error = %v; want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("Materialize: %v", err)
			}
			var got RawArray
			switch tc.field {
			case "tools":
				got = materialized.Tools
			case "mcp_servers":
				got = materialized.MCPServers
			case "skills":
				got = materialized.Skills
			default:
				t.Fatalf("unknown test field %q", tc.field)
			}
			if tc.wantEmpty {
				if len(got) != 0 {
					t.Fatalf("%s should clear, got %s", tc.field, string(mustMarshalJSONForTest(t, got)))
				}
				return
			}
			wantLength := 1
			if tc.field == "tools" {
				wantLength = 2
			}
			if len(got) != wantLength {
				t.Fatalf("%s replacement length = %d; want %d", tc.field, len(got), wantLength)
			}
			if !strings.Contains(string(mustMarshalJSONForTest(t, got)), tc.contains) {
				t.Fatalf("%s replacement = %s; want substring %s", tc.field, string(mustMarshalJSONForTest(t, got)), tc.contains)
			}
		})
	}
}

func TestAgentPatchMaterializeRejectsNonCatalogMCPServerURL(t *testing.T) {
	current := AgentConfig{
		Name:       "current",
		Model:      "anthropic/claude-opus-4-8",
		MCPServers: RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)},
	}.Normalize()
	patch := decodePatchForTest(t, `{"version":1,"mcp_servers":[{"type":"url","name":"github","url":"https://not-github.example.com/mcp"}]}`)
	_, err := patch.Materialize(current)
	if err == nil {
		t.Fatal("non-catalog MCP server URL must reject")
	}
	if !strings.Contains(err.Error(), "mcp_servers[0].url") || !strings.Contains(err.Error(), "supported MCP catalog") {
		t.Fatalf("error %q must name catalog URL admission", err.Error())
	}
}

func mustMarshalJSONForTest(t *testing.T, value any) []byte {
	t.Helper()
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return out
}

func TestAgentPatchMaterializePreservesOmittedFields(t *testing.T) {
	current := AgentConfig{
		Name:        "current",
		Model:       "anthropic/claude-opus-4-8",
		System:      stringPointerForTest("system"),
		Description: stringPointerForTest("description"),
		Tools:       RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		MCPServers:  RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)},
		Skills:      RawArray{json.RawMessage(`{"type":"custom","skill_id":"skill_a","version":"latest"}`)},
		Metadata:    StringMap{"team": "core"},
	}.Normalize()
	patch := decodePatchForTest(t, `{"version":1}`)
	materialized, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !reflect.DeepEqual(materialized, current) {
		t.Fatalf("omitted fields must preserve.\ncurrent: %+v\nmaterialized: %+v", current, materialized)
	}
}

func TestAgentPatchMaterializeAppliesAnthropicPatchSemantics(t *testing.T) {
	current := AgentConfig{
		Name:        "current",
		Model:       "anthropic/claude-opus-4-8",
		System:      stringPointerForTest("system"),
		Description: stringPointerForTest("description"),
		Tools:       RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		MCPServers:  RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)},
		Skills:      RawArray{json.RawMessage(`{"type":"custom","skill_id":"skill_a","version":"latest"}`)},
		Metadata:    StringMap{"team": "core", "delete": "yes"},
	}.Normalize()
	patch := decodePatchForTest(t, `{
		"version":1,
		"name":"next",
		"model":"openai/gpt-5.5",
		"system":null,
		"description":"",
		"tools":[{"type":"tetral_agent_toolset","family":"claude"}],
		"mcp_servers":null,
		"skills":[],
		"metadata":{"team":"infra","delete":null,"new":"value"}
	}`)
	materialized, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if materialized.Name != "next" || materialized.Model != "openai/gpt-5.5" {
		t.Fatalf("required scalars not replaced: %+v", materialized)
	}
	if materialized.System != nil || materialized.Description != nil {
		t.Fatalf("nullable strings should clear to nil: %+v", materialized)
	}
	if len(materialized.Tools) != 1 || len(materialized.MCPServers) != 0 || len(materialized.Skills) != 0 {
		t.Fatalf("replaceable arrays should update and clear: %+v", materialized)
	}
	if materialized.Metadata["team"] != "infra" || materialized.Metadata["new"] != "value" {
		t.Fatalf("metadata upsert failed: %+v", materialized.Metadata)
	}
	if _, ok := materialized.Metadata["delete"]; ok {
		t.Fatalf("metadata delete failed: %+v", materialized.Metadata)
	}
}
