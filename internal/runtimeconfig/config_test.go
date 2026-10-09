package runtimeconfig

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestInstalledSessionSettingsLiteralVectors(t *testing.T) {
	for _, test := range []struct {
		name      string
		agent     string
		installed string
		family    string
		system    *string
		policy    string
	}{
		{
			name:      "installed policy replaces original agent tools",
			agent:     `{"system":"Operate as the session specialist.","tools":[{"type":"tetral_agent_toolset","family":"gpt"}],"mcp_servers":[]}`,
			installed: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"approval_mode":"always_ask"},"configs":[{"name":"github_search","enabled":true,"approval_mode":"always_allow"}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://github.example/mcp"}]}`,
			family:    "claude",
			system:    stringPointer("Operate as the session specialist."),
			policy:    `{"approvalMode":"approve_for_me","mcpServers":[{"type":"url","name":"github","url":"https://github.example/mcp"}],"mcpToolsets":[{"mcpServerName":"github","defaultConfig":{"enabled":false,"approval_mode":"always_ask"},"configs":[{"name":"github_search","enabled":true,"approval_mode":"always_allow"}]}]}`,
		},
		{
			name:      "empty MCP policy and nullable system",
			agent:     `{"system":null,"tools":[{"type":"mcp_toolset","mcp_server_name":"obsolete"}]}`,
			installed: `{"tools":[{"type":"tetral_agent_toolset","family":"gpt"}]}`,
			family:    "gpt",
			policy:    `{"approvalMode":"approve_for_me","mcpServers":[],"mcpToolsets":[]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			memory := []MemoryStore{{MemoryStoreID: "memstore_runtime_config", Name: "Project notes", Access: "read_write", Instructions: stringPointer("Preserve this guidance.")}}
			settings, err := InterpretSessionSettings("approve_for_me", test.agent, test.installed, memory)
			if err != nil {
				t.Fatal(err)
			}
			family, err := InstalledBuiltinFamily(settings.Config)
			if err != nil || family != test.family {
				t.Fatalf("installed family = %q, %v; want %q", family, err, test.family)
			}
			if !reflect.DeepEqual(settings.System, test.system) {
				t.Fatalf("system = %#v; want %#v", settings.System, test.system)
			}
			if !reflect.DeepEqual(settings.MemoryStores, []MemoryStore{{MemoryStoreID: "memstore_runtime_config", Name: "Project notes", Access: "read_write", Instructions: stringPointer("Preserve this guidance.")}}) {
				t.Fatalf("memory stores = %#v", settings.MemoryStores)
			}
			actual, err := json.Marshal(settings.ToolPolicy)
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal(actual, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(test.policy), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("tool policy = %s; want %s", actual, test.policy)
			}
		})
	}
}

func TestInstalledSnapshotRejectsMissingOrConflictingBuiltinAuthority(t *testing.T) {
	for _, installed := range []string{
		`{"tools":[]}`,
		`{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"tetral_agent_toolset","family":"gpt"}]}`,
	} {
		if _, err := InterpretSessionSettings("approve_for_me", `{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`, installed, []MemoryStore{}); err == nil {
			t.Fatalf("installed snapshot %s inherited original-agent authority", installed)
		}
	}
}

func stringPointer(value string) *string { return &value }
