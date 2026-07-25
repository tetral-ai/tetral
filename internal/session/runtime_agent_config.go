package session

import (
	"bytes"
	"encoding/json"
)

// RuntimeAgentConfig is the per-session overlay (Tools + MCPServers, persisted in
// the session row as installed_tools_json) that, together with approval_mode,
// forms the runtime-visible agent config. Changing any of these three is the only
// thing that advances config_generation and hands work to the runtime.
//
// Runtime-visible config-update lifecycle (the session update path):
//
//	stage   rule / effect                                      writer
//	-----   -------------                                      ------
//	admit   the session must be runtime-idle: runtime status   postgresql_store.go
//	        not 'running', no pending/leased runtime_input,    (rejectRuntimeConfigUpdateRaces)
//	        runtime_config_update, or cleanup_session job, no
//	        delivering/accepted inbox, no cleanup claim.
//	        Any of these -> 409 conflict, no write.
//	apply   approval_mode: empty normalizes to ask_for_approval service.go (Create seeds
//	        (normalizeApprovalMode); Create seeds it from the   from the agent version),
//	        resolved agent version; an omitted approval_mode on postgresql_store.go
//	        update preserves the currently stored value.        (UpdateSession)
//	commit  config_generation++ AND exactly one                postgresql_store.go
//	        runtime_config_update queue job enqueued IFF        (UpdateSession,
//	        approval_mode or the Tools/MCPServers overlay       enqueueRuntimeConfigUpdate)
//	        actually changes; an effective no-op advances
//	        neither the generation nor a job.
//
// The runtime bridge reads config_generation + approval_mode + installed_tools_json
// to deliver the change.
//
// UPDATE-WITH: postgresql_store.go (UpdateSession, rejectRuntimeConfigUpdateRaces,
// enqueueRuntimeConfigUpdate), service.go (Create, Update),
// services/bridge/runtime_delivery.go.
type RuntimeAgentConfig struct {
	Tools      []json.RawMessage `json:"tools"`
	MCPServers []json.RawMessage `json:"mcp_servers"`
}

func normalizeRuntimeAgentConfig(config RuntimeAgentConfig) RuntimeAgentConfig {
	if config.Tools == nil {
		config.Tools = []json.RawMessage{}
	} else {
		config.Tools = cloneRawMessages(config.Tools)
	}
	if config.MCPServers == nil {
		config.MCPServers = []json.RawMessage{}
	} else {
		config.MCPServers = cloneRawMessages(config.MCPServers)
	}
	return config
}

func cloneRuntimeAgentConfigPointer(config *RuntimeAgentConfig) *RuntimeAgentConfig {
	if config == nil {
		return nil
	}
	cloned := normalizeRuntimeAgentConfig(*config)
	return &cloned
}

func runtimeAgentConfigEqual(a RuntimeAgentConfig, b RuntimeAgentConfig) bool {
	left, leftErr := json.Marshal(normalizeRuntimeAgentConfig(a))
	right, rightErr := json.Marshal(normalizeRuntimeAgentConfig(b))
	if leftErr != nil || rightErr != nil {
		return false
	}
	return bytes.Equal(left, right)
}

func cloneRawMessages(values []json.RawMessage) []json.RawMessage {
	cloned := make([]json.RawMessage, len(values))
	for index, value := range values {
		cloned[index] = append(json.RawMessage(nil), value...)
	}
	return cloned
}
