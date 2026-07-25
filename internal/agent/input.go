package agent

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/endpointurl"
)

// maxSystemBytes mirrors the Gateway SystemSegment text ceiling so an
// admitted agent instruction always fits its provider-request carrier.
const maxSystemBytes = 64 * 1024

const (
	maxNameCodePoints              = 256
	maxDescriptionCodePoints       = 2048
	maxMetadataPairs               = 16
	maxMetadataKeyCodePoints       = 64
	maxMetadataValueCodePoints     = 512
	maxToolsEntries                = 128
	maxMCPServersEntries           = 20
	maxSkillsEntries               = 20
	maxMCPServerNameCodePoints     = 255
	maxMCPToolConfigNameCodePoints = 128
)

const githubMCPCatalogURL = "https://api.githubcopilot.com/mcp/"

func supportedModelIDs() [7]string {
	return [7]string{
		"openai/gpt-5.5",
		"openai/gpt-5.6-sol",
		"anthropic/claude-opus-4-8",
		"anthropic/claude-fable-5",
		"deepseek/deepseek-v4-pro",
		"moonshotai/kimi-k3",
		"zai/glm-5.2",
	}
}

var allowedCreateKeys = map[string]struct{}{
	"name":          {},
	"model":         {},
	"approval_mode": {},
	"multiagent":    {},
	"system":        {},
	"description":   {},
	"tools":         {},
	"mcp_servers":   {},
	"skills":        {},
	"metadata":      {},
}

var allowedUpdateKeys = func() map[string]struct{} {
	keys := make(map[string]struct{}, len(allowedCreateKeys)+1)
	for key := range allowedCreateKeys {
		keys[key] = struct{}{}
	}
	keys["version"] = struct{}{}
	return keys
}()

func DecodeCreateAgentRequest(body []byte) (CreateAgentRequest, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return CreateAgentRequest{}, &ValidationError{Message: "invalid request body"}
	}
	for key := range raw {
		if _, ok := allowedCreateKeys[key]; !ok {
			return CreateAgentRequest{}, &ValidationError{Message: fmt.Sprintf("unknown field %q", key)}
		}
	}

	config := AgentConfig{}
	nameRaw, ok := raw["name"]
	if !ok {
		return CreateAgentRequest{}, &ValidationError{Message: "name is required"}
	}
	name, err := decodeRequiredStringField("name", nameRaw)
	if err != nil {
		return CreateAgentRequest{}, err
	}
	config.Name = name

	modelRaw, ok := raw["model"]
	if !ok {
		return CreateAgentRequest{}, &ValidationError{Message: "model is required"}
	}
	model, variant, err := decodePublicModelField("model", modelRaw)
	if err != nil {
		return CreateAgentRequest{}, err
	}
	config.Model = model
	config.ModelVariant = variant

	if rawValue, ok := raw["approval_mode"]; ok {
		config.ApprovalMode, err = decodeApprovalModeField("approval_mode", rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["multiagent"]; ok {
		if err := decodeUnsupportedNullField("multiagent", rawValue); err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["system"]; ok {
		config.System, err = decodeNullableCreateString("system", rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["description"]; ok {
		config.Description, err = decodeNullableCreateString("description", rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["tools"]; ok {
		config.Tools, err = decodeStrictCreateArray("tools", rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["mcp_servers"]; ok {
		config.MCPServers, err = decodeStrictCreateArray("mcp_servers", rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["skills"]; ok {
		config.Skills, err = decodeStrictCreateArray("skills", rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	if rawValue, ok := raw["metadata"]; ok {
		config.Metadata, err = decodeStrictCreateMetadata(rawValue)
		if err != nil {
			return CreateAgentRequest{}, err
		}
	}
	config = config.Normalize()
	if err := ValidateConfig(config); err != nil {
		return CreateAgentRequest{}, err
	}
	if err := ValidateGenericLimits(config); err != nil {
		return CreateAgentRequest{}, err
	}
	config, err = ValidateAndCanonicalizeConfig(config)
	if err != nil {
		return CreateAgentRequest{}, err
	}
	return CreateAgentRequest{AgentConfig: config}, nil
}

func ValidateConfig(config AgentConfig) error {
	config = config.Normalize()
	if config.Name == "" {
		return &ValidationError{Message: "name is required"}
	}
	if config.Model == "" {
		return &ValidationError{Message: "model is required"}
	}
	return validateApprovalMode(config.ApprovalMode)
}

func ValidateGenericLimits(config AgentConfig) error {
	if count := utf8.RuneCountInString(config.Name); count < 1 || count > maxNameCodePoints {
		return &ValidationError{Message: fmt.Sprintf("name must be 1-%d Unicode code points (got %d)", maxNameCodePoints, count)}
	}
	if config.System != nil {
		if size := len(*config.System); size > maxSystemBytes {
			return &ValidationError{Message: fmt.Sprintf("system must be <= %d UTF-8 bytes (got %d)", maxSystemBytes, size)}
		}
	}
	if config.Description != nil {
		if count := utf8.RuneCountInString(*config.Description); count > maxDescriptionCodePoints {
			return &ValidationError{Message: fmt.Sprintf("description must be <= %d Unicode code points (got %d)", maxDescriptionCodePoints, count)}
		}
	}
	if len(config.Tools) > maxToolsEntries {
		return &ValidationError{Message: fmt.Sprintf("tools must have <= %d entries (got %d)", maxToolsEntries, len(config.Tools))}
	}
	if len(config.MCPServers) > maxMCPServersEntries {
		return &ValidationError{Message: fmt.Sprintf("mcp_servers must have <= %d entries (got %d)", maxMCPServersEntries, len(config.MCPServers))}
	}
	if len(config.Skills) > maxSkillsEntries {
		return &ValidationError{Message: fmt.Sprintf("skills must have <= %d entries (got %d)", maxSkillsEntries, len(config.Skills))}
	}
	if len(config.Metadata) > maxMetadataPairs {
		return &ValidationError{Message: fmt.Sprintf("metadata must have <= %d pairs (got %d)", maxMetadataPairs, len(config.Metadata))}
	}
	for key, value := range config.Metadata {
		if count := utf8.RuneCountInString(key); count > maxMetadataKeyCodePoints {
			return &ValidationError{Message: fmt.Sprintf("metadata key must be <= %d Unicode code points (got %d)", maxMetadataKeyCodePoints, count)}
		}
		if count := utf8.RuneCountInString(value); count > maxMetadataValueCodePoints {
			return &ValidationError{Message: fmt.Sprintf("metadata value must be <= %d Unicode code points (got %d)", maxMetadataValueCodePoints, count)}
		}
	}
	return nil
}

type AgentPatch struct {
	Version int
	raw     map[string]json.RawMessage
}

func (patch *AgentPatch) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return &ValidationError{Message: "invalid request body"}
	}
	versionRaw, ok := raw["version"]
	if !ok || string(versionRaw) == "null" {
		return &ValidationError{Message: "version is required"}
	}
	if err := json.Unmarshal(versionRaw, &patch.Version); err != nil {
		return &ValidationError{Message: "version must be an integer"}
	}
	patch.raw = raw
	return nil
}

func (patch AgentPatch) Prevalidate() error {
	for key := range patch.raw {
		if _, ok := allowedUpdateKeys[key]; !ok {
			return &ValidationError{Message: fmt.Sprintf("unknown field %q", key)}
		}
	}
	if rawValue, ok := patch.raw["name"]; ok {
		if _, err := decodeRequiredStringField("name", rawValue); err != nil {
			return err
		}
	}
	if rawValue, ok := patch.raw["model"]; ok {
		if _, _, err := decodePublicModelField("model", rawValue); err != nil {
			return err
		}
	}
	if rawValue, ok := patch.raw["approval_mode"]; ok {
		if _, err := decodeApprovalModeField("approval_mode", rawValue); err != nil {
			return err
		}
	}
	if rawValue, ok := patch.raw["multiagent"]; ok {
		if err := decodeUnsupportedNullField("multiagent", rawValue); err != nil {
			return err
		}
	}
	for _, field := range []string{"system", "description"} {
		rawValue, ok := patch.raw[field]
		if !ok {
			continue
		}
		if _, err := decodeNullablePatchString(field, rawValue); err != nil {
			return err
		}
	}
	for _, field := range []string{"tools", "mcp_servers", "skills"} {
		rawValue, ok := patch.raw[field]
		if !ok {
			continue
		}
		if _, err := decodeArrayPatch(field, rawValue); err != nil {
			return err
		}
	}
	if rawValue, ok := patch.raw["metadata"]; ok {
		if _, err := decodeMetadataPatch(rawValue); err != nil {
			return err
		}
	}
	return nil
}

func (patch AgentPatch) Materialize(current AgentConfig) (AgentConfig, error) {
	out := current.Normalize()
	if rawValue, ok := patch.raw["name"]; ok {
		value, err := decodeRequiredStringField("name", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.Name = value
	}
	if rawValue, ok := patch.raw["model"]; ok {
		value, variant, err := decodePublicModelField("model", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.Model = value
		out.ModelVariant = variant
	}
	if rawValue, ok := patch.raw["approval_mode"]; ok {
		value, err := decodeApprovalModeField("approval_mode", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.ApprovalMode = value
	}
	if rawValue, ok := patch.raw["system"]; ok {
		value, err := decodeNullablePatchString("system", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.System = value
	}
	if rawValue, ok := patch.raw["description"]; ok {
		value, err := decodeNullablePatchString("description", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.Description = value
	}
	if rawValue, ok := patch.raw["tools"]; ok {
		value, err := decodeArrayPatch("tools", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.Tools = value
	}
	if rawValue, ok := patch.raw["mcp_servers"]; ok {
		value, err := decodeArrayPatch("mcp_servers", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.MCPServers = value
	}
	if rawValue, ok := patch.raw["skills"]; ok {
		value, err := decodeArrayPatch("skills", rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.Skills = value
	}
	if rawValue, ok := patch.raw["metadata"]; ok {
		metadata, err := mergeMetadataPatch(out.Metadata, rawValue)
		if err != nil {
			return AgentConfig{}, err
		}
		out.Metadata = metadata
	}
	out = out.Normalize()
	if err := ValidateConfig(out); err != nil {
		return AgentConfig{}, err
	}
	if err := ValidateGenericLimits(out); err != nil {
		return AgentConfig{}, err
	}
	return ValidateAndCanonicalizeConfig(out)
}

func ValidateAndCanonicalizeConfig(config AgentConfig) (AgentConfig, error) {
	config = config.Normalize()
	model, variant, err := canonicalizeModelRef(config.Model, config.ModelVariant, "model")
	if err != nil {
		return AgentConfig{}, err
	}
	config.Model = model
	config.ModelVariant = variant
	tools, err := canonicalizeTools(config.Tools)
	if err != nil {
		return AgentConfig{}, err
	}
	config.Tools = tools
	servers, err := canonicalizeMCPServers(config.MCPServers)
	if err != nil {
		return AgentConfig{}, err
	}
	config.MCPServers = servers
	skills, err := canonicalizeSkills(config.Skills)
	if err != nil {
		return AgentConfig{}, err
	}
	config.Skills = skills
	if _, err := canonicalTetralToolsetFamily(config.Tools); err != nil {
		return AgentConfig{}, err
	}
	if err := ValidateCrossArray(config); err != nil {
		return AgentConfig{}, err
	}
	return config.Normalize(), nil
}

func decodeRequiredStringField(field string, raw json.RawMessage) (string, error) {
	if string(raw) == "null" {
		return "", &ValidationError{Message: field + " is required and cannot be null"}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", &ValidationError{Message: field + " must be a string"}
	}
	if value == "" {
		return "", &ValidationError{Message: field + " is required and cannot be empty"}
	}
	return value, nil
}

func decodePublicModelField(path string, raw json.RawMessage) (string, string, error) {
	if string(raw) == "null" {
		return "", "", &ValidationError{Message: path + " is required and cannot be null"}
	}
	var modelID string
	if err := json.Unmarshal(raw, &modelID); err == nil {
		if modelID == "" {
			return "", "", &ValidationError{Message: path + " is required and cannot be empty"}
		}
		return canonicalizeModelRef(modelID, "", path)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return "", "", &ValidationError{Message: path + " must be a string or object"}
	}
	if object == nil {
		return "", "", &ValidationError{Message: path + " must be a string or object"}
	}
	for key := range object {
		if key != "id" && key != "speed" {
			return "", "", &ValidationError{Message: path + "." + key + " is an unknown field"}
		}
	}
	idRaw, ok := object["id"]
	if !ok {
		return "", "", &ValidationError{Message: path + ".id is required"}
	}
	modelID, err := decodeRequiredStringField(path+".id", idRaw)
	if err != nil {
		return "", "", err
	}
	if speedRaw, ok := object["speed"]; ok {
		speed, err := decodeRequiredStringField(path+".speed", speedRaw)
		if err != nil {
			return "", "", err
		}
		if speed != "standard" {
			return "", "", &ValidationError{Message: path + ".speed must be standard; fast mode is not supported"}
		}
	}
	return canonicalizeModelRef(modelID, "", path)
}

func canonicalizeModelRef(modelID string, variant string, path string) (string, string, error) {
	modelID = strings.TrimSpace(modelID)
	variant = strings.TrimSpace(variant)
	if modelID == "" {
		return "", "", &ValidationError{Message: path + " is required and cannot be empty"}
	}
	if variant != "" {
		return "", "", &ValidationError{Message: path + ".variant is not supported in this stage"}
	}
	if strings.Count(modelID, "/") != 1 {
		return "", "", &ValidationError{Message: supportedModelErrorMessage(path)}
	}
	if !isSupportedModelID(modelID) {
		return "", "", &ValidationError{Message: supportedModelErrorMessage(path)}
	}
	return modelID, variant, nil
}

func isSupportedModelID(modelID string) bool {
	for _, supported := range supportedModelIDs() {
		if modelID == supported {
			return true
		}
	}
	return false
}

func decodeApprovalModeField(field string, raw json.RawMessage) (ApprovalMode, error) {
	if string(raw) == "null" {
		return "", &ValidationError{Message: field + " cannot be null"}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", &ValidationError{Message: field + " must be a string"}
	}
	mode := ApprovalMode(strings.TrimSpace(value))
	if err := validateApprovalMode(mode); err != nil {
		return "", err
	}
	return mode, nil
}

func validateApprovalMode(mode ApprovalMode) error {
	switch mode {
	case ApprovalModeFullAccess, ApprovalModeAskForApproval, ApprovalModeApproveForMe:
		return nil
	default:
		return &ValidationError{Message: "approval_mode must be one of full_access, ask_for_approval, approve_for_me"}
	}
}

func supportedModelErrorMessage(path string) string {
	modelIDs := supportedModelIDs()
	return fmt.Sprintf("%s must be one of %s", path, strings.Join(modelIDs[:], ", "))
}

func decodeUnsupportedNullField(field string, raw json.RawMessage) error {
	if string(raw) == "null" {
		return nil
	}
	return &ValidationError{Message: field + " must be null when provided"}
}

func decodeNullableCreateString(field string, raw json.RawMessage) (*string, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, &ValidationError{Message: field + " must be a string"}
	}
	if value == "" {
		return nil, nil
	}
	return &value, nil
}

func decodeNullablePatchString(field string, raw json.RawMessage) (*string, error) {
	return decodeNullableCreateString(field, raw)
}

func decodeStrictCreateArray(field string, raw json.RawMessage) (RawArray, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: field + " cannot be null on create; omit or send []"}
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, &ValidationError{Message: field + " must be an array"}
	}
	if items == nil {
		items = []json.RawMessage{}
	}
	return RawArray(items), nil
}

func decodeArrayPatch(field string, raw json.RawMessage) (RawArray, error) {
	if string(raw) == "null" {
		return RawArray{}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, &ValidationError{Message: field + " must be an array"}
	}
	if items == nil {
		items = []json.RawMessage{}
	}
	return RawArray(items), nil
}

func decodeStrictCreateMetadata(raw json.RawMessage) (StringMap, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: "metadata cannot be null on create; omit or send {}"}
	}
	var typed map[string]string
	if err := json.Unmarshal(raw, &typed); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object of string values"}
	}
	if typed == nil {
		typed = map[string]string{}
	}
	return StringMap(typed), nil
}

func decodeMetadataPatch(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var patch map[string]json.RawMessage
	if err := json.Unmarshal(raw, &patch); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	for key, value := range patch {
		if string(value) == "null" {
			continue
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil {
			return nil, &ValidationError{Message: "metadata[" + key + "] must be a string or null"}
		}
	}
	return patch, nil
}

func mergeMetadataPatch(current StringMap, raw json.RawMessage) (StringMap, error) {
	patch, err := decodeMetadataPatch(raw)
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return StringMap{}, nil
	}
	out := make(StringMap, len(current)+len(patch))
	for key, value := range current {
		out[key] = value
	}
	for key, rawValue := range patch {
		if string(rawValue) == "null" {
			delete(out, key)
			continue
		}
		var text string
		_ = json.Unmarshal(rawValue, &text)
		out[key] = text
	}
	return out, nil
}

type tetralToolsetEntry struct {
	Type   string `json:"type"`
	Family string `json:"family"`
}

type mcpToolsetEntry struct {
	Type          string             `json:"type"`
	MCPServerName string             `json:"mcp_server_name"`
	DefaultConfig *toolDefaultConfig `json:"default_config,omitempty"`
	Configs       []toolConfig       `json:"configs,omitempty"`
}

type toolDefaultConfig struct {
	Enabled          *bool             `json:"enabled,omitempty"`
	PermissionPolicy *permissionPolicy `json:"permission_policy,omitempty"`
}

type toolConfig struct {
	Name             string            `json:"name"`
	Enabled          *bool             `json:"enabled,omitempty"`
	PermissionPolicy *permissionPolicy `json:"permission_policy,omitempty"`
}

type permissionPolicy struct {
	Type string `json:"type"`
}

func canonicalizeTools(input RawArray) (RawArray, error) {
	output := make(RawArray, 0, len(input))
	foundTetralToolset := false
	for index, raw := range input {
		entry := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, &ValidationError{Message: fmt.Sprintf("tools[%d] must be a JSON object", index)}
		}
		toolType, err := decodeRequiredObjectString(entry, "type", fmt.Sprintf("tools[%d]", index))
		if err != nil {
			return nil, err
		}
		switch toolType {
		case "tetral_agent_toolset":
			if foundTetralToolset {
				return nil, &ValidationError{Message: fmt.Sprintf("tools[%d] duplicates the tetral_agent_toolset entry; exactly one is required", index)}
			}
			foundTetralToolset = true
			if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"type": {}, "family": {}}, fmt.Sprintf("tools[%d]", index)); err != nil {
				return nil, err
			}
			family, err := decodeRequiredObjectString(entry, "family", fmt.Sprintf("tools[%d]", index))
			if err != nil {
				return nil, err
			}
			switch family {
			case "claude", "gpt":
			default:
				return nil, &ValidationError{Message: fmt.Sprintf("tools[%d].family is unsupported", index)}
			}
			canonical, marshalErr := json.Marshal(tetralToolsetEntry{Type: toolType, Family: family})
			if marshalErr != nil {
				return nil, marshalErr
			}
			output = append(output, canonical)
		case "mcp_toolset":
			canonical, err := canonicalizeMCPToolset(entry, index)
			if err != nil {
				return nil, err
			}
			output = append(output, canonical)
		case "custom":
			return nil, &ValidationError{Message: fmt.Sprintf("tools[%d].type custom tools are not supported in this stage", index)}
		default:
			return nil, &ValidationError{Message: fmt.Sprintf("tools[%d].type is unsupported", index)}
		}
	}
	return output, nil
}

// TetralToolsetFamily returns the family from an already canonicalized tools
// collection. It applies the same exactly-one law used by config admission.
func TetralToolsetFamily(input RawArray) (string, error) {
	family := ""
	for index, raw := range input {
		var entry tetralToolsetEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return "", err
		}
		if entry.Type != "tetral_agent_toolset" {
			continue
		}
		if family != "" {
			return "", &ValidationError{Message: fmt.Sprintf("tools[%d] duplicates the tetral_agent_toolset entry; exactly one is required", index)}
		}
		switch entry.Family {
		case "claude", "gpt":
			family = entry.Family
		default:
			return "", &ValidationError{Message: fmt.Sprintf("tools[%d].family is unsupported", index)}
		}
	}
	if family == "" {
		return "", &ValidationError{Message: "tools must contain exactly one tetral_agent_toolset entry"}
	}
	return family, nil
}

func canonicalTetralToolsetFamily(canonical RawArray) (string, error) {
	for _, raw := range canonical {
		var entry tetralToolsetEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return "", err
		}
		if entry.Type == "tetral_agent_toolset" {
			return entry.Family, nil
		}
	}
	return "", &ValidationError{Message: "tools must contain exactly one tetral_agent_toolset entry"}
}

func canonicalizeMCPToolset(entry map[string]json.RawMessage, index int) (json.RawMessage, error) {
	path := fmt.Sprintf("tools[%d]", index)
	if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"type": {}, "mcp_server_name": {}, "default_config": {}, "configs": {}}, path); err != nil {
		return nil, err
	}
	name, err := decodeRequiredObjectString(entry, "mcp_server_name", path)
	if err != nil {
		return nil, err
	}
	if count := utf8.RuneCountInString(name); count < 1 || count > maxMCPServerNameCodePoints {
		return nil, &ValidationError{Message: fmt.Sprintf("%s.mcp_server_name must be 1-%d Unicode code points", path, maxMCPServerNameCodePoints)}
	}
	var defaults *toolDefaultConfig
	if rawDefault, ok := entry["default_config"]; ok {
		defaults, err = decodeToolDefaultConfig(rawDefault, path+".default_config")
		if err != nil {
			return nil, err
		}
	}
	var configs []toolConfig
	if rawConfigs, ok := entry["configs"]; ok {
		configs, err = decodeToolConfigs(rawConfigs, path+".configs")
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(mcpToolsetEntry{
		Type:          "mcp_toolset",
		MCPServerName: name,
		DefaultConfig: defaults,
		Configs:       configs,
	})
}

func decodeToolDefaultConfig(raw json.RawMessage, path string) (*toolDefaultConfig, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: path + " must be a JSON object"}
	}
	entry := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, &ValidationError{Message: path + " must be a JSON object"}
	}
	if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"enabled": {}, "permission_policy": {}}, path); err != nil {
		return nil, err
	}
	var output toolDefaultConfig
	if rawEnabled, ok := entry["enabled"]; ok {
		enabled, err := decodeBoolean(rawEnabled, path+".enabled")
		if err != nil {
			return nil, err
		}
		output.Enabled = &enabled
	}
	if rawPolicy, ok := entry["permission_policy"]; ok {
		policy, err := decodePermissionPolicy(rawPolicy, path+".permission_policy")
		if err != nil {
			return nil, err
		}
		output.PermissionPolicy = policy
	}
	return &output, nil
}

func decodeToolConfigs(raw json.RawMessage, path string) ([]toolConfig, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: path + " must be an array"}
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(raw, &rawItems); err != nil {
		return nil, &ValidationError{Message: path + " must be an array"}
	}
	output := make([]toolConfig, 0, len(rawItems))
	seen := map[string]struct{}{}
	for index, rawItem := range rawItems {
		itemPath := fmt.Sprintf("%s[%d]", path, index)
		entry := map[string]json.RawMessage{}
		if err := json.Unmarshal(rawItem, &entry); err != nil {
			return nil, &ValidationError{Message: itemPath + " must be a JSON object"}
		}
		if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"name": {}, "enabled": {}, "permission_policy": {}}, itemPath); err != nil {
			return nil, err
		}
		name, err := decodeRequiredObjectString(entry, "name", itemPath)
		if err != nil {
			return nil, err
		}
		if count := utf8.RuneCountInString(name); count < 1 || count > maxMCPToolConfigNameCodePoints {
			return nil, &ValidationError{Message: fmt.Sprintf("%s.name must be 1-%d Unicode code points", itemPath, maxMCPToolConfigNameCodePoints)}
		}
		if _, ok := seen[name]; ok {
			return nil, &ValidationError{Message: itemPath + ".name is duplicated"}
		}
		seen[name] = struct{}{}
		config := toolConfig{Name: name}
		if rawEnabled, ok := entry["enabled"]; ok {
			enabled, err := decodeBoolean(rawEnabled, itemPath+".enabled")
			if err != nil {
				return nil, err
			}
			config.Enabled = &enabled
		}
		if rawPolicy, ok := entry["permission_policy"]; ok {
			policy, err := decodePermissionPolicy(rawPolicy, itemPath+".permission_policy")
			if err != nil {
				return nil, err
			}
			config.PermissionPolicy = policy
		}
		output = append(output, config)
	}
	return output, nil
}

func decodePermissionPolicy(raw json.RawMessage, path string) (*permissionPolicy, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: path + " must be a JSON object"}
	}
	entry := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, &ValidationError{Message: path + " must be a JSON object"}
	}
	if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"type": {}}, path); err != nil {
		return nil, err
	}
	policyType, err := decodeRequiredObjectString(entry, "type", path)
	if err != nil {
		return nil, err
	}
	if policyType != "always_allow" && policyType != "always_ask" {
		return nil, &ValidationError{Message: path + ".type must be one of always_allow, always_ask"}
	}
	return &permissionPolicy{Type: policyType}, nil
}

type mcpServerURLEntry struct {
	Type string `json:"type"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

func canonicalizeMCPServers(input RawArray) (RawArray, error) {
	if len(input) == 0 {
		return RawArray{}, nil
	}
	output := make(RawArray, 0, len(input))
	for index, raw := range input {
		path := fmt.Sprintf("mcp_servers[%d]", index)
		entry := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, &ValidationError{Message: path + " must be a JSON object"}
		}
		if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"type": {}, "name": {}, "url": {}}, path); err != nil {
			return nil, err
		}
		entryType, err := decodeRequiredObjectString(entry, "type", path)
		if err != nil {
			return nil, err
		}
		if entryType != "url" {
			return nil, &ValidationError{Message: path + `.type must be "url"`}
		}
		name, err := decodeRequiredObjectString(entry, "name", path)
		if err != nil {
			return nil, err
		}
		if count := utf8.RuneCountInString(name); count < 1 || count > maxMCPServerNameCodePoints {
			return nil, &ValidationError{Message: fmt.Sprintf("%s.name must be 1-%d Unicode code points", path, maxMCPServerNameCodePoints)}
		}
		rawURL, err := decodeRequiredObjectString(entry, "url", path)
		if err != nil {
			return nil, err
		}
		canonicalURL, err := endpointurl.CanonicalizePublicURL(rawURL, path+".url")
		if err != nil {
			return nil, &ValidationError{Message: err.Error()}
		}
		catalogURL, ok := mcpCatalogCanonicalURL(canonicalURL)
		if !ok {
			return nil, &ValidationError{Message: path + ".url must reference a supported MCP catalog entry"}
		}
		canonical, err := json.Marshal(mcpServerURLEntry{Type: "url", Name: name, URL: catalogURL})
		if err != nil {
			return nil, err
		}
		output = append(output, canonical)
	}
	return output, nil
}

func mcpCatalogCanonicalURL(canonicalURL string) (string, bool) {
	switch strings.TrimSuffix(canonicalURL, "/") {
	case strings.TrimSuffix(githubMCPCatalogURL, "/"):
		return githubMCPCatalogURL, true
	default:
		return "", false
	}
}

type skillEntry struct {
	Type    string `json:"type"`
	SkillID string `json:"skill_id"`
	Version string `json:"version"`
}

func canonicalizeSkills(input RawArray) (RawArray, error) {
	if len(input) == 0 {
		return RawArray{}, nil
	}
	output := make(RawArray, 0, len(input))
	for index, raw := range input {
		path := fmt.Sprintf("skills[%d]", index)
		entry := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, &ValidationError{Message: path + " must be a JSON object"}
		}
		if err := rejectUnknownObjectKeys(entry, map[string]struct{}{"type": {}, "skill_id": {}, "version": {}}, path); err != nil {
			return nil, err
		}
		entryType, err := decodeRequiredObjectString(entry, "type", path)
		if err != nil {
			return nil, err
		}
		if entryType != "custom" {
			return nil, &ValidationError{Message: path + ".type must be custom"}
		}
		skillID, err := decodeRequiredObjectString(entry, "skill_id", path)
		if err != nil {
			return nil, err
		}
		version := "latest"
		if rawVersion, ok := entry["version"]; ok {
			if string(rawVersion) != "null" {
				version, err = decodeRequiredObjectString(entry, "version", path)
				if err != nil {
					return nil, err
				}
			}
		}
		canonical, err := json.Marshal(skillEntry{Type: "custom", SkillID: skillID, Version: version})
		if err != nil {
			return nil, err
		}
		output = append(output, canonical)
	}
	return output, nil
}

func decodeRequiredObjectString(entry map[string]json.RawMessage, field string, path string) (string, error) {
	raw, ok := entry[field]
	if !ok {
		return "", &ValidationError{Message: path + "." + field + " is required"}
	}
	return decodeRequiredStringField(path+"."+field, raw)
}

func decodeBoolean(raw json.RawMessage, path string) (bool, error) {
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, &ValidationError{Message: path + " must be a boolean"}
	}
	return value, nil
}

func rejectUnknownObjectKeys(actual map[string]json.RawMessage, allowed map[string]struct{}, path string) error {
	for key := range actual {
		if _, ok := allowed[key]; !ok {
			return &ValidationError{Message: path + "." + key + " is an unknown field"}
		}
	}
	return nil
}
