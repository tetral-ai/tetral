package environment

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"unicode/utf8"
)

const (
	maxMetadataPairs           = 16
	maxMetadataKeyCodePoints   = 64
	maxMetadataValueCodePoints = 512
)

var allowedCreateFields = map[string]struct{}{
	"name":        {},
	"description": {},
	"config":      {},
	"metadata":    {},
}

var allowedUpdateFields = map[string]struct{}{
	"name":        {},
	"description": {},
	"config":      {},
	"metadata":    {},
}

var allowedConfigFields = map[string]struct{}{
	"type":       {},
	"networking": {},
	"packages":   {},
}

var allowedNetworkingFields = map[string]struct{}{
	"type":               {},
	"network_allow_list": {},
}

var allowedPackageManagers = map[string]struct{}{
	"apt":   {},
	"cargo": {},
	"gem":   {},
	"go":    {},
	"npm":   {},
	"pip":   {},
}

// DecodeCreateEnvironmentRequest strictly decodes and normalizes the
// public create body.
func DecodeCreateEnvironmentRequest(body []byte) (CreateEnvironmentRequest, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return CreateEnvironmentRequest{}, &ValidationError{Message: "invalid request body"}
	}
	if raw == nil {
		return CreateEnvironmentRequest{}, &ValidationError{Message: "invalid request body"}
	}
	for key := range raw {
		if _, ok := allowedCreateFields[key]; !ok {
			return CreateEnvironmentRequest{}, &ValidationError{Message: fmt.Sprintf("unknown field %q", key)}
		}
	}

	request := CreateEnvironmentRequest{}
	if value, ok := raw["name"]; ok {
		name, err := decodeNullableEnvironmentString("name", value)
		if err != nil {
			return CreateEnvironmentRequest{}, err
		}
		request.Name = name
	}
	if value, ok := raw["description"]; ok {
		description, err := decodeNullableEnvironmentString("description", value)
		if err != nil {
			return CreateEnvironmentRequest{}, err
		}
		request.Description = description
	}
	if value, ok := raw["config"]; ok {
		cfg, err := decodeCreateConfig(value)
		if err != nil {
			return CreateEnvironmentRequest{}, err
		}
		request.Config = cfg
	}
	if value, ok := raw["metadata"]; ok {
		metadata, err := decodeCreateMetadata(value)
		if err != nil {
			return CreateEnvironmentRequest{}, err
		}
		request.Metadata = metadata
	}
	return NormalizeCreateEnvironmentRequest(request)
}

// DecodeUpdateEnvironmentRequest strictly decodes a partial update patch.
func DecodeUpdateEnvironmentRequest(body []byte) (EnvironmentPatch, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return EnvironmentPatch{}, &ValidationError{Message: "invalid request body"}
	}
	if raw == nil {
		return EnvironmentPatch{}, &ValidationError{Message: "invalid request body"}
	}
	for key, value := range raw {
		if _, ok := allowedUpdateFields[key]; !ok {
			return EnvironmentPatch{}, &ValidationError{Message: fmt.Sprintf("unknown field %q", key)}
		}
		switch key {
		case "name":
			if _, err := decodeNullableEnvironmentString("name", value); err != nil {
				return EnvironmentPatch{}, err
			}
		case "description":
			if _, err := decodeNullableEnvironmentString("description", value); err != nil {
				return EnvironmentPatch{}, err
			}
		case "config":
			if string(value) != "null" {
				if err := validateConfigPatch(value); err != nil {
					return EnvironmentPatch{}, err
				}
			}
		case "metadata":
			if _, err := decodeMetadataPatch(value); err != nil {
				return EnvironmentPatch{}, err
			}
		}
	}
	return EnvironmentPatch{raw: raw}, nil
}

// NormalizeCreateEnvironmentRequest validates and fills every omitted
// Environment create field that has a contract default.
func NormalizeCreateEnvironmentRequest(request CreateEnvironmentRequest) (CreateEnvironmentRequest, error) {
	cfg, err := NormalizeEnvironmentConfig(request.Config)
	if err != nil {
		return CreateEnvironmentRequest{}, err
	}
	metadata := normalizeMetadata(request.Metadata)
	if err := validateMetadataLimits(metadata); err != nil {
		return CreateEnvironmentRequest{}, err
	}
	request.Config = cfg
	request.Metadata = metadata
	return request, nil
}

// Materialize applies an update patch onto the current Environment.
func (p EnvironmentPatch) Materialize(current Environment) (Environment, error) {
	out := current
	out.Config = normalizeConfigContainers(current.Config)
	out.Metadata = normalizeMetadata(current.Metadata)

	if value, ok := p.raw["name"]; ok {
		name, err := decodeNullableEnvironmentString("name", value)
		if err != nil {
			return Environment{}, err
		}
		out.Name = name
	}
	if value, ok := p.raw["description"]; ok {
		description, err := decodeNullableEnvironmentString("description", value)
		if err != nil {
			return Environment{}, err
		}
		out.Description = description
	}
	if value, ok := p.raw["config"]; ok {
		var cfg EnvironmentConfig
		var err error
		if string(value) == "null" {
			cfg, err = NormalizeEnvironmentConfig(EnvironmentConfig{})
		} else {
			cfg, err = materializeConfigPatch(out.Config, value)
		}
		if err != nil {
			return Environment{}, err
		}
		out.Config = cfg
	}
	if value, ok := p.raw["metadata"]; ok {
		metadata, err := mergeMetadataPatch(out.Metadata, value)
		if err != nil {
			return Environment{}, err
		}
		out.Metadata = metadata
	}

	cfg, err := NormalizeEnvironmentConfig(out.Config)
	if err != nil {
		return Environment{}, err
	}
	if err := validateMetadataLimits(out.Metadata); err != nil {
		return Environment{}, err
	}
	out.Config = cfg
	return out, nil
}

// Environment artifact input and reuse rule.
//
// Bounds which config changes force a fresh provider artifact build versus
// which reuse or only re-record an existing one. `packages` is the sole
// artifact input: a packages change yields a new artifact_input_hash and can
// force a build. `networking` is not an artifact input: a networking-only
// change leaves artifact_input_hash unchanged, so it is never itself a build
// trigger. Admission then resolves on that unchanged hash like any other:
// admitted ready via the default or a reusable ready artifact, ridden as
// pending against a matching in-flight build, or enqueuing a fresh
// environment_build when the only same-hash prior generation is failed with no
// reusable ready artifact — always recording the updated
// runtime_network_policy_json.
//
// Derived: artifact_input_hash is the SHA-256 of the normalized packages JSON
// alone (environmentArtifactSnapshot); normalized_config_hash covers the whole
// config and does not gate rebuilds. The split of a config into its packages
// and networking destinies is fixed by NormalizeEnvironmentConfig below. Reuse
// of a ready provider_artifact_ref is limited to the same
// (workspace_id, environment_id) lineage, an earlier `ready` generation, and a
// matching artifact_input_hash (reusableEnvironmentArtifactRef). A matching
// in-flight pending/building build in the same lineage is ridden without a
// second build (reusableInFlightEnvironmentArtifact); otherwise a pending
// generation enqueues environment_build.
//
// UPDATE-WITH:
//   internal/environment/postgresql_store.go (environmentArtifactSnapshot,
//     createEnvironmentArtifactAdmission, reusableEnvironmentArtifactRef,
//     reusableInFlightEnvironmentArtifact, enqueueEnvironmentBuild)
//   internal/queue (KindEnvironmentBuild)

// NormalizeEnvironmentConfig returns a complete canonical cloud config.
func NormalizeEnvironmentConfig(cfg EnvironmentConfig) (EnvironmentConfig, error) {
	if cfg.Type == "" {
		cfg.Type = "cloud"
	}
	if cfg.Type != "cloud" {
		return EnvironmentConfig{}, &ValidationError{Message: "config.type must be \"cloud\""}
	}
	if cfg.Networking == nil {
		cfg.Networking = &NetworkingConfig{Type: "unrestricted"}
	}
	networking, err := normalizeNetworking(*cfg.Networking)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	cfg.Networking = &networking
	if cfg.Packages == nil {
		cfg.Packages = PackageMap{}
	}
	packages, err := normalizePackages(cfg.Packages)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	cfg.Packages = packages
	return cfg, nil
}

func normalizeConfigContainers(cfg EnvironmentConfig) EnvironmentConfig {
	if cfg.Networking == nil {
		cfg.Networking = &NetworkingConfig{Type: "unrestricted"}
	}
	if cfg.Packages == nil {
		cfg.Packages = PackageMap{}
	}
	if cfg.Type == "" {
		cfg.Type = "cloud"
	}
	return cfg
}

func decodeRequiredString(field string, raw json.RawMessage) (string, error) {
	if string(raw) == "null" {
		return "", &ValidationError{Message: field + " cannot be null"}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", &ValidationError{Message: field + " must be a string"}
	}
	return value, nil
}

func decodeNullableEnvironmentString(field string, raw json.RawMessage) (string, error) {
	if string(raw) == "null" {
		return "", nil
	}
	return decodeRequiredString(field, raw)
}

func decodeCreateConfig(raw json.RawMessage) (EnvironmentConfig, error) {
	if string(raw) == "null" {
		return EnvironmentConfig{}, &ValidationError{Message: "config cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return EnvironmentConfig{}, &ValidationError{Message: "config must be an object"}
	}
	for key := range obj {
		if _, ok := allowedConfigFields[key]; !ok {
			return EnvironmentConfig{}, &ValidationError{Message: fmt.Sprintf("unknown config field %q", key)}
		}
	}
	cfg := EnvironmentConfig{}
	if value, ok := obj["type"]; ok {
		configType, err := decodeRequiredString("config.type", value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		if configType != "cloud" {
			return EnvironmentConfig{}, &ValidationError{Message: "config.type must be \"cloud\""}
		}
		cfg.Type = configType
	}
	if value, ok := obj["networking"]; ok {
		networking, err := decodeNetworking(value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		cfg.Networking = &networking
	}
	if value, ok := obj["packages"]; ok {
		packages, err := decodePackages(value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		cfg.Packages = packages
	}
	return NormalizeEnvironmentConfig(cfg)
}

func validateConfigPatch(raw json.RawMessage) error {
	if string(raw) == "null" {
		return &ValidationError{Message: "config cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return &ValidationError{Message: "config must be an object"}
	}
	for key, value := range obj {
		if _, ok := allowedConfigFields[key]; !ok {
			return &ValidationError{Message: fmt.Sprintf("unknown config field %q", key)}
		}
		switch key {
		case "type":
			configType, err := decodeRequiredString("config.type", value)
			if err != nil {
				return err
			}
			if configType != "cloud" {
				return &ValidationError{Message: "config.type must be \"cloud\""}
			}
		case "networking":
			if _, err := decodeNetworking(value); err != nil {
				return err
			}
		case "packages":
			if _, err := decodePackages(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func materializeConfigPatch(current EnvironmentConfig, raw json.RawMessage) (EnvironmentConfig, error) {
	var obj map[string]json.RawMessage
	_ = json.Unmarshal(raw, &obj)
	out := normalizeConfigContainers(current)
	if value, ok := obj["type"]; ok {
		configType, err := decodeRequiredString("config.type", value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		out.Type = configType
	}
	if value, ok := obj["networking"]; ok {
		networking, err := decodeNetworking(value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		out.Networking = &networking
	}
	if value, ok := obj["packages"]; ok {
		packages, err := decodePackages(value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		out.Packages = packages
	}
	return NormalizeEnvironmentConfig(out)
}

func decodeNetworking(raw json.RawMessage) (NetworkingConfig, error) {
	if string(raw) == "null" {
		return NetworkingConfig{}, &ValidationError{Message: "config.networking cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return NetworkingConfig{}, &ValidationError{Message: "config.networking must be an object"}
	}
	for key := range obj {
		if _, ok := allowedNetworkingFields[key]; !ok {
			return NetworkingConfig{}, &ValidationError{Message: fmt.Sprintf("unknown networking field %q", key)}
		}
	}
	rawType, ok := obj["type"]
	if !ok {
		return NetworkingConfig{}, &ValidationError{Message: "config.networking.type is required"}
	}
	networkingType, err := decodeRequiredString("config.networking.type", rawType)
	if err != nil {
		return NetworkingConfig{}, err
	}
	switch networkingType {
	case "unrestricted":
		if err := rejectNetworkAllowListField(obj); err != nil {
			return NetworkingConfig{}, err
		}
		return NetworkingConfig{Type: "unrestricted"}, nil
	case "blocked":
		if err := rejectNetworkAllowListField(obj); err != nil {
			return NetworkingConfig{}, err
		}
		return NetworkingConfig{Type: "blocked"}, nil
	case "cidr_allow_list":
	default:
		return NetworkingConfig{}, &ValidationError{Message: "config.networking.type must be \"unrestricted\", \"blocked\", or \"cidr_allow_list\""}
	}
	networking := NetworkingConfig{Type: networkingType}
	if value, ok := obj["network_allow_list"]; ok {
		allowList, err := decodeRequiredString("config.networking.network_allow_list", value)
		if err != nil {
			return NetworkingConfig{}, err
		}
		networking.NetworkAllowList = allowList
	}
	return normalizeNetworking(networking)
}

func rejectNetworkAllowListField(obj map[string]json.RawMessage) error {
	if _, ok := obj["network_allow_list"]; ok {
		return &ValidationError{Message: "config.networking.network_allow_list is only valid for cidr_allow_list networking"}
	}
	return nil
}

func normalizeNetworking(networking NetworkingConfig) (NetworkingConfig, error) {
	switch networking.Type {
	case "", "unrestricted":
		if networking.NetworkAllowList != "" {
			return NetworkingConfig{}, &ValidationError{Message: "unrestricted networking must not include network_allow_list"}
		}
		return NetworkingConfig{Type: "unrestricted"}, nil
	case "blocked":
		if networking.NetworkAllowList != "" {
			return NetworkingConfig{}, &ValidationError{Message: "blocked networking must not include network_allow_list"}
		}
		return NetworkingConfig{Type: "blocked"}, nil
	case "cidr_allow_list":
		normalized, err := normalizeCIDRAllowList(networking.NetworkAllowList)
		if err != nil {
			return NetworkingConfig{}, err
		}
		return NetworkingConfig{Type: "cidr_allow_list", NetworkAllowList: normalized}, nil
	default:
		return NetworkingConfig{}, &ValidationError{Message: "config.networking.type must be \"unrestricted\", \"blocked\", or \"cidr_allow_list\""}
	}
}

func normalizeCIDRAllowList(raw string) (string, error) {
	if strings.ContainsFunc(raw, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return "", &ValidationError{Message: "config.networking.network_allow_list entries must not contain control characters"}
	}
	parts := strings.Split(raw, ",")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			return "", &ValidationError{Message: "config.networking.network_allow_list must be a comma-separated CIDR list"}
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return "", &ValidationError{Message: "config.networking.network_allow_list entries must be CIDR prefixes"}
		}
		normalized = append(normalized, prefix.String())
	}
	return strings.Join(normalized, ","), nil
}

func decodePackages(raw json.RawMessage) (PackageMap, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: "config.packages cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, &ValidationError{Message: "config.packages must be an object"}
	}
	out := make(PackageMap, len(obj))
	for manager, value := range obj {
		if manager == "type" {
			marker, err := decodeRequiredString("config.packages.type", value)
			if err != nil {
				return nil, err
			}
			if marker != "packages" {
				return nil, &ValidationError{Message: "config.packages.type must be \"packages\""}
			}
			continue
		}
		if _, ok := allowedPackageManagers[manager]; !ok {
			return nil, &ValidationError{Message: fmt.Sprintf("unsupported package manager %q", manager)}
		}
		packages, err := decodeStringArray("packages."+manager, value)
		if err != nil {
			return nil, err
		}
		out[manager] = packages
	}
	return out, nil
}

func normalizePackages(packages PackageMap) (PackageMap, error) {
	out := make(PackageMap, len(packages))
	for manager, entries := range packages {
		if _, ok := allowedPackageManagers[manager]; !ok {
			return nil, &ValidationError{Message: fmt.Sprintf("unsupported package manager %q", manager)}
		}
		if entries == nil {
			entries = []string{}
		}
		copyEntries := append([]string(nil), entries...)
		out[manager] = copyEntries
	}
	return out, nil
}

func decodeStringArray(field string, raw json.RawMessage) ([]string, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: field + " must be an array of strings"}
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, &ValidationError{Message: field + " must be an array of strings"}
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if string(item) == "null" {
			return nil, &ValidationError{Message: field + " entries must be strings"}
		}
		var value string
		if err := json.Unmarshal(item, &value); err != nil {
			return nil, &ValidationError{Message: field + " entries must be strings"}
		}
		out = append(out, value)
	}
	if out == nil {
		out = []string{}
	}
	return out, nil
}

func decodeCreateMetadata(raw json.RawMessage) (StringMap, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: "metadata cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	out := make(StringMap, len(obj))
	for key, value := range obj {
		if string(value) == "null" {
			return nil, &ValidationError{Message: "metadata[" + key + "] must be a string"}
		}
		var str string
		if err := json.Unmarshal(value, &str); err != nil {
			return nil, &ValidationError{Message: "metadata[" + key + "] must be a string"}
		}
		out[key] = str
	}
	if err := validateMetadataLimits(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeMetadataPatch(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: "metadata cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	for key, value := range obj {
		if string(value) == "null" {
			continue
		}
		var str string
		if err := json.Unmarshal(value, &str); err != nil {
			return nil, &ValidationError{Message: "metadata[" + key + "] must be a string or null"}
		}
	}
	return obj, nil
}

func mergeMetadataPatch(current StringMap, raw json.RawMessage) (StringMap, error) {
	patch, err := decodeMetadataPatch(raw)
	if err != nil {
		return nil, err
	}
	out := normalizeMetadata(current)
	for key, value := range patch {
		if string(value) == "null" {
			delete(out, key)
			continue
		}
		var str string
		_ = json.Unmarshal(value, &str)
		if str == "" {
			delete(out, key)
			continue
		}
		out[key] = str
	}
	if err := validateMetadataLimits(out); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeMetadata(metadata StringMap) StringMap {
	out := make(StringMap, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

func validateMetadataLimits(metadata StringMap) error {
	if len(metadata) > maxMetadataPairs {
		return &ValidationError{Message: fmt.Sprintf("metadata must have <= %d pairs", maxMetadataPairs)}
	}
	for key, value := range metadata {
		if utf8.RuneCountInString(key) > maxMetadataKeyCodePoints {
			return &ValidationError{Message: fmt.Sprintf("metadata key must be <= %d Unicode code points", maxMetadataKeyCodePoints)}
		}
		if utf8.RuneCountInString(value) > maxMetadataValueCodePoints {
			return &ValidationError{Message: fmt.Sprintf("metadata value must be <= %d Unicode code points", maxMetadataValueCodePoints)}
		}
	}
	return nil
}
