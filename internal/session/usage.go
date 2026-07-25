package session

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type Usage struct {
	InputTokens          int64              `json:"input_tokens"`
	OutputTokens         int64              `json:"output_tokens"`
	CacheCreation        UsageCacheCreation `json:"cache_creation"`
	CacheReadInputTokens int64              `json:"cache_read_input_tokens"`
	ServerToolUse        UsageServerToolUse `json:"server_tool_use"`
}

type UsageCacheCreation struct {
	Ephemeral1hInputTokens *int64 `json:"ephemeral_1h_input_tokens"`
	Ephemeral5mInputTokens *int64 `json:"ephemeral_5m_input_tokens"`
}

type UsageServerToolUse struct {
	WebSearchRequests int64 `json:"web_search_requests"`
	WebFetchRequests  int64 `json:"web_fetch_requests"`
}

func parseUsageJSON(raw string) (Usage, error) {
	if raw == "" {
		raw = "{}"
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return Usage{}, err
	}
	usage := Usage{}
	var err error
	if usage.InputTokens, err = usageJSONInt64(payload, "input_tokens"); err != nil {
		return Usage{}, err
	}
	if usage.OutputTokens, err = usageJSONInt64(payload, "output_tokens"); err != nil {
		return Usage{}, err
	}
	if usage.CacheReadInputTokens, err = usageJSONInt64(payload, "cache_read_input_tokens"); err != nil {
		return Usage{}, err
	}
	if cacheCreation, ok := payload["cache_creation"]; ok && cacheCreation != nil {
		nested, ok := cacheCreation.(map[string]any)
		if !ok {
			return Usage{}, fmt.Errorf("usage cache_creation must be an object")
		}
		if usage.CacheCreation.Ephemeral1hInputTokens, err = usageJSONOptionalInt64(nested, "ephemeral_1h_input_tokens"); err != nil {
			return Usage{}, err
		}
		if usage.CacheCreation.Ephemeral5mInputTokens, err = usageJSONOptionalInt64(nested, "ephemeral_5m_input_tokens"); err != nil {
			return Usage{}, err
		}
	}
	if usage.CacheCreation.Ephemeral1hInputTokens == nil {
		if flat, err := usageJSONOptionalInt64(payload, "cache_creation_input_tokens"); err != nil {
			return Usage{}, err
		} else if flat != nil {
			usage.CacheCreation.Ephemeral1hInputTokens = flat
		}
	}
	if serverToolUse, ok := payload["server_tool_use"]; ok && serverToolUse != nil {
		nested, ok := serverToolUse.(map[string]any)
		if !ok {
			return Usage{}, fmt.Errorf("usage server_tool_use must be an object")
		}
		if usage.ServerToolUse.WebSearchRequests, err = usageJSONInt64(nested, "web_search_requests"); err != nil {
			return Usage{}, err
		}
		if usage.ServerToolUse.WebFetchRequests, err = usageJSONInt64(nested, "web_fetch_requests"); err != nil {
			return Usage{}, err
		}
	} else {
		if usage.ServerToolUse.WebSearchRequests, err = usageJSONInt64(payload, "web_search_requests"); err != nil {
			return Usage{}, err
		}
		if usage.ServerToolUse.WebFetchRequests, err = usageJSONInt64(payload, "web_fetch_requests"); err != nil {
			return Usage{}, err
		}
	}
	return usage, nil
}

func usageJSONInt64(payload map[string]any, key string) (int64, error) {
	value, ok := payload[key]
	if !ok || value == nil {
		return 0, nil
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("usage %s must be an integer", key)
	}
	parsed, err := number.Int64()
	if err != nil {
		return 0, fmt.Errorf("usage %s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func usageJSONOptionalInt64(payload map[string]any, key string) (*int64, error) {
	value, ok := payload[key]
	if !ok || value == nil {
		return nil, nil
	}
	parsed, err := usageJSONInt64(payload, key)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
