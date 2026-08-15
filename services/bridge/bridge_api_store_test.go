package agentruntimebridge

import (
	"strings"
	"testing"
)

func TestStripInternalProviderFieldsRemovesNestedProviderMetadata(t *testing.T) {
	result := stripInternalProviderFields(`{"status":"completed","provider_metadata":{"raw":"secret"},"nested":{"provider_command_id":"cmd_1","provider_metadata_json":"{\"raw\":\"secret\"}","items":[{"provider_session_id":"sess_1","text":"ok"}]}}`)
	if strings.Contains(result, "provider_command_id") ||
		strings.Contains(result, "provider_session_id") ||
		strings.Contains(result, "provider_metadata") ||
		strings.Contains(result, "provider_metadata_json") {
		t.Fatalf("provider metadata leaked after recursive strip: %s", result)
	}
	if !strings.Contains(result, `"text":"ok"`) {
		t.Fatalf("recursive strip removed safe content: %s", result)
	}
}
