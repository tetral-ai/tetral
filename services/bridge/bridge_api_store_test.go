package agentruntimebridge

import (
	"os"
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

func TestBridgeSessionMessageProjectionInsertsAreConflictSafe(t *testing.T) {
	assertConflictSafeInsert := func(t *testing.T, source string, signature string) {
		t.Helper()
		body := sourceFunctionBody(t, source, signature)
		for _, fragment := range []string{
			"ON CONFLICT (workspace_id, session_id, session_thread_id, source_event_id)",
			"WHERE source_event_id IS NOT NULL",
			"DO NOTHING",
		} {
			if !strings.Contains(body, fragment) {
				t.Fatalf("%s missing source_event_id conflict protection fragment %q in:\n%s", signature, fragment, body)
			}
		}
	}

	for filename, signatures := range map[string][]string{
		"bridge_api_events.go": {
			"func insertSessionMessageProjectionTx",
		},
	} {
		bridgeSourceBytes, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("read %s: %v", filename, err)
		}
		for _, signature := range signatures {
			assertConflictSafeInsert(t, string(bridgeSourceBytes), signature)
		}
	}

	runtimeSourceBytes, err := os.ReadFile("runtime_delivery.go")
	if err != nil {
		t.Fatalf("read runtime_delivery.go: %v", err)
	}
	assertConflictSafeInsert(t, string(runtimeSourceBytes), "func insertPendingToolTerminalMessageTx")
}
