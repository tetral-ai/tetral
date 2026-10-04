package integration

import (
	"testing"
	"time"
)

// The public approval API commits each decision after the actual pending
// Runtime residency has been removed. Queue delivery must reconstruct it.
func TestContentPermissionColdRecovery(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	provider := &contentCycleProvider{contentE2EProvider: contentE2EProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}}, entries: map[string]*contentCycleCommand{}, root: t.TempDir()}
	chain := startContentE2EWithOptions(t, "text", false, false, contentE2EOptions{Provider: provider, StopProvider: provider.finishAll, Budget: 120 * time.Second, ApprovalMode: "ask_for_approval", Runtime: map[string]any{"controlCommands": true, "approvalMode": "ask_for_approval"}, Gateway: map[string]any{"sessionScenarioPlans": true, "recordContext": false, "measureResources": true, "memorySampleIntervalMs": 1000}})
	for _, kind := range []string{"allow-cold", "deny-cold"} {
		t.Run(kind, func(t *testing.T) { runContentResourceCycle(t, chain, provider, chain.newSession(t), kind, nil, false) })
	}
}
