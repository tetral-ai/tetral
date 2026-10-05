//go:build !linux

package agentruntimebridge

import "testing"

func observeSubagentDeclarationResources(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("subagent declaration resource observations unavailable: Linux resource observer unsupported")
		}
	})
}
