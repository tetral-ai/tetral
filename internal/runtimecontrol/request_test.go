package runtimecontrol

import (
	"testing"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestScopeForThreadPreservesIndependentProcessCustody(t *testing.T) {
	parent := &bridgev1.RuntimeScope{WorkspaceId: "workspace", SessionId: "session", SessionThreadId: "parent", Binding: &bridgev1.RuntimeBindingRef{BindingId: "binding", BindingGeneration: 9, TargetPodUid: "pod", RuntimeProcessId: "boot"}}
	child := ScopeForThread(parent, "reviewer")
	if child.WorkspaceId != parent.WorkspaceId || child.SessionId != parent.SessionId || child.SessionThreadId != "reviewer" || child.Binding.BindingId != "binding" || child.Binding.BindingGeneration != 9 || child.Binding.TargetPodUid != "pod" || child.Binding.RuntimeProcessId != "boot" {
		t.Fatalf("child lost custody: %v", child)
	}
	// Child derivation must not share a mutable binding or change its parent's
	// Thread when reviewer/closeout code adjusts the derived scope.
	child.Binding.RuntimeProcessId = "replacement"
	child.SessionThreadId = "other"
	if parent.Binding.RuntimeProcessId != "boot" || parent.SessionThreadId != "parent" {
		t.Fatal("derived scope mutated parent authority")
	}
	sibling := ScopeForThread(parent, "sibling")
	if sibling.Binding.RuntimeProcessId != "boot" {
		t.Fatal("sibling inherited modified child authority")
	}
}
