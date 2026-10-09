package runtimecontrol

import "testing"

func TestCompletionDeliveryIdentityIsScopedToTheSettlingChild(t *testing.T) {
	first := completionDeliveryID("thr_child_a", "rwrite_shared")
	second := completionDeliveryID("thr_child_b", "rwrite_shared")
	if first == second {
		t.Fatalf("sender-scoped completion delivery ids collided: %q", first)
	}
	if first != completionDeliveryID("thr_child_a", "rwrite_shared") {
		t.Fatal("completion delivery identity is not deterministic")
	}
}
