package sessionevent

import (
	"strings"
	"testing"
)

func TestNewEventIDUsesCanonicalSessionEventPrefix(t *testing.T) {
	first := NewEventID()
	second := NewEventID()
	if !strings.HasPrefix(first, IDPrefix) {
		t.Fatalf("NewEventID() = %q; want prefix %q", first, IDPrefix)
	}
	if !strings.HasPrefix(first, "sevt_") {
		t.Fatalf("NewEventID() = %q; want canonical sevt_ prefix", first)
	}
	if first == second {
		t.Fatalf("NewEventID generated duplicate IDs: %q", first)
	}
}
