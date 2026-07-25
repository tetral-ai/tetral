package gitproxy

import (
	"testing"
	"time"
)

// The git-proxy contract fixes these numbers, not the identifiers that carry
// them: a rename is refactoring, a value change is a contract change. This test
// pins the values so the second cannot happen by accident.
func TestContractConstantValuesStayPinned(t *testing.T) {
	if DefaultDrainGraceSeconds != 1800 {
		t.Fatalf("DefaultDrainGraceSeconds = %d; want 1800", DefaultDrainGraceSeconds)
	}
	if DefaultTicketRotationGraceSeconds != 1800 {
		t.Fatalf("DefaultTicketRotationGraceSeconds = %d; want 1800", DefaultTicketRotationGraceSeconds)
	}
	if HeaderReadTimeout != 30*time.Second {
		t.Fatalf("HeaderReadTimeout = %s; want 30s", HeaderReadTimeout)
	}
	if UpstreamDialTimeout != 10*time.Second {
		t.Fatalf("UpstreamDialTimeout = %s; want 10s", UpstreamDialTimeout)
	}
	if IdleProgressTimeout != 120*time.Second {
		t.Fatalf("IdleProgressTimeout = %s; want 120s", IdleProgressTimeout)
	}
	if MaxRequestBodyBytes != 2*1024*1024*1024 {
		t.Fatalf("MaxRequestBodyBytes = %d; want 2GiB", MaxRequestBodyBytes)
	}
	if MaxConnsPerTicket != 16 {
		t.Fatalf("MaxConnsPerTicket = %d; want 16", MaxConnsPerTicket)
	}
	if DrainGraceSeconds != DefaultDrainGraceSeconds {
		t.Fatalf("DrainGraceSeconds = %d; want %d", DrainGraceSeconds, DefaultDrainGraceSeconds)
	}
}
