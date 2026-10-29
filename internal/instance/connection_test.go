package instance

import (
	"testing"
	"time"
)

func TestGrantRoundsFractionalMillisecondsDown(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	expiry := now.Add(1234*time.Millisecond + 800*time.Microsecond)
	lease, validForMS, ok := Grant(now, expiry)
	if !ok || validForMS != 1234 || !lease.Equal(expiry) {
		t.Fatalf("grant = (%v, %d, %v); want (%v, 1234, true)", lease, validForMS, ok, expiry)
	}
}
