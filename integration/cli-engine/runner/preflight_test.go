package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// TestMacRoundTripPreflight starts the Mac lane only when the shortest of
// the timed connects is at most -max-mac-rtt: a burst of slow connects on
// a local path passes, a path whose every connect is slow fails.
func TestMacRoundTripPreflight(t *testing.T) {
	const ms = time.Millisecond
	for _, c := range []struct {
		name     string
		samples  []time.Duration
		shortest time.Duration
		refused  bool
	}{
		{"local path", []time.Duration{66 * ms, 40 * ms, 37 * ms, 4 * ms, 31 * ms}, 4 * ms, false},
		{"hotspot", []time.Duration{390 * ms, 381 * ms, 385 * ms, 379 * ms, 383 * ms}, 379 * ms, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &runner{cfg: config{maxMacRoundTrip: 20 * ms}, logOut: io.Discard, started: time.Now()}
			next := 0
			err := r.checkMacRoundTrip(context.Background(), func(context.Context) (time.Duration, error) {
				next++
				return c.samples[next-1], nil
			})
			if next != len(c.samples) || r.macRoundTrip != c.shortest {
				t.Errorf("round trip %s after %d connects; want the shortest %s of %d", r.macRoundTrip, next, c.shortest, len(c.samples))
			}
			switch {
			case !c.refused && err != nil:
				t.Errorf("refused the Mac lane: %v", err)
			case c.refused && err == nil:
				t.Error("started the Mac lane")
			case c.refused && !strings.Contains(err.Error(), "is 379ms, above 20ms"):
				t.Errorf("refusal %q does not name the round trip and the limit", err)
			}
		})
	}
}
