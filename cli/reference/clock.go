package reference

import (
	"sync"
	"time"
)

// Clock is the server's only clock. It moves only when Advance moves it, so
// every lease, grant, heartbeat and stream age changes exactly when a test
// says so.
type Clock struct {
	// mu guards now and changed: the control listener advances the clock
	// while the protocol listener's handlers read it.
	mu      sync.Mutex
	now     time.Time
	changed chan struct{}
}

// NewClock starts a clock at start.
func NewClock(start time.Time) *Clock {
	return &Clock{now: start, changed: make(chan struct{})}
}

// Now is the clock's time.
func (clock *Clock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

// Advance moves the clock forward by step and closes the channel that
// Changed gave.
func (clock *Clock) Advance(step time.Duration) time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if step > 0 {
		clock.now = clock.now.Add(step)
	}
	close(clock.changed)
	clock.changed = make(chan struct{})
	return clock.now
}

// Changed is closed at the next Advance.
func (clock *Clock) Changed() <-chan struct{} {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.changed
}
