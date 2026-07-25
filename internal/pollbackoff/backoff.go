package pollbackoff

import "time"

type Backoff struct {
	initial time.Duration
	maximum time.Duration
	next    time.Duration
}

func New(initial time.Duration, maximum time.Duration) *Backoff {
	if initial <= 0 {
		initial = time.Second
	}
	if maximum < initial {
		maximum = initial
	}
	return &Backoff{initial: initial, maximum: maximum, next: initial}
}

func (b *Backoff) Next(hadWork bool) time.Duration {
	if b == nil {
		return time.Second
	}
	if hadWork {
		b.next = b.initial
		return b.initial
	}
	current := b.next
	if current <= 0 {
		current = b.initial
	}
	if current < b.maximum {
		next := current * 2
		if next < current || next > b.maximum {
			next = b.maximum
		}
		b.next = next
	}
	return current
}
