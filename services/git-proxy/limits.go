package gitproxy

import "sync"

type TicketConnectionLimiter struct {
	mu     sync.Mutex
	limit  int
	counts map[string]int
}

func NewTicketConnectionLimiter(limit int) *TicketConnectionLimiter {
	if limit <= 0 {
		limit = MaxConnsPerTicket
	}
	return &TicketConnectionLimiter{
		limit:  limit,
		counts: make(map[string]int),
	}
}

func (l *TicketConnectionLimiter) Acquire(ticketID string) (func(), bool) {
	if l == nil {
		return func() {}, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts == nil {
		l.counts = make(map[string]int)
	}
	if l.counts[ticketID] >= l.limit {
		return nil, false
	}
	l.counts[ticketID]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.counts[ticketID] <= 1 {
			delete(l.counts, ticketID)
			return
		}
		l.counts[ticketID]--
	}, true
}
