package storage

import "time"

// DurablePrecision is the resolution of a durable timestamp column. PostgreSQL
// stores `timestamptz` with microsecond resolution, so a Go timestamp carrying
// nanoseconds cannot survive a write/read round trip unchanged.
const DurablePrecision = time.Microsecond

// Now mints a timestamp at durable precision. Every timestamp the platform
// creates for persistence comes from here, so the value a caller receives from
// a write is byte-identical to the value a later read returns. Minting with
// `time.Now()` directly reintroduces the mismatch: the response would carry
// nanoseconds the database silently drops, and a client comparing the two would
// see the timestamp change on its own.
func Now() time.Time {
	return time.Now().UTC().Truncate(DurablePrecision)
}

// Durable truncates an externally supplied timestamp to durable precision. Use
// it when a caller-provided or test-injected time is written to the database and
// the same value is handed back.
func Durable(value time.Time) time.Time {
	return value.UTC().Truncate(DurablePrecision)
}
