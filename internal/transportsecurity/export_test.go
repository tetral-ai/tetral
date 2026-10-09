package transportsecurity

// ReloadFailureCount exposes the rejected-observation counter to the external
// test package without adding a production accessor.
func ReloadFailureCount(o *Owner) uint64 { return o.reloadFailures.Load() }
