// Package runtimepath owns the helper's private runtime-state root. Production
// helpers use DefaultRoot; tests replace the root before invoking helper code
// and link test-built helpers with the same root. There is no environment or
// command-line override in the production helper.
package runtimepath

import "sync"

const DefaultRoot = "/tmp/tetral-runtime"

// root is a string variable so test-built helpers can use Go's -X linker flag.
var root = DefaultRoot
var mu sync.RWMutex

func Root() string {
	mu.RLock()
	defer mu.RUnlock()
	return root
}

// SetForTesting changes the root for in-process helper tests. Callers must
// serialize fixtures, finish their helper work, and restore the previous root.
func SetForTesting(value string) {
	mu.Lock()
	defer mu.Unlock()
	root = value
}
