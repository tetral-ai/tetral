// Package id provides prefixed unique ID generation for Engine resources.
package id

import (
	"crypto/rand"
	"encoding/hex"
)

// New generates a unique ID with the given prefix.
// The suffix is 16 random hex characters from crypto/rand.
func New(prefix string) string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return prefix + hex.EncodeToString(bytes)
}
