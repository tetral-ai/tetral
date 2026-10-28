// Package wire holds the reference control server's own types for the
// protocol between tetral and Engine, one type for each schema definition
// that a protocol fixture names. Decode and DecodeAnswer read a message and
// Encode writes one; README.md states the design rules and where each
// concern lives.
package wire
