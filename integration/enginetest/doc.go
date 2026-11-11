// Package enginetest holds the test clients that stand in for Engine's
// remote peers in cross-process tests: CLI is an HTTP and SSE client in the
// CLI's place, speaking the client connection protocol of internal/protocol
// to computer-hub, directly or through the public edge; Runtime is a gRPC
// client in one Runtime pod's place, driving Bridge's machine-bound tool
// calls before the Runtime makes them.
//
// The clients send exactly what a test gives them and decode what Engine
// answers; they apply no retry and never repair a message, so a test
// controls every byte that reaches Engine. A CLI holds every message it
// sends and receives to the contract of protocol/ and fails its test for
// each one outside it (README.md, "The contract check").
package enginetest
