// Package enginetest holds the test clients that stand in for Engine's
// remote peers in cross-process tests: CLI is an HTTP and SSE client in the
// CLI's place, speaking the client connection protocol of internal/protocol
// to computer-hub, directly or through the public edge.
//
// The clients send exactly what a test gives them and decode what Engine
// answers; they keep no protocol state, apply no retry and never repair a
// message, so a test controls every byte that reaches Engine.
package enginetest
