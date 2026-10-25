// Package joint runs builds of the CLI against the real Engine: PostgreSQL,
// MinIO and NATS in containers, auth, api and computer-hub as processes behind
// a public edge of the test process, and CLI machines on the Linux host or on
// a Mac over SSH. Each test writes a record of what it ran and observed into
// TETRAL_ACCEPTANCE_DIR. README.md lists the inputs and how to run a test.
//
// DESIGN RULES:
//   - The module has its own go.mod, so no test profile of the repository and
//     no CI workflow runs it.
//   - A test fails at once, and never skips, when an input it needs is missing
//     or invalid (requireInputs).
//   - Engine runs as it does in a deployment: every request from a machine or
//     a client goes through the edge, which asks auth's Check first, and every
//     service authenticates internal calls through the TokenReview stand-in.
//   - The CLI is a black box: a test drives it only through its commands and
//     reads it only through its status output and Engine's own reads.
//
// FILES:
//
//	harness_test.go           TestMain, the inputs, starting Engine's
//	                          containers and processes, and Linux machines.
//	edge_test.go              the public edge stand-in and its request log.
//	identity_test.go          the TokenReview stand-in and service tokens.
//	mac_test.go               macOS machines over one SSH session with port
//	                          forwards.
//	record_test.go            the record each test writes.
//	controller_client_test.go the Controller's client of the instance routes.
//	developer_client_test.go  the developer's client of the Session routes.
//	client_connection_test.go connection, resume and renewal tests.
//	computer_management_test.go
//	                          instance registration and revocation tests.
package joint
