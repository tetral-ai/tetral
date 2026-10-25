# integration/cli-engine

End-to-end tests of the CLI against the real Engine. Each test starts Engine
as it runs in a deployment, drives one or more CLI machines through the CLI's
own commands, and checks what Engine and the CLI then show. The tests run
builds of the CLI that the caller supplies; they never build the CLI.

The directory is its own Go module (package `joint`), so no test profile of
the repository and no CI workflow runs it.

## What a test starts

- PostgreSQL, MinIO and NATS in containers (`internal/testinfra`), the
  database prepared by `tetral-db-prepare` with one role per workload and the
  workspace created by `tetral-bootstrap`.
- `auth`, `api` and `computer-hub` as processes built from this checkout, each
  on its own database role.
- A public edge in the test process (`edge_test.go`): every request but the
  token exchange goes to `auth`'s Check first; a denial answers with Check's
  answer, and an allowed request goes on, with the signed principal, to the
  service its path names, round robin over that service's backends that are
  not down. A test can delay the answers of a path, take a backend down or
  read the event stream requests the edge saw.
- A TokenReview stand-in (`identity_test.go`) that authenticates each
  service's own service account token for internal gRPC calls.
- CLI machines: a home directory and the CLI binary on the Linux host, or, on
  macOS, on the Mac named by `TETRAL_MAC_SSH_DESTINATION`, with one SSH
  session that forwards Engine's endpoints to the same loopback URLs on the
  Mac (`mac_test.go`).
- A Controller client of the instance routes and a developer client of the
  Session routes, both through the edge with a workspace API key.

## Inputs

| Variable | Read when | Value |
|---|---|---|
| `TETRAL_MACHINE_OS` | always | `linux` or `macos`, where the machines run |
| `TETRAL_CLI_BIN` | always | a CLI build for the Linux host; its `version` names the CLI commit of the record |
| `TETRAL_ACCEPTANCE_DIR` | always | the directory records are written to |
| `TETRAL_CLI_BIN_MACOS` | `macos` | a macOS build of the same CLI commit |
| `TETRAL_MAC_SSH_DESTINATION`, `TETRAL_MAC_SSH_KEY` | `macos` | the Mac's `user@host` and the private key for it |
| `TETRAL_MCP_TEST_SERVER_BIN`, `TETRAL_MCP_TEST_SERVER_BIN_MACOS` | a test that enables Computer Use | the MCP test server builds |
| `TETRAL_CLI_BIN_NEXT` | a test that names it | a later CLI build, for upgrade tests |
| `TETRAL_APP_EXAMPLE_SCRIPT` | a test that names it | the product app's launcher example |

A test fails at once, and never skips, when an input it needs is unset or
invalid: a binary that is not executable, an operating system the test does
not run on, or two CLI builds of different commits.

## Running a test

Set the inputs and run one test from this directory:

```bash
go test -count=1 -timeout=60m -run '^TestLiveRenewalMovesTheConnectionToTheNewCredential$' ./...
```

Docker must be running for the containers.

## Records

Each test writes, when it ends, one Markdown record to
`TETRAL_ACCEPTANCE_DIR` named
`<test>_<os>-<arch>_<Engine commit>_<CLI commit>.md` (`record_test.go`): the
commands it ran, what it observed, what it does not cover yet, and its result.
A later run of the same test on the same platform at the same two commits
replaces the file.

## Tests

- `client_connection_test.go`: a stream that resumes on another pod after its
  pod is killed, a stream-only disconnect, a held ready acknowledgement, and a
  live credential renewal.
- `computer_management_test.go`: user instances launched the way a product
  app launches them, and a revocation that ends the live connection.
