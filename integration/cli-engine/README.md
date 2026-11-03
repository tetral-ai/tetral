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
- `auth`, `api`, `computer-hub` and `git-proxy` as processes built from this
  checkout, each on its own database role. The Drive retention scenario also
  runs `tetral-cleanup drives` with the cleanup role against the same stores.
- A public edge in the test process (`edge_test.go`): every request but the
  token exchange goes to `auth`'s Check first; a denial answers with Check's
  answer, and an allowed request goes on, with the signed principal, to the
  service its path names, round robin over that service's backends that are
  not down. A test can delay the answers of a path, take a backend down or
  read the event stream requests the edge saw. It also captures actual
  Session reports and ACKs in memory for authenticated report replay.
- GitHub for repository preparation (`github_test.go`): a TLS front in the
  test process serves `git-proxy` at the `git_proxy_url` of every git ticket,
  and machines trust it through `GIT_SSL_CAINFO`. `git-proxy` reaches
  `github.com` through a CONNECT proxy in the test process (`HTTPS_PROXY`),
  which tunnels to a GitHub stand-in: `git http-backend` over bare
  repositories behind a `github.com` certificate of the test CA, which
  `git-proxy` trusts through `SSL_CERT_FILE`, answering only requests that
  carry the repository's token. Running needs `git` on the Linux host and on
  every machine.
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
| `TETRAL_CLI_BIN` | always | a CLI build for the Linux host at `<full-commit>/<os>-<arch>/<binary>`; the directory names the recorded source commit |
| `TETRAL_ACCEPTANCE_DIR` | always | the directory records are written to |
| `TETRAL_CLI_BIN_MACOS` | `macos` | a macOS build of the same CLI commit |
| `TETRAL_MAC_SSH_DESTINATION`, `TETRAL_MAC_SSH_KEY` | `macos` | the Mac's `user@host` and the private key for it |
| `TETRAL_MCP_TEST_SERVER_BIN`, `TETRAL_MCP_TEST_SERVER_BIN_MACOS` | a test that enables Computer Use | the MCP test server builds |
| `TETRAL_CLI_BIN_NEXT` | a test that names it | a later CLI build, for upgrade tests |
| `TETRAL_ENGINE_SDK_ROOT` | SDK consumer tests; optional provenance elsewhere | SDK source checkout; Bun on PATH runs its actual public client |
| `TETRAL_APP_EXAMPLE_SCRIPT` | a test that names it | the product app's launcher example |

A test fails at once, and never skips, when an input it needs is unset or
invalid: a binary that is not executable, an operating system the test does
not run on, or two CLI builds of different commits.

## Running a test

Set the inputs and run one test from this directory:

```bash
go test -count=1 -timeout=60m -run '^TestStreamOnlyDisconnectResumesTheSameConnection$' ./...
```

Docker must be running for the containers.

## Records

Each test writes, when it ends, one Markdown record to
`TETRAL_ACCEPTANCE_DIR` named
`<test>_<os>-<arch>_<Engine commit>_<CLI commit>.md` (`record_test.go`): the
commands it ran, what it observed, what it does not cover yet, and its result.
It records the selected CLI binary SHA-256, Engine source cleanliness and,
when supplied, SDK commit/cleanliness. Modified sources are explicitly
development runs, not evidence for the recorded clean commit. SDK consumption
is stated separately from merely recording its version.
A later run of the same test on the same platform at the same two commits
replaces the file.

## Tests

- `client_connection_test.go`: a stream that resumes on another pod after its
  pod is killed, a stream-only disconnect, a held ready acknowledgement with
  70 Sessions to prepare, and a new git ticket at each preparation.
  The live credential renewal scenario is present but requires a CLI that
  consumes `credential.renew`; the current CLI does not implement that consumer,
  so this scenario cannot yet pass and is not current compatibility evidence.
- `computer_management_test.go`: user instances launched the way a product
  app launches them, a revocation that ends the live connection, and
  Controller claims of new and wake requests.
- `developer_errors_test.go`: a preparation error written once per code.
- `session_drive_test.go`: repositories cloned only on their computer.

- `drive_cleanup_test.go`: cleanup through its command process, followed by
  public API reads of stored/current bytes, versions, change-cursor expiry and
  restore, including a repeated pass. This scenario has no CLI participant.

- `computer_error_sdk_test.go` (Linux, unprivileged test user, SDK checkout and
  Bun required): a nonexistent branch in a repository covering cwd fails
  preparation with `repository_preparation_failed`; its success control becomes
  ready in the cloned repository. A real permission denial on a cleanup root's
  parent leaves that root and produces a terminal `computer_cleanup_error`.
  The actual CLI report is replayed byte-for-byte, returning its saved ACK
  without another event; successful deletion has no cleanup error. All errors
  are consumed through the real SDK and authenticated public event API. These
  are development proofs: request/close initiation calls the lifecycle owners
  in-process, so they do not certify future tool/removal admission. macOS
  online/ready tests do not cover repository isolation or Drive mounts.
