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
  Mac (`mac_test.go`). A Mac machine's drives are enabled once it is
  configured, as the app enables them.
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
| `TETRAL_SMALL_VOLUME` | `linux`, a test that needs a small volume | a directory on a volume of 6 to 8 GiB; on macOS the test attaches a disk image itself |
| `TETRAL_PERSON_AT_MAC` | a test that names it | any value: a person at the Mac does each step that `~/tetral-drives-consent/NEXT.txt` names |
| `TETRAL_JOINT_LIST` | optional | `1`: list the rows and tests with their budgets instead of running them ([Budgets](#budgets)) |
| `TETRAL_JOINT_LANE` | optional | `benchmark`: run only the benchmark rows, whose claim is a recorded number; unset, every other row |
| `TETRAL_ENGINE_BUILDS_DIR` | optional | a directory holding the Engine services, built once for many test processes; unset, each process builds them |
| `TETRAL_JOINT_PORTS` | optional | `<first>-<last>`: the loopback ports the process listens on, a range no other process is handed |
| `TETRAL_MAC_LANE_DIR` | optional, `macos` | a Mac directory holding copies of the Mac binaries; each machine copies them from there once their SHA-256 matches, instead of over scp |

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
`<test>_<os>-<arch>_<Engine commit>_<CLI commit>_<start>.md`
(`record_test.go`): the commands it ran, what it observed, what it does not
cover yet, and its result.
It records the selected CLI binary SHA-256, Engine source cleanliness and,
when supplied, SDK commit/cleanliness. Modified sources are explicitly
development runs, not evidence for the recorded clean commit. SDK consumption
is stated separately from merely recording its version.
`<start>` is the UTC time the test started, so each run writes a new file
and a rerun never replaces an earlier record. Its Timing section lists the
run's timed setup steps and every wait of more than 5 seconds before a
recorded line, with the harness code that recorded it.

## Budgets

Every table row and every test that is no table row has a time budget in
seconds on Linux and on the Mac, set where the row is listed
(`.budget(linux, mac)`, `ownBudget`). A row that takes longer, its
machines' cleanups included, fails, and its record says by how much. The
budget is the largest passing time `e` seen under the runner, as
`ceil5(e + max(20, 0.3 e))`; a change that moves a row's time sets its
budget again from new runs.

With `TETRAL_JOINT_LIST=1` and `TETRAL_MACHINE_OS` set, a run starts
nothing and prints one line per row that would run, `joint-row`, its name,
its budget and its place in its table, tab-separated; a test that would run
with no budget fails the listing. A test that needs a person at the Mac is
printed as `joint-person` and is never run by the runner.

## Running the lanes

`runner/` runs every row of both machine platforms as one run, with the
inputs above in its environment, from this directory:

```bash
go run ./runner -out <new run directory> -records <record directory>
```

| Flag | Default | Meaning |
|---|---|---|
| `-out` | required | the run's directory, which must not exist |
| `-records` | required | `TETRAL_ACCEPTANCE_DIR` of every job |
| `-lanes` | `linux,macos` | the lanes to run |
| `-benchmark` | off | run the benchmark lane instead |
| `-linux-slots`, `-mac-slots` | 6, 8 | the jobs each lane runs at once |
| `-job-limit` | 200 | budget-seconds of one job |
| `-only` | all rows | a regular expression; only the rows whose name matches run |
| `-max-mac-rtt` | 20ms | the longest round trip to the Mac at which the Mac lane starts |

In order, the runner:

1. Checks the hosts and exits before building anything when a check fails,
   naming the value it found: the first `bwrap` in `PATH` is 0.13.0 or
   later (older versions race Docker's containerd mounts,
   containers/bubblewrap#650); the shortest of five TCP connects to the
   Mac's SSH port is at most `-max-mac-rtt` (a Mac off its local network
   makes the Mac lane's numbers invalid); and the Mac's 1-minute load
   average is at most 4.0, checked again every 30 s for up to 5 minutes,
   after which it prints the Mac's 5 busiest processes.
2. Compiles the harness once and builds the Engine services once into
   `-out` (`TETRAL_ENGINE_BUILDS_DIR`).
3. Lists each lane's rows ([Budgets](#budgets)) and packs each table's rows,
   in table order, into jobs of at most `-job-limit` budget-seconds; a row
   of that budget or more is a job of its own. A job is one test process
   with one Engine, its rows one after another as in a serial run.
4. Creates Docker networks one at a time, two per Engine that runs at once,
   then removes them. When two fewer can be created, the Mac lane runs one
   job fewer at once; with fewer still, the runner exits.
5. Runs both lanes at once, longest jobs first, each job with a loopback
   port range of its own (`TETRAL_JOINT_PORTS`). The Mac lane holds
   `caffeinate -i` and copies the Mac binaries once into a lane directory
   (`TETRAL_MAC_LANE_DIR`). On Linux, unless `TETRAL_SMALL_VOLUME` is set,
   the test that needs a small volume runs in a user and mount namespace of
   its own with a 7 GiB tmpfs as that volume, where the CLI runs as the
   namespace's root. As the child subreaper of its processes, the runner
   reaps each orphan from another PID namespace once it exits, as init
   would, so that it does not stay a zombie for the rest of the run.
6. Writes `rows.tsv` (each row's result, time, budget and job) and
   `summary.txt` (each lane's wall from the runner's start, the highest
   budget use, the preflight values and the peak of testinfra's Docker
   networks), and exits nonzero unless every row passed and every job
   exited 0 (a test can fail after its rows passed, in its cleanup).
7. Cleans up, by PID or ID, only what the run created: as the child
   subreaper of its processes it ends every process still below it, removes
   the Docker containers and networks labelled with one of its test
   processes, detaches the tetral FUSE mounts and removes the in-memory
   machine homes that appeared after its start, and on the Mac ends the
   processes of the machine directories and RAM disks it created, detaches
   those disk images, removes those directories and ends `caffeinate`. On
   the Mac a RAM disk counts as the run's only while its `tetral-joint-*`
   volume is mounted; any other RAM disk attached during the run is left
   and named in the log.

## Tests

- `client_connection_test.go`: a stream that resumes on another pod after its
  pod is killed, a stream-only disconnect, a held ready acknowledgement with
  70 Sessions to prepare, and a new git ticket at each preparation.
  The live credential renewal scenario checks that renewal preserves the
  connection and moves it to the new credential. It creates no Session or task,
  so it does not prove task continuity during renewal. The CLI implements
  `credential.renew`; compatibility evidence comes from a passing run that
  records both the supplied CLI revision and the Engine revision.
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
  two successive preparations on distinct bindings with
  `repository_preparation_failed`, without publishing or reusing a default-branch
  clone. The SDK receives one error for that repeated failure code; the success
  control becomes ready in the cloned repository. A real permission denial on a
  cleanup root's parent leaves that root and produces a terminal
  `computer_cleanup_error`.
  The actual CLI report is replayed byte-for-byte, returning its saved ACK
  without another event; successful deletion has no cleanup error. All errors
  are consumed through the real SDK and authenticated public event API. These
  are development proofs: request/close initiation calls the lifecycle owners
  in-process, so they do not certify future tool/removal admission. macOS
  online/ready tests do not cover repository isolation or Drive mounts.
  The failed-outer-repository case submits the inner destination first, covers
  plain and lexical-alias paths, and checks the actual report, absent outer
  destination, no inner Git request, unaffected sibling and ownership inventory.
  After credential rotation, stopping and waking the machine issues a fresh
  binding that prepares both nested clones; wake request admission is in-process.
