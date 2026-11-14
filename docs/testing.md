# Testing and verification

The repository holds two products, Engine and the Tetral CLI, and each is
verified by its own command:

- Engine: the runner in `internal/testinfra`, behind `make test`,
  `make test-affected` and `make test-full`. It owns every path outside
  `cli/`.
- The CLI: `python3 scripts/verify-cli.py <profile> --evidence-dir <dir>` at
  the repository root.
  It owns `cli/` and is described in
  [`cli/README.md`](../cli/README.md#build-and-test).

A change under `cli/` runs the CLI's command; Engine's runner selects nothing
for it. A change under `protocol/`, the contract between the two, runs both:
Engine's runner falls back to its full profile. CI decides the same way (see
[Continuous integration](#continuous-integration)).

## Test layers

A test sits at the lowest layer that reproduces its claim. Its layer is judged
by what it really touches, never by its directory or its name:

- Layer 1 touches nothing outside the test process.
- Layer 2 has exactly one real component of ours, real local infrastructure
  (PostgreSQL, MinIO, NATS, Docker, Envoy, Keycloak) and stand-ins for the
  rest.
- Layer 3 has two or more components of one product over their deployed
  interfaces. The CLI is one component and has no layer 3.

The behaviour of a process itself (crash, kill, restart, startup
configuration) is tested only with that component running as a separate
process that the test kills for real.

## Engine's local profiles

Engine uses one repository-owned test inventory and runner across local and CI
verification. The profiles differ in scope, not in their interpretation of a
passing test. Before pushing a change, run `make test-affected` and
`make lint`.

| Command | What it runs | Expected duration |
| --- | --- | --- |
| `make test` | The fast edit loop: no Docker, PostgreSQL or MinIO. | About 4 minutes |
| `make test-affected` | The current change's owners from the inventory, with only the disposable dependencies they declare. An unknown or infrastructure-owning change expands to Full. | Between the two, by what the change selects |
| `make test-full` | The complete local evidence set, Go under Race. | About 24 minutes |

The durations were measured on a 12-CPU Linux workstation with a warm Go build
cache; a cold cache adds the compilation of every package.

A profile runs its groups one after another and stops at the first that
fails; the Go packages run last, so a failure in another group leaves Go
unrun. To run one group, pass its name to the runner:
`make test-runner && .test-results/bin/tetral-test --profile fast --groups go`.

To stop a run, press Ctrl-C or send the run command SIGTERM (`make` or the
built runner; `go run` does not pass SIGTERM on to the program). It cancels
the running commands, ends each one's whole process group, writes its result
(status `fail`, with `context canceled` as the first failure, in the step that
was running) and removes the containers, networks and volumes of the dependencies it
started before it returns. A runner killed outright (SIGKILL) cannot clean up.
Its `go test` processes run on until they end, at the latest at their package
watchdog below. Every Docker object the runner and the tests create carries
its owner's process ID and start time, and the next owner-aware cleanup, in
the orphaned tests or in the next run, removes those whose owner is gone.

Non-Fast Go commands use a 20-minute package watchdog. The exact `integration`
package has a 25-minute watchdog for its complete composition inventory; a
serial command containing that package applies 25 minutes to every package in
that invocation. In a sharded run, such as a Go Race job, the integration
package's slice has a 15-minute watchdog: each shard starts that slice first
and plans about six minutes for it, so Go names a hung test before the job's
25-minute limit ends the job. These are package-wide runaway limits, separate
from each test's unchanged request, fault, recovery and cleanup deadlines.

Engine's tests do not use the SDK. Every request a test sends to a public
route goes through `enginetest.Developer` (`integration/enginetest`), which
holds each request, answer, header and stream event to Engine's OpenAPI file.
[`api/README.md`](../api/README.md) states the file and the coverage check
the merge gate runs after the full suite.

A Go test whose infrastructure its source does not show is declared in
`internal/testinfra/inventory.json` under `go_tests`, with its exact package,
test name and complete dependencies. Full runs it, Affected runs it when its
package is in the dependency closure, and Fast compiles it without starting
its infrastructure. Dependency setup failures and unexpected skips fail
verification. To run the Go group with automatic setup, from a clean Engine
checkout:

```sh
make test-runner && .test-results/bin/tetral-test --profile full --groups go
```

The runner tears down dependencies before a profile returns.

Bridge, Job Runner and their shared Runtime configuration, MCP manifest and
durable-control packages form one verification boundary. Affected selection
includes both Go owners, cross-service integration tests and Runtime/Gateway
consumers; Go imports alone cannot describe their RPC and Bun test dependencies.
Content lifecycle changes include the Runtime context and client adapters,
Gateway assembly and transport, Bridge declarations and context loading, and
the `integration/content_*` tests and the content-lifecycle drivers under the
Runtime pod and Provider Gateway `test/fixtures/` directories.
Each selects the durable Go compositions and both TypeScript consumers, even
when the changed fixture has no Go import edge.
Service-owned `k8s/` changes select deployment evidence, including the raw
manifest invariants, Helm rendering tests and Helm lint. Deployment evidence
also runs the tests of the nested `integration/envoy-gateway-secret-helper`
module with the Go toolchain that `deploy/dependencies.lock.json` selects; the
root package listing excludes nested modules, so no Go profile reaches them,
and a change confined to that module selects deployment evidence. Helm chart
changes under `deploy/helm/tetral/` also select the integration package,
because its direct Runtime TLS and Envoy Gateway compositions render that
chart. Changes under `deploy/envoy-gateway/` likewise select the integration
package, because the Envoy Gateway translation reads its GatewayClass. Changes to
`deploy/nats/values.yaml` or `values-hardened.yaml` select the integration and
`internal/testinfra` packages, because the runner's broker and the integration
TLS cluster project their client policy from those values. Unknown ownership
continues to select Full. Service-owned `proto/` changes also select protocol
generation and compatibility checks.

The runner starts the selected dependencies before the first step and records
each one's identity and setup time. Keycloak and egctl, which mostly wait on a
JVM start or on a download and a helper build, start in the background while
the others start one after another; setup ends when all are ready. The egctl
archive download makes up to three attempts when the connection fails or the
server answers 5xx or 429; any other status, or an archive that differs from
its locked checksum, fails setup at once. The first
failure cancels every start, waits for all of them and then removes what they
created, including a container whose dispatch was still in flight; when the
caller stopped setup, its cancellation is reported as the cause. The runner's
containers that publish loopback ports start one at a time, from reserving a
port until Docker has bound it, so none of them takes a port another reserved;
their images are pulled before that, and fixtures that publish ports from test
processes are outside this order.

The runner's own PostgreSQL is disposable and runs with `fsync`,
`synchronous_commit` and `full_page_writes` off: no test restarts it or reads
its write-ahead log, and the checkpoint each dropped test database requests no
longer waits for the disk. Its evidence records these settings. A database
named by `TETRAL_TEST_DATABASE_URL` keeps its own settings.

Local transport fixtures use pinned Envoy and Bun images and own their Docker
containers and networks. PID and process-start labels protect live owners from
orphan cleanup. Concurrent collectors join an already-started removal for at
most two seconds, clipped by the caller's deadline; a resource still present or
an unavailable daemon remains a cleanup failure. Containers close before their
networks, including after partial fixture startup.

The runner's NATS broker is shared across concurrent package consumers; tests
may publish and subscribe but must not stop, restart or reconfigure its lifetime.
Destructive broker cases use `testinfra.NewNATSFixture` with the same pinned image
and the role permissions projected from the NATS release values. Register its
cleanup before clients so they join first; cleanup removes even a stopped broker
and its private credential files, including after partial startup, without
changing the shared descriptor or environment.

OIDC integration uses the native `keycloak` dependency. Full and the normal
Go Race CI shards execute `TestKeycloakHTTPSIdentityAndSigningKeyLifecycle`;
Affected selects the dependency for its declared consumers, while Fast only
compiles the real-issuer test. The runner starts the exact version and immutable
image digest in `internal/testinfra/keycloak.lock.json`, verifies the running
version, and records the digest and public realm-recipe checksum. The selected
[Keycloak 26.7.5 release](https://www.keycloak.org/2026/09/keycloak-2675-released)
is a test dependency, separate from Engine deployment.

The fixture serves actual HTTPS with a generated CA and an IP SAN for its
loopback endpoint. Its private descriptor, named by
`TETRAL_TEST_KEYCLOAK_CONFIG`, supplies the CA and an explicit `127.0.0.1/32`
destination allowance to the owning test. TLS verification stays enabled;
fixture clients reject redirects and any other destination. Every HTTPS request
has a five-second maximum, startup has a 120-second bound, and each independent
realm provision has a separate 180-second bound, all clipped by cancellation.
Realm consumers use the same fixed client, Engine audience, human username and
service selector, with distinct immutable subjects for simultaneous realms.
Credential files stay private and ephemeral. Actual realm signing-key controls
support fresh-key issuance, old/new overlap, passive keys and disabled-key
retirement; the fixture test independently verifies JWT signatures and claims
against observed JWKS. Realm close deletes its IdP state and credential files;
native dependency teardown removes the container, trust material and admin
credentials. This fixture proof supports the Auth compositions without
claiming their business behavior or later Kubernetes issuer routing.

Each invocation prints its Selection Plan and writes structured evidence below
`.test-results/`. When a step fails, the runner also prints an excerpt of that
step's diagnostic file: its first 20 and last 60 lines, after a `go test -json`
report is reduced to the decoded output of the tests and packages that did not
pass (a skipped test counts as not passing where the step rejects skips).
Under GitHub Actions the same excerpt goes to the job summary; the complete
diagnostic stays in the evidence artifact. These dependency setup commands end
the setup error the runner prints with up to the last 20 lines of their output
when they fail: the Bun workspace installs, the Docker check, image pulls, container
starts and removals, and the egctl SecretType helper build. Native package
commands remain appropriate while developing one owning package; the repository
profiles are the pre-submission contract.

## The CLI's tests

The CLI is one component of its own, so its tests have two layers and no
third. A layer 1 test touches nothing outside its test process; most unit
tests inside the crates of `cli/crates/` are of this kind. A layer 2 test runs
the CLI's real code on the host's real file system, mounts, Git and sandbox,
with a stand-in for everything that is not the CLI: the reference control
server of `cli/reference` in place of Engine, and the test seams for the
clock and for the operating system's answers. The tests of `cli/tests/tests/`
are layer 2 and start the real `tetral` program in processes of its own
(`cli/tests/README.md` holds their rules); a crate's test that mounts a drive
or starts a command in its own process is layer 2 as well. No test of the CLI
starts Engine or reads its code. What a process does when it crashes, is
killed or starts again is claimed only by a test that kills the real process.

One command runs each part, at the repository root, on Linux and on macOS:

```sh
python3 scripts/verify-cli.py <profile> --evidence-dir <directory>
```

| Profile | What it runs | Development host | CI job |
|---|---|---|---|
| `functional` | every unit and integration test of the workspace but the release test and, on Linux, the task-capacity test or, on macOS, the full-ledger test | 2 min | Linux 21 min, macOS 25 min |
| `release` | the release build and its one test, that the build holds no test seam | 1 min | Linux 14 min, macOS 18 min |
| `task-capacity` | the one test of 5,000 tasks on one stream (Linux only) | 4 min | 10 min |
| `full-ledger` | the one test of a full replay ledger sent again at once, which on Linux `functional` also runs | 35 s | macOS 11 min |
| `reference` | `go vet`, `golangci-lint run` and `go test -race` inside `cli/reference` | 20 s | inside the functional job |

A change to the CLI runs the profiles of its system before it is pushed. The command needs the
pinned Rust toolchain of `cli/rust-toolchain.toml`, Go, cargo-nextest
(`./scripts/install-cargo-nextest.sh`), on Linux bubblewrap 0.13.0 or later
(`./scripts/install-bubblewrap.sh`) and unprivileged user namespaces, and for
`reference` golangci-lint. The durations are those of a 12-core development
host with the workspace already built, and of a GitHub-hosted runner without
a restored build cache; a first build on the development host adds about 2 min to `functional`.
`cargo test` inside `cli/` stays the edit loop for one test or one crate and
is not the check before a push.

The script builds the programs the tests start, lists the tests with
cargo-nextest and checks the profile's selection against the listing, then
runs each test in a process of its own. A test still running at its limit in
`cli/.config/nextest.toml`, five minutes or fifteen for the tests listed
there, is terminated and reported by name.

`--evidence-dir` is required by every profile, `reference` included. The
script creates the directory, with its parents, when it is missing, and
writes there:

- `verification.json`: the profile; the revision (`GITHUB_SHA`, or `local`);
  the platform and architecture; on Linux the tests' `bwrap` and its version;
  each command with its arguments, exit code and seconds; the selected tests;
  for `reference`, `tests`, each test that `go test` reported with its
  package, result and seconds, a test still running without them;
  `success`; for a run that did not pass, `error` with its message; and
  `cleared_of_ended_runs`, `cleared` and `left`, below. It is written after
  each step, so a run that is killed leaves the steps it reached.
- `<step>.log`, the output of each command it ran: `build`, `seams`,
  `reference-server`, `digest`, `list` and `test`, as far as the profile runs
  them; for `reference`, `vet`, `lint` and `test`.
- `junit.xml`, each test's result and time, once the tests have run, also
  when they failed.
- `acceptance/`, the acceptance records of the tests of `cli/tests/tests/`.

A run replaces the files of an earlier run that have the same name and
keeps the others, an earlier run's acceptance records and a `junit.xml` it
did not reach among them, so each run gets a directory of its own.

Each run keeps what its tests create below a directory of its own in the
runs directory, `tetral-cli-runs-<uid>` in `/tmp` on Linux and
`.tetral-cli-runs` in the home directory on macOS, or the directory
`TETRAL_CLI_RUNS` names, and on Linux below a directory of the same name in
`/dev/shm`. The directory is short because the tests' Unix sockets live
below it. Ctrl-C or SIGTERM to the script stops the run: the script ends
the tests, kills every process the run started, unmounts the drives they
mounted, removes the run's directories, writes `stopped by <signal>` as
`error` in `verification.json`, with `success` false, and exits 1, within
seconds. The `reference` profile runs each command in a process group of its
own, which a stop ends whole, and stops its whole run at 480 s, below the
10 minutes of its CI step, with `stopped by the run's limit of 480 s` as
`error`. A run killed with SIGKILL
clears nothing itself: the next run of the script, with any profile, first
clears every run that has ended, and names what it cleared in
`cleared_of_ended_runs`. Of an ended run it kills the processes of its
tests' session, unless the host has restarted since or the session's ID now
names a process started later, and the processes without a terminal whose
working directory is below the run's directories, so never a shell. A run that cannot clear something lists it in
`left` and fails. The tests create no container, volume or Docker network.
On a hosted runner a cancelled job ends with its machine.

## Continuous integration

CI Go Race evidence jobs using the shared evidence action run at most two Go
package processes at a time. Each Race package can also run concurrent Go
goroutines, Bun children, and shared database work; the package limit is
intended to leave capacity for those owners on the four-CPU hosted runner.
It changes package scheduling only. Shards, test selection, per-scenario
concurrency, deadlines, and assertions retain their own policies. Local runs
keep their existing default and can set `--workers` explicitly.

The readable CI topology is:

- **Pull Request Verification**: read-only, hermetic PR evidence for Engine,
  split by ownership. It starts on every pull request; its first job, **Decide
  What Runs**, reads the changed paths and selects Engine's jobs for any
  change outside `cli/`, and none for a change inside it. Each selected job
  runs `go run ./internal/testinfra/cmd/tetral-test --profile full --workers 2
  --groups <group>` through `.github/actions/run-test-evidence`:

  | Job | Groups | Measured run |
  | --- | --- | --- |
  | Go Race (shard 0 to 4) | `go`, one shard each | 13 to 21 min |
  | Go Static Analysis | `go-static` | 6.5 min |
  | Provider Gateway | `gateway` | 5 min |
  | Repository, Security and Runtime | `repository`, `security`, `runtime` | 4 min |
  | Sandbox Image | `sandbox-image` | 2.5 min |
  | Deployment and Protocol | `deployment`, `protocol` | 9 min |
  | Merge Gate | reconciles the results | under 1 min |

  The Go Race jobs, one per `go_shards` entry of
  `internal/testinfra/pr_producers.json` (five), are balanced by duration and
  cover every Go package under Race. A group's run stops at its own time limit
  with a failed result, and every step runs even after an earlier group in its
  job failed, so each group that reaches its run uploads its own result.
  Re-running a failed shared job reruns all of its groups. **Merge Gate**
  passes only when the deciding job succeeded and every job it selected
  succeeded; a selected job that failed, was cancelled or was skipped fails
  it. It reconciles exactly one result from every selected producer for the
  same PR event, tested integration commit, workflow source and run, taking
  each producer's result from the latest attempt that has one, and accepts
  each only from the job that runs that producer. To stop a run, cancel it in
  GitHub; its runners are discarded with everything they started, and its
  gate fails.

  A pull request's run must finish in under 30 minutes, counted from the
  moment its first job was created, waiting for a runner included, to Merge
  Gate's last step, which fails the run at 30 minutes or more and prints each
  job's runner wait and run time. A partial rerun does not make a run faster:
  a job that is not run again keeps its first creation time, so a rerun of the
  failed jobs alone that ends later than 30 minutes after the first start
  fails the gate again. Only a rerun of all jobs or a new push starts a new
  measure. CLI results applies the same rule to CLI Verification.
- **CLI Verification**: reports on every pull request. Its `changed` job
  takes the decision from `tetral-ci-flow decide` (package
  `internal/testinfra/ciflow`): the CLI's jobs run for a change under `cli/`
  or `protocol/`, for a change to a file only the CLI's CI uses (its
  workflows, the build cache action, `scripts/verify-cli.py`,
  `scripts/install-bubblewrap.sh`, `scripts/install-cargo-nextest.sh`,
  `scripts/cli-build-cache.sh`), and when the changed paths cannot be read.
  Three independent Linux matrix jobs cover ordinary functionality, the
  release-without-test-seams check, and the serial 5,000-task regression.
  Matrix failure does not cancel the other jobs. A `macos` matrix runs the
  functional, the release and the full-ledger profile natively on
  `macos-15`, one job each (the 5,000-task test is compiled on Linux only;
  the full-ledger test's 4,030 Seatbelt launches take minutes, so it has a
  job of its own there), so the macOS adapters cannot reach `main`
  unchecked. One job running both would build the CLI
  twice on one runner and pass the job limit, whether the two builds run one
  after the other or together. **CLI results** applies the shared gate rule (`tetral-ci-flow
  needs`): the deciding job succeeded and every matrix job ended `success`, or
  `skipped` when the CLI was not selected. Its last step (`tetral-ci-flow
  duration`) fails a run that took 30 minutes or more from the creation of
  its earliest job, runner wait included; the job limits of 2, 26 and 2
  minutes allow no chain of jobs more. Each job runs its profile
  through `scripts/verify-cli.py` under cargo-nextest, which
  `scripts/install-cargo-nextest.sh` installs at a pinned version after its
  SHA-256 check: each test runs in a process of its own, and one that passes
  its limit in `cli/.config/nextest.toml` (five minutes, fifteen for the
  listed tests whose work is minutes long) is terminated and reported by
  name. The script first builds the programs the tests start, as steps of
  their own, so that no test's limit holds a build; it then checks, from
  cargo-nextest's listing, that each isolated test exists exactly once and
  that the profile's filter selects exactly what it should, before running
  it. The functional profile runs every other unit and integration test of
  the workspace, which has no documentation tests. Each job uploads command
  logs, timings, the selection, each test's result and time (`junit.xml`),
  acceptance records and setup outcomes even on failure; a runner forcibly
  cancelled before upload can still lose evidence.
  The functional job also checks the reference control server inside its own
  Go module, `cli/reference`, which no workflow of Engine builds or tests:
  `scripts/verify-cli.py reference` runs `go vet`, `golangci-lint run` with
  the module's `.golangci.yml`, and `go test -race`, also when the Cargo
  tests failed.
  Each `linux` job installs bubblewrap 0.13.0 as `/usr/bin/bwrap`, where the
  tests find the host's `bwrap`, built from the upstream release tarball after
  its SHA-256 check (`scripts/install-bubblewrap.sh`), and `scripts/verify-cli.py`
  refuses to start with an older one: before 0.13.0, bubblewrap remounts each
  submount from a snapshot of the mount table, and a launch fails when a mount
  goes away in between, as a concurrent test's drive mount does.
  Each `macos` job uploads the same evidence, as `cli-macos-<profile>`.
  Engine's runner has no part of the CLI: a change under `cli/` runs these
  jobs and no job of Pull Request Verification.
  Manual dispatch with `measure-builds` disables remote cache restore and
  measures the relevant compilation phases cold and warm on each `linux` matrix
  job's runner before executing its full test selection; the `macos` jobs
  neither restore nor measure. These are compilation
  measurements, not cache-transfer or repeated runtime measurements.
  Label events never start CLI Verification.
- **CLI Dependency Check**: runs on pull requests that change
  `cli/Cargo.toml`, `cli/Cargo.lock`, `cli/deny.toml`, the reference control
  server's `cli/reference/go.mod` or `go.sum`, or the workflow, and once a
  day. Its job `cargo-deny` checks the `cli/` workspace's advisories,
  licences and sources with `cargo deny check advisories licenses sources`.
  Its job `govulncheck` scans the reference control server's Go module, which
  no scan of Engine's module reads, with the govulncheck version the workflow
  pins.
- **CLI Release**: a pushed tag `cli-v*` starts it, and the label
  `cli-release-check` on a pull request starts its release check, which runs
  the four `build` jobs only and publishes nothing. Its jobs are `tag-check`,
  which accepts only a tag that names `cli/Cargo.toml`'s version on a commit
  of `main`; `build`, one per target, which packages the target's archive
  with `cli/scripts/package.sh` on a runner of that target and tests it with
  `cargo test --test release_archive -- --ignored`; and `release`, which on a
  tag publishes the CLI's GitHub Release with the four archives and
  `SHA256SUMS`, a prerelease while `cli_version` has a prerelease part.
- **Main Branch Verification**: Engine's evidence owners run independently on
  the exact merged commit; a push that changes only the CLI's tree runs none of
  them, and a run started by hand runs them all. Go Race runs the same shards; repository, static,
  Runtime, Gateway, protocol, deployment, image, and security evidence run in
  parallel. A push to main does not collect coverage.
- **Scheduled Verification**: bounded repetitions of named concurrency owners
  each week; daily compatibility and repository-health checks; a separate
  daily **Dependency Advisories** job for the online dependency audits, so a
  new advisory and a compatibility regression fail different jobs; and daily
  ordinary Go, Runtime, and Gateway coverage as a separate report-only
  artifact. A manual dispatch runs all of them, and the CI health
  job reports on each run after its other jobs finish. A later pass is
  recorded but never erases the first failure.
- **Go Build Cache**: after every push to main and daily, compiles what Go
  Race jobs compile and saves it as main's Go build cache entry; see
  [Go build cache](#go-build-cache).
- **CI Benchmark**: dispatched by hand to measure a speed change; see
  [CI Benchmark](#ci-benchmark). Nothing gates on it.

`internal/testinfra/go_shard_weights.json` balances the Go Race shards. It
holds medians of Go Race evidence in milliseconds: every package's `Elapsed`
(summed over its slices in a run) and its overhead per step (step time outside
its units, mostly compile), and for each package listed as `sliceable` every
top-level test's active time. A test's active time runs from its `run` event
to its final event less the time it spent paused, so a top-level
`t.Parallel()` test's wait is left out and a test whose subtests run in
parallel, which reports an `Elapsed` near 0, counts their time. The file also
names its source runs, the commit they all contain and the date. Only the
`sliceable` list is chosen by hand; the generator keeps it as it is.

GitHub assigns runner CPU models at random, and in the source runs an EPYC
7763 ran the same tests about 1.4 times slower than an EPYC 9V45. All weights
are on one machine scale: every sample is divided by a factor for its job's
recorded CPU model before medians are taken. The generator fits the factors
jointly with unit weights, alternating the two until they settle, from the
tests of sliceable packages and the whole Elapsed of other packages that
weigh at least a second. Within the fit a unit's weight is the geometric
mean of its scaled samples, which stays stable with three source runs where
a median would pin the fit to one sample; the weights written are medians. A
unit measured on one model only is still scaled by that model's factor, which
the units it shares with other models fix. The scale is the median source
job, the slower of the two middle jobs when their count is even: its CPU
model's factor is 1. The file records
each model's factor, jobs, fit samples and shared units. Generation stops
when a result records no CPU model or a model shares fewer than five units
with the others, rather than guess its factor.

The planner splits each sliceable package by top-level test and keeps other
packages whole. It places these units longest first, each on the shard whose
simulated makespan is lowest after taking it, the lowest-numbered shard on a
tie; the simulation runs a shard's steps on its two workers and charges a
package's overhead once per shard. The runner starts each shard's steps
longest first, as planned. A test the file does not know costs its package's
median test weight, a package it does not know costs its test count times the
repository's median test weight, and names the plan no longer has are
ignored. Planning reads no network, so
`make test-runner && .test-results/bin/tetral-test --profile full --groups go --shard-index N --shard-count K`,
where K is the number of `go_shards` in `internal/testinfra/pr_producers.json`,
reproduces CI shard N locally.

The predicted makespan is on that scale and leaves out the speed of the
machines a run draws, so a run's slowest shard is mostly set by the slowest
machine it drew. A job's speed is the median, over its passed units that the
weights give at least a second, of measured time ÷ weight, pricing units as
the weights do; a job with fewer than five such units has no speed and is
reported unnormalized, never given a guessed one. Drift checks and CI
Benchmark compare steps phases divided by their job's speed.

To regenerate the file, download the Go Race evidence (the `*-evidence-go-*`
artifacts) of at least three Pull Request or Main Branch Verification runs,
fetch the commits they tested, and run:

```sh
go run ./internal/testinfra/cmd/tetral-shard-weights --base <commit> <evidence-dir>...
```

`--base` is the newest change to test durations that the weights must
reflect. The generator uses only runs whose tested commit contains it and
whose every shard restored the Go build cache, and refuses to write the file
when fewer than three runs qualify. It counts only passed units and gives a
unit a new weight only from three samples; a unit that ran with fewer keeps
its current weight, or falls back to the median weights when it has none. It
lists the runs it left out and both kinds of short units, and warns when a
package that is not sliceable takes more than one worker's share of the total
over the `go_shards` and their two workers each.

The daily CI health job checks the weights against the newest verification
runs whose base contains the weights' base commit. It downloads the Go Race
evidence of each of the last two weeks' ten newest Pull Request and Main
Branch Verification runs whose Go Race shards ran (a change outside Engine's
tree skips them) and raises a warning annotation, repeated in its
step summary, when over at least three such runs the median speed-normalized
slowest steps phase (each shard's steps phase divided by its job's speed) of
the runs whose shards all passed exceeds the weights' predicted makespan for
their plans by more than 60 s, when a unit's median, with every sample
divided by its CPU model's factor, or by its job's speed for a model the file
has no factor for, differs from its weight by more than 30 s, or when a
top-level test of 10 s or more in a sliceable package, or in a package the
file does not know, has no weight of its own. Samples with neither a factor
nor a job speed are left out and counted by model in the summary. With fewer
runs it reports
insufficient data. The summary lists each run's measured slowest steps
phase, its normalized one and its jobs' range of speeds, and compares summed
Go test work with the weights' total: about 10 more minutes of it (one per Go
package process, the `go_shards` times two) add a minute of solo wall time.
For each of the two weeks it counts the Pull Request
Verification first attempts that ended in success or failure, and from those
of up to 40 of them spread across the week whose Go Race shards ran shows the share queued, the median solo
wall time as measured and normalized by the week's Go test work, and the
runner-minutes. A check makes about 200 GitHub API requests. A warning calls
for a pull request with regenerated weights; nothing rewrites the file
automatically.

Report-only coverage runs `go test ./...` once without Race, so the integration
package executes every top-level test sequentially in one binary. Its Go
budget is 60 minutes: more than twice the 25 to 28 minutes that package took
in the coverage runs of Main Branch Verification runs 38151172754 and
38149638991. Coverage times are not part of the repository's evidence, so
`TestCoverageInstallsCrossLanguageDependenciesBeforeGoTests` checks the budget
against twice the package's summed Race time in `go_shard_weights.json`, a
lower figure that grows with the package. The coverage job's 80-minute limit
leaves room for dependency setup, compilation, and the Bun coverage commands
around it.

Online Bun dependency audits are deliberately separated from deterministic
security checks. Pull requests run them when a `package.json`, `bun.lock`, or
the audit execution plumbing changes. The main-branch workflow does not repeat
the same remote query after merge; Scheduled Verification's daily Dependency
Advisories job catches advisories published after the dependency graph was
accepted. One audit covers the Runtime and Gateway packages and prints every
package's findings before it fails, so one package's advisories cannot hide
another's. Secret, redaction, import, and boundary checks remain part of normal
repository evidence at every layer, including the dependency setup they
require.

To force the online audit locally, run:

```sh
make test-runner && .test-results/bin/tetral-test \
  --profile full \
  --groups security \
  --dependency-audit always
```

The main-branch ruleset requires two checks: the **Merge Gate** produced by
Pull Request Verification and **CLI results** produced by CLI Verification.
Individual producer jobs are evidence inputs; none can bypass or substitute for
the reconciled verdict.

Merge Gate also checks that the Go Race results partition the Go selection
exactly once. Each records `go_selection`: a digest of the complete Go
selection before slicing (every package with its tests and every Go exclusion
with its disposition), its shard index and the shard count. The Gate takes the
Go shard producers from `go_shards` in `internal/testinfra/pr_producers.json`,
not from producer names, and fails unless their indices are exactly 0..K−1 for
the K shards listed, no test runs in two shards, each zero-test package is in
exactly one shard, every shard carries the same exclusions, and the union of
the shards hashes to every recorded digest. It does not plan again; the
planner's own reconciliation owns the selection. Main Branch Verification has
no Merge Gate: nothing reconciles its results, and its failed jobs are the
signal.

Pull request jobs have read-only repository permission, do not receive external
service credentials, and never execute PR code through `pull_request_target`.
External Provider, Daytona, MCP, publication, and deployment rehearsals remain
operator-owned release activities.
Their step-level report and protected publication handoff are described in
[Rehearsal evidence and release](rehearsal-release.md).

Fork pull requests follow the repository's `all_external_contributors` policy:
the workflow stays pending until a maintainer approves execution. Approval runs
the same complete hermetic evidence as a same-repository pull request; it does
not grant secrets or write permission and does not reduce the Merge Gate.

Third-party Actions are listed in `internal/testinfra/actions.json` by exact
repository, immutable object SHA, exact release tag, object type, and resolved
target commit. To update one, verify the release in the upstream repository,
record the exact tag and peeled target, update every executable `uses` reference
to the reviewed immutable object, advance `reviewed_at`, and run
`TestRepositoryActionsMatchReviewedInventory`. A major moving tag alone is not
provenance. A sub-path action such as `actions/cache/restore` runs code of its
repository, so it is listed under that repository (`actions/cache`) and every
sub-path of one repository uses the same SHA.

### Go build cache

Go Race jobs and the Deployment and Protocol job restore one Go build cache
entry, read-only, instead of setup-go's own cache: the build cache, the module
cache and installed tools (`~/.cache/go-build`, `~/go/pkg/mod`, `~/go/bin`).
Its key is
`go-race-v1-Linux-<Go version>-cc<gcc fingerprint>-<go.sum hashes>-<commit>`.
A job restores the entry of its own commit, else the newest one for its
`go.sum` files, else for its compiler, else for its Go version, and Go
recompiles whatever differs. The entry extracts to about 8.4 GB, so a job with
less than 25 GB of free disk before the restore, or whose free disk could not
be measured, skips it and compiles cold.
The result of the step that restores it records the restore: key, matched key,
an exact, prefix, missed or skipped match, its duration and size, and free disk
before and after.

A job that runs several evidence groups restores a Go cache, either this entry
or setup-go's own, in its first evidence step only. Its later steps pass
`go-cache: none` and use what is already on disk: a second restore would
download the archive again and cannot extract over the read-only module cache.
`VerifyGoBuildCacheWorkflows` checks this for Pull Request Verification.

Only the **Go Build Cache** workflow (`.github/workflows/go-build-cache.yml`)
writes these entries in main's scope. It runs `scripts/build-go-cache.sh`,
which compiles every package's race test build and vet, the pinned SecretType
helper, buf, the repository runner and two process fixtures that integration
tests build (the web connector and the transport backend), each with the
flags, environment and directory of the build it warms. It runs after every
push to main, daily and on dispatch, and first looks up the entry for its
commit: when one exists it stops, so the daily run rebuilds only after the Go
or gcc version changed or the entry was evicted or expired (seven days
unused). Per ref, a newer run replaces a pending one and never cancels a
running one, so an upload always completes.

Pull request workflows only read the Actions cache: Pull Request Verification,
CLI Verification, CLI Dependency Check, CLI Release and
`engine-vulncheck.yml` declare `cache-mode: read`, which GitHub enforces with
scoped cache tokens, and main's Go Race jobs restore with the same mode.
`VerifyGoBuildCacheWorkflows` keeps writes explicit: only the Go Build Cache
job, CI Benchmark's guarded `produce` job and the separately validated CLI
Build Cache job declare a write-capable
`cache-mode` or use `actions/cache/save`, no workflow uses `actions/cache`,
whose post step saves, and pull request workflows declare `read`. Other
main-branch jobs keep the default write access of trusted triggers, which
setup-go uses for its own keys.

The cache is trusted because of who can write it, not because of its content.
Only main-branch runs of trusted triggers can write main's scope, and only the
Go Build Cache job saves `go-race-` keys there; CI Benchmark's `produce` job
saves them only in a non-main branch's scope. The build script executes no
repository or test binary and stores no test result. Content addressing makes
a stale entry harmless; it does not detect a tampered one. Runner checks still
run on a hit, but they read binaries built from cached outputs, so they are as
strong as the writers are trusted, as with setup-go's entry. Other main jobs
could write any key, but they run merged code, which is trusted.

### CLI build cache

**CLI Build Cache** (`.github/workflows/cli-build-cache.yml`) is the only
writer of `cli-build-v1-` entries. It runs on main pushes and manual dispatch,
with a main-only job guard, read-only repository permissions and no persisted
checkout credentials. Its runs queue without cancelling an upload. CLI
Verification uses the read-only composite `.github/actions/cli-build-cache`;
it never saves, including on benchmark dispatches.

`scripts/cli-build-cache.sh` owns the path lists, compatible keys and build
recipe. The producer looks up each exact key, then builds missing components
once without executing repository test binaries: ordinary `cargo build
--locked`, `cargo test --locked --no-run`, the debug test-seams fixture, the
digest fixture with the actual `/usr/bin/bwrap` SHA-256, and the release
fixture. The producer installs bubblewrap with the same script as the
consumers, whose build is reproducible, so their bubblewrap digests match. Cargo's build scripts still execute as part of compilation. Every
Cargo command runs in `cli/` to select its pinned Rust toolchain. The existing
four target directories remain distinct so nested fixture builds do not
contend with the running test build's Cargo lock.
Nested fixture commands remove the test harness's inherited package metadata;
Cargo supplies each built package's own metadata. This keeps build-script
environment fingerprints compatible with direct producer builds while
preserving compiler flags, toolchain and Cargo configuration.

| Entry | Paths | Consumers |
| --- | --- | --- |
| Common | Cargo registry index, archives and sources; Cargo Git databases and checkouts; `cli/target/debug`; `cli/target/test-seams` | Functional, release seams, 5,000 tasks |
| Digest | `cli/target/test-seams-bwrap-digest/debug` | Functional |
| Release | `cli/target/release-seams/release` | Release seams |

All entries exclude incremental directories. They contain no Cargo credentials,
user configuration, acceptance records or cached test outcomes. The common
entry does not encompass `cli/target`, which would also restore the other
variants. Keys contain a versioned component namespace, a hash of runner
OS/architecture/image, Rust/Cargo/Go and native compiler identities, Cargo
lock/manifests/build scripts and the shared recipe, followed by the producer
commit. The digest component additionally includes the host bubblewrap digest.
Consumers can fall back only within this compatible prefix. Cargo and fixture
builders still check current sources and run every selected test on a hit.

Local extracted outputs were about 8 GiB common, 4 GiB digest and 2 GiB release.
Restoration requires those components plus 6 GiB headroom: 14 GiB for the
5,000-task profile, 18 GiB for functional, 16 GiB for release, and 20 GiB for the
producer's combined restore. Insufficient or unmeasurable free disk skips the
restore and uses a cold build. These thresholds are an initial extraction and
rebuild budget, not measured hosted-runner peak usage. Evidence records the
requested/matched keys, exact/prefix/miss/skipped status, elapsed restore time,
extracted bytes and free disk; the producer also records pre-save size. Actions
cache logs supply compressed archive transfer sizes when available.

`VerifyCLIBuildCacheWorkflows` validates this producer independently before the
global writer scan allows its save steps. Mutation tests reject PR/off-main
writers, implicit saving actions, unrecognized save jobs, mismatched keys or
paths, unsafe fallbacks and unguarded variant restores. Script tests exercise
the actual recipe boundary, build failures, compatible-key invalidation and
low-disk fallback.

There is no trusted CLI cache until this workflow lands on main and its first
producer completes. A premerge cold/warm compilation measurement demonstrates
local Cargo reuse only; it does not establish an Actions cache hit or net
transfer benefit. The three verification jobs each download common outputs,
so total runner-minutes and transferred bytes may rise even when feedback is
faster. Report those separately from concurrent wall time.

### CI Benchmark

`.github/workflows/ci-benchmark.yml` runs Go Race for a base commit (side A)
and for the dispatched branch head (side B) in one run, then compares them.
Each side checks out its own commit and runs that commit's evidence action
with the inputs of Pull Request Verification's Go Race job, so A really runs
the base's runner. Each side runs as many shards as its own commit's
`go_shards` lists, so a change of shard count is measured against the old
count; the run fails before any Go Race job when a side's commit lists none.
The workflow only reads: its token holds `contents: read` and `actions: read`,
it uses no secrets, and `cache-mode: read` stops every job but the optional
`produce` job from saving to the Actions cache. A dispatch runs the dispatched
branch's copy of the workflow, so the branch must contain it. Dispatches on
one ref queue rather than cancel each other; a run holds both sides' Go Race
jobs (ten at five shards each), so dispatch one at a time.

```sh
gh workflow run ci-benchmark.yml --ref <branch> [-f base=<commit>] [-f produce-cache=true]
```

Without `base`, side A is the merge base of the branch with `origin/main`. A
branch stacked on another passes its base branch head, as a SHA or
`origin/<branch>`. A dispatch on `main` without `base` is an A/A run: both
sides test the same commit.

`produce-cache=true` measures a change to the Go build cache before it merges.
On a branch other than main, the `produce` job first runs the branch's
`scripts/build-go-cache.sh` on side A's commit and saves that entry into the
branch's cache scope, unless one exists. Both sides start only once it has
succeeded or was skipped, so B never measures a miss caused by a failed or
cancelled `produce`. B then restores the entry by restore key, as a pull
request restores main's entry. Pull requests based on that branch would also
restore it before main's, so use it only on a branch nothing is based on.

The **Compare A and B** job writes the report to the run summary and uploads
`compare.json` as `ci-benchmark-compare-<run>-<attempt>`. It first checks that
every result tested its side's commit. A side is complete when its results
record the shard count K that `go_shards` lists at its commit, hold shard
indices 0..K−1 and every shard ran all its steps. K is read from the commit,
not from the workflow, so a wrong count given to a side's jobs leaves it
incomplete with a warning naming both counts; `compare.json` and the summary
report each side's K as `shard_count`. For a commit without `go_shards`, K is
the count its results record; results from before shard identities were
recorded cannot show a missing last shard. Per side and shard it reports the
time before the runner starts (GitHub steps, `go run` and planning),
dependency setup, preparation (job start to the first test), each step's
compile time before its first test, fixed preparation (time before the runner,
setup and the shard's longest step compile), the steps phase, job time, runner
CPU model and image, the Go build cache restore, and Go Race runner-minutes.
Metrics are d = A − B, so a positive d means B is faster. Two effects shape
how to read them:

- Hosted runners differ in speed, even within one run. A test's B/A ratio is
  therefore divided by the relative speed of its two jobs: the median B/A
  ratio of the unchanged top-level tests that passed and ran at least one
  second in both. The report states how many tests support each factor and
  leaves ratios unnormalized when fewer than eight do. Tests of packages whose
  files differ between the two commits, and the tests named by
  `--changed-tests` (full import path and test name), support no factor.
  `changed_tests_duration` sums exactly the named tests: A's durations against
  B's divided by their jobs' speed factor.
- Adding, removing or re-weighting a test re-balances the shard plan, which
  moves the slowest job by minutes regardless of the change. When A and B ran
  different plans, the measured slowest job, steps phase and runner-minutes
  are reported but not judged. Preparation depends on which step a plan puts
  first, so it is only reported. `fixed_preparation` counts the shard's
  longest compile wherever it runs, so it is nearly plan-invariant: only that
  compile depends on which packages share a shard. The replays are
  plan-invariant: two workers take a shard plan's steps in order. A whole
  package costs its measured step; a slice of a split package costs its tests'
  active times, as the shard weights count them, plus the overhead (step time
  outside them, mostly compile) of that package's step in the same shard, or
  the package's mean overhead when that shard ran no step of it, so a run's
  own plan replays to its measured steps phase. Per-test ratios, speed factors
  and `changed_tests_duration` compare Go's reported `Elapsed` instead.
  `normalized_replayed_slowest_steps_phase` replays both plans over A's
  durations, taking B's only for packages A never ran, after dividing every
  unit by the speed of the job that measured it, so neither plan keeps the
  machines A's jobs drew; the report lists every job's speed, computed with
  the shard weights built into the tool, so a side from an older runner gets
  speeds too. `replayed_slowest_steps_phase` is the same replay without that
  division: A's plan keeps A's machine draw and B's mixes it, so it is only
  reported;
  `plan_fixed_slowest_job` prices A's plan with each side's durations and adds
  that side's mean time before the runner and setup;
  `plan_fixed_runner_minutes` adds each side's durations replayed through A's
  plan to the runner time outside the steps phase (before the runner, setup,
  teardown and upload) of every job that side ran, so a side with more shards
  pays for each extra job; the package overhead its own plan repeats on those
  shards is left out.

The same command compares any two downloaded Go Race evidence sets, for
example a pull request run against a main run. A side's commit is the
`plan.revision.head` its results record, which for a pull request run is the
test merge commit; `--repo` must hold both commits.

```sh
go run ./internal/testinfra/cmd/tetral-ci-bench compare \
  --a <downloaded-a> --a-sha <commit> --b <downloaded-b> --b-sha <commit>
```

A change is judged from downloaded `compare.json` files. `noise` reads those of
A/A runs and reports s_D, the standard deviation of the paired difference, per
metric with its sample count. It is estimated from per-shard pairs: a mean of
K shards divides it by √K, a sum multiplies it by √K, and a slowest-shard
metric uses it as is. `verdict` applies the rule for one metric and the
expected reduction E: E ≥ 10 s_D needs one run, 3 s_D ≤ E < 10 s_D three, and
2 s_D ≤ E < 3 s_D five. It passes when d > 0 in every run and the mean d is at
least E/2; below 2 s_D there is no measurable change. Every verdict also
requires B's plan-fixed runner-minutes and plan-fixed slowest job to stay
within A + 2 s_D, and refuses plan-dependent metrics from runs whose plans
differ.
`normalized_replayed_slowest_steps_phase` prices both plans over the same
durations, so A/A runs give it no s_D: its verdict always takes five runs. A
shard plan change, including a change of shard count, is judged on it:
`verdict --metric normalized_replayed_slowest_steps_phase --effect E` passes
when d > 0 in all five runs and the mean d is at least E/2.

```sh
go run ./internal/testinfra/cmd/tetral-ci-bench noise --output noise.json aa-1.json aa-2.json aa-3.json
go run ./internal/testinfra/cmd/tetral-ci-bench verdict \
  --metric fixed_preparation --effect 270 --noise noise.json run-1.json run-2.json run-3.json
```

A/A runs change no test, so the s_D of `changed_tests_duration` comes from
`noise --changed-tests <list>` with the change's own list: it sums the listed
tests in each A/A `compare.json` (kept 30 days). A comparison keeps a test
under one second only when it was made with that list; rerun `compare
--changed-tests` on the A/A run's Go Race evidence while it is kept (14 days),
or dispatch new A/A runs.

Benchmark evidence never feeds the shard weights or the CI health job:
`tetral-shard-weights` and the health job's weights check use only Pull
Request and Main Branch Verification runs.

## Evidence and diagnosis

Structured results include the exact tested revision, selection, dependency
identity, command outcome, each step's start, end and duration, first failure,
and the CI execution envelope with the runner's CPU model and image and the Go
build cache restore. The isolated TLS, OIDC, public streaming process and
platform key integration roots run their body in a child of the test binary
and log the child's combined output with every line prefixed by the seconds
since the child started.
Do not treat a rerun as erasing an earlier failure. Preserve the first result,
classify apparatus failures separately from product failures, and diagnose
with the printed `Reproduce:` command, which repeats the whole profile, with
`--groups` naming the group that failed.
