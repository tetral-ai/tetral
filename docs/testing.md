# Testing and verification

Tetral uses one repository-owned test inventory and runner across local and CI
verification. The profiles differ in scope, not in their interpretation of a
passing test.

## Local profiles

- `make test` is the fast edit loop. It does not require Docker, PostgreSQL, or
  MinIO.
- `make test-affected` reconciles the current change against the inventory and
  starts only the disposable dependencies required by the selected owners. An
  unknown or infrastructure-owning change expands to Full.
- `make test-full` runs the complete local evidence set. It includes Race and
  can take materially longer.

Non-Fast Go commands use a 20-minute package watchdog. The exact `integration`
package has a 25-minute watchdog for its complete composition inventory; a
serial command containing that package applies 25 minutes to every package in
that invocation. These are package-wide runaway limits, separate from each
test's unchanged request, fault, recovery and cleanup deadlines.

The local SDK integration test is declared in `internal/testinfra/inventory.json`
under `go_tests`, with its exact package, test name, and complete `sdk` and
`postgresql` dependencies. These declarations replace source inference for the
named test. Full runs it, Affected runs it when its package is in the dependency
closure, and Fast compiles it without starting its infrastructure.

The runner prepares disposable PostgreSQL and a clean, pinned SDK checkout,
checking Bun and Node and installing the SDK's frozen Yarn dependencies. The Go
test then starts local Engine Auth and API services, issues a test API key, and launches
the SDK suite against that local address. Its child-process execution flag is
only an SDK launch contract, not a CI selection rule. Jest's structured report
must contain executed, passing assertions with no skipped or failed cases.
This proves API authentication and database write/read behavior; it does not
start a Sandbox or execute Git. Dependency setup failures and unexpected skips
fail verification.

When reusing a clean SDK checkout at the pinned commit, the runner installs
dependencies in that directory. This applies to `TETRAL_ENGINE_SDK_ROOT` and
an automatically discovered sibling `tetral-sdk-typescript` checkout. The
installation can update ignored files such as `node_modules` even while Git
reports a clean working tree. These installed files remain after verification;
the runner removes only temporary checkouts it created itself.

SDK pin changes also require the SDK's standalone source compatibility profile;
the repository's protocol checks validate types and proof registration but do
not execute that profile. From a clean SDK checkout at the runner's pinned
commit, install frozen dependencies with lifecycle scripts disabled, run
`bash ./scripts/build`, then run
`TETRAL_ENGINE_ROOT=/absolute/path/to/engine bun run test:compatibility:static`.
These source proofs complement the local SDK integration suites; they do not
establish runtime or database interoperability by themselves.

The registered SDK Session preview and typed OIDC Memory proofs run through
`TestForkSDKIntegrationCompatibilityProofs` in the existing Go evidence owner. Its
inventory declaration requires real HTTPS Keycloak, TLS PostgreSQL via Docker,
MinIO, NATS, Bun workspaces and the pinned SDK. Full and Affected execute it
while those dependencies are alive;
the normal CI Go Race shards include it. Fast compiles it without execution.
To reproduce with automatic setup, run from a clean Engine checkout:

```sh
go run ./internal/testinfra/cmd/tetral-test --profile full --groups go
```

The test runs the SDK's `test:compatibility:integration` command with the exact
Engine root and revision. Frozen SDK installation disables lifecycle scripts;
this launcher resolves source aliases directly and requires no SDK build or
`dist` directory. The SDK selects the named public streaming Identity subtest
and the complete `TestOIDCKeycloakSDK` root. The latter awaits typed SDK Memory
retrieve/redact responses for human and service actors, then independently checks
wire and stored attribution against the applied Engine identity IDs. Existing
Session, token caching, revoked-bearer retry and durable-effect assertions also
run. Both SDK and Engine validate executed Go JSONL, all five preview observations
and four OIDC observations under their owning subtests. The wrapper requires
exact source/scenario/test/assertion markers and four passing handlers; names,
counts or exit zero alone cannot pass. Nested processes inherit the runner's
process and
database cleanup custody. Their original output remains in the Go artifact.
Each fixed SDK child has an 11-minute process budget and the wrapper has a
24-minute context; its owning native Go selection uses a 30-minute timeout.
Other native Go selections use the package watchdog policy: 25 minutes for
the exact integration package and 20 minutes for other packages.
The runner tears down dependencies before a profile returns; a later standalone
SDK command cannot reuse them. Standalone execution requires independently
prepared dependencies and clean, matching Engine and SDK sources.

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
package, because the Envoy Gateway translation reads its GatewayClass. OIDC SDK
driver changes under `integration/testdata/oidc-*` select the integration
package too, because `TestOIDCKeycloakSDK` executes that Bun driver. Changes to
`deploy/nats/values.yaml` or `values-hardened.yaml` select the integration and
`internal/testinfra` packages, because the runner's broker and the integration
TLS cluster project their client policy from those values. Unknown ownership
continues to select Full. Service-owned `proto/` changes also select protocol
generation and compatibility checks.

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
credentials. This fixture proof supports the Auth/SDK compositions without
claiming their business behavior or later Kubernetes issuer routing.

Each invocation prints its Selection Plan and writes structured evidence below
`.test-results/`. Native package commands remain appropriate while developing
one owning package; the repository profiles are the pre-submission contract.

## Continuous integration

CI Go Race evidence jobs using the shared evidence action run at most two Go
package processes at a time. Each Race package can also run concurrent Go
goroutines, Bun children, and shared database work; the package limit is
intended to leave capacity for those owners on the four-CPU hosted runner.
It changes package scheduling only. Shards, test selection, per-scenario
concurrency, deadlines, and assertions retain their own policies. Local runs
keep their existing default and can set `--workers` explicitly.

The readable CI topology is:

- **Pull Request Verification**: read-only, hermetic PR evidence split by
  ownership. Four duration-balanced jobs cover every Go package under Race;
  TypeScript, protocol, deployment, image, security, and repository evidence
  have separate owners. **Merge Gate** reconciles exactly one result from every
  required producer for the same PR event, tested integration commit, workflow
  source, run, and attempt.
- **Main Branch Verification**: the same evidence owners run independently on
  the exact merged commit. Four jobs shard Go Race; repository, static,
  Runtime, Gateway, protocol, deployment, image, and security evidence run in
  parallel. Ordinary Go, Runtime, and Gateway coverage is a separate
  report-only artifact.
- **Scheduled Verification**: bounded repetitions of named concurrency owners
  each week, plus daily compatibility, repository-health, and online dependency
  audits. A later pass is recorded but never erases the first failure.
- **CI Benchmark**: dispatched by hand to measure a speed change; see
  [CI Benchmark](#ci-benchmark). Nothing gates on it.

`internal/testinfra/go_shard_weights.json` balances the four Go Race shards.
Each package listed there is split across shards by top-level test, weighted by
the test's measured duration or the package's `default_ms`; other packages stay
whole with relative weights. In the integration entry, every top-level test
that ran longer than about ten seconds carries its `Elapsed` duration from the
`go test -json` evidence of all four Go Race shards of Pull Request
Verification run 37512221678, collected on the `calibrated_at` date. Recalibrate
from the same per-test evidence of a current run, combining every shard of a
sliced package.

Report-only coverage runs `go test ./...` once without Race, so the integration
package executes every top-level test sequentially in one binary. Its Go
budget is 60 minutes: more than twice that package's measured Race total of
about 26 minutes, and longer than the other tests plus the SDK wrapper's
24-minute context. The coverage job's 80-minute limit leaves room for
dependency setup, compilation, and the Bun coverage commands around it.

Online Bun dependency audits are deliberately separated from deterministic
security checks. Pull requests run them when a `package.json`, `bun.lock`, or
the audit execution plumbing changes. The main-branch workflow does not repeat
the same remote query after merge; the daily scheduled workflow catches
advisories published after the dependency graph was accepted. Secret,
redaction, import, and boundary checks remain part of normal repository
evidence at every layer, including the dependency setup they require.

To force the online audit locally, run:

```sh
go run ./internal/testinfra/cmd/tetral-test \
  --profile full \
  --groups security \
  --dependency-audit always
```

The main-branch ruleset requires the **Merge Gate** produced by Pull Request
Verification. Individual producer jobs are evidence inputs; none can bypass or
substitute for the reconciled verdict.

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
provenance.

### CI Benchmark

`.github/workflows/ci-benchmark.yml` runs the four Go Race shards for a base
commit (side A) and for the dispatched branch head (side B) in one run, then
compares them. Each side checks out its own commit and runs that commit's
evidence action with the inputs of Pull Request Verification's Go Race job,
so A really runs the base's runner. The workflow only reads: its token holds
`contents: read` and `actions: read`, it uses no secrets, and
`cache-mode: read` stops every job from saving to the Actions cache. A
dispatch runs the dispatched branch's copy of the workflow, so the branch must
contain it. Dispatches on one ref queue rather than cancel each other; a run
holds eight Go Race jobs, so dispatch one at a time.

```sh
gh workflow run ci-benchmark.yml --ref <branch> [-f base=<commit>]
```

Without `base`, side A is the merge base of the branch with `origin/main`. A
branch stacked on another passes its base branch head, as a SHA or
`origin/<branch>`. A dispatch on `main` without `base` is an A/A run: both
sides test the same commit.

The **Compare A and B** job writes the report to the run summary and uploads
`compare.json` as `ci-benchmark-compare-<run>-<attempt>`. It first checks that
every result tested its side's commit. Per side and shard it reports the time
before the runner starts (GitHub steps, `go run` and planning), dependency
setup, preparation (job start to the first test), each step's compile time
before its first test, fixed preparation (time before the runner, setup and
the shard's longest step compile), the steps phase, job time, runner CPU model
and image, and Go Race runner-minutes. Metrics are d = A − B, so a positive d
means B is faster. Two effects shape how to read them:

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
  package costs its measured step; a slice of a split package costs its tests
  plus the overhead (time outside its tests, mostly compile) of that package's
  step in the same shard, or the package's mean overhead when that shard ran
  no step of it, so a run's own plan replays to its measured steps phase.
  `replayed_slowest_steps_phase` prices both plans with A's durations, taking
  B's only for packages A never ran;
  `plan_fixed_slowest_job` prices A's plan with each side's durations and adds
  that side's mean time before the runner and setup;
  `plan_fixed_runner_minutes` adds each side's durations replayed through A's
  plan to the runner time outside the steps phase (before the runner, setup,
  teardown and upload) of every job that side ran, so a side with more shards
  pays for each.

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
four shards divides it by two, a sum multiplies it by two, and a slowest-shard
metric uses it as is. `verdict` applies the rule for one metric and the
expected reduction E: E ≥ 10 s_D needs one run, 3 s_D ≤ E < 10 s_D three, and
2 s_D ≤ E < 3 s_D five. It passes when d > 0 in every run and the mean d is at
least E/2; below 2 s_D there is no measurable change. Every verdict also
requires B's plan-fixed runner-minutes and plan-fixed slowest job to stay
within A + 2 s_D, and refuses plan-dependent metrics from runs whose plans
differ.
`replayed_slowest_steps_phase` prices both plans over the same durations, so
A/A runs give it no s_D: its verdict always takes five runs.

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

Benchmark evidence never feeds shard calibration or the CI health job.
Calibrate `go_shard_weights.json` only from Pull Request or Main Branch
Verification evidence; the health job reads only its own run's `scheduled-*`
artifacts.

## Evidence and diagnosis

Structured results include the exact tested revision, selection, dependency
identity, command outcome, each step's start, end and duration, first failure,
and the CI execution envelope with the runner's CPU model and image.
Do not treat a rerun as erasing an earlier failure. Preserve the first result,
classify apparatus failures separately from product failures, and use the
printed focused reproduction command for diagnosis.
