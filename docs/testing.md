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

The registered SDK Session preview proofs run through
`TestForkSDKPreviewCompatibilityProofs` in the existing Go evidence owner. Its
inventory declaration requires PostgreSQL, MinIO, NATS, Bun workspaces and the
pinned SDK. Full and Affected execute it while those dependencies are alive;
the normal CI Go Race shards include it. Fast compiles it without execution.
To reproduce with automatic setup, run from a clean Engine checkout:

```sh
go run ./internal/testinfra/cmd/tetral-test --profile full --groups go
```

The test runs the SDK's `test:compatibility:integration` command with the exact
Engine root and revision. Frozen SDK installation disables lifecycle scripts;
this launcher resolves source aliases directly and requires no SDK build or
`dist` directory. The SDK selects only the named public streaming Identity
subtest, validates executed Go JSONL and all five preview observations, and
settles its two registered handlers. The Engine wrapper requires matching
source/pass markers and exactly two passing handlers, so exit zero with an
empty registry cannot pass. Nested processes inherit the runner's process and
database cleanup custody. Their original output remains in the Go artifact.
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
manifest invariants, Helm rendering tests and Helm lint. Helm chart changes
under `deploy/helm/tetral/` also select the integration package, because its
direct Runtime TLS composition renders that chart. Changes to
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

## Evidence and diagnosis

Structured results include the exact tested revision, selection, dependency
identity, command outcome, duration, first failure, and CI execution envelope.
Do not treat a rerun as erasing an earlier failure. Preserve the first result,
classify apparatus failures separately from product failures, and use the
printed focused reproduction command for diagnosis.
