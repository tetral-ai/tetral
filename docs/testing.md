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

Each invocation prints its Selection Plan and writes structured evidence below
`.test-results/`. Native package commands remain appropriate while developing
one owning package; the repository profiles are the pre-submission contract.

## Continuous integration

The readable CI topology is:

- **Pull Request Verification**: read-only, hermetic PR evidence split by
  ownership. Four duration-balanced jobs cover every Go package under Race;
  TypeScript, protocol, deployment, image, security, and repository evidence
  have separate owners. **Merge Gate** reconciles exactly one result from every
  required producer for the same PR event, tested integration commit, workflow
  source, run, and attempt.
- **Main Branch Verification**: integrated correctness and unsharded full Race
  on the exact merged commit. Ordinary Go, Runtime, and Gateway coverage is a
  report-only artifact here, not a PR Race burden.
- **Scheduled Verification**: bounded repetitions of named concurrency owners
  each week, plus daily compatibility, repository-health, and online dependency
  audits. A later pass is recorded but never erases the first failure.

Online Bun dependency audits are deliberately separated from deterministic
security checks. Pull requests run them when a `package.json`, `bun.lock`, or
the audit runner changes. The main-branch workflow does not repeat the same
remote query after merge; the daily scheduled workflow catches advisories
published after the dependency graph was accepted. Secret, redaction, import,
and boundary checks remain part of normal repository evidence at every layer.

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
