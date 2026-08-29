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
  plus compatibility and repository-health evidence. A later pass is recorded
  but never erases the first failure.

The existing **engine-ci** workflow and its required checks remain authoritative
during the measured shadow period. Pull Request Verification uploads shadow
evidence but cannot satisfy or bypass those checks. A future ruleset change is a
separate, reviewed operation after old/new results have been compared; this
repository state does not perform that cutover.

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
The shadow collector and observation-window enumerator are read-only. Enumerate
the complete GitHub window before collecting individual rows:

```bash
go run ./internal/testinfra/cmd/tetral-shadow-enumerate \
  --repository tetral-ai/tetral \
  --eligible-after <RFC3339-window-start> \
  --output shadow-universe.json
```

Then collect every enumerated PR head:

```bash
go run ./internal/testinfra/cmd/tetral-shadow-collect \
  --repository tetral-ai/tetral \
  --pull-request <number> \
  --output shadow-ledger.json
```

It joins GitHub API facts, legacy metadata sidecars, and new result artifacts.
Mismatched carriers, workflow sources, Apps, runs, attempts, rerun ancestry,
jobs, checks, or durations are rejected instead of compared.

After collection, evaluate the complete ledger with the fixed paired-row
estimator. The pull request that introduced shadow collection is excluded:

```bash
go run ./internal/testinfra/cmd/tetral-shadow-evaluate \
  --input shadow-ledger.json \
  --universe shadow-universe.json \
  --introduction-pull-request <shadow-workflow-pr> \
  --introduced-by-commit <merged-introduction-commit> \
  --workflow-source-sha <eligible-workflow-source> \
  --eligible-after <introduction-merge-time-rfc3339> \
  --output shadow-acceptance.json
```

The evaluator fails closed until it has ten distinct, first-attempt,
all-successful old/new integration tuples with the required change classes, a
real external-fork approval observation, acceptable wall and runner cost, and
balanced Go shards. Reruns and incomplete rows remain in the reliability and
cost record but cannot improve the acceptance medians. Any unexplained failed,
missing, skipped, or disagreeing row blocks acceptance even when ten other rows
are green. For an external fork, capture the exact workflow-run JSON while its
status is `action_required`, then pass that file with `--fork-pending-capture`,
the agreed Issue number with `--agreed-issue`, and the maintainer agreement
comment ID with `--agreement-comment`. The collector joins those facts to
GitHub's read-only approval history, exact fork head, and closed or merged PR;
two operator-entered timestamps are not approval evidence. A same-repository or
organization-member fixture is not equivalent evidence.

The shadow gate is currently pending. Until its report is green and a separate
repository-policy change is explicitly authorized, the legacy workflow and
required checks remain authoritative.

Once the measured gate is green, the repository can prepare an offline policy
bundle from read-only GitHub captures and an exact Git archive:

```bash
gh api repos/tetral-ai/tetral/rulesets/<ruleset-id> > ruleset.json
gh api repos/tetral-ai/tetral > repository.json
gh api repos/tetral-ai/tetral/actions/permissions > actions.json
git archive --format=tar HEAD > legacy-capable-source.tar
make test-full
go run ./internal/testinfra/cmd/tetral-policy-plan \
  --repository tetral-ai/tetral \
  --ruleset ruleset.json \
  --repository-settings repository.json \
  --actions actions.json \
  --legacy-archive legacy-capable-source.tar \
  --legacy-proof-result <full-result-for-this-clean-head.json> \
  --source-commit "$(git rev-parse HEAD)" \
  --tree-sha "$(git rev-parse HEAD^{tree})" \
  --output github-policy-cutover.json
```

This command validates the captured legacy protections, binds the rollback
archive to the named commit and tree, and rehearses every policy-transition and
recovery failure point. It only writes a mode-`0600` plan; it never calls a
GitHub mutation API. Applying that byte-identical bundle requires separate
Owner authorization and readback after every step.

Final-state recovery has two explicit paths. The normal path restores the
legacy workflow through an exact-head, Merge-Gate-protected pull request before
restoring its required contexts. If Merge Gate cannot report, recovery requires
a separately authorized exclusive maintenance window: automatic merge stays
disabled, all open pull requests and the sole restore head are recorded, the
required-check rule is removed temporarily while pull-request, deletion, and
non-fast-forward protections remain, and only that restore head may merge. The
eleven legacy contexts must report before the captured legacy ruleset is
restored and the window closes. Permanent administrator bypass is not a
recovery mechanism.

Do not treat a rerun as erasing an earlier failure. Preserve the first result,
classify apparatus failures separately from product failures, and use the
printed focused reproduction command for diagnosis.
