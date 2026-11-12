# Tetral AI Engine

Open-source agent control-plane: durable sessions, sandboxed agent runtime,
provider gateway. This file is the index — where things are and how to work
here. Detailed rules live in the documents and tests that own them; nothing
is restated here.

## Doc index

| Doc | What it holds |
|-----|---------------|
| `README.md` | The front page: what Tetral is, how to install it, where to go next. |
| `services/<name>/README.md` | Per-service contract: responsibilities, lifecycle, seams, testing. `services/api` also holds the boot environment, the workspace isolation model, and `/v1` surface status. |
| `internal/{computer,drive,hubflow,imagestore,instance,protocol,runtimecontrol}/doc.go` | New Go-package responsibilities, lifecycle invariants, file maps and update dependencies. |
| `internal/<name>/README.md` | Shared-package contracts: process lifecycle and diagnostics (`internal/workload`), Runtime configuration and control, MCP manifests, TypeScript observability and database pools. |
| `integration/<name>/README.md` | Cross-process test harnesses: the test clients (`integration/enginetest`) and the edge's secret helper (`integration/envoy-gateway-secret-helper`). |
| `deploy/**/README.md` | Installation and deployment contracts for the Helm chart (`deploy/helm/tetral`), the raw manifests and any separately installed dependency. |
| `cli/README.md` | The Tetral CLI: its Cargo workspace, crates, state owners, dependencies, and build and test commands; each crate's own `README.md` is its orientation. |
| `protocol/README.md` | The protocol between the CLI and Engine: connections, tool execution, the MCP proxy, drive transfer and drives on a machine, with every limit, timeout and error code; its schemas and fixtures sit beside it. |
| `docs/*.md` | Cross-service guides, for example testing and CI ownership, bootstrap, authentication (`docs/authentication.md`), and the service-stack upgrade from the previous release (`docs/upgrade-service-stack.md`). |
| `CONTRIBUTING.md` | The pre-PR self-check every contribution runs. |

## Service map

| Service | Role |
|---------|------|
| `services/auth` | API-key and federated bearer authentication; current workspace authority; signed principals; token exchange and `/v1/api_keys`. |
| `services/api` | Public REST control plane (sessions and their computers, drives, agents, environments, vaults, memory, files, skills). |
| `services/queue` | Durable job queue: lease, transition, wake. |
| `services/sandbox` | Provider-backed Sandbox lifecycle, tool execution, and resource projection. |
| `services/bridge` | Runtime RPC APIs, context loading, declaration and result settlement, attachment reads. |
| `services/job-runner` | Queue consumption, Runtime placement/delivery, binding-loss recovery and Session cleanup. |
| `services/agent-runtime` | Hot in-pod TypeScript Runtime Core (agent loop, tools). |
| `services/gateway` | Provider lowering and MCP connector packages, deployed as independent workloads. |
| `services/web-connector` | Independently deployed web search/fetch backend. |
| `services/event-stream` | Read-only public event list and SSE. |
| `services/computer-hub` | The machine side: CLI connections under `/v1/client/`, instances and their connection credentials, instance requests, Session preparation reports, and computer Git tickets. |
| `services/git-proxy` | Credential-injecting Git smart-HTTP proxy. |
| `services/cleanup` | Scheduled cleanup. |

## Build and test

Two products, each verified by its own command; `docs/testing.md` holds the
test layers, each command's expected duration, how to stop it and what CI runs.
A change under `cli/` runs the CLI's command only; a change under `protocol/`
runs both; any other change runs Engine's.

- Engine: run `make test-affected` and `make lint` before pushing.
- The CLI: run `python3 scripts/verify-cli.py <profile> --evidence-dir <dir>`
  from the repository root; `--help` lists the profiles and `cli/README.md`
  describes them.

Local test prerequisites include Python 3 (`python3` on PATH) for the release
command fixtures, in addition to Go and Bun, and `rustup` for the CLI.

Go (the root module, run from the repository root; the CLI's reference control
server is a module of its own, see `cli/README.md`): `make build` / `make test` /
`make test-affected` / `make test-full` / `make lint` / `make vulncheck`;
`make run-<workload>` boots one workload locally. `make test` is the fast,
no-infrastructure profile. `make test-affected` selects the current change's
owners and starts only their declared dependencies, falling back to Full when
ownership is unknown. `make test-full` is the complete hermetic pull-request
profile. Focused database tests still accept `TETRAL_TEST_DATABASE_URL` as an
administrative DSN; each test receives a private cloned database and runtime
role rather than a shared writable schema.

TypeScript (Bun; run inside `services/agent-runtime` or
`services/gateway` package dirs): `bun install --frozen-lockfile`,
`bun run typecheck`, `bun run test`, `bun run build`.

Rust (the Tetral CLI, a Cargo workspace with its own toolchain; run every
`cargo` command inside `cli/`, never with `--manifest-path` from the root):
`cargo build`, `cargo test`; `cargo test` writes its acceptance records into
the directory that `TETRAL_ACCEPTANCE_DIR` names and fails without it.

Pull requests are gated by two required checks: `Merge Gate` of
`.github/workflows/pull-request-verification.yml` and `CLI results` of
`.github/workflows/cli-verification.yml`. Repository-wide
architecture guards live in `integration/static`; when a check fails, the
failing test is the rule's authoritative text — read it.

## Contributing

Any authorship is welcome — human, AI, or both. Contributions nobody
understands are not. Before opening a PR, run the self-check in
[CONTRIBUTING.md](CONTRIBUTING.md) and paste the filled answer sheet into the
PR description — the PR template carries its headings. A PR without the sheet
gets the short review path. Coding agents reach the same procedure as a
`pr-selfcheck` skill (`.agents/skills/` for Codex, `.claude/skills/` for
Claude Code); both point at the same text.

- Commits: [Conventional Commits](https://www.conventionalcommits.org/),
  `<type>(<scope>): <description>`, with the types `feat`, `fix`, `test`,
  `docs`, `refactor`, `perf`, `build`, `ci` and `chore`. The scope is the
  changed component's directory name: `computer-hub` for
  `services/computer-hub/`, `instance` for `internal/instance/`, and `cli` for
  anything under `cli/`; a change across several components has no scope. A
  body only when the reason is not evident from the change. PR titles take
  the same form.
- PRs: the answer sheet filled; validation green; tests accompany behavior
  changes; docs move in the same PR as the behavior they describe; no secrets
  in code, logs, or responses.
