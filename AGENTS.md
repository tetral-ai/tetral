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
| `CONTRIBUTING.md` | The pre-PR self-check every contribution runs. |

## Service map

| Service | Role |
|---------|------|
| `services/auth` | API-key auth at the edge; signed internal principals; `/v1/api_keys`. |
| `services/api` | Public REST control plane (sessions, agents, environments, vaults, memory, files, skills). |
| `services/queue` | Durable job queue: lease, transition, wake. |
| `services/sandbox` | Provider-backed Sandbox lifecycle, tool execution, and resource projection. |
| `services/bridge` | Durable runtime reconciliation, Runtime APIs, settlement, cleanup. |
| `services/agent-runtime` | Hot in-pod TypeScript Runtime Core (agent loop, tools). |
| `services/gateway` | Provider lowering + MCP connector. |
| `services/web-connector` | Web search/fetch backend (gateway pod container). |
| `services/event-stream` | Read-only public event list and SSE. |
| `services/git-proxy` | Credential-injecting Git smart-HTTP proxy. |
| `services/cleanup` | Scheduled cleanup. |

## Build and test

Go (one module, run from the repository root): `make build` / `make test` /
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

Pull requests are gated by `.github/workflows/pull-request-verification.yml`;
its `Merge Gate` job is the repository's required check. Repository-wide
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

- Commits: `[engine] <description>`, body lines `Scope:` / `Goal:` / `Test:`.
- PRs: the answer sheet filled; validation green; tests accompany behavior
  changes; docs move in the same PR as the behavior they describe; no secrets
  in code, logs, or responses.
