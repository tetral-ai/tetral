# Tetral Agent Runtime Instructions

This directory is an agent Runtime. It is not a local CLI, desktop agent, local
filesystem operator, or deployment platform. Root project rules in
[../../AGENTS.md](../../AGENTS.md) still apply; this file adds the concrete
Runtime, testing, and architecture rules. The service contract itself lives in
[README.md](README.md).

These are stable engineering rules. They must not encode feature-specific
architecture, event names, identity fields, storage schemas, provider payloads,
lifecycle protocols, or roadmap detail — that belongs in the active issue and
execution contract. When an approved issue or contract defines a protocol, field
set, lifecycle, or scope boundary, follow that artifact instead of adding
compensating detail here. If this file appears to conflict with the active issue
or contract, preserve the stable rule intent here and follow the issue or
contract for the concrete behavior.

## TypeScript

Use Bun and TypeScript. Package scripts are the validation contract:
typecheck, lint/format, test, build, and compatibility checks run through
package scripts when configured.

- Import type-only symbols with `import type {}`; do not mix type and value
  imports.
- Use explicit `.js` import suffixes.
- Use Zod v4 only: import from `zod/v4`.
- Do not use `as any`, `@ts-ignore`, or `@ts-expect-error`.
- Prefer `readonly`, `as const`, `const`, early returns, dot notation, and
  inference inside functions.
- Add explicit types for exported contracts and schema-derived public types.
- Use full English, directly greppable names.
- Avoid single-letter variables except ordinary loop indices.
- Avoid destructuring when it removes useful object context.
- Do not use `export namespace`.
- Prefer flat exports and concrete sibling imports.
- Avoid barrel `index.ts` files for directories with unrelated siblings.
- If useful, self-reexport namespace-style surfaces:
  `export * as Provider from './provider.js'`.

Runtime `tsconfig` and lint config are part of the contract. Keep them explicit,
type-aware where configured, and aligned with Bun/ESM execution. Lint disables
must be local, named, and justified.

## Dependencies

Business logic receives dependencies; entrypoints assemble them. Route I/O,
time, randomness, logs, metrics, retries, queues, clients, and other side
effects through explicit dependencies or boundary adapters.

Tests inject fakes. Production dependency factories must never return fake,
mock, test, fallback, or implicitly configured clients.

Do not hide clients, mutable process state, watchers, queues, or background
resources in global state. If module-level state is unavoidable, expose explicit
cleanup for tests and shutdown.

## Config

All environment reads go through config loading. Do not read `process.env`
inside business logic.

Parse boot config once with Zod and export typed immutable config. Flags must
declare defaults, scope, and boot-time versus access-time behavior. Booleans
accept only explicit true/false forms. Numeric flags are positive integers
unless zero is explicitly meaningful.

Production must not use mock fallback, test fallback, implicit credentials, or
debug-only behavior.

## Boundaries

Cross-boundary data crosses as typed contracts or normalized domain objects,
never raw SDK, database, HTTP, filesystem, sandbox, or driver payloads.

Use Zod at trust boundaries, not for every internal function call. Use
`z.strictObject()` for owned API contracts. Use `safeParse()` when callers must
return structured errors. Use `parse()` for boot-time config that must fail
fast.

Do not create compatibility adapters, alternate schemas, renamed aliases, or
dual protocols unless the active issue or contract explicitly requires them.

## Errors And Logging

Normalize errors at the owning boundary. Never pass raw provider, sandbox,
database, SDK, HTTP, filesystem, or driver errors across package boundaries.
Raw dependency errors may appear only as private causes or sanitized diagnostic
details inside the owning layer.

Public API errors, model-visible errors, event-stream errors, app logs,
diagnostics, and debug logs are separate audiences. Each mapper chooses allowed
fields explicitly. Public messages and safe details are bounded.

Timeout is not cancellation. Normalize cancellation, shutdown, operation
timeout, stream idle timeout, and capacity timeout distinctly when the active
contract requires those outcomes. Do not retry cancellation. Retry timeouts only
when the operation is idempotent and retry is explicitly allowed.

No production `console.log`. Use structured logs with bounded metadata and
redaction. Telemetry metadata that may contain user text, code, paths, URLs,
prompts, headers, tool output, provider payloads, or credentials must pass
through a PII-safe helper before leaving the owning layer.

## Observability

Observability is a side channel for logs, metrics, and traces. It must not drive
business decisions, reducers, storage ownership, provider control flow,
permission decisions, retry decisions, or terminal status.

Observability failures must not change runtime behavior. Fire-and-forget work
must attach error handling. Flush, drain, queue, exporter, and backend behavior
belong only where an active contract explicitly adds that integration.

## Cancellation And Cleanup

Every long-running external operation accepts an `AbortSignal` and an explicit
timeout budget when cancellation is in scope. When combining signals, timers, or
listeners, register cleanup that runs in `finally`.

Cancelled operations must not write successful results after cancellation.
Cleanup must be idempotent. Bounded cleanup is preferred over unbounded waits.

## Testing

Tests prove behavior at the smallest useful boundary.

- Unit tests cover pure logic, schemas, config parsing, error mapping, and
  normalization without network, real credentials, or live external services.
- Integration tests cover package boundaries with fakes, emulators, containers,
  or explicitly named staging dependencies.
- Smoke tests use live dependencies only through explicit opt-in.

Production modules must not change behavior through `NODE_ENV === 'test'`. Use
typed modes, config, and injected dependencies.

Prefer fixture-backed integration tests over broad mocks. Mock only external
network, time, ids, clocks, credentials, and storage boundaries.

Do not add tests whose only purpose is to prove removed legacy fields, aliases,
or compatibility paths are absent unless the active contract explicitly asks for
that proof. Prefer positive behavior tests plus static scans for cleanup-only
surfaces.

Before committing package changes, run package validation scripts, at minimum:

```sh
bun run typecheck
bun test
```

Run smoke tests only when live verification is required and opt-in environment
variables exist.

## Security

Credentials live only in config, client construction, and secret storage. No
credentials in logs, errors, traces, model messages, tool results, database
records, or sandbox payloads unless the receiving contract explicitly requires
them.

Fail closed on missing policy, unknown external capability, unknown provider,
invalid config, malformed dependency response, and ambiguous authorization.

Bound all untrusted input and output before it crosses package, process, or
network boundaries. Large or binary output must be bounded, encoded, or
artifact-linked according to the active contract.
