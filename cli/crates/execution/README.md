# execution

Tool execution on this machine belongs here: one **Task supervisor** per task,
which owns that task's process tree; the translation of a call's
`sandbox_policy` into the operating system's isolation; and the tools.

## Design rules

- Supervisors follow the owner rules of `cli/README.md`.
- A call's permissions are decided before any side effect, read no user file
  and start no process. `policy::TaskPermissions::admit` validates the
  `sandbox_policy` value, where the first failing check decides between
  `invalid_input` and `unsupported_policy`, and translates it.
- Every call, however broad its grant, runs under one restricted Codex
  `PermissionProfile`: the policy's rules one to one, plus mandatory
  protections that no rule reaches (`policy/translate.rs`). A task's
  permissions are built once and never rebuilt.
- Every process of a tool call starts only through `isolation::Isolation`:
  `check` at each execution, then `launch`, which only an `Available` from
  that check can call. The launch has Codex's sandbox manager compile the
  task's profile, filesystem and network together, into one `bwrap` and
  seccomp command with the preference `Require`, so that no grant runs
  unsandboxed, and starts it with exactly the environment the tool gives and
  no descriptor beyond its pipes (`isolation/mod.rs`).
- The `bwrap` is the one Codex's helper picks for the same command: the
  system `bwrap` on `PATH` that passes the `--help` probe, otherwise the
  bundled one beside `tetral` whose digest matches `CODEX_BWRAP_SHA256`. None,
  WSL1 or a refusal of user namespaces rejects the call
  `sandbox_unavailable`; a command the transform cannot build rejects it
  `unsupported_policy`; neither starts a process (`isolation/linux.rs`).
  Every `bwrap` it could pick, like the running `tetral`, is a trusted
  program that each task's profile keeps read-only
  (`isolation::trusted_programs`).
- A missing protected path stays uncreatable. The Codex crates mount an
  empty read-only entry at a missing top-level `.git` of a writable path, and
  at a missing `read` or `deny` path inside one, which `bwrap` must first
  create; where no process of the user could create it, the launch profile
  leaves the protection to the operating system instead
  (`isolation/linux.rs`, `launch_profile`).
- No report is handed to the Report outbox that Engine could never accept:
  an error's `message` is cut to `ERROR_MESSAGE_MAX_BYTES` on UTF-8
  boundaries and its `detail` rebuilt within `ERROR_DETAIL_MAX_BYTES`, and a
  process result over the ordinary limits is rendered again from its stored
  output with half the visible byte budget until it fits, its counters and
  exit facts unchanged (`report`).
- Each probe of a `bwrap` candidate is bounded by `PROBE_TIMEOUT` and
  `PROBE_OUTPUT_LIMIT_BYTES`, which `Isolation::with_probe_bounds`
  overrides.

## Where things are

- `lib.rs`: `spawn_task_supervisor`, `SupervisorMessage` and
  `SupervisorMailbox`.
- `policy/mod.rs`: `TaskPermissions`, a task's stored permissions, and
  `PolicyRejection`.
- `policy/validate.rs`: the validation steps, path normalization and the
  network rules' host, CIDR and address forms.
- `policy/translate.rs`: `TranslationInputs` and the profile's entries.
- `isolation/mod.rs`: `Isolation`, its `check` and `launch`, `Available`,
  `Command`, `LaunchError` and `trusted_programs`.
- `isolation/linux.rs`: the choice of `bwrap` and its candidates, the digest
  check of the bundled one, the WSL1 test, the namespace probe and the
  launch profile.
- `report.rs`: `Report`, its `Correlation` and `Body`, the error bounds
  (`bounded`) and the size fitting of a process result (`fit`).
- `isolation/harness.rs`: the test entry `tetral-isolation-harness`, under the
  feature `test-seams`: one command through admission, the check and the
  launch.
