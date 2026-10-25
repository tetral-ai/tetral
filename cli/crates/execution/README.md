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
  `Command` and `LaunchError`.
- `isolation/linux.rs`: the choice of `bwrap`, the digest check of the
  bundled one, the WSL1 test and the namespace probe.
- `isolation/harness.rs`: the test entry `tetral-isolation-harness`, under the
  feature `test-seams`: one command through admission, the check and the
  launch.
