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

## Where things are

- `lib.rs`: `spawn_task_supervisor`, `SupervisorMessage` and
  `SupervisorMailbox`.
- `policy/mod.rs`: `TaskPermissions`, a task's stored permissions, and
  `PolicyRejection`.
- `policy/validate.rs`: the validation steps, path normalization and the
  network rules' host, CIDR and address forms.
- `policy/translate.rs`: `TranslationInputs` and the profile's entries.
