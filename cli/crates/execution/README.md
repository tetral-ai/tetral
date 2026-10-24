# execution

Tool execution on this machine belongs here: one **Task supervisor** per task,
which owns that task's process tree; the translation of a call's
`sandbox_policy` into the operating system's isolation; and the tools.

## Design rules

- Supervisors follow the owner rules of `cli/README.md`.

## Where things are

- `lib.rs`: `spawn_task_supervisor`, `SupervisorMessage` and
  `SupervisorMailbox`.
