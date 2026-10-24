# session

The Session binding owners belong here, one task per Session on this machine,
each the owner of its Session's record, bindings and their states and of the
admission of new work; and the directory probe and repository clones.

## Design rules

- A `session.close` and a command for the same Session are ordered by the
  arrival order of the owner's mailbox: once the close is handled nothing more
  starts, and a command handled before it has already started.
- An owner starts admitted commands only from its handler, through the
  `TaskStarter` it is given, so a start never races the Session's close.
- Owners follow the owner rules of `cli/README.md`.

## Where things are

- `owner.rs`: `spawn`, `SessionMessage`, `SessionMailbox` and the admission of
  commands (`Admission`, `TaskStart`, `TaskStarter`). Of the owner's state it
  holds whether the Session's close was admitted.
