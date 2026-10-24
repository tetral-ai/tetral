# session

The Session binding owners belong here, one task per Session on this machine,
each the owner of its Session's record, bindings and their states and of the
admission of new work; and the directory probe and repository clones.

## Design rules

- Owners follow the owner rules of `cli/README.md`.

## Where things are

- `owner.rs`: `spawn`, `SessionMessage` and `SessionMailbox`.
