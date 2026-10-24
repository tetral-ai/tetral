# state

The App Server's files and time belong here: paths under the state root,
private-file checks, the configuration and credential files, atomic
replacement, the clock and the test seams.

## Design rules

- It depends on no other workspace crate, holds no connection or Session logic
  and records nothing.

## Where things are

- `lib.rs`: this documentation and the print lints that `cli/README.md` states.
