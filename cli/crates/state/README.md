# state

The App Server's files and time belong here: paths under the state root,
private-file checks, the configuration and credential files, atomic
replacement, the clock and the test seams.

## Design rules

- It depends on no other workspace crate, holds no connection or Session logic
  and records nothing.
- Every timer and deadline of the CLI reads `seams::now`, a clock that counts
  the time the machine sleeps, never `std::time::Instant` (`seams`).
- Its test seams, `seams::check_harness`, `seams::barrier`,
  `seams::barrier_blocking` and `seams::count`, do nothing unless the crate
  feature `test-seams` is on (`seams`).

## Where things are

- `lib.rs`: this documentation and the print lints that `cli/README.md` states.
- `seams.rs`: the clock, the barriers, the counters and, under `test-seams`,
  the process side of the seam protocol.
