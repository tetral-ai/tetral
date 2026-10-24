# tetral

The `tetral` program belongs here: its commands, the assembly of the App
Server into one process, and the internal entry points that the App Server
runs as child processes of itself.

## Design rules

- Its errors follow the error rule of `cli/README.md`.
- Its feature `test-seams` turns on `state/test-seams` for the builds of the
  cross-process tests and is never enabled in a release build (`Cargo.toml`).

## Where things are

- `src/main.rs`: `main`, the program's entry point; it has no commands. It
  first checks the test harness (`state::seams::check_harness`).
