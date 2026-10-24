# tetral-tests

Tests of the workspace as a whole, and the harness of the cross-process
tests: those that run the `tetral` binary and the reference control server of
`cli/reference/` as separate processes, over real HTTP and SSE, on real
temporary directories. No workspace crate depends on this package.

## Design rules

- A cross-process test runs `binaries::tetral()`, built once per test process
  with `cargo build -p tetral --features test-seams --target-dir
  target/test-seams` in `cli/`, so that its seams answer to the test, and
  `binaries::reference_server()`, built with `go build` at the repository
  root. It never runs a binary of the target directory that `cargo test`
  itself holds.
- A test starts `tetral` as a child process whose environment holds only what
  the test sets (`run::Tetral`), its state on a temporary directory, and wires
  it to the test's `Seams` socket when the test drives its clock or barriers.
  A run that has not exited within `seams::WAIT_LIMIT` is killed and fails the
  test (`run::finish`).
- Time and ordering in a test come from the seams (`seams::Seams`): the
  `tetral` under test reads its clock from the test and waits at the barriers
  the test holds, and the reference control server's clock moves only through
  its `clock` control. A sleep is never the evidence that something did not
  happen; `seams::WAIT_LIMIT` bounds only a wait for something that must
  happen.
- Each run of a test file of `tests/` writes its acceptance record into the
  directory that `TETRAL_ACCEPTANCE_DIR` names (`record`), and fails at once
  when it is unset. A case that needs a non-root user is recorded as not run,
  with its reason, when the test runs as root
  (`record::Case::runs_as_non_root`); no case skips silently.

## Where things are

- `src/lib.rs`: `workspace_dir` and `repository_root`, where the builds run.
- `src/binaries.rs`: the builds of `tetral` and the reference control server.
- `src/reference.rs`: `ReferenceServer`, the server as a process, its startup
  input and its controls.
- `src/run.rs`: `Tetral`, a command line of the `tetral` under test, and
  `Run`, its exit status and output.
- `src/seams.rs`: `Seams`, the harness side of the seam protocol, which
  `state::seams` defines.
- `src/record.rs`: `Record`, `Case` and the `record!` macro.
- `tests/crate_graph.rs`: the rules of the crate graph, checked over the
  output of `cargo metadata`, and the rule that no library crate prints,
  checked over the library crates' sources.
- `tests/release_seams.rs`: a release build of `tetral` holds no test seam.
