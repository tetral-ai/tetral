# tetral

The `tetral` program belongs here: its commands, the assembly of the App
Server into one process, and the internal entry points that the App Server
runs as child processes of itself.

## Design rules

- Every command writes exactly one compact JSON object and a newline to
  stdout, and its records to stderr. It exits 0 with its result, or 1 with
  `{"error": {"code", "message"}}`; a message is fixed text, or names a path
  and the operating system's error, and never holds a key (`output`).
- Every command waits for its records to be written before it exits, at most
  `observe::EXIT_FLUSH_LIMIT`; after a shutdown, never past the shutdown's
  deadline, so that `server run` exits within it (`commands`, `server`).
- Run with `argv[0]` `codex-linux-sandbox`, it is Codex's sandbox helper
  (`codex_linux_sandbox::run_main`) and nothing else, decided before any
  argument is parsed: every sandboxed command runs this executable under that
  name (`main`).
- Run with `argv[0]` `tetral-cwd-probe`, it is the App Server's directory
  probe and nothing else: it reads at most
  `session::probe_child::INPUT_MAX_BYTES` of stdin and writes the probe's one
  line to stdout (`main`).
- `tetral --version` alone answers next, reading no environment variable and
  no file. Every other command runs the checks of `commands` in order (the
  arguments, the root checks of `state::private`, the app recorded in an
  existing `config.toml`) before it does anything, and the first that fails
  ends it (`commands`).
- Its errors follow the error rule of `cli/README.md`.
- Its feature `test-seams` turns on `state/test-seams` and
  `execution/test-seams` for the builds of the cross-process tests and is
  never enabled in a release build (`Cargo.toml`). Under it, `argv[0]`
  `tetral-isolation-harness` runs one command through the isolation's check
  and launch (`execution::isolation::harness`) and writes its one answer
  line (`main`).

## Commands

- `tetral --version`: `{"cli_version":"<version>"}`, the package version.
- `tetral configure --app <app> --from-stdin`: reads a bootstrap of at most
  65,536 bytes from stdin and checks it, creates a missing root and `run/`,
  takes the root's lock without waiting, removes the temporary files of
  interrupted replacements, checks the recorded app again, and replaces
  `credentials.json`, then `config.toml`, which keeps its local
  options; it writes `{}`. It never reads `credentials.json`, so it replaces a
  missing or broken credential and a pair that disagrees.
- `tetral server run --app <app> [--idle-exit <duration>]`: runs the App
  Server in the foreground (`appserver`) until a shutdown, then writes the
  status as of its exit. Its records from the configuration phase on wait for
  the log file, which it opens once the start can no longer fail; one that
  cannot be opened is recorded as `log.file_open_failed`, and the App Server
  runs on. The command checks `--idle-exit`'s `<duration>`, a positive
  integer followed by `s`, `m` or `h`, with its other arguments; the App
  Server does not act on it.
- `tetral server status --app <app>` and `tetral server stop --app <app>`:
  `GET /v1/status` and `POST /v1/shutdown` through the SDK, writing the status
  or `{}`; `server_not_running` when no App Server answers. `server stop`
  returns at the `202`, before the App Server has exited.

## Where things are

- `src/main.rs`: `main`, the sandbox helper's dispatch, and the entry points
  of the directory probe and of the isolation harness. Past
  `--version`, it first checks the test harness
  (`state::seams::check_harness`).
- `src/commands.rs`: the command line and the checks every command runs.
- `src/server.rs`: `server run`, `server status` and `server stop`, and the
  codes of their failures.
- `src/configure.rs`: `configure`, its commit with the barriers
  `configure.locked`, `configure.credentials_replaced` and
  `configure.config_replaced`, and the in-process tests of a commit stopped
  at each.
- `src/output.rs`: the error codes, `CommandError` and the one result object.
