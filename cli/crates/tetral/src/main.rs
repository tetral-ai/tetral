#![doc = include_str!("../README.md")]

mod commands;
mod configure;
mod output;
mod server;

use std::ffi::OsStr;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

use session::probe_child;

fn main() -> ExitCode {
    let arguments: Vec<_> = std::env::args_os().collect();
    let argv0 = arguments.first().map(Path::new);
    // A sandboxed command runs this executable under this name, before any
    // argument of it is parsed. The walk covers a `bwrap` planted after the
    // availability check, before Codex's lookup could choose it. The inner
    // stage, which `bwrap` starts inside the sandbox, looks up no `bwrap`, and
    // its root, a directory of the user, would make every candidate look
    // replaceable.
    if argv0.and_then(Path::file_name)
        == Some(OsStr::new(
            codex_sandboxing::landlock::CODEX_LINUX_SANDBOX_ARG0,
        ))
    {
        #[cfg(target_os = "linux")]
        if !arguments[1..]
            .iter()
            .take_while(|argument| argument.as_os_str() != "--")
            .any(|argument| argument == "--apply-seccomp-then-exec")
            && let Some(planted) = execution::isolation::planted_launcher()
        {
            let _ = writeln!(std::io::stderr(), "{planted}");
            return ExitCode::FAILURE;
        }
        codex_linux_sandbox::run_main();
    }
    #[cfg(feature = "test-seams")]
    if arguments
        .first()
        .is_some_and(|argv0| execution::isolation::harness::is_harness(argv0))
    {
        return isolation_harness();
    }
    if arguments
        .first()
        .is_some_and(|argv0| probe_child::is_probe(argv0))
    {
        return probe();
    }
    if commands::is_version(&arguments) {
        return output::write(Ok(output::version()));
    }
    state::seams::check_harness();
    observe::STARTUP.process_started(state::seams::now());
    output::write(commands::run(arguments))
}

/// The directory probe: one input on stdin, one line on stdout.
fn probe() -> ExitCode {
    let input = probe_child::read_input(std::io::stdin(), probe_child::INPUT_MAX_BYTES);
    let line = probe_child::run(&input);
    let mut stdout = std::io::stdout().lock();
    match stdout.write_all(&line).and_then(|()| stdout.flush()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

/// The isolation's test entry: one request on stdin, one answer line on
/// stdout.
#[cfg(feature = "test-seams")]
fn isolation_harness() -> ExitCode {
    use std::io::Read;
    let mut input = Vec::new();
    let line = match std::io::stdin().read_to_end(&mut input) {
        Ok(_) => execution::isolation::harness::answer(&input),
        Err(error) => format!(
            "{}\n",
            serde_json::json!({"spawn_error": error.to_string()})
        ),
    };
    let mut stdout = std::io::stdout().lock();
    match stdout
        .write_all(line.as_bytes())
        .and_then(|()| stdout.flush())
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
