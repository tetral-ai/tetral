#![doc = include_str!("../README.md")]

mod commands;
mod configure;
mod output;
mod server;

use std::io::Write;
use std::process::ExitCode;

use session::probe_child;

fn main() -> ExitCode {
    let arguments: Vec<_> = std::env::args_os().collect();
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
