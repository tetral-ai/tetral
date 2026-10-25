#![doc = include_str!("../README.md")]

mod commands;
mod configure;
mod output;
mod server;

use std::process::ExitCode;

fn main() -> ExitCode {
    let arguments: Vec<_> = std::env::args_os().collect();
    if commands::is_version(&arguments) {
        return output::write(Ok(output::version()));
    }
    state::seams::check_harness();
    observe::STARTUP.process_started(state::seams::now());
    output::write(commands::run(arguments))
}
