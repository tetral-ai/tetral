//! A stand-in for the sandbox launcher `bwrap` that launches nothing: it
//! writes the file `stderr` beside its own executable to stderr and exits 1.
//! It is a program rather than a script because the bundled `bwrap` is run
//! through a descriptor that closes at exec, which a script's interpreter
//! could not open.

use std::io::Write;
use std::process::ExitCode;

fn main() -> ExitCode {
    let written = std::env::current_exe()
        .and_then(|executable| std::fs::read(executable.with_file_name("stderr")))
        .unwrap_or_default();
    let _ = std::io::stderr().write_all(&written);
    ExitCode::FAILURE
}
