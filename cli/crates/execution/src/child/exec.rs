//! `exec_command`'s launcher entry, `__tetral-task exec <directory> <program>
//! <args>...`, run inside the sandbox with the launch-status pipe at
//! [`PASSED_DESCRIPTOR`]: it checks and enters the command's working
//! directory, then executes `[<shell>, "-c", <cmd>]`, so that a directory
//! missing or removed by the time the command starts is a directory error and
//! never a fallback.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::Write;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::ExitCode;

use rustix::io::FdFlags;
use serde_json::json;

use super::DirectoryError;
use crate::isolation::{ENVIRONMENT_DESCRIPTOR, PASSED_DESCRIPTOR};

/// The entry's kind, after [`super::ENTRY`].
pub const KIND: &str = "exec";

/// The message of every failure to start other than a directory's.
const NOT_STARTED: &str = "process could not start";

/// The arguments of `tetral`, after its own path, that run `cmd` with
/// `shell` in `directory`.
pub fn arguments(directory: &Path, shell: &str, cmd: &str) -> Vec<OsString> {
    vec![
        super::ENTRY.into(),
        KIND.into(),
        directory.into(),
        shell.into(),
        "-c".into(),
        cmd.into(),
    ]
}

pub(super) fn run(arguments: &[OsString]) -> ExitCode {
    let [directory, program, command_arguments @ ..] = arguments else {
        return ExitCode::FAILURE;
    };
    // SAFETY: the launch places the launch-status pipe at this descriptor
    // for this entry alone, and nothing else in the process holds it.
    let status = unsafe { OwnedFd::from_raw_fd(PASSED_DESCRIPTOR) };
    let failure = launch(&status, Path::new(directory), program, command_arguments);
    let mut status = File::from(status);
    let _ = status.write_all(failure.to_string().as_bytes());
    ExitCode::FAILURE
}

/// Enters `directory` and executes the shell; returns only the failure.
fn launch(
    status: &OwnedFd,
    directory: &Path,
    program: &OsStr,
    arguments: &[OsString],
) -> serde_json::Value {
    // SAFETY: isolation passes this entry its own environment carrier at
    // descriptor 4. Reading consumes it, and the file closes before exec.
    let environment_file = unsafe { File::from_raw_fd(ENVIRONMENT_DESCRIPTOR) };
    let environment =
        serde_json::from_reader::<_, std::collections::HashMap<String, String>>(environment_file);
    let Ok(environment) = environment else {
        return not_started();
    };
    let directory_failure = |error: DirectoryError| match error {
        DirectoryError::Other => not_started(),
        error => serde_json::to_value(error.tool_error(directory)).unwrap_or_default(),
    };
    if let Err(error) = super::check_directory(directory) {
        return directory_failure(error);
    }
    state::seams::barrier_blocking("task.launch");
    if let Err(error) = super::enter_directory(directory) {
        return directory_failure(error);
    }
    // The PID namespace's first process passes the signals it receives on
    // to this process's group, which then holds the shell and every process
    // it leaves behind.
    #[cfg(target_os = "linux")]
    if rustix::process::setpgid(None, None).is_err() {
        return not_started();
    }
    if rustix::io::fcntl_setfd(status, FdFlags::CLOEXEC).is_err() {
        return not_started();
    }
    let mut command = std::process::Command::new(program);
    command.args(arguments).env_clear().envs(environment);
    if let Some((name, _)) = state::seams::harness_variable() {
        command.env_remove(name);
    }
    if rustix::io::write(status, super::STARTED).is_err() {
        return not_started();
    }
    let _ = command.exec();
    not_started()
}

fn not_started() -> serde_json::Value {
    json!({"kind": "exec_spawn_failed", "message": NOT_STARTED, "path": null})
}
