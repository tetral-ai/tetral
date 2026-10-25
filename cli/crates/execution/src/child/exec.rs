//! `exec_command`'s launcher entry, `__tetral-task exec <directory> <shell>
//! <cmd>`, run inside the sandbox with the launch-status pipe at
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

use rustix::io::{Errno, FdFlags};
use serde_json::json;

use crate::isolation::PASSED_DESCRIPTOR;

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
        cmd.into(),
    ]
}

pub(super) fn run(arguments: &[OsString]) -> ExitCode {
    let [directory, shell, cmd] = arguments else {
        return ExitCode::FAILURE;
    };
    // SAFETY: the launch places the launch-status pipe at this descriptor
    // for this entry alone, and nothing else in the process holds it.
    let status = unsafe { OwnedFd::from_raw_fd(PASSED_DESCRIPTOR) };
    let failure = launch(&status, Path::new(directory), shell, cmd);
    let mut status = File::from(status);
    let _ = status.write_all(failure.to_string().as_bytes());
    ExitCode::FAILURE
}

/// Enters `directory` and executes the shell; returns only the failure.
fn launch(status: &OwnedFd, directory: &Path, shell: &OsStr, cmd: &OsStr) -> serde_json::Value {
    let directory_failure = |errno: Errno| match errno {
        Errno::NOENT | Errno::NOTDIR => failure(
            "cwd_not_found",
            "working directory does not exist",
            Some(directory),
        ),
        Errno::ACCESS | Errno::PERM => failure(
            "cwd_inaccessible",
            "working directory is not accessible",
            Some(directory),
        ),
        _ => failure("exec_spawn_failed", NOT_STARTED, None),
    };
    match rustix::fs::stat(directory) {
        Err(errno) => return directory_failure(errno),
        Ok(stat) if !rustix::fs::FileType::from_raw_mode(stat.st_mode).is_dir() => {
            return failure(
                "cwd_not_directory",
                "working directory is not a directory",
                Some(directory),
            );
        }
        Ok(_) => {}
    }
    state::seams::barrier_blocking("task.launch");
    if let Err(errno) = rustix::process::chdir(directory) {
        return directory_failure(errno);
    }
    // The PID namespace's first process passes the signals it receives on
    // to this process's group, which then holds the shell and every process
    // it leaves behind.
    #[cfg(target_os = "linux")]
    if rustix::process::setpgid(None, None).is_err() {
        return failure("exec_spawn_failed", NOT_STARTED, None);
    }
    if rustix::io::fcntl_setfd(status, FdFlags::CLOEXEC).is_err() {
        return failure("exec_spawn_failed", NOT_STARTED, None);
    }
    let mut command = std::process::Command::new(shell);
    command.arg("-c").arg(cmd);
    if let Some((name, _)) = state::seams::harness_variable() {
        command.env_remove(name);
    }
    if rustix::io::write(status, super::STARTED).is_err() {
        return failure("exec_spawn_failed", NOT_STARTED, None);
    }
    let _ = command.exec();
    failure("exec_spawn_failed", NOT_STARTED, None)
}

fn failure(kind: &str, message: &str, path: Option<&Path>) -> serde_json::Value {
    json!({"kind": kind, "message": message, "path": path.map(|path| path.display().to_string())})
}
