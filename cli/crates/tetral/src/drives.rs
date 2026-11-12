//! `tetral drives enable`: it runs only while no App Server owns the root.
//! On Linux, where drives need no consent, it then succeeds with `{}` and
//! changes nothing. On a Mac it mounts a test drive from its own NFS server
//! and has a child of its own executable, run as `tetral-drives-check`, read
//! the drive's one file, so macOS asks for network-volume access naming the
//! app that launched the command, and records the user's consent as
//! `drives_enabled = true`; every failure records nothing.

use appserver::lock::{self, LockError};
use serde_json::{Value, json};
use state::paths::{self, StateRoot};

use crate::commands::io_failed;
use crate::output::{Code, CommandError};

pub fn enable(root: &StateRoot) -> Result<Value, CommandError> {
    #[cfg(target_os = "macos")]
    macos::recorded(root)?;
    #[cfg(target_os = "macos")]
    state::files::create_private_dirs(&root.join(paths::RUN)).map_err(io_failed)?;
    // A root without `run/` has no App Server and no lock to take.
    let _lock = if root.join(paths::RUN).is_dir() {
        Some(lock::acquire(root).map_err(|error| match error {
            LockError::Held => CommandError::new(
                Code::ServerRunning,
                "an App Server or another command owns the state root",
            ),
            LockError::Io(failure) => io_failed(failure),
        })?)
    } else {
        None
    };
    #[cfg(target_os = "macos")]
    macos::enable(root)?;
    Ok(json!({}))
}

#[cfg(target_os = "macos")]
pub use macos::{check, is_check};

#[cfg(target_os = "macos")]
mod macos {
    use std::ffi::{OsStr, OsString};
    use std::io::{self, Write};
    use std::path::Path;
    use std::process::{ExitCode, Stdio};

    use drive::mount_macos::{force_unmount, listed, mount_nfs};
    use drive::nfs::TestDrive;
    use state::config::{self, Config};
    use state::paths::{self, StateRoot};
    use tokio::io::AsyncReadExt;
    use tokio::signal::unix::{SignalKind, signal};

    use crate::commands::{config_failed, io_failed};
    use crate::output::{Code, CommandError};

    /// The configuration the consent is recorded into; a root without one
    /// fails before anything is mounted.
    pub(super) fn recorded(root: &StateRoot) -> Result<Config, CommandError> {
        config::read(root).map_err(config_failed)?.ok_or_else(|| {
            CommandError::new(
                Code::InvalidConfiguration,
                format!("{} is missing", root.join(paths::CONFIG).display()),
            )
        })
    }

    fn mount_failed(message: String) -> CommandError {
        CommandError::new(Code::DrivesMountFailed, message)
    }

    /// The `argv[0]` under which `tetral` runs as the test read.
    const CHECK_NAME: &str = "tetral-drives-check";

    /// `argv[0]` names the test read.
    pub fn is_check(argv0: &OsStr) -> bool {
        Path::new(argv0).file_name() == Some(OsStr::new(CHECK_NAME))
    }

    /// The test read: reads the file `arguments[0]` and exits 0, or exits 1
    /// with the read's OS error number on stdout.
    pub fn check(arguments: &[OsString]) -> ExitCode {
        let Some(file) = arguments.first() else {
            return ExitCode::FAILURE;
        };
        match std::fs::read(file) {
            Ok(_) => ExitCode::SUCCESS,
            Err(error) => {
                let _ = write!(io::stdout(), "{}", error.raw_os_error().unwrap_or(0));
                ExitCode::FAILURE
            }
        }
    }

    /// The test mount, read by the test read with no time limit, until
    /// `SIGINT` or `SIGTERM`.
    pub(super) fn enable(root: &StateRoot) -> Result<(), CommandError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
        runtime.block_on(async {
            // Taken first, so a signal at any later moment ends the
            // command through `cancelled` with the test mount removed.
            let signals = signal(SignalKind::interrupt())
                .and_then(|interrupt| Ok((interrupt, signal(SignalKind::terminate())?)));
            let (mut interrupt, mut terminate) =
                signals.map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
            let canonical = std::fs::canonicalize(root.path())
                .map_err(|error| io_failed(state::files::IoFailure::new(root.path(), error)))?;
            let point = canonical.join(paths::RUN).join("drives-enable");
            let server =
                TestDrive::start().map_err(|error| mount_failed(format!("NFS server: {error}")))?;
            prepare(&point)?;
            mount_nfs(server.port(), &server.source(), &point, false).map_err(mount_failed)?;
            let file = point.join("check.txt");
            let spawned = std::env::current_exe().and_then(|program| {
                tokio::process::Command::new(program)
                    .arg0(CHECK_NAME)
                    .arg(&file)
                    .current_dir("/")
                    .env_clear()
                    .process_group(0)
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .spawn()
            });
            let mut child = match spawned {
                Ok(child) => child,
                Err(error) => {
                    let _ = force_unmount(&point);
                    return Err(io_failed(state::files::IoFailure::new(&file, error)));
                }
            };
            let exited = tokio::select! {
                biased;
                _ = interrupt.recv() => None,
                _ = terminate.recv() => None,
                exited = child.wait() => Some(exited),
            };
            let recorded = match exited {
                None => {
                    // While macOS asks, the read's open waits in the kernel
                    // until a fatal signal, and `umount -f` waits for it.
                    let _ = child.start_kill();
                    Err(CommandError::new(
                        Code::Cancelled,
                        "drives enable was cancelled while macOS asked for network-volume access",
                    ))
                }
                Some(Ok(status)) if status.success() => record(root),
                Some(Ok(status)) => {
                    let mut output = String::new();
                    if let Some(mut stdout) = child.stdout.take() {
                        let _ = stdout.read_to_string(&mut output).await;
                    }
                    Err(read_failed(&file, &output, status))
                }
                Some(Err(error)) => Err(io_failed(state::files::IoFailure::new(&file, error))),
            };
            let _ = force_unmount(&point);
            let _ = child.wait().await;
            recorded
        })
    }

    /// The failure of a test read that ended with `ended`, its OS error
    /// number on `output`: access withheld when the read was refused, by
    /// the test drive's server or by macOS.
    fn read_failed(file: &Path, output: &str, ended: impl std::fmt::Display) -> CommandError {
        let error = match output.parse::<i32>() {
            Ok(code) if code != 0 => io::Error::from_raw_os_error(code),
            _ => io::Error::other(format!("the test read ended with {ended}")),
        };
        let failure = state::files::IoFailure::new(file, error);
        // `EPERM` and `EACCES` alike.
        if failure.source.kind() == io::ErrorKind::PermissionDenied {
            CommandError::new(Code::DrivesAccessDenied, failure.to_string())
        } else {
            io_failed(failure)
        }
    }

    /// The mount point `R/run/drives-enable` with mode `0700`, after a
    /// stale mount there is force-unmounted without touching the path.
    fn prepare(point: &Path) -> Result<(), CommandError> {
        if listed(point)
            .map_err(|error| mount_failed(format!("mount: {error}")))?
            .is_some()
        {
            force_unmount(point)
                .map_err(|error| mount_failed(format!("force unmount: {error}")))?;
        }
        state::files::create_private_dirs(point)
            .map_err(|failure| mount_failed(format!("mount point: {}", failure.source)))
    }

    /// Writes `drives_enabled = true` into the configuration as it stands
    /// under the lock.
    fn record(root: &StateRoot) -> Result<(), CommandError> {
        let mut config = recorded(root)?;
        config.drives_enabled = true;
        let encoded = config
            .encode()
            .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
        encoded.replace(root).map_err(io_failed)
    }

    #[cfg(test)]
    mod tests {
        use std::path::Path;

        use super::read_failed;
        use crate::output::Code;

        #[test]
        fn a_test_read_refused_by_the_server_or_by_macos_is_access_withheld() {
            for (output, code) in [
                ("1", Code::DrivesAccessDenied),  // EPERM
                ("13", Code::DrivesAccessDenied), // EACCES
                ("5", Code::IoFailed),            // EIO
                ("", Code::IoFailed),
            ] {
                let failed = read_failed(Path::new("/check.txt"), output, "exit status: 1");
                assert_eq!(failed.code, code, "{output:?}: {}", failed.message);
            }
        }
    }
}
