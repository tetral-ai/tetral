//! macOS: the availability check of `sandbox-exec`, the profile that a
//! launch hands to the Codex crates, and the rules appended to the Seatbelt
//! profile that Codex's sandbox manager generates. A file rule does not stop
//! a `connect` to a Unix socket, so the appended rules keep every command
//! from connecting below the state root, the socket directory and the task
//! directories, and from the App Server's NFS server, which serves every
//! Session's drives to whoever connects; and under `direct` let a tool's
//! commands bind and connect below their own temporary directory, which lies
//! inside the task directories. They come last, so they decide over every
//! grant before them.

use std::io;
use std::path::{Path, PathBuf};

use codex_protocol::models::{ManagedFileSystemPermissions, PermissionProfile};
use codex_protocol::permissions::{FileSystemAccessMode, FileSystemPath, FileSystemSandboxEntry};

use super::probe::Probed;
use super::{Isolation, Refusal};
use crate::policy::TaskPermissions;

/// The `sandbox-exec` that Codex's launch runs.
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// The profile of the check, which allows everything: only a `sandbox-exec`
/// that cannot apply a profile fails it.
const PERMISSIVE_PROFILE: &str = "(version 1)(allow default)";

/// The test build's replacement for the `sandbox-exec` that the check runs.
#[cfg(feature = "test-seams")]
pub const TEST_SANDBOX_EXEC_VARIABLE: &str = "TETRAL_TEST_SANDBOX_EXEC";

/// Runs `sandbox-exec` with the permissive profile on `/usr/bin/true`,
/// which must exit 0 within the probe timeout.
pub(super) async fn check(isolation: &Isolation) -> Result<(), Refusal> {
    let program = checked_program();
    let probed = isolation
        .probe(&program, &["-p", PERMISSIVE_PROFILE, "/usr/bin/true"])
        .await;
    judge(probed, isolation.probe_timeout)
}

fn judge(probed: Probed, timeout: std::time::Duration) -> Result<(), Refusal> {
    match probed {
        Probed::Exited(output) if output.success => Ok(()),
        Probed::Exited(_) => Err(Refusal::Unavailable(
            "sandbox-exec cannot apply a sandbox profile on this machine".into(),
        )),
        Probed::TimedOut => Err(Refusal::Unavailable(
            format!(
                "sandbox-exec did not apply a sandbox profile within {} milliseconds",
                timeout.as_millis()
            )
            .into(),
        )),
        Probed::Failed(error) => match shortage(&error) {
            Some(resource) => Err(Refusal::Exhausted(format!(
                "The sandbox could not start: {resource}. The command did not run."
            ))),
            None => Err(Refusal::Unavailable(
                format!("sandbox-exec could not start: {error}").into(),
            )),
        },
    }
}

/// The system resource a start failed for want of, with its errno, which a
/// later call may find free; `None` for any other failure.
fn shortage(error: &io::Error) -> Option<&'static str> {
    match error.raw_os_error()? {
        libc::EMFILE => Some("too many open files (EMFILE)"),
        libc::ENFILE => Some("the system's file table is full (ENFILE)"),
        libc::EAGAIN => Some("too many processes (EAGAIN)"),
        libc::ENOMEM => Some("not enough memory (ENOMEM)"),
        _ => None,
    }
}

fn checked_program() -> PathBuf {
    #[cfg(feature = "test-seams")]
    if let Some(program) = std::env::var_os(TEST_SANDBOX_EXEC_VARIABLE) {
        return PathBuf::from(program);
    }
    PathBuf::from(SANDBOX_EXEC)
}

/// `profile` without the writable entries that may skip a missing path and
/// whose path cannot be inspected for another reason, such as one below a
/// regular file or below a directory this user cannot search. Codex's
/// Seatbelt profile tolerates only a missing path there and otherwise fails
/// the whole launch; left out, such a path keeps Codex's own protection.
pub(super) fn launch_profile(profile: &PermissionProfile) -> PermissionProfile {
    let PermissionProfile::Managed {
        file_system:
            ManagedFileSystemPermissions::Restricted {
                entries,
                glob_scan_max_depth,
            },
        network,
    } = profile
    else {
        return profile.clone();
    };
    let uninspectable = |entry: &FileSystemSandboxEntry| {
        let FileSystemPath::Path { path } = &entry.path else {
            return false;
        };
        entry.access == FileSystemAccessMode::Write
            && entry.skips_missing_path()
            && path
                .to_abs_path()
                .ok()
                .map(PathBuf::from)
                .is_some_and(|path| {
                    std::fs::symlink_metadata(path)
                        .is_err_and(|error| error.kind() != io::ErrorKind::NotFound)
                })
    };
    PermissionProfile::Managed {
        file_system: ManagedFileSystemPermissions::Restricted {
            entries: entries
                .iter()
                .filter(|entry| !uninspectable(entry))
                .cloned()
                .collect(),
            glob_scan_max_depth: *glob_scan_max_depth,
        },
        network: *network,
    }
}

/// Appends the connection rules of `permissions` to the profile of
/// `command`, `sandbox-exec -p <profile> ...` as Codex's transform builds it.
pub(super) fn append_socket_rules(
    command: &mut [String],
    permissions: &TaskPermissions,
) -> io::Result<()> {
    let [program, flag, profile, ..] = command else {
        return Err(io::Error::other(
            "the sandboxed command is not sandbox-exec with a profile",
        ));
    };
    if program != SANDBOX_EXEC || flag != "-p" {
        return Err(io::Error::other(
            "the sandboxed command is not sandbox-exec with a profile",
        ));
    }
    let sockets = permissions.unix_sockets();
    for directory in &sockets.denied {
        profile.push_str(&format!(
            "\n(deny network-outbound (remote unix-socket (subpath {})))",
            quoted(directory)?
        ));
    }
    if let Some(port) = permissions.drive_server_port() {
        profile.push_str(&format!(
            "\n(deny network-outbound (remote tcp \"localhost:{port}\"))"
        ));
    }
    if permissions.profile().network_sandbox_policy().is_enabled()
        && let Some(own) = &sockets.own
    {
        let own = quoted(own)?;
        profile.push_str(&format!(
            "\n(allow network-outbound (remote unix-socket (subpath {own})))\n(allow network-bind (local unix-socket (subpath {own})))"
        ));
    }
    Ok(())
}

/// `path` as a profile string, quoted and escaped as Codex quotes the paths
/// of its own socket rules.
fn quoted(path: &Path) -> io::Result<String> {
    let text = path
        .to_str()
        .ok_or_else(|| io::Error::other("a Unix-socket rule's path is not UTF-8"))?;
    serde_json::to_string(text).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;
    use std::time::Duration;

    use protocol::tool::RejectionCode;

    use super::judge;
    use crate::isolation::Isolation;
    use crate::isolation::probe::Probed;

    const TIMEOUT: Duration = Duration::from_millis(200);

    async fn probed(program: &str, args: &[&str]) -> Probed {
        Isolation::with_probe_bounds(TIMEOUT, 4_096)
            .probe(Path::new(program), args)
            .await
    }

    #[tokio::test]
    async fn each_way_the_probe_ends_decides_the_calls_rejection() {
        let unavailable = |message: String| Err((RejectionCode::SandboxUnavailable, message));
        let mut rows = vec![
            (
                "an exit 0",
                probed("/bin/sh", &["-c", "exit 0"]).await,
                Ok(()),
            ),
            (
                "an exit 71",
                probed("/bin/sh", &["-c", "exit 71"]).await,
                unavailable("sandbox-exec cannot apply a sandbox profile on this machine".into()),
            ),
            (
                "no exit within the timeout",
                probed("/bin/sh", &["-c", "sleep 5"]).await,
                unavailable(
                    "sandbox-exec did not apply a sandbox profile within 200 milliseconds".into(),
                ),
            ),
            (
                "a program that does not exist",
                probed("/nonexistent/sandbox-exec", &[]).await,
                unavailable(format!(
                    "sandbox-exec could not start: {}",
                    io::Error::from_raw_os_error(libc::ENOENT)
                )),
            ),
        ];
        for (name, code, resource) in [
            (
                "too many open files",
                libc::EMFILE,
                "too many open files (EMFILE)",
            ),
            (
                "a full file table",
                libc::ENFILE,
                "the system's file table is full (ENFILE)",
            ),
            (
                "no process to spare",
                libc::EAGAIN,
                "too many processes (EAGAIN)",
            ),
            ("no memory", libc::ENOMEM, "not enough memory (ENOMEM)"),
        ] {
            rows.push((
                name,
                Probed::Failed(io::Error::from_raw_os_error(code)),
                Err((
                    RejectionCode::ResourceExhausted,
                    format!("The sandbox could not start: {resource}. The command did not run."),
                )),
            ));
        }
        let wrong: Vec<String> = rows
            .into_iter()
            .filter_map(|(name, probed, expected)| {
                let judged = judge(probed, TIMEOUT)
                    .map_err(|refusal| refusal.rejection())
                    .map_err(|rejection| (rejection.code, rejection.message));
                (judged != expected).then(|| format!("{name}: {judged:?}, not {expected:?}"))
            })
            .collect();
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}
