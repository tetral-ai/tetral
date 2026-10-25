//! Linux: the `bwrap` that a launch uses, chosen as Codex's helper chooses
//! it, Codex's check that it can create the namespaces a sandbox needs, and
//! the profile that a launch hands to the Codex crates.

use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use codex_protocol::models::{ManagedFileSystemPermissions, PermissionProfile};
use codex_protocol::permissions::{
    FileSystemAccessMode, FileSystemPath, FileSystemSandboxEntry, FileSystemSandboxPolicy,
};
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::Isolation;
use crate::policy::TaskPermissions;

/// The digest of the bundled `bwrap`, built into release builds. A build
/// without it accepts the bundled `bwrap` unchecked, as Codex does.
const BUNDLED_BWRAP_SHA256: Option<&str> = option_env!("CODEX_BWRAP_SHA256");

/// The stderr texts with which Codex recognizes a host that refuses the user
/// namespaces `bwrap` needs.
const USER_NAMESPACE_FAILURES: [&str; 4] = [
    "loopback: Failed RTM_NEWADDR",
    "loopback: Failed RTM_NEWLINK",
    "setting up uid map: Permission denied",
    "No permissions to create a new namespace",
];

pub(super) async fn check(
    isolation: &Isolation,
    permissions: &TaskPermissions,
    cwd: &Path,
    tetral: &Path,
) -> Result<(), &'static str> {
    let file_system = permissions.profile().file_system_sandbox_policy();
    let Some(bwrap) = select(isolation, &file_system, cwd, tetral).await else {
        return Err(
            "no bwrap is available: none on PATH passes the capability probe, and none beside tetral matches its digest",
        );
    };
    if is_wsl1() {
        return Err("WSL1 cannot create the namespaces that bwrap needs");
    }
    let probe = isolation
        .probe(
            &bwrap,
            &[
                "--unshare-user",
                "--unshare-net",
                "--ro-bind",
                "/",
                "/",
                "/bin/true",
            ],
        )
        .await;
    // As in Codex, a probe that cannot start or does not finish in time
    // counts as available; only a refusal of the namespaces does not.
    match probe {
        Some(output)
            if !output.success
                && USER_NAMESPACE_FAILURES
                    .iter()
                    .any(|failure| output.stderr.contains(failure)) =>
        {
            Err("bwrap cannot create user namespaces on this machine")
        }
        _ => Ok(()),
    }
}

/// Every `bwrap` that `select` could pick for a call: each existing `bwrap`
/// file in a directory of `PATH`, a leading `~` expanded as Codex's lookup
/// expands it, and the bundled one, each canonical. The profile is built
/// before the selection, which skips a system `bwrap` that the profile would
/// leave writable; keeping every candidate read-only keeps whichever one it
/// picks out of the call's reach.
pub(super) fn bwrap_candidates(tetral: &Path, cwd: &Path) -> Vec<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let directories = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    let system = directories.into_iter().filter_map(|directory| {
        let directory = match (directory.strip_prefix("~"), &home) {
            (Ok(rest), Some(home)) => home.join(rest),
            _ => directory,
        };
        let candidate = directory.join("bwrap");
        if !std::fs::metadata(&candidate).ok()?.is_file() {
            return None;
        }
        candidate.canonicalize().ok()
    });
    let bundled =
        codex_linux_sandbox::find_bundled_bwrap_for_exe(tetral, cwd, |name| std::env::var_os(name))
            .and_then(|bundled| bundled.into_path_buf().canonicalize().ok());
    system.chain(bundled).collect()
}

/// The system `bwrap` when one on `PATH` passes the capability probe,
/// otherwise the bundled one beside `tetral` when its digest matches.
async fn select(
    isolation: &Isolation,
    file_system: &FileSystemSandboxPolicy,
    cwd: &Path,
    tetral: &Path,
) -> Option<PathBuf> {
    // Skips candidates inside the App Server's working directory and those
    // the call's permissions could replace.
    if let Some(system) = codex_sandboxing::find_system_bwrap_in_path(file_system, cwd)
        && let Some(help) = isolation.probe(&system, &["--help"]).await
        && help.lists("--as-pid-1")
        && help.lists("--perms")
    {
        return Some(system);
    }
    let bundled = codex_linux_sandbox::find_bundled_bwrap_for_exe(tetral, cwd, |name| {
        std::env::var_os(name)
    })?
    .into_path_buf();
    digest_matches(&bundled).then_some(bundled)
}

/// The profile that a launch hands to the Codex crates. Their `bwrap`
/// arguments protect each missing `.git` at the top of a writable path, and
/// each missing `read` or `deny` path inside one, by first creating an empty
/// entry there as a mount target. In a directory that belongs to another user
/// and grants this user no write, or on a read-only filesystem, `bwrap`
/// cannot create it, and the operating system already refuses its creation:
/// there the profile leaves the protection to that refusal, with write for
/// such a `.git`, so that nothing is mounted at it, and without such a
/// `read` or `deny` entry, as `Skip` would leave it out (the Codex crates
/// apply `Skip` on Windows only).
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
    let path_of = |entry: &FileSystemSandboxEntry| match &entry.path {
        FileSystemPath::Path { path } => path.to_abs_path().ok().map(PathBuf::from),
        _ => None,
    };
    let writable: Vec<PathBuf> = entries
        .iter()
        .filter(|entry| entry.access == FileSystemAccessMode::Write)
        .filter_map(path_of)
        .collect();
    let left_to_the_system = |entry: &FileSystemSandboxEntry| {
        entry.access != FileSystemAccessMode::Write
            && path_of(entry)
                .and_then(|path| first_missing_component(&path))
                .is_some_and(|missing| {
                    writable.iter().any(|root| missing.starts_with(root))
                        && missing.parent().is_some_and(refuses_new_entries)
                })
    };
    let mut adjusted: Vec<FileSystemSandboxEntry> = entries
        .iter()
        .filter(|entry| !left_to_the_system(entry))
        .cloned()
        .collect();
    for root in &writable {
        let git = root.join(".git");
        // Judged after the left-out entries, which no longer mount anything.
        let has_entry = adjusted
            .iter()
            .any(|entry| path_of(entry).as_deref() == Some(git.as_path()));
        if !has_entry
            && root.is_dir()
            && std::fs::symlink_metadata(&git).is_err()
            && refuses_new_entries(root)
            && let Ok(git) = AbsolutePathBuf::from_absolute_path(&git)
        {
            adjusted.push(FileSystemSandboxEntry::new(
                FileSystemPath::Path {
                    path: PathUri::from_abs_path(&git),
                },
                FileSystemAccessMode::Write,
            ));
        }
    }
    PermissionProfile::Managed {
        file_system: ManagedFileSystemPermissions::Restricted {
            entries: adjusted,
            glob_scan_max_depth: *glob_scan_max_depth,
        },
        network: *network,
    }
}

/// The first component of `path` that does not exist, as the Codex crates
/// find it when they place a mount target.
fn first_missing_component(path: &Path) -> Option<PathBuf> {
    path.ancestors()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .find(|ancestor| !ancestor.exists())
        .map(Path::to_path_buf)
}

/// Whether no process of this user can create an entry in `directory`:
/// `access(2)` with `W_OK` fails, with `EROFS` or for a directory that another
/// user owns. A directory of this user's own keeps the Codex default even
/// without write, since a command could grant itself the write.
fn refuses_new_entries(directory: &Path) -> bool {
    match rustix::fs::access(directory, rustix::fs::Access::WRITE_OK) {
        Ok(()) => false,
        Err(rustix::io::Errno::ROFS) => true,
        Err(_) => std::fs::metadata(directory)
            .is_ok_and(|metadata| metadata.uid() != rustix::process::getuid().as_raw()),
    }
}

fn digest_matches(bwrap: &Path) -> bool {
    let Some(expected) = BUNDLED_BWRAP_SHA256 else {
        return true;
    };
    let expected = expected.to_ascii_lowercase();
    // Codex reads an all-zero digest as no digest.
    if expected.bytes().all(|digit| digit == b'0') {
        return true;
    }
    let Ok(mut file) = File::open(bwrap) else {
        return false;
    };
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(_) => return false,
        }
    }
    let actual: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    actual == expected
}

fn is_wsl1() -> bool {
    std::fs::read_to_string("/proc/version").is_ok_and(|version| proc_version_is_wsl1(&version))
}

/// Codex's reading of `/proc/version`: a `wsl<digits>` marker names the
/// version, and otherwise a Microsoft kernel that is not WSL2's standard
/// one is WSL1.
fn proc_version_is_wsl1(version: &str) -> bool {
    let version = version.to_ascii_lowercase();
    let mut remaining = version.as_str();
    while let Some(marker) = remaining.find("wsl") {
        let digits: String = remaining[marker + 3..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if let Ok(number) = digits.parse::<u32>() {
            return number == 1;
        }
        remaining = &remaining[marker + 3..];
    }
    version.contains("microsoft") && !version.contains("microsoft-standard")
}

struct ProbeOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

impl ProbeOutput {
    fn lists(&self, option: &str) -> bool {
        self.stdout.contains(option) || self.stderr.contains(option)
    }
}

impl Isolation {
    /// Runs `program` with an empty environment, reading at most the output
    /// limit of each stream; `None` when it cannot start or does not exit
    /// within the probe timeout, after which it is killed.
    async fn probe(&self, program: &Path, args: &[&str]) -> Option<ProbeOutput> {
        let mut child = tokio::process::Command::new(program)
            .args(args)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let stdout = child.stdout.take()?;
        let stderr = child.stderr.take()?;
        let limit = self.probe_output_limit;
        let finished = async {
            let (stdout, stderr) =
                tokio::join!(read_bounded(stdout, limit), read_bounded(stderr, limit));
            let status = child.wait().await.ok()?;
            Some(ProbeOutput {
                success: status.success(),
                stdout,
                stderr,
            })
        };
        let deadline = state::seams::now() + self.probe_timeout;
        tokio::select! {
            output = finished => output,
            () = state::seams::sleep_until(deadline) => None,
        }
    }
}

async fn read_bounded(stream: impl AsyncRead + Unpin, limit: usize) -> String {
    let mut bytes = Vec::new();
    let _ = stream
        .take(u64::try_from(limit).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .await;
    String::from_utf8_lossy(&bytes).into_owned()
}
