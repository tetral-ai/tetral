//! Linux: the `bwrap` that a launch uses, chosen as Codex's helper chooses
//! it, Codex's check that it can create the namespaces a sandbox needs, and
//! the profile that a launch hands to the Codex crates.

use std::borrow::Cow;
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

/// The program name that Codex's lookup searches `PATH` for.
const BWRAP: &str = "bwrap";

/// The sticky bit of a directory's mode.
const STICKY: u32 = 0o1000;

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
) -> Result<(), Cow<'static, str>> {
    if let Some(planted) = planted_launcher() {
        return Err(planted.into());
    }
    let file_system = permissions.profile().file_system_sandbox_policy();
    let Some(bwrap) = select(isolation, &file_system, cwd, tetral).await else {
        return Err(
            "no bwrap is available: none on PATH passes the capability probe, and none beside tetral matches its digest".into(),
        );
    };
    if is_wsl1() {
        return Err("WSL1 cannot create the namespaces that bwrap needs".into());
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
            Err("bwrap cannot create user namespaces on this machine".into())
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

/// The message that refuses every `bwrap`, when the first `bwrap` on this
/// process's `PATH` that Codex's lookup could choose is one that this user
/// can modify, or replace through any directory above it, ahead of every one
/// that this user can neither modify nor replace. Codex's lookup skips such a
/// `bwrap` only when the call's own profile could write it, so one that an
/// earlier call with a broader grant left on `PATH` would run, as the
/// launcher, outside any sandbox; and since a task runs as this user, no
/// `bwrap` this user cannot change was planted by one. A later `bwrap` that
/// this user can change is never chosen and is ignored. The walk resolves
/// candidates as Codex's lookup does and judges each as Codex's
/// `current_user_can_modify` and `current_user_can_replace_child` do, whatever
/// the profile; the availability check runs it before any `bwrap`, and
/// `tetral` as `codex-linux-sandbox` again before the launch.
pub(super) fn planted_launcher() -> Option<String> {
    let search_path = std::env::var_os("PATH")?;
    let cwd = std::env::current_dir().ok()?;
    let cwd = cwd.canonicalize().unwrap_or(cwd);
    let cwd_is_root = cwd.parent().is_none();
    for directory in std::env::split_paths(&search_path) {
        let binary = if directory.starts_with("~") {
            PathBuf::from(BWRAP)
        } else {
            cwd.join(&directory).join(BWRAP)
        };
        let Ok(found) = which::which_in(binary, Some(&directory), &cwd) else {
            continue;
        };
        if !found.is_absolute() {
            continue;
        }
        let (Some(name), Some(Ok(parent))) =
            (found.file_name(), found.parent().map(std::fs::canonicalize))
        else {
            continue;
        };
        let alias = parent.join(name);
        let Ok(canonical) = found.canonicalize() else {
            continue;
        };
        if !cwd_is_root && (alias.starts_with(&cwd) || canonical.starts_with(&cwd)) {
            continue;
        }
        let changeable = [&found, &alias, &canonical]
            .into_iter()
            .any(|path| user_can_modify(path) || user_can_replace(path));
        return changeable.then(|| {
            format!(
                "{} on PATH is a bwrap that this user can modify or replace, so no bwrap is run; remove it, or put a bwrap that this user cannot change ahead of it on PATH",
                found.display()
            )
        });
    }
    None
}

/// Whether this user can write `path` or make it writable: its owner, one
/// that the kernel grants write with effective credentials and ACLs, or one
/// whose check fails for any reason but a denial.
fn user_can_modify(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return true;
    };
    if metadata.uid() == rustix::process::geteuid().as_raw() {
        return true;
    }
    match rustix::fs::accessat(
        rustix::fs::CWD,
        path,
        rustix::fs::Access::WRITE_OK,
        rustix::fs::AtFlags::EACCESS,
    ) {
        Ok(()) => true,
        Err(rustix::io::Errno::ACCESS | rustix::io::Errno::ROFS) => false,
        Err(_) => true,
    }
}

/// Whether this user can rename or remove `path` or any directory above it.
fn user_can_replace(path: &Path) -> bool {
    path.ancestors().any(|component| {
        let (Some(parent), Some(name)) = (component.parent(), component.file_name()) else {
            return false;
        };
        let Ok(parent) = parent.canonicalize() else {
            return true;
        };
        user_can_replace_child(&parent, &parent.join(name))
    })
}

/// Whether this user can replace `child` in the directory `parent`: it can
/// modify `parent` and, when `parent` is sticky and another user's, owns
/// `child`.
fn user_can_replace_child(parent: &Path, child: &Path) -> bool {
    if !user_can_modify(parent) {
        return false;
    }
    let euid = rustix::process::geteuid();
    if euid.is_root() {
        return true;
    }
    let Ok(metadata) = std::fs::metadata(parent) else {
        return true;
    };
    if metadata.mode() & STICKY == 0 || metadata.uid() == euid.as_raw() {
        return true;
    }
    std::fs::symlink_metadata(child).map_or(true, |metadata| metadata.uid() == euid.as_raw())
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
/// entry there as a mount target. Under an entry that is not a directory, in
/// a directory that belongs to another user and grants this user no write,
/// or on a read-only filesystem, `bwrap` cannot create it, and the operating
/// system already refuses its creation:
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
            && root.exists()
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

/// Whether no process of this user can create an entry in `holder`: it is
/// not a directory, or `access(2)` with `W_OK` fails, with `EROFS` or for a
/// directory that another user owns. A directory of this user's own keeps the
/// Codex default even without write, since a command could grant itself the
/// write.
fn refuses_new_entries(holder: &Path) -> bool {
    if !holder.is_dir() {
        return true;
    }
    match rustix::fs::access(holder, rustix::fs::Access::WRITE_OK) {
        Ok(()) => false,
        Err(rustix::io::Errno::ROFS) => true,
        Err(_) => std::fs::metadata(holder)
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

/// The bytes of one read of `/proc/self/fd`.
const DESCRIPTOR_LIST_READ_BYTES: usize = 4_096;

/// Marks close-on-exec every descriptor from `first` up that `/proc/self/fd`
/// lists. It runs between fork and exec, so it reads the directory with
/// `getdents64` into a buffer on the stack and allocates nothing; a failure
/// leaves the remaining descriptors as they are.
pub(super) fn mark_listed_close_on_exec(first: std::os::fd::RawFd) {
    // SAFETY: the path is NUL-terminated, and open allocates nothing.
    let directory = unsafe {
        libc::open(
            c"/proc/self/fd".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if directory == -1 {
        return;
    }
    let mut buffer = [0_u8; DESCRIPTOR_LIST_READ_BYTES];
    'read: loop {
        // SAFETY: getdents64 writes at most `buffer.len()` bytes into it.
        let count = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                directory,
                buffer.as_mut_ptr(),
                buffer.len(),
            )
        };
        if count == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        let Ok(count) = usize::try_from(count) else {
            break;
        };
        if count == 0 {
            break;
        }
        let mut entries = &buffer[..count];
        // Each entry: an inode and an offset of 8 bytes each, its length in
        // 2 bytes, its type in 1, then its NUL-terminated name.
        while !entries.is_empty() {
            let length = match entries.get(16..18) {
                Some(&[low, high]) => usize::from(u16::from_ne_bytes([low, high])),
                _ => break 'read,
            };
            if length < 20 || length > entries.len() {
                break 'read;
            }
            if let Ok(name) = std::ffi::CStr::from_bytes_until_nul(&entries[19..length])
                && let Ok(name) = name.to_str()
                && let Ok(descriptor) = name.parse::<std::os::fd::RawFd>()
                && descriptor >= first
            {
                // SAFETY: fcntl changes only this descriptor's flags.
                unsafe {
                    let flags = libc::fcntl(descriptor, libc::F_GETFD);
                    if flags != -1 && flags & libc::FD_CLOEXEC == 0 {
                        libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC);
                    }
                }
            }
            entries = &entries[length..];
        }
    }
    // SAFETY: closes the descriptor that this function opened.
    unsafe { libc::close(directory) };
}
