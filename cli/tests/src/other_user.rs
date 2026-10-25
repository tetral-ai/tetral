//! Paths that another user owns: one that root already owns, for the cases
//! that need no privilege, and those that root makes with `sudo -n`. Where
//! `sudo -n` fails, a case that needs it is not run, for the reason given;
//! under GitHub Actions (`GITHUB_ACTIONS=true`), whose runners allow
//! `sudo -n`, the failure fails the test instead, so such a case cannot go
//! unrun there.

use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// A directory that root already owns and the current user may look up,
/// `/root` on Linux and `/var/root` on macOS, to stand as a state root owned
/// by another user; the reason when there is none, such as a test that runs
/// as root, which owns it.
pub fn directory() -> Result<&'static Path, String> {
    let path = Path::new(if cfg!(target_os = "macos") {
        "/var/root"
    } else {
        "/root"
    });
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.is_dir() {
        return Err(format!("{} is not a directory", path.display()));
    }
    if metadata.uid() == rustix::process::geteuid().as_raw() {
        return Err(format!(
            "the test runs as the owner of {}, so it is no other user's",
            path.display()
        ));
    }
    Ok(path)
}

/// Runs `sudo -n` with `args`; the reason when that is not possible here.
pub fn sudo<'a>(args: impl IntoIterator<Item = &'a OsStr>) -> Result<(), String> {
    let args: Vec<&OsStr> = args.into_iter().collect();
    let ran = Command::new("sudo")
        .arg("-n")
        .args(&args)
        .output()
        .map_err(|error| format!("sudo did not start: {error}"));
    let ran = ran.and_then(|output| {
        if output.status.success() {
            return Ok(());
        }
        let command: Vec<_> = args.iter().map(|arg| arg.to_string_lossy()).collect();
        Err(format!(
            "`sudo -n {}` needs passwordless sudo: {}",
            command.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    });
    if let Err(reason) = &ran
        && std::env::var_os("GITHUB_ACTIONS").is_some_and(|value| value == "true")
    {
        panic!("{reason}");
    }
    ran
}

/// A directory of mode `0755` that root owns, directly in `/tmp`, whose
/// sticky bit keeps this user from renaming it, with the files root
/// installs in it; removed with them when dropped.
pub struct RootDirectory {
    path: PathBuf,
}

impl RootDirectory {
    /// `/tmp/<name>-<process ID>`, made by root.
    pub fn new(name: &str) -> Result<RootDirectory, String> {
        let path = PathBuf::from(format!("/tmp/{name}-{}", std::process::id()));
        sudo(
            ["install", "-d", "-m", "0755", "-o", "0", "-g", "0"]
                .map(OsStr::new)
                .into_iter()
                .chain([path.as_os_str()]),
        )?;
        Ok(RootDirectory { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Installs a copy of `source` as `name`, owned by the user and group IDs
    /// `owner`, with mode `0555`: no write bit, so that only its owner can
    /// change it.
    pub fn install(&self, source: &Path, name: &str, owner: (u32, u32)) -> Result<PathBuf, String> {
        let target = self.path.join(name);
        let (user, group) = (owner.0.to_string(), owner.1.to_string());
        sudo(
            ["install", "-m", "0555", "-o", &user, "-g", &group]
                .map(OsStr::new)
                .into_iter()
                .chain([source.as_os_str(), target.as_os_str()]),
        )?;
        Ok(target)
    }
}

impl Drop for RootDirectory {
    fn drop(&mut self) {
        let _ = Command::new("sudo")
            .args(["-n", "rm", "-rf"])
            .arg(&self.path)
            .status();
    }
}
