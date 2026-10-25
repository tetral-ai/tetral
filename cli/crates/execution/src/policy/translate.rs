//! The translation of a valid policy into the one Codex `PermissionProfile`
//! that every call runs under, its rules one to one plus the mandatory
//! protections. It reads no file.

use std::path::{Path, PathBuf};

use codex_protocol::models::{ManagedFileSystemPermissions, PermissionProfile};
use codex_protocol::permissions::{
    FileSystemAccessMode, FileSystemPath, FileSystemSandboxEntry, FileSystemSpecialPath,
    NetworkSandboxPolicy, PROTECTED_METADATA_PATH_NAMES,
};
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use protocol::connection::DriveAccess;
use protocol::tool::{FilesystemAccess, RejectionCode};

use super::PolicyRejection;
use super::validate::{Network, ValidPolicy};

/// What a profile protects besides the policy's rules, all canonical paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslationInputs {
    /// The state root `R`.
    pub state_root: PathBuf,
    /// The control socket's directory `S`.
    pub socket_directory: PathBuf,
    /// The parent `T` of every task's private temporary directory.
    pub task_directories: PathBuf,
    /// This task's private temporary directory, inside `T`.
    pub task_temporary_directory: PathBuf,
    /// The Session's drives, when it binds any.
    pub drives: Option<SessionDrives>,
    /// The running `tetral`, the `bwrap` the isolation uses and the recorded
    /// cua-driver.
    pub trusted_programs: Vec<PathBuf>,
}

/// The Session's drive mounts, `R/sessions/<session_id>/drives/<drive_id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDrives {
    /// `R/sessions/<session_id>/drives`.
    pub directory: PathBuf,
    pub mounts: Vec<DriveMount>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriveMount {
    pub path: PathBuf,
    pub access: DriveAccess,
}

/// The one name that a `read_write` rule keeps read-only at the top of its
/// path; Codex keeps more there (`PROTECTED_METADATA_PATH_NAMES`), and the
/// translation makes the others writable.
const READ_ONLY_METADATA_NAME: &str = ".git";

/// Builds the profile. An error of the Codex crates rejects the call as
/// `unsupported_policy`; no policy is weakened to fit.
pub fn translate(
    policy: &ValidPolicy,
    inputs: &TranslationInputs,
) -> Result<PermissionProfile, PolicyRejection> {
    let protected: Vec<&Path> = [
        inputs.state_root.as_path(),
        inputs.socket_directory.as_path(),
        inputs.task_directories.as_path(),
    ]
    .into_iter()
    .chain(inputs.trusted_programs.iter().map(PathBuf::as_path))
    .collect();
    // A rule at or below a protected path is left out, so that the protection
    // decides there however broad the rule.
    let reachable = |path: &Path| {
        !protected
            .iter()
            .any(|protected| path.starts_with(protected))
    };

    let mut entries = vec![FileSystemSandboxEntry::new(
        FileSystemPath::Special {
            value: FileSystemSpecialPath::Minimal,
        },
        FileSystemAccessMode::Read,
    )];
    for (path, access) in &policy.rules {
        let path = Path::new(path);
        if reachable(path) {
            entries.push(entry(path, mode(*access), false)?);
        }
    }
    for (path, access) in &policy.rules {
        if *access != FilesystemAccess::ReadWrite {
            continue;
        }
        for name in PROTECTED_METADATA_PATH_NAMES
            .iter()
            .filter(|name| **name != READ_ONLY_METADATA_NAME)
        {
            let metadata = Path::new(path).join(name);
            let has_own_rule = metadata
                .to_str()
                .is_some_and(|metadata| policy.rules.contains_key(metadata));
            if !has_own_rule && reachable(&metadata) {
                entries.push(entry(&metadata, FileSystemAccessMode::Write, true)?);
            }
        }
    }

    for path in [
        &inputs.state_root,
        &inputs.socket_directory,
        &inputs.task_directories,
    ] {
        entries.push(entry(path, FileSystemAccessMode::Deny, false)?);
    }
    if let Some(drives) = &inputs.drives {
        entries.push(entry(&drives.directory, FileSystemAccessMode::Read, false)?);
        let writes_allowed = policy
            .rules
            .values()
            .any(|access| *access == FilesystemAccess::ReadWrite);
        for mount in &drives.mounts {
            if writes_allowed && mount.access == DriveAccess::ReadWrite {
                writable(&mut entries, &mount.path)?;
            } else {
                entries.push(entry(&mount.path, FileSystemAccessMode::Read, false)?);
            }
        }
    }
    writable(&mut entries, &inputs.task_temporary_directory)?;
    for program in &inputs.trusted_programs {
        entries.push(entry(program, FileSystemAccessMode::Read, false)?);
    }

    let network = match policy.network {
        Network::Disabled => NetworkSandboxPolicy::Restricted,
        Network::Direct => NetworkSandboxPolicy::Enabled,
    };
    Ok(PermissionProfile::Managed {
        file_system: ManagedFileSystemPermissions::Restricted {
            entries,
            glob_scan_max_depth: None,
        },
        network,
    })
}

/// A writable protection. The `.git` protection belongs to the policy's
/// `read_write` rules, so every name that Codex keeps read-only at its top is
/// writable too.
fn writable(entries: &mut Vec<FileSystemSandboxEntry>, path: &Path) -> Result<(), PolicyRejection> {
    entries.push(entry(path, FileSystemAccessMode::Write, false)?);
    for name in PROTECTED_METADATA_PATH_NAMES {
        entries.push(entry(&path.join(name), FileSystemAccessMode::Write, true)?);
    }
    Ok(())
}

fn mode(access: FilesystemAccess) -> FileSystemAccessMode {
    match access {
        FilesystemAccess::Deny => FileSystemAccessMode::Deny,
        FilesystemAccess::Read => FileSystemAccessMode::Read,
        FilesystemAccess::ReadWrite => FileSystemAccessMode::Write,
    }
}

fn entry(
    path: &Path,
    access: FileSystemAccessMode,
    skip_missing: bool,
) -> Result<FileSystemSandboxEntry, PolicyRejection> {
    let path = AbsolutePathBuf::from_absolute_path(path).map_err(|_| PolicyRejection {
        code: RejectionCode::UnsupportedPolicy,
        message: "sandbox_policy holds a path that the isolation cannot express".to_owned(),
    })?;
    let path = FileSystemPath::Path {
        path: PathUri::from_abs_path(&path),
    };
    Ok(if skip_missing {
        FileSystemSandboxEntry::skip_missing_path(path, access)
    } else {
        FileSystemSandboxEntry::new(path, access)
    })
}
